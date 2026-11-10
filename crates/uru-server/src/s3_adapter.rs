use crate::auth::UserStore;
use async_trait::async_trait;
use futures::StreamExt;
use s3s::dto::*;
use s3s::{s3_error, S3Request, S3Response, S3Result, S3};
use std::sync::Arc;
use std::time::SystemTime;
use uru_core::chunker::chunk_and_hash_data;
use uru_storage::StorageEngine;
use uuid::Uuid;

pub struct UruS3Adapter {
    storage: Arc<StorageEngine>,
    users: Arc<UserStore>,
}

impl UruS3Adapter {
    pub fn new(storage: Arc<StorageEngine>, users: Arc<UserStore>) -> Self {
        Self { storage, users }
    }

    fn resolve_user<T>(&self, req: &s3s::S3Request<T>) -> String {
        if let Some(cred) = &req.credentials {
            if let Some(user) = self.users.get_user(&cred.access_key) {
                return user.user_id;
            }
            if let Some(user) = self.users.authenticate_api_key(&cred.access_key) {
                return user.user_id;
            }
        }
        "default_user".to_string()
    }

    fn object_path(bucket: &str, key: &str) -> String {
        format!("/{}/{}", bucket.trim_matches('/'), key.trim_matches('/'))
    }
}

#[async_trait]
impl S3 for UruS3Adapter {
    async fn create_bucket(
        &self,
        _req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        Ok(S3Response::new(CreateBucketOutput::default()))
    }

    async fn list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let mut out = ListBucketsOutput::default();
        let bucket = Bucket {
            name: Some("default".into()),
            creation_date: Some(Timestamp::from(SystemTime::UNIX_EPOCH)),
            bucket_region: None,
            bucket_arn: None,
        };
        out.buckets = Some(vec![bucket]);
        Ok(S3Response::new(out))
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        metrics::counter!("uru_cas_http_requests_total", "protocol" => "s3", "method" => "PUT", "status" => "200").increment(1);
        let user_id = self.resolve_user(&req);
        let input = req.input;
        let vpath = Self::object_path(&input.bucket, &input.key);

        let mut payload_bytes = Vec::new();
        if let Some(mut stream) = input.body {
            while let Some(chunk_res) = stream.next().await {
                let chunk = chunk_res.map_err(|e| s3_error!(InternalError, "{e}"))?;
                payload_bytes.extend_from_slice(&chunk);
            }
        }

        metrics::counter!("uru_cas_bytes_ingested_raw_total").increment(payload_bytes.len() as u64);

        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let content_type = input.content_type.map(|c| c.to_string());
        let (manifest, batch_chunks) =
            chunk_and_hash_data(&payload_bytes, content_type, now_ms);

        for chunk in &batch_chunks {
            if !self.storage.has_chunk(&chunk.hash) {
                self.storage
                    .write_chunk(&chunk.hash, &chunk.data)
                    .map_err(|e| s3_error!(InternalError, "{e}"))?;
            }
        }

        let etag = manifest.checksums.md5.as_deref().unwrap_or(&manifest.checksums.blake3).to_string();
        let sha256 = manifest.checksums.sha256.clone();

        let version = self
            .storage
            .commit_file(&user_id, &vpath, manifest, None)
            .map_err(|e| s3_error!(InternalError, "{e}"))?;

        let mut out = PutObjectOutput::default();
        out.e_tag = Some(ETag::Strong(etag));
        out.version_id = Some(version.version_id.to_string());
        out.checksum_sha256 = sha256;

        Ok(S3Response::new(out))
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        metrics::counter!("uru_cas_http_requests_total", "protocol" => "s3", "method" => "GET", "status" => "200").increment(1);
        let user_id = self.resolve_user(&req);
        let input = req.input;
        let vpath = Self::object_path(&input.bucket, &input.key);

        let version_uuid = input
            .version_id
            .as_deref()
            .and_then(|v| Uuid::parse_str(v).ok());

        let file_version = self
            .storage
            .get_file(&user_id, &vpath, version_uuid)
            .map_err(|_| s3_error!(NoSuchKey, "The specified key does not exist."))?;

        let manifest = file_version
            .manifest
            .ok_or_else(|| s3_error!(NoSuchKey, "The specified object was deleted."))?;

        let total_size = manifest.size;
        let etag = manifest.checksums.md5.as_deref().unwrap_or(&manifest.checksums.blake3).to_string();

        let (data, content_range) = if let Some(range) = input.range {
            match range.check(total_size) {
                Ok(r) => {
                    let start = r.start;
                    let end = r.end.saturating_sub(1);
                    let bytes = self
                        .storage
                        .read_file_range(&manifest, start, end)
                        .map_err(|e| s3_error!(InternalError, "{e}"))?;
                    let cr = format!("bytes {start}-{end}/{total_size}");
                    (bytes, Some(cr))
                }
                Err(_) => return Err(s3_error!(InvalidRange, "The requested range is not satisfiable.")),
            }
        } else if total_size == 0 {
            (Vec::new(), None)
        } else {
            let bytes = self
                .storage
                .read_file_range(&manifest, 0, total_size.saturating_sub(1))
                .map_err(|e| s3_error!(InternalError, "{e}"))?;
            (bytes, None)
        };

        let mut out = GetObjectOutput::default();
        out.content_length = Some(data.len() as i64);
        out.content_range = content_range;
        out.e_tag = Some(ETag::Strong(etag));
        out.version_id = Some(file_version.version_id.to_string());
        out.content_type = manifest.content_type.map(ContentType::from);
        out.body = Some(StreamingBlob::from_bytes(bytes::Bytes::from(data)));

        Ok(S3Response::new(out))
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let user_id = self.resolve_user(&req);
        let input = req.input;
        let vpath = Self::object_path(&input.bucket, &input.key);

        let version_uuid = input
            .version_id
            .as_deref()
            .and_then(|v| Uuid::parse_str(v).ok());

        let file_version = self
            .storage
            .get_file(&user_id, &vpath, version_uuid)
            .map_err(|_| s3_error!(NoSuchKey, "The specified key does not exist."))?;

        let manifest = file_version
            .manifest
            .ok_or_else(|| s3_error!(NoSuchKey, "The specified object was deleted."))?;

        let etag = manifest.checksums.md5.as_deref().unwrap_or(&manifest.checksums.blake3).to_string();

        let mut out = HeadObjectOutput::default();
        out.content_length = Some(manifest.size as i64);
        out.e_tag = Some(ETag::Strong(etag));
        out.version_id = Some(file_version.version_id.to_string());
        out.content_type = manifest.content_type.map(ContentType::from);

        Ok(S3Response::new(out))
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let user_id = self.resolve_user(&req);
        let input = req.input;
        let vpath = Self::object_path(&input.bucket, &input.key);

        let version_uuid = input
            .version_id
            .as_deref()
            .and_then(|v| Uuid::parse_str(v).ok());

        let del_res = self
            .storage
            .delete_file(&user_id, &vpath, version_uuid)
            .map_err(|e| s3_error!(InternalError, "{e}"))?;

        let mut out = DeleteObjectOutput::default();
        out.version_id = Some(del_res.version_id.to_string());
        out.delete_marker = Some(del_res.is_delete_marker);

        Ok(S3Response::new(out))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let user_id = self.resolve_user(&req);
        let input = req.input;
        let prefix = format!("/{}", input.bucket.trim_matches('/'));

        let files = self.storage.list_files(&user_id, &prefix);
        let mut objects = Vec::new();

        for (path, ver) in files {
            if let Some(manifest) = ver.manifest {
                let key = path.strip_prefix(&prefix).unwrap_or(&path).trim_start_matches('/');
                let etag = manifest.checksums.md5.as_deref().unwrap_or(&manifest.checksums.blake3).to_string();
                let obj = Object {
                    key: Some(key.to_string()),
                    size: Some(manifest.size as i64),
                    e_tag: Some(ETag::Strong(etag)),
                    last_modified: Some(Timestamp::from(SystemTime::UNIX_EPOCH)),
                    ..Default::default()
                };
                objects.push(obj);
            }
        }

        let mut out = ListObjectsV2Output::default();
        out.name = Some(input.bucket);
        out.key_count = Some(objects.len() as i32);
        out.contents = Some(objects);

        Ok(S3Response::new(out))
    }

    async fn list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        let user_id = self.resolve_user(&req);
        let input = req.input;
        let prefix = format!("/{}", input.bucket.trim_matches('/'));

        let mut versions = Vec::new();
        let mut delete_markers = Vec::new();

        let live_files = self.storage.list_files(&user_id, &prefix);
        for (path, _) in live_files {
            if let Ok(file_vers) = self.storage.list_versions(&user_id, &path) {
                let key = path.strip_prefix(&prefix).unwrap_or(&path).trim_start_matches('/');
                for fv in file_vers {
                    if fv.is_delete_marker {
                        delete_markers.push(DeleteMarkerEntry {
                            key: Some(key.to_string()),
                            version_id: Some(fv.version_id.to_string()),
                            is_latest: Some(false),
                            last_modified: Some(Timestamp::from(SystemTime::UNIX_EPOCH)),
                            owner: None,
                        });
                    } else if let Some(m) = fv.manifest {
                        let etag = m.checksums.md5.as_deref().unwrap_or(&m.checksums.blake3).to_string();
                        versions.push(ObjectVersion {
                            key: Some(key.to_string()),
                            version_id: Some(fv.version_id.to_string()),
                            size: Some(m.size as i64),
                            e_tag: Some(ETag::Strong(etag)),
                            is_latest: Some(false),
                            last_modified: Some(Timestamp::from(SystemTime::UNIX_EPOCH)),
                            ..Default::default()
                        });
                    }
                }
            }
        }

        let mut out = ListObjectVersionsOutput::default();
        out.name = Some(input.bucket);
        out.versions = Some(versions);
        out.delete_markers = Some(delete_markers);

        Ok(S3Response::new(out))
    }
}
