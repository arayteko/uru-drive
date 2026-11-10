use bytes::{Buf, Bytes};
use dav_server::{
    davpath::DavPath,
    fs::{
        DavDirEntry, DavFile, DavFileSystem, DavMetaData, FsError, FsFuture, FsResult, FsStream,
        OpenOptions, ReadDirMeta,
    },
};
use futures::{stream, FutureExt};
use std::fmt::Debug;
use std::io::SeekFrom;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uru_core::chunker::chunk_and_hash_data;
use uru_storage::StorageEngine;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct UruDavMetaData {
    pub is_dir: bool,
    pub len: u64,
    pub modified: SystemTime,
}

impl DavMetaData for UruDavMetaData {
    fn len(&self) -> u64 {
        self.len
    }

    fn modified(&self) -> FsResult<SystemTime> {
        Ok(self.modified)
    }

    fn is_dir(&self) -> bool {
        self.is_dir
    }

    fn is_symlink(&self) -> bool {
        false
    }
}

pub struct UruDavDirEntry {
    pub name: Vec<u8>,
    pub meta: UruDavMetaData,
}

impl DavDirEntry for UruDavDirEntry {
    fn name(&self) -> Vec<u8> {
        self.name.clone()
    }

    fn metadata(&self) -> FsFuture<'_, Box<dyn DavMetaData>> {
        let meta = self.meta.clone();
        async move { Ok(Box::new(meta) as Box<dyn DavMetaData>) }.boxed()
    }
}

pub struct UruDavFile {
    user_id: String,
    path: String,
    storage: Arc<StorageEngine>,
    read_data: Option<Vec<u8>>,
    cursor: usize,
    write_buf: Option<Vec<u8>>,
    is_write: bool,
    created_at_ms: u64,
}

impl Debug for UruDavFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UruDavFile")
            .field("path", &self.path)
            .field("cursor", &self.cursor)
            .field("is_write", &self.is_write)
            .finish()
    }
}

impl DavFile for UruDavFile {
    fn metadata(&mut self) -> FsFuture<'_, Box<dyn DavMetaData>> {
        let len = if self.is_write {
            self.write_buf.as_ref().map_or(0, |b| b.len() as u64)
        } else {
            self.read_data.as_ref().map_or(0, |b| b.len() as u64)
        };

        let modified = UNIX_EPOCH + Duration::from_millis(self.created_at_ms);

        let meta = UruDavMetaData {
            is_dir: false,
            len,
            modified,
        };

        async move { Ok(Box::new(meta) as Box<dyn DavMetaData>) }.boxed()
    }

    fn write_buf(&mut self, mut buf: Box<dyn Buf + Send>) -> FsFuture<'_, ()> {
        let mut bytes = vec![0u8; buf.remaining()];
        buf.copy_to_slice(&mut bytes);
        self.write_bytes(Bytes::from(bytes))
    }

    fn write_bytes(&mut self, buf: Bytes) -> FsFuture<'_, ()> {
        if let Some(wbuf) = &mut self.write_buf {
            wbuf.extend_from_slice(&buf);
        }
        async move { Ok(()) }.boxed()
    }

    fn read_bytes(&mut self, count: usize) -> FsFuture<'_, Bytes> {
        let res = if let Some(data) = &self.read_data {
            let start = self.cursor.min(data.len());
            let end = (self.cursor + count).min(data.len());
            self.cursor = end;
            Bytes::copy_from_slice(&data[start..end])
        } else {
            Bytes::new()
        };
        async move { Ok(res) }.boxed()
    }

    fn seek(&mut self, pos: SeekFrom) -> FsFuture<'_, u64> {
        let total = self.read_data.as_ref().map_or(0, |b| b.len());
        let new_pos = match pos {
            SeekFrom::Start(offset) => offset as usize,
            SeekFrom::Current(offset) => (self.cursor as i64 + offset).max(0) as usize,
            SeekFrom::End(offset) => (total as i64 + offset).max(0) as usize,
        };
        self.cursor = new_pos.min(total);
        let ret = self.cursor as u64;
        async move { Ok(ret) }.boxed()
    }

    fn flush(&mut self) -> FsFuture<'_, ()> {
        if !self.is_write {
            return async move { Ok(()) }.boxed();
        }

        let payload = self.write_buf.take().unwrap_or_default();
        let storage = self.storage.clone();
        let user_id = self.user_id.clone();
        let path = self.path.clone();

        async move {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            let (manifest, batch_chunks) = chunk_and_hash_data(&payload, None, now_ms);

            // Write chunks to global store
            for c in &batch_chunks {
                if !storage.has_chunk(&c.hash) {
                    storage
                        .write_chunk(&c.hash, &c.data)
                        .map_err(|_| FsError::GeneralFailure)?;
                }
            }

            // Commit manifest to user's isolated VFS path
            storage
                .commit_file(&user_id, &path, manifest, None)
                .map_err(|_| FsError::GeneralFailure)?;

            metrics::counter!("uru_cas_http_requests_total", "protocol" => "dav", "method" => "PUT", "status" => "200").increment(1);
            Ok(())
        }
        .boxed()
    }
}

#[derive(Clone)]
pub struct UruDavFileSystem {
    user_id: String,
    storage: Arc<StorageEngine>,
}

impl UruDavFileSystem {
    pub fn new(user_id: String, storage: Arc<StorageEngine>) -> Self {
        Self { user_id, storage }
    }
}

impl DavFileSystem for UruDavFileSystem {
    fn open<'a>(
        &'a self,
        path: &'a DavPath,
        options: OpenOptions,
    ) -> FsFuture<'a, Box<dyn DavFile>> {
        let vpath = path.as_url_string();
        let storage = self.storage.clone();
        let user_id = self.user_id.clone();

        async move {
            metrics::counter!("uru_cas_http_requests_total", "protocol" => "dav", "method" => "OPEN", "status" => "200").increment(1);

            // Virtual history tree check: /.history/<file_path>/<version_id>
            if let Some(hist_path) = vpath.strip_prefix("/.history") {
                if options.write {
                    return Err(FsError::Forbidden); // History tree is strictly read-only
                }

                // Format: /.history/<original_path>/<version_id>
                let parts: Vec<&str> = hist_path.trim_matches('/').rsplitn(2, '/').collect();
                if parts.len() == 2 {
                    let version_str = parts[0];
                    let orig_path = format!("/{}", parts[1]);
                    if let Ok(vid) = Uuid::parse_str(version_str) {
                        if let Ok(ver) = storage.get_file(&user_id, &orig_path, Some(vid)) {
                            if let Some(m) = ver.manifest {
                                let data = storage
                                    .read_file_range(&m, 0, m.size.saturating_sub(1))
                                    .map_err(|_| FsError::GeneralFailure)?;

                                return Ok(Box::new(UruDavFile {
                                    user_id,
                                    path: vpath,
                                    storage,
                                    read_data: Some(data),
                                    cursor: 0,
                                    write_buf: None,
                                    is_write: false,
                                    created_at_ms: ver.created_at_ms,
                                }) as Box<dyn DavFile>);
                            }
                        }
                    }
                }
                return Err(FsError::NotFound);
            }

            if options.write {
                Ok(Box::new(UruDavFile {
                    user_id,
                    path: vpath,
                    storage,
                    read_data: None,
                    cursor: 0,
                    write_buf: Some(Vec::new()),
                    is_write: true,
                    created_at_ms: 0,
                }) as Box<dyn DavFile>)
            } else {
                let file_version = storage
                    .get_file(&user_id, &vpath, None)
                    .map_err(|_| FsError::NotFound)?;

                let manifest = file_version.manifest.ok_or(FsError::NotFound)?;
                let data = storage
                    .read_file_range(&manifest, 0, manifest.size.saturating_sub(1))
                    .map_err(|_| FsError::GeneralFailure)?;

                Ok(Box::new(UruDavFile {
                    user_id,
                    path: vpath,
                    storage,
                    read_data: Some(data),
                    cursor: 0,
                    write_buf: None,
                    is_write: false,
                    created_at_ms: file_version.created_at_ms,
                }) as Box<dyn DavFile>)
            }
        }
        .boxed()
    }

    fn read_dir<'a>(
        &'a self,
        path: &'a DavPath,
        _meta: ReadDirMeta,
    ) -> FsFuture<'a, FsStream<Box<dyn DavDirEntry>>> {
        let vpath = path.as_url_string();
        let storage = self.storage.clone();
        let user_id = self.user_id.clone();

        async move {
            let mut entries: Vec<Box<dyn DavDirEntry>> = Vec::new();

            // History directory handling
            if vpath == "/.history" || vpath.starts_with("/.history/") {
                let subpath = format!("/{}", vpath.strip_prefix("/.history").unwrap_or("").trim_matches('/'));
                if subpath != "/" {
                    if let Ok(versions) = storage.list_versions(&user_id, &subpath) {
                        for v in versions {
                            if let Some(m) = v.manifest {
                                let name = format!("{}.ver", v.version_id);
                                let entry = UruDavDirEntry {
                                    name: name.into_bytes(),
                                    meta: UruDavMetaData {
                                        is_dir: false,
                                        len: m.size,
                                        modified: UNIX_EPOCH + Duration::from_millis(v.created_at_ms),
                                    },
                                };
                                entries.push(Box::new(entry));
                            }
                        }
                    }
                }
                return Ok(Box::pin(stream::iter(entries.into_iter().map(Ok))) as FsStream<Box<dyn DavDirEntry>>);
            }

            // Normal directory listing from isolated user files
            let prefix = if vpath == "/" { "/".to_string() } else { format!("{}/", vpath.trim_end_matches('/')) };
            let files = storage.list_files(&user_id, &prefix);

            use std::collections::HashSet;
            let mut seen_dirs = HashSet::new();

            for (file_path, ver) in files {
                let rel = file_path.strip_prefix(&prefix).unwrap_or(&file_path);
                if let Some(first_slash) = rel.find('/') {
                    let dir_name = &rel[..first_slash];
                    if seen_dirs.insert(dir_name.to_string()) {
                        entries.push(Box::new(UruDavDirEntry {
                            name: dir_name.as_bytes().to_vec(),
                            meta: UruDavMetaData {
                                is_dir: true,
                                len: 0,
                                modified: SystemTime::now(),
                            },
                        }));
                    }
                } else if let Some(m) = ver.manifest {
                    entries.push(Box::new(UruDavDirEntry {
                        name: rel.as_bytes().to_vec(),
                        meta: UruDavMetaData {
                            is_dir: false,
                            len: m.size,
                            modified: UNIX_EPOCH + Duration::from_millis(ver.created_at_ms),
                        },
                    }));
                }
            }

            // Also expose virtual .history entry at root
            if vpath == "/" {
                entries.push(Box::new(UruDavDirEntry {
                    name: b".history".to_vec(),
                    meta: UruDavMetaData {
                        is_dir: true,
                        len: 0,
                        modified: SystemTime::now(),
                    },
                }));
            }

            Ok(Box::pin(stream::iter(entries.into_iter().map(Ok))) as FsStream<Box<dyn DavDirEntry>>)
        }
        .boxed()
    }

    fn metadata<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, Box<dyn DavMetaData>> {
        let vpath = path.as_url_string();
        let storage = self.storage.clone();
        let user_id = self.user_id.clone();

        async move {
            if vpath == "/" || vpath == "/.history" {
                return Ok(Box::new(UruDavMetaData {
                    is_dir: true,
                    len: 0,
                    modified: SystemTime::now(),
                }) as Box<dyn DavMetaData>);
            }

            if let Ok(file_version) = storage.get_file(&user_id, &vpath, None) {
                if let Some(m) = file_version.manifest {
                    return Ok(Box::new(UruDavMetaData {
                        is_dir: false,
                        len: m.size,
                        modified: UNIX_EPOCH + Duration::from_millis(file_version.created_at_ms),
                    }) as Box<dyn DavMetaData>);
                }
            }

            // Check if it's a directory prefix
            let prefix = format!("{}/", vpath.trim_end_matches('/'));
            let files = storage.list_files(&user_id, &prefix);
            if !files.is_empty() {
                return Ok(Box::new(UruDavMetaData {
                    is_dir: true,
                    len: 0,
                    modified: SystemTime::now(),
                }) as Box<dyn DavMetaData>);
            }

            Err(FsError::NotFound)
        }
        .boxed()
    }

    fn symlink_metadata<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, Box<dyn DavMetaData>> {
        self.metadata(path)
    }

    fn remove_file<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, ()> {
        let vpath = path.as_url_string();
        let storage = self.storage.clone();
        let user_id = self.user_id.clone();

        async move {
            storage
                .delete_file(&user_id, &vpath, None)
                .map_err(|_| FsError::NotFound)?;
            Ok(())
        }
        .boxed()
    }
}
