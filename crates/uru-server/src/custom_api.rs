use crate::auth::AuthenticatedUser;
use crate::state::AppState;
use axum::{
    body::Bytes,
    extract::{Path as AxumPath, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use uru_core::batch::decode_batch;
use uru_core::hash::ChunkHash;
use uru_core::manifest::FileManifest;
use uru_core::version::FileVersion;
use uru_storage::StorageError;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct CheckChunksRequest {
    pub hashes: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct CheckChunksResponse {
    pub missing: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct CommitFileRequest {
    pub path: String,
    pub manifest: FileManifest,
    pub parent_version_id: Option<Uuid>,
}

#[derive(Debug, Deserialize)]
pub struct RestoreVersionRequest {
    pub version_id: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct FileQuery {
    pub version_id: Option<Uuid>,
}

#[derive(Debug, Deserialize)]
pub struct ListFilesQuery {
    pub prefix: Option<String>,
}

pub fn custom_api_router() -> Router<AppState> {
    Router::new()
        .route("/bloom", get(get_bloom_filter))
        .route("/chunks/check", post(check_chunks))
        .route("/chunks/{hash}", put(upload_chunk))
        .route("/chunks/batch", post(upload_batch))
        .route("/files/commit", post(commit_file))
        .route("/files", get(list_files))
        .route("/files/{*path}", get(download_file).delete(delete_file))
        .route("/files-history/{*path}", get(get_file_history))
        .route("/files-restore/{*path}", post(restore_file_version))
        .route("/gc", post(trigger_gc))
        .route("/stats", get(get_storage_stats))
}

/// GET /api/v1/bloom
async fn get_bloom_filter(
    _user: AuthenticatedUser,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let bytes = state.storage.export_bloom_filter();
    ([(header::CONTENT_TYPE, "application/octet-stream")], bytes)
}

/// POST /api/v1/chunks/check
async fn check_chunks(
    _user: AuthenticatedUser,
    State(state): State<AppState>,
    Json(payload): Json<CheckChunksRequest>,
) -> Result<Json<CheckChunksResponse>, Response> {
    let mut chunk_hashes = Vec::with_capacity(payload.hashes.len());
    for h in &payload.hashes {
        let chunk_hash = ChunkHash::from_hex(h).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": format!("Invalid chunk hash '{h}': {e}") })),
            )
                .into_response()
        })?;
        chunk_hashes.push(chunk_hash);
    }

    let missing = state.storage.check_missing_chunks(&chunk_hashes);
    let missing_hex = missing.iter().map(|h| h.to_hex()).collect();

    Ok(Json(CheckChunksResponse {
        missing: missing_hex,
    }))
}

/// PUT /api/v1/chunks/{hash}
async fn upload_chunk(
    _user: AuthenticatedUser,
    State(state): State<AppState>,
    AxumPath(hash_str): AxumPath<String>,
    body: Bytes,
) -> Result<StatusCode, Response> {
    let declared_hash = ChunkHash::from_hex(&hash_str).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": format!("Invalid chunk hash: {e}") })),
        )
            .into_response()
    })?;

    // Server-side BLAKE3 verification
    let computed_hash = ChunkHash::new_blake3(&body);
    if computed_hash != declared_hash {
        return Err((
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "Conflict",
                "message": format!(
                    "Payload BLAKE3 ({}) does not match declared hash ({})",
                    computed_hash.to_hex(),
                    declared_hash.to_hex()
                )
            })),
        )
            .into_response());
    }

    state
        .storage
        .write_chunk(&declared_hash, &body)
        .map_err(storage_error_response)?;

    Ok(StatusCode::CREATED)
}

/// POST /api/v1/chunks/batch
async fn upload_batch(
    _user: AuthenticatedUser,
    State(state): State<AppState>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, Response> {
    let chunks = decode_batch(&body).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": format!("Invalid batch wire format: {e}") })),
        )
            .into_response()
    })?;

    // Verify all chunks
    for c in &chunks {
        let computed = ChunkHash::new_blake3(&c.data);
        if computed != c.hash {
            return Err((
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error": "Batch chunk hash mismatch",
                    "chunk": c.hash.to_hex()
                })),
            )
                .into_response());
        }
    }

    let written = state
        .storage
        .write_batch(&chunks)
        .map_err(storage_error_response)?;

    Ok(Json(serde_json::json!({ "uploaded": written })))
}

/// POST /api/v1/files/commit
async fn commit_file(
    user: AuthenticatedUser,
    State(state): State<AppState>,
    Json(payload): Json<CommitFileRequest>,
) -> Result<Json<FileVersion>, Response> {
    let version = state
        .storage
        .commit_file(
            &user.0.user_id,
            &payload.path,
            payload.manifest,
            payload.parent_version_id,
        )
        .map_err(storage_error_response)?;

    Ok(Json(version))
}

/// GET /api/v1/files
async fn list_files(
    user: AuthenticatedUser,
    State(state): State<AppState>,
    Query(query): Query<ListFilesQuery>,
) -> Json<Vec<(String, FileVersion)>> {
    let prefix = query.prefix.unwrap_or_else(|| "/".into());
    let files = state.storage.list_files(&user.0.user_id, &prefix);
    Json(files)
}

/// GET /api/v1/files/{*path}
async fn download_file(
    user: AuthenticatedUser,
    State(state): State<AppState>,
    AxumPath(path): AxumPath<String>,
    Query(query): Query<FileQuery>,
    headers: HeaderMap,
) -> Result<Response, Response> {
    let file_version = state
        .storage
        .get_file(&user.0.user_id, &path, query.version_id)
        .map_err(storage_error_response)?;

    let manifest = file_version.manifest.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "File was deleted (delete marker)" })),
        )
            .into_response()
    })?;

    let total_size = manifest.size;
    let content_type = manifest
        .content_type
        .clone()
        .unwrap_or_else(|| "application/octet-stream".into());

    let etag = format!("\"{}\"", manifest.checksums.blake3);

    // Check for HTTP Range header
    if let Some(range_header) = headers.get(header::RANGE).and_then(|v| v.to_str().ok()) {
        if let Some(range_spec) = range_header.strip_prefix("bytes=") {
            let mut parts = range_spec.split('-');
            if let Some(start_str) = parts.next() {
                let start = start_str.parse::<u64>().unwrap_or(0);
                let end = match parts.next() {
                    Some(end_str) if !end_str.is_empty() => {
                        end_str.parse::<u64>().unwrap_or(total_size.saturating_sub(1))
                    }
                    _ => total_size.saturating_sub(1),
                };

                let clamped_end = end.min(total_size.saturating_sub(1));
                if start <= clamped_end && total_size > 0 {
                    let data = state
                        .storage
                        .read_file_range(&manifest, start, clamped_end)
                        .map_err(storage_error_response)?;

                    let content_range = format!("bytes {}-{}/{}", start, clamped_end, total_size);

                    let response = Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header(header::CONTENT_TYPE, content_type)
                        .header(header::CONTENT_RANGE, content_range)
                        .header(header::CONTENT_LENGTH, data.len().to_string())
                        .header(header::ETAG, etag)
                        .body(axum::body::Body::from(data))
                        .unwrap();

                    return Ok(response);
                }
            }
        }
    }

    // Full file download
    let data = if total_size == 0 {
        Vec::new()
    } else {
        state
            .storage
            .read_file_range(&manifest, 0, total_size.saturating_sub(1))
            .map_err(storage_error_response)?
    };

    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_LENGTH, data.len().to_string())
        .header(header::ETAG, etag)
        .header("x-amz-version-id", file_version.version_id.to_string())
        .body(axum::body::Body::from(data))
        .unwrap();

    Ok(response)
}

/// DELETE /api/v1/files/{*path}
async fn delete_file(
    user: AuthenticatedUser,
    State(state): State<AppState>,
    AxumPath(path): AxumPath<String>,
    Query(query): Query<FileQuery>,
) -> Result<StatusCode, Response> {
    state
        .storage
        .delete_file(&user.0.user_id, &path, query.version_id)
        .map_err(storage_error_response)?;

    Ok(StatusCode::NO_CONTENT)
}

/// GET /api/v1/files-history/{*path}
async fn get_file_history(
    user: AuthenticatedUser,
    State(state): State<AppState>,
    AxumPath(path): AxumPath<String>,
) -> Result<Json<Vec<FileVersion>>, Response> {
    let versions = state
        .storage
        .list_versions(&user.0.user_id, &path)
        .map_err(storage_error_response)?;

    Ok(Json(versions))
}

/// POST /api/v1/files-restore/{*path}
async fn restore_file_version(
    user: AuthenticatedUser,
    State(state): State<AppState>,
    AxumPath(path): AxumPath<String>,
    Json(payload): Json<RestoreVersionRequest>,
) -> Result<Json<FileVersion>, Response> {
    let restored = state
        .storage
        .restore_version(&user.0.user_id, &path, payload.version_id)
        .map_err(storage_error_response)?;

    Ok(Json(restored))
}

/// POST /api/v1/gc
async fn trigger_gc(
    _user: AuthenticatedUser,
    State(state): State<AppState>,
) -> Result<Json<uru_storage::GcStats>, Response> {
    let stats = state
        .storage
        .run_gc(&state.retention_policy)
        .map_err(storage_error_response)?;

    Ok(Json(stats))
}

/// GET /api/v1/stats
async fn get_storage_stats(
    _user: AuthenticatedUser,
    State(state): State<AppState>,
) -> Json<serde_json::Value> {
    let total_chunks = state.storage.chunk_count();
    let live_chunks = state.storage.live_chunk_count();

    Json(serde_json::json!({
        "total_chunks_indexed": total_chunks,
        "live_chunks": live_chunks,
        "tombstoned_chunks": total_chunks.saturating_sub(live_chunks),
    }))
}

fn storage_error_response(err: StorageError) -> Response {
    let (status, msg) = match err {
        StorageError::FileNotFound(f) => (StatusCode::NOT_FOUND, format!("File not found: {f}")),
        StorageError::ChunkNotFound(c) => (StatusCode::NOT_FOUND, format!("Chunk not found: {c}")),
        StorageError::VersionNotFound(v) => (StatusCode::NOT_FOUND, format!("Version not found: {v}")),
        StorageError::InvalidPath(p) => (StatusCode::BAD_REQUEST, format!("Invalid path: {p}")),
        StorageError::MissingChunks(chunks) => (
            StatusCode::BAD_REQUEST,
            format!("Missing chunks during commit: {:?}", chunks),
        ),
        StorageError::Io(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("IO error: {e}")),
        StorageError::Serialization(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Serialization error: {e}"),
        ),
        StorageError::Database(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Database error: {e}"),
        ),
    };

    (status, Json(serde_json::json!({ "error": msg }))).into_response()
}
