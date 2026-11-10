pub mod batch;
pub mod chunker;
pub mod hash;
pub mod manifest;
pub mod version;

pub use batch::{decode_batch, encode_batch, BatchChunk, BatchCodecError};
pub use chunker::{
    chunk_and_hash_data, chunk_slice, FastCdcChunker, AVG_CHUNK_SIZE, MAX_CHUNK_SIZE,
    MIN_CHUNK_SIZE,
};
pub use hash::{ChunkHash, ChunkHashError, FileChecksums, HashAlgo, MultiHasher};
pub use manifest::{ChunkRef, FileManifest};
pub use version::{FileVersion, RetentionPolicy, VersionDiffStats, VersionedFile};

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct GcStats {
    pub versions_pruned: usize,
    pub chunks_tombstoned: usize,
    pub bytes_punched: u64,
    pub packfiles_compacted: usize,
    pub bytes_compacted: u64,
    pub live_chunks_count: usize,
    pub duration_ms: u64,
}

/// Strictly sanitize and normalize virtual paths.
/// Rejects path traversal sequences (..), null bytes, and empty paths.
pub fn normalize_path(path: &str) -> Result<String, String> {
    if path.is_empty() || path.contains('\0') {
        return Err("Path contains null bytes or is empty".into());
    }

    let mut parts = Vec::new();
    for seg in path.split('/') {
        if seg.is_empty() || seg == "." {
            continue;
        }
        if seg == ".." {
            return Err("Path traversal (..) forbidden".into());
        }
        parts.push(seg);
    }

    Ok(format!("/{}", parts.join("/")))
}
