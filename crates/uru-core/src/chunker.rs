use crate::batch::BatchChunk;
use crate::hash::{ChunkHash, MultiHasher};
use crate::manifest::{ChunkRef, FileManifest};
use bytes::Bytes;
use fastcdc::v2020::FastCDC;

pub const MIN_CHUNK_SIZE: usize = 16_384; // 16 KB
pub const AVG_CHUNK_SIZE: usize = 65_536; // 64 KB
pub const MAX_CHUNK_SIZE: usize = 262_144; // 256 KB

pub struct FastCdcChunker;

impl FastCdcChunker {
    /// Chunk an in-memory byte slice using FastCDC v2020
    pub fn chunk_slice(data: &[u8]) -> Vec<ChunkRef> {
        if data.is_empty() {
            return Vec::new();
        }

        let chunker = FastCDC::new(data, MIN_CHUNK_SIZE, AVG_CHUNK_SIZE, MAX_CHUNK_SIZE);
        let mut chunks = Vec::new();

        for chunk in chunker {
            let chunk_data = &data[chunk.offset..chunk.offset + chunk.length];
            let hash = ChunkHash::new_blake3(chunk_data);
            chunks.push(ChunkRef {
                hash,
                offset: chunk.offset as u64,
                length: chunk.length as u32,
            });
        }

        chunks
    }
}

/// Helper that chunks in-memory data, computes all multi-checksums,
/// and produces both the `FileManifest` and the raw `Vec<BatchChunk>`.
pub fn chunk_and_hash_data(
    data: &[u8],
    content_type: Option<String>,
    now_ms: u64,
) -> (FileManifest, Vec<BatchChunk>) {
    let mut multi_hasher = MultiHasher::new();
    multi_hasher.update(data);
    let checksums = multi_hasher.finalize();

    if data.is_empty() {
        let manifest = FileManifest::new(0, Vec::new(), checksums, content_type, now_ms);
        return (manifest, Vec::new());
    }

    let chunker = FastCDC::new(data, MIN_CHUNK_SIZE, AVG_CHUNK_SIZE, MAX_CHUNK_SIZE);
    let mut chunk_refs = Vec::new();
    let mut batch_chunks = Vec::new();

    for chunk in chunker {
        let slice = &data[chunk.offset..chunk.offset + chunk.length];
        let bytes = Bytes::copy_from_slice(slice);
        let hash = ChunkHash::new_blake3(&bytes);

        chunk_refs.push(ChunkRef {
            hash,
            offset: chunk.offset as u64,
            length: chunk.length as u32,
        });

        batch_chunks.push(BatchChunk { hash, data: bytes });
    }

    let manifest = FileManifest::new(
        data.len() as u64,
        chunk_refs,
        checksums,
        content_type,
        now_ms,
    );
    (manifest, batch_chunks)
}

pub fn chunk_slice(data: &[u8]) -> Vec<ChunkRef> {
    FastCdcChunker::chunk_slice(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_slice_boundaries() {
        // Generate pseudo-random data to test FastCDC cut-points
        let mut data = Vec::with_capacity(300_000);
        for i in 0..300_000 {
            data.push(((i * 31) ^ (i >> 3)) as u8);
        }

        let chunks = chunk_slice(&data);
        assert!(!chunks.is_empty());

        let mut reconstructed_len = 0;
        for c in &chunks {
            assert!(c.length >= MIN_CHUNK_SIZE as u32 || c.length == (data.len() - c.offset as usize) as u32);
            assert!(c.length <= MAX_CHUNK_SIZE as u32);
            reconstructed_len += c.length as usize;
        }
        assert_eq!(reconstructed_len, data.len());
    }

    #[test]
    fn test_deterministic_chunking() {
        let data = vec![77u8; 150_000];
        let run1 = chunk_slice(&data);
        let run2 = chunk_slice(&data);

        assert_eq!(run1.len(), run2.len());
        for (c1, c2) in run1.iter().zip(run2.iter()) {
            assert_eq!(c1.offset, c2.offset);
            assert_eq!(c1.length, c2.length);
            assert_eq!(c1.hash, c2.hash);
        }
    }
}
