use crate::hash::{ChunkHash, FileChecksums};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkRef {
    pub hash: ChunkHash,
    pub offset: u64,
    pub length: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileManifest {
    pub size: u64,
    pub chunks: Vec<ChunkRef>,
    pub checksums: FileChecksums,
    pub content_type: Option<String>,
    pub created_at_ms: u64,
}

/// Sliced reference to a chunk for range reading
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkSliceRef<'a> {
    pub chunk: &'a ChunkRef,
    pub chunk_index: usize,
    /// Offset within this chunk to start reading
    pub slice_offset: usize,
    /// Number of bytes to read from this chunk
    pub slice_len: usize,
}

impl FileManifest {
    pub fn new(
        size: u64,
        chunks: Vec<ChunkRef>,
        checksums: FileChecksums,
        content_type: Option<String>,
        created_at_ms: u64,
    ) -> Self {
        Self {
            size,
            chunks,
            checksums,
            content_type,
            created_at_ms,
        }
    }

    /// O(log C) range resolution using partition_point.
    /// `start` and `end_inclusive` are byte offsets within the file.
    pub fn find_chunks_for_range(
        &self,
        start: u64,
        end_inclusive: u64,
    ) -> Vec<ChunkSliceRef<'_>> {
        if self.chunks.is_empty() || start > end_inclusive || start >= self.size {
            return Vec::new();
        }

        let clamped_end = end_inclusive.min(self.size.saturating_sub(1));

        // Binary search the first chunk whose end offset > start:
        // partition_point finds the first index where predicate is false.
        let first_idx = self
            .chunks
            .partition_point(|c| c.offset + (c.length as u64) <= start);

        let mut results = Vec::new();
        for idx in first_idx..self.chunks.len() {
            let chunk = &self.chunks[idx];
            if chunk.offset > clamped_end {
                break;
            }

            let chunk_end = chunk.offset + (chunk.length as u64) - 1;

            let slice_start = if start > chunk.offset {
                (start - chunk.offset) as usize
            } else {
                0
            };

            let slice_end = if clamped_end < chunk_end {
                (clamped_end - chunk.offset + 1) as usize
            } else {
                chunk.length as usize
            };

            if slice_end > slice_start {
                results.push(ChunkSliceRef {
                    chunk,
                    chunk_index: idx,
                    slice_offset: slice_start,
                    slice_len: slice_end - slice_start,
                });
            }
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_chunks_binary_search() {
        let chunk1 = ChunkRef {
            hash: ChunkHash::new_blake3(b"chunk1"),
            offset: 0,
            length: 1000,
        };
        let chunk2 = ChunkRef {
            hash: ChunkHash::new_blake3(b"chunk2"),
            offset: 1000,
            length: 2000,
        };
        let chunk3 = ChunkRef {
            hash: ChunkHash::new_blake3(b"chunk3"),
            offset: 3000,
            length: 1500,
        };

        let manifest = FileManifest {
            size: 4500,
            chunks: vec![chunk1, chunk2, chunk3],
            checksums: FileChecksums {
                blake3: "hash".into(),
                sha256: None,
                md5: None,
                crc32c: None,
            },
            content_type: None,
            created_at_ms: 0,
        };

        // Query middle of chunk 2 into chunk 3
        let slices = manifest.find_chunks_for_range(2500, 3500);
        assert_eq!(slices.len(), 2);

        // chunk2: offset 2500 is chunk2 offset 1500, length up to 3000 (500 bytes)
        assert_eq!(slices[0].chunk_index, 1);
        assert_eq!(slices[0].slice_offset, 1500);
        assert_eq!(slices[0].slice_len, 500);

        // chunk3: offset 3000 to 3500 (501 bytes: 3000..=3500)
        assert_eq!(slices[1].chunk_index, 2);
        assert_eq!(slices[1].slice_offset, 0);
        assert_eq!(slices[1].slice_len, 501);
    }
}
