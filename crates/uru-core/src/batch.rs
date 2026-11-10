use crate::hash::ChunkHash;
use bytes::Bytes;
use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum BatchCodecError {
    #[error("Unexpected EOF while parsing batch chunk header (need 36 bytes)")]
    UnexpectedEofHeader,
    #[error("Unexpected EOF while reading chunk payload: declared {declared} bytes, remaining {remaining}")]
    UnexpectedEofPayload { declared: usize, remaining: usize },
    #[error("Chunk size {0} exceeds maximum allowed chunk limit (16MB)")]
    ChunkTooLarge(u32),
}

/// A chunk ready for or decoded from the packed batch format
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchChunk {
    pub hash: ChunkHash,
    pub data: Bytes,
}

impl BatchChunk {
    pub fn new(hash: ChunkHash, data: impl Into<Bytes>) -> Self {
        Self {
            hash,
            data: data.into(),
        }
    }

    pub fn from_data(data: impl Into<Bytes>) -> Self {
        let bytes = data.into();
        let hash = ChunkHash::new_blake3(&bytes);
        Self { hash, data: bytes }
    }
}

/// Encode chunks into packed binary wire format:
/// `[32-byte BLAKE3 Hash][4-byte Little-Endian Length][Raw Chunk Bytes]...`
pub fn encode_batch(chunks: &[BatchChunk]) -> Vec<u8> {
    let total_len: usize = chunks.iter().map(|c| 32 + 4 + c.data.len()).sum();
    let mut out = Vec::with_capacity(total_len);

    for chunk in chunks {
        out.extend_from_slice(chunk.hash.as_bytes());
        let len = chunk.data.len() as u32;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&chunk.data);
    }

    out
}

/// Decode packed binary wire format into `Vec<BatchChunk>`
pub fn decode_batch(mut buf: &[u8]) -> Result<Vec<BatchChunk>, BatchCodecError> {
    let mut chunks = Vec::new();
    const MAX_CHUNK_LIMIT: u32 = 16 * 1024 * 1024; // 16MB safety cap

    while !buf.is_empty() {
        if buf.len() < 36 {
            return Err(BatchCodecError::UnexpectedEofHeader);
        }

        let mut digest = [0u8; 32];
        digest.copy_from_slice(&buf[..32]);
        let hash = ChunkHash::from_blake3_digest(digest);

        let len = u32::from_le_bytes(buf[32..36].try_into().unwrap());
        if len > MAX_CHUNK_LIMIT {
            return Err(BatchCodecError::ChunkTooLarge(len));
        }

        buf = &buf[36..];
        let len_usize = len as usize;
        if buf.len() < len_usize {
            return Err(BatchCodecError::UnexpectedEofPayload {
                declared: len_usize,
                remaining: buf.len(),
            });
        }

        let chunk_data = Bytes::copy_from_slice(&buf[..len_usize]);
        buf = &buf[len_usize..];

        chunks.push(BatchChunk {
            hash,
            data: chunk_data,
        });
    }

    Ok(chunks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_codec_roundtrip() {
        let chunk1_data = b"chunk 1 data for batch";
        let chunk2_data = b"chunk 2 another slightly longer piece of data";
        let chunk3_data = vec![0xAB; 2000];

        let chunks = vec![
            BatchChunk::from_data(chunk1_data.as_slice()),
            BatchChunk::from_data(chunk2_data.as_slice()),
            BatchChunk::from_data(chunk3_data),
        ];

        let encoded = encode_batch(&chunks);
        let decoded = decode_batch(&encoded).unwrap();

        assert_eq!(chunks.len(), decoded.len());
        for (orig, dec) in chunks.iter().zip(decoded.iter()) {
            assert_eq!(orig.hash, dec.hash);
            assert_eq!(orig.data, dec.data);
        }
    }

    #[test]
    fn test_batch_codec_partial_buffer() {
        let chunk = BatchChunk::from_data(b"valid chunk".as_slice());
        let mut encoded = encode_batch(&[chunk]);
        encoded.truncate(encoded.len() - 3); // Truncate payload

        let err = decode_batch(&encoded);
        assert!(matches!(err, Err(BatchCodecError::UnexpectedEofPayload { .. })));
    }
}
