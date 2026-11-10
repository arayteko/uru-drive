use crc32fast::Hasher as Crc32Hasher;
use md5::Md5;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use std::fmt;
use std::str::FromStr;
use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum ChunkHashError {
    #[error("Invalid hash algorithm tag: {0:#x}")]
    InvalidAlgo(u8),
    #[error("Invalid hex encoding: {0}")]
    InvalidHex(String),
    #[error("Invalid byte length for chunk hash: expected 32 or 33 bytes, got {0}")]
    InvalidLength(usize),
}

/// Tagged Hash Algorithm byte prefix.
/// 0x1e is assigned to BLAKE3-256 for future extensibility without breaking schemas.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum HashAlgo {
    Blake3_256 = 0x1e,
}

impl HashAlgo {
    pub fn as_u8(&self) -> u8 {
        *self as u8
    }

    pub fn from_u8(tag: u8) -> Result<Self, ChunkHashError> {
        match tag {
            0x1e => Ok(HashAlgo::Blake3_256),
            _ => Err(ChunkHashError::InvalidAlgo(tag)),
        }
    }
}

/// Future-proof tagged 256-bit chunk hash.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChunkHash {
    pub algo: HashAlgo,
    pub digest: [u8; 32],
}

impl ChunkHash {
    pub fn new_blake3(data: &[u8]) -> Self {
        let hash = blake3::hash(data);
        Self {
            algo: HashAlgo::Blake3_256,
            digest: *hash.as_bytes(),
        }
    }

    pub fn from_blake3_digest(digest: [u8; 32]) -> Self {
        Self {
            algo: HashAlgo::Blake3_256,
            digest,
        }
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.digest
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.digest)
    }

    pub fn to_tagged_hex(&self) -> String {
        format!("{:02x}{}", self.algo.as_u8(), hex::encode(self.digest))
    }

    pub fn from_hex(s: &str) -> Result<Self, ChunkHashError> {
        let s = s.trim();
        // Support both raw 32-byte (64 hex chars) and tagged 33-byte (66 hex chars)
        if s.len() == 64 {
            let mut digest = [0u8; 32];
            hex::decode_to_slice(s, &mut digest)
                .map_err(|e| ChunkHashError::InvalidHex(e.to_string()))?;
            Ok(Self {
                algo: HashAlgo::Blake3_256,
                digest,
            })
        } else if s.len() == 66 {
            let bytes = hex::decode(s).map_err(|e| ChunkHashError::InvalidHex(e.to_string()))?;
            let algo = HashAlgo::from_u8(bytes[0])?;
            let mut digest = [0u8; 32];
            digest.copy_from_slice(&bytes[1..33]);
            Ok(Self { algo, digest })
        } else if let Some(stripped) = s.strip_prefix("1e:") {
            Self::from_hex(stripped)
        } else {
            Err(ChunkHashError::InvalidLength(s.len() / 2))
        }
    }
}

impl fmt::Debug for ChunkHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChunkHash({}:{})", self.algo.as_u8(), self.to_hex())
    }
}

impl fmt::Display for ChunkHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl FromStr for ChunkHash {
    type Err = ChunkHashError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

impl Serialize for ChunkHash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for ChunkHash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        ChunkHash::from_hex(&s).map_err(serde::de::Error::custom)
    }
}

/// Multi-hash file metadata for S3, WebDAV, and rclone support
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileChecksums {
    pub blake3: String,         // Required: Whole-file BLAKE3
    pub sha256: Option<String>, // For S3 x-amz-checksum-sha256 & rclone
    pub md5: Option<String>,    // For S3 standard ETag & WebDAV
    pub crc32c: Option<u32>,    // For S3 CRC32C & bit-rot scrubbing
}

/// Multi-hasher computing all 4 file-level checksums in a single pass.
#[derive(Debug, Clone)]
pub struct MultiHasher {
    blake3_hasher: blake3::Hasher,
    sha256_hasher: Sha256,
    md5_hasher: Md5,
    crc32_hasher: Crc32Hasher,
}

impl Default for MultiHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl MultiHasher {
    pub fn new() -> Self {
        Self {
            blake3_hasher: blake3::Hasher::new(),
            sha256_hasher: Sha256::new(),
            md5_hasher: Md5::new(),
            crc32_hasher: Crc32Hasher::new(),
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        self.blake3_hasher.update(data);
        self.sha256_hasher.update(data);
        self.md5_hasher.update(data);
        self.crc32_hasher.update(data);
    }

    pub fn finalize(self) -> FileChecksums {
        let blake3 = self.blake3_hasher.finalize().to_hex().to_string();
        let sha256 = hex::encode(self.sha256_hasher.finalize());
        let md5 = hex::encode(self.md5_hasher.finalize());
        let crc32c = self.crc32_hasher.finalize();

        FileChecksums {
            blake3,
            sha256: Some(sha256),
            md5: Some(md5),
            crc32c: Some(crc32c),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_hash_hex_roundtrip() {
        let data = b"Hello, world Content Addressable Storage!";
        let hash = ChunkHash::new_blake3(data);
        let hex_str = hash.to_hex();
        assert_eq!(hex_str.len(), 64);

        let parsed = ChunkHash::from_hex(&hex_str).unwrap();
        assert_eq!(hash, parsed);
        assert_eq!(parsed.algo, HashAlgo::Blake3_256);

        let tagged_hex = hash.to_tagged_hex();
        assert_eq!(tagged_hex.len(), 66);
        let parsed_tagged = ChunkHash::from_hex(&tagged_hex).unwrap();
        assert_eq!(hash, parsed_tagged);
    }

    #[test]
    fn test_multi_hasher() {
        let data = b"MultiHasher test chunk bytes 1234567890";
        let mut hasher = MultiHasher::new();
        hasher.update(data);
        let checksums = hasher.finalize();

        assert!(!checksums.blake3.is_empty());
        assert!(checksums.sha256.is_some());
        assert!(checksums.md5.is_some());
        assert!(checksums.crc32c.is_some());
    }
}
