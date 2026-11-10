use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use tracing::{debug, warn};
use uru_core::hash::ChunkHash;

pub const PACKFILE_MAGIC: &[u8; 8] = b"URUPACK1";
pub const MAX_PACKFILE_SIZE: u64 = 64 * 1024 * 1024; // 64 MB
pub const TRAILER_ENTRY_SIZE: usize = 32 + 8 + 4; // 44 bytes

#[derive(Debug, Clone)]
pub struct TrailerEntry {
    pub hash: ChunkHash,
    pub offset: u64,
    pub length: u32,
}

pub struct PackManager {
    packs_dir: PathBuf,
}

impl PackManager {
    pub fn new(packs_dir: impl Into<PathBuf>) -> io::Result<Self> {
        let packs_dir = packs_dir.into();
        std::fs::create_dir_all(&packs_dir)?;
        Ok(Self { packs_dir })
    }

    pub fn pack_path(&self, pack_id: u32) -> PathBuf {
        self.packs_dir.join(format!("pack_{:08}.pack", pack_id))
    }

    /// Read trailer entries from a sealed packfile
    pub fn read_trailer(path: &Path) -> io::Result<Option<Vec<TrailerEntry>>> {
        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(e) => return Err(e),
        };

        let file_len = file.metadata()?.len();
        if file_len < 12 {
            return Ok(None);
        }

        // Seek to the last 12 bytes
        file.seek(SeekFrom::End(-12))?;
        let mut footer = [0u8; 12];
        file.read_exact(&mut footer)?;

        let trailer_len = u32::from_le_bytes(footer[0..4].try_into().unwrap()) as u64;
        let magic = &footer[4..12];

        if magic != PACKFILE_MAGIC {
            debug!(path = %path.display(), "File does not end with URUPACK1 magic; likely active/unsealed");
            return Ok(None);
        }

        if trailer_len + 12 > file_len {
            warn!(path = %path.display(), "Corrupt pack trailer length: {trailer_len} > {file_len}");
            return Ok(None);
        }

        let trailer_offset = file_len - 12 - trailer_len;
        file.seek(SeekFrom::Start(trailer_offset))?;

        let mut trailer_buf = vec![0u8; trailer_len as usize];
        file.read_exact(&mut trailer_buf)?;

        let num_entries = trailer_buf.len() / TRAILER_ENTRY_SIZE;
        let mut entries = Vec::with_capacity(num_entries);

        for chunk_bytes in trailer_buf.chunks_exact(TRAILER_ENTRY_SIZE) {
            let mut digest = [0u8; 32];
            digest.copy_from_slice(&chunk_bytes[0..32]);
            let hash = ChunkHash::from_blake3_digest(digest);
            let offset = u64::from_le_bytes(chunk_bytes[32..40].try_into().unwrap());
            let length = u32::from_le_bytes(chunk_bytes[40..44].try_into().unwrap());

            entries.push(TrailerEntry {
                hash,
                offset,
                length,
            });
        }

        Ok(Some(entries))
    }

    /// Seal a packfile by appending its trailer, length, and magic
    pub fn seal_packfile(path: &Path, entries: &[TrailerEntry]) -> io::Result<()> {
        let mut file = OpenOptions::new().append(true).open(path)?;

        let mut trailer_data = Vec::with_capacity(entries.len() * TRAILER_ENTRY_SIZE);
        for entry in entries {
            trailer_data.extend_from_slice(entry.hash.as_bytes());
            trailer_data.extend_from_slice(&entry.offset.to_le_bytes());
            trailer_data.extend_from_slice(&entry.length.to_le_bytes());
        }

        let trailer_len = trailer_data.len() as u32;
        file.write_all(&trailer_data)?;
        file.write_all(&trailer_len.to_le_bytes())?;
        file.write_all(PACKFILE_MAGIC)?;
        file.sync_data()?;

        Ok(())
    }

    /// Read raw bytes for a chunk from a specific packfile
    pub fn read_chunk(&self, pack_id: u32, offset: u64, length: u32) -> io::Result<Vec<u8>> {
        let path = self.pack_path(pack_id);
        let mut file = File::open(&path)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; length as usize];
        file.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Immediately reclaim underlying disk blocks on Linux (ext4/XFS/Btrfs/ZFS)
    /// using FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE on 4KB aligned boundaries.
    pub fn punch_hole(&self, pack_id: u32, offset: u64, length: u32) -> io::Result<u64> {
        let path = self.pack_path(pack_id);
        let file = OpenOptions::new().write(true).open(&path)?;

        const PAGE_SIZE: u64 = 4096;
        let aligned_start = (offset + PAGE_SIZE - 1) / PAGE_SIZE * PAGE_SIZE;
        let end = offset + length as u64;
        let aligned_end = (end / PAGE_SIZE) * PAGE_SIZE;

        if aligned_end > aligned_start {
            let punch_len = aligned_end - aligned_start;

            #[cfg(target_os = "linux")]
            {
                use std::os::unix::io::AsRawFd;
                let ret = unsafe {
                    libc::fallocate(
                        file.as_raw_fd(),
                        libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
                        aligned_start as libc::off_t,
                        punch_len as libc::off_t,
                    )
                };
                if ret != 0 {
                    let err = io::Error::last_os_error();
                    debug!("fallocate punch hole skipped/not supported: {err}");
                    return Ok(0);
                }
            }

            Ok(punch_len)
        } else {
            Ok(0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_seal_and_recover_pack_trailer() {
        let tmp = tempdir().unwrap();
        let mgr = PackManager::new(tmp.path()).unwrap();
        let pack_path = mgr.pack_path(1);

        let mut file = File::create(&pack_path).unwrap();
        file.write_all(b"payload chunk 1").unwrap();
        let offset1 = 0;
        let len1 = 15;

        file.write_all(b"payload chunk 2 (longer)").unwrap();
        let offset2 = 15;
        let len2 = 24;
        drop(file);

        let entries = vec![
            TrailerEntry {
                hash: ChunkHash::new_blake3(b"payload chunk 1"),
                offset: offset1,
                length: len1,
            },
            TrailerEntry {
                hash: ChunkHash::new_blake3(b"payload chunk 2 (longer)"),
                offset: offset2,
                length: len2,
            },
        ];

        PackManager::seal_packfile(&pack_path, &entries).unwrap();

        let recovered = PackManager::read_trailer(&pack_path).unwrap().unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].hash, entries[0].hash);
        assert_eq!(recovered[0].offset, entries[0].offset);
        assert_eq!(recovered[0].length, entries[0].length);

        assert_eq!(recovered[1].hash, entries[1].hash);
        assert_eq!(recovered[1].offset, entries[1].offset);
        assert_eq!(recovered[1].length, entries[1].length);

        // Read chunk bytes through manager
        let c1 = mgr.read_chunk(1, offset1, len1).unwrap();
        assert_eq!(c1, b"payload chunk 1");
        let c2 = mgr.read_chunk(1, offset2, len2).unwrap();
        assert_eq!(c2, b"payload chunk 2 (longer)");
    }
}
