use crate::bloom::GlobalBloomFilter;
use crate::gc::{ActivePackState, GcEngine, GcStats};
use crate::index::{ChunkIndex, ChunkLocation, ChunkState};
use crate::pack::{PackManager, TrailerEntry, MAX_PACKFILE_SIZE};
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::Deserialize;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tracing::{info, warn};
use uru_core::batch::BatchChunk;
use uru_core::hash::ChunkHash;
use uru_core::manifest::FileManifest;
use uru_core::version::{RetentionPolicy, FileVersion, VersionedFile};
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Chunk not found: {0}")]
    ChunkNotFound(String),
    #[error("File not found: {0}")]
    FileNotFound(String),
    #[error("Invalid virtual path: {0}")]
    InvalidPath(String),
    #[error("Missing chunks during commit: {0:?}")]
    MissingChunks(Vec<ChunkHash>),
    #[error("Version not found: {0}")]
    VersionNotFound(Uuid),
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Database error: {0}")]
    Database(String),
}

impl From<rocksdb::Error> for StorageError {
    fn from(err: rocksdb::Error) -> Self {
        StorageError::Database(err.to_string())
    }
}

#[inline]
fn vfs_key(user_id: &str, norm_path: &str) -> Vec<u8> {
    format!("{}\0{}", user_id, norm_path).into_bytes()
}

pub struct StorageEngine {
    root_dir: PathBuf,
    vfs_db: Arc<rocksdb::DB>,
    pack_mgr: PackManager,
    chunk_index: ChunkIndex,
    bloom: GlobalBloomFilter,
    active_pack: RwLock<ActivePackState>,
}

impl StorageEngine {
    pub fn open(root_dir: impl Into<PathBuf>) -> Result<Arc<Self>, StorageError> {
        let root_dir = root_dir.into();
        let packs_dir = root_dir.join("packs");
        let metadata_dir = root_dir.join("metadata");
        let vfs_db_path = root_dir.join("vfs_db");

        std::fs::create_dir_all(&packs_dir)?;
        std::fs::create_dir_all(&metadata_dir)?;

        let mut db_opts = rocksdb::Options::default();
        db_opts.create_if_missing(true);
        let vfs_db = Arc::new(rocksdb::DB::open(&db_opts, &vfs_db_path)?);

        let pack_mgr = PackManager::new(&packs_dir)?;
        let chunk_index = DashMap::new();
        let bloom = GlobalBloomFilter::new(100_000, 0.001);

        // Scan existing packfiles
        let mut highest_pack_id = 0u32;
        let mut total_chunks_recovered = 0;

        let mut pack_paths: Vec<PathBuf> = std::fs::read_dir(&packs_dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().map_or(false, |ext| ext == "pack"))
            .collect();
        pack_paths.sort();

        for path in &pack_paths {
            if let Some(file_name) = path.file_stem().and_then(|s| s.to_str()) {
                if let Some(id_str) = file_name.strip_prefix("pack_") {
                    if let Ok(id) = id_str.parse::<u32>() {
                        highest_pack_id = highest_pack_id.max(id);

                        if let Ok(Some(entries)) = PackManager::read_trailer(path) {
                            for entry in entries {
                                chunk_index.insert(
                                    entry.hash,
                                    ChunkLocation::new(id, entry.offset, entry.length),
                                );
                                bloom.insert(&entry.hash);
                                total_chunks_recovered += 1;
                            }
                        }
                    }
                }
            }
        }

        info!(
            recovered_chunks = total_chunks_recovered,
            highest_pack_id, "Packfile trailers scanned and in-memory index recovered"
        );

        // Open active packfile (create a new one or continue if highest was empty)
        let active_pack_id = highest_pack_id + 1;
        let active_pack_path = pack_mgr.pack_path(active_pack_id);
        let active_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active_pack_path)?;

        let active_pack = RwLock::new(ActivePackState {
            pack_id: active_pack_id,
            file: active_file,
            current_offset: 0,
            entries: Vec::new(),
        });

        // Migrate legacy vfs.json if it exists and vfs_db has no keys
        let legacy_vfs_file = metadata_dir.join("vfs.json");
        if legacy_vfs_file.exists() {
            let mut iter = vfs_db.raw_iterator();
            iter.seek_to_first();
            if !iter.valid() {
                if let Ok(file) = File::open(&legacy_vfs_file) {
                    #[derive(Deserialize)]
                    struct LegacyEntry {
                        user_id: String,
                        path: String,
                        file: VersionedFile,
                    }
                    if let Ok(entries) = serde_json::from_reader::<_, Vec<LegacyEntry>>(file) {
                        for entry in entries {
                            let key = vfs_key(&entry.user_id, &entry.path);
                            if let Ok(val) = serde_json::to_vec(&entry.file) {
                                let _ = vfs_db.put(&key, val);
                            }
                        }
                        info!("Migrated legacy vfs.json into RocksDB");
                    }
                }
            }
        }

        metrics::gauge!("uru_cas_chunk_index_size").set(chunk_index.len() as f64);
        metrics::gauge!("uru_cas_packfiles_active_total").set(pack_paths.len().max(1) as f64);

        Ok(Arc::new(Self {
            root_dir,
            vfs_db,
            pack_mgr,
            chunk_index,
            bloom,
            active_pack,
        }))
    }

    /// Fast Content-Addressable existence check:
    /// Global AtomicBloomFilter -> ChunkIndex (state == Live)
    pub fn has_chunk(&self, hash: &ChunkHash) -> bool {
        if !self.bloom.contains(hash) {
            return false;
        }

        if let Some(entry) = self.chunk_index.get(hash) {
            if entry.state == ChunkState::Live {
                metrics::counter!("uru_cas_chunks_processed_total", "result" => "cache_hit").increment(1);
                return true;
            }
        }

        metrics::counter!("uru_cas_chunks_processed_total", "result" => "bloom_false_positive").increment(1);
        false
    }

    /// Check missing chunks from a batch of hashes
    pub fn check_missing_chunks(&self, hashes: &[ChunkHash]) -> Vec<ChunkHash> {
        hashes
            .iter()
            .filter(|h| !self.has_chunk(h))
            .copied()
            .collect()
    }

    /// Write a single raw chunk.
    /// Prevents resurrection race conditions by safely writing to active packfile.
    pub fn write_chunk(&self, hash: &ChunkHash, data: &[u8]) -> Result<(), StorageError> {
        // Fast path: if already live, no-op
        if let Some(entry) = self.chunk_index.get(hash) {
            if entry.state == ChunkState::Live {
                return Ok(());
            }
        }

        let mut active = self.active_pack.write();

        // Check if writing exceeds MAX_PACKFILE_SIZE; if so, seal and rotate
        if active.current_offset > 0 && (active.current_offset + data.len() as u64 > MAX_PACKFILE_SIZE) {
            let old_id = active.pack_id;
            let old_path = self.pack_mgr.pack_path(old_id);
            if let Err(e) = PackManager::seal_packfile(&old_path, &active.entries) {
                warn!(pack_id = old_id, "Failed to seal packfile: {e}");
            }

            let new_id = old_id + 1;
            let new_path = self.pack_mgr.pack_path(new_id);
            let new_file = OpenOptions::new().create(true).append(true).open(&new_path)?;

            active.pack_id = new_id;
            active.file = new_file;
            active.current_offset = 0;
            active.entries.clear();

            metrics::gauge!("uru_cas_packfiles_active_total").increment(1.0);
        }

        let offset = active.current_offset;
        active.file.write_all(data)?;
        active.current_offset += data.len() as u64;
        active.entries.push(TrailerEntry {
            hash: *hash,
            offset,
            length: data.len() as u32,
        });

        // Insert / update chunk index
        let loc = ChunkLocation::new(active.pack_id, offset, data.len() as u32);
        self.chunk_index.insert(*hash, loc);
        self.bloom.insert(hash);

        drop(active);

        metrics::counter!("uru_cas_chunks_processed_total", "result" => "uploaded").increment(1);
        metrics::counter!("uru_cas_bytes_stored_disk_total").increment(data.len() as u64);
        metrics::gauge!("uru_cas_chunk_index_size").set(self.chunk_index.len() as f64);

        Ok(())
    }

    /// Write a batch of chunks
    pub fn write_batch(&self, chunks: &[BatchChunk]) -> Result<usize, StorageError> {
        let mut count = 0;
        for c in chunks {
            if !self.has_chunk(&c.hash) {
                self.write_chunk(&c.hash, &c.data)?;
                count += 1;
            }
        }
        Ok(count)
    }

    /// Read raw chunk bytes
    pub fn read_chunk(&self, hash: &ChunkHash) -> Result<Bytes, StorageError> {
        let loc = self
            .chunk_index
            .get(hash)
            .ok_or_else(|| StorageError::ChunkNotFound(hash.to_hex()))?;

        if loc.state != ChunkState::Live {
            return Err(StorageError::ChunkNotFound(hash.to_hex()));
        }

        let (pack_id, offset, length) = (loc.pack_id, loc.offset, loc.length);
        drop(loc);

        // If chunk is currently in the active packfile, flush before reading
        {
            let mut active = self.active_pack.write();
            if active.pack_id == pack_id {
                active.file.flush()?;
            }
        }

        let data = self.pack_mgr.read_chunk(pack_id, offset, length)?;
        Ok(Bytes::from(data))
    }

    /// Read an arbitrary byte-range across a file manifest
    pub fn read_file_range(
        &self,
        manifest: &FileManifest,
        start: u64,
        end_inclusive: u64,
    ) -> Result<Vec<u8>, StorageError> {
        let slices = manifest.find_chunks_for_range(start, end_inclusive);
        let total_bytes: usize = slices.iter().map(|s| s.slice_len).sum();
        let mut result = Vec::with_capacity(total_bytes);

        for slice in slices {
            let chunk_bytes = self.read_chunk(&slice.chunk.hash)?;
            let slice_end = slice.slice_offset + slice.slice_len;
            if slice_end > chunk_bytes.len() {
                return Err(StorageError::ChunkNotFound(format!(
                    "Chunk length mismatch for {}",
                    slice.chunk.hash
                )));
            }
            result.extend_from_slice(&chunk_bytes[slice.slice_offset..slice_end]);
        }

        Ok(result)
    }

    /// Commit a FileManifest to a user's isolated VFS path
    pub fn commit_file(
        &self,
        user_id: &str,
        path: &str,
        manifest: FileManifest,
        parent_version: Option<Uuid>,
    ) -> Result<FileVersion, StorageError> {
        let norm_path = normalize_path(path).map_err(StorageError::InvalidPath)?;

        // Validate that all chunks exist and are Live
        let missing = self.check_missing_chunks(
            &manifest.chunks.iter().map(|c| c.hash).collect::<Vec<_>>(),
        );
        if !missing.is_empty() {
            return Err(StorageError::MissingChunks(missing));
        }

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let key = vfs_key(user_id, &norm_path);
        let mut vf = match self.vfs_db.get(&key)? {
            Some(val) => serde_json::from_slice::<VersionedFile>(&val)?,
            None => VersionedFile::new(&norm_path),
        };

        let version = vf.add_version(manifest, now_ms, parent_version);
        let val = serde_json::to_vec(&vf)?;
        self.vfs_db.put(&key, val)?;

        Ok(version)
    }

    /// Get a file's version (HEAD if version_id is None)
    pub fn get_file(
        &self,
        user_id: &str,
        path: &str,
        version_id: Option<Uuid>,
    ) -> Result<FileVersion, StorageError> {
        let norm_path = normalize_path(path).map_err(StorageError::InvalidPath)?;
        let key = vfs_key(user_id, &norm_path);
        let val = self
            .vfs_db
            .get(&key)?
            .ok_or_else(|| StorageError::FileNotFound(norm_path.clone()))?;

        let vf: VersionedFile = serde_json::from_slice(&val)?;
        match version_id {
            Some(vid) => vf
                .find_version(vid)
                .cloned()
                .ok_or(StorageError::VersionNotFound(vid)),
            None => vf
                .head_live()
                .cloned()
                .ok_or_else(|| StorageError::FileNotFound(norm_path)),
        }
    }

    /// Soft delete (appends delete marker) or permanently remove specific version
    pub fn delete_file(
        &self,
        user_id: &str,
        path: &str,
        version_id: Option<Uuid>,
    ) -> Result<FileVersion, StorageError> {
        let norm_path = normalize_path(path).map_err(StorageError::InvalidPath)?;
        let key = vfs_key(user_id, &norm_path);
        let val = self
            .vfs_db
            .get(&key)?
            .ok_or_else(|| StorageError::FileNotFound(norm_path.clone()))?;

        let mut vf: VersionedFile = serde_json::from_slice(&val)?;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let result = match version_id {
            Some(vid) => vf
                .remove_version(vid)
                .ok_or(StorageError::VersionNotFound(vid))?,
            None => vf.add_delete_marker(now_ms),
        };

        if vf.versions.is_empty() {
            self.vfs_db.delete(&key)?;
        } else {
            self.vfs_db.put(&key, serde_json::to_vec(&vf)?)?;
        }

        Ok(result)
    }

    /// List live files for a user matching a prefix
    pub fn list_files(&self, user_id: &str, prefix: &str) -> Vec<(String, FileVersion)> {
        let user_prefix = format!("{}\0", user_id);
        let mut results = Vec::new();
        let mut iter = self.vfs_db.raw_iterator();
        iter.seek(user_prefix.as_bytes());

        while iter.valid() {
            if let (Some(k), Some(v)) = (iter.key(), iter.value()) {
                if !k.starts_with(user_prefix.as_bytes()) {
                    break;
                }
                if let Ok(key_str) = std::str::from_utf8(k) {
                    if let Some(path) = key_str.strip_prefix(&user_prefix) {
                        if path.starts_with(prefix) {
                            if let Ok(vf) = serde_json::from_slice::<VersionedFile>(v) {
                                if let Some(head) = vf.head_live() {
                                    results.push((path.to_string(), head.clone()));
                                }
                            }
                        }
                    }
                }
            }
            iter.next();
        }

        results.sort_by(|a, b| a.0.cmp(&b.0));
        results
    }

    /// List full version history for a file
    pub fn list_versions(
        &self,
        user_id: &str,
        path: &str,
    ) -> Result<Vec<FileVersion>, StorageError> {
        let norm_path = normalize_path(path).map_err(StorageError::InvalidPath)?;
        let key = vfs_key(user_id, &norm_path);
        let val = self
            .vfs_db
            .get(&key)?
            .ok_or_else(|| StorageError::FileNotFound(norm_path))?;
        let vf: VersionedFile = serde_json::from_slice(&val)?;
        Ok(vf.versions)
    }

    /// Restore an older version to HEAD
    pub fn restore_version(
        &self,
        user_id: &str,
        path: &str,
        version_id: Uuid,
    ) -> Result<FileVersion, StorageError> {
        let norm_path = normalize_path(path).map_err(StorageError::InvalidPath)?;
        let key = vfs_key(user_id, &norm_path);
        let val = self
            .vfs_db
            .get(&key)?
            .ok_or_else(|| StorageError::FileNotFound(norm_path.clone()))?;

        let mut vf: VersionedFile = serde_json::from_slice(&val)?;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let restored = vf
            .restore_version(version_id, now_ms)
            .ok_or(StorageError::VersionNotFound(version_id))?;

        self.vfs_db.put(&key, serde_json::to_vec(&vf)?)?;
        Ok(restored)
    }

    /// Run Garbage Collection and Compaction
    pub fn run_gc(&self, retention: &RetentionPolicy) -> Result<GcStats, StorageError> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let stats = GcEngine::run_gc(
            &self.vfs_db,
            &self.chunk_index,
            &self.bloom,
            &self.pack_mgr,
            &self.active_pack,
            retention,
            now_ms,
        )?;

        Ok(stats)
    }

    /// Export Bloom filter bytes for GET /api/v1/bloom
    pub fn export_bloom_filter(&self) -> Vec<u8> {
        self.bloom.export_bytes()
    }

    /// Seal the current active packfile with its trailer and start a new active pack
    pub fn seal_active_pack(&self) -> io::Result<()> {
        let mut active = self.active_pack.write();
        if !active.entries.is_empty() {
            let old_id = active.pack_id;
            let old_path = self.pack_mgr.pack_path(old_id);
            PackManager::seal_packfile(&old_path, &active.entries)?;

            let new_id = old_id + 1;
            let new_path = self.pack_mgr.pack_path(new_id);
            let new_file = OpenOptions::new().create(true).append(true).open(&new_path)?;

            active.pack_id = new_id;
            active.file = new_file;
            active.current_offset = 0;
            active.entries.clear();

            metrics::gauge!("uru_cas_packfiles_active_total").increment(1.0);
        }
        Ok(())
    }

    /// Get the root storage directory
    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    /// Total count of items in ChunkIndex
    pub fn chunk_count(&self) -> usize {
        self.chunk_index.len()
    }

    /// Total count of live chunks
    pub fn live_chunk_count(&self) -> usize {
        self.chunk_index
            .iter()
            .filter(|e| e.value().state == ChunkState::Live)
            .count()
    }
}

impl Drop for StorageEngine {
    fn drop(&mut self) {
        let active = self.active_pack.get_mut();
        if !active.entries.is_empty() {
            let path = self.pack_mgr.pack_path(active.pack_id);
            let _ = PackManager::seal_packfile(&path, &active.entries);
        }
        let _ = self.vfs_db.flush();
    }
}

pub use uru_core::normalize_path;
