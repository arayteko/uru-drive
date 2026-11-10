use crate::bloom::GlobalBloomFilter;
use crate::index::{ChunkIndex, ChunkLocation, ChunkState};
use crate::pack::{PackManager, TrailerEntry};
use dashmap::DashMap;
use parking_lot::RwLock;
pub use uru_core::GcStats;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, Write};
use std::time::Instant;
use tracing::info;
use uru_core::hash::ChunkHash;
use uru_core::version::{RetentionPolicy, VersionedFile};

pub struct ActivePackState {
    pub pack_id: u32,
    pub file: File,
    pub current_offset: u64,
    pub entries: Vec<TrailerEntry>,
}

pub struct GcEngine;

impl GcEngine {
    pub fn run_gc(
        vfs: &DashMap<(String, String), VersionedFile>,
        chunk_index: &ChunkIndex,
        bloom: &GlobalBloomFilter,
        pack_mgr: &PackManager,
        active_pack: &RwLock<ActivePackState>,
        retention: &RetentionPolicy,
        now_ms: u64,
    ) -> io::Result<GcStats> {
        let start_time = Instant::now();
        metrics::counter!("uru_cas_gc_runs_total").increment(1);

        // 1. Retention Policy Pruning
        let mut versions_pruned = 0;
        for mut entry in vfs.iter_mut() {
            let pruned = entry.value_mut().prune_retention(retention, now_ms);
            versions_pruned += pruned;
        }

        // 2. Mark Phase: collect all referenced chunk hashes from all live FileVersions
        let mut live_hashes = HashSet::new();
        for entry in vfs.iter() {
            for ver in &entry.value().versions {
                if let Some(manifest) = &ver.manifest {
                    for chunk in &manifest.chunks {
                        live_hashes.insert(chunk.hash);
                    }
                }
            }
        }

        // 3. Sweep & Tombstoning + Hole Punching
        let mut chunks_tombstoned = 0;
        let mut bytes_punched = 0u64;

        // Active packfile ID should not be hole-punched or compacted while being written
        let active_pack_id = active_pack.read().pack_id;

        for mut entry in chunk_index.iter_mut() {
            let (hash, loc) = entry.pair_mut();
            if loc.state == ChunkState::Live && !live_hashes.contains(hash) {
                loc.state = ChunkState::Tombstoned;
                chunks_tombstoned += 1;

                // Immediate hole punching if not the active writing packfile
                if loc.pack_id != active_pack_id && !loc.hole_punched {
                    match pack_mgr.punch_hole(loc.pack_id, loc.offset, loc.length) {
                        Ok(punched) => {
                            bytes_punched += punched;
                            loc.hole_punched = true;
                        }
                        Err(e) => {
                            tracing::warn!(pack_id = loc.pack_id, "Hole punching failed: {e}");
                        }
                    }
                }
            }
        }

        if bytes_punched > 0 {
            metrics::counter!("uru_cas_gc_bytes_freed_total").increment(bytes_punched);
        }

        // 4. Compaction: Identify packfiles where dead-byte ratio > 30%
        let mut pack_stats: HashMap<u32, (u64, u64)> = HashMap::new(); // pack_id -> (live_bytes, dead_bytes)

        for entry in chunk_index.iter() {
            let loc = entry.value();
            let (live, dead) = pack_stats.entry(loc.pack_id).or_insert((0, 0));
            match loc.state {
                ChunkState::Live => *live += loc.length as u64,
                ChunkState::Tombstoned => *dead += loc.length as u64,
            }
        }

        let mut packfiles_compacted = 0;
        let mut bytes_compacted = 0u64;
        let mut compacted_any = false;

        for (&pack_id, &(live_bytes, dead_bytes)) in &pack_stats {
            if pack_id == active_pack_id {
                continue; // Do not compact active packfile
            }

            let total_bytes = live_bytes + dead_bytes;
            if total_bytes == 0 {
                continue;
            }

            let dead_ratio = (dead_bytes as f64) / (total_bytes as f64);
            metrics::gauge!("uru_cas_packfile_dead_bytes_ratio", "pack_id" => pack_id.to_string())
                .set(dead_ratio);

            if dead_ratio > 0.30 {
                // Collect all live chunks in this packfile
                let live_chunks_in_pack: Vec<(ChunkHash, ChunkLocation)> = chunk_index
                    .iter()
                    .filter(|e| e.value().pack_id == pack_id && e.value().state == ChunkState::Live)
                    .map(|e| (*e.key(), *e.value()))
                    .collect();

                let mut active = active_pack.write();

                for (chunk_hash, old_loc) in live_chunks_in_pack {
                    let chunk_data = match pack_mgr.read_chunk(pack_id, old_loc.offset, old_loc.length) {
                        Ok(d) => d,
                        Err(e) => {
                            tracing::error!(pack_id, offset = old_loc.offset, "Failed to read chunk during compaction: {e}");
                            continue;
                        }
                    };

                    let new_offset = active.current_offset;
                    if let Err(e) = active.file.write_all(&chunk_data) {
                        tracing::error!("Failed to write chunk to active pack during compaction: {e}");
                        continue;
                    }
                    active.current_offset += chunk_data.len() as u64;
                    active.entries.push(TrailerEntry {
                        hash: chunk_hash,
                        offset: new_offset,
                        length: chunk_data.len() as u32,
                    });

                    // Update index atomically to new location
                    if let Some(mut loc) = chunk_index.get_mut(&chunk_hash) {
                        loc.pack_id = active.pack_id;
                        loc.offset = new_offset;
                        loc.length = chunk_data.len() as u32;
                        loc.state = ChunkState::Live;
                        loc.hole_punched = false;
                    }

                    bytes_compacted += old_loc.length as u64;
                }

                drop(active);

                // Unlink old .pack file
                let old_path = pack_mgr.pack_path(pack_id);
                if let Err(e) = std::fs::remove_file(&old_path) {
                    tracing::warn!(path = %old_path.display(), "Failed to remove compacted packfile: {e}");
                }

                packfiles_compacted += 1;
                compacted_any = true;
            }
        }

        // 5. If any packfile was compacted or tombstoned, rebuild Bloom filter with strictly live chunks
        let live_chunks_count = chunk_index
            .iter()
            .filter(|e| e.value().state == ChunkState::Live)
            .count();

        if compacted_any || chunks_tombstoned > 0 {
            let live_hashes_iter = chunk_index
                .iter()
                .filter(|e| e.value().state == ChunkState::Live)
                .map(|e| *e.key());
            bloom.rebuild(live_hashes_iter, live_chunks_count);
        }

        let duration = start_time.elapsed();
        metrics::histogram!("uru_cas_gc_duration_seconds").record(duration.as_secs_f64());

        let stats = GcStats {
            versions_pruned,
            chunks_tombstoned,
            bytes_punched,
            packfiles_compacted,
            bytes_compacted,
            live_chunks_count,
            duration_ms: duration.as_millis() as u64,
        };

        info!(?stats, "GC execution completed successfully");
        Ok(stats)
    }
}
