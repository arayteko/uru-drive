use crate::manifest::FileManifest;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileVersion {
    pub version_id: Uuid,
    pub parent_version_id: Option<Uuid>,
    pub created_at_ms: u64,
    pub is_delete_marker: bool,
    pub manifest: Option<FileManifest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VersionedFile {
    pub path: String,
    pub versions: Vec<FileVersion>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetentionPolicy {
    pub max_versions_per_file: Option<usize>,
    pub max_age_days: Option<u32>,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            max_versions_per_file: Some(30),
            max_age_days: Some(90),
        }
    }
}

/// Statistics when comparing two versions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VersionDiffStats {
    pub previous_version_id: Option<Uuid>,
    pub current_version_id: Uuid,
    pub total_chunks: usize,
    pub reused_chunks: usize,
    pub new_chunks: usize,
    pub raw_bytes_new: u64,
    pub raw_bytes_reused: u64,
}

impl VersionedFile {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            versions: Vec::new(),
        }
    }

    /// Latest version in the list (HEAD), which may be a delete marker
    pub fn head(&self) -> Option<&FileVersion> {
        self.versions.last()
    }

    /// Latest live version (if HEAD is not a delete marker)
    pub fn head_live(&self) -> Option<&FileVersion> {
        match self.versions.last() {
            Some(ver) if !ver.is_delete_marker => Some(ver),
            _ => None,
        }
    }

    /// Find a specific version by its version_id
    pub fn find_version(&self, id: Uuid) -> Option<&FileVersion> {
        self.versions.iter().find(|v| v.version_id == id)
    }

    /// Append a new live version as HEAD
    pub fn add_version(
        &mut self,
        manifest: FileManifest,
        now_ms: u64,
        parent_version_id: Option<Uuid>,
    ) -> FileVersion {
        let parent = parent_version_id.or_else(|| self.head().map(|v| v.version_id));
        let ver = FileVersion {
            version_id: Uuid::now_v7(),
            parent_version_id: parent,
            created_at_ms: now_ms,
            is_delete_marker: false,
            manifest: Some(manifest),
        };
        self.versions.push(ver.clone());
        ver
    }

    /// Append a soft-delete marker as HEAD
    pub fn add_delete_marker(&mut self, now_ms: u64) -> FileVersion {
        let parent = self.head().map(|v| v.version_id);
        let ver = FileVersion {
            version_id: Uuid::now_v7(),
            parent_version_id: parent,
            created_at_ms: now_ms,
            is_delete_marker: true,
            manifest: None,
        };
        self.versions.push(ver.clone());
        ver
    }

    /// Permanently remove a specific version by its UUID
    pub fn remove_version(&mut self, version_id: Uuid) -> Option<FileVersion> {
        if let Some(idx) = self.versions.iter().position(|v| v.version_id == version_id) {
            Some(self.versions.remove(idx))
        } else {
            None
        }
    }

    /// Restore an older version to become the new HEAD
    pub fn restore_version(&mut self, version_id: Uuid, now_ms: u64) -> Option<FileVersion> {
        let manifest = self.find_version(version_id)?.manifest.clone()?;
        Some(self.add_version(manifest, now_ms, Some(version_id)))
    }

    /// Prune older versions according to retention policy.
    /// HEAD is ALWAYS preserved (even if it's a delete marker).
    pub fn prune_retention(&mut self, policy: &RetentionPolicy, now_ms: u64) -> usize {
        if self.versions.len() <= 1 {
            return 0;
        }

        let orig_len = self.versions.len();
        let head = self.versions.pop().unwrap(); // Keep HEAD safe

        let max_age_ms = policy
            .max_age_days
            .map(|days| (days as u64) * 24 * 60 * 60 * 1000);

        // Filter non-HEAD versions
        self.versions.retain(|v| {
            if let Some(max_age) = max_age_ms {
                if now_ms.saturating_sub(v.created_at_ms) > max_age {
                    return false;
                }
            }
            true
        });

        // Enforce max_versions_per_file (counting HEAD as 1)
        if let Some(max_count) = policy.max_versions_per_file {
            let max_non_head = max_count.saturating_sub(1);
            if self.versions.len() > max_non_head {
                let excess = self.versions.len() - max_non_head;
                self.versions.drain(0..excess);
            }
        }

        self.versions.push(head);
        orig_len - self.versions.len()
    }

    /// Calculate chunk diff stats between a version and its parent
    pub fn diff_version(&self, version_id: Uuid) -> Option<VersionDiffStats> {
        let target_idx = self.versions.iter().position(|v| v.version_id == version_id)?;
        let target = &self.versions[target_idx];
        let manifest = target.manifest.as_ref()?;

        let parent_manifest = target
            .parent_version_id
            .and_then(|pid| self.find_version(pid))
            .and_then(|pv| pv.manifest.as_ref())
            .or_else(|| {
                if target_idx > 0 {
                    self.versions[target_idx - 1].manifest.as_ref()
                } else {
                    None
                }
            });

        let mut reused_chunks = 0;
        let mut new_chunks = 0;
        let mut raw_bytes_new = 0;
        let mut raw_bytes_reused = 0;

        match parent_manifest {
            Some(pm) => {
                use std::collections::HashSet;
                let parent_chunk_hashes: HashSet<_> = pm.chunks.iter().map(|c| c.hash).collect();
                for c in &manifest.chunks {
                    if parent_chunk_hashes.contains(&c.hash) {
                        reused_chunks += 1;
                        raw_bytes_reused += c.length as u64;
                    } else {
                        new_chunks += 1;
                        raw_bytes_new += c.length as u64;
                    }
                }
            }
            None => {
                new_chunks = manifest.chunks.len();
                raw_bytes_new = manifest.size;
            }
        }

        Some(VersionDiffStats {
            previous_version_id: target.parent_version_id,
            current_version_id: target.version_id,
            total_chunks: manifest.chunks.len(),
            reused_chunks,
            new_chunks,
            raw_bytes_new,
            raw_bytes_reused,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunker::chunk_and_hash_data;

    #[test]
    fn test_versioning_and_retention() {
        let mut vf = VersionedFile::new("/docs/report.pdf");

        let (m1, _) = chunk_and_hash_data(b"version 1 content", None, 1000);
        let v1_id = vf.add_version(m1, 1000, None).version_id;

        let (m2, _) = chunk_and_hash_data(b"version 2 content", None, 2000);
        let v2_id = vf.add_version(m2, 2000, None).version_id;

        assert_eq!(vf.head_live().unwrap().version_id, v2_id);
        assert_eq!(vf.versions.len(), 2);

        // Soft delete
        vf.add_delete_marker(3000);
        assert!(vf.head_live().is_none());
        assert!(vf.head().unwrap().is_delete_marker);
        assert_eq!(vf.versions.len(), 3);

        // Restore v1
        let v4 = vf.restore_version(v1_id, 4000).unwrap();
        assert_eq!(v4.parent_version_id, Some(v1_id));
        assert!(!v4.is_delete_marker);
        assert_eq!(vf.head_live().unwrap().version_id, v4.version_id);
        assert_eq!(vf.versions.len(), 4);

        // Retention policy with max 2 versions
        let policy = RetentionPolicy {
            max_versions_per_file: Some(2),
            max_age_days: None,
        };
        let pruned = vf.prune_retention(&policy, 5000);
        assert_eq!(pruned, 2);
        assert_eq!(vf.versions.len(), 2);
        // HEAD is preserved
        assert_eq!(vf.head().unwrap().version_id, v4.version_id);
    }
}
