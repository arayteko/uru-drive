use tempfile::tempdir;
use uru_core::chunker::chunk_and_hash_data;
use uru_core::hash::ChunkHash;
use uru_core::version::RetentionPolicy;
use uru_storage::StorageEngine;

#[test]
fn test_recovery_from_disk_trailers() {
    let tmp = tempdir().unwrap();
    let root = tmp.path().to_path_buf();

    let hash1 = ChunkHash::new_blake3(b"chunk data one");
    let hash2 = ChunkHash::new_blake3(b"chunk data two");
    let hash3 = ChunkHash::new_blake3(b"chunk data three");

    // Scope 1: Write chunks and commit files, then close
    {
        let engine = StorageEngine::open(&root).unwrap();
        engine.write_chunk(&hash1, b"chunk data one").unwrap();
        engine.write_chunk(&hash2, b"chunk data two").unwrap();
        engine.write_chunk(&hash3, b"chunk data three").unwrap();

        let (manifest, _) = chunk_and_hash_data(b"chunk data one", None, 1000);
        engine.commit_file("alice", "/file1.txt", manifest, None).unwrap();

        assert!(engine.has_chunk(&hash1));
        assert!(engine.has_chunk(&hash2));
        assert!(engine.has_chunk(&hash3));

        engine.seal_active_pack().unwrap();
    } // engine dropped here, in-memory index destroyed

    // Scope 2: Re-open engine and verify 100% recovery
    {
        let engine2 = StorageEngine::open(&root).unwrap();

        // Chunks must exist and be Live
        assert!(engine2.has_chunk(&hash1));
        assert!(engine2.has_chunk(&hash2));
        assert!(engine2.has_chunk(&hash3));

        // Read chunk data back
        let data1 = engine2.read_chunk(&hash1).unwrap();
        assert_eq!(data1, &b"chunk data one"[..]);

        let data2 = engine2.read_chunk(&hash2).unwrap();
        assert_eq!(data2, &b"chunk data two"[..]);

        // File manifest should still exist
        let file = engine2.get_file("alice", "/file1.txt", None).unwrap();
        assert_eq!(file.manifest.unwrap().size, 14);
    }
}

#[test]
fn test_deletion_gc_tombstone_and_resurrection_race() {
    let tmp = tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    let engine = StorageEngine::open(&root).unwrap();

    let data = b"payload to be deleted and resurrected";
    let hash = ChunkHash::new_blake3(data);
    engine.write_chunk(&hash, data).unwrap();

    let (manifest, _) = chunk_and_hash_data(data, None, 1000);
    engine.commit_file("bob", "/data.bin", manifest, None).unwrap();

    assert!(engine.has_chunk(&hash));

    // Phase 1: Soft delete the file
    engine.delete_file("bob", "/data.bin", None).unwrap();
    assert!(engine.get_file("bob", "/data.bin", None).is_err());

    // Phase 2: Run GC with policy that prunes delete markers / old versions
    // To ensure the version is pruned, set retention max_age_days: 0 and max_versions: 1
    let aggressive_policy = RetentionPolicy {
        max_versions_per_file: Some(1),
        max_age_days: Some(0),
    };

    let stats = engine.run_gc(&aggressive_policy).unwrap();
    assert_eq!(stats.chunks_tombstoned, 1);

    // Verify chunk is now missing from user's perspective
    assert!(!engine.has_chunk(&hash));

    // Resurrection Race: Upload the same chunk again after GC tombstoned it
    engine.write_chunk(&hash, data).unwrap();

    // Chunk should be back to Live and readable!
    assert!(engine.has_chunk(&hash));
    let read_back = engine.read_chunk(&hash).unwrap();
    assert_eq!(read_back, &data[..]);
}

#[test]
fn test_user_isolation() {
    let tmp = tempdir().unwrap();
    let engine = StorageEngine::open(tmp.path()).unwrap();

    let common_data = b"identical multi-tenant shared payload 12345";
    let (m1, _) = chunk_and_hash_data(common_data, None, 1000);
    let (m2, _) = chunk_and_hash_data(common_data, None, 1000);

    for chunk in &m1.chunks {
        engine.write_chunk(&chunk.hash, common_data).unwrap();
    }

    // User A commits /secrets/user_a.txt
    engine.commit_file("user_a", "/secrets/data.txt", m1, None).unwrap();

    // User B commits /public/shared.txt
    engine.commit_file("user_b", "/public/shared.txt", m2, None).unwrap();

    // User A cannot read User B's file
    assert!(engine.get_file("user_a", "/public/shared.txt", None).is_err());

    // User B cannot read User A's file
    assert!(engine.get_file("user_b", "/secrets/data.txt", None).is_err());

    // User A can read User A's file
    let file_a = engine.get_file("user_a", "/secrets/data.txt", None).unwrap();
    assert_eq!(file_a.manifest.unwrap().size, common_data.len() as u64);

    // Listing isolated files
    let list_a = engine.list_files("user_a", "/");
    assert_eq!(list_a.len(), 1);
    assert_eq!(list_a[0].0, "/secrets/data.txt");

    let list_b = engine.list_files("user_b", "/");
    assert_eq!(list_b.len(), 1);
    assert_eq!(list_b[0].0, "/public/shared.txt");
}
