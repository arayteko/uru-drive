use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use http_body_util::BodyExt;
use std::sync::Arc;
use tempfile::tempdir;
use tower::ServiceExt;
use uru_core::batch::encode_batch;
use uru_core::chunker::chunk_and_hash_data;
use uru_server::{
    auth::UserStore,
    build_app,
    state::AppState,
};
use uru_storage::StorageEngine;

#[tokio::test]
async fn test_deduplication_5mb_pattern_and_skip_ratio() {
    let tmp = tempdir().unwrap();
    let storage = StorageEngine::open(tmp.path()).unwrap();
    let users = Arc::new(UserStore::new());
    let state = AppState::new(storage, users);
    let app = build_app(state);

    // 1. Generate 5MB pattern file: repeats of a 64KB block pattern
    let pattern_block: Vec<u8> = (0..65536).map(|i| (i % 251) as u8).collect();
    let mut file_data = Vec::with_capacity(5 * 1024 * 1024);
    for _ in 0..80 {
        file_data.extend_from_slice(&pattern_block);
    }
    assert_eq!(file_data.len(), 80 * 65536);

    let (manifest, batch_chunks) = chunk_and_hash_data(&file_data, Some("application/octet-stream".into()), 1000);

    // Upload missing chunks via POST /api/v1/chunks/batch
    let batch_encoded = encode_batch(&batch_chunks);
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/chunks/batch")
        .header(header::AUTHORIZATION, "Bearer uru_demo_key_1234567890abcdef")
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(batch_encoded))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Commit file
    let commit_payload = serde_json::json!({
        "path": "/large_file.bin",
        "manifest": manifest,
        "parent_version_id": null
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/files/commit")
        .header(header::AUTHORIZATION, "Bearer uru_demo_key_1234567890abcdef")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&commit_payload).unwrap()))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 2. Upload a modified version: mutate only the middle 200KB of the 5MB file
    let mut modified_data = file_data.clone();
    for i in 2_500_000..2_700_000 {
        modified_data[i] ^= 0xFF;
    }

    let (modified_manifest, _) = chunk_and_hash_data(&modified_data, Some("application/octet-stream".into()), 2000);
    let modified_hashes: Vec<String> = modified_manifest.chunks.iter().map(|c| c.hash.to_hex()).collect();
    let total_chunks = modified_hashes.len();

    // Check missing chunks via POST /api/v1/chunks/check
    let check_payload = serde_json::json!({ "hashes": modified_hashes });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/chunks/check")
        .header(header::AUTHORIZATION, "Bearer uru_demo_key_1234567890abcdef")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&check_payload).unwrap()))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let check_res: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let missing = check_res["missing"].as_array().unwrap();

    let skipped_chunks = total_chunks - missing.len();
    let skip_ratio = (skipped_chunks as f64) / (total_chunks as f64);

    println!(
        "Total chunks: {}, Missing (new): {}, Skipped (deduplicated): {}, Ratio: {:.2}%",
        total_chunks,
        missing.len(),
        skipped_chunks,
        skip_ratio * 100.0
    );

    // Assert that >80% of chunks are skipped
    assert!(
        skip_ratio > 0.80,
        "Expected >80% chunks skipped, but got {:.2}%",
        skip_ratio * 100.0
    );
}

#[tokio::test]
async fn test_range_download_multi_chunk_accuracy() {
    let tmp = tempdir().unwrap();
    let storage = StorageEngine::open(tmp.path()).unwrap();
    let users = Arc::new(UserStore::new());
    let state = AppState::new(storage, users);
    let app = build_app(state);

    // Create 300KB file with pseudo-random content
    let mut file_data = Vec::with_capacity(300_000);
    for i in 0..300_000 {
        file_data.push(((i * 37) ^ (i >> 5)) as u8);
    }

    let (manifest, batch_chunks) = chunk_and_hash_data(&file_data, None, 1000);

    // Upload batch
    let batch_encoded = encode_batch(&batch_chunks);
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/chunks/batch")
        .header(header::AUTHORIZATION, "Bearer uru_demo_key_1234567890abcdef")
        .body(Body::from(batch_encoded))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Commit file
    let commit_payload = serde_json::json!({
        "path": "/data/sample.dat",
        "manifest": manifest,
        "parent_version_id": null
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/files/commit")
        .header(header::AUTHORIZATION, "Bearer uru_demo_key_1234567890abcdef")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&commit_payload).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Perform partial Range request: Range: bytes=50000-150000
    let start = 50_000;
    let end = 150_000;
    let expected_slice = &file_data[start..=end];

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/files/data/sample.dat")
        .header(header::AUTHORIZATION, "Bearer uru_demo_key_1234567890abcdef")
        .header(header::RANGE, format!("bytes={}-{}", start, end))
        .body(Body::empty())
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.headers().get(header::CONTENT_RANGE).unwrap(),
        &format!("bytes {}-{}/300000", start, end)
    );

    let downloaded_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(downloaded_bytes.len(), expected_slice.len());
    assert_eq!(downloaded_bytes.as_ref(), expected_slice);
}

#[tokio::test]
async fn test_tenant_isolation_via_api() {
    let tmp = tempdir().unwrap();
    let storage = StorageEngine::open(tmp.path()).unwrap();
    let users = Arc::new(UserStore::new());
    let state = AppState::new(storage, users);
    let app = build_app(state);

    let secret_data = b"secret payload for alice only";
    let (manifest, batch) = chunk_and_hash_data(secret_data, None, 1000);

    // Upload chunks as Alice
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/chunks/batch")
        .header(header::AUTHORIZATION, "Bearer uru_alice_api_key_1234567890")
        .body(Body::from(encode_batch(&batch)))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Commit file as Alice: /secret.txt
    let commit_payload = serde_json::json!({
        "path": "/secret.txt",
        "manifest": manifest,
        "parent_version_id": null
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/files/commit")
        .header(header::AUTHORIZATION, "Bearer uru_alice_api_key_1234567890")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&commit_payload).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Bob tries to read Alice's /secret.txt -> MUST be 404 Not Found
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/files/secret.txt")
        .header(header::AUTHORIZATION, "Bearer uru_bob_api_key_1234567890")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // Alice reads her /secret.txt -> 200 OK
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/files/secret.txt")
        .header(header::AUTHORIZATION, "Bearer uru_alice_api_key_1234567890")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let downloaded = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(downloaded.as_ref(), secret_data);
}
