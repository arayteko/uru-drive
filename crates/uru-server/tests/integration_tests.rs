use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use http_body_util::BodyExt;
use tempfile::tempdir;
use tower::ServiceExt;
use uru_core::batch::encode_batch;
use uru_core::chunker::chunk_and_hash_data;
use uru_server::{
    auth::{UserRole, UserStore},
    build_app,
    state::AppState,
};
use uru_storage::StorageEngine;

#[tokio::test]
async fn test_first_boot_admin_generation_and_auth() {
    let tmp = tempdir().unwrap();
    let auth_dir = tmp.path().join("auth");

    let saved_token;

    // First boot: open user store on empty directory
    {
        let storage = StorageEngine::open(tmp.path()).unwrap();
        let (users, admin_creds) = UserStore::open(&auth_dir).unwrap();
        assert!(admin_creds.is_some(), "Admin credentials must be generated on first boot");
        let creds = admin_creds.unwrap();
        assert_eq!(creds.username, "admin");
        assert!(!creds.password.is_empty());
        assert!(creds.token.starts_with("uru_"));
        saved_token = creds.token.clone();

        let state = AppState::new(storage, users.clone());
        let app = build_app(state);

        // Authenticate with generated admin token
        let req = Request::builder()
            .method("GET")
            .uri("/api/v1/users/me")
            .header(header::AUTHORIZATION, format!("Bearer {}", creds.token))
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let user_info: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(user_info["user_id"], "admin");
        assert_eq!(user_info["role"], "admin");
    } // First boot dropped here, releasing RocksDB lock

    // Second boot: re-open user store on existing directory
    let (users2, creds2) = UserStore::open(&auth_dir).unwrap();
    assert!(creds2.is_none(), "No new credentials should be generated on subsequent boots");
    assert!(users2.authenticate_api_key(&saved_token).is_some());
}

#[tokio::test]
async fn test_rbac_admin_vs_regular_user() {
    let tmp = tempdir().unwrap();
    let storage = StorageEngine::open(tmp.path()).unwrap();
    let (users, admin_creds) = UserStore::open(tmp.path().join("auth")).unwrap();
    let admin_token = admin_creds.unwrap().token;

    let state = AppState::new(storage, users.clone());
    let app = build_app(state);

    // Admin creates a standard user 'alice'
    let create_alice_req = Request::builder()
        .method("POST")
        .uri("/api/v1/users")
        .header(header::AUTHORIZATION, format!("Bearer {}", admin_token))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&serde_json::json!({
            "user_id": "alice",
            "password": "alicepassword123",
            "role": "user"
        })).unwrap()))
        .unwrap();

    let resp = app.clone().oneshot(create_alice_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Admin creates an API token for alice
    let alice_token_req = Request::builder()
        .method("POST")
        .uri("/api/v1/tokens")
        .header(header::AUTHORIZATION, format!("Bearer {}", admin_token))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&serde_json::json!({
            "user_id": "alice",
            "name": "Alice CLI Key"
        })).unwrap()))
        .unwrap();

    let resp = app.clone().oneshot(alice_token_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let token_body: serde_json::Value = serde_json::from_slice(
        &resp.into_body().collect().await.unwrap().to_bytes()
    ).unwrap();
    let alice_token = token_body["token"].as_str().unwrap().to_string();

    // 1. Regular user 'alice' CANNOT list all users (Admin only)
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/users")
        .header(header::AUTHORIZATION, format!("Bearer {}", alice_token))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // 2. Regular user 'alice' CANNOT create another user (Admin only)
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/users")
        .header(header::AUTHORIZATION, format!("Bearer {}", alice_token))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&serde_json::json!({
            "user_id": "eve",
            "password": "evepassword123",
            "role": "admin"
        })).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // 3. Regular user 'alice' CAN manage itself:
    // 3a. Read own profile
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/users/me")
        .header(header::AUTHORIZATION, format!("Bearer {}", alice_token))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 3b. Update own password
    let req = Request::builder()
        .method("PUT")
        .uri("/api/v1/users/me/password")
        .header(header::AUTHORIZATION, format!("Bearer {}", alice_token))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&serde_json::json!({
            "new_password": "new_alice_password_999"
        })).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Verify login with new password
    assert!(users.authenticate_password("alice", "new_alice_password_999").is_some());

    // 3c. Create new token for self
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/tokens")
        .header(header::AUTHORIZATION, format!("Bearer {}", alice_token))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&serde_json::json!({
            "name": "Alice Second Token"
        })).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body: serde_json::Value = serde_json::from_slice(
        &resp.into_body().collect().await.unwrap().to_bytes()
    ).unwrap();
    let second_token_id = body["token_id"].as_str().unwrap().to_string();

    // 3d. Revoke own token
    let req = Request::builder()
        .method("DELETE")
        .uri(format!("/api/v1/tokens/{}", second_token_id))
        .header(header::AUTHORIZATION, format!("Bearer {}", alice_token))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn test_deduplication_5mb_pattern_and_skip_ratio() {
    let tmp = tempdir().unwrap();
    let storage = StorageEngine::open(tmp.path()).unwrap();
    let (users, admin_creds) = UserStore::open(tmp.path().join("auth")).unwrap();
    let admin_token = admin_creds.unwrap().token;
    let state = AppState::new(storage, users);
    let app = build_app(state);

    // 1. Generate 5MB pattern file: repeats of a 64KB block pattern
    let pattern_block: Vec<u8> = (0..65536).map(|i| (i % 251) as u8).collect();
    let mut file_data = Vec::with_capacity(5 * 1024 * 1024);
    for _ in 0..80 {
        file_data.extend_from_slice(&pattern_block);
    }
    assert_eq!(file_data.len(), 80 * 65536);

    let (manifest, batch_chunks) =
        chunk_and_hash_data(&file_data, Some("application/octet-stream".into()), 1000);

    // Upload missing chunks via POST /api/v1/chunks/batch
    let batch_encoded = encode_batch(&batch_chunks);
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/chunks/batch")
        .header(header::AUTHORIZATION, format!("Bearer {}", admin_token))
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
        .header(header::AUTHORIZATION, format!("Bearer {}", admin_token))
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

    let (modified_manifest, _) =
        chunk_and_hash_data(&modified_data, Some("application/octet-stream".into()), 2000);
    let modified_hashes: Vec<String> = modified_manifest
        .chunks
        .iter()
        .map(|c| c.hash.to_hex())
        .collect();
    let total_chunks = modified_hashes.len();

    // Check missing chunks via POST /api/v1/chunks/check
    let check_payload = serde_json::json!({ "hashes": modified_hashes });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/chunks/check")
        .header(header::AUTHORIZATION, format!("Bearer {}", admin_token))
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
    let (users, admin_creds) = UserStore::open(tmp.path().join("auth")).unwrap();
    let admin_token = admin_creds.unwrap().token;
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
        .header(header::AUTHORIZATION, format!("Bearer {}", admin_token))
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
        .header(header::AUTHORIZATION, format!("Bearer {}", admin_token))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&commit_payload).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Partial Range request: Range: bytes=50000-150000
    let start = 50_000;
    let end = 150_000;
    let expected_slice = &file_data[start..=end];

    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/files/data/sample.dat")
        .header(header::AUTHORIZATION, format!("Bearer {}", admin_token))
        .header(header::RANGE, format!("bytes={}-{}", start, end))
        .body(Body::empty())
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);

    let downloaded_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(downloaded_bytes.len(), expected_slice.len());
    assert_eq!(downloaded_bytes.as_ref(), expected_slice);
}

#[tokio::test]
async fn test_tenant_isolation_via_api() {
    let tmp = tempdir().unwrap();
    let storage = StorageEngine::open(tmp.path()).unwrap();
    let (users, admin_creds) = UserStore::open(tmp.path().join("auth")).unwrap();
    let _admin_token = admin_creds.unwrap().token;

    // Create user alice and bob dynamically
    users.create_user("alice", "alicepass", UserRole::User).unwrap();
    users.create_user("bob", "bobpass", UserRole::User).unwrap();

    let (_, alice_token) = users.create_token("alice", "Alice Key").unwrap();
    let (_, bob_token) = users.create_token("bob", "Bob Key").unwrap();

    let state = AppState::new(storage, users);
    let app = build_app(state);

    let secret_data = b"secret payload for alice only";
    let (manifest, batch) = chunk_and_hash_data(secret_data, None, 1000);

    // Upload chunks as Alice
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/chunks/batch")
        .header(header::AUTHORIZATION, format!("Bearer {}", alice_token))
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
        .header(header::AUTHORIZATION, format!("Bearer {}", alice_token))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&commit_payload).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Bob tries to read Alice's /secret.txt -> MUST be 404 Not Found
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/files/secret.txt")
        .header(header::AUTHORIZATION, format!("Bearer {}", bob_token))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // Alice reads her /secret.txt -> 200 OK
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/files/secret.txt")
        .header(header::AUTHORIZATION, format!("Bearer {}", alice_token))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let downloaded = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(downloaded.as_ref(), secret_data);
}
