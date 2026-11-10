use tempfile::tempdir;
use tokio::process::Command;
use tokio::time::{sleep, Duration};
use uru_server::{
    auth::UserStore,
    build_app,
    state::AppState,
};
use uru_storage::StorageEngine;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cli_user_and_token_management_e2e() {
    let tmp = tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    let storage = StorageEngine::open(&data_dir).unwrap();
    let auth_dir = data_dir.join("auth_db");

    let (users, admin_creds) = UserStore::open(&auth_dir).unwrap();
    let creds = admin_creds.expect("First boot must generate admin credentials");
    let admin_token = creds.token;

    let state = AppState::new(storage, users);
    let app = build_app(state);

    // Pick an available port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    let server_url = format!("http://{}", local_addr);

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    sleep(Duration::from_millis(50)).await;

    // Path to uru-cli binary
    let cli_bin = env!("CARGO_BIN_EXE_uru-cli");

    // 1. Admin creates user 'carol'
    let output = Command::new(cli_bin)
        .args([
            "--server", &server_url,
            "--api-key", &admin_token,
            "user", "create", "carol", "carolpass123", "--role", "user",
        ])
        .output()
        .await
        .expect("Failed to execute uru-cli");

    assert!(output.status.success(), "Failed to create user carol: {}", String::from_utf8_lossy(&output.stderr));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("carol"));

    // 2. Admin lists users
    let output = Command::new(cli_bin)
        .args([
            "--server", &server_url,
            "--api-key", &admin_token,
            "user", "ls",
        ])
        .output()
        .await
        .expect("Failed to execute uru-cli");

    assert!(output.status.success(), "Failed to update password: stderr={}, stdout={}", String::from_utf8_lossy(&output.stderr), String::from_utf8_lossy(&output.stdout));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("admin"));
    assert!(stdout.contains("carol"));

    // 3. Admin creates token for carol
    let output = Command::new(cli_bin)
        .args([
            "--server", &server_url,
            "--api-key", &admin_token,
            "token", "create", "Carol Token", "--user-id", "carol",
        ])
        .output()
        .await
        .expect("Failed to execute uru-cli");

    assert!(output.status.success(), "Failed to update password: stderr={}, stdout={}", String::from_utf8_lossy(&output.stderr), String::from_utf8_lossy(&output.stdout));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Token created successfully"));

    // Extract Carol's API token from stdout
    let carol_token = stdout
        .lines()
        .find(|line| line.contains("API Token:"))
        .and_then(|line| line.split("API Token:").nth(1))
        .map(|t| t.trim().to_string())
        .expect("Must parse Carol's API token");
    assert!(carol_token.starts_with("uru_"));

    // 4. Carol manages herself: reads own profile via 'user get me'
    let output = Command::new(cli_bin)
        .args([
            "--server", &server_url,
            "--api-key", &carol_token,
            "user", "get", "me",
        ])
        .output()
        .await
        .expect("Failed to execute uru-cli");

    assert!(output.status.success(), "Failed to update password: stderr={}, stdout={}", String::from_utf8_lossy(&output.stderr), String::from_utf8_lossy(&output.stdout));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("carol"));
    assert!(stdout.contains("user"));

    // 5. Carol updates her own password
    let output = Command::new(cli_bin)
        .args([
            "--server", &server_url,
            "--api-key", &carol_token,
            "user", "passwd", "me", "new_carol_secret_777",
        ])
        .output()
        .await
        .expect("Failed to execute uru-cli");

    assert!(output.status.success(), "Failed to update password: stderr={}, stdout={}", String::from_utf8_lossy(&output.stderr), String::from_utf8_lossy(&output.stdout));

    // 6. Carol cannot list all users (Role restriction: user role cannot manage other users)
    let output = Command::new(cli_bin)
        .args([
            "--server", &server_url,
            "--api-key", &carol_token,
            "user", "ls",
        ])
        .output()
        .await
        .expect("Failed to execute uru-cli");

    assert!(!output.status.success(), "Regular user must NOT be able to list all users");

    // 7. Carol uploads a file
    let test_file = tmp.path().join("sample.txt");
    std::fs::write(&test_file, b"Hello from Carol using FastCDC deduplicated UruDrive!").unwrap();

    let output = Command::new(cli_bin)
        .args([
            "--server", &server_url,
            "--api-key", &carol_token,
            "upload",
            test_file.to_str().unwrap(),
            "/docs/hello.txt",
        ])
        .output()
        .await
        .expect("Failed to execute uru-cli");

    assert!(output.status.success(), "Upload failed: {}", String::from_utf8_lossy(&output.stderr));

    // 8. Carol lists files
    let output = Command::new(cli_bin)
        .args([
            "--server", &server_url,
            "--api-key", &carol_token,
            "ls", "/",
        ])
        .output()
        .await
        .expect("Failed to execute uru-cli");

    assert!(output.status.success(), "Failed to update password: stderr={}, stdout={}", String::from_utf8_lossy(&output.stderr), String::from_utf8_lossy(&output.stdout));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("/docs/hello.txt"));

    // 9. Carol downloads the file
    let download_target = tmp.path().join("downloaded.txt");
    let output = Command::new(cli_bin)
        .args([
            "--server", &server_url,
            "--api-key", &carol_token,
            "download",
            "/docs/hello.txt",
            download_target.to_str().unwrap(),
        ])
        .output()
        .await
        .expect("Failed to execute uru-cli");

    assert!(output.status.success(), "Failed to update password: stderr={}, stdout={}", String::from_utf8_lossy(&output.stderr), String::from_utf8_lossy(&output.stdout));
    let downloaded_content = std::fs::read(&download_target).unwrap();
    assert_eq!(downloaded_content, b"Hello from Carol using FastCDC deduplicated UruDrive!");

    // 10. Admin lists tokens
    let output = Command::new(cli_bin)
        .args([
            "--server", &server_url,
            "--api-key", &admin_token,
            "token", "ls",
        ])
        .output()
        .await
        .expect("Failed to execute uru-cli");

    assert!(output.status.success(), "Failed to update password: stderr={}, stdout={}", String::from_utf8_lossy(&output.stderr), String::from_utf8_lossy(&output.stdout));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Carol Token"));
}
