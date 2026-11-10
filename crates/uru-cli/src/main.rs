use clap::{Parser, Subcommand};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;
use uru_core::batch::{encode_batch, BatchChunk};
use uru_core::chunker::chunk_and_hash_data;
use uru_core::normalize_path;
use uru_core::version::FileVersion;
use uru_core::GcStats;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(
    name = "uru-cli",
    author = "UruDrive Team",
    version,
    about = "High-performance Content-Addressable Storage (CAS) client"
)]
struct Cli {
    #[arg(
        long,
        global = true,
        default_value = "http://127.0.0.1:8080",
        env = "URU_SERVER",
        help = "UruDrive server URL"
    )]
    server: String,

    #[arg(
        long,
        global = true,
        env = "URU_API_KEY",
        help = "UruDrive API token (or set URU_API_KEY env var)"
    )]
    api_key: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Upload a file with client-side FastCDC deduplication
    Upload {
        local_path: PathBuf,
        remote_path: String,
    },
    /// Download a file
    Download {
        remote_path: String,
        local_path: PathBuf,
        #[arg(long, help = "Optional historical version ID to download")]
        version: Option<Uuid>,
    },
    /// List remote files
    Ls {
        #[arg(default_value = "/", help = "Prefix path to list")]
        prefix: String,
    },
    /// Delete a remote file or a specific version
    Delete {
        remote_path: String,
        #[arg(long, help = "Permanently delete a specific version")]
        version: Option<Uuid>,
    },
    /// Trigger background garbage collection and compaction
    Gc,
    /// View file version history and chunk diffs
    History {
        remote_path: String,
    },
    /// Restore an older version to become HEAD
    Restore {
        remote_path: String,
        version_id: Uuid,
    },
    /// Manage users and roles (admin or self)
    #[command(subcommand)]
    User(UserCommands),
    /// Manage API tokens (admin or self)
    #[command(subcommand)]
    Token(TokenCommands),
}

#[derive(Subcommand, Debug)]
enum UserCommands {
    /// Create a new user (admin only)
    Create {
        user_id: String,
        password: String,
        #[arg(long, default_value = "user", help = "Role: 'admin' or 'user'")]
        role: String,
    },
    /// List all users (admin only)
    Ls,
    /// Get details of a user ('me' for caller, or user_id)
    Get {
        #[arg(default_value = "me", help = "User ID to query (or 'me')")]
        user_id: String,
    },
    /// Update user password (self or admin)
    Passwd {
        #[arg(help = "User ID ('me' or specific user_id)")]
        user_id: String,
        #[arg(help = "New password")]
        new_password: String,
    },
    /// Update user role (admin only)
    SetRole {
        user_id: String,
        #[arg(help = "New role: 'admin' or 'user'")]
        role: String,
    },
    /// Delete user (admin only)
    Delete {
        user_id: String,
    },
}

#[derive(Subcommand, Debug)]
enum TokenCommands {
    /// Create a new API token (self or admin)
    Create {
        name: String,
        #[arg(long, help = "Target user ID (admin only, defaults to caller)")]
        user_id: Option<String>,
    },
    /// List API tokens
    Ls {
        #[arg(long, help = "Filter by user ID (admin only, or 'all')")]
        user_id: Option<String>,
    },
    /// Revoke an API token by ID
    Revoke {
        token_id: Uuid,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let client = reqwest::Client::new();
    let base_url = cli.server.trim_end_matches('/');

    let api_key = match cli.api_key {
        Some(k) => k,
        None => {
            eprintln!("Error: Authentication token required.");
            eprintln!("Please specify --api-key <token> or set URU_API_KEY environment variable.");
            std::process::exit(1);
        }
    };

    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", api_key))?,
    );

    match cli.command {
        Commands::Upload {
            local_path,
            remote_path,
        } => {
            println!("Reading local file: {}", local_path.display());
            let mut file = File::open(&local_path)?;
            let mut data = Vec::new();
            file.read_to_end(&mut data)?;

            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis() as u64;

            println!("Running client-side FastCDC chunking and BLAKE3 hashing...");
            let (manifest, batch_chunks) = chunk_and_hash_data(&data, None, now_ms);
            let total_chunks = manifest.chunks.len();
            let total_bytes = data.len();

            println!(
                "File size: {} bytes across {} chunks",
                total_bytes, total_chunks
            );

            // Step 1: Check missing chunks via /api/v1/chunks/check
            let hashes: Vec<String> = manifest.chunks.iter().map(|c| c.hash.to_hex()).collect();
            let check_url = format!("{}/api/v1/chunks/check", base_url);
            let check_res = client
                .post(&check_url)
                .headers(headers.clone())
                .json(&serde_json::json!({ "hashes": hashes }))
                .send()
                .await?;

            if !check_res.status().is_success() {
                eprintln!("Check failed: {}", check_res.text().await?);
                std::process::exit(1);
            }

            let check_body: serde_json::Value = check_res.json().await?;
            let missing_hex = check_body["missing"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            use std::collections::HashSet;
            let missing_set: HashSet<String> = missing_hex
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();

            let missing_count = missing_set.len();
            let skipped_count = total_chunks - missing_count;
            let dedup_ratio = if total_chunks > 0 {
                (skipped_count as f64 / total_chunks as f64) * 100.0
            } else {
                0.0
            };

            println!(
                "Deduplication: {} skipped ({:.1}%), {} missing chunks to transfer",
                skipped_count, dedup_ratio, missing_count
            );

            // Step 2: Upload missing chunks via POST /api/v1/chunks/batch
            if missing_count > 0 {
                let missing_batches: Vec<BatchChunk> = batch_chunks
                    .into_iter()
                    .filter(|c| missing_set.contains(&c.hash.to_hex()))
                    .collect();

                let missing_bytes: usize = missing_batches.iter().map(|c| c.data.len()).sum();
                println!(
                    "Uploading {} missing chunks ({} bytes raw payload)...",
                    missing_batches.len(),
                    missing_bytes
                );

                let batch_bytes = encode_batch(&missing_batches);
                let upload_url = format!("{}/api/v1/chunks/batch", base_url);
                let upload_res = client
                    .post(&upload_url)
                    .headers(headers.clone())
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .body(batch_bytes)
                    .send()
                    .await?;

                if !upload_res.status().is_success() {
                    eprintln!("Batch upload failed: {}", upload_res.text().await?);
                    std::process::exit(1);
                }
                println!("Chunks uploaded successfully.");
            } else {
                println!("All chunks already present on server (100% deduplication).");
            }

            // Step 3: Commit FileManifest via POST /api/v1/files/commit
            println!("Committing file manifest to '{}'...", remote_path);
            let commit_url = format!("{}/api/v1/files/commit", base_url);
            let commit_payload = serde_json::json!({
                "path": remote_path,
                "manifest": manifest,
                "parent_version_id": null
            });

            let commit_res = client
                .post(&commit_url)
                .headers(headers.clone())
                .json(&commit_payload)
                .send()
                .await?;

            if !commit_res.status().is_success() {
                eprintln!("Commit failed: {}", commit_res.text().await?);
                std::process::exit(1);
            }

            let commit_body: serde_json::Value = commit_res.json().await?;
            println!(
                "Successfully committed! Version ID: {}",
                commit_body["version_id"].as_str().unwrap_or("unknown")
            );
        }

        Commands::Download {
            remote_path,
            local_path,
            version,
        } => {
            println!("Downloading '{}'...", remote_path);
            let mut url = format!("{}/api/v1/files{}", base_url, normalize_path(&remote_path)?);
            if let Some(v) = version {
                url.push_str(&format!("?version_id={}", v));
            }

            let res = client.get(&url).headers(headers.clone()).send().await?;
            if !res.status().is_success() {
                eprintln!("Download failed: {}", res.text().await?);
                std::process::exit(1);
            }

            let bytes = res.bytes().await?;
            let mut file = File::create(&local_path)?;
            file.write_all(&bytes)?;
            println!("Saved {} bytes to {}", bytes.len(), local_path.display());
        }

        Commands::Ls { prefix } => {
            let url = format!("{}/api/v1/files?prefix={}", base_url, prefix);
            let res = client.get(&url).headers(headers.clone()).send().await?;

            if !res.status().is_success() {
                eprintln!("List failed: {}", res.text().await?);
                std::process::exit(1);
            }

            let files: Vec<(String, FileVersion)> = res.json().await?;
            println!("{:<50} {:<12} {:<38}", "PATH", "SIZE", "HEAD VERSION ID");
            println!("{:-<102}", "");
            for (p, v) in files {
                let size = v.manifest.as_ref().map_or(0, |m| m.size);
                println!("{:<50} {:<12} {:<38}", p, size, v.version_id);
            }
        }

        Commands::Delete {
            remote_path,
            version,
        } => {
            let mut url = format!("{}/api/v1/files{}", base_url, normalize_path(&remote_path)?);
            if let Some(v) = version {
                url.push_str(&format!("?version_id={}", v));
            }

            let res = client.delete(&url).headers(headers.clone()).send().await?;
            if !res.status().is_success() {
                eprintln!("Delete failed: {}", res.text().await?);
                std::process::exit(1);
            }

            println!("File '{}' successfully deleted.", remote_path);
        }

        Commands::Gc => {
            println!("Triggering background GC and compaction...");
            let url = format!("{}/api/v1/gc", base_url);
            let res = client.post(&url).headers(headers.clone()).send().await?;

            if !res.status().is_success() {
                eprintln!("GC trigger failed: {}", res.text().await?);
                std::process::exit(1);
            }

            let stats: GcStats = res.json().await?;
            println!("Garbage Collection & Compaction Completed:");
            println!("  Versions Pruned:      {}", stats.versions_pruned);
            println!("  Chunks Tombstoned:    {}", stats.chunks_tombstoned);
            println!("  Hole-Punched Bytes:   {} bytes", stats.bytes_punched);
            println!("  Packfiles Compacted:  {}", stats.packfiles_compacted);
            println!("  Compacted Bytes:      {} bytes", stats.bytes_compacted);
            println!("  Live Chunks Count:    {}", stats.live_chunks_count);
            println!("  Duration:             {} ms", stats.duration_ms);
        }

        Commands::History { remote_path } => {
            let url = format!("{}/api/v1/files-history{}", base_url, normalize_path(&remote_path)?);
            let res = client.get(&url).headers(headers.clone()).send().await?;

            if !res.status().is_success() {
                eprintln!("History query failed: {}", res.text().await?);
                std::process::exit(1);
            }

            let versions: Vec<FileVersion> = res.json().await?;
            println!("{:<38} {:<10} {:<8} {:<24}", "VERSION ID", "SIZE", "DELETED", "PARENT ID");
            println!("{:-<85}", "");
            for v in versions {
                let size = v.manifest.as_ref().map_or(0, |m| m.size);
                let parent = v.parent_version_id.map_or("none".into(), |p| p.to_string());
                println!("{:<38} {:<10} {:<8} {:<24}", v.version_id, size, v.is_delete_marker, parent);
            }
        }

        Commands::Restore {
            remote_path,
            version_id,
        } => {
            let url = format!("{}/api/v1/files-restore{}", base_url, normalize_path(&remote_path)?);
            let res = client
                .post(&url)
                .headers(headers.clone())
                .json(&serde_json::json!({ "version_id": version_id }))
                .send()
                .await?;

            if !res.status().is_success() {
                eprintln!("Restore failed: {}", res.text().await?);
                std::process::exit(1);
            }

            let restored: FileVersion = res.json().await?;
            println!("Restored successfully! New HEAD version: {}", restored.version_id);
        }

        Commands::User(user_cmd) => match user_cmd {
            UserCommands::Create {
                user_id,
                password,
                role,
            } => {
                let url = format!("{}/api/v1/users", base_url);
                let res = client
                    .post(&url)
                    .headers(headers.clone())
                    .json(&serde_json::json!({
                        "user_id": user_id,
                        "password": password,
                        "role": role
                    }))
                    .send()
                    .await?;

                if !res.status().is_success() {
                    eprintln!("Failed to create user: {}", res.text().await?);
                    std::process::exit(1);
                }

                let body: serde_json::Value = res.json().await?;
                println!("User created successfully:");
                println!("  Username: {}", body["user_id"]);
                println!("  Role:     {}", body["role"]);
            }
            UserCommands::Ls => {
                let url = format!("{}/api/v1/users", base_url);
                let res = client.get(&url).headers(headers.clone()).send().await?;

                if !res.status().is_success() {
                    eprintln!("Failed to list users: {}", res.text().await?);
                    std::process::exit(1);
                }

                let users: serde_json::Value = res.json().await?;
                println!("{:<24} {:<12} {:<24}", "USER ID", "ROLE", "CREATED AT (MS)");
                println!("{:-<62}", "");
                if let Some(arr) = users.as_array() {
                    for u in arr {
                        println!(
                            "{:<24} {:<12} {:<24}",
                            u["user_id"].as_str().unwrap_or(""),
                            u["role"].as_str().unwrap_or(""),
                            u["created_at_ms"].as_u64().unwrap_or(0)
                        );
                    }
                }
            }
            UserCommands::Get { user_id } => {
                let url = format!("{}/api/v1/users/{}", base_url, user_id);
                let res = client.get(&url).headers(headers.clone()).send().await?;

                if !res.status().is_success() {
                    eprintln!("Failed to get user: {}", res.text().await?);
                    std::process::exit(1);
                }

                let u: serde_json::Value = res.json().await?;
                println!("User Details:");
                println!("  User ID:    {}", u["user_id"].as_str().unwrap_or(""));
                println!("  Role:       {}", u["role"].as_str().unwrap_or(""));
                println!("  Created Ms: {}", u["created_at_ms"].as_u64().unwrap_or(0));
            }
            UserCommands::Passwd {
                user_id,
                new_password,
            } => {
                let url = format!("{}/api/v1/users/{}/password", base_url, user_id);
                let res = client
                    .put(&url)
                    .headers(headers.clone())
                    .json(&serde_json::json!({ "new_password": new_password }))
                    .send()
                    .await?;

                if !res.status().is_success() {
                    eprintln!("Failed to change password: {}", res.text().await?);
                    std::process::exit(1);
                }

                println!("Password for user '{}' updated successfully.", user_id);
            }
            UserCommands::SetRole { user_id, role } => {
                let url = format!("{}/api/v1/users/{}/role", base_url, user_id);
                let res = client
                    .put(&url)
                    .headers(headers.clone())
                    .json(&serde_json::json!({ "role": role }))
                    .send()
                    .await?;

                if !res.status().is_success() {
                    eprintln!("Failed to change role: {}", res.text().await?);
                    std::process::exit(1);
                }

                println!("Role for user '{}' updated to '{}'.", user_id, role);
            }
            UserCommands::Delete { user_id } => {
                let url = format!("{}/api/v1/users/{}", base_url, user_id);
                let res = client.delete(&url).headers(headers.clone()).send().await?;

                if !res.status().is_success() {
                    eprintln!("Failed to delete user: {}", res.text().await?);
                    std::process::exit(1);
                }

                println!("User '{}' and all associated tokens deleted.", user_id);
            }
        },

        Commands::Token(token_cmd) => match token_cmd {
            TokenCommands::Create { name, user_id } => {
                let url = format!("{}/api/v1/tokens", base_url);
                let mut payload = serde_json::json!({ "name": name });
                if let Some(uid) = user_id {
                    payload["user_id"] = serde_json::Value::String(uid);
                }

                let res = client
                    .post(&url)
                    .headers(headers.clone())
                    .json(&payload)
                    .send()
                    .await?;

                if !res.status().is_success() {
                    eprintln!("Failed to create token: {}", res.text().await?);
                    std::process::exit(1);
                }

                let body: serde_json::Value = res.json().await?;
                println!("Token created successfully!");
                println!("  Token ID:  {}", body["token_id"].as_str().unwrap_or(""));
                println!("  User ID:   {}", body["user_id"].as_str().unwrap_or(""));
                println!("  Name:      {}", body["name"].as_str().unwrap_or(""));
                println!("  API Token: {}", body["token"].as_str().unwrap_or(""));
                println!("\nKeep this token safe! It will not be shown again.");
            }
            TokenCommands::Ls { user_id } => {
                let mut url = format!("{}/api/v1/tokens", base_url);
                if let Some(uid) = user_id {
                    url.push_str(&format!("?user_id={}", uid));
                }

                let res = client.get(&url).headers(headers.clone()).send().await?;

                if !res.status().is_success() {
                    eprintln!("Failed to list tokens: {}", res.text().await?);
                    std::process::exit(1);
                }

                let tokens: serde_json::Value = res.json().await?;
                println!("{:<38} {:<18} {:<24} {:<20}", "TOKEN ID", "USER ID", "NAME", "CREATED AT (MS)");
                println!("{:-<102}", "");
                if let Some(arr) = tokens.as_array() {
                    for t in arr {
                        println!(
                            "{:<38} {:<18} {:<24} {:<20}",
                            t["token_id"].as_str().unwrap_or(""),
                            t["user_id"].as_str().unwrap_or(""),
                            t["name"].as_str().unwrap_or(""),
                            t["created_at_ms"].as_u64().unwrap_or(0)
                        );
                    }
                }
            }
            TokenCommands::Revoke { token_id } => {
                let url = format!("{}/api/v1/tokens/{}", base_url, token_id);
                let res = client.delete(&url).headers(headers.clone()).send().await?;

                if !res.status().is_success() {
                    eprintln!("Failed to revoke token: {}", res.text().await?);
                    std::process::exit(1);
                }

                println!("Token '{}' revoked successfully.", token_id);
            }
        },
    }

    Ok(())
}
