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
        default_value = "http://127.0.0.1:8080",
        env = "URU_SERVER",
        help = "UruDrive server URL"
    )]
    server: String,

    #[arg(
        long,
        default_value = "uru_demo_key_1234567890abcdef",
        env = "URU_API_KEY",
        help = "UruDrive API key"
    )]
    api_key: String,

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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let client = reqwest::Client::new();
    let base_url = cli.server.trim_end_matches('/');

    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", cli.api_key))?,
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

            // Step 2: Batch upload only missing chunks
            if missing_count > 0 {
                let to_upload: Vec<BatchChunk> = batch_chunks
                    .into_iter()
                    .filter(|c| missing_set.contains(&c.hash.to_hex()))
                    .collect();

                let mut current_batch = Vec::new();
                let mut current_size = 0usize;
                const BATCH_LIMIT: usize = 15 * 1024 * 1024; // 15MB chunks per batch

                for chunk in to_upload {
                    let chunk_len = chunk.data.len();
                    if current_size + chunk_len > BATCH_LIMIT && !current_batch.is_empty() {
                        let encoded = encode_batch(&current_batch);
                        let upload_url = format!("{}/api/v1/chunks/batch", base_url);
                        let up_res = client
                            .post(&upload_url)
                            .headers(headers.clone())
                            .header(CONTENT_TYPE, "application/octet-stream")
                            .body(encoded)
                            .send()
                            .await?;

                        if !up_res.status().is_success() {
                            eprintln!("Batch upload failed: {}", up_res.text().await?);
                            std::process::exit(1);
                        }
                        current_batch.clear();
                        current_size = 0;
                    }
                    current_size += chunk_len;
                    current_batch.push(chunk);
                }

                if !current_batch.is_empty() {
                    let encoded = encode_batch(&current_batch);
                    let upload_url = format!("{}/api/v1/chunks/batch", base_url);
                    let up_res = client
                        .post(&upload_url)
                        .headers(headers.clone())
                        .header(CONTENT_TYPE, "application/octet-stream")
                        .body(encoded)
                        .send()
                        .await?;

                    if !up_res.status().is_success() {
                        eprintln!("Final batch upload failed: {}", up_res.text().await?);
                        std::process::exit(1);
                    }
                }
            }

            // Step 3: Commit FileManifest
            let commit_url = format!("{}/api/v1/files/commit", base_url);
            let commit_res = client
                .post(&commit_url)
                .headers(headers.clone())
                .json(&serde_json::json!({
                    "path": remote_path,
                    "manifest": manifest,
                    "parent_version_id": null,
                }))
                .send()
                .await?;

            if !commit_res.status().is_success() {
                eprintln!("Commit failed: {}", commit_res.text().await?);
                std::process::exit(1);
            }

            let version: FileVersion = commit_res.json().await?;
            println!("Uploaded successfully!");
            println!("  Remote Path: {}", remote_path);
            println!("  Version ID:  {}", version.version_id);
            println!("  BLAKE3:      {}", manifest.checksums.blake3);
        }

        Commands::Download {
            remote_path,
            local_path,
            version,
        } => {
            let mut url = format!("{}/api/v1/files{}", base_url, normalize_path(&remote_path)?);
            if let Some(vid) = version {
                url.push_str(&format!("?version_id={vid}"));
            }

            println!("Downloading {} -> {}", url, local_path.display());
            let res = client.get(&url).headers(headers.clone()).send().await?;

            if !res.status().is_success() {
                eprintln!("Download failed: {}", res.text().await?);
                std::process::exit(1);
            }

            let bytes = res.bytes().await?;
            let mut file = File::create(&local_path)?;
            file.write_all(&bytes)?;

            println!("Downloaded {} bytes to {}", bytes.len(), local_path.display());
        }

        Commands::Ls { prefix } => {
            let url = format!("{}/api/v1/files?prefix={}", base_url, prefix);
            let res = client.get(&url).headers(headers.clone()).send().await?;

            if !res.status().is_success() {
                eprintln!("List failed: {}", res.text().await?);
                std::process::exit(1);
            }

            let files: Vec<(String, FileVersion)> = res.json().await?;
            println!("{:<40} {:<10} {:<38}", "PATH", "SIZE", "VERSION ID");
            println!("{:-<90}", "");
            for (path, ver) in files {
                let size = ver.manifest.as_ref().map_or(0, |m| m.size);
                println!("{:<40} {:<10} {:<38}", path, size, ver.version_id);
            }
        }

        Commands::Delete {
            remote_path,
            version,
        } => {
            let mut url = format!("{}/api/v1/files{}", base_url, normalize_path(&remote_path)?);
            if let Some(vid) = version {
                url.push_str(&format!("?version_id={vid}"));
            }

            let res = client.delete(&url).headers(headers.clone()).send().await?;
            if !res.status().is_success() {
                eprintln!("Delete failed: {}", res.text().await?);
                std::process::exit(1);
            }

            println!("Successfully deleted {}", remote_path);
        }

        Commands::Gc => {
            let url = format!("{}/api/v1/gc", base_url);
            let res = client.post(&url).headers(headers.clone()).send().await?;

            if !res.status().is_success() {
                eprintln!("GC failed: {}", res.text().await?);
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
    }

    Ok(())
}
