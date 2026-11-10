# UruDrive (Multi-Protocol Content-Addressable Storage System)

**UruDrive** is a production-grade Content-Addressable Storage (CAS) system in Rust with client-side **FastCDC** deduplication across both native CLI and Browser WASM environments, exposing a unified Virtual File System (VFS) across three protocols:
1. **Custom Deduplicating HTTP API** (`/api/v1/...`)
2. **S3-Compatible API** (`/s3/...` via `s3s`)
3. **WebDAV** (`/dav/...` via `dav-server`)

---

## Workspace Architecture

```text
urudrive/
├── Cargo.toml                  # Workspace root
├── crates/
│   ├── uru-core/               # Pure Rust (WASM-compatible): FastCDC config, BLAKE3, wire types, binary batch codec
│   ├── uru-storage/            # Storage engine: Packfiles, Sparse Hole Punching, Global Bloom Filter, Index, Compaction GC
│   ├── uru-server/             # Axum binary: Auth middleware, Custom API, S3 adapter, WebDAV adapter, Metrics
│   ├── uru-cli/                # Native Rust client binary with clap & reqwest
│   └── uru-wasm/               # wasm-bindgen crate for browser WebWorker streaming chunking + SIMD BLAKE3
└── web-client/                 # Browser frontend (HTML/TS/Vite) + WebWorker dashboard
```

---

## Key Features & Implementations

### 1. FastCDC Chunking & Tagged BLAKE3 Hashing (`uru-core`)
- **FastCDC v2020 Parameters (64KB Average)**:
  - `MIN_CHUNK`: `16,384` (16 KB)
  - `AVG_CHUNK`: `65,536` (64 KB)
  - `MAX_CHUNK`: `262,144` (256 KB)
- **Tagged Chunk Hash**: Wrapped in tagged enum `HashAlgo::Blake3_256 = 0x1e` with raw 32-byte BLAKE3 digests.
- **File Multi-Checksums**: Whole-file BLAKE3, SHA-256 (for S3 checksums & rclone), MD5 (for S3 ETag & WebDAV), and CRC32C.
- **Packed Binary Batch Wire Format**:
  `[32-byte BLAKE3 Hash][4-byte Little-Endian Length][Raw Chunk Bytes]...`

### 2. Global Bloom Filter & Packfile Engine (`uru-storage`)
- **Global In-Memory Bloom Filter**: Single `fastbloom::AtomicBloomFilter` wrapped in `arc_swap::ArcSwap` configured for a 0.1% (`0.001`) false positive rate.
- **64MB Append-Only Packfiles with Self-Describing Trailers**:
  - `pack_00000001.pack`
  - Trailer format: `[Chunk Payloads...][Trailer: Array of (32B Hash, 8B Offset, 4B Length)][4B Trailer Size][8B Magic: "URUPACK1"]`
  - Millisecond recovery from disk trailers on server startup without re-hashing payloads.
- **Immediate Disk Reclamation via Sparse Hole Punching (`fallocate`)**:
  - Reclaims physical disk blocks on Linux filesystems (`ext4`/`XFS`/`Btrfs`/`ZFS`) on 4KB-aligned boundaries using `libc::fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, length)`.
- **Resurrection Race Prevention & 30% Compaction**:
  - When packfile dead-byte ratio exceeds 30%, live chunks are compacted, the old packfile is unlinked, and the Bloom filter is atomically swapped via `ArcSwap`.

### 3. File Versioning & History Architecture
- Full time-sortable version history using `uuid::Uuid::now_v7()`.
- Chunks shared automatically between file versions.
- Soft delete markers (`is_delete_marker: true`) and configurable `RetentionPolicy`.
- Historical read-only tree exposed over WebDAV (`/.history/<path>/`).

### 4. Security & Tenant Isolation (`uru-server`)
- Private VFS namespaces: `(user_id, virtual_path) -> FileVersion`.
- API keys (`uru_...`) stored as keyed BLAKE3 hashes with constant-time verification (`subtle::ConstantTimeEq`).
- Password authentication via Argon2id (`argon2`).
- Basic Auth support for WebDAV.
- Server-side payload hash verification rejecting mismatches with `409 Conflict`.

### 5. Observability (Prometheus & OpenTelemetry)
- Real-time Prometheus metrics exported at `GET /metrics`.
- Gauges: `uru_cas_chunk_index_size`, `uru_cas_packfiles_active_total`, `uru_cas_packfile_dead_bytes_ratio`.
- Counters: `uru_cas_chunks_processed_total` (cache_hit, bloom_false_positive, uploaded), `uru_cas_bytes_ingested_raw_total`, `uru_cas_bytes_stored_disk_total`, `uru_cas_gc_bytes_freed_total`.

---

## Verification & Testing

Run all unit, storage, and multi-protocol integration tests:

```bash
cargo test --workspace
```

Build WebAssembly package:
```bash
cd crates/uru-wasm
RUSTFLAGS="-C target-feature=+simd128" wasm-pack build --target web --out-dir ../../web-client/pkg
```

Build Web Client:
```bash
cd web-client
npm install
npm run build
```

---

## CLI Usage

```bash
# Start server
cargo run -p uru-server -- --port 8080 --data-dir ./data

# Upload a file with FastCDC deduplication
cargo run -p uru-cli -- upload /path/to/local.pdf /docs/local.pdf

# List files
cargo run -p uru-cli -- ls

# Download file
cargo run -p uru-cli -- download /docs/local.pdf ./restored.pdf

# View version history
cargo run -p uru-cli -- history /docs/local.pdf

# Trigger Garbage Collection & Compaction
cargo run -p uru-cli -- gc
```
