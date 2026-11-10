import init, { WasmStreamChunker } from '../pkg/uru_wasm.js';

interface UploadMessage {
  action: 'upload';
  file: File;
  remotePath: string;
  serverUrl: string;
  apiKey: string;
}

interface ChunkRecord {
  hash: string;
  offset: number;
  length: number;
  data: Uint8Array;
}

let wasmInitialized = false;

self.onmessage = async (e: MessageEvent<UploadMessage>) => {
  if (e.data.action !== 'upload') return;

  const { file, remotePath, serverUrl, apiKey } = e.data;
  const baseUrl = serverUrl.replace(/\/+$/, '');

  try {
    if (!wasmInitialized) {
      await init();
      wasmInitialized = true;
    }

    self.postMessage({ type: 'log', message: `Starting upload for ${file.name} (${file.size} bytes)` });

    const chunker = new WasmStreamChunker();
    const reader = file.stream().getReader();
    const allChunks: ChunkRecord[] = [];

    const startTime = performance.now();
    let readBytes = 0;

    // Read 4MB slices from browser file stream
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      readBytes += value.length;
      const res = chunker.push_bytes(value);
      if (res && res.chunks) {
        for (const c of res.chunks) {
          allChunks.push(c);
        }
      }

      self.postMessage({
        type: 'progress',
        phase: 'chunking',
        percent: (readBytes / file.size) * 40,
        message: `FastCDC chunking: ${Math.round((readBytes / file.size) * 100)}% (${allChunks.length} chunks)`
      });
    }

    // Finish chunking to get any tail bytes and whole-file BLAKE3
    const finalRes = chunker.finish();
    if (finalRes && finalRes.chunks) {
      for (const c of finalRes.chunks) {
        allChunks.push(c);
      }
    }

    const wholeFileBlake3 = finalRes.whole_file_blake3 || '';
    self.postMessage({
      type: 'log',
      message: `Chunking finished: ${allChunks.length} chunks discovered. Whole-file BLAKE3: ${wholeFileBlake3}`
    });

    // Check missing chunks in batches of 256
    const missingHashSet = new Set<string>();
    const batchSize = 256;

    for (let i = 0; i < allChunks.length; i += batchSize) {
      const batch = allChunks.slice(i, i + batchSize);
      const hashes = batch.map(c => c.hash);

      const checkRes = await fetch(`${baseUrl}/api/v1/chunks/check`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${apiKey}`,
        },
        body: JSON.stringify({ hashes }),
      });

      if (!checkRes.ok) {
        throw new Error(`Failed to check chunk existence: ${await checkRes.text()}`);
      }

      const checkJson = await checkRes.json();
      for (const h of checkJson.missing) {
        missingHashSet.add(h);
      }

      const progress = 40 + ((i + batch.length) / allChunks.length) * 20;
      self.postMessage({
        type: 'progress',
        phase: 'checking',
        percent: progress,
        message: `Queried Global Bloom Filter & Index: ${Math.min(i + batchSize, allChunks.length)}/${allChunks.length}`
      });
    }

    const totalChunks = allChunks.length;
    const missingChunks = allChunks.filter(c => missingHashSet.has(c.hash));
    const skippedChunks = totalChunks - missingChunks.length;
    const dedupRatio = totalChunks > 0 ? (skippedChunks / totalChunks) * 100 : 0;

    const savedBytes = allChunks
      .filter(c => !missingHashSet.has(c.hash))
      .reduce((sum, c) => sum + c.length, 0);

    self.postMessage({
      type: 'metrics',
      totalSize: file.size,
      chunksDiscovered: totalChunks,
      chunksSkipped: skippedChunks,
      dedupRatio,
      savedBytes,
      missingChunksCount: missingChunks.length
    });

    self.postMessage({
      type: 'log',
      message: `Deduplication efficiency: ${skippedChunks}/${totalChunks} chunks skipped (${dedupRatio.toFixed(1)}%). Network bytes saved: ${savedBytes} bytes.`
    });

    // Upload missing chunks in binary batch format (up to ~12MB per batch)
    if (missingChunks.length > 0) {
      const MAX_BATCH_BYTES = 12 * 1024 * 1024;
      let currentBatch: ChunkRecord[] = [];
      let currentBatchBytes = 0;
      let uploadedMissing = 0;

      for (const chunk of missingChunks) {
        if (currentBatchBytes + chunk.length > MAX_BATCH_BYTES && currentBatch.length > 0) {
          await sendBinaryBatch(baseUrl, apiKey, currentBatch);
          uploadedMissing += currentBatch.length;
          currentBatch = [];
          currentBatchBytes = 0;

          const progress = 60 + (uploadedMissing / missingChunks.length) * 35;
          self.postMessage({
            type: 'progress',
            phase: 'uploading',
            percent: progress,
            message: `Uploading missing chunks: ${uploadedMissing}/${missingChunks.length}`
          });
        }
        currentBatch.push(chunk);
        currentBatchBytes += chunk.length;
      }

      if (currentBatch.length > 0) {
        await sendBinaryBatch(baseUrl, apiKey, currentBatch);
        uploadedMissing += currentBatch.length;
      }
    }

    // Finalize: Commit FileManifest
    self.postMessage({ type: 'progress', phase: 'committing', percent: 96, message: 'Committing FileManifest to VFS...' });

    const chunkRefs = allChunks.map(c => ({
      hash: c.hash,
      offset: c.offset,
      length: c.length,
    }));

    const manifest = {
      size: file.size,
      chunks: chunkRefs,
      checksums: {
        blake3: wholeFileBlake3,
        sha256: null,
        md5: null,
        crc32c: null,
      },
      content_type: file.type || 'application/octet-stream',
      created_at_ms: Date.now(),
    };

    const commitRes = await fetch(`${baseUrl}/api/v1/files/commit`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        path: remotePath,
        manifest,
        parent_version_id: null,
      }),
    });

    if (!commitRes.ok) {
      throw new Error(`Failed to commit file manifest: ${await commitRes.text()}`);
    }

    const version = await commitRes.json();
    const elapsedSec = (performance.now() - startTime) / 1000;
    const effectiveSpeedMBps = (file.size / (1024 * 1024)) / Math.max(elapsedSec, 0.001);

    self.postMessage({
      type: 'done',
      version,
      elapsedSec,
      speedMBps: effectiveSpeedMBps,
      message: `File committed successfully as version ${version.version_id} in ${elapsedSec.toFixed(2)}s (${effectiveSpeedMBps.toFixed(2)} MB/s effective speed)`
    });

  } catch (err: any) {
    self.postMessage({ type: 'error', error: err.message || String(err) });
  }
};

async function sendBinaryBatch(baseUrl: string, apiKey: string, chunks: ChunkRecord[]) {
  // Wire format: [32-byte hash][4-byte LE length][payload]...
  const totalLength = chunks.reduce((sum, c) => sum + 32 + 4 + c.data.length, 0);
  const buffer = new Uint8Array(totalLength);
  const view = new DataView(buffer.buffer);

  let offset = 0;
  for (const c of chunks) {
    // 32-byte hash
    const hashBytes = hexToBytes(c.hash);
    buffer.set(hashBytes, offset);
    offset += 32;

    // 4-byte LE length
    view.setUint32(offset, c.length, true);
    offset += 4;

    // Raw payload
    buffer.set(c.data, offset);
    offset += c.data.length;
  }

  const res = await fetch(`${baseUrl}/api/v1/chunks/batch`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/octet-stream',
      'Authorization': `Bearer ${apiKey}`,
    },
    body: buffer,
  });

  if (!res.ok) {
    throw new Error(`Batch upload HTTP error: ${await res.text()}`);
  }
}

function hexToBytes(hex: string): Uint8Array {
  const cleanHex = hex.trim();
  const bytes = new Uint8Array(32);
  for (let i = 0; i < 32; i++) {
    bytes[i] = parseInt(cleanHex.substr(i * 2, 2), 16);
  }
  return bytes;
}
