use fastcdc::v2020::FastCDC;
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

pub const MIN_CHUNK_SIZE: usize = 16_384; // 16 KB
pub const AVG_CHUNK_SIZE: usize = 65_536; // 64 KB
pub const MAX_CHUNK_SIZE: usize = 262_144; // 256 KB

#[wasm_bindgen]
pub fn init_panic_hook() {
    console_error_panic_hook::set_once();
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ChunkInfo {
    pub hash: String,
    pub offset: u64,
    pub length: u32,
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub struct ChunkerResult {
    pub chunks: Vec<ChunkInfo>,
    pub whole_file_blake3: Option<String>,
}

#[wasm_bindgen]
pub struct WasmStreamChunker {
    hasher: blake3::Hasher,
    buffer: Vec<u8>,
    total_processed: u64,
}

#[wasm_bindgen]
impl WasmStreamChunker {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            hasher: blake3::Hasher::new(),
            buffer: Vec::with_capacity(MAX_CHUNK_SIZE * 2),
            total_processed: 0,
        }
    }

    /// Push a byte slice from browser File.stream()
    pub fn push_bytes(&mut self, slice: &[u8]) -> Result<JsValue, JsValue> {
        self.hasher.update(slice);
        self.buffer.extend_from_slice(slice);

        let mut emitted_chunks = Vec::new();

        // While buffer has enough data to potentially emit full chunks
        while self.buffer.len() >= MAX_CHUNK_SIZE {
            let chunker = FastCDC::new(&self.buffer, MIN_CHUNK_SIZE, AVG_CHUNK_SIZE, MAX_CHUNK_SIZE);
            let mut cut_point = None;

            for chunk in chunker {
                // If chunk is complete (not touching the end of buffer)
                if chunk.offset + chunk.length < self.buffer.len() {
                    let chunk_data = self.buffer[chunk.offset..chunk.offset + chunk.length].to_vec();
                    let hash = blake3::hash(&chunk_data).to_hex().to_string();

                    emitted_chunks.push(ChunkInfo {
                        hash,
                        offset: self.total_processed,
                        length: chunk.length as u32,
                        data: chunk_data,
                    });
                    self.total_processed += chunk.length as u64;
                    cut_point = Some(chunk.offset + chunk.length);
                    break;
                }
            }

            if let Some(cut) = cut_point {
                self.buffer.drain(0..cut);
            } else {
                break;
            }
        }

        let res = ChunkerResult {
            chunks: emitted_chunks,
            whole_file_blake3: None,
        };

        serde_wasm_bindgen::to_value(&res).map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Flush remaining buffer and return final chunks + whole file BLAKE3
    pub fn finish(&mut self) -> Result<JsValue, JsValue> {
        let mut final_chunks = Vec::new();

        if !self.buffer.is_empty() {
            let chunker = FastCDC::new(&self.buffer, MIN_CHUNK_SIZE, AVG_CHUNK_SIZE, MAX_CHUNK_SIZE);
            for chunk in chunker {
                let chunk_data = self.buffer[chunk.offset..chunk.offset + chunk.length].to_vec();
                let hash = blake3::hash(&chunk_data).to_hex().to_string();

                final_chunks.push(ChunkInfo {
                    hash,
                    offset: self.total_processed,
                    length: chunk.length as u32,
                    data: chunk_data,
                });
                self.total_processed += chunk.length as u64;
            }
            self.buffer.clear();
        }

        let whole_file_blake3 = self.hasher.finalize().to_hex().to_string();

        let res = ChunkerResult {
            chunks: final_chunks,
            whole_file_blake3: Some(whole_file_blake3),
        };

        serde_wasm_bindgen::to_value(&res).map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Fast one-shot chunker for Uint8Array
    pub fn chunk_buffer(data: &[u8]) -> Result<JsValue, JsValue> {
        let whole_file_blake3 = blake3::hash(data).to_hex().to_string();
        let chunker = FastCDC::new(data, MIN_CHUNK_SIZE, AVG_CHUNK_SIZE, MAX_CHUNK_SIZE);
        let mut chunks = Vec::new();

        for chunk in chunker {
            let chunk_data = data[chunk.offset..chunk.offset + chunk.length].to_vec();
            let hash = blake3::hash(&chunk_data).to_hex().to_string();
            chunks.push(ChunkInfo {
                hash,
                offset: chunk.offset as u64,
                length: chunk.length as u32,
                data: chunk_data,
            });
        }

        let res = ChunkerResult {
            chunks,
            whole_file_blake3: Some(whole_file_blake3),
        };

        serde_wasm_bindgen::to_value(&res).map_err(|e| JsValue::from_str(&e.to_string()))
    }
}

/// Encode a batch of chunk infos into the packed wire format
#[wasm_bindgen]
pub fn pack_chunks(chunk_hashes: Vec<String>, chunk_payloads: Vec<u8>, offsets: Vec<u32>, lengths: Vec<u32>) -> Result<Vec<u8>, JsValue> {
    if chunk_hashes.len() != lengths.len() || offsets.len() != lengths.len() {
        return Err(JsValue::from_str("Mismatched chunk arrays length"));
    }

    let mut out = Vec::new();
    for i in 0..chunk_hashes.len() {
        let digest_bytes = hex::decode(&chunk_hashes[i])
            .map_err(|e| JsValue::from_str(&format!("Invalid hex: {e}")))?;

        if digest_bytes.len() != 32 {
            return Err(JsValue::from_str("Chunk hash must be 32 bytes"));
        }

        out.extend_from_slice(&digest_bytes);
        let len = lengths[i];
        out.extend_from_slice(&len.to_le_bytes());

        let off = offsets[i] as usize;
        let l = len as usize;
        out.extend_from_slice(&chunk_payloads[off..off + l]);
    }

    Ok(out)
}
