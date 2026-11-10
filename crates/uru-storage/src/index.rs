use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use uru_core::hash::ChunkHash;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkState {
    Live,
    Tombstoned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkLocation {
    pub pack_id: u32,
    pub offset: u64,
    pub length: u32,
    pub state: ChunkState,
    pub hole_punched: bool,
}

impl ChunkLocation {
    pub fn new(pack_id: u32, offset: u64, length: u32) -> Self {
        Self {
            pack_id,
            offset,
            length,
            state: ChunkState::Live,
            hole_punched: false,
        }
    }
}

pub type ChunkIndex = DashMap<ChunkHash, ChunkLocation>;
