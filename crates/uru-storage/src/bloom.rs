use arc_swap::ArcSwap;
use fastbloom::AtomicBloomFilter;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use uru_core::hash::ChunkHash;

pub struct GlobalBloomFilter {
    filter: ArcSwap<AtomicBloomFilter>,
    fp_rate: f64,
}

impl GlobalBloomFilter {
    pub fn new(expected_items: usize, fp_rate: f64) -> Self {
        let items = expected_items.max(10_000);
        let bloom = AtomicBloomFilter::with_false_pos(fp_rate).expected_items(items);
        Self {
            filter: ArcSwap::from_pointee(bloom),
            fp_rate,
        }
    }

    /// Check if hash may exist in the filter
    #[inline]
    pub fn contains(&self, hash: &ChunkHash) -> bool {
        self.filter.load().contains(hash.as_bytes().as_slice())
    }

    /// Insert a chunk hash into the bloom filter
    #[inline]
    pub fn insert(&self, hash: &ChunkHash) {
        self.filter.load().insert(hash.as_bytes().as_slice());
    }

    /// Atomically swap in a freshly rebuilt Bloom filter with new live entries
    pub fn rebuild<I>(&self, hashes: I, expected_items: usize)
    where
        I: IntoIterator<Item = ChunkHash>,
    {
        let items = expected_items.max(10_000);
        let new_bloom = AtomicBloomFilter::with_false_pos(self.fp_rate).expected_items(items);
        for h in hashes {
            new_bloom.insert(h.as_bytes().as_slice());
        }
        self.filter.store(Arc::new(new_bloom));
    }

    /// Export underlying bit-vector words as bytes (e.g. for GET /api/v1/bloom)
    pub fn export_bytes(&self) -> Vec<u8> {
        let guard = self.filter.load();
        let words = guard.as_slice();
        let mut bytes = Vec::with_capacity(words.len() * 8);
        for word in words {
            let val = word.load(Ordering::Relaxed);
            bytes.extend_from_slice(&val.to_le_bytes());
        }
        bytes
    }
}
