#[derive(Clone)]
pub struct Caches {
    pub block_cache: rocksdb::Cache,
}

impl Default for Caches {
    fn default() -> Self {
        Self::with_capacity(Self::MIN_CAPACITY)
    }
}

impl Caches {
    const MIN_CAPACITY: usize = 64 * 1024 * 1024; // 64 MB

    pub fn with_capacity(capacity: usize) -> Self {
        let block_cache_capacity = std::cmp::max(capacity, Self::MIN_CAPACITY);

        Self {
            block_cache: rocksdb::Cache::new_lru_cache(block_cache_capacity),
        }
    }
}
