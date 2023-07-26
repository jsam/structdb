pub struct Stats {
    pub whole_db_stats: rocksdb::perf::MemoryUsageStats,
    pub block_cache_usage: usize,
    pub block_cache_pined_usage: usize,
}
