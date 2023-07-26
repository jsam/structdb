use std::{path::PathBuf, sync::Arc};

use crate::{
    caches::Caches,
    database::Database,
    errors::Error,
    stats::Stats,
    table::{Table, TableImpl},
};

/// The simplest stored semver.
pub type Semver = [u8; 3];

pub type Migration = Box<dyn Fn(&StructDB) -> Result<Semver, Error>>;

pub trait VersionProvider {
    fn get_version(&self, db: &StructDB) -> Result<Option<Semver>, Error>;
    fn set_version(&self, db: &StructDB, version: Semver) -> Result<(), Error>;
}

/// A simple version provider.
///
/// Uses `weedb_version` entry in the `default` column family.
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultVersionProvider;

impl DefaultVersionProvider {
    const DB_VERSION_KEY: &str = "weedb_version";
}

impl VersionProvider for DefaultVersionProvider {
    fn get_version(&self, db: &StructDB) -> Result<Option<Semver>, Error> {
        match db.db.raw.get(Self::DB_VERSION_KEY)? {
            Some(version) => version
                .try_into()
                .map_err(|_| Error::InvalidDbVersion)
                .map(Some),
            None => Ok(None),
        }
    }

    fn set_version(&self, db: &StructDB, version: Semver) -> Result<(), Error> {
        db.db
            .raw
            .put(Self::DB_VERSION_KEY, version)
            .map_err(Error::DbError)
    }
}

pub struct Builder {
    path: PathBuf,
    options: rocksdb::Options,
    caches: Caches,
    descriptors: Vec<rocksdb::ColumnFamilyDescriptor>,
}

impl Builder {
    pub fn new(path: PathBuf, caches: Caches) -> Self {
        Self {
            path,
            options: Default::default(),
            caches,
            descriptors: Default::default(),
        }
    }

    pub fn options<F>(mut self, mut f: F) -> Self
    where
        F: FnMut(&mut rocksdb::Options, &Caches),
    {
        f(&mut self.options, &self.caches);
        self
    }

    pub fn with_table<T>(mut self) -> Self
    where
        T: Table,
    {
        let mut opts = Default::default();
        T::options(&mut opts, &self.caches);
        self.descriptors
            .push(rocksdb::ColumnFamilyDescriptor::new(T::NAME, opts));
        self
    }

    /// Opens a DB instance.
    pub fn build(self) -> Result<StructDB, rocksdb::Error> {
        let db = Database::open(self.path, &self.options, self.descriptors)?;

        Ok(StructDB {
            db: db,
            caches: self.caches,
        })
    }
}

pub struct StructDB {
    pub db: Database,
    pub caches: Caches,
}

impl StructDB {
    pub fn builder<P: Into<PathBuf>>(path: P, caches: Caches) -> Builder {
        Builder::new(path.into(), caches)
    }

    pub fn make_table<T: Table>(&self) -> TableImpl<T> {
        TableImpl::new(self.db.raw.clone())
    }

    pub fn stats(&self) -> Result<Stats, rocksdb::Error> {
        let whole_db_stats = rocksdb::perf::get_memory_usage_stats(
            Some(&[&self.db.raw]),
            Some(&[&self.caches.block_cache]),
        )?;

        let block_cache_usage = self.caches.block_cache.get_usage();
        let block_cache_pined_usage = self.caches.block_cache.get_pinned_usage();

        Ok(Stats {
            whole_db_stats,
            block_cache_usage,
            block_cache_pined_usage,
        })
    }

    /// Returns an underlying RocksDB instance.
    #[inline]
    pub fn raw(&self) -> &Arc<rocksdb::DB> {
        &self.db.raw
    }

    /// Returns an underlying caches group.
    #[inline]
    pub fn caches(&self) -> &Caches {
        &self.caches
    }
}

#[cfg(test)]
mod tests {
    use crate::{caches::Caches, table::Table};

    use super::StructDB;

    struct MyTable;

    impl Table for MyTable {
        // Column family name
        const NAME: &'static str = "my_table11233";

        // Modify general options
        fn options(opts: &mut rocksdb::Options, caches: &Caches) {
            opts.set_write_buffer_size(128 * 1024 * 1024);

            let mut block_factory = rocksdb::BlockBasedOptions::default();
            block_factory.set_block_cache(&caches.block_cache);
            block_factory.set_data_block_index_type(rocksdb::DataBlockIndexType::BinaryAndHash);

            opts.set_block_based_table_factory(&block_factory);
            opts.create_if_missing(true);
            opts.create_missing_column_families(true);
            opts.set_optimize_filters_for_hits(true);
        }

        // Modify read options
        fn read_options(opts: &mut rocksdb::ReadOptions) {
            opts.set_verify_checksums(false);
        }

        // Modify write options
        fn write_options(opts: &mut rocksdb::WriteOptions) {
            // ...
        }
    }

    #[test]
    fn test_table() {
        let db = StructDB::builder("test_table.db", Caches::default())
            .options(|opts, _| {
                opts.set_level_compaction_dynamic_level_bytes(true);

                // Compression opts
                opts.set_zstd_max_train_bytes(32 * 1024 * 1024);
                opts.set_compression_type(rocksdb::DBCompressionType::Zstd);

                // Logging
                opts.set_log_level(rocksdb::LogLevel::Error);
                opts.set_keep_log_file_num(2);
                opts.set_recycle_log_file_num(2);

                // Cfs
                opts.create_if_missing(true);
                opts.create_missing_column_families(true);
            })
            .with_table::<MyTable>()
            .build()
            .unwrap();
    }
}
