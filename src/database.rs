use std::{path::Path, rc::Rc, sync::Arc};

use rocksdb::{
    BoundColumnFamily, DBIteratorWithThreadMode, DBWithThreadMode, IteratorMode, MultiThreaded,
};
use serde::{Deserialize, Serialize};

use crate::{errors::Result, id::StreamID, serialization::BinCode, wals::WALMetadata};

#[derive(Serialize, Deserialize)]
#[serde(remote = "rocksdb::DBCompressionType")]
pub enum DBCompressionTypeDef {
    None,
    Snappy,
    Zlib,
    Bz2,
    Lz4,
    Lz4hc,
    Zstd,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct DBOptions {
    pub max_open_files: Option<i32>,
    pub create_if_missing: bool,

    #[serde(with = "DBCompressionTypeDef")]
    pub compression_type: rocksdb::DBCompressionType,

    pub max_total_wal_size: Option<u64>,
}

impl DBOptions {
    pub fn new(
        max_open_files: Option<i32>,
        create_if_missing: bool,
        compression_type: rocksdb::DBCompressionType,
        max_total_wal_size: Option<u64>,
    ) -> Self {
        Self {
            max_open_files,
            create_if_missing,
            compression_type,
            max_total_wal_size,
        }
    }
}

impl Default for DBOptions {
    fn default() -> Self {
        Self::new(None, true, rocksdb::DBCompressionType::None, None)
    }
}

impl std::convert::From<&DBOptions> for rocksdb::Options {
    fn from(options: &DBOptions) -> Self {
        let mut default = Self::default();

        default.create_if_missing(options.create_if_missing);
        default.set_compression_type(options.compression_type);
        default.set_max_open_files(options.max_open_files.unwrap_or(4096));
        default.set_max_total_wal_size(options.max_total_wal_size.unwrap_or(0));

        default
    }
}

pub struct Database {
    pub db: rocksdb::DB,
    options: DBOptions,
}

impl Database {
    /// Creates a database object and corresponding filesystem elements.
    pub fn open<P: AsRef<Path>>(path: P, options: &DBOptions) -> Result<Rc<Self>> {
        let _db = match rocksdb::DB::list_cf(&rocksdb::Options::default(), &path) {
            Ok(cf_names) => Self {
                // NOTE: Database files exists, sourcing all CFs.
                db: rocksdb::DB::open_cf(&options.into(), path, cf_names.clone())?,
                options: *options,
            },
            Err(_) => Self {
                // NOTE: DB files don't exists, create new structure.
                db: rocksdb::DB::open(&options.into(), path)?,
                options: *options,
            },
        };

        Ok(Rc::new(_db))
    }

    pub fn list_cf(&self) -> Result<Vec<String>> {
        let result = rocksdb::DB::list_cf(&rocksdb::Options::default(), self.db.path())?;
        Ok(result)
    }

    pub fn get_cf(&self, name: &str) -> Result<Arc<BoundColumnFamily>> {
        self.db
            .cf_handle(name)
            .ok_or_else(|| "column family does not exists".to_string())
    }

    /// Checks if column family with the given name exists.
    pub fn cf_exists(&self, name: &str) -> bool {
        self.get_cf(name).is_ok()
    }

    /// Creates new column family.
    pub fn create_cf(&self, name: &str) -> Result<()> {
        self.db
            .create_cf(name, &rocksdb::Options::from(&self.options))?;

        self.db.flush().map_err(Into::into)
    }

    /// Drop column family with a given name.
    pub fn drop_cf(&self, name: &str) -> Result<()> {
        self.db.drop_cf(name).map_err(Into::into)
    }

    /// Set specified key value.
    pub fn set(&self, cf_name: &str, key: &str, value: &[u8]) -> Result<()> {
        self.db
            .put_cf(&self.get_cf(cf_name)?, key, value)
            .map_err(Into::into)
    }

    /// Get specified key.
    pub fn get(&self, cf_name: &str, key: &str) -> Result<Option<Vec<u8>>> {
        self.db
            .get_cf(&self.get_cf(cf_name)?, key)
            .map_err(Into::into)
    }

    /// Delete specified key.
    pub fn delete(&self, cf_name: &str, key: &str) -> Result<()> {
        self.db
            .delete_cf(&self.get_cf(cf_name)?, key)
            .map_err(Into::into)
    }

    /// Get an iterator to read column family with a given name with `IteratorMode::Start`.
    pub fn read_cf(
        &self,
        cf_name: &str,
    ) -> Result<DBIteratorWithThreadMode<DBWithThreadMode<MultiThreaded>>> {
        let iter = self
            .db
            .iterator_cf(&self.get_cf(cf_name)?, IteratorMode::Start);

        Ok(iter)
    }

    pub(super) fn get_metadata(&self, cf_name: &str) -> Result<WALMetadata> {
        if let Some(record) = self.get(cf_name, StreamID::metadata().to_string().as_str())? {
            Ok(WALMetadata::from_byte_vec(record.as_slice())?)
        } else {
            Err("record not found".to_string())
        }
    }

    pub(super) fn set_metadata(&self, cf_name: &str, metadata: WALMetadata) -> Result<()> {
        self.set(
            cf_name,
            StreamID::metadata().to_string().as_str(),
            metadata.to_byte_vec()?.as_slice(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::database::{DBOptions, Database};

    #[test]
    fn test_rdb_open() {
        let _ = fs::remove_dir_all("test_rdb_open.db");

        {
            let db = Database::open("test_rdb_open.db", &DBOptions::default());
            assert!(db.is_ok());
        }

        {
            let db1 = Database::open("test_rdb_open.db", &DBOptions::default());
            assert!(db1.is_ok());

            let db2 = Database::open("test_rdb_open.db", &DBOptions::default());
            assert!(db2.is_err());
            assert!(db2.err().unwrap().contains("lock hold by current process"));
        }
    }

    #[test]
    fn test_create_delete_cf() {
        let _ = fs::remove_dir_all("test_create_column_family.db");

        let db = Database::open("test_create_column_family.db", &DBOptions::default());
        assert!(db.is_ok());

        let rdb = db.unwrap();
        assert!(!rdb.cf_exists("stream1"));

        let created = rdb.create_cf("stream1");
        assert!(created.is_ok());
        assert!(rdb.cf_exists("stream1"));

        let _ = rdb.drop_cf("stream1");
        assert!(!rdb.cf_exists("stream1"));
    }

    #[test]
    fn test_list_cf() {
        let _ = fs::remove_dir_all("test_list_cf.db");
        let expected = vec!["default", "stream1", "stream2", "stream3"];

        {
            let db = Database::open("test_list_cf.db", &DBOptions::default());
            assert!(db.is_ok());

            let rdb = db.unwrap();
            assert!(!rdb.cf_exists("stream1"));
            assert!(!rdb.cf_exists("stream2"));
            assert!(!rdb.cf_exists("stream3"));

            let _ = rdb.create_cf("stream1");
            let _ = rdb.create_cf("stream2");
            let _ = rdb.create_cf("stream3");

            let received = rdb.list_cf().unwrap();
            assert_eq!(received, expected);
        }

        let new_db = Database::open("test_list_cf.db", &DBOptions::default()).unwrap();
        assert_eq!(new_db.list_cf().unwrap(), expected);
    }

    #[test]
    fn test_set_get() {
        let _ = fs::remove_dir_all("test_set_get.db");

        let db = Database::open("test_set_get.db", &DBOptions::default()).unwrap();
        let cf_name = "stream1";
        let _ = db.create_cf(cf_name);

        let key = "iterators".to_string();
        let value = "iter1,iter2,iter3,iter4";

        let result = db.set(cf_name, &key.clone(), value.as_bytes());
        assert!(result.is_ok());

        let result = db.get(cf_name, &key.clone());
        assert!(result.is_ok());

        let result_opt = result.unwrap();
        assert!(result_opt.is_some());
        assert_eq!(result_opt.unwrap().as_slice(), value.as_bytes());
    }
}
