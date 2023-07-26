use std::{path::Path, sync::Arc};

use crate::errors::{Result, RocksResult};
use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBIteratorWithThreadMode, DBWithThreadMode,
    IteratorMode, MultiThreaded, WriteBatch,
};
use serde::{Deserialize, Serialize};

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

pub struct Database {
    pub raw: Arc<rocksdb::DB>,
    options: rocksdb::Options,
}

impl Database {
    /// Creates a database object and corresponding filesystem elements.
    pub fn open<P: AsRef<Path>, I: IntoIterator<Item = ColumnFamilyDescriptor>>(
        path: P,
        options: &rocksdb::Options,
        cfd: I,
    ) -> RocksResult<Self> {
        let db = Arc::new(rocksdb::DB::open_cf_descriptors(&options, path, cfd)?);

        Ok(Database {
            raw: db,
            options: options.clone(),
        })
    }

    pub fn list_cf(&self) -> RocksResult<Vec<String>> {
        let result = rocksdb::DB::list_cf(&rocksdb::Options::default(), self.raw.path())?;
        Ok(result)
    }

    pub fn get_cf(&self, name: &str) -> Option<Arc<BoundColumnFamily>> {
        let result = self.raw.cf_handle(name);
        result
    }

    /// Checks if column family with the given name exists.
    pub fn cf_exists(&self, name: &str) -> bool {
        self.get_cf(name).is_some()
    }

    /// Creates new column family.
    pub fn create_cf(&self, name: &str) -> RocksResult<()> {
        self.raw.create_cf(name, &self.options)?;

        self.raw.flush().map_err(Into::into)
    }

    /// Drop column family with a given name.
    pub fn drop_cf(&self, name: &str) -> RocksResult<()> {
        self.raw.drop_cf(name).map_err(Into::into)
    }

    /// Set specified key value.
    pub fn set(&self, cf_name: &str, key: &str, value: &[u8]) -> Result<()> {
        let cf = match self.get_cf(cf_name) {
            Some(cf) => cf,
            None => return Err("column family not found".to_string()),
        };

        self.raw.put_cf(&cf, key, value).map_err(Into::into)
    }

    // Set batch of records.
    pub fn set_batch(&self, batch: WriteBatch) -> crate::errors::Result<()> {
        self.raw.write(batch).map_err(Into::into)
    }

    /// Get specified key.
    pub fn get(&self, cf_name: &str, key: &str) -> Result<Option<Vec<u8>>> {
        let cf = match self.get_cf(cf_name) {
            Some(cf) => cf,
            None => return Err("column family not found".to_string()),
        };

        self.raw.get_cf(&cf, key).map_err(Into::into)
    }

    /// Delete specified key.
    pub fn delete(&self, cf_name: &str, key: &str) -> Result<()> {
        let cf = match self.get_cf(cf_name) {
            Some(cf) => cf,
            None => return Err("column family not found".to_string()),
        };

        self.raw.delete_cf(&cf, key).map_err(Into::into)
    }

    /// Get an iterator to read column family with a given name with `IteratorMode::Start`.
    pub fn read_cf(
        &self,
        cf_name: &str,
    ) -> Result<DBIteratorWithThreadMode<DBWithThreadMode<MultiThreaded>>> {
        let cf = match self.get_cf(cf_name) {
            Some(cf) => cf,
            None => return Err("column family not found".to_string()),
        };

        let iter = self.raw.iterator_cf(&cf, IteratorMode::Start);

        Ok(iter)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::database::Database;

    #[test]
    fn test_rdb_open() {
        let _ = fs::remove_dir_all("test_rdb_open.db");

        {
            let db = Database::open(
                "test_rdb_open.db",
                &rocksdb::Options::default(),
                vec![rocksdb::ColumnFamilyDescriptor::new(
                    "test_table",
                    Default::default(),
                )],
            );
            assert!(db.is_ok());
        }

        {
            let db1 = Database::open(
                "test_rdb_open.db",
                &rocksdb::Options::default(),
                vec![rocksdb::ColumnFamilyDescriptor::new(
                    "test_table",
                    Default::default(),
                )],
            );
            let err = db1.err();
            //assert!(db1.is_ok());

            let db2 = Database::open(
                "test_rdb_open.db",
                &rocksdb::Options::default(),
                vec![rocksdb::ColumnFamilyDescriptor::new(
                    "test_table",
                    Default::default(),
                )],
            );
            assert!(db2.is_err());
            //assert!(db2.err().unwrap().contains("lock hold by current process"));
        }
    }

    #[test]
    fn test_create_delete_cf() {
        let _ = fs::remove_dir_all("test_create_column_family.db");
        let mut opts = Default::default();
        let descriptors = vec![rocksdb::ColumnFamilyDescriptor::new("test_table", opts)];
        let db = Database::open(
            "test_create_column_family.db",
            &rocksdb::Options::default(),
            descriptors,
        );
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
            let db = Database::open(
                "test_list_cf.db",
                &rocksdb::Options::default(),
                vec![rocksdb::ColumnFamilyDescriptor::new(
                    "test_table",
                    Default::default(),
                )],
            );
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

        let new_db = Database::open(
            "test_list_cf.db",
            &rocksdb::Options::default(),
            vec![rocksdb::ColumnFamilyDescriptor::new(
                "test_table",
                Default::default(),
            )],
        )
        .unwrap();
        assert_eq!(new_db.list_cf().unwrap(), expected);
    }

    #[test]
    fn test_set_get() {
        let _ = fs::remove_dir_all("test_set_get.db");
        let mut opts = Default::default();
        let descriptors = vec![rocksdb::ColumnFamilyDescriptor::new("test_table", opts)];

        let db =
            Database::open("test_set_get.db", &rocksdb::Options::default(), descriptors).unwrap();
        let cf_name = "stream1";
        let _ = db.create_cf(cf_name);

        let key = "iterators".to_string();
        let value = "iter1,iter2,iter3,iter4";

        let result = db.set(cf_name, &key, value.as_bytes());
        assert!(result.is_ok());

        let result = db.get(cf_name, &key);
        assert!(result.is_ok());

        let result_opt = result.unwrap();
        assert!(result_opt.is_some());
        assert_eq!(result_opt.unwrap().as_slice(), value.as_bytes());
    }
}
