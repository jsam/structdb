use std::sync::Arc;

use rocksdb::BoundColumnFamily;

use crate::{
    database::Database,
    errors::Result,
    id::StreamID,
    iterators::{IteratorState, IteratorType, StreamIterator},
    window::SlideWindow,
};

/// A snapshot of `Database` with specified column family.
pub struct DatabaseSnapshot<'a> {
    pub cf_name: &'a str,
    pub db: &'a Database,
    pub column_family: Arc<BoundColumnFamily<'a>>,
    pub snapshot: rocksdb::Snapshot<'a>,
}

/// Implementation of `DBSnapshot` type.
impl<'a> DatabaseSnapshot<'a> {
    pub fn new(db: &'a Database, cf_name: &'a str) -> Result<Self> {
        db.db
            .cf_handle(cf_name)
            .map(|result| {
                return Self {
                    cf_name,
                    db,
                    column_family: result.clone(),
                    snapshot: db.db.snapshot(),
                };
            })
            .ok_or_else(|| "column family not found".to_string())
    }

    // Stateless iterator.
    pub fn iter(&self, from: &StreamID) -> StreamIterator {
        StreamIterator::new(self, IteratorType::Stateless(from.clone()))
    }

    pub fn set(&self, key: &str, value: &[u8]) -> crate::errors::Result<()> {
        self.db.set(self.cf_name, key, value)
    }

    // Statefull iterator.
    pub fn siter(&self, name: &str) -> crate::errors::Result<StreamIterator> {
        let iter_state = IteratorState::get(self, name.to_string())?;
        let iter = StreamIterator::new(self, IteratorType::Stateful(iter_state));

        Ok(iter)
    }

    pub fn siter_override(
        &self,
        override_state: IteratorState,
    ) -> crate::errors::Result<StreamIterator> {
        let iter = StreamIterator::new(self, IteratorType::Stateful(override_state));
        Ok(iter)
    }

    pub fn window(&self, size: u32, from: &StreamID) -> SlideWindow {
        SlideWindow::new(size, self.iter(from))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::{
        database::{DBOptions, Database},
        id::StreamID,
    };

    use super::DatabaseSnapshot;

    #[test]
    fn test_db_snapshot_from() {
        let _ = fs::remove_dir_all("test_db_snapshot_from.db");

        let db = Database::open("test_db_snapshot_from.db", &DBOptions::default()).unwrap();
        let cf_name = "stream1";
        let _ = db.create_cf(cf_name);

        let snap = DatabaseSnapshot::new(&db, cf_name).unwrap();
        assert_eq!(snap.cf_name, "stream1");
    }

    #[test]
    fn test_snapshot_iteration() {
        let _ = fs::remove_dir_all("test_snapshot_iteration.db");

        let _db = Database::open("test_snapshot_iteration.db", &DBOptions::default()).unwrap();
        let _ = _db.create_cf("0");

        let metadata = StreamID::metadata();
        let _ = _db.set(
            "0",
            metadata.to_string().as_str(),
            format!("head=000").as_bytes(),
        );

        let mut start = StreamID::default();
        for i in 0..1e3 as u32 {
            let _ = _db.set(
                "0",
                start.to_string().as_str(),
                format!("value_{0}", i).as_bytes(),
            );
            start = start.next();
        }
        let _ = _db.set(
            "0",
            metadata.to_string().as_str(),
            format!("iterator=123").as_bytes(),
        );

        let snapshot = DatabaseSnapshot::new(&_db, "0");
        assert!(snapshot.is_ok());

        let raw_snapshot = snapshot.unwrap();
        let iter = raw_snapshot.iter(&StreamID::default());

        let mut count: u32 = 0;
        for (key, value) in iter.raw_iter {
            let _key = String::from_utf8(key.to_vec()).unwrap();
            let _value = String::from_utf8(value.to_vec()).unwrap();
            assert_eq!(format!("value_{0}", count), _value);

            let _debug = format!("key={0}, value={1}, count={2}", _key, _value, count);
            println!("{}", _debug);

            count += 1;
        }
    }
}
