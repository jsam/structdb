use rocksdb::DBIterator;
use serde::{Deserialize, Serialize};

use crate::{id::ByteID, record::StreamRecord};

/// An iterator over the entries of a `Stream`.
pub struct StreamIterator<'a> {
    pub cf_name: &'a str,
    pub raw_iter: DBIterator<'a>,

    start: ByteID,
    current: Option<StreamRecord>,

    ended: bool,
}

impl<'a> StreamIterator<'a> {
    pub fn new(cf_name: &'a str, iter: DBIterator<'a>, start: &ByteID) -> Self {
        Self {
            cf_name: cf_name,
            raw_iter: iter,
            start: start.to_owned(),
            current: None,
            ended: false,
        }
    }
}

impl<'a> Iterator for StreamIterator<'a> {
    type Item = StreamRecord;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.raw_iter.next();

        match item {
            Some((key, value)) => {
                let record = StreamRecord::new(key, value);
                self.current = Some(record.clone());
                Some(record)
            }
            None => {
                self.ended = true;
                None
            }
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct StreamIteratorMeta {
    start: ByteID,
    current: Option<StreamRecord>,
    ended: bool,
}

impl From<StreamIterator<'_>> for StreamIteratorMeta {
    fn from(iter: StreamIterator) -> Self {
        Self {
            start: iter.start,
            current: iter.current,
            ended: iter.ended,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::{
        database::{DBOptions, Database},
        id::ByteID,
        snapshot::DatabaseSnapshot,
    };

    #[test]
    fn test_iterator() {
        let _ = fs::remove_dir_all("test_iterator.db");

        let mut _db = Database::open("test_iterator.db", &DBOptions::default()).unwrap();
        let _ = _db.create_cf("0");

        let metadata = ByteID::metadata();
        let _ = _db.set(
            "0",
            metadata.to_string().as_str(),
            format!("head=000").as_bytes(),
        );

        let mut start = ByteID::default();
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
        let iter = raw_snapshot.iter(&ByteID::default());

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

    #[test]
    fn test_stream_iterator() {
        let _ = fs::remove_dir_all("test_stream_iterator.db");

        let orig = Database::open("test_stream_iterator.db", &DBOptions::default()).unwrap();
        let _db = orig.clone();
        let _ = _db.create_cf("0");

        let metadata = ByteID::metadata();
        let _ = _db.set(
            "0",
            metadata.to_string().as_str(),
            format!("head=000").as_bytes(),
        );

        let mut start = ByteID::default();
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

        let new_db = orig.clone();
        let snapshot = DatabaseSnapshot::new(&new_db, "0");
        assert!(snapshot.is_ok());

        let raw_snapshot = snapshot.unwrap();
        let iter = raw_snapshot.iter(&ByteID::default());

        let mut count: u32 = 0;
        for record in iter {
            assert_eq!(format!("value_{0}", count), record.to_string());

            let _debug = format!(
                "key={0}, value={1}, count={2}",
                record.key.to_string(),
                record.to_string(),
                count
            );
            println!("{}", _debug);

            count += 1;
        }
    }
}
