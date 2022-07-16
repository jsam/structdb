use rocksdb::{DBIterator, Direction, IteratorMode};

use crate::{database::Database, id::StreamID, record::StreamRecord, snapshot::DatabaseSnapshot};

#[derive(Clone)]
pub struct IteratorState {
    pub iter_name: String,
    pub from: StreamID,
}

impl IteratorState {
    pub fn new(iter_name: String) -> Self {
        Self {
            iter_name,
            from: StreamID::default(),
        }
    }

    pub fn get<'a>(
        snapshot: &'a DatabaseSnapshot,
        iter_name: String,
    ) -> crate::errors::Result<Self> {
        let from = match snapshot.snapshot.get(format!("iterator-{0}", iter_name))? {
            Some(bytes) => StreamID::from(String::from_utf8_lossy(bytes.as_ref()).as_ref()),
            None => StreamID::default(),
        };

        let _self = Self { iter_name, from };
        Ok(_self)
    }

    pub fn set<'a>(
        &self,
        snapshot: &'a DatabaseSnapshot,
        new_id: StreamID,
    ) -> crate::errors::Result<Self> {
        let new_from = new_id.next();
        let key = format!("iterator-{0}", self.iter_name);

        let _ = snapshot.set(&key, new_from.to_string().as_bytes())?;
        let new_self = Self {
            iter_name: self.iter_name.clone(),
            from: new_from,
        };
        Ok(new_self)
    }
}

#[derive(Clone)]
pub enum IteratorType {
    Stateless(StreamID),
    Statefull(IteratorState),
}

/// An iterator over the entries of a `Stream`.
pub struct StreamIterator<'a> {
    snapshot: &'a DatabaseSnapshot<'a>,
    pub raw_iter: DBIterator<'a>,

    // TODO: `current` is not fully utilized, we should consider expanding the state here.
    current: Option<StreamRecord>,
    ended: bool,
    iter_type: IteratorType, // Determines statefulness of an iterator.
}

impl<'a> StreamIterator<'a> {
    pub fn new(snapshot: &'a DatabaseSnapshot, iter_type: IteratorType) -> Self {
        match iter_type {
            IteratorType::Stateless(stream_id) => {
                let raw_iter = snapshot.snapshot.iterator_cf(
                    &snapshot.column_family,
                    IteratorMode::From(stream_id.to_string().as_bytes(), Direction::Forward),
                );

                Self {
                    snapshot,
                    raw_iter,
                    current: None,
                    ended: false,
                    iter_type: IteratorType::Stateless(stream_id.clone()),
                }
            }
            IteratorType::Statefull(state) => {
                let raw_iter = snapshot.snapshot.iterator_cf(
                    &snapshot.column_family,
                    IteratorMode::From(state.from.to_string().as_bytes(), Direction::Forward),
                );

                Self {
                    snapshot,
                    raw_iter,
                    current: None,
                    ended: false,
                    iter_type: IteratorType::Statefull(state),
                }
            }
        }
    }
}

impl<'a> Iterator for StreamIterator<'a> {
    type Item = StreamRecord;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.raw_iter.next();
            if item.is_none() {
                self.ended = true;
                return None;
            }

            if let Some((key, value)) = item {
                let key = StreamID::from(key);
                if !key.valid {
                    continue;
                }

                let record = StreamRecord::new(key, value);
                self.current = Some(record.clone());

                if let IteratorType::Statefull(state) = &self.iter_type {
                    // TODO: Implement proper error handling and remove `unwrap`.
                    let new_state = state.set(self.snapshot, record.key.clone()).unwrap();
                    self.iter_type = IteratorType::Statefull(new_state);
                }

                return Some(record);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::{
        database::{DBOptions, Database},
        id::StreamID,
        snapshot::DatabaseSnapshot,
    };

    #[test]
    fn test_iterator() {
        let _ = fs::remove_dir_all("test_iterator.db");

        let mut _db = Database::open("test_iterator.db", &DBOptions::default()).unwrap();
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

    #[test]
    fn test_stream_iterator() {
        let _ = fs::remove_dir_all("test_stream_iterator.db");

        let orig = Database::open("test_stream_iterator.db", &DBOptions::default()).unwrap();
        let _db = orig.clone();
        let _ = _db.create_cf("0");

        let _ = _db.set("0", "random-start", format!("randomvalue").as_bytes());

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
                format!("random{}", i).as_str(),
                format!("randomvalue").as_bytes(),
            );
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
        let _ = _db.set("0", "random-end", format!("randomvalue").as_bytes());

        let new_db = orig.clone();
        let snapshot = DatabaseSnapshot::new(&new_db, "0");
        assert!(snapshot.is_ok());

        let raw_snapshot = snapshot.unwrap();

        {
            let iter = raw_snapshot.iter(&StreamID::default());

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
        {
            let iter = raw_snapshot.iter(&StreamID::from("stream-000000000000000000003228"));

            let mut count: u32 = 995;
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
}
