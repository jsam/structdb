use crate::errors::Result;
use serde::{Deserialize, Serialize};

use crate::{
    database::Database, id::StreamID, serialization::BinCode,
    snapshot::DatabaseSnapshot,
};
use std::rc::Rc;

/*
    Streamly::new("stream1", database)
        .snapshot()
        .iter("iterator1", Iter::Begin)
        .for_each(|record| {
            let stream2 = Stream::from("stream1-model1-inferences", config);
            let result = infer(record);
            stream2.append(result);
        })
*/
trait Stream {
    /// Return all stream in a database.
    fn all(&self) -> Result<Vec<String>>;

    /// Set arbitrary key with value to a `Stream`.
    fn set(&mut self, key: &str, value: &[u8]) -> Result<()>;

    /// Get arbitrary key from a `Stream`.
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Append an item to the `Stream`.
    fn append(&mut self, value: &[u8]) -> Result<()>;
}

#[derive(Serialize, Deserialize, Clone)]
pub struct StreamlyMetadata {
    stream_name: String,
    last_insert: StreamID,
}

impl BinCode for StreamlyMetadata {}

impl Default for StreamlyMetadata {
    fn default() -> Self {
        Self {
            stream_name: Default::default(),
            last_insert: Default::default(),
        }
    }
}

impl StreamlyMetadata {
    fn new(name: &str) -> Self {
        let mut default = Self::default();
        default.stream_name = name.to_string();
        default
    }
}

pub struct Streamly {
    database: Rc<Database>,
    pub metadata: StreamlyMetadata,
}

impl Streamly {
    fn new(name: &str, db: &Rc<Database>) -> Result<Self> {
        let database = db.clone();
        match database.cf_exists(name) {
            true => {
                let metadata = database.get_metadata(name)?;

                let result = Self { database, metadata };
                Ok(result)
            }
            false => {
                let metadata = StreamlyMetadata::new(name);
                let _ = database.create_cf(name)?;
                let _ = database.set_metadata(name, metadata.clone())?;

                let result = Self { database, metadata };
                Ok(result)
            }
        }
    }

    fn snapshot(&self) -> Result<DatabaseSnapshot> {
        DatabaseSnapshot::new(&self.database, &self.metadata.stream_name)
    }
}

impl Stream for Streamly {
    fn all(&self) -> Result<Vec<String>> {
        let _db = &self.database;
        let result = _db.list_cf()?;
        Ok(result)
    }

    fn set(&mut self, key: &str, value: &[u8]) -> Result<()> {
        self.database.set(&self.metadata.stream_name, key, value)
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.database.get(&self.metadata.stream_name, key)
    }

    fn append(&mut self, value: &[u8]) -> Result<()> {
        let _db = &self.database;
        self.metadata.last_insert = self.metadata.last_insert.next();

        _db.set(
            &self.metadata.stream_name,
            &self.metadata.last_insert.to_string(),
            value,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::{
        database::{DBOptions, Database},
        id::StreamID,
        stream::StreamlyMetadata,
        timestamped::{epoch_ns, epoch_secs},
    };

    use super::{Stream, Streamly};

    #[test]
    fn test_streamly_new() {
        let _ = fs::remove_dir_all("test_streamly_new.db");

        {
            // NOTE: Check fresh storage setup.
            let db = Database::open("test_streamly_new.db", &DBOptions::default()).unwrap();
            assert!(!db.cf_exists("my-stream"));

            let stream = Streamly::new("my-stream", &db);
            assert!(stream.is_ok());

            let stream_unroll = stream.unwrap();
            assert_eq!(stream_unroll.metadata.stream_name, "my-stream".to_string());
            assert_eq!(
                stream_unroll.metadata.last_insert.to_string(),
                StreamID::default().to_string()
            );
        }

        {
            // NOTE: Check reading from storage.
            let db = Database::open("test_streamly_new.db", &DBOptions::default()).unwrap();
            assert!(db.cf_exists("my-stream"));

            let stream = Streamly::new("my-stream", &db);
            assert!(stream.is_ok());

            let stream_unroll = stream.unwrap();
            assert_eq!(stream_unroll.metadata.stream_name, "my-stream".to_string());
            assert_eq!(
                stream_unroll.metadata.last_insert.to_string(),
                StreamID::default().to_string()
            );
        }
    }

    #[test]
    fn test_stream_iter() {
        let _ = fs::remove_dir_all("test_stream_iter.db");

        {
            // NOTE: Check fresh storage setup.
            let db = Database::open("test_stream_iter.db", &DBOptions::default()).unwrap();
            assert!(!db.cf_exists("my-stream"));

            let stream = Streamly::new("my-stream", &db);
            assert!(stream.is_ok());

            let stream_unroll = stream.unwrap();
            assert_eq!(stream_unroll.metadata.stream_name, "my-stream".to_string());
            assert_eq!(
                stream_unroll.metadata.last_insert.to_string(),
                StreamID::default().to_string()
            );
        }

        let db = Database::open("test_stream_iter.db", &DBOptions::default()).unwrap();
        let mut stream = Streamly::new("my-stream", &db).unwrap();
        let _ = stream.append(&[1]);
        let _ = stream.append(&[1, 2]);
        let _ = stream.append(&[1, 2, 3]);

        let mut stream2 = Streamly::new("new-stream", &db).unwrap();

        stream
            .snapshot()
            .unwrap()
            .iter(&StreamID::default())
            .for_each(|record| {
                println!("record={:?}", record.value);

                let new_record = &record.clone();
                let _ = stream2.append(&new_record.value);
            });
    }

    #[test]
    fn test_stream_iter_perf() {
        let _ = fs::remove_dir_all("test_stream_iter_perf.db");

        {
            // NOTE: Check fresh storage setup.
            let db = Database::open("test_stream_iter_perf.db", &DBOptions::default()).unwrap();
            assert!(!db.cf_exists("my-stream"));

            let stream = Streamly::new("my-stream", &db);
            assert!(stream.is_ok());

            let stream_unroll = stream.unwrap();
            assert_eq!(stream_unroll.metadata.stream_name, "my-stream".to_string());
            assert_eq!(
                stream_unroll.metadata.last_insert.to_string(),
                StreamID::default().to_string()
            );
        }

        let db = Database::open("test_stream_iter_perf.db", &DBOptions::default()).unwrap();
        let mut stream = Streamly::new("my-stream", &db).unwrap();

        let start = epoch_secs();
        for _ in 0..65000 {
            let _ = stream.append(&[1, 2, 3]);
        }
        let end = epoch_secs();

        assert_eq!(end - start, 1);
    }
}
