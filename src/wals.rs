// use crate::{errors::Result, writer::WALWriteBuffer};
// use rocksdb::BoundColumnFamily;

// use crate::{database::Database, snapshot::DatabaseSnapshot};
// use std::{rc::Rc, sync::Arc};

// pub struct WALS<'a> {
//     pub stream_name: String,
//     pub database: Rc<Database>,
//     pub cf: Arc<BoundColumnFamily<'a>>,

//     writer: WALWriteBuffer<'a>,
// }

// impl<'a> WALS<'a> {
//     pub fn new(name: &str, db: &'a Rc<Database>) -> Result<Self> {
//         let database = db.clone();

//         let cf = match database.cf_exists(name) {
//             true => db.get_cf(name)?.clone(),
//             false => {
//                 database.create_cf(name)?;
//                 db.get_cf(name)?.clone()
//             }
//         };

//         let writer = WALWriteBuffer::new(db, cf.clone());
//         Ok(Self {
//             stream_name: name.to_string(),
//             database: db.clone(),
//             cf,
//             writer,
//         })
//     }

//     pub fn flush(mut self) -> Self {
//         let _ = self.writer.flush(true);
//         self
//     }

//     pub fn snapshot(&self) -> Result<DatabaseSnapshot> {
//         DatabaseSnapshot::new(&self.database, &self.stream_name)
//     }

//     pub fn all(&self) -> Result<Vec<String>> {
//         let _db = &self.database;
//         let result = _db.list_cf()?;
//         Ok(result)
//     }

//     pub fn set(&mut self, key: &str, value: &[u8]) -> Result<()> {
//         self.database.set(&self.stream_name, key, value)
//     }

//     pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
//         self.database.get(&self.stream_name, key)
//     }

//     pub fn append(&mut self, value: &[u8]) -> Result<()> {
//         self.writer.add(value);
//         self.writer.flush(false)?;

//         Ok(())
//     }
// }

// #[cfg(test)]
// mod tests {
//     use std::fs;

//     use crate::{
//         database::{DBOptions, Database},
//         id::StreamID,
//         timestamped::epoch_ns,
//     };

//     use super::WALS;

//     #[test]
//     fn test_wals_new() {
//         let _ = fs::remove_dir_all("test_wals_new.db");

//         {
//             // NOTE: Check fresh storage setup.
//             let db = Database::open("test_wals_new.db", &DBOptions::default()).unwrap();
//             assert!(!db.cf_exists("my-stream"));

//             let stream = WALS::new("my-stream", &db);
//             assert!(stream.is_ok());

//             let stream_unroll = stream.unwrap();
//             assert_eq!(stream_unroll.stream_name, "my-stream".to_string());
//             assert_eq!(
//                 stream_unroll.writer.last_insert.to_string(),
//                 StreamID::default().to_string()
//             );
//         }

//         {
//             // NOTE: Check reading from storage.
//             let db = Database::open("test_wals_new.db", &DBOptions::default()).unwrap();
//             assert!(db.cf_exists("my-stream"));

//             let stream = WALS::new("my-stream", &db);
//             assert!(stream.is_ok());

//             let stream_unroll = stream.unwrap();
//             assert_eq!(stream_unroll.stream_name, "my-stream".to_string());
//             assert_eq!(
//                 stream_unroll.writer.last_insert.to_string(),
//                 StreamID::default().to_string()
//             );
//         }
//     }

//     #[test]
//     fn test_stream_iter() {
//         let _ = fs::remove_dir_all("test_stream_iter.db");

//         {
//             // NOTE: Check fresh storage setup.
//             let db = Database::open("test_stream_iter.db", &DBOptions::default()).unwrap();
//             assert!(!db.cf_exists("my-stream"));

//             let stream = WALS::new("my-stream", &db);
//             assert!(stream.is_ok());

//             let stream_unroll = stream.unwrap();
//             assert_eq!(stream_unroll.stream_name, "my-stream".to_string());
//             assert_eq!(
//                 stream_unroll.writer.last_insert.to_string(),
//                 StreamID::default().to_string()
//             );
//         }

//         let db = Database::open("test_stream_iter.db", &DBOptions::default()).unwrap();
//         let mut stream = WALS::new("my-stream", &db).unwrap();
//         let _ = stream.append(&[1]);
//         let _ = stream.append(&[1, 2]);
//         let _ = stream.append(&[1, 2, 3]);
//         stream = stream.flush();

//         let snap = stream.snapshot().unwrap();
//         let mut iter = snap.iter(&StreamID::default());
//         let d = iter.tail_distance().unwrap();
//         assert_eq!(d, 3);

//         let not_none = iter.next(); // Start the iterator.
//         assert!(not_none.is_some());

//         let d = iter.tail_distance().unwrap();
//         assert_eq!(d, 2);

//         iter.next();
//         iter.next();
//         let d = iter.tail_distance().unwrap();
//         assert_eq!(d, 0);

//         iter.next();
//         assert!(iter.ended);

//         let mut stream2 = WALS::new("new-stream", &db).unwrap();

//         stream
//             .snapshot()
//             .unwrap()
//             .iter(&StreamID::default())
//             .for_each(|record| {
//                 println!("record={:?}", record.value);
//                 let _ = stream2.append(&record.value);
//             });
//     }

//     #[test]
//     fn test_stream_iter_perf() {
//         let _ = fs::remove_dir_all("test_stream_iter_perf.db");

//         {
//             // NOTE: Check fresh storage setup.
//             let db = Database::open("test_stream_iter_perf.db", &DBOptions::default()).unwrap();
//             assert!(!db.cf_exists("my-stream"));

//             let stream = WALS::new("my-stream", &db);
//             assert!(stream.is_ok());

//             let stream_unroll = stream.unwrap();
//             assert_eq!(stream_unroll.stream_name, "my-stream".to_string());
//             assert_eq!(
//                 stream_unroll.writer.last_insert.to_string(),
//                 StreamID::default().to_string()
//             );
//         }

//         let db = Database::open("test_stream_iter_perf.db", &DBOptions::default()).unwrap();
//         let mut stream = WALS::new("my-stream", &db).unwrap();

//         let start = epoch_ns();
//         for _ in 0..100000 {
//             let _ = stream.append(&[1; 32]);
//         }
//         let end = epoch_ns();
//         let result: f64 = end as f64 - start as f64;
//         let as_secs = result * 1e-9;

//         // NOTE: Assert guarantee at 3mb per sec
//         assert!(as_secs <= 1.05_f64);
//     }

//     #[test]
//     fn test_slide_window() {
//         let _ = fs::remove_dir_all("test_slide_window.db");

//         let db = Database::open("test_slide_window.db", &DBOptions::default()).unwrap();

//         {
//             let mut stream = WALS::new("my-stream", &db).unwrap();
//             for _ in 0..101 {
//                 let _ = stream.append(&[1; 32]);
//             }

//             let mut counter = 0;
//             stream
//                 .flush()
//                 .snapshot()
//                 .unwrap()
//                 .window(10, &StreamID::default())
//                 .for_each(|batch| {
//                     println!("size={}", batch.len());
//                     assert_eq!(batch.len(), 10);
//                     counter += 1;
//                 });

//             assert_eq!(counter, 10)
//         }
//     }
// }
