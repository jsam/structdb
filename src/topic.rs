use byte_counter::counter::ByteCounter;

use crate::errors::{Error, Result};
use crate::iterator_batch::IteratorBatch;
use crate::iterator_single::IteratorSingle;
use crate::record::{Record, SeqRecord};
use crate::table::{Table, TableImpl};

pub const TOPIC_ITERATOR_KEY_PREFIX: &str = "iter";
pub const TOPIC_KEY_PREFIX: &str = "topic";
pub const TOPIC_LAST_INSERT_KEY: &str = "last";

pub trait Topic: Table {}

pub struct TopicImpl<T> {
    pub table: TableImpl<T>,
    pub next_insert: ByteCounter,
}

impl<T> TopicImpl<T>
where
    T: Topic,
{
    pub fn new(table: TableImpl<T>) -> Self {
        let mut topic = Self {
            table: table,
            next_insert: ByteCounter::new_with_prefix(TOPIC_KEY_PREFIX.to_string()),
        };
        topic.seek_last();

        topic
    }

    fn seek_last(&mut self) {
        let mut iter = self.table.prefix_iterator(TOPIC_KEY_PREFIX);
        loop {
            if !iter.valid() {
                break;
            }
            let item = iter.item();
            if item.is_none() {
                break;
            }

            let record = SeqRecord::from(item);
            if record.is_valid() {
                self.next_insert = record.key;
            }

            iter.next();
        }

        self.next_insert = self.next_insert.next_id();
    }

    pub fn append(&mut self, value: &Record) -> Result<SeqRecord> {
        let key = self.next_insert.to_string();

        self.table
            .insert(key, value)
            .map_err(|err| Error::DbError(err))?;

        self.next_insert = self.next_insert.next_id();
        Ok(SeqRecord::new(self.next_insert.clone(), value.clone()))
    }

    pub fn window(&'_ self, name: &str, batch_size: usize) -> IteratorBatch<'_, T> {
        IteratorBatch::new(Box::new(self), name, batch_size)
    }

    pub fn iter(&'_ self) -> IteratorSingle<'_, T> {
        IteratorSingle::new(Box::new(self))
    }
}

#[cfg(test)]
mod tests {

    use std::fs;

    use crate::{
        builder::StructDB, caches::Caches, database::Database, iterator_batch::BatchIterator,
        record::SeqRecord, serialization::BinCode, table::Table,
    };

    use super::Topic;

    struct MyTopic;

    impl Table for MyTopic {
        const NAME: &'static str = "my-topic";
    }

    impl Topic for MyTopic {}

    #[test]
    fn test_topic_write() {
        let _ = fs::remove_dir_all("test_topic_write.db");
        let db = StructDB::builder("test_topic_write.db", Caches::default())
            .with_struct::<MyTopic>()
            .build();
        assert!(db.is_ok());

        let db = db.unwrap();
        let mut topic = db.make_topic::<MyTopic>();

        let value = "topic-value-1".to_bytes().unwrap();
        let result = topic.append(&value);
        assert!(result.is_ok());
    }

    #[test]
    fn test_topic_write_sharded() {
        let _ = fs::remove_dir_all("test_topic_write_sharded.db");

        {
            let db = StructDB::builder("test_topic_write_sharded.db", Caches::default())
                .with_struct::<MyTopic>()
                .build();
            assert!(db.is_ok());

            let db = db.unwrap();
            let _ = db.make_topic::<MyTopic>();
            let _ = db.make_sharded_topic::<MyTopic>(&"shard1".to_string());
            let _ = db.make_sharded_topic::<MyTopic>(&"shard1".to_string());
            let _ = db.make_sharded_topic::<MyTopic>(&"shard2".to_string());

            let received = Database::list_cf(db.db.raw.path()).expect("list_cf failed");
            assert_eq!(
                received,
                vec!["default", "my-topic", "my-topic_shard1", "my-topic_shard2"]
            );
        }
        {
            let db =
                StructDB::builder("test_topic_write_sharded.db", Caches::default()).build_all();
            assert!(db.is_ok());
            let db = db.unwrap();

            let received = db.db.list_cfamily().expect("list_cf failed");
            assert_eq!(
                received,
                vec!["default", "my-topic", "my-topic_shard1", "my-topic_shard2"]
            )
        }
    }

    #[test]
    fn test_topic_write_continuously() {
        let _ = fs::remove_dir_all("test_topic_write_continuously.db");
        let db = StructDB::builder("test_topic_write_continuously.db", Caches::default())
            .with_struct::<MyTopic>()
            .build()
            .unwrap();
        // assert!(db.is_ok());

        // let db = db.unwrap();

        {
            let mut topic = db.make_topic::<MyTopic>();

            for i in 0..100 {
                let value = format!("topic-value-{}", i).to_bytes().unwrap();
                let result = topic.append(&value);
                assert!(result.is_ok());

                let record = result.unwrap();
                assert!(record.is_valid());
            }
        }

        {
            let mut topic = db.make_topic::<MyTopic>();

            for i in 100..200 {
                let value = format!("topic-value-{}", i).to_bytes().unwrap();
                let result = topic.append(&value);
                assert!(result.is_ok());

                let record = result.unwrap();
                assert!(record.is_valid());
            }
        }

        {
            let topic = db.make_topic::<MyTopic>();
            let mut iter = topic.window("iter1", 10);

            let mut count = 0;
            loop {
                let batch = iter.next().unwrap();
                if batch.is_empty() {
                    break;
                }
                assert!(batch.len() == 10 || batch.len() == 1);

                for record in batch.iter() {
                    let view = String::from_bytes(record.value.clone().as_mut_slice()).unwrap();
                    //println!("PREFIX FIX VIEW: {:?} == {:?}", view, "topic");
                    let received = format!("topic-value-{}", count);
                    assert_eq!(received, view);
                    count += 1;
                }
            }
            assert_eq!(count, 200);
        }
    }

    #[test]
    fn test_topic_read() {
        let _ = fs::remove_dir_all("test_topic_read.db");

        let db = StructDB::builder("test_topic_read.db", Caches::default())
            .with_struct::<MyTopic>()
            .build();
        assert!(db.is_ok());

        let db = db.unwrap();
        let mut topic = db.make_topic::<MyTopic>();

        for i in 0..101 {
            let value = format!("topic-value-{}", i).to_bytes().unwrap();
            let result = topic.append(&value);
            assert!(result.is_ok());

            let record = result.unwrap();
            assert!(record.is_valid());
            //let key = record.to_string();
            //println!("record: {}", record);
        }

        {
            let mut iter = topic.table.raw_iterator();
            iter.seek_to_first();

            let mut count = 0;
            loop {
                if !iter.valid() {
                    println!("iter is not valid");
                    break;
                }
                let item = iter.item();
                if item.is_none() {
                    break;
                }
                let record = SeqRecord::from(item);
                assert!(record.is_valid());

                //println!("record_read_raw: {:?}", record.key.id);
                count += 1;

                iter.next();
            }
            assert_eq!(count, 101);
        }

        {
            let mut iter = topic.table.raw_iterator();
            iter.seek("topic".as_bytes());

            let mut count = 0;
            loop {
                if !iter.valid() {
                    break;
                }
                let item = iter.item();
                if item.is_none() {
                    break;
                }
                let record = SeqRecord::from(item);
                assert!(record.is_valid());

                // println!("record_read_prefix: {:?}", record);
                count += 1;

                iter.next();
            }
            assert_eq!(count, 101);
        }

        {
            let mut iter = topic.window("iter1", 10);

            let mut count = 0;
            loop {
                let batch = iter.next().unwrap();
                assert!(batch.len() == 10 || batch.len() == 1);

                for record in batch.iter() {
                    let view = String::from_bytes(record.value.clone().as_mut_slice()).unwrap();
                    //println!("PREFIX FIX VIEW: {:?} == {:?}", view, "topic");
                    let received = format!("topic-value-{}", count);
                    assert_eq!(received, view);
                    count += 1;
                }

                if count == 50 {
                    break;
                }
            }
            assert_eq!(count, 50);
            assert_eq!(iter.tail_distance(), 51);

            let mut iter2 = topic.window("iter1", 10);

            loop {
                let batch = iter2.next().unwrap();
                assert!(batch.len() == 10 || batch.len() == 1);

                for record in batch.iter() {
                    let view = String::from_bytes(record.value.clone().as_mut_slice()).unwrap();
                    //println!("PREFIX FIX VIEW: {:?} == {:?}", view, "topic");
                    let received = format!("topic-value-{}", count);
                    assert_eq!(received, view);
                    count += 1;
                }

                if batch.is_empty() || batch.len() < 10 {
                    break;
                }
            }
            assert_eq!(count, 101);
            assert_eq!(iter.tail_distance(), 0);

            let mut iter3 = topic.window("iter1", 10);

            let batch = iter3.next().unwrap();
            assert!(batch.len() == 0);
            assert_eq!(iter.tail_distance(), 0);
        }

        {
            let mut expected = 1;
            for item in topic.iter() {
                let record = item;
                assert!(record.is_valid());
                assert_eq!(record.key.to_u128(), expected);
                println!("record: {:?}", record);
                expected += 1;
            }
        }
    }
}
