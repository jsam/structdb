use vlseqid::id::BigID;

use crate::errors::{Error, Result};
use crate::iterator::TopicIter;
use crate::record::{Record, SeqRecord};
use crate::table::{Table, TableImpl};

pub const TOPIC_ITERATOR_KEY_PREFIX: &str = "iterator";
pub const TOPIC_KEY_PREFIX: &str = "topic";
pub const TOPIC_LAST_INSERT_KEY: &str = "last";

pub trait Topic: Table {}

pub struct TopicImpl<T> {
    pub table: TableImpl<T>,
    pub stream_name: String,
    pub last_insert: BigID,
}

impl<T> TopicImpl<T>
where
    T: Topic,
{
    pub fn new(table: TableImpl<T>) -> Self {
        Self {
            table: table,
            stream_name: T::NAME.to_owned(),
            last_insert: BigID::new(Some(TOPIC_KEY_PREFIX.to_string())),
        }
    }

    pub fn append(&mut self, value: &Record) -> Result<SeqRecord> {
        let key = self.last_insert.to_string();

        self.table
            .insert(key, value)
            .map_err(|err| Error::DbError(err))?;

        self.last_insert = self.last_insert.next();
        Ok(SeqRecord::new(self.last_insert.clone(), value.clone()))
    }

    pub fn window(&'_ self, name: &str, batch_size: usize) -> TopicIter<'_, T> {
        TopicIter::new(Box::new(self), name, batch_size)
    }
}

#[cfg(test)]
mod tests {

    use std::fs;

    use crate::{
        builder::StructDB, caches::Caches, iterator::BatchIterator, record::SeqRecord,
        serialization::BinCode, table::Table,
    };

    use super::Topic;

    struct MyTopic;

    impl Table for MyTopic {
        const NAME: &'static str = "my-topic";
    }

    impl Topic for MyTopic {}

    #[test]
    fn test_topic_write() {
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
    }
}
