use byte_counter::counter::ByteCounter;

use crate::{
    record::SeqRecord,
    table::Table,
    topic::{TopicImpl, TOPIC_ITERATOR_KEY_PREFIX, TOPIC_KEY_PREFIX},
};

pub trait BatchIterator {
    type Item;

    fn next(&mut self) -> crate::errors::Result<Vec<Self::Item>>;

    fn tail_distance(&self) -> u128;
}

pub struct IteratorBatch<'a, T>
where
    T: Table + 'a,
{
    pub topic: Box<&'a TopicImpl<T>>,
    //pub table: Box<&'a TableImpl<T>>,
    pub name: String,

    _batch_size: usize,
    _state: rocksdb::DBRawIterator<'a>,
}

impl<'a, T> IteratorBatch<'a, T>
where
    T: Table,
{
    pub fn new(topic: Box<&'a TopicImpl<T>>, name: &str, batch_size: usize) -> Self {
        let start_from = iter_checkpoint(name, &topic);

        let _state = topic.table.prefix_iterator(start_from);

        Self {
            topic: topic,
            name: name.to_string(),
            _batch_size: batch_size,
            _state: _state,
        }
    }
}

fn iter_checkpoint<T: Table>(name: &str, topic: &Box<&TopicImpl<T>>) -> String {
    let last_iter = format!("{}:{}", TOPIC_ITERATOR_KEY_PREFIX, name);

    let result = topic.table.get(last_iter.as_bytes());
    let start_from = match result {
        Ok(value) => match value {
            Some(value) => {
                let value = value.as_ref();
                let value = String::from_utf8_lossy(value).to_string();
                let from = ByteCounter::from(&value);
                if from.valid {
                    from.next_id().to_string()
                } else {
                    TOPIC_KEY_PREFIX.to_string()
                }
            }
            None => TOPIC_KEY_PREFIX.to_string(),
        },
        Err(_) => TOPIC_KEY_PREFIX.to_string(),
    };
    start_from
}

impl<'a, T> BatchIterator for IteratorBatch<'a, T>
where
    T: Table,
{
    type Item = SeqRecord;

    fn next(&mut self) -> crate::errors::Result<Vec<Self::Item>> {
        let mut result = vec![];

        while result.len() < self._batch_size {
            if !self._state.valid() {
                break;
            }

            let item = self._state.item();
            if item.is_none() {
                break;
            }

            let record = SeqRecord::from(item);
            if !record.is_valid() {
                break;
            }

            result.push(record);
            self._state.next();
        }

        match result.last() {
            Some(last) => {
                let key = format!("{}:{}", TOPIC_ITERATOR_KEY_PREFIX, self.name);
                let key_ser = key.as_bytes();

                let value = last.key.to_string();
                let value_ser = value.as_bytes();

                let _ = self.topic.table.insert(key_ser, value_ser)?;
            }
            None => {}
        }

        Ok(result)
    }

    fn tail_distance(&self) -> u128 {
        // TODO: Last insert should also be persisted so that when restarted, we continue inserting into the right place.
        let last_insert = self.topic.next_insert.clone();
        let checkpoint = iter_checkpoint(self.name.as_ref(), &self.topic);
        let checkpoint = ByteCounter::from(&checkpoint);
        last_insert.distance(&checkpoint)
    }
}
