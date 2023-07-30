use vlseqid::id::BigID;

use crate::{
    record::SeqRecord,
    table::{Table, TableImpl},
    topic::{TOPIC_KEY_PREFIX, TOPIC_LAST_INSERT_KEY},
};

pub trait BatchIterator {
    type Item;

    fn next(&mut self) -> crate::errors::Result<Vec<Self::Item>>;

    fn tail_distance(&self) -> crate::errors::Result<u128>;
}

pub struct StatefulIter<'a, T>
where
    T: Table + 'a,
{
    pub table: Box<&'a TableImpl<T>>,
    pub name: String,

    _batch_size: usize,
    _state: rocksdb::DBRawIterator<'a>,
}

impl<'a, T> StatefulIter<'a, T>
where
    T: Table,
{
    pub fn new(table: Box<&'a TableImpl<T>>, name: &str, batch_size: usize) -> Self {
        let last_iter = format!("{}:{}", TOPIC_LAST_INSERT_KEY, name);

        let result = table.get(last_iter.as_bytes());
        let start_from = match result {
            Ok(value) => match value {
                Some(value) => {
                    let value = value.as_ref();
                    let value = String::from_utf8_lossy(value);
                    let from = BigID::from(value.as_ref());
                    if from.valid {
                        from.next().to_string()
                    } else {
                        TOPIC_KEY_PREFIX.to_string()
                    }
                }
                None => TOPIC_KEY_PREFIX.to_string(),
            },
            Err(_) => TOPIC_KEY_PREFIX.to_string(),
        };

        let _state = table.prefix_iterator(start_from);

        Self {
            table: table,
            name: name.to_string(),
            _batch_size: batch_size,
            _state: _state,
        }
    }
}

impl<'a, T> BatchIterator for StatefulIter<'a, T>
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
                let key = format!("{}:{}", TOPIC_LAST_INSERT_KEY, self.name);
                let key_ser = key.as_bytes();

                let value = last.key.to_string();
                let value_ser = value.as_bytes();

                let _ = self.table.insert(key_ser, value_ser)?;
            }
            None => {}
        }

        Ok(result)
    }

    fn tail_distance(&self) -> crate::errors::Result<u128> {
        todo!()
    }
}
