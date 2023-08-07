use crate::{
    record::SeqRecord,
    table::Table,
    topic::{TopicImpl, TOPIC_KEY_PREFIX},
};

pub struct IteratorSingle<'a, T>
where
    T: Table + 'a,
{
    pub topic: Box<&'a TopicImpl<T>>,
    _state: rocksdb::DBRawIterator<'a>,
}

impl<'a, T> IteratorSingle<'a, T>
where
    T: Table,
{
    pub fn new(topic: Box<&'a TopicImpl<T>>) -> Self {
        let _state = topic.table.prefix_iterator(TOPIC_KEY_PREFIX);

        Self {
            topic: topic,
            _state: _state,
        }
    }
}

impl<'a, T> Iterator for IteratorSingle<'a, T>
where
    T: Table,
{
    type Item = SeqRecord;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self._state.item();
        if item.is_none() {
            return None;
        }

        let record = SeqRecord::from(item);
        if !record.is_valid() {
            return None;
        }

        self._state.next();
        Some(record)
    }
}
