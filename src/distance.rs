use crate::{id::StreamID, iterators::StreamIterator};

pub struct WALDistance<'a> {
    iterator: StreamIterator<'a>,
    current: StreamID,
}

impl<'a> WALDistance<'a> {}
