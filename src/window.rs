use crate::{iterators::StreamIterator, record::StreamRecord};

trait Window {
    type Item;

    fn window(n: u32) -> Vec<Self::Item>;
}

pub struct SlideWindow<'a> {
    size: u32,
    iterator: StreamIterator<'a>,
}

impl<'a> SlideWindow<'a> {
    pub(crate) fn new(size: u32, iterator: StreamIterator<'a>) -> Self {
        Self { size, iterator }
    }
}

impl<'a> Iterator for SlideWindow<'a> {
    type Item = Vec<StreamRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        let distance = self.iterator.tail_distance().unwrap_or(0);
        if distance < self.size as u128 {
            None
        } else {
            let mut result = vec![];
            for _ in 0..self.size {
                match self.iterator.next() {
                    Some(record) => result.push(record),
                    None => continue,
                };
            }

            Some(result)
        }
    }
}
