use crate::errors::Result;
use rocksdb::{BoundColumnFamily, WriteBatch};
use vlseqid::id::BigID;

use crate::database::Database;
use std::{rc::Rc, sync::Arc};

pub struct WriteBuffer<'a> {
    db: &'a Rc<Database>,
    cf: Arc<BoundColumnFamily<'a>>,
    pub last_insert: BigID,

    buffer: Vec<Vec<u8>>,
    buffer_size: u128,
    txn_size: u128, // NOTE: specified in bytes
}

impl<'a> WriteBuffer<'a> {
    pub const LAST_INSERT_KEY: &'a str = "last-insert";

    pub fn new(db: &'a Rc<Database>, cf: Arc<BoundColumnFamily<'a>>) -> Self {
        Self {
            db,
            cf,
            last_insert: BigID::default(),
            buffer: vec![],
            buffer_size: 0,
            txn_size: 64512,
        }
    }

    pub fn add(&mut self, value: &[u8]) {
        self.buffer_size += value.len() as u128;
        self.buffer.push(value.to_owned());
    }

    pub fn flush(&mut self, force: bool) -> Result<()> {
        if !force && self.txn_size >= self.buffer_size {
            return Ok(());
        }

        let mut batch = WriteBatch::default();

        for record in self.buffer.iter() {
            self.last_insert = self.last_insert.next();
            batch.put_cf(&self.cf, self.last_insert.to_string(), record);
        }

        // NOTE: Update `last-insert` value.
        batch.put_cf(
            &self.cf,
            WriteBuffer::LAST_INSERT_KEY,
            self.last_insert.to_string(),
        );
        self.db.set_batch(batch)?;

        self.buffer = vec![];
        self.buffer_size = 0;

        Ok(())
    }
}
