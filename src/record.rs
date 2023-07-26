use serde::{Deserialize, Serialize};
use vlseqid::id::BigID;

use crate::timestamp::Timestamp;

#[derive(Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct SeqRecord {
    pub key: BigID,
    pub value: Box<[u8]>,

    pub timestamp: Timestamp,
}

impl ToString for SeqRecord {
    fn to_string(&self) -> String {
        let result = match String::from_utf8(self.value.to_vec()) {
            Ok(result) => result,
            Err(_) => {
                let v = self.value.as_ref();
                v.iter().map(|c| *c as char).collect::<String>()
            }
        };

        result
    }
}

impl SeqRecord {
    pub fn new(key: BigID, value: Box<[u8]>) -> Self {
        let timestamp = Timestamp::new();
        Self {
            key,
            value,
            timestamp,
        }
    }

    pub fn size(&self) -> usize {
        self.value.len()
    }
}

pub struct KVRecord {
    pub key: String,
    pub value: Box<[u8]>,

    pub timestamp: Timestamp,
}

impl KVRecord {
    pub fn key(&mut self) -> &[u8] {
        let value = self.key.as_bytes();
        value
    }

    pub fn value(&mut self) -> &[u8] {
        let value = self.value.as_ref();
        value
    }
}
