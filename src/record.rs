use serde::{Deserialize, Serialize};

use crate::id::ByteID;

#[derive(Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct StreamRecord {
    pub key: ByteID,
    pub value: Box<[u8]>,
}

impl ToString for StreamRecord {
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

impl StreamRecord {
    pub fn new(key: Box<[u8]>, value: Box<[u8]>) -> Self {
        Self {
            key: ByteID::from(key),
            value: value,
        }
    }
}
