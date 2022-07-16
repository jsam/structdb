use std::mem;

use serde::{Deserialize, Serialize};

pub const IDENTIFIER_SIZE: usize = mem::size_of::<u64>();

#[derive(Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct StreamID {
    id: [u8; IDENTIFIER_SIZE],

    #[serde(skip_serializing, skip_deserializing)]
    pub valid: bool,
}

impl Default for StreamID {
    fn default() -> Self {
        let mut id = [0; IDENTIFIER_SIZE];
        id[IDENTIFIER_SIZE - 1] = 1;

        Self {
            id: id,
            valid: true,
        }
    }
}

impl ToString for StreamID {
    fn to_string(&self) -> String {
        let _id = self
            .id
            .map(|b| {
                if b < 10 {
                    return format!("00{}", b);
                }

                if b < 100 {
                    return format!("0{}", b);
                }

                format!("{}", b)
            })
            .to_vec()
            .join("");

        format!("stream-{0}", _id)
    }
}

impl From<&str> for StreamID {
    fn from(key: &str) -> Self {
        let mut parts = key.split("-");
        let stream_part = parts.next();
        if stream_part.is_none() || stream_part.unwrap() != "stream" {
            return Self {
                valid: false,
                ..Default::default()
            };
        }

        let id_part = parts.next();
        if id_part.is_none() {
            return Self {
                valid: false,
                ..Default::default()
            };
        }

        let _bytes = id_part
            .unwrap()
            .chars()
            .collect::<Vec<char>>()
            .chunks(3)
            .map(|c| c.iter().collect::<String>().parse::<u8>().unwrap())
            .collect::<Vec<u8>>();

        let mut mem_id = [0x0_u8; IDENTIFIER_SIZE];
        mem_id.clone_from_slice(_bytes.as_ref());

        Self {
            id: mem_id,
            valid: true,
        }
    }
}

impl From<Box<[u8]>> for StreamID {
    fn from(key: Box<[u8]>) -> Self {
        return StreamID::from(String::from_utf8_lossy(&key).as_ref());
    }
}

impl StreamID {
    pub fn metadata() -> Self {
        Self {
            id: [0; IDENTIFIER_SIZE],
            valid: true,
        }
    }

    pub fn raw_value(&self) -> &[u8] {
        self.id.as_ref()
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.id.to_vec()
    }

    pub fn next(&self) -> Self {
        let mut next_id = self.id;
        for byte in next_id.iter_mut().rev() {
            if *byte == u8::MAX {
                *byte = 0
            } else {
                *byte += 1;
                break;
            }
        }

        Self {
            id: next_id,
            valid: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::id::{StreamID, IDENTIFIER_SIZE};

    #[test]
    fn test_byte_id() {
        {
            assert_eq!(IDENTIFIER_SIZE, 8);
        }

        {
            assert_eq!(StreamID::default().id, [0, 0, 0, 0, 0, 0, 0, 1]);
        }

        {
            let mut bid = StreamID::default();

            let next_bid = bid.next();
            assert_eq!(next_bid.id, [0, 0, 0, 0, 0, 0, 0, 2]);

            let next_next_bid = next_bid.next();

            assert_eq!(next_bid.id, [0, 0, 0, 0, 0, 0, 0, 2]);
            assert_eq!(next_next_bid.id, [0, 0, 0, 0, 0, 0, 0, 3]);

            for _ in 0..1e+6 as u64 {
                bid = bid.next();
            }

            assert_eq!(bid.id, [0, 0, 0, 0, 0, 15, 66, 65]);
        }

        {
            let mut bid = StreamID::default();
            for _ in 0..1e+6 as u64 {
                bid = bid.next();
            }

            assert_eq!("stream-000000000000000015066065", bid.to_string());

            let _bid: StreamID = StreamID::from("stream-000000000000000015066065");
            assert_eq!(bid.to_string(), _bid.to_string());
        }
    }
}
