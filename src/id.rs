use std::mem;

use serde::{Deserialize, Serialize};

pub const IDENTIFIER_SIZE: usize = mem::size_of::<u128>();

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

        Self { id, valid: true }
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
        let mut parts = key.split('-');
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

        let _bytes = id_part.unwrap();
        let byte_count = _bytes.len() / 3;
        let _aligned = match byte_count.cmp(&IDENTIFIER_SIZE) {
            std::cmp::Ordering::Less => {
                let prefix_size = IDENTIFIER_SIZE - byte_count;
                let result = format!(
                    "{0}{1}",
                    String::from_utf8(vec![b'0'; prefix_size * 3]).unwrap(),
                    _bytes
                );
                result
            }
            std::cmp::Ordering::Equal => _bytes.to_string(),
            std::cmp::Ordering::Greater => {
                let start_idx = _bytes.len() - (IDENTIFIER_SIZE * 3);
                let sl = _bytes.as_bytes().to_owned();
                let slice = &sl[start_idx..];
                String::from_utf8(slice.to_vec()).unwrap()
            }
        };

        let __bytes = _aligned
            .chars()
            .collect::<Vec<char>>()
            .chunks(3)
            .map(|c| c.iter().collect::<String>().parse::<u8>().unwrap())
            .collect::<Vec<u8>>();

        let mut mem_id = [0x0_u8; IDENTIFIER_SIZE];
        mem_id.clone_from_slice(__bytes.as_ref());

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

    pub fn to_u128(&self) -> u128 {
        let mut result: u128 = 0;
        for byte in self.id.iter().rev() {
            let deref = *byte as u128;
            if deref == 0 {
                break;
            }
            if result == 0 {
                result = deref;
                continue;
            } else {
                result *= deref;
            }
        }

        result
    }

    pub fn distance(&self, other: &StreamID) -> u128 {
        let lhs = self.to_u128();
        let rhs = other.to_u128();

        if lhs < rhs {
            return 0;
        }

        lhs - rhs
    }
}

#[cfg(test)]
mod tests {
    use crate::id::{StreamID, IDENTIFIER_SIZE};

    #[test]
    fn test_stream_id() {
        {
            assert_eq!(IDENTIFIER_SIZE, 16);
        }

        {
            assert_eq!(
                StreamID::default().id,
                [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
            );
        }

        {
            let mut bid = StreamID::default();

            let next_bid = bid.next();
            assert_eq!(
                next_bid.id,
                [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]
            );

            let next_next_bid = next_bid.next();

            assert_eq!(
                next_bid.id,
                [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]
            );
            assert_eq!(
                next_next_bid.id,
                [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3]
            );

            for _ in 0..1e+6 as u64 {
                bid = bid.next();
            }

            assert_eq!(bid.id, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 66, 65]);
        }

        {
            let mut bid = StreamID::default();
            for _ in 0..1e+6 as u64 {
                bid = bid.next();
            }

            assert_eq!(
                "stream-000000000000000000000000000000000000000015066065",
                bid.to_string()
            );

            let _bid: StreamID =
                StreamID::from("stream-000000000000000000000000000000000000000015066065");
            assert_eq!(bid.to_string(), _bid.to_string());
        }
    }

    #[test]
    fn test_distance() {
        {
            let default = StreamID::default();
            let meta = StreamID::metadata();

            assert_eq!(default.to_u128(), 1);
            assert_eq!(meta.to_u128(), 0);
            assert_eq!(default.distance(&meta), 1);
        }

        {
            let default = StreamID::default();
            let mut default2 = StreamID::default();

            default2 = default2.next();
            default2 = default2.next();
            default2 = default2.next();
            default2 = default2.next();

            assert_eq!(default2.distance(&default), 4);
            assert_eq!(default.distance(&default2), 0);
        }
    }
}
