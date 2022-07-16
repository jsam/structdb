use crate::errors::Result;
use serde::{Deserialize, Serialize};

pub trait BinCode {
    fn to_byte_vec(&self) -> Result<Vec<u8>>
    where
        Self: Serialize,
    {
        match bincode::serialize(&self) {
            Ok(result) => Ok(result),
            Err(err) => Err(err.to_string()),
        }
    }

    fn from_byte_vec<'de>(encoded: &'de [u8]) -> Result<Self>
    where
        Self: Deserialize<'de>,
    {
        match bincode::deserialize(encoded) {
            Ok(result) => Ok(result),
            Err(err) => Err(err.to_string()),
        }
    }
}
