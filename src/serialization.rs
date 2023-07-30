use crate::errors::{Error, Result};
use serde::{Deserialize, Serialize};
use vlseqid::id::BigID;

pub trait BinCode {
    fn to_bytes(&self) -> Result<Vec<u8>>
    where
        Self: Serialize,
    {
        match bincode::serialize(&self) {
            Ok(result) => Ok(result),
            Err(err) => Err(Error::SerializationFailed(err.to_string())),
        }
    }

    fn from_bytes<'de>(encoded: &'de [u8]) -> Result<Self>
    where
        Self: Deserialize<'de>,
    {
        match bincode::deserialize(encoded) {
            Ok(result) => Ok(result),
            Err(err) => Err(Error::DeserializationFailed(err.to_string())),
        }
    }
}

impl BinCode for &str {}

impl BinCode for String {}

impl BinCode for BigID {}

#[cfg(test)]
mod tests {
    use crate::serialization::BinCode;

    #[test]
    fn test_string() {
        let value = "test".to_string();
        let encoded = value.to_bytes().unwrap();
        let decoded = String::from_bytes(&encoded).unwrap();

        //println!("{:?} == {:?}", value, decoded);
        assert_eq!(value, decoded);
    }
}
