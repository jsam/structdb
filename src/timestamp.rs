use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn epoch_ns() -> u128 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(time) => time.as_nanos(),
        Err(_) => panic!("Unable to determine time."),
    }
}

pub fn epoch_secs() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(time) => time.as_secs(),
        Err(_) => panic!("Unable to determine time."),
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq)]
pub struct Timestamp {
    pub created_at: u64,
    pub updated_at: u64,
}

impl Timestamp {
    pub fn new() -> Self {
        Self {
            created_at: epoch_secs(),
            updated_at: epoch_secs(),
        }
    }

    pub fn hash(&self, name: &str) -> u64 {
        let mut hasher = DefaultHasher::new();

        let unique = (name, self.created_at);
        unique.hash(&mut hasher);

        hasher.finish()
    }

    pub fn update_now(&mut self) {
        self.updated_at = epoch_secs();
    }
}

impl Default for Timestamp {
    fn default() -> Self {
        Self::new()
    }
}
