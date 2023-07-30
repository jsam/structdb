use crate::builder::Semver;

pub type RocksResult<I> = std::result::Result<I, rocksdb::Error>;

/// Error type for migration related errors.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("incompatible DB version")]
    IncompatibleDbVersion { version: Semver, expected: Semver },
    #[error("existing DB version not found")]
    VersionNotFound,
    #[error("invalid version")]
    InvalidDbVersion,
    #[error("migration not found: {0:?}")]
    MigrationNotFound(Semver),
    #[error("duplicate migration: {0:?}")]
    DuplicateMigration(Semver),
    #[error("db error")]
    DbError(#[from] rocksdb::Error),
    #[error("column family not found")]
    ColumnFamilyNotFound(String),
    #[error("serialization failed")]
    SerializationFailed(String),
    #[error("deserialization failed")]
    DeserializationFailed(String),
}

pub type Result<I> = std::result::Result<I, Error>;
