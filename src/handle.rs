use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use crate::{
    builder::{DefaultVersionProvider, Migration, Semver, StructDB, VersionProvider},
    errors::Error,
};

/// Column family handle which is bounded to the DB instance.
#[derive(Copy, Clone)]
pub struct BoundedCfHandle<'a> {
    inner: *mut librocksdb_sys::rocksdb_column_family_handle_t,
    _lifetime: PhantomData<&'a ()>,
}

impl<'a> BoundedCfHandle<'a> {
    pub fn new(inner: *mut librocksdb_sys::rocksdb_column_family_handle_t) -> Self {
        Self {
            inner,
            _lifetime: PhantomData,
        }
    }
}

impl rocksdb::AsColumnFamilyRef for BoundedCfHandle<'_> {
    #[inline]
    fn inner(&self) -> *mut librocksdb_sys::rocksdb_column_family_handle_t {
        self.inner
    }
}

unsafe impl Send for BoundedCfHandle<'_> {}

/// Column family handle which could be moved into different thread.
#[derive(Clone)]
pub struct UnboundedCfHandle {
    inner: *mut librocksdb_sys::rocksdb_column_family_handle_t,
    _db: Arc<rocksdb::DB>,
}

impl UnboundedCfHandle {
    pub fn new(
        inner: *mut librocksdb_sys::rocksdb_column_family_handle_t,
        db: Arc<rocksdb::DB>,
    ) -> Self {
        Self { inner, _db: db }
    }

    #[inline]
    pub fn bound(&self) -> BoundedCfHandle<'_> {
        BoundedCfHandle {
            inner: self.inner,
            _lifetime: PhantomData,
        }
    }
}

unsafe impl Send for UnboundedCfHandle {}
unsafe impl Sync for UnboundedCfHandle {}

#[derive(Copy, Clone)]
#[repr(transparent)]
pub struct CfHandle(pub *mut librocksdb_sys::rocksdb_column_family_handle_t);

impl rocksdb::AsColumnFamilyRef for CfHandle {
    #[inline]
    fn inner(&self) -> *mut librocksdb_sys::rocksdb_column_family_handle_t {
        self.0
    }
}

unsafe impl Send for CfHandle {}
unsafe impl Sync for CfHandle {}

/// Migrations collection up to the target version.
pub struct Migrations<P> {
    target_version: Semver,
    migrations: HashMap<Semver, Migration>,
    version_provider: P,
}

impl Migrations<DefaultVersionProvider> {
    /// Creates a migrations collection up to the specified version
    /// with the default version provider.
    pub fn with_target_version(target_version: Semver) -> Self {
        Self {
            target_version,
            migrations: Default::default(),
            version_provider: DefaultVersionProvider,
        }
    }
}

impl<P: VersionProvider> Migrations<P> {
    /// Creates a migrations collection up to the specified version
    /// with the specified version provider.
    pub fn with_target_version_and_provider(target_version: Semver, version_provider: P) -> Self {
        Self {
            target_version,
            migrations: Default::default(),
            version_provider,
        }
    }

    /// Registers a new migration.
    pub fn register<F>(&mut self, from: Semver, to: Semver, migration: F) -> Result<(), Error>
    where
        F: Fn(&StructDB) -> Result<(), Error> + 'static,
    {
        use std::collections::hash_map;

        match self.migrations.entry(from) {
            hash_map::Entry::Vacant(entry) => {
                entry.insert(Box::new(move |db| {
                    migration(db)?;
                    Ok(to)
                }));
                Ok(())
            }
            hash_map::Entry::Occupied(entry) => Err(Error::DuplicateMigration(*entry.key())),
        }
    }
}
