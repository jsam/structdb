use std::{marker::PhantomData, sync::Arc};

use crate::handle::{BoundedCfHandle, CfHandle, UnboundedCfHandle};

use crate::caches::Caches;

pub trait Table {
    const NAME: &'static str;

    fn options(opts: &mut rocksdb::Options, caches: &Caches) {
        let _unused = opts;
        let _unused = caches;
    }

    fn write_options(opts: &mut rocksdb::WriteOptions) {
        let _unused = opts;
    }

    fn read_options(opts: &mut rocksdb::ReadOptions) {
        let _unused = opts;
    }
}

pub struct TableImpl<T> {
    cf: CfHandle,
    db: Arc<rocksdb::DB>,
    write_config: rocksdb::WriteOptions,
    read_config: rocksdb::ReadOptions,
    _ty: PhantomData<T>,
}

impl<T> TableImpl<T>
where
    T: Table,
{
    pub fn new(db: Arc<rocksdb::DB>) -> Self {
        use rocksdb::AsColumnFamilyRef;

        let handle = db.cf_handle(T::NAME).unwrap().inner();
        let cf = CfHandle(handle);

        let mut write_config = Default::default();
        T::write_options(&mut write_config);

        let mut read_config = Default::default();
        T::read_options(&mut read_config);

        Self {
            cf,
            db,
            write_config,
            read_config,
            _ty: Default::default(),
        }
    }

    pub fn cf(&'_ self) -> BoundedCfHandle<'_> {
        BoundedCfHandle::new(self.cf.0)
    }

    pub fn get_unbounded_cf(&self) -> UnboundedCfHandle {
        UnboundedCfHandle::new(self.cf.0, self.db.clone())
    }

    #[inline]
    pub fn db(&self) -> &Arc<rocksdb::DB> {
        &self.db
    }

    #[inline]
    pub fn read_config(&self) -> &rocksdb::ReadOptions {
        &self.read_config
    }

    pub fn new_read_config(&self) -> rocksdb::ReadOptions {
        let mut read_config = Default::default();
        T::read_options(&mut read_config);
        read_config
    }

    #[inline]
    pub fn write_config(&self) -> &rocksdb::WriteOptions {
        &self.write_config
    }

    pub fn new_write_config(&self) -> rocksdb::WriteOptions {
        let mut write_config = Default::default();
        T::write_options(&mut write_config);
        write_config
    }

    #[inline]
    pub fn get<K: AsRef<[u8]>>(
        &self,
        key: K,
    ) -> Result<Option<rocksdb::DBPinnableSlice>, rocksdb::Error> {
        fn db_get<'a>(
            db: &'a rocksdb::DB,
            cf: CfHandle,
            key: &[u8],
            readopts: &rocksdb::ReadOptions,
        ) -> Result<Option<rocksdb::DBPinnableSlice<'a>>, rocksdb::Error> {
            db.get_pinned_cf_opt(&cf, key, readopts)
        }
        db_get(self.db.as_ref(), self.cf, key.as_ref(), &self.read_config)
    }

    #[inline]
    pub fn insert<K, V>(&self, key: K, value: V) -> Result<(), rocksdb::Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        fn db_insert(
            db: &rocksdb::DB,
            cf: CfHandle,
            key: &[u8],
            value: &[u8],
            writeopts: &rocksdb::WriteOptions,
        ) -> Result<(), rocksdb::Error> {
            db.put_cf_opt(&cf, key, value, writeopts)
        }
        db_insert(
            self.db.as_ref(),
            self.cf,
            key.as_ref(),
            value.as_ref(),
            &self.write_config,
        )
    }

    #[allow(unused)]
    #[inline]
    pub fn remove<K: AsRef<[u8]>>(&self, key: K) -> Result<(), rocksdb::Error> {
        fn db_remove(
            db: &rocksdb::DB,
            cf: CfHandle,
            key: &[u8],
            writeopts: &rocksdb::WriteOptions,
        ) -> Result<(), rocksdb::Error> {
            db.delete_cf_opt(&cf, key, writeopts)
        }
        db_remove(self.db.as_ref(), self.cf, key.as_ref(), &self.write_config)
    }

    #[inline]
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> Result<bool, rocksdb::Error> {
        fn db_contains_key(
            db: &rocksdb::DB,
            cf: CfHandle,
            key: &[u8],
            readopts: &rocksdb::ReadOptions,
        ) -> Result<bool, rocksdb::Error> {
            match db.get_pinned_cf_opt(&cf, key, readopts) {
                Ok(value) => Ok(value.is_some()),
                Err(e) => Err(e),
            }
        }
        db_contains_key(self.db.as_ref(), self.cf, key.as_ref(), &self.read_config)
    }

    pub fn iterator(&'_ self, mode: rocksdb::IteratorMode) -> rocksdb::DBIterator<'_> {
        let mut read_config = Default::default();
        T::read_options(&mut read_config);

        self.db.iterator_cf_opt(&self.cf, read_config, mode)
    }

    #[allow(unused)]
    pub fn prefix_iterator<P>(&'_ self, prefix: P) -> rocksdb::DBRawIterator<'_>
    where
        P: AsRef<[u8]>,
    {
        let mut read_config = Default::default();
        T::read_options(&mut read_config);
        read_config.set_prefix_same_as_start(true);

        let mut iter = self.db.raw_iterator_cf_opt(&self.cf, read_config);
        iter.seek(prefix.as_ref());

        iter
    }

    pub fn raw_iterator(&'_ self) -> rocksdb::DBRawIterator<'_> {
        let mut read_config = Default::default();
        T::read_options(&mut read_config);

        self.db.raw_iterator_cf_opt(&self.cf, read_config)
    }
}
