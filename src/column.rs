use crate::caches::Caches;

pub trait ColumnFamily {
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
