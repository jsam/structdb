use std::sync::Arc;

use rocksdb::BoundColumnFamily;

use crate::{
    database::Database,
    errors::{Error, Result},
};

/// A snapshot of `Database` with specified column family.
pub struct DatabaseSnapshot<'a> {
    pub cf_name: &'a str,
    pub db: &'a Database,
    pub column_family: Arc<BoundColumnFamily<'a>>,
    pub snapshot: rocksdb::Snapshot<'a>,
}

/// Implementation of `DBSnapshot` type.
impl<'a> DatabaseSnapshot<'a> {
    pub fn new(db: &'a Database, cf_name: &'a str) -> Result<Self> {
        db.raw
            .cf_handle(cf_name)
            .map(|result| {
                return Self {
                    cf_name,
                    db,
                    column_family: result.clone(),
                    snapshot: db.raw.snapshot(),
                };
            })
            .ok_or_else(|| Error::ColumnFamilyNotFound(cf_name.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::database::Database;

    use super::DatabaseSnapshot;

    #[test]
    fn test_db_snapshot_from() {
        let _ = fs::remove_dir_all("test_db_snapshot_from.db");
        let opts = Default::default();
        let descriptors = vec![rocksdb::ColumnFamilyDescriptor::new("test_table", opts)];
        let db = Database::open(
            "test_db_snapshot_from.db",
            &mut rocksdb::Options::default(),
            descriptors,
        )
        .unwrap();
        let cf_name = "stream1";
        let _ = db.create_cf(cf_name);

        let snap = DatabaseSnapshot::new(&db, cf_name).unwrap();
        assert_eq!(snap.cf_name, "stream1");
    }
}
