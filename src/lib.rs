pub mod database;
pub mod errors;
pub mod id;
pub mod iterators;
pub mod record;
pub mod serialization;
pub mod snapshot;
pub mod timestamped;
pub mod wals;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
