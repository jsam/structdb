pub mod database;
pub mod errors;
pub mod id;
pub mod iterators;
pub mod record;
pub mod serialization;
pub mod snapshot;
pub mod wals;
pub mod timestamped;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
