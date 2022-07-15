pub mod database;
pub mod id;
pub mod iterator;
pub mod record;
pub mod serialization;
pub mod snapshot;
pub mod stream;
pub mod timestamped;

type Result<I> = std::result::Result<I, String>;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
