# WAL-S

Implementation of write-ahead log with full storage support.


# Features

- Write-Ahead Log 
- Read snapshots
- Metadata support
- Stateless iterators
- Stateful iterators
- Counter and time based identifiers 
- Distance metrics
- Data exporting


# Getting started

```rust
WALS::new("stream1", db)
    .snapshot()
    .iter()
    .for_each(|record| {
        let stream2 = WALS::from("stream2", db);
        let result = process(record);
        stream2.append(result);
    })
```


# Docs

TBD