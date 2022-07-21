# WAL-S

Implementation of write-ahead log based storage.


# Features

- Write-Ahead Log 
- Read snapshots
- Internal write batching
- Metadata support
- Stateless iterators
- Stateful iterators
- Sliding window
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

```rust
 stream
    .flush()
    .snapshot()
    .unwrap()
    .window(10, &StreamID::default())
    .for_each(|batch| {
        println!("size={}", batch.len());
        assert_eq!(batch.len(), 10);
        counter += 1;
    });
```


# Docs

TBD