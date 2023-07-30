extern crate criterion;

use std::fs;

use criterion::{criterion_group, criterion_main, Criterion};
use structdb::{builder::StructDB, caches::Caches, table::Table, topic::Topic};

struct MyTopic;

impl Table for MyTopic {
    const NAME: &'static str = "my-topic";
}

impl Topic for MyTopic {}

pub fn benchmark_wals(c: &mut Criterion) {
    let _ = fs::remove_dir_all("test_topic_write.db");
    let db = StructDB::builder("test_topic_write.db", Caches::default())
        .with_struct::<MyTopic>()
        .build();
    assert!(db.is_ok());

    let db = db.unwrap();
    let mut topic = db.make_topic::<MyTopic>();

    let mut append = c.benchmark_group("append");
    append.significance_level(0.1).sample_size(1000);
    append.bench_function("append", |b| {
        b.iter(|| {
            let data = Vec::from([1; 32]);
            let _ = topic.append(&data);
        })
    });
    append.finish();

    let mut append_10k = c.benchmark_group("append_10k");
    append_10k.significance_level(0.3).sample_size(10);
    append_10k.bench_function("append_10k", |b| {
        b.iter(|| {
            for _ in 0..10_000 {
                let data = Vec::from([1; 32]);
                let _ = topic.append(&data);
            }
        })
    });
    append_10k.finish();

    let mut append_100k = c.benchmark_group("append_100k");
    append_100k.significance_level(0.6).sample_size(10);
    append_100k.bench_function("append_100k", |b| {
        b.iter(|| {
            for _ in 0..100_000 {
                let data = Vec::from([1; 32]);
                let _ = topic.append(&data);
            }
        })
    });
    append_100k.finish();

    // c.bench_function("append_500k", |b| {
    //     b.iter(|| {
    //         for _ in 0..500_000 {
    //             let data = Vec::from([1; 32]);
    //             let _ = topic.append(&data);
    //         }
    //     })
    // });

    // c.bench_function("append_1M", |b| {
    //     b.iter(|| {
    //         for _ in 0..1_000_000 {
    //             let _ = stream.append(&[1; 32]);
    //         }
    //     })
    // });
}

criterion_group!(benches, benchmark_wals);
criterion_main!(benches);
