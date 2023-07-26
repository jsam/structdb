extern crate criterion;

use criterion::{criterion_group, criterion_main, Criterion};
use structdb::{
    database::{DBOptions, Database},
    wals::WALS,
};

pub fn benchmark_wals(c: &mut Criterion) {
    let db = Database::open("benchmark_wals.db", &DBOptions::default()).unwrap();
    let mut stream = WALS::new("data-stream", &db).unwrap();

    c.bench_function("append_10k", |b| {
        b.iter(|| {
            for _ in 0..10_000 {
                let _ = stream.append(&[1; 32]);
            }
        })
    });

    c.bench_function("append_100k", |b| {
        b.iter(|| {
            for _ in 0..100_000 {
                let _ = stream.append(&[1; 32]);
            }
        })
    });

    c.bench_function("append_500k", |b| {
        b.iter(|| {
            for _ in 0..500_000 {
                let _ = stream.append(&[1; 32]);
            }
        })
    });

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
