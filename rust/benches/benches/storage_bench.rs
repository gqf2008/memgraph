use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mgcore::delta::IsolationLevel;
use mgcore::property_value::PropertyValue;
use mgcore::types::Gid;
use mgstorage::storage::Storage;
use std::sync::{Arc, Barrier};
use std::thread;

// ─── Concurrent Write Throughput ───────────────────────────────────────

fn bench_concurrent_write_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_write");
    group.sample_size(20);

    for n_threads in [2, 4, 8].iter() {
        group.bench_function(format!("{}_threads_100_writes", n_threads), |b| {
            b.iter(|| {
                let storage = Arc::new(Storage::new());
                let barrier = Arc::new(Barrier::new(*n_threads));
                let mut handles = Vec::new();

                for t in 0..*n_threads {
                    let s = storage.clone();
                    let bar = barrier.clone();
                    handles.push(thread::spawn(move || {
                        bar.wait();
                        for i in 0..100 {
                            let tx = s.begin_transaction(IsolationLevel::SnapshotIsolation);
                            let gid = Gid::from((t * 10000 + i) as u64 + 1);
                            black_box(s.create_vertex(&tx, gid).ok());
                            s.commit_transaction(&tx);
                        }
                    }));
                }

                for h in handles {
                    h.join().unwrap();
                }
                black_box(storage.all_vertices().len());
            })
        });
    }
    group.finish();
}

// ─── Delta Chain Depth Impact ──────────────────────────────────────────

fn bench_delta_chain_depth(c: &mut Criterion) {
    let mut group = c.benchmark_group("delta_depth");
    group.sample_size(30);

    for depth in [1, 10, 50, 100, 500].iter() {
        group.bench_function(format!("depth_{}", depth), |b| {
            let storage = Storage::new();
            let gid = Gid::from(1u64);
            let prop = mgcore::types::PropertyId::from(0u32);

            // Create vertex
            let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
            storage.create_vertex(&tx, gid).unwrap();
            storage.commit_transaction(&tx);

            // Build delta chain
            for i in 0..*depth {
                let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
                storage
                    .vertex_set_property(&tx, gid, prop, PropertyValue::Int(i as i64))
                    .unwrap();
                storage.commit_transaction(&tx);
            }

            b.iter(|| {
                let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
                black_box(storage.get_vertex(gid, &tx));
            })
        });
    }
    group.finish();
}

// ─── Bulk Create vs Individual ─────────────────────────────────────────

fn bench_bulk_create(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_create");
    group.sample_size(20);

    for n in [100, 1000, 10000].iter() {
        group.bench_function(format!("individual_{}", n), |b| {
            b.iter(|| {
                let storage = Storage::new();
                for i in 0..*n {
                    let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
                    storage.create_vertex(&tx, Gid::from(i as u64 + 1)).unwrap();
                    storage.commit_transaction(&tx);
                }
                black_box(storage.all_vertices().len());
            })
        });

        group.bench_function(format!("single_tx_{}", n), |b| {
            b.iter(|| {
                let storage = Storage::new();
                let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
                for i in 0..*n {
                    storage.create_vertex(&tx, Gid::from(i as u64 + 1)).unwrap();
                }
                storage.commit_transaction(&tx);
                black_box(storage.all_vertices().len());
            })
        });
    }
    group.finish();
}

// ─── Read: Index vs Full Scan ──────────────────────────────────────────

fn bench_index_vs_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_vs_scan");
    group.sample_size(30);

    for n_vertices in [100, 1000, 10000].iter() {
        // Setup: create vertices with label and indexed property
        let label = mgcore::types::LabelId::from(1u32);
        let prop = mgcore::types::PropertyId::from(0u32);
        let prop_score = mgcore::types::PropertyId::from(1u32);

        let storage = Storage::new();
        storage.create_label_index(label);
        storage.create_label_property_index(label, prop_score);

        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        for i in 0..*n_vertices {
            let gid = Gid::from(i as u64 + 1);
            storage.create_vertex(&tx, gid).unwrap();
            storage.vertex_add_label(&tx, gid, label).unwrap();
            storage
                .vertex_set_property(&tx, gid, prop, PropertyValue::Int(i as i64))
                .unwrap();
            storage
                .vertex_set_property(&tx, gid, prop_score, PropertyValue::Int((i % 100) as i64))
                .unwrap();
        }
        storage.commit_transaction(&tx);

        group.bench_function(format!("label_scan_{}", n_vertices), |b| {
            b.iter(|| {
                black_box(storage.vertices_by_label(label).len());
            })
        });

        group.bench_function(format!("label_property_scan_{}", n_vertices), |b| {
            b.iter(|| {
                black_box(
                    storage
                        .vertices_by_label_property(label, prop_score, &PropertyValue::Int(42))
                        .len(),
                );
            })
        });

        group.bench_function(format!("full_scan_{}", n_vertices), |b| {
            b.iter(|| {
                black_box(storage.all_vertices().len());
            })
        });
    }
    group.finish();
}

// ─── Mixed Read-Write Throughput ───────────────────────────────────────

fn bench_mixed_read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_rw");
    group.sample_size(15);

    for n_threads in [2, 4, 6].iter() {
        group.bench_function(format!("{}_threads", n_threads), |b| {
            b.iter(|| {
                let storage = Arc::new(Storage::new());
                let n_readers = n_threads / 2;
                let n_writers = n_threads - n_readers;
                let barrier = Arc::new(Barrier::new(*n_threads));

                // Pre-populate with data
                let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
                for i in 0..500u64 {
                    storage.create_vertex(&tx, Gid::from(i + 1)).unwrap();
                }
                storage.commit_transaction(&tx);

                let mut handles = Vec::new();

                // Writers
                for t in 0..n_writers {
                    let s = storage.clone();
                    let bar = barrier.clone();
                    handles.push(thread::spawn(move || {
                        bar.wait();
                        for i in 0..50 {
                            let tx = s.begin_transaction(IsolationLevel::SnapshotIsolation);
                            let gid = Gid::from(500 + (t * 100 + i) as u64 + 1);
                            black_box(s.create_vertex(&tx, gid).ok());
                            s.commit_transaction(&tx);
                        }
                    }));
                }

                // Readers
                for _ in 0..n_readers {
                    let s = storage.clone();
                    let bar = barrier.clone();
                    handles.push(thread::spawn(move || {
                        bar.wait();
                        for _ in 0..200 {
                            let _tx = s.begin_transaction(IsolationLevel::SnapshotIsolation);
                            black_box(s.all_vertices().len());
                        }
                    }));
                }

                for h in handles {
                    h.join().unwrap();
                }
            })
        });
    }
    group.finish();
}

// ─── Hot Path Read Benchmarks ──────────────────────────────────────────

fn bench_hot_path_get_vertex(c: &mut Criterion) {
    let mut group = c.benchmark_group("hot_path_vertex");
    group.sample_size(30);

    // Setup: create 1000 vertices, no active transactions
    let storage = Storage::new();
    let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    for i in 0..1000u64 {
        storage.create_vertex(&tx, Gid::from(i + 1)).unwrap();
    }
    storage.commit_transaction(&tx);

    group.bench_function("get_vertex_no_active_tx", |b| {
        b.iter(|| {
            for i in 0..100u64 {
                black_box(storage.get_vertex(Gid::from(i + 1), &tx));
            }
        })
    });

    group.finish();
}

fn bench_hot_path_get_edge(c: &mut Criterion) {
    let mut group = c.benchmark_group("hot_path_edge");
    group.sample_size(30);

    // Setup: create vertices and edges between them
    let storage = Storage::new();
    let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    for i in 0..100u64 {
        storage.create_vertex(&tx, Gid::from(i + 1)).unwrap();
    }
    for i in 0..99u64 {
        storage
            .create_edge(
                &tx,
                Gid::from(1000 + i + 1),
                Gid::from(i + 1),
                Gid::from(i + 2),
                mgcore::types::EdgeTypeId::from(1u32),
            )
            .unwrap();
    }
    storage.commit_transaction(&tx);

    group.bench_function("get_edge_no_active_tx", |b| {
        b.iter(|| {
            for i in 0..50u64 {
                black_box(storage.get_edge(Gid::from(1000 + i + 1), &tx));
            }
        })
    });

    group.finish();
}

fn bench_vertex_snapshot_cache(c: &mut Criterion) {
    let mut group = c.benchmark_group("vertex_snapshot_cache");
    group.sample_size(30);

    let storage = Storage::new();
    let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    for i in 0..500u64 {
        storage.create_vertex(&tx, Gid::from(i + 1)).unwrap();
    }
    storage.commit_transaction(&tx);

    // Warm cache
    for i in 0..500u64 {
        let _ = storage.get_vertex(Gid::from(i + 1), &tx);
    }

    group.bench_function("cached_reads_500", |b| {
        b.iter(|| {
            for i in 0..500u64 {
                black_box(storage.get_vertex(Gid::from(i + 1), &tx));
            }
        })
    });

    group.finish();
}

criterion_group!(
    storage_benches,
    bench_concurrent_write_throughput,
    bench_delta_chain_depth,
    bench_bulk_create,
    bench_index_vs_scan,
    bench_mixed_read_write,
    bench_hot_path_get_vertex,
    bench_hot_path_get_edge,
    bench_vertex_snapshot_cache,
);
criterion_main!(storage_benches);
