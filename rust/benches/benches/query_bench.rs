use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mgcore::delta::IsolationLevel;
use mgcore::property_value::PropertyValue;
use mgcore::types::Gid;
use mgstorage::storage::Storage;

// ─── Storage micro-benchmarks ────────────────────────────────────────────

fn bench_create_vertex(c: &mut Criterion) {
    c.bench_function("create_vertex", |b| {
        b.iter(|| {
            let storage = Storage::new();
            let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
            let gid = Gid::from(1u64);
            black_box(storage.create_vertex(&tx, gid).unwrap());
            storage.commit_transaction(&tx);
        })
    });
}

fn bench_match_scan_1k(c: &mut Criterion) {
    c.bench_function("match_scan_1000", |b| {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        for i in 0..1000u64 {
            storage.create_vertex(&tx, Gid::from(i)).unwrap();
        }
        storage.commit_transaction(&tx);

        b.iter(|| {
            black_box(storage.all_vertices());
        })
    });
}

fn bench_delta_chain_walk(c: &mut Criterion) {
    c.bench_function("delta_chain_walk_100_updates", |b| {
        let storage = Storage::new();
        let gid = Gid::from(1u64);
        let label = mgcore::types::LabelId::from(1u32);
        let prop = mgcore::types::PropertyId::from(0u32);

        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, gid).unwrap();
        storage.vertex_add_label(&tx, gid, label).unwrap();
        storage.commit_transaction(&tx);

        // Create 100 property updates (delta chain)
        for i in 0..100u64 {
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

fn bench_bulk_create_10k(c: &mut Criterion) {
    c.bench_function("bulk_create_10000", |b| {
        b.iter(|| {
            let storage = Storage::new();
            let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
            for i in 0..10_000u64 {
                black_box(storage.create_vertex(&tx, Gid::from(i)).unwrap());
            }
            storage.commit_transaction(&tx);
        })
    });
}

fn bench_label_scan_1k(c: &mut Criterion) {
    c.bench_function("label_scan_1000", |b| {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let label_person = mgcore::types::LabelId::from(1u32);
        let label_company = mgcore::types::LabelId::from(2u32);
        for i in 0..1000u64 {
            storage.create_vertex(&tx, Gid::from(i)).unwrap();
            if i % 2 == 0 {
                storage
                    .vertex_add_label(&tx, Gid::from(i), label_person)
                    .unwrap();
            } else {
                storage
                    .vertex_add_label(&tx, Gid::from(i), label_company)
                    .unwrap();
            }
        }
        storage.commit_transaction(&tx);

        b.iter(|| {
            black_box(storage.vertices_by_label(label_person));
        })
    });
}

// ─── End-to-end query benchmarks ─────────────────────────────────────────

fn bench_e2e_simple_match(c: &mut Criterion) {
    let storage = Storage::new();
    let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    for i in 0..1000u64 {
        storage.create_vertex(&tx, Gid::from(i)).unwrap();
    }
    storage.commit_transaction(&tx);

    c.bench_function("e2e_simple_match_1000", |b| {
        b.iter(|| {
            black_box(mginterp::execute(&storage, "MATCH (n) RETURN n").unwrap());
        })
    });
}

fn bench_e2e_match_with_where(c: &mut Criterion) {
    let storage = Storage::new();
    let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    let prop_age = mgcore::types::PropertyId::from(0u32);
    for i in 0..1000u64 {
        storage.create_vertex(&tx, Gid::from(i)).unwrap();
        storage
            .vertex_set_property(
                &tx,
                Gid::from(i),
                prop_age,
                PropertyValue::Int((i % 100) as i64),
            )
            .unwrap();
    }
    storage.commit_transaction(&tx);

    c.bench_function("e2e_match_where_1000", |b| {
        b.iter(|| {
            black_box(mginterp::execute(&storage, "MATCH (n) WHERE n.age > 50 RETURN n").unwrap());
        })
    });
}

fn bench_e2e_aggregate(c: &mut Criterion) {
    let storage = Storage::new();
    let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    let label_person = mgcore::types::LabelId::from(1u32);
    let prop_age = mgcore::types::PropertyId::from(0u32);
    for i in 0..1000u64 {
        storage.create_vertex(&tx, Gid::from(i)).unwrap();
        storage
            .vertex_add_label(&tx, Gid::from(i), label_person)
            .unwrap();
        storage
            .vertex_set_property(
                &tx,
                Gid::from(i),
                prop_age,
                PropertyValue::Int((i % 100) as i64),
            )
            .unwrap();
    }
    storage.commit_transaction(&tx);

    c.bench_function("e2e_aggregate_count_1000", |b| {
        b.iter(|| {
            black_box(
                mginterp::execute(
                    &storage,
                    "MATCH (n:Person) RETURN COUNT(n) AS cnt, AVG(n.age) AS avg",
                )
                .unwrap(),
            );
        })
    });
}

fn bench_e2e_create(c: &mut Criterion) {
    c.bench_function("e2e_create", |b| {
        b.iter(|| {
            let storage = Storage::new();
            black_box(
                mginterp::execute(&storage, "CREATE (n:Person {name: 'Alice', age: 30})").unwrap(),
            );
        })
    });
}

fn bench_e2e_shortest_path_2hop(c: &mut Criterion) {
    // Create a chain: 0 -[:KNOWS]-> 1 -[:KNOWS]-> 2 ... -[:KNOWS]-> 99
    let storage = Storage::new();
    let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    let edge_type = mgcore::types::EdgeTypeId::from(1u32);
    for i in 0..100u64 {
        storage.create_vertex(&tx, Gid::from(i)).unwrap();
    }
    for i in 0..99u64 {
        storage
            .create_edge(
                &tx,
                Gid::from(1000 + i),
                Gid::from(i),
                Gid::from(i + 1),
                edge_type,
            )
            .unwrap();
    }
    storage.commit_transaction(&tx);

    c.bench_function("e2e_path_2hop_100", |b| {
        b.iter(|| {
            black_box(
                mginterp::execute(&storage, "MATCH (a)-[:KNOWS*1..3]->(b) RETURN a, b").unwrap(),
            );
        })
    });
}

fn bench_e2e_order_by_limit(c: &mut Criterion) {
    let storage = Storage::new();
    let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    let prop_score = mgcore::types::PropertyId::from(0u32);
    for i in 0..1000u64 {
        storage.create_vertex(&tx, Gid::from(i)).unwrap();
        storage
            .vertex_set_property(
                &tx,
                Gid::from(i),
                prop_score,
                PropertyValue::Int((i * 7 % 1000) as i64),
            )
            .unwrap();
    }
    storage.commit_transaction(&tx);

    c.bench_function("e2e_order_by_limit_1000", |b| {
        b.iter(|| {
            black_box(
                mginterp::execute(
                    &storage,
                    "MATCH (n) RETURN n ORDER BY n.score DESC LIMIT 10",
                )
                .unwrap(),
            );
        })
    });

    c.bench_function("e2e_order_by_no_limit_1000", |b| {
        b.iter(|| {
            black_box(
                mginterp::execute(
                    &storage,
                    "MATCH (n) RETURN n ORDER BY n.score DESC",
                )
                .unwrap(),
            );
        })
    });
}

// ─── Cold-cache e2e benchmarks (cache cleared each iteration) ────────────

fn bench_e2e_simple_match_cold(c: &mut Criterion) {
    let storage = Storage::new();
    let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    for i in 0..1000u64 {
        storage.create_vertex(&tx, Gid::from(i)).unwrap();
    }
    storage.commit_transaction(&tx);

    c.bench_function("e2e_simple_match_1000_cold", |b| {
        b.iter(|| {
            mginterp::clear_query_caches();
            black_box(mginterp::execute(&storage, "MATCH (n) RETURN n").unwrap());
        })
    });
}

fn bench_e2e_match_with_where_cold(c: &mut Criterion) {
    let storage = Storage::new();
    let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    let prop_age = mgcore::types::PropertyId::from(0u32);
    for i in 0..1000u64 {
        storage.create_vertex(&tx, Gid::from(i)).unwrap();
        storage
            .vertex_set_property(
                &tx,
                Gid::from(i),
                prop_age,
                PropertyValue::Int((i % 100) as i64),
            )
            .unwrap();
    }
    storage.commit_transaction(&tx);

    c.bench_function("e2e_match_where_1000_cold", |b| {
        b.iter(|| {
            mginterp::clear_query_caches();
            black_box(
                mginterp::execute(&storage, "MATCH (n) WHERE n.age > 50 RETURN n").unwrap(),
            );
        })
    });
}

fn bench_e2e_aggregate_cold(c: &mut Criterion) {
    let storage = Storage::new();
    let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    let label_person = mgcore::types::LabelId::from(1u32);
    let prop_age = mgcore::types::PropertyId::from(0u32);
    for i in 0..1000u64 {
        storage.create_vertex(&tx, Gid::from(i)).unwrap();
        storage
            .vertex_add_label(&tx, Gid::from(i), label_person)
            .unwrap();
        storage
            .vertex_set_property(
                &tx,
                Gid::from(i),
                prop_age,
                PropertyValue::Int((i % 100) as i64),
            )
            .unwrap();
    }
    storage.commit_transaction(&tx);

    c.bench_function("e2e_aggregate_count_1000_cold", |b| {
        b.iter(|| {
            mginterp::clear_query_caches();
            black_box(
                mginterp::execute(
                    &storage,
                    "MATCH (n:Person) RETURN COUNT(n) AS cnt, AVG(n.age) AS avg",
                )
                .unwrap(),
            );
        })
    });
}

criterion_group!(
    benches,
    bench_create_vertex,
    bench_match_scan_1k,
    bench_delta_chain_walk,
    bench_bulk_create_10k,
    bench_label_scan_1k,
    bench_e2e_simple_match,
    bench_e2e_match_with_where,
    bench_e2e_aggregate,
    bench_e2e_create,
    bench_e2e_shortest_path_2hop,
    bench_e2e_order_by_limit,
    bench_e2e_simple_match_cold,
    bench_e2e_match_with_where_cold,
    bench_e2e_aggregate_cold,
);
criterion_main!(benches);
