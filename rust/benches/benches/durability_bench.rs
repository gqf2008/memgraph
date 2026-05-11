use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mgcore::property_value::PropertyValue;
use mgcore::types::{EdgeTypeId, Gid, LabelId, PropertyId};
use mgdurability::delta_record::DeltaRecord;
use mgdurability::snapshot::{EdgeSnapshotEntry, NameMapperSnapshot, VertexSnapshotEntry};
use mgdurability::{SnapshotData, SnapshotReader, SnapshotWriter, WalReader, WalWriter};
use std::fs;

fn temp_path(name: &str) -> String {
    let pid = std::process::id();
    let tid = std::thread::current().id();
    format!("/tmp/mg_bench_{}_{:?}_{}", pid, tid, name)
}

fn cleanup(path: &str) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_dir_all(path);
}

fn empty_name_mapper() -> NameMapperSnapshot {
    NameMapperSnapshot {
        labels: vec![],
        properties: vec![],
        edge_types: vec![],
    }
}

// ─── WAL Write Benchmarks ────────────────────────────────────────────────

fn bench_wal_append_vertex_create(c: &mut Criterion) {
    let path = temp_path("wal_append");
    cleanup(&path);

    let record = DeltaRecord::VertexCreate {
        gid: Gid::from(42u64),
        timestamp: 1_000_000,
    };

    c.bench_function("wal_append_vertex_create", |b| {
        let mut writer = WalWriter::create(&path).unwrap();
        b.iter(|| {
            black_box(writer.append_record(&record).unwrap());
        });
        cleanup(&path);
    });
}

fn bench_wal_append_100_records(c: &mut Criterion) {
    let path = temp_path("wal_100");
    cleanup(&path);

    let records: Vec<DeltaRecord> = (0..100)
        .map(|i| DeltaRecord::VertexCreate {
            gid: Gid::from(i as u64),
            timestamp: i as u64 * 1_000,
        })
        .collect();

    c.bench_function("wal_append_100_records", |b| {
        b.iter(|| {
            cleanup(&path);
            let mut writer = WalWriter::create(&path).unwrap();
            for rec in &records {
                writer.append_record(rec).unwrap();
            }
            black_box(writer.records_written());
        });
    });
}

fn bench_wal_append_1000_records(c: &mut Criterion) {
    let path = temp_path("wal_1k");
    cleanup(&path);

    let records: Vec<DeltaRecord> = (0..1000)
        .map(|i| DeltaRecord::VertexSetProperty {
            gid: Gid::from(i as u64),
            key: PropertyId::from(1u32),
            value: PropertyValue::String(format!("value_{}", i)),
        })
        .collect();

    c.bench_function("wal_append_1000_records", |b| {
        b.iter(|| {
            cleanup(&path);
            let mut writer = WalWriter::create(&path).unwrap();
            for rec in &records {
                writer.append_record(rec).unwrap();
            }
            black_box(writer.records_written());
        });
    });
}

fn bench_wal_sync_latency(c: &mut Criterion) {
    let path = temp_path("wal_sync");
    cleanup(&path);

    let mut writer = WalWriter::create(&path).unwrap();
    for i in 0..100 {
        writer
            .append_record(&DeltaRecord::VertexCreate {
                gid: Gid::from(i as u64),
                timestamp: i as u64,
            })
            .unwrap();
    }

    c.bench_function("wal_sync_100_records", |b| {
        b.iter(|| {
            black_box(writer.sync().unwrap());
        });
    });

    cleanup(&path);
}

// ─── WAL Read Benchmarks ─────────────────────────────────────────────────

fn bench_wal_read_1000_records(c: &mut Criterion) {
    let path = temp_path("wal_read_1k");
    cleanup(&path);

    {
        let mut writer = WalWriter::create(&path).unwrap();
        for i in 0..1000 {
            writer
                .append_record(&DeltaRecord::VertexSetProperty {
                    gid: Gid::from(i as u64),
                    key: PropertyId::from(1u32),
                    value: PropertyValue::Int(i as i64),
                })
                .unwrap();
        }
        writer.sync().unwrap();
    }

    c.bench_function("wal_read_1000_records", |b| {
        b.iter(|| {
            let reader = WalReader::open(&path).unwrap();
            black_box(reader.len());
        });
    });

    cleanup(&path);
}

// ─── Snapshot Write/Read Benchmarks ──────────────────────────────────────

fn bench_snapshot_write_small(c: &mut Criterion) {
    let path = temp_path("snap_small");
    cleanup(&path);

    let data = SnapshotData {
        name_mapper: empty_name_mapper(),
        vertices: (0..100)
            .map(|i| VertexSnapshotEntry {
                gid: Gid::from(i as u64),
                labels: vec![LabelId::from(1u32)],
                properties: vec![(PropertyId::from(1u32), PropertyValue::Int(i as i64))],
            })
            .collect(),
        edges: (0..50)
            .map(|i| EdgeSnapshotEntry {
                gid: Gid::from(1000 + i as u64),
                from_vertex: Gid::from(i as u64),
                to_vertex: Gid::from((i + 1) as u64),
                edge_type: EdgeTypeId::from(1u32),
                properties: vec![(PropertyId::from(2u32), PropertyValue::String("KNOWS".into()))],
            })
            .collect(),
    };

    c.bench_function("snapshot_write_100v_50e", |b| {
        b.iter(|| {
            cleanup(&path);
            SnapshotWriter::write(&path, &data).unwrap();
        });
    });

    cleanup(&path);
}

fn bench_snapshot_write_large(c: &mut Criterion) {
    let path = temp_path("snap_large");
    cleanup(&path);

    let data = SnapshotData {
        name_mapper: empty_name_mapper(),
        vertices: (0..10000)
            .map(|i| VertexSnapshotEntry {
                gid: Gid::from(i as u64),
                labels: vec![LabelId::from(1u32), LabelId::from(2u32)],
                properties: vec![
                    (PropertyId::from(1u32), PropertyValue::Int(i as i64)),
                    (
                        PropertyId::from(2u32),
                        PropertyValue::String(format!("name_{}", i)),
                    ),
                ],
            })
            .collect(),
        edges: (0..5000)
            .map(|i| EdgeSnapshotEntry {
                gid: Gid::from(100000 + i as u64),
                from_vertex: Gid::from(i as u64),
                to_vertex: Gid::from((i + 1) as u64),
                edge_type: EdgeTypeId::from(1u32),
                properties: vec![(PropertyId::from(3u32), PropertyValue::Bool(true))],
            })
            .collect(),
    };

    c.bench_function("snapshot_write_10kv_5ke", |b| {
        b.iter(|| {
            cleanup(&path);
            SnapshotWriter::write(&path, &data).unwrap();
        });
    });

    cleanup(&path);
}

fn bench_snapshot_roundtrip(c: &mut Criterion) {
    let path = temp_path("snap_roundtrip");
    cleanup(&path);

    let data = SnapshotData {
        name_mapper: empty_name_mapper(),
        vertices: (0..1000)
            .map(|i| VertexSnapshotEntry {
                gid: Gid::from(i as u64),
                labels: vec![LabelId::from(1u32)],
                properties: vec![(PropertyId::from(1u32), PropertyValue::Int(i as i64))],
            })
            .collect(),
        edges: (0..500)
            .map(|i| EdgeSnapshotEntry {
                gid: Gid::from(10000 + i as u64),
                from_vertex: Gid::from(i as u64),
                to_vertex: Gid::from((i + 1) as u64),
                edge_type: EdgeTypeId::from(1u32),
                properties: vec![],
            })
            .collect(),
    };

    SnapshotWriter::write(&path, &data).unwrap();

    c.bench_function("snapshot_read_1kv_500e", |b| {
        b.iter(|| {
            let read_back = SnapshotReader::read(&path).unwrap();
            black_box(read_back.vertices.len());
        });
    });

    cleanup(&path);
}

// ─── SLK Serialization Benchmarks ────────────────────────────────────────

fn bench_slk_serialize_property_value(c: &mut Criterion) {
    use mgcore::property_value::PropertyValue;
    use mgslk::{Builder, BuilderCollector, SlkSave};

    let values: Vec<PropertyValue> = vec![
        PropertyValue::Null,
        PropertyValue::Bool(true),
        PropertyValue::Int(42),
        PropertyValue::Double(3.14159),
        PropertyValue::String("hello world".into()),
        PropertyValue::List(vec![
            PropertyValue::Int(1),
            PropertyValue::Int(2),
            PropertyValue::Int(3),
        ]),
    ];

    c.bench_function("slk_serialize_property_value_mixed", |b| {
        b.iter(|| {
            for val in &values {
                let (mut builder, collector): (Builder, BuilderCollector) =
                    Builder::new_collecting();
                val.slk_save(&mut builder);
                builder.finalize();
                black_box(collector.into_vec());
            }
        });
    });
}

criterion_group!(
    durability_benches,
    bench_wal_append_vertex_create,
    bench_wal_append_100_records,
    bench_wal_append_1000_records,
    bench_wal_sync_latency,
    bench_wal_read_1000_records,
    bench_snapshot_write_small,
    bench_snapshot_write_large,
    bench_snapshot_roundtrip,
    bench_slk_serialize_property_value,
);
criterion_main!(durability_benches);
