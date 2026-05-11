use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mgbolt::decoder::decode_value;
use mgbolt::message::Message;
use mgbolt::value::Value;
use std::collections::HashMap;

// ─── Bolt Encode Benchmarks ──────────────────────────────────────────────

fn bench_bolt_encode_node(c: &mut Criterion) {
    // Build a node as a PropertyValue::Map equivalent using Bolt Value types
    let mut properties = HashMap::new();
    properties.insert("name".into(), Value::String("Alice".into()));
    properties.insert("age".into(), Value::Int(30));
    properties.insert("active".into(), Value::Bool(true));
    properties.insert("score".into(), Value::Float(95.5));

    let node = Value::node(42, vec!["Person".into(), "Employee".into()], properties);

    c.bench_function("bolt_encode_node", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(256);
            black_box(node.encode(&mut buf).unwrap());
            black_box(buf.len());
        })
    });
}

fn bench_bolt_encode_path(c: &mut Criterion) {
    // Build a path with 5 nodes and 4 edges
    let nodes: Vec<Value> = (0..5)
        .map(|i| {
            let mut props = HashMap::new();
            props.insert("id".into(), Value::Int(i));
            Value::node(i, vec!["Node".into()], props)
        })
        .collect();

    let rels: Vec<Value> = (0..4)
        .map(|i| {
            let mut props = HashMap::new();
            props.insert("since".into(), Value::Int(2020 + i));
            Value::unbound_relationship(100 + i, "KNOWS", props)
        })
        .collect();

    let path = Value::path(nodes, rels);

    c.bench_function("bolt_encode_path_5n4e", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(512);
            black_box(path.encode(&mut buf).unwrap());
            black_box(buf.len());
        })
    });
}

fn bench_bolt_decode_record(c: &mut Criterion) {
    // Encode a record message first, then benchmark decoding it
    let record = Message::Record {
        fields: vec![
            Value::Int(42),
            Value::String("hello".into()),
            Value::Bool(true),
            Value::Float(3.14),
        ],
    };
    let value = record.to_value();

    let mut encoded = Vec::new();
    value.encode(&mut encoded).unwrap();

    c.bench_function("bolt_decode_record", |b| {
        b.iter(|| {
            let (decoded, _) = black_box(decode_value(&encoded).unwrap());
            black_box(decoded);
        })
    });
}

fn bench_bolt_encode_int(c: &mut Criterion) {
    let mut group = c.benchmark_group("bolt_encode_int");

    // Small integer (fits in tiny int, single byte)
    let small_int = Value::Int(42);
    group.bench_function("small_int_42", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(8);
            black_box(small_int.encode(&mut buf).unwrap());
            black_box(buf.len());
        })
    });

    // Large integer (requires INT64 marker, 9 bytes)
    let large_int = Value::Int(9_223_372_036_854_775_807i64);
    group.bench_function("large_int_i64_max", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(16);
            black_box(large_int.encode(&mut buf).unwrap());
            black_box(buf.len());
        })
    });

    // Negative large integer
    let neg_large_int = Value::Int(-9_223_372_036_854_775_808i64);
    group.bench_function("large_int_i64_min", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(16);
            black_box(neg_large_int.encode(&mut buf).unwrap());
            black_box(buf.len());
        })
    });

    group.finish();
}

fn bench_bolt_encode_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("bolt_encode_string");

    // Short string (fits in tiny string, <= 15 bytes)
    let short_string = Value::String("hi".into());
    group.bench_function("short_string_2b", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(32);
            black_box(short_string.encode(&mut buf).unwrap());
            black_box(buf.len());
        })
    });

    // Medium string (requires STRING8 marker, <= 255 bytes)
    let medium_string = Value::String("a".repeat(200));
    group.bench_function("medium_string_200b", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(256);
            black_box(medium_string.encode(&mut buf).unwrap());
            black_box(buf.len());
        })
    });

    // Long string (requires STRING16 marker, <= 65535 bytes)
    let long_string = Value::String("x".repeat(5000));
    group.bench_function("long_string_5kb", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(6000);
            black_box(long_string.encode(&mut buf).unwrap());
            black_box(buf.len());
        })
    });

    group.finish();
}

criterion_group!(
    bolt_benches,
    bench_bolt_encode_node,
    bench_bolt_encode_path,
    bench_bolt_decode_record,
    bench_bolt_encode_int,
    bench_bolt_encode_string,
);
criterion_main!(bolt_benches);
