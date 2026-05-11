use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_parse_simple_match(c: &mut Criterion) {
    c.bench_function("parse_simple_match", |b| {
        b.iter(|| {
            black_box(mgparser::parse_query("MATCH (n) RETURN n").unwrap());
        })
    });
}

fn bench_parse_match_with_where(c: &mut Criterion) {
    let query = "MATCH (n:Person {name: 'Alice'}) WHERE n.age > 30 RETURN n";
    c.bench_function("parse_match_with_where", |b| {
        b.iter(|| {
            black_box(mgparser::parse_query(query).unwrap());
        })
    });
}

fn bench_parse_multi_hop_match(c: &mut Criterion) {
    let query = "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company) RETURN a, b, c";
    c.bench_function("parse_multi_hop_match", |b| {
        b.iter(|| {
            black_box(mgparser::parse_query(query).unwrap());
        })
    });
}

fn bench_parse_create(c: &mut Criterion) {
    let query = "CREATE (n:Person {name: 'Alice', age: 30})";
    c.bench_function("parse_create", |b| {
        b.iter(|| {
            black_box(mgparser::parse_query(query).unwrap());
        })
    });
}

fn bench_parse_aggregate(c: &mut Criterion) {
    let query = "MATCH (n:Person) RETURN COUNT(n) AS cnt, AVG(n.age) AS avg_age";
    c.bench_function("parse_aggregate", |b| {
        b.iter(|| {
            black_box(mgparser::parse_query(query).unwrap());
        })
    });
}

fn bench_parse_exists_subquery(c: &mut Criterion) {
    let query = "MATCH (n:Person) WHERE EXISTS { (n)-[:KNOWS]->(:Person) } RETURN n";
    c.bench_function("parse_exists_subquery", |b| {
        b.iter(|| {
            black_box(mgparser::parse_query(query).unwrap());
        })
    });
}

fn bench_parse_complex_query(c: &mut Criterion) {
    let query = r#"
        MATCH (p:Person)-[:KNOWS*1..3]->(f:Person)
        WHERE p.name = 'Alice' AND f.age > 25
        WITH p, COUNT(f) AS friend_count
        WHERE friend_count > 5
        RETURN p.name, friend_count
        ORDER BY friend_count DESC
        SKIP 10 LIMIT 20
    "#;
    c.bench_function("parse_complex_query", |b| {
        b.iter(|| {
            black_box(mgparser::parse_query(query).unwrap());
        })
    });
}

fn bench_parse_union(c: &mut Criterion) {
    let query = "MATCH (n:Person) RETURN n.name UNION MATCH (n:Company) RETURN n.name";
    c.bench_function("parse_union", |b| {
        b.iter(|| {
            black_box(mgparser::parse_query(query).unwrap());
        })
    });
}

criterion_group!(
    benches,
    bench_parse_simple_match,
    bench_parse_match_with_where,
    bench_parse_multi_hop_match,
    bench_parse_create,
    bench_parse_aggregate,
    bench_parse_exists_subquery,
    bench_parse_complex_query,
    bench_parse_union,
);
criterion_main!(benches);
