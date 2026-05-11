use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mgvector::{Distance, HnswConfig, HnswIndex};

// Seeded pseudo-random number generator for reproducible benchmarks.
struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        // xorshift64*
        self.state ^= self.state >> 12;
        self.state ^= self.state << 25;
        self.state ^= self.state >> 27;
        self.state.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn next_f32(&mut self) -> f32 {
        // Generate a value in [0.0, 1.0)
        (self.next_u64() as f32) / (u64::MAX as f32 + 1.0)
    }
}

fn random_vector(rng: &mut Rng, dim: usize) -> Vec<f32> {
    (0..dim).map(|_| rng.next_f32()).collect()
}

// ─── HNSW Insert Benchmarks ──────────────────────────────────────────────

fn bench_hnsw_insert_1k(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_insert_1k");
    group.sample_size(30);

    group.bench_function("insert_1000_cosine_128d", |b| {
        let mut rng = Rng::new(42);
        let vectors: Vec<Vec<f32>> = (0..1000).map(|_| random_vector(&mut rng, 128)).collect();

        b.iter(|| {
            let mut idx = HnswIndex::new(128, Distance::Cosine, HnswConfig::default());
            for v in &vectors {
                black_box(idx.insert(v));
            }
            black_box(idx.len());
        })
    });

    group.finish();
}

fn bench_hnsw_insert_10k(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_insert_10k");
    group.sample_size(20);

    group.bench_function("insert_10000_cosine_128d", |b| {
        let mut rng = Rng::new(42);
        let vectors: Vec<Vec<f32>> = (0..10_000).map(|_| random_vector(&mut rng, 128)).collect();

        b.iter(|| {
            let mut idx = HnswIndex::new(128, Distance::Cosine, HnswConfig::default());
            for v in &vectors {
                black_box(idx.insert(v));
            }
            black_box(idx.len());
        })
    });

    group.finish();
}

// ─── HNSW Search Benchmarks ──────────────────────────────────────────────

fn bench_hnsw_search_1k(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_search_1k");
    group.sample_size(30);

    // Setup: build index once outside the benchmark loop
    let mut rng = Rng::new(42);
    let vectors: Vec<Vec<f32>> = (0..1000).map(|_| random_vector(&mut rng, 128)).collect();
    let mut idx = HnswIndex::new(128, Distance::Cosine, HnswConfig::default());
    for v in &vectors {
        idx.insert(v);
    }

    // Use a fixed query vector
    let query = random_vector(&mut Rng::new(12345), 128);

    group.bench_function("search_top10_1000_cosine_128d", |b| {
        b.iter(|| {
            black_box(idx.search(&query, 10));
        })
    });

    group.finish();
}

fn bench_hnsw_search_10k(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_search_10k");
    group.sample_size(20);

    let mut rng = Rng::new(42);
    let vectors: Vec<Vec<f32>> = (0..10_000).map(|_| random_vector(&mut rng, 128)).collect();
    let mut idx = HnswIndex::new(128, Distance::Cosine, HnswConfig::default());
    for v in &vectors {
        idx.insert(v);
    }

    let query = random_vector(&mut Rng::new(12345), 128);

    group.bench_function("search_top10_10000_cosine_128d", |b| {
        b.iter(|| {
            black_box(idx.search(&query, 10));
        })
    });

    group.finish();
}

// ─── HNSW Delete Benchmark ───────────────────────────────────────────────

fn bench_hnsw_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_delete");
    group.sample_size(30);

    // Setup: generate vectors and record IDs to delete
    let mut rng = Rng::new(42);
    let vectors: Vec<Vec<f32>> = (0..1000).map(|_| random_vector(&mut rng, 128)).collect();
    let mut idx = HnswIndex::new(128, Distance::Cosine, HnswConfig::default());
    let mut ids = Vec::with_capacity(1000);
    for v in &vectors {
        ids.push(idx.insert(v));
    }

    // IDs to delete (first 100)
    let to_delete: Vec<usize> = ids.iter().take(100).copied().collect();
    let query = random_vector(&mut Rng::new(12345), 128);

    group.bench_function("delete_100_then_search", |b| {
        b.iter(|| {
            // Rebuild the index from scratch each iteration (no Clone on HnswIndex)
            let mut idx = HnswIndex::new(128, Distance::Cosine, HnswConfig::default());
            let mut ids = Vec::with_capacity(1000);
            for v in &vectors {
                ids.push(idx.insert(v));
            }
            let to_delete: Vec<usize> = ids.iter().take(100).copied().collect();
            for id in &to_delete {
                black_box(idx.delete(*id));
            }
            // Verify search still works after deletion
            let results = idx.search(&query, 10);
            black_box(results);
        })
    });

    group.finish();
}

// ─── HNSW Batch Insert Benchmark ─────────────────────────────────────────

fn bench_hnsw_batch_insert_10k(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_batch_insert_10k");
    group.sample_size(20);

    let mut rng = Rng::new(42);
    let vectors: Vec<Vec<f32>> = (0..10_000).map(|_| random_vector(&mut rng, 128)).collect();

    // Benchmark individual inserts
    group.bench_function("individual_insert_10000_cosine_128d", |b| {
        b.iter(|| {
            let mut idx = HnswIndex::new(128, Distance::Cosine, HnswConfig::default());
            for v in &vectors {
                black_box(idx.insert(v));
            }
            black_box(idx.len());
        })
    });

    // Benchmark batch insert
    group.bench_function("batch_insert_10000_cosine_128d", |b| {
        b.iter(|| {
            let mut idx = HnswIndex::new(128, Distance::Cosine, HnswConfig::default());
            let ids = idx.insert_batch(&vectors);
            black_box(ids.len());
        })
    });

    // Benchmark batch insert with caller IDs
    let vectors_with_ids: Vec<(Vec<f32>, usize)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| (v.clone(), i))
        .collect();
    group.bench_function("batch_insert_with_ids_10000_cosine_128d", |b| {
        b.iter(|| {
            let mut idx = HnswIndex::new(128, Distance::Cosine, HnswConfig::default());
            let map = idx.insert_batch_with_ids(&vectors_with_ids);
            black_box(map.len());
        })
    });

    group.finish();
}

criterion_group!(
    vector_benches,
    bench_hnsw_insert_1k,
    bench_hnsw_search_1k,
    bench_hnsw_insert_10k,
    bench_hnsw_search_10k,
    bench_hnsw_delete,
    bench_hnsw_batch_insert_10k,
);
criterion_main!(vector_benches);
