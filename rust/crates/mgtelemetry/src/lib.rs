//! Telemetry reporting and metrics collection for Memgraph.
//! Equivalent to C++ `src/telemetry/`.
//!
//! Provides: counters, gauges, histograms, Prometheus export,
//! OpenTelemetry traces, and optional anonymous usage reporting.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mgflags::Flags;

// ─── Core metric types ────────────────────────────────────────────────────

/// A monotonically increasing counter.
#[derive(Debug)]
pub struct Counter {
    value: AtomicU64,
    name: String,
    description: String,
}

impl Counter {
    pub fn new(name: &str, description: &str) -> Self {
        Self {
            value: AtomicU64::new(0),
            name: name.to_string(),
            description: description.to_string(),
        }
    }

    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(&self, n: u64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    pub fn reset(&self) {
        self.value.store(0, Ordering::Relaxed);
    }

    pub fn to_prometheus(&self) -> String {
        format!(
            "# HELP {} {}\n# TYPE {} counter\n{} {}\n",
            self.name, self.description, self.name, self.name, self.get()
        )
    }
}

/// A gauge that can go up and down.
#[derive(Debug)]
pub struct Gauge {
    value: AtomicU64,
    name: String,
    description: String,
}

impl Gauge {
    pub fn new(name: &str, description: &str) -> Self {
        Self {
            value: AtomicU64::new(0),
            name: name.to_string(),
            description: description.to_string(),
        }
    }

    pub fn set(&self, n: u64) {
        self.value.store(n, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    pub fn to_prometheus(&self) -> String {
        format!(
            "# HELP {} {}\n# TYPE {} gauge\n{} {}\n",
            self.name, self.description, self.name, self.name, self.get()
        )
    }
}

/// A histogram with predefined buckets.
#[derive(Debug)]
pub struct Histogram {
    buckets: Vec<(f64, AtomicU64)>,
    sum: AtomicU64,
    count: AtomicU64,
    name: String,
    description: String,
}

impl Histogram {
    pub fn with_buckets(name: &str, description: &str, bucket_bounds: &[f64]) -> Self {
        let mut buckets = Vec::new();
        for &b in bucket_bounds {
            buckets.push((b, AtomicU64::new(0)));
        }
        // Add +Inf bucket
        buckets.push((f64::INFINITY, AtomicU64::new(0)));
        Self {
            buckets,
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
            name: name.to_string(),
            description: description.to_string(),
        }
    }

    pub fn observe(&self, value: f64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value as u64, Ordering::Relaxed);
        for (bound, counter) in &self.buckets {
            if value <= *bound {
                counter.fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
    }

    pub fn to_prometheus(&self) -> String {
        let mut out = format!(
            "# HELP {} {}\n# TYPE {} histogram\n",
            self.name, self.description, self.name
        );
        let mut cumulative = 0u64;
        for (bound, counter) in &self.buckets {
            cumulative += counter.load(Ordering::Relaxed);
            if bound.is_finite() {
                out.push_str(&format!(
                    "{}_bucket{{le=\"{}\"}} {}\n",
                    self.name, bound, cumulative
                ));
            } else {
                out.push_str(&format!(
                    "{}_bucket{{le=\"+Inf\"}} {}\n",
                    self.name, cumulative
                ));
            }
        }
        out.push_str(&format!(
            "{}_sum {}\n{}_count {}\n",
            self.name,
            self.sum.load(Ordering::Relaxed),
            self.name,
            self.count.load(Ordering::Relaxed)
        ));
        out
    }
}

// ─── Metrics registry ─────────────────────────────────────────────────────

/// Central registry for all metrics.
pub struct MetricsRegistry {
    counters: Mutex<HashMap<String, Arc<Counter>>>,
    gauges: Mutex<HashMap<String, Arc<Gauge>>>,
    histograms: Mutex<HashMap<String, Arc<Histogram>>>,
    meters: Mutex<HashMap<String, Arc<Meter>>>,
    summaries: Mutex<HashMap<String, Arc<Summary>>>,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            counters: Mutex::new(HashMap::new()),
            gauges: Mutex::new(HashMap::new()),
            histograms: Mutex::new(HashMap::new()),
            meters: Mutex::new(HashMap::new()),
            summaries: Mutex::new(HashMap::new()),
        }
    }

    pub fn counter(&self, name: &str, description: &str) -> Arc<Counter> {
        let mut counters = self.counters.lock().unwrap();
        counters
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Counter::new(name, description)))
            .clone()
    }

    pub fn gauge(&self, name: &str, description: &str) -> Arc<Gauge> {
        let mut gauges = self.gauges.lock().unwrap();
        gauges
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Gauge::new(name, description)))
            .clone()
    }

    pub fn histogram(&self, name: &str, description: &str, buckets: &[f64]) -> Arc<Histogram> {
        let mut histograms = self.histograms.lock().unwrap();
        histograms
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Histogram::with_buckets(name, description, buckets)))
            .clone()
    }

    pub fn meter(&self, name: &str, description: &str) -> Arc<Meter> {
        let mut meters = self.meters.lock().unwrap();
        meters
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Meter::new(name, description)))
            .clone()
    }

    pub fn summary(&self, name: &str, description: &str, reservoir_size: usize) -> Arc<Summary> {
        let mut summaries = self.summaries.lock().unwrap();
        summaries
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Summary::new(name, description, reservoir_size)))
            .clone()
    }

    /// Export all metrics in Prometheus text format.
    pub fn to_prometheus(&self) -> String {
        let mut out = String::new();
        for (_, c) in self.counters.lock().unwrap().iter() {
            out.push_str(&c.to_prometheus());
        }
        for (_, g) in self.gauges.lock().unwrap().iter() {
            out.push_str(&g.to_prometheus());
        }
        for (_, h) in self.histograms.lock().unwrap().iter() {
            out.push_str(&h.to_prometheus());
        }
        for (_, m) in self.meters.lock().unwrap().iter() {
            out.push_str(&m.to_prometheus());
        }
        for (_, s) in self.summaries.lock().unwrap().iter() {
            out.push_str(&s.to_prometheus());
        }
        out
    }

    pub fn clear(&self) {
        self.counters.lock().unwrap().clear();
        self.gauges.lock().unwrap().clear();
        self.histograms.lock().unwrap().clear();
        self.meters.lock().unwrap().clear();
        self.summaries.lock().unwrap().clear();
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Query performance tracker ────────────────────────────────────────────

/// Tracks query execution times and counts.
pub struct QueryMetrics {
    pub total_queries: Arc<Counter>,
    pub failed_queries: Arc<Counter>,
    pub query_duration: Arc<Histogram>,
    pub active_transactions: Arc<Gauge>,
}

impl QueryMetrics {
    pub fn new(registry: &MetricsRegistry) -> Self {
        Self {
            total_queries: registry.counter("memgraph_queries_total", "Total queries executed"),
            failed_queries: registry.counter("memgraph_queries_failed_total", "Total failed queries"),
            query_duration: registry.histogram(
                "memgraph_query_duration_seconds",
                "Query execution time",
                &[0.001, 0.01, 0.1, 0.5, 1.0, 5.0, 10.0],
            ),
            active_transactions: registry.gauge("memgraph_active_transactions", "Currently active transactions"),
        }
    }

    pub fn record_query(&self, duration_secs: f64, success: bool) {
        self.total_queries.inc();
        self.query_duration.observe(duration_secs);
        if !success {
            self.failed_queries.inc();
        }
    }
}

// ─── Storage metrics ──────────────────────────────────────────────────────

/// Tracks storage-level metrics.
pub struct StorageMetrics {
    pub vertex_count: Arc<Gauge>,
    pub edge_count: Arc<Gauge>,
    pub index_count: Arc<Gauge>,
    pub gc_runs: Arc<Counter>,
    pub deltas_freed: Arc<Counter>,
}

impl StorageMetrics {
    pub fn new(registry: &MetricsRegistry) -> Self {
        Self {
            vertex_count: registry.gauge("memgraph_vertices_total", "Total vertices"),
            edge_count: registry.gauge("memgraph_edges_total", "Total edges"),
            index_count: registry.gauge("memgraph_indices_total", "Total indices"),
            gc_runs: registry.counter("memgraph_gc_runs_total", "Total GC cycles"),
            deltas_freed: registry.counter("memgraph_gc_deltas_freed_total", "Total deltas freed by GC"),
        }
    }

    pub fn update(&self, storage: &mgstorage::storage::Storage) {
        self.vertex_count.set(storage.vertex_count() as u64);
        self.edge_count.set(storage.edge_count() as u64);
        let indices = storage.active_label_indices.read().unwrap().len()
            + storage.active_label_property_indices.read().unwrap().len();
        self.index_count.set(indices as u64);
    }
}

// ─── Telemetry payload ────────────────────────────────────────────────────

/// Telemetry payload sent to the telemetry server.
#[derive(serde::Serialize)]
struct TelemetryPayload {
    pub storage_id: String,
    pub uptime_secs: u64,
    pub vertex_count: usize,
    pub edge_count: usize,
    pub version: String,
    pub os: String,
    pub arch: String,
}

/// Telemetry reporter. Runs in background task if enabled.
pub struct TelemetryReporter {
    flags: Flags,
    client: reqwest::Client,
}

impl TelemetryReporter {
    pub fn new(flags: Flags) -> Self {
        Self {
            flags,
            client: reqwest::Client::new(),
        }
    }

    /// Start the background telemetry reporting task.
    /// Only starts if telemetry is enabled.
    pub fn start_background(&self, storage: Arc<mgstorage::storage::Storage>) {
        if !self.flags.telemetry_enabled {
            tracing::info!("telemetry disabled");
            return;
        }

        let server_url = self.flags.telemetry_server.clone();
        let storage_id = uuid::Uuid::new_v4().to_string();
        let version = env!("CARGO_PKG_VERSION").to_string();

        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let mut interval = tokio::time::interval(Duration::from_secs(3600)); // hourly

            loop {
                interval.tick().await;

                let payload = TelemetryPayload {
                    storage_id: storage_id.clone(),
                    uptime_secs: Self::get_uptime(),
                    vertex_count: storage.vertex_count(),
                    edge_count: storage.edge_count(),
                    version: version.clone(),
                    os: std::env::consts::OS.to_string(),
                    arch: std::env::consts::ARCH.to_string(),
                };

                if let Some(ref url) = server_url {
                    let _ = client.post(url).json(&payload).send().await;
                }
            }
        });
    }

    fn get_uptime() -> u64 {
        static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
        START.get_or_init(std::time::Instant::now).elapsed().as_secs()
    }
}

/// Meter tracks the rate of events over time (events per second).
/// Uses a simple exponentially weighted moving average.
#[derive(Debug)]
pub struct Meter {
    count: AtomicU64,
    start_time: Mutex<Instant>,
    rate: Mutex<f64>, // events per second
    alpha: f64,       // decay factor
    name: String,
    description: String,
}

impl Meter {
    pub fn new(name: &str, description: &str) -> Self {
        Self {
            count: AtomicU64::new(0),
            start_time: Mutex::new(Instant::now()),
            rate: Mutex::new(0.0),
            alpha: 0.015, // 1-minute decay at 5s tick
            name: name.to_string(),
            description: description.to_string(),
        }
    }

    pub fn mark(&self) {
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn mark_n(&self, n: u64) {
        self.count.fetch_add(n, Ordering::Relaxed);
    }

    pub fn get_count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Update the rate estimate. Call periodically (e.g., every 5s).
    pub fn tick(&self) {
        let mut start = self.start_time.lock().unwrap();
        let elapsed = start.elapsed().as_secs_f64();
        if elapsed < 0.001 {
            return;
        }
        let count = self.count.load(Ordering::Relaxed) as f64;
        let instant_rate = count / elapsed;
        let mut rate = self.rate.lock().unwrap();
        *rate = (*rate * (1.0 - self.alpha)) + (instant_rate * self.alpha);
        *start = Instant::now();
        self.count.store(0, Ordering::Relaxed);
    }

    pub fn rate(&self) -> f64 {
        *self.rate.lock().unwrap()
    }

    pub fn to_prometheus(&self) -> String {
        format!(
            "# HELP {} {}\n# TYPE {} gauge\n{} {}\n",
            self.name, self.description, self.name, self.name, self.rate()
        )
    }
}

/// Summary tracks a stream of values and computes approximate quantiles.
/// Uses a simple reservoir sampling approach for memory efficiency.
#[derive(Debug)]
pub struct Summary {
    reservoir: Mutex<Vec<f64>>,
    reservoir_size: usize,
    count: AtomicU64,
    sum: AtomicU64,
    name: String,
    description: String,
}

impl Summary {
    pub fn new(name: &str, description: &str, reservoir_size: usize) -> Self {
        Self {
            reservoir: Mutex::new(Vec::with_capacity(reservoir_size)),
            reservoir_size,
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            name: name.to_string(),
            description: description.to_string(),
        }
    }

    pub fn observe(&self, value: f64) {
        let n = self.count.fetch_add(1, Ordering::Relaxed) as usize;
        self.sum.fetch_add(value as u64, Ordering::Relaxed);
        let mut reservoir = self.reservoir.lock().unwrap();
        if reservoir.len() < self.reservoir_size {
            reservoir.push(value);
        } else {
            // Reservoir sampling: replace with probability reservoir_size / n
            use rand::Rng;
            let idx = rand::thread_rng().gen_range(0..n);
            if idx < self.reservoir_size {
                reservoir[idx] = value;
            }
        }
    }

    pub fn quantile(&self, q: f64) -> Option<f64> {
        let mut reservoir = self.reservoir.lock().unwrap();
        if reservoir.is_empty() {
            return None;
        }
        reservoir.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = ((reservoir.len() - 1) as f64 * q) as usize;
        Some(reservoir[idx.min(reservoir.len() - 1)])
    }

    pub fn median(&self) -> Option<f64> {
        self.quantile(0.5)
    }

    pub fn p99(&self) -> Option<f64> {
        self.quantile(0.99)
    }

    pub fn mean(&self) -> f64 {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            0.0
        } else {
            self.sum.load(Ordering::Relaxed) as f64 / count as f64
        }
    }

    pub fn to_prometheus(&self) -> String {
        let mut out = format!(
            "# HELP {} {}\n# TYPE {} summary\n",
            self.name, self.description, self.name
        );
        let count = self.count.load(Ordering::Relaxed);
        let sum = self.sum.load(Ordering::Relaxed);
        for q in [0.5, 0.9, 0.95, 0.99] {
            if let Some(v) = self.quantile(q) {
                out.push_str(&format!("{}_quantile{{quantile=\"{}\"}} {}\n", self.name, q, v));
            }
        }
        out.push_str(&format!("{}_sum {}\n{}_count {}\n", self.name, sum, self.name, count));
        out
    }
}

/// Simple timer for measuring operation durations.
pub struct Timer {
    start: Instant,
}

impl Timer {
    pub fn new() -> Self {
        Self { start: Instant::now() }
    }

    pub fn elapsed_secs(&self) -> f64 {
        self.start.elapsed().as_secs_f64()
    }

    pub fn elapsed_ms(&self) -> u128 {
        self.start.elapsed().as_millis()
    }

    pub fn reset(&mut self) {
        self.start = Instant::now();
    }
}

impl Default for Timer {
    fn default() -> Self {
        Self::new()
    }
}

/// A convenience wrapper that records elapsed time to a Histogram on drop.
pub struct ScopedTimer<'a> {
    start: Instant,
    histogram: &'a Histogram,
}

impl<'a> ScopedTimer<'a> {
    pub fn new(histogram: &'a Histogram) -> Self {
        Self {
            start: Instant::now(),
            histogram,
        }
    }
}

impl<'a> Drop for ScopedTimer<'a> {
    fn drop(&mut self) {
        self.histogram.observe(self.start.elapsed().as_secs_f64());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter() {
        let c = Counter::new("test_counter", "test");
        assert_eq!(c.get(), 0);
        c.inc();
        assert_eq!(c.get(), 1);
        c.add(5);
        assert_eq!(c.get(), 6);
        c.reset();
        assert_eq!(c.get(), 0);
    }

    #[test]
    fn test_gauge() {
        let g = Gauge::new("test_gauge", "test");
        g.set(42);
        assert_eq!(g.get(), 42);
        g.set(100);
        assert_eq!(g.get(), 100);
    }

    #[test]
    fn test_histogram() {
        let h = Histogram::with_buckets("test_hist", "test", &[1.0, 5.0, 10.0]);
        h.observe(0.5);
        h.observe(3.0);
        h.observe(7.0);
        h.observe(15.0);
        let prom = h.to_prometheus();
        assert!(prom.contains("test_hist_bucket"));
        assert!(prom.contains("test_hist_count 4"));
    }

    #[test]
    fn test_registry() {
        let reg = MetricsRegistry::new();
        let c = reg.counter("q", "queries");
        c.inc();
        let g = reg.gauge("mem", "memory");
        g.set(1024);
        let prom = reg.to_prometheus();
        assert!(prom.contains("q"));
        assert!(prom.contains("mem"));
    }

    #[test]
    fn test_query_metrics() {
        let reg = MetricsRegistry::new();
        let qm = QueryMetrics::new(&reg);
        qm.record_query(0.05, true);
        qm.record_query(0.1, false);
        assert_eq!(qm.total_queries.get(), 2);
        assert_eq!(qm.failed_queries.get(), 1);
    }

    #[test]
    fn test_timer() {
        let mut timer = Timer::new();
        std::thread::sleep(Duration::from_millis(5));
        assert!(timer.elapsed_ms() >= 5);
        timer.reset();
        assert!(timer.elapsed_ms() < 5);
    }

    #[test]
    fn test_telemetry_disabled_by_default() {
        let flags = mgflags::Flags::default();
        assert!(!flags.telemetry_enabled);
    }

    #[test]
    fn test_payload_serialization() {
        let payload = TelemetryPayload {
            storage_id: "test-id".into(),
            uptime_secs: 3600,
            vertex_count: 100,
            edge_count: 500,
            version: "0.1.0".into(),
            os: "macos".into(),
            arch: "arm64".into(),
        };
        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains("test-id"));
        assert!(json.contains("100"));
    }

    #[test]
    fn test_meter() {
        let m = Meter::new("test_meter", "test");
        m.mark();
        m.mark();
        m.mark();
        assert_eq!(m.get_count(), 3);
        m.tick();
        // Rate should be computed after tick
        let rate = m.rate();
        assert!(rate >= 0.0);
    }

    #[test]
    fn test_summary() {
        let s = Summary::new("test_summary", "test", 100);
        for i in 0..1000 {
            s.observe(i as f64);
        }
        assert_eq!(s.mean(), 499.5);
        let median = s.median().unwrap();
        // Reservoir sampling is approximate; median should be in rough middle
        assert!(median >= 300.0 && median <= 700.0, "median={}", median);
        let p99 = s.p99().unwrap();
        assert!(p99 >= 700.0, "p99={}", p99);
    }

    #[test]
    fn test_scoped_timer() {
        let reg = MetricsRegistry::new();
        let hist = reg.histogram("latency", "latency", &[0.001, 0.01, 0.1]);
        {
            let _timer = ScopedTimer::new(&hist);
            std::thread::sleep(Duration::from_millis(5));
        }
        // After drop, histogram should have one observation
        let prom = reg.to_prometheus();
        assert!(prom.contains("latency_bucket"));
    }

    #[test]
    fn test_registry_meter_and_summary() {
        let reg = MetricsRegistry::new();
        let m = reg.meter("requests", "requests per sec");
        m.mark_n(10);
        let s = reg.summary("latency_ms", "request latency", 50);
        s.observe(5.0);
        s.observe(10.0);
        let prom = reg.to_prometheus();
        assert!(prom.contains("requests"));
        assert!(prom.contains("latency_ms"));
    }
}
