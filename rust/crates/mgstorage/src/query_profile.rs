//! Query execution profiling and performance tracking.
//!
//! Records per-query statistics: planning time, execution time,
//! rows scanned, index hits, and memory usage.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// Profile data for a single query execution.
#[derive(Clone, Debug)]
pub struct QueryProfile {
    pub query_text: String,
    pub planning_time: Duration,
    pub execution_time: Duration,
    pub rows_scanned: u64,
    pub rows_returned: u64,
    pub index_hits: u64,
    pub memory_used_bytes: u64,
    pub cache_hits: u64,
}

/// Accumulated statistics for query patterns.
#[derive(Clone, Debug, Default)]
pub struct QueryStats {
    pub count: u64,
    pub total_planning_time: Duration,
    pub total_execution_time: Duration,
    pub total_rows_scanned: u64,
    pub total_rows_returned: u64,
    pub max_execution_time: Duration,
    pub min_execution_time: Duration,
}

/// Global query profiler.
pub struct QueryProfiler {
    history: RwLock<Vec<QueryProfile>>,
    stats_by_pattern: RwLock<HashMap<String, QueryStats>>,
    max_history: usize,
    next_id: AtomicU64,
}

impl QueryProfiler {
    pub fn new(max_history: usize) -> Self {
        Self {
            history: RwLock::new(Vec::with_capacity(max_history)),
            stats_by_pattern: RwLock::new(HashMap::new()),
            max_history,
            next_id: AtomicU64::new(1),
        }
    }

    pub fn record(&self, profile: QueryProfile) {
        let mut stats = self.stats_by_pattern.write().unwrap();
        let entry = stats.entry(profile.query_text.clone()).or_default();
        entry.count += 1;
        entry.total_planning_time += profile.planning_time;
        entry.total_execution_time += profile.execution_time;
        entry.total_rows_scanned += profile.rows_scanned;
        entry.total_rows_returned += profile.rows_returned;
        if profile.execution_time > entry.max_execution_time {
            entry.max_execution_time = profile.execution_time;
        }
        if entry.min_execution_time.is_zero() || profile.execution_time < entry.min_execution_time {
            entry.min_execution_time = profile.execution_time;
        }
        drop(stats);

        let mut history = self.history.write().unwrap();
        if history.len() >= self.max_history {
            history.remove(0);
        }
        history.push(profile);
    }

    pub fn history(&self) -> Vec<QueryProfile> {
        self.history.read().unwrap().clone()
    }

    pub fn stats(&self) -> HashMap<String, QueryStats> {
        self.stats_by_pattern.read().unwrap().clone()
    }

    pub fn slowest_queries(&self, n: usize) -> Vec<QueryProfile> {
        let mut h = self.history.read().unwrap().clone();
        h.sort_by(|a, b| b.execution_time.cmp(&a.execution_time));
        h.into_iter().take(n).collect()
    }

    pub fn clear(&self) {
        self.history.write().unwrap().clear();
        self.stats_by_pattern.write().unwrap().clear();
    }

    pub fn next_query_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}

/// Timer helper for profiling query phases.
pub struct QueryTimer {
    start: Instant,
}

impl QueryTimer {
    pub fn new() -> Self {
        Self { start: Instant::now() }
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

impl Default for QueryTimer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_history() {
        let profiler = QueryProfiler::new(10);
        profiler.record(QueryProfile {
            query_text: "MATCH (n) RETURN n".into(),
            planning_time: Duration::from_millis(5),
            execution_time: Duration::from_millis(50),
            rows_scanned: 100,
            rows_returned: 10,
            index_hits: 0,
            memory_used_bytes: 1024,
            cache_hits: 0,
        });

        let history = profiler.history();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].rows_scanned, 100);
    }

    #[test]
    fn test_slowest_queries() {
        let profiler = QueryProfiler::new(10);
        profiler.record(QueryProfile {
            query_text: "fast".into(),
            planning_time: Duration::from_millis(1),
            execution_time: Duration::from_millis(10),
            rows_scanned: 10,
            rows_returned: 1,
            index_hits: 0,
            memory_used_bytes: 0,
            cache_hits: 0,
        });
        profiler.record(QueryProfile {
            query_text: "slow".into(),
            planning_time: Duration::from_millis(1),
            execution_time: Duration::from_millis(100),
            rows_scanned: 1000,
            rows_returned: 1,
            index_hits: 0,
            memory_used_bytes: 0,
            cache_hits: 0,
        });

        let slowest = profiler.slowest_queries(1);
        assert_eq!(slowest.len(), 1);
        assert_eq!(slowest[0].query_text, "slow");
    }

    #[test]
    fn test_stats_accumulation() {
        let profiler = QueryProfiler::new(10);
        let query = "MATCH (n) RETURN n";
        for _ in 0..3 {
            profiler.record(QueryProfile {
                query_text: query.into(),
                planning_time: Duration::from_millis(5),
                execution_time: Duration::from_millis(20),
                rows_scanned: 100,
                rows_returned: 10,
                index_hits: 0,
                memory_used_bytes: 0,
                cache_hits: 0,
            });
        }

        let stats = profiler.stats();
        let entry = stats.get(query).unwrap();
        assert_eq!(entry.count, 3);
        assert_eq!(entry.total_execution_time, Duration::from_millis(60));
    }

    #[test]
    fn test_history_limit() {
        let profiler = QueryProfiler::new(2);
        for i in 0..5 {
            profiler.record(QueryProfile {
                query_text: format!("q{}", i),
                planning_time: Duration::from_millis(1),
                execution_time: Duration::from_millis(10),
                rows_scanned: 1,
                rows_returned: 1,
                index_hits: 0,
                memory_used_bytes: 0,
                cache_hits: 0,
            });
        }
        assert_eq!(profiler.history().len(), 2);
    }
}
