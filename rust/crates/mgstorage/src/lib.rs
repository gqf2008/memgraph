//! # mgstorage — MVCC graph storage engine
//!
//! In-memory graph storage with MVCC transaction support. Equivalent to the
//! C++ storage engine in `src/storage/v2/inmemory/`.
//!
//! ## Module overview
//!
//! | Module | Contents |
//! |---|---|
//! | `transaction` | MVCC transaction engine (begin/commit/abort, ID allocator) |
//! | `indices` | Label, property, edge type, edge global indices |
//! | `storage` | Vertex/edge storage with delta chains |
//! | `accessor` | DbAccessor — query engine API |

pub mod accessor;
pub mod analytics;
pub mod bulk_import;
pub mod cache;
pub mod constraints;
pub mod gc_engine;
pub mod indices;
pub mod query_profile;
pub mod replication_hooks;
pub mod snapshot;
pub mod storage;
pub mod text_index;
pub mod transaction;
pub mod triggers;

pub use accessor::DbAccessor;
pub use analytics::{
    average_clustering_coefficient, compute_graph_stats, degree_correlation, degree_histogram,
    power_law_exponent, reciprocity, triangle_density, GraphStats,
};
pub use bulk_import::{
    import_edge_list, import_edges_csv, import_vertices_csv, import_vertices_jsonl,
    parse_property_value, StreamingBulkLoader,
};
pub use cache::{LruCache, PreparedStatement, PreparedStatementCache, QueryCache};
pub use constraints::{ConstraintError, ConstraintType, Constraints, TtlConfig};
pub use gc_engine::{BackgroundGcWorker, GcCycleStats, GcEngine, GcPolicy};
pub use indices::EdgePropertyIndex;
pub use query_profile::{QueryProfile, QueryProfiler, QueryStats, QueryTimer};
pub use replication_hooks::{
    BufferedReplicationHook, FilteredReplicationHook, MulticastReplicationHook, ReplicationEvent,
    ReplicationHook,
};
pub use schema_info::SchemaInfo;
pub use snapshot::{GraphSnapshot, SnapshotEdge, SnapshotManager, SnapshotMeta, SnapshotVertex};
pub use storage::{Storage, WalAppender, WalRecord};
pub use triggers::{
    Trigger, TriggerContext, TriggerEvent, TriggerExecutor, TriggerRegistry, TriggerTiming,
};
pub mod config;
pub mod metrics;
pub mod point_index;
pub mod schema_info;
