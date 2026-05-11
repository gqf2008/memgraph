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

pub mod transaction;
pub mod indices;
pub mod constraints;
pub mod text_index;
pub mod storage;
pub mod accessor;
pub mod triggers;
pub mod query_profile;
pub mod bulk_import;
pub mod gc_engine;
pub mod cache;
pub mod analytics;
pub mod snapshot;
pub mod replication_hooks;

pub use storage::{Storage, WalRecord, WalAppender};
pub use accessor::DbAccessor;
pub use constraints::{Constraints, ConstraintError, ConstraintType, TtlConfig};
pub use indices::EdgePropertyIndex;
pub use schema_info::SchemaInfo;
pub use triggers::{TriggerRegistry, Trigger, TriggerEvent, TriggerTiming, TriggerContext, TriggerExecutor};
pub use query_profile::{QueryProfiler, QueryProfile, QueryStats, QueryTimer};
pub use bulk_import::{import_vertices_csv, import_edges_csv, import_vertices_jsonl, import_edge_list, StreamingBulkLoader, parse_property_value};
pub use gc_engine::{GcEngine, GcPolicy, GcCycleStats, BackgroundGcWorker};
pub use cache::{LruCache, QueryCache, PreparedStatementCache, PreparedStatement};
pub use analytics::{GraphStats, compute_graph_stats, degree_histogram, power_law_exponent, reciprocity, average_clustering_coefficient, triangle_density, degree_correlation};
pub use snapshot::{SnapshotManager, SnapshotMeta, GraphSnapshot, SnapshotVertex, SnapshotEdge};
pub use replication_hooks::{ReplicationEvent, ReplicationHook, BufferedReplicationHook, MulticastReplicationHook, FilteredReplicationHook};
pub mod point_index;
pub mod schema_info;
pub mod metrics;
pub mod config;
