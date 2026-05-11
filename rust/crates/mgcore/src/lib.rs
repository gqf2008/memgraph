//! # mgcore — Memgraph core types
//!
//! This crate provides the foundational data structures used throughout
//! Memgraph's Rust rewrite. All types have zero external dependencies (std only)
//! and maintain wire-compatibility with the C++ implementation where applicable.
//!
//! ## Module overview
//!
//! | Module | Contents |
//! |---|---|
//! | `types` | Strongly-typed IDs: Gid, LabelId, PropertyId, EdgeTypeId, composite keys |
//! | `edge_ref` | EdgeRef union (Gid or pointer) |
//! | `pointer_pack` | PointerPack<N> — pointer + N flag bits in a single AtomicU64 |
//! | `spin_lock` | RwSpinLock — futex-based reader-writer spinlock |
//! | `property_value` | PropertyValue — 17-type tagged union (Cypher value type) |
//! | `property_store` | PropertyStore — sparse property storage indexed by PropertyId |
//! | `temporal` | Date, LocalTime, LocalDateTime, ZonedDateTime, Duration |
//! | `point` | Point2D, Point3D with CRS |
//! | `name_id_mapper` | Bidirectional string ↔ integer interning |
//! | `delta` | Delta, DeltaChain, CommitInfo, TaggedPtr, MVCC traversal |
//! | `vertex` | Vertex with labels, edges, properties, delta chain |
//! | `edge` | Edge + EdgeMetadata with properties, delta chain |

pub mod types;
pub mod edge_ref;
pub mod pointer_pack;
pub mod spin_lock;
pub mod property_value;
pub mod property_store;
pub mod temporal;
pub mod point;
pub mod name_id_mapper;
pub mod delta;
pub mod vertex;
pub mod edge;
pub mod slk_impls;

// Re-export key types at crate root for internal cross-referencing.
pub use delta::Delta;
pub use edge::Edge;
pub use vertex::Vertex;
