//! # mgdurability — Database persistence (WAL + Snapshots)
//!
//! Write-Ahead Log and Snapshot persistence for Memgraph.
//! Wire-compatible with the C++ durability format.

pub mod checkpoint;
pub mod cpp_format;
pub mod delta_record;
pub mod durability_manager;
pub mod incremental;
pub mod legacy;
pub mod migration;
pub mod recovery;
pub mod snapshot;
pub mod version;
pub mod wal;

pub use checkpoint::{CheckpointManager, CheckpointMeta};
pub use delta_record::DeltaRecord;
pub use durability_manager::{
    cleanup_old_wals, list_wal_files, DurabilityConfig, DurabilityManager,
};
pub use incremental::{
    IncrementalSnapshotData, IncrementalSnapshotHeader, IncrementalSnapshotReader,
    IncrementalSnapshotWriter,
};
pub use legacy::{
    detect_format_version, read_legacy_delta_record, read_legacy_snapshot, read_legacy_wal,
    CppReader, LegacyDeltaType, LegacySnapshotReader, LegacyWalReader,
};
pub use migration::{
    convert_legacy_wal_to_current, convert_v14_to_current, convert_v20_to_current, ConversionError,
};
pub use recovery::{dump_snapshot, recover, replay_record, Recovery};
pub use snapshot::{SnapshotData, SnapshotReader, SnapshotWriter};
pub use version::{detect_format, FormatKind};
pub use wal::{WalReader, WalWriter};
