//! MVCC transaction engine.
//!
//! Matches the C++ transaction system in `src/storage/v2/transaction.hpp`.
//! Transactions use snapshot isolation: each transaction sees a consistent
//! snapshot of the database as of its start timestamp.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use mgcore::delta::{CommitInfo, IsolationLevel, TRANSACTION_INITIAL_ID};
use mgcore::types::Gid;

/// Unique transaction ID. Transaction IDs double as timestamps.
/// When a transaction is active, its timestamp doubles as its ID.
/// Committed transactions get a commit timestamp.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct TransactionId(pub u64);

/// Transaction states.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TransactionState {
    Active,
    Committed,
    Aborted,
}

/// MVCC Transaction.
///
/// Each transaction gets a `start_timestamp` when it begins. It can read all
/// data committed before that timestamp (snapshot isolation). On commit, it
/// gets a `commit_timestamp` and writes become visible to future transactions.
pub struct Transaction {
    /// Unique transaction ID (= start timestamp when active).
    pub id: TransactionId,
    /// The MVCC snapshot timestamp — this transaction can see data with
    /// commit_timestamp < start_timestamp.
    pub start_timestamp: u64,
    /// Set at commit time. 0 while active.
    pub commit_timestamp: AtomicU64,
    /// Commit info shared with all deltas created by this transaction.
    pub commit_info: std::sync::Arc<CommitInfo>,
    /// Isolation level for this transaction.
    pub isolation_level: IsolationLevel,
    /// Current state.
    state: std::sync::Mutex<TransactionState>,
    /// Command counter within the transaction (for ordering deltas).
    command_id: AtomicU64,
    /// Gids modified by this transaction (vertices + edges). Used for
    /// write-write conflict detection at commit time.
    write_set: Mutex<HashSet<Gid>>,
}

impl Transaction {
    /// Begin a new transaction with the given start timestamp.
    pub fn new(id: TransactionId, start_timestamp: u64, isolation_level: IsolationLevel) -> Self {
        Self {
            id,
            start_timestamp,
            commit_timestamp: AtomicU64::new(0),
            commit_info: std::sync::Arc::new(CommitInfo::new(id.0)),
            isolation_level,
            state: std::sync::Mutex::new(TransactionState::Active),
            command_id: AtomicU64::new(0),
            write_set: Mutex::new(HashSet::new()),
        }
    }

    /// Allocate the next command ID within this transaction.
    pub fn next_command_id(&self) -> u64 {
        self.command_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Check if this transaction is still active.
    pub fn is_active(&self) -> bool {
        matches!(*self.state.lock().unwrap(), TransactionState::Active)
    }

    /// Mark as committed and assign a commit timestamp.
    /// Returns false if already committed/aborted.
    pub fn commit(&self, commit_timestamp: u64) -> bool {
        let mut state = self.state.lock().unwrap();
        if !matches!(*state, TransactionState::Active) {
            return false;
        }
        *state = TransactionState::Committed;
        drop(state);
        self.commit_timestamp.store(commit_timestamp, Ordering::Release);
        self.commit_info.set_timestamp(commit_timestamp);
        true
    }

    /// Mark as aborted. Returns false if already committed/aborted.
    pub fn abort(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        if !matches!(*state, TransactionState::Active) {
            return false;
        }
        *state = TransactionState::Aborted;
        true
    }

    pub fn commit_timestamp(&self) -> u64 {
        self.commit_timestamp.load(Ordering::Acquire)
    }

    pub fn state(&self) -> TransactionState {
        *self.state.lock().unwrap()
    }

    /// Record that this transaction modified the given Gid.
    pub fn record_write(&self, gid: Gid) {
        self.write_set.lock().unwrap().insert(gid);
    }

    /// Take the write set out of the transaction (leaving it empty).
    pub fn take_write_set(&self) -> HashSet<Gid> {
        std::mem::take(&mut *self.write_set.lock().unwrap())
    }
}

/// Global transaction engine.
///
/// Uses two allocators:
/// - Transaction IDs start above TRANSACTION_INITIAL_ID (2^62).
///   These are used to identify transactions and as delta timestamps while
///   the transaction is active. Because they are >= TRANSACTION_INITIAL_ID,
///   they FAIL the SnapshotIsolation visibility check (ts < start_timestamp),
///   since start_timestamp is always < TRANSACTION_INITIAL_ID.
/// - Commit timestamps are allocated from a separate counter starting at 1.
///   When a transaction commits, all its deltas get the commit_timestamp,
///   which is < TRANSACTION_INITIAL_ID, making them visible.
pub struct TransactionEngine {
    next_tx_id: AtomicU64,
    next_commit_ts: AtomicU64,
}

impl TransactionEngine {
    pub fn new() -> Self {
        Self {
            next_tx_id: AtomicU64::new(TRANSACTION_INITIAL_ID + 1),
            next_commit_ts: AtomicU64::new(1),
        }
    }

    /// Begin a new transaction. The start_timestamp is the NEXT commit timestamp.
    /// This ensures that all previously committed data (ts < start_timestamp)
    /// is visible to this transaction. Uncommitted deltas have timestamps
    /// >= TRANSACTION_INITIAL_ID and won't pass the visibility check.
    pub fn begin(&self, isolation_level: IsolationLevel) -> Transaction {
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::Relaxed);
        // Use the next commit_ts as the snapshot point so that all commits
        // before this transaction (ts = 0..next_commit_ts-1) are visible.
        let start_ts = self.next_commit_ts.load(Ordering::Acquire);
        Transaction::new(TransactionId(tx_id), start_ts, isolation_level)
    }

    /// Commit a transaction, assigning a commit timestamp (< TRANSACTION_INITIAL_ID).
    pub fn commit(&self, tx: &Transaction) -> bool {
        let commit_ts = self.next_commit_ts.fetch_add(1, Ordering::Relaxed);
        tx.commit(commit_ts)
    }

    /// Abort a transaction.
    pub fn abort(&self, tx: &Transaction) -> bool {
        tx.abort()
    }

    /// Get the next commit timestamp (current logical clock value).
    pub fn current_timestamp(&self) -> u64 {
        self.next_commit_ts.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_lifecycle() {
        let engine = TransactionEngine::new();
        let tx = engine.begin(IsolationLevel::SnapshotIsolation);
        assert!(tx.is_active());
        assert_eq!(tx.commit_timestamp(), 0);

        assert!(engine.commit(&tx));
        assert!(!tx.is_active());
        assert!(tx.commit_timestamp() > 0);
    }

    #[test]
    fn test_transaction_abort() {
        let engine = TransactionEngine::new();
        let tx = engine.begin(IsolationLevel::SnapshotIsolation);
        assert!(tx.abort());
        assert!(!tx.is_active());
        // Cannot commit after abort
        assert!(!engine.commit(&tx));
    }

    #[test]
    fn test_command_ids() {
        let engine = TransactionEngine::new();
        let tx = engine.begin(IsolationLevel::SnapshotIsolation);
        assert_eq!(tx.next_command_id(), 0);
        assert_eq!(tx.next_command_id(), 1);
        assert_eq!(tx.next_command_id(), 2);
    }

    #[test]
    fn test_start_timestamps_can_be_same() {
        let engine = TransactionEngine::new();
        // next_commit_ts starts at 1
        let tx1 = engine.begin(IsolationLevel::SnapshotIsolation);
        let tx2 = engine.begin(IsolationLevel::SnapshotIsolation);
        // Both start before any commits, so both see snapshot at ts=1
        assert_eq!(tx1.start_timestamp, 1);
        assert_eq!(tx2.start_timestamp, 1);
    }

    #[test]
    fn test_commit_advances_snapshot() {
        let engine = TransactionEngine::new();
        let tx1 = engine.begin(IsolationLevel::SnapshotIsolation);
        engine.commit(&tx1);

        // tx2 starts after tx1 committed (commit_ts=1, next_commit_ts=2)
        let tx2 = engine.begin(IsolationLevel::SnapshotIsolation);
        assert_eq!(tx2.start_timestamp, 2);
        assert!(tx2.start_timestamp < TRANSACTION_INITIAL_ID);
    }

    #[test]
    fn test_transaction_id_unique() {
        let engine = TransactionEngine::new();
        let tx1 = engine.begin(IsolationLevel::SnapshotIsolation);
        let tx2 = engine.begin(IsolationLevel::SnapshotIsolation);
        assert_ne!(tx1.id.0, tx2.id.0);
    }
}
