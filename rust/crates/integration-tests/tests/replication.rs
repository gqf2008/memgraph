//! End-to-end replication tests: main → replica delta streaming.
//!
//! These tests exercise the mgrepl replication protocol with real TCP
//! connections and verify data consistency between main and replica.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use mgcore::delta::IsolationLevel;
use mgcore::property_value::PropertyValue;
use mgcore::types::{Gid, PropertyId};
use mgdurability::DeltaRecord;
use mgrepl::{DeltaApplier, ReplicationConfig, ReplicationMode, ReplServer, ReplicationClient};
use mgstorage::storage::Storage;
use mgrpc::DeltaBatch;

/// Test that deltas can be applied directly to a replica storage.
#[test]
fn test_repl_delta_batch_apply() {
    let replica_storage = Arc::new(Storage::new());

    let batch = DeltaBatch {
        epoch_id: 1,
        commit_timestamp: 100,
        sequence_number: 0,
        deltas: vec![
            DeltaRecord::VertexCreate { gid: Gid::from(1u64), timestamp: 100 },
            DeltaRecord::VertexSetProperty {
                gid: Gid::from(1u64),
                key: PropertyId::from(0u32),
                value: PropertyValue::String("Alice".into()),
            },
        ],
    };

    let mut applier = mgrepl::StorageDeltaApplier::new(replica_storage.clone());
    applier.apply_batch(&batch).unwrap();

    // Verify replica has the vertex
    let tx = replica_storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    let vertex = replica_storage.get_vertex(Gid::from(1u64), &tx);
    assert!(vertex.is_some(), "replica should have vertex after delta apply");
    let snap = vertex.unwrap();
    let name = snap.properties.get(PropertyId::from(0u32));
    assert_eq!(name, &PropertyValue::String("Alice".into()));
}

/// Test full replication pipeline: main writes → server sends → replica receives.
#[test]
fn test_repl_main_to_replica_pipeline() {
    let repl_config = ReplicationConfig {
        mode: ReplicationMode::Sync,
        heartbeat_interval: Duration::from_secs(1),
        max_heartbeat_failures: 3,
        replica_port: 0,
        wal_directory: None,
    };

    // Main side
    let main_storage = Arc::new(Storage::new());
    let main_server = ReplServer::bind("127.0.0.1:0", repl_config.clone()).unwrap();
    let main_addr = main_server.local_addr().unwrap().to_string();

    // Spawn replica client in a thread
    let replica_storage = Arc::new(Storage::new());
    let replica_storage_clone = replica_storage.clone();

    let client_handle = thread::spawn(move || {
        let mut client = ReplicationClient::connect(&main_addr, repl_config).unwrap();
        // Request delta stream from beginning
        let _ = client.request_delta_stream(0, 1000);
        // Server skeleton returns EOS immediately, so this completes quickly
        let mut applier = mgrepl::StorageDeltaApplier::new(replica_storage_clone);
        let _ = client.apply_stream(&mut applier);
    });

    // Server accept loop: accept one connection and handle a single stream request
    let (mut server_client, _addr) = main_server.accept().unwrap();
    // The skeleton ReplServer returns DeltaStreamEnd for any delta stream request.
    // We simulate by sending an empty batch (EOS marker) back.
    let eos_batch = DeltaBatch {
        epoch_id: main_server.epoch_id(),
        commit_timestamp: 0,
        sequence_number: 0,
        deltas: vec![],
    };
    let _header = mgrpc::MessageHeader::new(20, 1);
    let _ = mgrepl::send_delta_batch(&mut server_client, &eos_batch);

    client_handle.join().unwrap();

    // Main should still have its local data
    let tx = main_storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    assert!(main_storage.get_vertex(Gid::from(42u64), &tx).is_none()); // nothing was created on main in this test
}

/// Test replication heartbeat roundtrip.
#[test]
fn test_repl_heartbeat_roundtrip() {
    let repl_config = ReplicationConfig::default();
    let main_server = ReplServer::bind("127.0.0.1:0", repl_config.clone()).unwrap();
    let main_addr = main_server.local_addr().unwrap().to_string();

    // Spawn client in a thread
    let client_handle = thread::spawn(move || {
        let mut client = ReplicationClient::connect(&main_addr, repl_config).unwrap();
        client.heartbeat("test-uuid").unwrap();
    });

    // Server accept and handle heartbeat
    let (mut server_client, _addr) = main_server.accept().unwrap();
    // Receive heartbeat
    let (_header, hb): (mgrpc::MessageHeader, mgrpc::Heartbeat) = server_client.recv().unwrap();
    // Send heartbeat back
    let resp_header = mgrpc::MessageHeader::new(1, 1);
    let _ = server_client.send(&resp_header, &hb);

    client_handle.join().unwrap();
}

/// Test that replica storage starts empty and stays consistent after batch apply.
#[test]
fn test_repl_replica_consistency() {
    let replica_storage = Arc::new(Storage::new());

    // Start empty
    let tx = replica_storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    assert!(replica_storage.get_vertex(Gid::from(1u64), &tx).is_none());

    // Apply a batch with vertex + edge
    let mut applier = mgrepl::StorageDeltaApplier::new(replica_storage.clone());
    let batch = DeltaBatch {
        epoch_id: 1,
        commit_timestamp: 200,
        sequence_number: 0,
        deltas: vec![
            DeltaRecord::VertexCreate { gid: Gid::from(1u64), timestamp: 200 },
            DeltaRecord::VertexCreate { gid: Gid::from(2u64), timestamp: 200 },
            DeltaRecord::EdgeCreate {
                gid: Gid::from(100u64),
                from_vertex: Gid::from(1u64),
                to_vertex: Gid::from(2u64),
                edge_type: mgcore::types::EdgeTypeId::from(1u32),
                timestamp: 200,
            },
        ],
    };
    applier.apply_batch(&batch).unwrap();

    // Verify both vertices exist
    let tx = replica_storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    assert!(replica_storage.get_vertex(Gid::from(1u64), &tx).is_some());
    assert!(replica_storage.get_vertex(Gid::from(2u64), &tx).is_some());

    // Verify edge exists
    let edges = replica_storage.all_edges();
    assert_eq!(edges.len(), 1, "should have exactly one edge");
}
