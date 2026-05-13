#![allow(dead_code)]
//! memgraph-rs — Rust-native Memgraph binary.
//!
//! Interactive Cypher query shell with persistence, Bolt protocol support,
//! HTTP REST API, graceful shutdown, and runtime config reloading.

mod admin;
mod auth;
mod bolt_server;
mod http;
mod import;
mod query_cache;
mod replication;
mod startup;

use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use mgcatalog::Catalog;
use mgdurability::WalWriter;
use mgstorage::storage::Storage;
use mgstorage::{WalAppender, WalRecord};
use mgsystem::SystemInfo;
use openraft::storage::RaftLogStorage;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use crate::admin::AdminState;
use crate::query_cache::{PreparedStatementCache, QueryCache};
use crate::startup::{
    initialize, persist_state, reload_config, shutdown, start_background_tasks, ServerContext,
    StartupConfig,
};

struct ServerWalWriter {
    inner: WalWriter,
}

impl WalAppender for ServerWalWriter {
    fn append(&mut self, record: WalRecord) {
        let delta = match record {
            WalRecord::VertexCreate { gid, timestamp } => {
                mgdurability::DeltaRecord::VertexCreate { gid, timestamp }
            }
            WalRecord::VertexDelete { gid } => mgdurability::DeltaRecord::VertexDelete { gid },
            WalRecord::VertexAddLabel { gid, label } => {
                mgdurability::DeltaRecord::VertexAddLabel { gid, label }
            }
            WalRecord::VertexRemoveLabel { gid, label } => {
                mgdurability::DeltaRecord::VertexRemoveLabel { gid, label }
            }
            WalRecord::VertexSetProperty { gid, key, value } => {
                mgdurability::DeltaRecord::VertexSetProperty { gid, key, value }
            }
            WalRecord::EdgeCreate {
                gid,
                from_vertex,
                to_vertex,
                edge_type,
                timestamp,
            } => mgdurability::DeltaRecord::EdgeCreate {
                gid,
                from_vertex,
                to_vertex,
                edge_type,
                timestamp,
            },
            WalRecord::EdgeDelete { gid } => mgdurability::DeltaRecord::EdgeDelete { gid },
            WalRecord::EdgeSetProperty { gid, key, value } => {
                mgdurability::DeltaRecord::EdgeSetProperty { gid, key, value }
            }
            WalRecord::EdgeChangeType { gid, old_type, new_type } => {
                mgdurability::DeltaRecord::EdgeChangeType { gid, old_type, new_type }
            }
            WalRecord::EdgeSetFrom { gid, old_from, new_from } => {
                mgdurability::DeltaRecord::EdgeSetFrom { gid, old_from, new_from }
            }
            WalRecord::EdgeSetTo { gid, old_to, new_to } => {
                mgdurability::DeltaRecord::EdgeSetTo { gid, old_to, new_to }
            }
            WalRecord::TransactionStart { timestamp } => {
                mgdurability::DeltaRecord::TransactionStart { timestamp }
            }
            WalRecord::TransactionEnd {
                timestamp,
                commit_timestamp,
            } => mgdurability::DeltaRecord::TransactionEnd {
                timestamp,
                commit_timestamp,
            },
        };
        let _ = self.inner.append_record(&delta);
    }

    fn sync(&mut self) -> Result<(), std::io::Error> {
        self.inner.sync()
    }
}

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const VERSION: &str = "0.1.0";

#[derive(Parser, Debug)]
#[command(name = "memgraph-rs", version = VERSION, about = "Rust-native Memgraph database")]
struct Args {
    /// Data directory for persistence
    #[arg(long, default_value = "memgraph_data")]
    data_directory: PathBuf,

    /// Start Bolt server on specified port
    #[arg(long)]
    bolt_port: Option<u16>,

    /// Start Bolt server on default port 7687
    #[arg(long)]
    bolt: bool,

    /// Execute a single Cypher query and exit
    #[arg(long, short)]
    query: Option<String>,

    /// Import JSONL file
    #[arg(long)]
    import_jsonl: Option<PathBuf>,

    /// Import CSV file (format: path:label)
    #[arg(long)]
    import_csv: Option<String>,

    /// Log filter (e.g. "debug", "info", "mgstorage=trace")
    #[arg(long, default_value = "info")]
    log_filter: String,

    /// Authentication username for Bolt connections
    #[arg(long)]
    bolt_user: Option<String>,

    /// Authentication password for Bolt connections
    #[arg(long)]
    bolt_pass: Option<String>,

    /// Load configuration from JSON file
    #[arg(long)]
    config: Option<PathBuf>,

    /// Start HTTP server on specified port
    #[arg(long)]
    http_port: Option<u16>,

    /// Enable query caching
    #[arg(long, default_value = "true")]
    query_cache: bool,

    /// Maximum query cache entries
    #[arg(long, default_value = "1000")]
    query_cache_size: usize,

    /// Query cache TTL in seconds (0 = no TTL)
    #[arg(long, default_value = "0")]
    query_cache_ttl: u64,

    /// Query cache eviction policy (lru, lfu, fifo)
    #[arg(long, default_value = "lru")]
    query_cache_policy: String,

    /// GC interval in seconds
    #[arg(long, default_value = "60")]
    gc_interval: u64,

    /// Memory limit in MiB (0 = unlimited)
    #[arg(long, default_value = "0")]
    memory_limit: u64,

    /// Print startup banner and exit
    #[arg(long)]
    show_version: bool,

    /// Replication role: main, replica, or none (default: none)
    #[arg(long, default_value = "none")]
    replication_role: String,

    /// Replication port for main to accept replica connections
    #[arg(long, default_value = "10000")]
    replication_port: u16,

    /// Main instance address to connect to (for replica role)
    #[arg(long)]
    replica_of: Option<String>,

    /// Replication mode: sync, async, strict_sync (default: sync)
    #[arg(long, default_value = "sync")]
    replication_mode: String,

    /// Coordinator ID (for HA cluster mode)
    #[arg(long)]
    coordinator_id: Option<String>,

    /// Coordinator port for cluster communication
    #[arg(long, default_value = "12000")]
    coordinator_port: u16,

    /// Comma-separated list of coordinator peers (host:port)
    #[arg(long)]
    coordinator_peers: Option<String>,

    /// Use openraft consensus for coordinator HA (instead of standalone health-check coordinator)
    #[arg(long)]
    coordinator_use_raft: bool,

    /// Raft node ID for this coordinator (required when --coordinator-use-raft is set)
    #[arg(long)]
    coordinator_raft_id: Option<u64>,

    /// Health-check interval for the raft coordinator (seconds)
    #[arg(long, default_value_t = 5)]
    coordinator_health_interval: u64,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    if args.show_version {
        println!("memgraph-rs v{}", VERSION);
        println!("Rust-native Memgraph graph database");
        return;
    }

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log_filter)),
        )
        .init();

    print_banner();
    info!("memgraph-rs v{} starting", VERSION);

    // Parse cache policy
    let cache_policy = match args.query_cache_policy.as_str() {
        "lfu" => crate::query_cache::EvictionPolicy::Lfu,
        "fifo" => crate::query_cache::EvictionPolicy::Fifo,
        _ => crate::query_cache::EvictionPolicy::Lru,
    };

    let cache_ttl = if args.query_cache_ttl > 0 {
        Some(std::time::Duration::from_secs(args.query_cache_ttl))
    } else {
        None
    };

    // Create server context
    let ctx = Arc::new(ServerContext::new_with_config(
        args.data_directory.clone(),
        args.query_cache_size,
        cache_ttl,
        cache_policy,
    ));
    ctx.set_memory_limit(args.memory_limit);

    // Build startup config from CLI args
    let startup_config = StartupConfig::from_args(&args);

    // Initialize server
    if let Err(e) = initialize(&ctx, &startup_config) {
        error!("Initialization failed: {}", e);
        std::process::exit(1);
    }

    // ─── Replication setup ──────────────────────────────────────────────────
    let repl_mode = match args.replication_mode.as_str() {
        "async" => mgrepl::ReplicationMode::Async,
        "strict_sync" => mgrepl::ReplicationMode::StrictSync,
        _ => mgrepl::ReplicationMode::Sync,
    };
    let repl_config = mgrepl::ReplicationConfig {
        mode: repl_mode,
        heartbeat_interval: Duration::from_secs(1),
        max_heartbeat_failures: 3,
        replica_port: args.replication_port,
        wal_directory: Some(ctx.data_directory.join("wal").to_string_lossy().to_string()),
    };

    match args.replication_role.as_str() {
        "main" => {
            let bind_addr = format!("0.0.0.0:{}", args.replication_port);
            match crate::replication::ReplicationManager::new_main(
                &bind_addr,
                repl_config,
                ctx.storage.clone(),
            ) {
                Ok(manager) => {
                    info!("Replication role: MAIN on port {}", args.replication_port);
                    *ctx.replication.lock().unwrap() = Some(manager);
                }
                Err(e) => warn!("Failed to start replication server: {}", e),
            }
        }
        "replica" => {
            if let Some(ref main_addr) = args.replica_of {
                match crate::replication::ReplicationManager::new_replica(
                    main_addr,
                    repl_config,
                    ctx.storage.clone(),
                ) {
                    Ok(manager) => {
                        info!("Replication role: REPLICA of {}", main_addr);
                        *ctx.replication.lock().unwrap() = Some(manager);
                    }
                    Err(e) => warn!("Failed to connect to main: {}", e),
                }
            } else {
                warn!("--replica-of required when role is replica");
            }
        }
        _ => {}
    }

    // ─── Coordinator setup ────────────────────────────────────────────────
    if let Some(ref coordinator_id) = args.coordinator_id {
        // Parse peers into Vec<mgcoord::Instance> (shared by both paths)
        let mut instances = Vec::new();
        if let Some(ref peers_str) = args.coordinator_peers {
            for peer in peers_str.split(',') {
                let peer = peer.trim();
                if peer.is_empty() {
                    continue;
                }
                let parts: Vec<&str> = peer.splitn(2, ':').collect();
                if parts.len() != 2 {
                    warn!("Invalid peer format: {}", peer);
                    continue;
                }
                let host = parts[0];
                let port: u16 = match parts[1].parse() {
                    Ok(p) => p,
                    Err(_) => {
                        warn!("Invalid peer port: {}", parts[1]);
                        continue;
                    }
                };
                let addr = match format!("{}:{}", host, port).parse() {
                    Ok(a) => a,
                    Err(_e) => {
                        warn!("Invalid peer address: {}:{}", host, port);
                        continue;
                    }
                };
                instances.push(mgcoord::Instance::new(
                    format!("{}:{}", host, port),
                    addr,
                    mgcoord::InstanceRole::Main,
                ));
            }
        }

        if args.coordinator_use_raft {
            let raft_id = args.coordinator_raft_id.unwrap_or_else(|| {
                // Derive a stable u64 from the coordinator_id string
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut hasher = DefaultHasher::new();
                coordinator_id.hash(&mut hasher);
                hasher.finish()
            });

            match start_raft_coordinator(coordinator_id, raft_id, instances, &ctx.data_directory)
                .await
            {
                Ok((cm, state)) => {
                    *ctx.cluster_manager.write().await = Some(cm);
                    *ctx.cluster_state.write().unwrap() = state;
                    info!(
                        "Raft coordinator '{}' (node_id={}) started with persistent storage",
                        coordinator_id, raft_id
                    );

                    // Background task: watch Raft metrics and log leadership changes
                    let cm_watch = ctx.cluster_manager.clone();
                    tokio::spawn(async move {
                        let mut last_leader: Option<mgcoord::raft::CoordinatorNodeId> = None;
                        let mut interval = tokio::time::interval(Duration::from_secs(2));
                        loop {
                            interval.tick().await;
                            let guard = cm_watch.read().await;
                            if let Some(ref cm) = *guard {
                                let current = cm.get_leader();
                                if current != last_leader {
                                    match current {
                                        Some(id) => {
                                            info!("[raft] leader changed to node {}", id)
                                        }
                                        None => {
                                            info!("[raft] no leader currently elected")
                                        }
                                    }
                                    last_leader = current;
                                }
                            } else {
                                break;
                            }
                        }
                    });

                    // Background task: periodic health check + auto-failover
                    let cm_health = ctx.cluster_manager.clone();
                    let state_health = ctx.cluster_state.read().unwrap().clone();
                    let health_interval = Duration::from_secs(args.coordinator_health_interval);
                    let health_timeout =
                        Duration::from_secs(args.coordinator_health_interval.saturating_mul(3));
                    tokio::spawn(async move {
                        run_coordinator_health_check(
                            cm_health,
                            state_health,
                            health_interval,
                            health_timeout,
                        )
                        .await;
                    });
                }
                Err(e) => {
                    warn!("Failed to start Raft coordinator: {}", e);
                }
            }
        } else {
            // Legacy non-consensus coordinator path
            let coordinator =
                mgcoord::Coordinator::new(coordinator_id.clone(), Duration::from_secs(5));

            let cluster_config =
                mgcoord::ClusterConfig::new(instances.clone(), "memgraph".to_string());
            let discovery_items: Vec<(String, std::net::SocketAddr, mgcoord::InstanceRole)> =
                instances
                    .iter()
                    .map(|i| (i.id.clone(), i.address, i.role))
                    .collect();
            let discovery = mgcoord::raft::StaticDiscovery::from_slice(&discovery_items);
            coordinator.bootstrap_cluster(&cluster_config, &discovery);

            *ctx.coordinator.lock().unwrap() = Some(coordinator);
            info!(
                "Coordinator '{}' bootstrapped with {} peer(s)",
                coordinator_id,
                discovery_items.len()
            );

            // Start background tick thread
            let coord_ctx = ctx.coordinator.clone();
            std::thread::Builder::new()
                .name("coordinator-tick".into())
                .spawn(move || {
                    let interval = Duration::from_secs(5);
                    loop {
                        std::thread::sleep(interval);
                        let guard = coord_ctx.lock().unwrap();
                        if let Some(ref coord) = *guard {
                            let down = coord.tick();
                            if !down.is_empty() {
                                warn!("[coordinator] down instances: {:?}", down);
                            }
                        } else {
                            break;
                        }
                    }
                    info!("[coordinator-tick] thread exiting");
                })
                .expect("failed to spawn coordinator tick thread");
        }
    }

    // Attach WAL (with replication hook if enabled)
    let wal_path = ctx.data_directory.join("wal.mgwal");
    if let Ok(wal_writer) = WalWriter::create(&wal_path) {
        let base_writer = Box::new(ServerWalWriter { inner: wal_writer });
        let repl_guard = ctx.replication.lock().unwrap();
        if let Some(ref repl) = *repl_guard {
            if let Some(ref main_state) = repl.main_state {
                main_state.set_wal_path(wal_path.clone());
            }
            drop(repl_guard);
            let repl_writer =
                crate::replication::ReplicatingWalWriter::new(base_writer, ctx.replication.clone());
            ctx.storage.set_wal(Box::new(repl_writer));
            info!("WAL attached with replication at {}", wal_path.display());
        } else {
            drop(repl_guard);
            ctx.storage.set_wal(base_writer);
            info!("WAL attached at {}", wal_path.display());
        }
    } else {
        warn!("Failed to create WAL at {}", wal_path.display());
    }

    // Handle data import before starting services
    if let Some(ref path) = args.import_jsonl {
        match import::import_jsonl(&ctx.storage, path) {
            Ok(n) => info!("Imported {} records from {}", n, path.display()),
            Err(e) => error!("Import error: {}", e),
        }
    }
    if let Some(ref spec) = args.import_csv {
        if let Some((path, label)) = spec.split_once(':') {
            match import::import_csv(&ctx.storage, &PathBuf::from(path), label) {
                Ok(n) => info!("Imported {} rows as :{}", n, label),
                Err(e) => error!("Import error: {}", e),
            }
        }
    }

    // Start background maintenance tasks
    start_background_tasks(ctx.clone(), args.gc_interval);

    // Determine ports
    let bolt_port = args.bolt_port.or(if args.bolt {
        Some(bolt_server::DEFAULT_PORT)
    } else {
        None
    });

    // Authentication config (legacy, for Bolt server compat)
    let auth_legacy = Arc::new(match (args.bolt_user.clone(), args.bolt_pass.clone()) {
        (Some(user), Some(pass)) => auth::AuthConfig::basic(user, pass),
        _ => auth::AuthConfig::none(),
    });

    // Start Bolt server if requested
    let bolt_handle = if let Some(port) = bolt_port {
        let bolt_storage = ctx.storage.clone();
        let bolt_catalog = ctx.catalog.clone();
        let bolt_auth = auth_legacy.clone();
        let bolt_admin = ctx.admin.clone();
        let bolt_cache = ctx.query_cache.clone();
        Some(tokio::spawn(async move {
            bolt_server::run(
                bolt_storage,
                bolt_catalog,
                bolt_auth,
                bolt_admin,
                bolt_cache,
                port,
                bolt_server::DEFAULT_MAX_CONNECTIONS,
            )
            .await;
        }))
    } else {
        None
    };

    // Start HTTP server if requested
    let http_handle = if let Some(port) = args.http_port {
        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
        let http_storage = ctx.storage.clone();
        let http_catalog = ctx.catalog.clone();
        let http_admin = ctx.admin.clone();
        let http_cluster_state = Some(ctx.cluster_state.read().unwrap().clone());
        let http_cluster_manager = Some(ctx.cluster_manager.clone());
        Some(tokio::spawn(async move {
            let server = http::HttpServer::new(
                http_storage,
                http_admin,
                http_catalog,
                http_cluster_state,
                http_cluster_manager,
            );
            server.run(addr).await;
        }))
    } else {
        None
    };

    // Execute single query mode
    if let Some(q) = args.query {
        let result = execute_cached(&ctx, &q);
        match result {
            Ok(result) => print_result(&result),
            Err(e) => error!("Error: {}", e),
        }
        if bolt_port.is_none() {
            persist_state(&ctx);
        }
        return;
    }

    // Server modes: daemon (with network) or interactive REPL
    let running_daemon = bolt_port.is_some() || args.http_port.is_some();

    if running_daemon {
        if let Some(port) = bolt_port {
            println!("Bolt server running on port {}", port);
        }
        if let Some(port) = args.http_port {
            println!("HTTP server running on port {}", port);
        }
        println!("Press Ctrl+C to stop");

        // Main daemon loop: handle signals, config reloads, and periodic tasks
        let shutdown_clone = ctx.shutdown_flag.clone();
        let _ = ctrlc::set_handler(move || {
            shutdown_clone.store(true, Ordering::SeqCst);
        });

        while !ctx.is_shutting_down() {
            tokio::time::sleep(Duration::from_millis(200)).await;

            // Check for config reload request (SIGUSR1)
            if ctx.signal_handler.is_reload_requested() {
                if let Some(ref config_path) = args.config {
                    if let Err(e) = reload_config(&ctx, config_path) {
                        warn!("Config reload failed: {}", e);
                    }
                } else {
                    warn!("SIGUSR1 received but no config file specified");
                    ctx.signal_handler.clear_reload();
                }
            }
        }

        println!("\nShutting down...");
        // Gracefully shut down Raft cluster manager before persisting state
        {
            let cm_ref = ctx.cluster_manager.clone();
            let shutdown_result = tokio::spawn(async move {
                if let Some(cm) = cm_ref.write().await.take() {
                    info!("ClusterManager shutting down");
                    cm.shutdown().await
                } else {
                    Ok(())
                }
            })
            .await;
            match shutdown_result {
                Ok(Err(e)) => warn!("ClusterManager shutdown error: {}", e),
                Err(e) => warn!("ClusterManager shutdown task failed: {}", e),
                _ => {}
            }
        }
        shutdown(&ctx);
    } else {
        // Interactive REPL
        println!("Type :help for commands, :quit to exit");
        repl(&ctx);
        persist_state(&ctx);
        println!("Goodbye.");
    }

    // Wait for servers to finish
    if let Some(h) = bolt_handle {
        let _ = h.await;
    }
    if let Some(h) = http_handle {
        let _ = h.await;
    }
}

/// Start an openraft-backed coordinator with persistent log and state machine.
///
/// Returns the [`ClusterManager`] and the shared [`ClusterState`] on success.
async fn start_raft_coordinator(
    coordinator_id: &str,
    raft_id: u64,
    instances: Vec<mgcoord::Instance>,
    data_dir: &std::path::Path,
) -> Result<(mgcoord::ClusterManager, Arc<mgcoord::ClusterState>), String> {
    let cluster_state = Arc::new(mgcoord::ClusterState::new(coordinator_id.to_string()));
    for inst in &instances {
        cluster_state.register(inst.clone());
    }
    if let Some(main) = cluster_state.main_instance() {
        cluster_state.set_route("memgraph", main.address);
    }

    let log_dir = data_dir.join("coordinator").join("log");
    let sm_dir = data_dir.join("coordinator").join("sm");

    let mut log_storage = mgcoord::raft::PersistentCoordinatorLogStorage::open(&log_dir)
        .map_err(|e| format!("failed to open log storage at {}: {}", log_dir.display(), e))?;

    // Detect whether this node has already been initialized by checking
    // for a persisted vote. If so, skip init_single_node on restart.
    let already_initialized = log_storage
        .read_vote()
        .await
        .map(|v| v.is_some())
        .unwrap_or(false);

    let mut state_machine = mgcoord::raft::PersistentCoordinatorStateMachine::open(&sm_dir)
        .map_err(|e| {
            format!(
                "failed to open state machine at {}: {}",
                sm_dir.display(),
                e
            )
        })?;
    state_machine.attach_cluster_state(cluster_state.clone());

    let raft_config = Arc::new(
        openraft::Config::default()
            .validate()
            .unwrap_or_else(|_| openraft::Config::default()),
    );
    let network = mgcoord::raft::CoordinatorNetworkFactory::new();

    let cluster_manager = mgcoord::ClusterManager::with_log_storage(
        raft_id,
        raft_config,
        network,
        log_storage,
        state_machine,
    )
    .await
    .map_err(|e| format!("failed to create cluster manager: {}", e))?;

    if !already_initialized {
        cluster_manager
            .init_single_node()
            .await
            .map_err(|e| format!("failed to initialize single-node cluster: {}", e))?;
    }

    Ok((cluster_manager, cluster_state))
}

/// Periodically check instance health and propose failover through Raft.
///
/// Only the leader proposes `Promote` commands; followers skip health checks.
/// The loop exits when the [`ClusterManager`] is removed from `cm` (shutdown).
async fn run_coordinator_health_check(
    cm: Arc<tokio::sync::RwLock<Option<mgcoord::ClusterManager>>>,
    state: Arc<mgcoord::ClusterState>,
    interval: Duration,
    health_timeout: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;

        let guard = cm.read().await;
        let Some(ref cluster_manager) = *guard else {
            break;
        };

        if !cluster_manager.is_leader() {
            continue;
        }

        let down = state.check_health(health_timeout);
        if !down.is_empty() {
            warn!("[raft-health] instances marked down: {:?}", down);
        }

        if let Some(candidate) = state.failover_candidate() {
            info!(
                "[raft-health] proposing failover: promote {} to main",
                candidate
            );
            match cluster_manager
                .propose(mgcoord::raft::ClusterCommand::Promote {
                    instance_id: candidate,
                })
                .await
            {
                Ok(_) => info!("[raft-health] failover proposal accepted"),
                Err(e) => warn!("[raft-health] failover proposal failed: {}", e),
            }
        }
    }

    info!("[raft-health] health-check task exiting");
}

fn print_banner() {
    println!(
        r#"
    __  ___                      __
   /  |/  /__  _______  ______  / /_____  ____
  / /|_/ / _ \/ ___/ / / / __ \/ __/ __ \/ __ \
 / /  / /  __/ /  / /_/ / /_/ / /_/ /_/ / /_/ /
/_/  /_/\___/_/   \__, / .___/\__/\____/ .___/
                 /____/_/            /_/
"#
    );
}

/// Helper to execute a query with all server contexts (catalog, auth, dbms).
fn exec_query(ctx: &ServerContext, query: &str) -> Result<mginterp::QueryResult, String> {
    mginterp::execute_with_catalog_auth_dbms_and_params(
        &ctx.storage,
        query,
        Some(&ctx.catalog),
        &std::collections::HashMap::new(),
        Some(&ctx.auth),
        Some(&ctx.dbms),
    )
    .map_err(|e| format!("{}", e))
}

/// Execute a query, using the query cache if enabled.
fn execute_cached(ctx: &ServerContext, query: &str) -> Result<mginterp::QueryResult, String> {
    // Check query cache first
    if let Some(entry) = ctx.query_cache.get(query) {
        // If we have a cached result and it's a simple read, return it directly
        if let Some(ref cached_result) = entry.cached_result {
            info!("[query-cache] result hit for query");
            return Ok(cached_result.clone());
        }
        // Otherwise re-execute using cached parsed AST
        info!("[query-cache] parse hit for query");
        return exec_query(ctx, query);
    }

    // Parse and cache
    let parsed = mgparser::parse_query(query).map_err(|e| format!("{}", e))?;
    let plan = mgplanner::plan_query(&ctx.storage, &parsed);
    let result = exec_query(ctx, query)?;

    // Only cache results for simple read-only queries (heuristic: starts with MATCH or RETURN)
    let is_read_only = query.trim().to_lowercase().starts_with("match")
        || query.trim().to_lowercase().starts_with("return");
    let cached_result = if is_read_only {
        Some(result.clone())
    } else {
        None
    };

    ctx.query_cache
        .put_with_result(query.to_string(), parsed, Some(plan), cached_result);
    Ok(result)
}

fn repl(ctx: &ServerContext) {
    let stdin = io::stdin();
    let mut stdout = io::stdout();

    loop {
        print!("memgraph> ");
        stdout.flush().unwrap();

        let mut line = String::new();
        match stdin.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) => {
                error!("Read error: {}", e);
                break;
            }
        }

        let input = line.trim();
        if input.is_empty() {
            continue;
        }

        if input.starts_with(':') {
            handle_repl_command(ctx, input);
            continue;
        }

        let qid = ctx.admin.start_query(0, input.to_string());
        let start = Instant::now();
        match execute_cached(ctx, input) {
            Ok(result) => {
                print_result(&result);
                let elapsed = start.elapsed();
                ctx.admin.finish_query(qid);
                if elapsed > Duration::from_secs(1) {
                    println!("({:?})", elapsed);
                }
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                ctx.admin.fail_query(qid);
            }
        }
    }
}

fn handle_repl_command(ctx: &ServerContext, input: &str) {
    let parts: Vec<&str> = input.split_whitespace().collect();
    let cmd = parts.first().copied().unwrap_or("");

    match cmd {
        ":quit" | ":q" | ":exit" => std::process::exit(0),
        ":help" => print_repl_help(),
        ":clear" => {
            print!("\x1B[2J\x1B[H");
            let _ = io::stdout().flush();
        }
        ":stats" => print_stats(&ctx.storage),
        ":schema" => print_schema(&ctx.catalog),
        ":indexes" => print_indexes(&ctx.storage, &ctx.catalog),
        ":constraints" => print_constraints(&ctx.storage),
        ":connections" => print_connections(&ctx.admin),
        ":queries" => print_active_queries(&ctx.admin),
        ":metrics" => print_metrics(&ctx.admin),
        ":health" => print_health(ctx),
        ":users" => print_users(&ctx.auth),
        ":roles" => print_roles(&ctx.auth),
        ":version" => println!("memgraph-rs v{}", VERSION),
        ":uptime" => println!("Uptime: {:?}", ctx.uptime()),
        ":system" => print_system_info(),
        ":cache" => print_cache_stats(&ctx.query_cache, &ctx.prepared_statements),
        ":profile" => {
            if parts.len() < 2 {
                eprintln!("Usage: :profile <query>");
            } else {
                let query = input.strip_prefix(":profile ").unwrap_or("");
                profile_query(ctx, query);
            }
        }
        ":explain" => {
            if parts.len() < 2 {
                eprintln!("Usage: :explain <query>");
            } else {
                let query = input.strip_prefix(":explain ").unwrap_or("");
                explain_query(&ctx.storage, query);
            }
        }
        ":snapshot" => {
            persist_state(ctx);
            println!("Snapshot saved.");
        }
        ":gc" => {
            let collected = ctx.storage.gc();
            println!("GC collected {} deltas", collected);
        }
        ":prepare" => {
            if parts.len() < 2 {
                eprintln!("Usage: :prepare <query>");
            } else {
                let query = input.strip_prefix(":prepare ").unwrap_or("");
                match ctx.prepared_statements.prepare(query.to_string()) {
                    Ok(stmt) => println!(
                        "Prepared statement #{} with params {:?}",
                        stmt.id, stmt.parameter_names
                    ),
                    Err(e) => eprintln!("Prepare error: {}", e),
                }
            }
        }
        ":execute" => {
            if parts.len() < 2 {
                eprintln!("Usage: :execute <id>");
            } else if let Ok(id) = parts[1].parse::<u64>() {
                match ctx.prepared_statements.get(id) {
                    Some(stmt) => {
                        let start = Instant::now();
                        match exec_query(ctx, &stmt.query_text) {
                            Ok(result) => {
                                let elapsed = start.elapsed().as_millis() as u64;
                                ctx.prepared_statements.record_execution(id, elapsed);
                                print_result(&result);
                            }
                            Err(e) => eprintln!("Error: {}", e),
                        }
                    }
                    None => eprintln!("Prepared statement #{} not found", id),
                }
            } else {
                eprintln!("Invalid statement ID");
            }
        }
        ":kill" => {
            if parts.len() < 2 {
                eprintln!("Usage: :kill <query_id>");
            } else if let Ok(qid) = parts[1].parse::<u64>() {
                if ctx.admin.kill_query(qid) {
                    println!("Killed query #{}", qid);
                } else {
                    eprintln!("Query #{} not found", qid);
                }
            }
        }
        cmd => {
            if cmd.starts_with(":profile ") {
                let query = cmd.strip_prefix(":profile ").unwrap_or("");
                profile_query(ctx, query);
            } else if cmd.starts_with(":explain ") {
                let query = cmd.strip_prefix(":explain ").unwrap_or("");
                explain_query(&ctx.storage, query);
            } else {
                eprintln!("Unknown command: {}. Type :help for commands.", cmd);
            }
        }
    }
}

fn print_repl_help() {
    println!("Commands:");
    println!("  :quit, :q, :exit   Exit the REPL");
    println!("  :help              Show this help");
    println!("  :clear             Clear the screen");
    println!("  :stats             Show database statistics");
    println!("  :schema            Show schema information");
    println!("  :indexes           Show active indexes");
    println!("  :constraints       Show active constraints");
    println!("  :profile <query>   Profile query execution time");
    println!("  :explain <query>   Show query execution plan");
    println!("  :connections       List active connections");
    println!("  :queries           List active queries");
    println!("  :metrics           Show server metrics");
    println!("  :health            Show health status");
    println!("  :users             List users");
    println!("  :roles             List roles");
    println!("  :version           Show server version");
    println!("  :uptime            Show server uptime");
    println!("  :system            Show system information");
    println!("  :cache             Show query cache statistics");
    println!("  :snapshot          Force a snapshot");
    println!("  :gc                Run garbage collection");
    println!("  :prepare <query>   Prepare a statement");
    println!("  :execute <id>      Execute a prepared statement");
    println!("  :kill <id>         Kill an active query");
    println!();
    println!("Enter Cypher queries to execute them against the database.");
}

fn print_connections(admin: &AdminState) {
    let conns = admin.list_connections();
    println!("Active connections: {}", conns.len());
    for c in conns {
        println!(
            "  #{} {} (user={:?}, client={}, bolt={}.{})",
            c.id, c.peer_addr, c.user, c.client_name, c.bolt_version.0, c.bolt_version.1
        );
    }
}

fn print_active_queries(admin: &AdminState) {
    let queries = admin.list_active_queries();
    println!("Active queries: {}", queries.len());
    for q in queries {
        let elapsed = q.started_at.elapsed().as_secs();
        println!(
            "  #{} conn={} elapsed={}s: {}",
            q.query_id, q.connection_id, elapsed, q.query_text
        );
    }
}

fn print_metrics(admin: &AdminState) {
    println!("{}", admin.metrics().to_prometheus());
}

fn print_health(ctx: &ServerContext) {
    let mut health = ctx.admin.health_check();
    health.uptime_secs = ctx.uptime().as_secs();
    let disk = ctx.disk_monitor.usage();
    let mem = ctx.system_info.used_memory_bytes;

    println!("Health: healthy={}", health.healthy);
    println!("  active_connections: {}", health.active_connections);
    println!("  active_queries: {}", health.active_queries);
    println!("  uptime_secs: {}", health.uptime_secs);
    println!("  memory_used_bytes: {}", mem);
    println!("  disk_usage_percent: {:.1}", disk.usage_percent());
    println!("  phase: {:?}", ctx.phase());
}

fn print_users(auth: &mgauth::AuthStore) {
    let users = auth.list_users();
    println!("Users: {}", users.len());
    for u in users {
        println!("  {} (role: {})", u.username, u.role.as_str());
    }
}

fn print_roles(auth: &mgauth::AuthStore) {
    let roles = auth.list_roles();
    if roles.is_empty() {
        println!("Roles: (none defined)");
    } else {
        println!("Roles: {}", roles.join(", "));
    }
}

fn print_indexes(storage: &Storage, catalog: &Catalog) {
    let labels = storage.active_label_indices.read().unwrap();
    let lp = storage.active_label_property_indices.read().unwrap();
    println!("Indexes:");
    println!("  Label indices: {}", labels.len());
    for label in labels.iter() {
        println!(
            "    :{} (id={})",
            catalog.label_name(*label),
            label.as_uint()
        );
    }
    println!("  Label+Property indices: {}", lp.len());
    for (label, prop) in lp.iter() {
        println!(
            "    :{} + {} (id={},{})",
            catalog.label_name(*label),
            catalog.property_name(*prop),
            label.as_uint(),
            prop.as_uint()
        );
    }
}

fn print_constraints(storage: &Storage) {
    let constraints = storage.constraints.list();
    println!("Constraints: {}", constraints.len());
    for c in constraints {
        println!(
            "  {:?} on label={} property={}",
            c.kind,
            c.label.as_uint(),
            c.property.as_uint()
        );
    }
}

fn profile_query(ctx: &ServerContext, query: &str) {
    let start = Instant::now();
    match execute_cached(ctx, query) {
        Ok(result) => {
            let elapsed = start.elapsed();
            print_result(&result);
            println!("---");
            println!("Profile: {} rows in {:?}", result.rows.len(), elapsed);
            if elapsed > Duration::from_millis(0) {
                println!(
                    "  Rows/sec: {:.0}",
                    result.rows.len() as f64 / elapsed.as_secs_f64()
                );
            }
        }
        Err(e) => eprintln!("Error: {}", e),
    }
}

fn explain_query(storage: &Storage, query: &str) {
    match mgparser::parse_query(query) {
        Ok(parsed) => {
            let plan = mgplanner::plan_query(storage, &parsed);
            println!("Query plan:");
            print_plan(&plan, 0);
        }
        Err(e) => eprintln!("Parse error: {}", e),
    }
}

fn print_plan(plan: &mgplanner::LogicalPlan, depth: usize) {
    let indent = "  ".repeat(depth);
    println!(
        "{}{:?} (cost={:.1}, card={:.1})",
        indent,
        plan.op,
        plan.cost.total(),
        plan.cardinality
    );
}

fn print_stats(storage: &Storage) {
    let vertices = storage.all_vertices().len();
    let edges = storage.all_edges().len();
    println!("Database statistics:");
    println!("  Vertices: {}", vertices);
    println!("  Edges:    {}", edges);
    let li = storage.active_label_indices.read().unwrap();
    let lpi = storage.active_label_property_indices.read().unwrap();
    println!("  Label indices: {}", li.len());
    println!("  Label+Property indices: {}", lpi.len());
}

fn print_schema(catalog: &Catalog) {
    let (labels, properties, edge_types) = catalog.dump_mappings();
    println!("Schema:");
    println!("  Labels:      {}", labels.len());
    for (name, id) in labels {
        println!("    :{} (id={})", name, id.as_uint());
    }
    println!("  Properties:  {}", properties.len());
    for (name, id) in properties {
        println!("    {} (id={})", name, id.as_uint());
    }
    println!("  Edge types:  {}", edge_types.len());
    for (name, id) in edge_types {
        println!("    :{} (id={})", name, id.as_uint());
    }
}

fn print_system_info() {
    let info = SystemInfo::gather();
    println!("System information:");
    println!("  OS:          {}", info.os_name);
    println!("  CPUs:        {}", info.cpu_count);
    println!(
        "  Memory:      {} MB total, {} MB used",
        info.total_memory_bytes / (1024 * 1024),
        info.used_memory_bytes / (1024 * 1024)
    );
    println!("  Hostname:    {}", info.hostname);
    println!("  PID:         {}", info.process_id);
}

fn print_cache_stats(query_cache: &QueryCache, prepared: &PreparedStatementCache) {
    let stats = query_cache.stats();
    println!("Query cache:");
    println!("  Entries:     {}/{}", stats.entries, stats.capacity);
    println!("  Hits:        {}", stats.hits);
    println!("  Misses:      {}", stats.misses);
    println!("  Evictions:   {}", stats.evictions);
    println!("  Hit rate:    {:.1}%", stats.hit_rate() * 100.0);
    println!("  Policy:      {:?}", stats.policy);
    if let Some(ttl) = stats.ttl_secs {
        println!("  TTL:         {}s", ttl);
    }
    println!("Prepared statements: {}", prepared.list().len());
    for stmt in prepared.list() {
        println!(
            "  #{}: exec_count={}, avg_ms={:.1}",
            stmt.id, stmt.execution_count, stmt.avg_duration_ms
        );
    }
}

fn print_result(result: &mginterp::QueryResult) {
    if result.columns.is_empty() && result.rows.is_empty() {
        println!("Empty set");
        return;
    }
    for (i, col) in result.columns.iter().enumerate() {
        if i > 0 {
            print!(" | ");
        }
        print!("{}", col);
    }
    println!();
    for (i, col) in result.columns.iter().enumerate() {
        if i > 0 {
            print!("-+-");
        }
        print!("{}", "-".repeat(col.len().max(3)));
    }
    println!();
    for row in &result.rows {
        for (i, col) in result.columns.iter().enumerate() {
            if i > 0 {
                print!(" | ");
            }
            match row.get(col) {
                Some(val) => print!("{}", val),
                None => print!("NULL"),
            }
        }
        println!();
    }
    if result.rows.is_empty() {
        println!("(empty result)");
    } else {
        println!("{} rows returned", result.rows.len());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn test_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    #[tokio::test]
    async fn test_start_raft_creates_cluster_manager() {
        let tmp = std::env::temp_dir().join(format!("mg_raft_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);

        let instances = vec![mgcoord::Instance::new(
            "i1".into(),
            test_addr(7687),
            mgcoord::InstanceRole::Main,
        )];

        let (cm, state) = start_raft_coordinator("coord-1", 1, instances, &tmp)
            .await
            .expect("start_raft_coordinator should succeed");

        // The node should elect itself leader in a single-node cluster
        assert!(cm.is_leader(), "single-node coordinator should be leader");
        assert_eq!(state.instance_count(), 1);
        assert_eq!(state.main_instance().unwrap().id, "i1");

        // Persistent directories should have been created
        assert!(tmp.join("coordinator").join("log").exists());
        assert!(tmp.join("coordinator").join("sm").exists());

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn test_start_raft_persists_across_restarts() {
        let tmp = std::env::temp_dir().join(format!("mg_raft_persist_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);

        let instances = vec![mgcoord::Instance::new(
            "i1".into(),
            test_addr(7687),
            mgcoord::InstanceRole::Main,
        )];

        // First start
        {
            let (cm, state) = start_raft_coordinator("coord-1", 1, instances.clone(), &tmp)
                .await
                .unwrap();
            assert!(cm.is_leader());
            assert_eq!(state.instance_count(), 1);
        }

        // Second start with the same directories should reopen persisted state
        {
            let (cm, state) = start_raft_coordinator("coord-1", 1, instances.clone(), &tmp)
                .await
                .unwrap();
            // After restart the node may need a moment to re-elect itself in a
            // single-node cluster. Poll for a short time.
            let mut is_leader = false;
            for _ in 0..20 {
                if cm.is_leader() {
                    is_leader = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            assert!(
                is_leader,
                "node should re-elect itself as leader after restart"
            );
            assert_eq!(state.instance_count(), 1);
            assert_eq!(state.main_instance().unwrap().id, "i1");
        }

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn test_start_raft_ignores_duplicate_init() {
        let tmp = std::env::temp_dir().join(format!("mg_raft_dup_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);

        let instances = vec![mgcoord::Instance::new(
            "i1".into(),
            test_addr(7687),
            mgcoord::InstanceRole::Main,
        )];

        // init_single_node on an already-initialized node may return an error;
        // the wrapper should not panic.
        let r1 = start_raft_coordinator("coord-1", 1, instances.clone(), &tmp).await;
        assert!(r1.is_ok(), "first start should succeed");

        // Re-initializing the same node is effectively a no-op in openraft
        // (it returns an error but does not corrupt state).
        let r2 = start_raft_coordinator("coord-1", 1, instances.clone(), &tmp).await;
        assert!(r2.is_ok(), "second start should also succeed");

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn test_raft_health_check_proposes_failover() {
        let tmp = std::env::temp_dir().join(format!("mg_raft_health_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);

        let mut main = mgcoord::Instance::new(
            "main-1".into(),
            test_addr(7687),
            mgcoord::InstanceRole::Main,
        );
        main.health = mgcoord::InstanceHealth::Up;
        let mut replica = mgcoord::Instance::new(
            "repl-1".into(),
            test_addr(7688),
            mgcoord::InstanceRole::Replica,
        );
        replica.health = mgcoord::InstanceHealth::Up;

        let (cm, state) = start_raft_coordinator("coord-1", 1, vec![main, replica], &tmp)
            .await
            .unwrap();

        // Wait for leadership
        let mut is_leader = false;
        for _ in 0..20 {
            if cm.is_leader() {
                is_leader = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(is_leader, "should be leader");

        let cm_arc = Arc::new(tokio::sync::RwLock::new(Some(cm)));
        let state_arc = state;

        // Sleep to age heartbeats past the 10 ms timeout
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Refresh replica heartbeat so it stays Up while main is aged out
        state_arc.update_health("repl-1", mgcoord::InstanceHealth::Up);

        let handle = tokio::spawn(run_coordinator_health_check(
            cm_arc.clone(),
            state_arc.clone(),
            Duration::from_millis(50),
            Duration::from_millis(10),
        ));

        // Wait for failover to be proposed and applied through Raft
        tokio::time::sleep(Duration::from_millis(300)).await;

        let new_main = state_arc.main_instance().unwrap();
        assert_eq!(new_main.id, "repl-1");

        // Shutdown
        let mut guard = cm_arc.write().await;
        if let Some(cm) = guard.take() {
            let _ = cm.shutdown().await;
        }
        drop(guard);
        let _ = handle.await;

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn test_raft_health_check_exits_on_shutdown() {
        let tmp = std::env::temp_dir().join(format!("mg_raft_health_exit_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);

        let main = mgcoord::Instance::new(
            "main-1".into(),
            test_addr(7687),
            mgcoord::InstanceRole::Main,
        );

        let (cm, state) = start_raft_coordinator("coord-1", 1, vec![main], &tmp)
            .await
            .unwrap();

        let cm_arc = Arc::new(tokio::sync::RwLock::new(Some(cm)));
        let state_arc = state;

        let handle = tokio::spawn(run_coordinator_health_check(
            cm_arc.clone(),
            state_arc.clone(),
            Duration::from_millis(50),
            Duration::from_secs(60),
        ));

        // Remove the ClusterManager to signal shutdown
        let mut guard = cm_arc.write().await;
        if let Some(cm) = guard.take() {
            let _ = cm.shutdown().await;
        }
        drop(guard);

        // The task should exit within a short time
        let result = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(
            result.is_ok(),
            "health-check task should exit after shutdown"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
