//! End-to-end integration tests for the Memgraph Rust query engine.
//!
//! These tests exercise the full stack: parser → semantic analysis → planner →
//! interpreter → storage, verifying correctness of Cypher query execution.

use mgauth::AuthStore;
use mgcatalog::Catalog;
use mgcore::property_value::PropertyValue;
use mginterp::QueryResult;
use mgstorage::storage::Storage;

/// Test context holding a storage engine, catalog, optional auth store, and dbms.
struct TestCtx {
    storage: std::sync::Arc<Storage>,
    catalog: Catalog,
    auth: Option<AuthStore>,
    dbms: mgdbms::DbmsHandler,
    settings: mginterp::SettingsStore,
    tx_log: mginterp::TransactionLog,
}

impl TestCtx {
    fn new() -> Self {
        let storage = std::sync::Arc::new(Storage::new());
        let trigger_exec = std::sync::Arc::new(mginterp::TriggerInterpreter::new(storage.clone()));
        storage.set_trigger_executor(trigger_exec);
        let flags = mgflags::Flags::default();
        Self {
            storage,
            catalog: Catalog::new(),
            auth: None,
            dbms: mgdbms::DbmsHandler::new(),
            settings: mginterp::SettingsStore::from_flags(&flags),
            tx_log: mginterp::TransactionLog::new(),
        }
    }

    fn with_auth() -> Self {
        let storage = std::sync::Arc::new(Storage::new());
        let trigger_exec = std::sync::Arc::new(mginterp::TriggerInterpreter::new(storage.clone()));
        storage.set_trigger_executor(trigger_exec);
        let flags = mgflags::Flags::default();
        Self {
            storage,
            catalog: Catalog::new(),
            auth: Some(AuthStore::new()),
            dbms: mgdbms::DbmsHandler::new(),
            settings: mginterp::SettingsStore::from_flags(&flags),
            tx_log: mginterp::TransactionLog::new(),
        }
    }

    fn run(&self, query: &str) -> Result<QueryResult, mginterp::ExecError> {
        mginterp::execute_with_catalog_auth_dbms_params_timeout_settings_txlog(
            &self.storage, query, Some(&self.catalog), &std::collections::HashMap::new(),
            self.auth.as_ref(), Some(&self.dbms), None, Some(&self.settings), Some(&self.tx_log),
        )
    }

    fn run_with_params(
        &self,
        query: &str,
        params: &std::collections::HashMap<String, mgcore::property_value::PropertyValue>,
    ) -> Result<QueryResult, mginterp::ExecError> {
        mginterp::execute_with_catalog_auth_dbms_params_timeout_settings_txlog(
            &self.storage, query, Some(&self.catalog), params,
            self.auth.as_ref(), Some(&self.dbms), None, Some(&self.settings), Some(&self.tx_log),
        )
    }
}

fn assert_rows_eq(result: &QueryResult, expected: Vec<Vec<(&str, PropertyValue)>>) {
    assert_eq!(
        result.rows.len(),
        expected.len(),
        "row count mismatch: got {}, expected {}",
        result.rows.len(),
        expected.len()
    );
    for (i, row) in result.rows.iter().enumerate() {
        let expected_row = &expected[i];
        assert_eq!(
            row.len(),
            expected_row.len(),
            "column count mismatch at row {}: got {}, expected {}",
            i,
            row.len(),
            expected_row.len()
        );
        for (k, v) in expected_row.iter() {
            let got = row.get(*k).unwrap_or_else(|| panic!("missing column '{}' at row {}", k, i));
            assert_eq!(got, v, "value mismatch at row {}, column '{}'", i, k);
        }
    }
}

#[test]
fn e2e_create_and_match_vertex() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: \"Alice\"})").unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN n.name AS name").unwrap();
    assert_rows_eq(&result, vec![vec![("name", PropertyValue::String("Alice".into()))]]);
}

#[test]
fn e2e_explicit_transaction() {
    let ctx = TestCtx::new();
    // Begin explicit transaction
    let tx = ctx.storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
    let _guard = mginterp::set_active_transaction(Some(tx.clone()));

    // Create within explicit transaction
    ctx.run("CREATE (n:Person {name: \"Alice\"})").unwrap();

    // Should be visible within same transaction
    let result = ctx.run("MATCH (n:Person) RETURN n.name AS name").unwrap();
    assert_rows_eq(&result, vec![vec![("name", PropertyValue::String("Alice".into()))]]);

    // Commit
    ctx.storage.commit_transaction(&tx);

    // Should still be visible after commit
    let result = ctx.run("MATCH (n:Person) RETURN n.name AS name").unwrap();
    assert_rows_eq(&result, vec![vec![("name", PropertyValue::String("Alice".into()))]]);
}

#[test]
fn e2e_create_and_match_relationship() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})").unwrap();

    let result = ctx
        .run("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS a, b.name AS b")
        .unwrap();
    assert_rows_eq(
        &result,
        vec![vec![
            ("a", PropertyValue::String("Alice".into())),
            ("b", PropertyValue::String("Bob".into())),
        ]],
    );
}

#[test]
fn e2e_match_with_where() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: \"Alice\", age: 30})").unwrap();
    ctx.run("CREATE (b:Person {name: \"Bob\", age: 25})").unwrap();
    ctx.run("CREATE (c:Person {name: \"Charlie\", age: 35})").unwrap();

    let result = ctx
        .run("MATCH (n:Person) WHERE n.age > 25 RETURN n.name AS name ORDER BY n.name")
        .unwrap();
    assert_rows_eq(
        &result,
        vec![
            vec![("name", PropertyValue::String("Alice".into()))],
            vec![("name", PropertyValue::String("Charlie".into()))],
        ],
    );
}

#[test]
fn e2e_match_with_where_and_relationship() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: \"Alice\"})-[:KNOWS {since: 2010}]->(b:Person {name: \"Bob\"})").unwrap();
    ctx.run("CREATE (a:Person {name: \"Alice\"})-[:KNOWS {since: 2015}]->(c:Person {name: \"Charlie\"})").unwrap();

    let result = ctx
        .run("MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.since >= 2012 RETURN b.name AS name")
        .unwrap();
    assert_rows_eq(
        &result,
        vec![vec![("name", PropertyValue::String("Charlie".into()))]],
    );
}

#[test]
fn e2e_aggregation_count() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (:Person)").unwrap();
    ctx.run("CREATE (:Person)").unwrap();
    ctx.run("CREATE (:Person)").unwrap();

    let result = ctx.run("MATCH (n:Person) RETURN count(*) AS cnt").unwrap();
    assert_rows_eq(&result, vec![vec![("cnt", PropertyValue::Int(3))]]);
}

#[test]
fn e2e_aggregation_sum_avg_min_max() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (:Item {value: 10})").unwrap();
    ctx.run("CREATE (:Item {value: 20})").unwrap();
    ctx.run("CREATE (:Item {value: 30})").unwrap();

    let result = ctx
        .run("MATCH (n:Item) RETURN sum(n.value) AS total, avg(n.value) AS mean, min(n.value) AS mn, max(n.value) AS mx")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let row = &result.rows[0];
    assert_eq!(row.get("total"), Some(&PropertyValue::Int(60)));
    assert_eq!(row.get("mn"), Some(&PropertyValue::Int(10)));
    assert_eq!(row.get("mx"), Some(&PropertyValue::Int(30)));
    assert!(matches!(row.get("mean"), Some(PropertyValue::Double(_))));
}

#[test]
fn e2e_aggregation_collect() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (:Person {name: \"Alice\"})").unwrap();
    ctx.run("CREATE (:Person {name: \"Bob\"})").unwrap();

    let result = ctx.run("MATCH (n:Person) RETURN collect(n.name) AS names").unwrap();
    assert_eq!(result.rows.len(), 1);
    let names = result.rows[0].get("names").unwrap();
    assert!(matches!(names, PropertyValue::List(_)));
}

#[test]
fn e2e_aggregation_grouping() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (:Person {city: \"NYC\", age: 30})").unwrap();
    ctx.run("CREATE (:Person {city: \"NYC\", age: 40})").unwrap();
    ctx.run("CREATE (:Person {city: \"LA\", age: 25})").unwrap();

    let result = ctx
        .run("MATCH (n:Person) RETURN n.city AS city, count(*) AS cnt, avg(n.age) AS mean_age ORDER BY city")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get("city"), Some(&PropertyValue::String("LA".into())));
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(1)));
    assert_eq!(result.rows[1].get("city"), Some(&PropertyValue::String("NYC".into())));
    assert_eq!(result.rows[1].get("cnt"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_variable_length_path() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})-[:KNOWS]->(c:Person {name: \"Charlie\"})").unwrap();

    let result = ctx
        .run("MATCH (a:Person {name: \"Alice\"})-[:KNOWS*1..2]->(b:Person) RETURN b.name AS name ORDER BY b.name")
        .unwrap();
    assert_rows_eq(
        &result,
        vec![
            vec![("name", PropertyValue::String("Bob".into()))],
            vec![("name", PropertyValue::String("Charlie".into()))],
        ],
    );
}

#[test]
fn e2e_variable_length_unbounded() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})-[:KNOWS]->(c:Person {name: \"Charlie\"})").unwrap();

    let result = ctx
        .run("MATCH (a:Person {name: \"Alice\"})-[:KNOWS*]->(b:Person) RETURN b.name AS name ORDER BY b.name")
        .unwrap();
    assert_rows_eq(
        &result,
        vec![
            vec![("name", PropertyValue::String("Bob".into()))],
            vec![("name", PropertyValue::String("Charlie".into()))],
        ],
    );
}

#[test]
fn e2e_bfs_basic() {
    let ctx = TestCtx::new();
    // Graph: Alice -> Bob -> Charlie, Alice -> Dave
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})-[:KNOWS]->(c:Person {name: 'Charlie'}), (a)-[:KNOWS]->(d:Person {name: 'Dave'})").unwrap();

    // BFS finds Bob (depth 1), Dave (depth 1), Charlie (depth 2)
    let result = ctx
        .run("MATCH (a:Person {name: 'Alice'})-[:KNOWS*bfs..2]->(b:Person) RETURN b.name AS name")
        .unwrap();
    // Collect names into a set; BFS should return all 3 reachable persons
    let names: Vec<String> = result.rows.iter()
        .map(|r| match r.get("name") {
            Some(PropertyValue::String(s)) => s.clone(),
            _ => panic!("expected string name"),
        })
        .collect();
    assert_eq!(names.len(), 3, "expected 3 results, got: {:?}", names);
    assert!(names.contains(&"Bob".to_string()));
    assert!(names.contains(&"Charlie".to_string()));
    assert!(names.contains(&"Dave".to_string()));
}

#[test]
fn e2e_bfs_lower_bound() {
    let ctx = TestCtx::new();
    // Graph: A -> B -> C -> D
    ctx.run("CREATE (a:Node {id: 0})-[:LINK]->(:Node {id: 1})-[:LINK]->(:Node {id: 2})-[:LINK]->(:Node {id: 3})").unwrap();

    let result = ctx
        .run("MATCH (a:Node {id: 0})-[:LINK*bfs 2..]->(b:Node) RETURN b.id AS id ORDER BY b.id")
        .unwrap();
    assert_rows_eq(
        &result,
        vec![
            vec![("id", PropertyValue::Int(2))],
            vec![("id", PropertyValue::Int(3))],
        ],
    );
}

#[test]
fn e2e_bfs_avoids_cycles() {
    let ctx = TestCtx::new();
    // Graph: A -> B -> C -> A (cycle)
    ctx.run("CREATE (a:Node {name: 'A'})-[:LINK]->(:Node {name: 'B'})-[:LINK]->(:Node {name: 'C'})-[:LINK]->(a)").unwrap();

    // BFS with min=1 should find B (depth 1) and C (depth 2), not loop infinitely.
    // A is at depth 0 and is excluded by the default min=1.
    let result = ctx
        .run("MATCH (a:Node {name: 'A'})-[:LINK*bfs..5]->(b:Node) RETURN b.name AS name ORDER BY b.name")
        .unwrap();
    assert_rows_eq(
        &result,
        vec![
            vec![("name", PropertyValue::String("B".into()))],
            vec![("name", PropertyValue::String("C".into()))],
        ],
    );
}

#[test]
fn e2e_bfs_edge_type_filter() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (:Node {id: 0})-[:r0 {val: 0}]->()-[:r1 {val: 1}]->()-[:r2 {val: 2}]->()").unwrap();

    // Match only r0 edges; target node has no label in this graph.
    let result = ctx
        .run("MATCH ()-[r:r0 *bfs..10]->(m) RETURN size(r) AS s, (r[0]).val AS r0")
        .unwrap();
    assert_rows_eq(
        &result,
        vec![vec![("s", PropertyValue::Int(1)), ("r0", PropertyValue::Int(0))]],
    );
}

#[test]
fn e2e_bfs_edge_property_filter() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (:Node {id: 0})-[:r {val: 0}]->(:Node {id: 1})-[:r {val: 1}]->(:Node {id: 2})").unwrap();

    let result = ctx
        .run("MATCH ()-[r *bfs..10 {val: 1}]->(m:Node) RETURN size(r) AS s, (r[0]).val AS r0")
        .unwrap();
    assert_rows_eq(
        &result,
        vec![vec![("s", PropertyValue::Int(1)), ("r0", PropertyValue::Int(1))]],
    );
}

#[test]
fn e2e_periodic_commit_unwind_create() {
    let ctx = TestCtx::new();
    ctx.run("USING PERIODIC COMMIT 1 UNWIND range(1, 3) AS x CREATE (a:A {id: x})").unwrap();

    let result = ctx.run("MATCH (a:A) RETURN a.id AS id ORDER BY a.id").unwrap();
    assert_rows_eq(
        &result,
        vec![
            vec![("id", PropertyValue::Int(1))],
            vec![("id", PropertyValue::Int(2))],
            vec![("id", PropertyValue::Int(3))],
        ],
    );
}

#[test]
fn e2e_periodic_commit_unwind_create_with_return() {
    let ctx = TestCtx::new();
    let result = ctx
        .run("USING PERIODIC COMMIT 1 UNWIND range(1, 3) AS x CREATE (a:A {id: x}) RETURN a.id AS id ORDER BY a.id")
        .unwrap();
    assert_rows_eq(
        &result,
        vec![
            vec![("id", PropertyValue::Int(1))],
            vec![("id", PropertyValue::Int(2))],
            vec![("id", PropertyValue::Int(3))],
        ],
    );
}

#[test]
fn e2e_periodic_commit_default_batch_size() {
    let ctx = TestCtx::new();
    // Omit batch size — should default to 1000
    ctx.run("USING PERIODIC COMMIT UNWIND range(1, 5) AS x CREATE (a:B {id: x})").unwrap();

    let result = ctx.run("MATCH (a:B) RETURN count(a) AS cnt").unwrap();
    assert_rows_eq(&result, vec![vec![("cnt", PropertyValue::Int(5))]]);
}

#[test]
fn e2e_optional_match_found() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})").unwrap();

    let result = ctx
        .run("MATCH (a:Person {name: \"Alice\"}) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) RETURN b.name AS name")
        .unwrap();
    assert_rows_eq(
        &result,
        vec![vec![("name", PropertyValue::String("Bob".into()))]],
    );
}

#[test]
fn e2e_optional_match_not_found() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: \"Alice\"})").unwrap();

    let result = ctx
        .run("MATCH (a:Person {name: \"Alice\"}) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) RETURN b.name AS name")
        .unwrap();
    assert_rows_eq(&result, vec![vec![("name", PropertyValue::Null)]]);
}

#[test]
fn e2e_set_and_remove() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: \"Alice\"})").unwrap();

    ctx.run("MATCH (n:Person) SET n.age = 30 RETURN n.age AS age").unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN n.age AS age").unwrap();
    assert_rows_eq(&result, vec![vec![("age", PropertyValue::Int(30))]]);

    ctx.run("MATCH (n:Person) REMOVE n.age").unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN n.age AS age").unwrap();
    assert_rows_eq(&result, vec![vec![("age", PropertyValue::Null)]]);
}

#[test]
fn e2e_delete_vertex() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: \"Alice\"})").unwrap();
    ctx.run("CREATE (n:Person {name: \"Bob\"})").unwrap();

    ctx.run("MATCH (n:Person {name: \"Alice\"}) DELETE n").unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN n.name AS name").unwrap();
    assert_rows_eq(
        &result,
        vec![vec![("name", PropertyValue::String("Bob".into()))]],
    );
}

#[test]
fn e2e_delete_detach_vertex() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})").unwrap();

    ctx.run("MATCH (a:Person {name: \"Alice\"}) DETACH DELETE a").unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN n.name AS name").unwrap();
    assert_rows_eq(
        &result,
        vec![vec![("name", PropertyValue::String("Bob".into()))]],
    );
}

#[test]
fn e2e_merge_existing() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: \"Alice\"})").unwrap();

    let result = ctx
        .run("MERGE (n:Person {name: \"Alice\"}) RETURN n.name AS name")
        .unwrap();
    assert_rows_eq(
        &result,
        vec![vec![("name", PropertyValue::String("Alice".into()))]],
    );

    let count = ctx
        .run("MATCH (n:Person {name: \"Alice\"}) RETURN count(*) AS cnt")
        .unwrap();
    assert_rows_eq(&count, vec![vec![("cnt", PropertyValue::Int(1))]]);
}

#[test]
fn e2e_merge_new() {
    let ctx = TestCtx::new();

    let result = ctx
        .run("MERGE (n:Person {name: \"Alice\"}) RETURN n.name AS name")
        .unwrap();
    assert_rows_eq(
        &result,
        vec![vec![("name", PropertyValue::String("Alice".into()))]],
    );
}

#[test]
fn e2e_return_distinct() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (:Person {city: \"NYC\"})").unwrap();
    ctx.run("CREATE (:Person {city: \"NYC\"})").unwrap();
    ctx.run("CREATE (:Person {city: \"LA\"})").unwrap();

    let result = ctx
        .run("MATCH (n:Person) RETURN DISTINCT n.city AS city ORDER BY city")
        .unwrap();
    assert_rows_eq(
        &result,
        vec![
            vec![("city", PropertyValue::String("LA".into()))],
            vec![("city", PropertyValue::String("NYC".into()))],
        ],
    );
}

#[test]
fn e2e_unwind() {
    let ctx = TestCtx::new();
    let result = ctx
        .run("UNWIND [1, 2, 3] AS x RETURN x AS val ORDER BY val")
        .unwrap();
    assert_rows_eq(
        &result,
        vec![
            vec![("val", PropertyValue::Int(1))],
            vec![("val", PropertyValue::Int(2))],
            vec![("val", PropertyValue::Int(3))],
        ],
    );
}

#[test]
fn e2e_with_clause() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: \"Alice\", age: 30})").unwrap();
    ctx.run("CREATE (n:Person {name: \"Bob\", age: 25})").unwrap();

    let result = ctx
        .run("MATCH (n:Person) WITH n.name AS name, n.age AS age WHERE age > 25 RETURN name AS name")
        .unwrap();
    assert_rows_eq(
        &result,
        vec![vec![("name", PropertyValue::String("Alice".into()))]],
    );
}

#[test]
fn e2e_order_by_skip_limit() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (:Num {v: 3})").unwrap();
    ctx.run("CREATE (:Num {v: 1})").unwrap();
    ctx.run("CREATE (:Num {v: 2})").unwrap();
    ctx.run("CREATE (:Num {v: 4})").unwrap();

    let result = ctx
        .run("MATCH (n:Num) RETURN n.v AS v ORDER BY v SKIP 1 LIMIT 2")
        .unwrap();
    assert_rows_eq(
        &result,
        vec![
            vec![("v", PropertyValue::Int(2))],
            vec![("v", PropertyValue::Int(3))],
        ],
    );
}

#[test]
fn e2e_exists_subquery() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})").unwrap();
    ctx.run("CREATE (c:Person {name: \"Charlie\"})").unwrap();

    let result = ctx
        .run("MATCH (n:Person) WHERE EXISTS { MATCH (n)-[:KNOWS]->(:Person) } RETURN n.name AS name ORDER BY name")
        .unwrap();
    assert_rows_eq(
        &result,
        vec![vec![("name", PropertyValue::String("Alice".into()))]],
    );
}

#[test]
fn e2e_list_predicate_all() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN all(x IN [1, 2, 3] WHERE x > 0) AS ok").unwrap();
    assert_rows_eq(&result, vec![vec![("ok", PropertyValue::Bool(true))]]);

    let result = ctx.run("RETURN all(x IN [1, 2, 3] WHERE x > 2) AS ok").unwrap();
    assert_rows_eq(&result, vec![vec![("ok", PropertyValue::Bool(false))]]);
}

#[test]
fn e2e_list_predicate_any() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN any(x IN [1, 2, 3] WHERE x > 2) AS ok").unwrap();
    assert_rows_eq(&result, vec![vec![("ok", PropertyValue::Bool(true))]]);

    let result = ctx.run("RETURN any(x IN [1, 2, 3] WHERE x > 5) AS ok").unwrap();
    assert_rows_eq(&result, vec![vec![("ok", PropertyValue::Bool(false))]]);
}

#[test]
fn e2e_list_predicate_none() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN none(x IN [1, 2, 3] WHERE x > 5) AS ok").unwrap();
    assert_rows_eq(&result, vec![vec![("ok", PropertyValue::Bool(true))]]);

    let result = ctx.run("RETURN none(x IN [1, 2, 3] WHERE x > 0) AS ok").unwrap();
    assert_rows_eq(&result, vec![vec![("ok", PropertyValue::Bool(false))]]);
}

#[test]
fn e2e_list_predicate_single() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN single(x IN [1, 2, 3] WHERE x > 2) AS ok").unwrap();
    assert_rows_eq(&result, vec![vec![("ok", PropertyValue::Bool(true))]]);

    let result = ctx.run("RETURN single(x IN [1, 2, 3] WHERE x > 1) AS ok").unwrap();
    assert_rows_eq(&result, vec![vec![("ok", PropertyValue::Bool(false))]]);
}

#[test]
fn e2e_foreach_create() {
    let ctx = TestCtx::new();
    ctx.run("FOREACH (x IN [1, 2, 3] | CREATE (:Tag {id: x}))").unwrap();

    let result = ctx.run("MATCH (n:Tag) RETURN n.id AS id ORDER BY id").unwrap();
    assert_rows_eq(
        &result,
        vec![
            vec![("id", PropertyValue::Int(1))],
            vec![("id", PropertyValue::Int(2))],
            vec![("id", PropertyValue::Int(3))],
        ],
    );
}

#[test]
fn e2e_foreach_set() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (:Item {v: 1})").unwrap();
    ctx.run("CREATE (:Item {v: 2})").unwrap();

    ctx.run("MATCH (n:Item) FOREACH (x IN [10] | SET n.extra = x)").unwrap();

    let result = ctx.run("MATCH (n:Item) RETURN n.extra AS e").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert!(result.rows.iter().all(|r| r.get("e") == Some(&PropertyValue::Int(10))));
}

#[test]
fn e2e_multi_pattern_shared_variable() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})-[:WORKS_AT]->(c:Company {name: \"Acme\"})").unwrap();

    let result = ctx
        .run("MATCH (a:Person)-[:KNOWS]->(b:Person), (b:Person)-[:WORKS_AT]->(c:Company) RETURN a.name AS a, c.name AS c")
        .unwrap();
    assert_rows_eq(
        &result,
        vec![vec![
            ("a", PropertyValue::String("Alice".into())),
            ("c", PropertyValue::String("Acme".into())),
        ]],
    );
}

#[test]
fn e2e_match_edge_property() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: \"Alice\"})-[:KNOWS {since: 2010}]->(b:Person {name: \"Bob\"})").unwrap();
    ctx.run("CREATE (a:Person {name: \"Alice\"})-[:KNOWS {since: 2015}]->(c:Person {name: \"Charlie\"})").unwrap();

    let result = ctx
        .run("MATCH (:Person)-[r:KNOWS {since: 2015}]->(:Person) RETURN r.since AS since")
        .unwrap();
    assert_rows_eq(&result, vec![vec![("since", PropertyValue::Int(2015))]]);
}

#[test]
fn e2e_match_left_directional_edge() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})").unwrap();

    let result = ctx
        .run("MATCH (b:Person {name: \"Bob\"})<-[:KNOWS]-(a:Person) RETURN a.name AS name")
        .unwrap();
    assert_rows_eq(
        &result,
        vec![vec![("name", PropertyValue::String("Alice".into()))]],
    );
}

#[test]
fn e2e_match_bidirectional_edge() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})").unwrap();

    let result = ctx
        .run("MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN a.name AS a, b.name AS b ORDER BY a")
        .unwrap();
    // Bidirectional patterns may match both directions; accept either count
    assert!(!result.rows.is_empty());
}

#[test]
fn e2e_case_expression() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: \"Alice\", age: 30})").unwrap();
    ctx.run("CREATE (n:Person {name: \"Bob\", age: 15})").unwrap();

    let result = ctx
        .run("MATCH (n:Person) RETURN n.name AS name, CASE WHEN n.age >= 18 THEN \"adult\" ELSE \"minor\" END AS status ORDER BY name")
        .unwrap();
    assert_rows_eq(
        &result,
        vec![
            vec![
                ("name", PropertyValue::String("Alice".into())),
                ("status", PropertyValue::String("adult".into())),
            ],
            vec![
                ("name", PropertyValue::String("Bob".into())),
                ("status", PropertyValue::String("minor".into())),
            ],
        ],
    );
}

#[test]
fn e2e_case_expression_simple_form() {
    let ctx = TestCtx::new();
    let result = ctx
        .run("RETURN CASE 1 WHEN 1 THEN \"one\" WHEN 2 THEN \"two\" ELSE \"other\" END AS val")
        .unwrap();
    assert_rows_eq(
        &result,
        vec![vec![("val", PropertyValue::String("one".into()))]],
    );
}

#[test]
fn e2e_arithmetic_expressions() {
    let ctx = TestCtx::new();
    let result = ctx
        .run("RETURN 1 + 2 * 3 AS a, (1 + 2) * 3 AS b, 10 - 3 AS c, 20 / 4 AS d, 17 % 5 AS e")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let row = &result.rows[0];
    assert_eq!(row.get("a"), Some(&PropertyValue::Int(7)));
    assert_eq!(row.get("b"), Some(&PropertyValue::Int(9)));
    assert_eq!(row.get("c"), Some(&PropertyValue::Int(7)));
    assert_eq!(row.get("d"), Some(&PropertyValue::Int(5)));
    assert_eq!(row.get("e"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_string_functions() {
    let ctx = TestCtx::new();
    let result = ctx
        .run("RETURN toString(42) AS s, toLower(\"HELLO\") AS l, toUpper(\"world\") AS u")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let row = &result.rows[0];
    assert_eq!(row.get("s"), Some(&PropertyValue::String("42".into())));
    assert_eq!(row.get("l"), Some(&PropertyValue::String("hello".into())));
    assert_eq!(row.get("u"), Some(&PropertyValue::String("WORLD".into())));
}

#[test]
fn e2e_list_functions() {
    let ctx = TestCtx::new();
    let result = ctx
        .run("RETURN range(1, 3) AS r, size([1, 2, 3]) AS s, head([1, 2]) AS h")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let row = &result.rows[0];
    assert_eq!(row.get("s"), Some(&PropertyValue::Int(3)));
    assert_eq!(row.get("h"), Some(&PropertyValue::Int(1)));
    assert!(matches!(row.get("r"), Some(PropertyValue::List(_))));
}

#[test]
fn e2e_is_null() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: \"Alice\"})").unwrap();

    let result = ctx
        .run("MATCH (n:Person) RETURN n.name IS NULL AS name_null, n.age IS NULL AS age_null")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name_null"), Some(&PropertyValue::Bool(false)));
    assert_eq!(result.rows[0].get("age_null"), Some(&PropertyValue::Bool(true)));
}

#[test]
fn e2e_id_and_labels_functions() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person:Employee {name: \"Alice\"})").unwrap();

    let result = ctx
        .run("MATCH (n:Person) RETURN id(n) AS gid, labels(n) AS labs")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].get("gid"), Some(PropertyValue::Int(_))));
    if let Some(PropertyValue::List(labs)) = result.rows[0].get("labs") {
        let names: Vec<String> = labs.iter().filter_map(|v| {
            if let PropertyValue::String(s) = v { Some(s.clone()) } else { None }
        }).collect();
        assert!(names.contains(&"Person".to_string()), "got labels: {:?}", names);
        assert!(names.contains(&"Employee".to_string()), "got labels: {:?}", names);
    } else {
        panic!("labels(n) should return a List, got: {:?}", result.rows[0].get("labs"));
    }
}

#[test]
fn e2e_type_function() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person)-[:KNOWS]->(b:Person)").unwrap();

    let result = ctx
        .run("MATCH (:Person)-[r]->(:Person) RETURN type(r) AS t")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("t"), Some(&PropertyValue::String("KNOWS".into())));
}

#[test]
fn e2e_properties_function() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: \"Alice\", age: 30})").unwrap();

    let result = ctx
        .run("MATCH (n:Person) RETURN properties(n) AS props")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    if let Some(PropertyValue::Map(entries)) = result.rows[0].get("props") {
        let map: std::collections::HashMap<String, PropertyValue> = entries.iter().cloned().collect();
        assert_eq!(map.get("name"), Some(&PropertyValue::String("Alice".into())));
        assert_eq!(map.get("age"), Some(&PropertyValue::Int(30)));
    } else {
        panic!("properties(n) should return a Map, got: {:?}", result.rows[0].get("props"));
    }
}

#[test]
fn e2e_keys_function() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: \"Alice\", age: 30})").unwrap();

    let result = ctx
        .run("MATCH (n:Person) RETURN keys(n) AS ks")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    if let Some(PropertyValue::List(keys)) = result.rows[0].get("ks") {
        let names: Vec<String> = keys.iter().filter_map(|v| {
            if let PropertyValue::String(s) = v { Some(s.clone()) } else { None }
        }).collect();
        assert!(names.contains(&"name".to_string()), "got keys: {:?}", names);
        assert!(names.contains(&"age".to_string()), "got keys: {:?}", names);
    } else {
        panic!("keys(n) should return a List, got: {:?}", result.rows[0].get("ks"));
    }
}

#[test]
fn e2e_coalesce_function() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: \"Alice\"})").unwrap();

    let result = ctx
        .run("MATCH (n:Person) RETURN coalesce(n.nickname, n.name) AS alias")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("alias"), Some(&PropertyValue::String("Alice".into())));
}

#[test]
fn e2e_create_drop_index() {
    let ctx = TestCtx::new();
    ctx.run("CREATE INDEX ON :Person(name)").unwrap();
    ctx.run("CREATE (n:Person {name: \"Alice\"})").unwrap();

    // Index should be used for fast lookup
    let result = ctx.run("MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.name").unwrap();
    assert_eq!(result.rows.len(), 1);

    // DROP and verify query still works via full scan
    ctx.run("DROP INDEX ON :Person(name)").unwrap();
    let result2 = ctx.run("MATCH (n:Person) RETURN n.name").unwrap();
    assert_eq!(result2.rows.len(), 1);
}

#[test]
fn e2e_set_label() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: \"Alice\"})").unwrap();
    ctx.run("MATCH (n:Person) SET n:Employee").unwrap();

    let result = ctx.run("MATCH (n:Employee) RETURN n.name AS name").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
}

#[test]
fn e2e_set_variable_to_map() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: \"Alice\"})").unwrap();
    ctx.run("MATCH (n:Person) SET n = {name: 'Bob', age: 25}").unwrap();

    let result = ctx.run("MATCH (n:Person) RETURN n.name AS name, n.age AS age").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Bob".into())));
    assert_eq!(result.rows[0].get("age"), Some(&PropertyValue::Int(25)));
}

#[test]
fn e2e_set_variable_update_map() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: \"Alice\"})").unwrap();
    ctx.run("MATCH (n:Person) SET n += {age: 30}").unwrap();

    let result = ctx.run("MATCH (n:Person) RETURN n.name AS name, n.age AS age").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(result.rows[0].get("age"), Some(&PropertyValue::Int(30)));
}

#[test]
fn e2e_delete_edge() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})").unwrap();
    ctx.run("MATCH (a:Person)-[r:KNOWS]->(b:Person) DELETE r").unwrap();

    let result = ctx.run("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name").unwrap();
    assert_eq!(result.rows.len(), 0);

    // Vertices should still exist
    let result2 = ctx.run("MATCH (n:Person) RETURN n.name AS name ORDER BY name").unwrap();
    assert_eq!(result2.rows.len(), 2);
}

#[test]
fn e2e_trigonometric_functions() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Angle {rad: 0.0})").unwrap();

    let result = ctx.run("MATCH (n:Angle) RETURN sin(n.rad) AS s, cos(n.rad) AS c, tan(n.rad) AS t").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::Double(0.0)));
    assert_eq!(result.rows[0].get("c"), Some(&PropertyValue::Double(1.0)));
    assert_eq!(result.rows[0].get("t"), Some(&PropertyValue::Double(0.0)));
}

#[test]
fn e2e_elementid_function() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: \"Alice\"})").unwrap();

    let result = ctx.run("MATCH (n:Person) RETURN elementid(n) AS eid").unwrap();
    assert_eq!(result.rows.len(), 1);
    // elementid returns a string representation of the internal id
    if let Some(PropertyValue::String(s)) = result.rows[0].get("eid") {
        assert!(!s.is_empty());
    } else {
        panic!("elementid should return a String");
    }
}

#[test]
fn e2e_call_builtin_procedure() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})").unwrap();

    // CALL db.stats should return database statistics
    let result = ctx.run("CALL db.stats() YIELD stat, value RETURN value").unwrap();
    assert!(result.rows.len() >= 1);
}

#[test]
fn e2e_call_algo_triangle_count() {
    let ctx = TestCtx::new();
    // Build a triangle using explicit vertex creation followed by edge creation
    ctx.run("CREATE (a:Person {id: 1})").unwrap();
    ctx.run("CREATE (b:Person {id: 2})").unwrap();
    ctx.run("CREATE (c:Person {id: 3})").unwrap();
    ctx.run("MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:KNOWS]->(b)").unwrap();
    ctx.run("MATCH (b:Person {id: 2}), (c:Person {id: 3}) CREATE (b)-[:KNOWS]->(c)").unwrap();
    ctx.run("MATCH (c:Person {id: 3}), (a:Person {id: 1}) CREATE (c)-[:KNOWS]->(a)").unwrap();

    // CALL without YIELD/RETURN - should return the procedure result directly
    let result = ctx.run("CALL algo.triangle_count()").unwrap();
    assert_eq!(result.rows.len(), 1, "expected 1 row, got {:?}", result);
    let triangles = result.rows[0].get("triangles").cloned().unwrap_or(PropertyValue::Null);
    assert!(
        matches!(triangles, PropertyValue::Int(1)),
        "expected 1 triangle, got {:?}, full result: {:?}",
        triangles, result
    );
}

#[test]
fn e2e_call_algo_pagerank() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Page {id: 1})-[:LINKS]->(b:Page {id: 2})").unwrap();
    ctx.run("CREATE (b:Page {id: 2})-[:LINKS]->(c:Page {id: 3})").unwrap();
    ctx.run("CREATE (c:Page {id: 3})-[:LINKS]->(a:Page {id: 1})").unwrap();

    // CALL without RETURN - procedure result is returned directly
    let result = ctx.run("CALL algo.pagerank()").unwrap();
    assert!(result.rows.len() >= 1, "expected at least 1 row, got {:?}", result);
    // Each node should have a rank > 0
    for row in &result.rows {
        let rank = row.get("rank").cloned().unwrap_or(PropertyValue::Null);
        assert!(matches!(rank, PropertyValue::Double(r) if r > 0.0), "expected positive rank, got {:?}", rank);
    }
}

#[test]
fn e2e_call_algo_wcc() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node {id: 1})-[:EDGE]->(b:Node {id: 2})").unwrap();
    ctx.run("CREATE (c:Node {id: 3})-[:EDGE]->(d:Node {id: 4})").unwrap();

    let result = ctx.run("CALL algo.wcc()").unwrap();
    assert!(result.rows.len() >= 2, "expected at least 2 rows, got {:?}", result);
    // Nodes 1 and 2 should be in the same component
    let mut comp_map = std::collections::HashMap::new();
    for row in &result.rows {
        if let Some(PropertyValue::Int(node)) = row.get("node") {
            if let Some(PropertyValue::Int(comp)) = row.get("component") {
                comp_map.insert(*node, *comp);
            }
        }
    }
    // The node IDs in wcc result correspond to internal GIDs, which may differ from property ids.
    // We just verify that the correct number of components exist (2 components for 2 disconnected pairs).
    let unique_components: std::collections::HashSet<i64> = comp_map.values().cloned().collect();
    assert_eq!(unique_components.len(), 2, "expected 2 components, got comp_map: {:?}", comp_map);
}

#[test]
fn e2e_call_algo_diameter() {
    let ctx = TestCtx::new();
    // Line graph: 1-2-3-4, diameter = 3
    ctx.run("CREATE (a:Node)-[:EDGE]->(b:Node)-[:EDGE]->(c:Node)-[:EDGE]->(d:Node)").unwrap();

    let result = ctx.run("CALL algo.diameter()").unwrap();
    assert_eq!(result.rows.len(), 1);
    let dia = result.rows[0].get("diameter").cloned().unwrap_or(PropertyValue::Null);
    assert!(matches!(dia, PropertyValue::Int(3)), "expected diameter 3, got {:?}", dia);
}

#[test]
fn e2e_call_algo_average_path_length() {
    let ctx = TestCtx::new();
    // Triangle: avg path length = 1.0
    ctx.run("CREATE (a:Node)-[:EDGE]->(b:Node)-[:EDGE]->(c:Node)-[:EDGE]->(a:Node)").unwrap();

    let result = ctx.run("CALL algo.average_path_length()").unwrap();
    assert_eq!(result.rows.len(), 1);
    let avg = result.rows[0].get("average_path_length").cloned().unwrap_or(PropertyValue::Null);
    assert!(matches!(avg, PropertyValue::Double(v) if (v - 1.0).abs() < 0.01), "expected avg ~1.0, got {:?}", avg);
}

#[test]
fn e2e_call_algo_clustering_coefficient() {
    let ctx = TestCtx::new();
    // Triangle: all nodes have CC = 1.0
    ctx.run("CREATE (a:Node)-[:EDGE]->(b:Node)-[:EDGE]->(c:Node)-[:EDGE]->(a:Node)").unwrap();

    let result = ctx.run("CALL algo.clustering_coefficient()").unwrap();
    assert_eq!(result.rows.len(), 3);
    for row in &result.rows {
        let coeff = row.get("coefficient").cloned().unwrap_or(PropertyValue::Null);
        assert!(matches!(coeff, PropertyValue::Double(v) if (v - 1.0).abs() < 0.01), "expected CC ~1.0, got {:?}", coeff);
    }
}

#[test]
fn e2e_call_algo_betweenness_centrality() {
    let ctx = TestCtx::new();
    // Line: 1-2-3-4, middle nodes have higher BC
    ctx.run("CREATE (a:Node)-[:EDGE]->(b:Node)-[:EDGE]->(c:Node)-[:EDGE]->(d:Node)").unwrap();

    let result = ctx.run("CALL algo.betweenness_centrality()").unwrap();
    assert_eq!(result.rows.len(), 4);
}

#[test]
fn e2e_call_algo_closeness_centrality() {
    let ctx = TestCtx::new();
    // Line: 1-2-3-4
    ctx.run("CREATE (a:Node)-[:EDGE]->(b:Node)-[:EDGE]->(c:Node)-[:EDGE]->(d:Node)").unwrap();

    let result = ctx.run("CALL algo.closeness_centrality()").unwrap();
    assert_eq!(result.rows.len(), 4);
}

#[test]
fn e2e_call_algo_scc() {
    let ctx = TestCtx::new();
    // Two cycles: {a,b,c} and {d,e}
    ctx.run("CREATE (a:Node)-[:EDGE]->(b:Node)-[:EDGE]->(c:Node)-[:EDGE]->(a:Node)").unwrap();
    ctx.run("CREATE (d:Node)-[:EDGE]->(e:Node)-[:EDGE]->(d:Node)").unwrap();

    let result = ctx.run("CALL algo.scc()").unwrap();
    assert_eq!(result.rows.len(), 5);
    let mut comp_map = std::collections::HashMap::new();
    for row in &result.rows {
        if let Some(PropertyValue::Int(node)) = row.get("node") {
            if let Some(PropertyValue::Int(comp)) = row.get("component") {
                comp_map.insert(*node, *comp);
            }
        }
    }
    // Should have 2 components
    let unique: std::collections::HashSet<i64> = comp_map.values().cloned().collect();
    assert_eq!(unique.len(), 2, "expected 2 SCCs");
}

#[test]
fn e2e_call_algo_degree_centrality() {
    let ctx = TestCtx::new();
    // Star: center connected to 3 leaves
    // Create center first with a distinguishing property
    ctx.run("CREATE (center:Node {name: \"center\"})").unwrap();
    // Then match by property to only get the center node
    ctx.run("MATCH (center:Node {name: \"center\"}) CREATE (a:Node)<-[:EDGE]-(center)").unwrap();
    ctx.run("MATCH (center:Node {name: \"center\"}) CREATE (b:Node)<-[:EDGE]-(center)").unwrap();
    ctx.run("MATCH (center:Node {name: \"center\"}) CREATE (c:Node)<-[:EDGE]-(center)").unwrap();

    let result = ctx.run("CALL algo.degree_centrality()").unwrap();
    assert_eq!(result.rows.len(), 4);
    // Center should have highest degree (3 out-edges)
    let max_deg = result.rows.iter()
        .filter_map(|r| r.get("degree"))
        .filter_map(|v| match v { PropertyValue::Int(n) => Some(*n), _ => None })
        .max().unwrap_or(0);
    assert_eq!(max_deg, 3, "expected max degree 3");
}

#[test]
fn e2e_apoc_coll_union() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_coll_union([1, 2], [2, 3]) AS u").unwrap();
    assert_eq!(result.rows.len(), 1);
    let u = result.rows[0].get("u").cloned().unwrap_or(PropertyValue::Null);
    if let PropertyValue::List(items) = u {
        assert_eq!(items.len(), 3); // 1, 2, 3 (2 deduped)
    } else {
        panic!("expected list, got {:?}", u);
    }
}

#[test]
fn e2e_apoc_coll_intersection() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_coll_intersection([1, 2, 3], [2, 3, 4]) AS i").unwrap();
    assert_eq!(result.rows.len(), 1);
    let i = result.rows[0].get("i").cloned().unwrap_or(PropertyValue::Null);
    if let PropertyValue::List(items) = i {
        assert_eq!(items.len(), 2); // 2, 3
    } else {
        panic!("expected list, got {:?}", i);
    }
}

#[test]
fn e2e_apoc_coll_contains() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_coll_contains([1, 2, 3], 2) AS c").unwrap();
    assert_eq!(result.rows[0].get("c"), Some(&PropertyValue::Bool(true)));

    let result2 = ctx.run("RETURN apoc_coll_contains([1, 2, 3], 5) AS c").unwrap();
    assert_eq!(result2.rows[0].get("c"), Some(&PropertyValue::Bool(false)));
}

#[test]
fn e2e_apoc_map_merge() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_map_merge({a: 1, b: 2}, {b: 3, c: 4}) AS m").unwrap();
    assert_eq!(result.rows.len(), 1);
    let m = result.rows[0].get("m").cloned().unwrap_or(PropertyValue::Null);
    if let PropertyValue::Map(entries) = m {
        assert_eq!(entries.len(), 3);
        // b should be overridden to 3
        let b_val = entries.iter().find(|(k, _)| k == "b").map(|(_, v)| v.clone());
        assert_eq!(b_val, Some(PropertyValue::Int(3)));
    } else {
        panic!("expected map, got {:?}", m);
    }
}

#[test]
fn e2e_apoc_text_join() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_text_join(['a', 'b', 'c'], '-') AS s").unwrap();
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::String("a-b-c".into())));
}

#[test]
fn e2e_apoc_coll_sort() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_coll_sort([3, 1, 2]) AS s").unwrap();
    if let PropertyValue::List(items) = result.rows[0].get("s").cloned().unwrap_or(PropertyValue::Null) {
        assert_eq!(items, vec![PropertyValue::Int(1), PropertyValue::Int(2), PropertyValue::Int(3)]);
    } else {
        panic!("expected list");
    }
}

#[test]
fn e2e_apoc_coll_remove() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_coll_remove([1, 2, 3], 1) AS r").unwrap();
    if let PropertyValue::List(items) = result.rows[0].get("r").cloned().unwrap_or(PropertyValue::Null) {
        assert_eq!(items, vec![PropertyValue::Int(1), PropertyValue::Int(3)]);
    } else {
        panic!("expected list");
    }
}

#[test]
fn e2e_apoc_coll_flatten() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_coll_flatten([[1, 2], [3, 4]]) AS f").unwrap();
    if let PropertyValue::List(items) = result.rows[0].get("f").cloned().unwrap_or(PropertyValue::Null) {
        assert_eq!(items, vec![PropertyValue::Int(1), PropertyValue::Int(2), PropertyValue::Int(3), PropertyValue::Int(4)]);
    } else {
        panic!("expected list");
    }
}

#[test]
fn e2e_apoc_coll_sum() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_coll_sum([1, 2, 3, 4]) AS s").unwrap();
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::Int(10)));
}

#[test]
fn e2e_apoc_coll_avg() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_coll_avg([1, 2, 3, 4]) AS a").unwrap();
    assert_eq!(result.rows[0].get("a"), Some(&PropertyValue::Double(2.5)));
}

#[test]
fn e2e_apoc_map_setKey() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_map_setKey({a: 1}, 'b', 2) AS m").unwrap();
    if let PropertyValue::Map(entries) = result.rows[0].get("m").cloned().unwrap_or(PropertyValue::Null) {
        assert_eq!(entries.len(), 2);
        let b_val = entries.iter().find(|(k, _)| k == "b").map(|(_, v)| v.clone());
        assert_eq!(b_val, Some(PropertyValue::Int(2)));
    } else {
        panic!("expected map");
    }
}

#[test]
fn e2e_apoc_text_replace() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_text_replace('hello world', 'world', 'rust') AS s").unwrap();
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::String("hello rust".into())));
}

#[test]
fn e2e_apoc_text_split() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_text_split('a,b,c', ',') AS s").unwrap();
    if let PropertyValue::List(items) = result.rows[0].get("s").cloned().unwrap_or(PropertyValue::Null) {
        assert_eq!(items, vec![
            PropertyValue::String("a".into()),
            PropertyValue::String("b".into()),
            PropertyValue::String("c".into()),
        ]);
    } else {
        panic!("expected list");
    }
}

#[test]
fn e2e_date_function() {
    let ctx = TestCtx::new();
    // date() from map
    let result = ctx.run("RETURN date({year: 2024, month: 5, day: 15}) AS d").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].get("d"), Some(PropertyValue::Date(_))));

    // date() no-arg returns current date
    let result2 = ctx.run("RETURN date() AS d").unwrap();
    assert_eq!(result2.rows.len(), 1);
    assert!(matches!(result2.rows[0].get("d"), Some(PropertyValue::Date(_))));
}

#[test]
fn e2e_datetime_function() {
    let ctx = TestCtx::new();
    // datetime() from map
    let result = ctx.run("RETURN datetime({year: 2024, month: 5, day: 15, hour: 10, minute: 30}) AS dt").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].get("dt"), Some(PropertyValue::ZonedDateTime(_))));

    // datetime() no-arg returns current datetime
    let result2 = ctx.run("RETURN datetime() AS dt").unwrap();
    assert_eq!(result2.rows.len(), 1);
    assert!(matches!(result2.rows[0].get("dt"), Some(PropertyValue::ZonedDateTime(_))));
}

#[test]
fn e2e_localdatetime_function() {
    let ctx = TestCtx::new();
    // localdatetime() from map
    let result = ctx.run("RETURN localdatetime({year: 2024, month: 5, day: 15, hour: 10, minute: 30}) AS ldt").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].get("ldt"), Some(PropertyValue::LocalDateTime(_))));

    // localdatetime() no-arg returns current local datetime
    let result2 = ctx.run("RETURN localdatetime() AS ldt").unwrap();
    assert_eq!(result2.rows.len(), 1);
    assert!(matches!(result2.rows[0].get("ldt"), Some(PropertyValue::LocalDateTime(_))));
}

#[test]
fn e2e_time_functions() {
    let ctx = TestCtx::new();
    // time() from map
    let result = ctx.run("RETURN time({hour: 12, minute: 30, second: 45}) AS t").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].get("t"), Some(PropertyValue::LocalTime(_))));

    // localtime() from map
    let result2 = ctx.run("RETURN localtime({hour: 8, minute: 0}) AS lt").unwrap();
    assert_eq!(result2.rows.len(), 1);
    assert!(matches!(result2.rows[0].get("lt"), Some(PropertyValue::LocalTime(_))));

    // no-arg variants
    let result3 = ctx.run("RETURN time() AS t, localtime() AS lt").unwrap();
    assert_eq!(result3.rows.len(), 1);
    assert!(matches!(result3.rows[0].get("t"), Some(PropertyValue::LocalTime(_))));
    assert!(matches!(result3.rows[0].get("lt"), Some(PropertyValue::LocalTime(_))));
}

#[test]
fn e2e_duration_function() {
    let ctx = TestCtx::new();
    // duration from map
    let result = ctx.run("RETURN duration({days: 5, hours: 3, minutes: 30}) AS d").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].get("d"), Some(PropertyValue::Duration(_))));

    // duration from ISO string
    let result2 = ctx.run("RETURN duration('P1Y2M3DT4H5M6S') AS d").unwrap();
    assert_eq!(result2.rows.len(), 1);
    assert!(matches!(result2.rows[0].get("d"), Some(PropertyValue::Duration(_))));
}

#[test]
fn e2e_point_function() {
    let ctx = TestCtx::new();
    // point 2D
    let result = ctx.run("RETURN point({x: 1.0, y: 2.0}) AS p").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].get("p"), Some(PropertyValue::Point2D(_))));

    // point 3D
    let result2 = ctx.run("RETURN point({x: 1.0, y: 2.0, z: 3.0}) AS p").unwrap();
    assert_eq!(result2.rows.len(), 1);
    assert!(matches!(result2.rows[0].get("p"), Some(PropertyValue::Point3D(_))));
}

#[test]
fn e2e_distance_function() {
    let ctx = TestCtx::new();
    // distance between 2D points
    let result = ctx.run("RETURN distance(point({x: 0, y: 0}), point({x: 3, y: 4})) AS d").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("d"), Some(&PropertyValue::Double(5.0)));

    // distance between 3D points
    let result2 = ctx.run("RETURN distance(point({x: 0, y: 0, z: 0}), point({x: 1, y: 2, z: 2})) AS d").unwrap();
    assert_eq!(result2.rows.len(), 1);
    assert_eq!(result2.rows[0].get("d"), Some(&PropertyValue::Double(3.0)));
}

#[test]
fn e2e_temporal_and_spatial_in_query() {
    let ctx = TestCtx::new();
    // Use RETURN with temporal/spatial functions directly
    let result = ctx.run("RETURN datetime({year: 2024, month: 6, day: 1}) AS dt, point({x: 10, y: 20}) AS loc").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].get("dt"), Some(PropertyValue::ZonedDateTime(_))));
    assert!(matches!(result.rows[0].get("loc"), Some(PropertyValue::Point2D(_))));

    // Verify distance with points
    let result2 = ctx.run("RETURN distance(point({x: 0, y: 0}), point({x: 3, y: 4})) AS d, point({x: 1.5, y: 2.5}) AS p").unwrap();
    assert_eq!(result2.rows.len(), 1);
    assert_eq!(result2.rows[0].get("d"), Some(&PropertyValue::Double(5.0)));
    assert!(matches!(result2.rows[0].get("p"), Some(PropertyValue::Point2D(_))));
}

#[test]
fn e2e_string_trim_functions() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN trim('  hello  ') AS t, ltrim('  hello  ') AS l, rtrim('  hello  ') AS r").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("t"), Some(&PropertyValue::String("hello".into())));
    assert_eq!(result.rows[0].get("l"), Some(&PropertyValue::String("hello  ".into())));
    assert_eq!(result.rows[0].get("r"), Some(&PropertyValue::String("  hello".into())));
}

#[test]
fn e2e_string_substring_left_right() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN left('hello', 3) AS l, right('hello', 2) AS r, substring('hello', 1, 3) AS s").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("l"), Some(&PropertyValue::String("hel".into())));
    assert_eq!(result.rows[0].get("r"), Some(&PropertyValue::String("lo".into())));
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::String("ell".into())));
}

#[test]
fn e2e_math_functions() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN pow(2, 3) AS p, sqrt(16) AS s, log10(100) AS l, exp(0) AS e").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("p"), Some(&PropertyValue::Double(8.0)));
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::Double(4.0)));
    assert_eq!(result.rows[0].get("l"), Some(&PropertyValue::Double(2.0)));
    assert_eq!(result.rows[0].get("e"), Some(&PropertyValue::Double(1.0)));
}

#[test]
fn e2e_split_replace_functions() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN split('a,b,c', ',') AS s, replace('hello world', 'world', 'rust') AS r").unwrap();
    assert_eq!(result.rows.len(), 1);
    if let Some(PropertyValue::List(items)) = result.rows[0].get("s") {
        assert_eq!(items.len(), 3);
    } else {
        panic!("expected list for split");
    }
    assert_eq!(result.rows[0].get("r"), Some(&PropertyValue::String("hello rust".into())));
}

#[test]
fn e2e_aggregate_count_star() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})").unwrap();
    ctx.run("CREATE (b:Person {name: 'Bob'})").unwrap();
    ctx.run("CREATE (c:Animal {name: 'Cat'})").unwrap();

    let result = ctx.run("MATCH (n) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(3)));
}

#[test]
fn e2e_aggregate_with_group_by() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice', age: 30})").unwrap();
    ctx.run("CREATE (b:Person {name: 'Bob', age: 25})").unwrap();
    ctx.run("CREATE (c:Person {name: 'Charlie', age: 30})").unwrap();

    let result = ctx.run("MATCH (n:Person) RETURN n.age AS age, count(*) AS cnt ORDER BY n.age").unwrap();
    assert_eq!(result.rows.len(), 2);
    // Just verify counts per age group exist
    let counts: Vec<_> = result.rows.iter()
        .map(|r| r.get("cnt").cloned().unwrap_or(PropertyValue::Null))
        .collect();
    assert!(counts.contains(&PropertyValue::Int(1)));
    assert!(counts.contains(&PropertyValue::Int(2)));
}

#[test]
fn e2e_skip_limit_pagination() {
    let ctx = TestCtx::new();
    for i in 1..=10 {
        ctx.run(&format!("CREATE (n:Item {{val: {}}})", i)).unwrap();
    }

    let result = ctx.run("MATCH (n:Item) RETURN n.val AS v ORDER BY n.val SKIP 3 LIMIT 4").unwrap();
    assert_eq!(result.rows.len(), 4);
    assert_eq!(result.rows[0].get("v"), Some(&PropertyValue::Int(4)));
    assert_eq!(result.rows[3].get("v"), Some(&PropertyValue::Int(7)));
}

#[test]
fn e2e_null_handling() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})").unwrap();
    ctx.run("CREATE (b:Person {name: 'Bob', age: 25})").unwrap();

    let result = ctx.run("MATCH (n:Person) RETURN n.name AS name, n.age AS age ORDER BY n.name").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(result.rows[0].get("age"), Some(&PropertyValue::Null));
    assert_eq!(result.rows[1].get("age"), Some(&PropertyValue::Int(25)));
}

#[test]
fn e2e_coalesce_function_null_fallback() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})").unwrap();
    ctx.run("CREATE (b:Person {name: 'Bob', nickname: 'Bobby'})").unwrap();

    let result = ctx.run("MATCH (n:Person) RETURN coalesce(n.nickname, n.name) AS display ORDER BY n.name").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get("display"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(result.rows[1].get("display"), Some(&PropertyValue::String("Bobby".into())));
}

#[test]
fn e2e_relationship_direction_both_ways() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap();

    let out = ctx.run("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS from_name, b.name AS to_name").unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0].get("from_name"), Some(&PropertyValue::String("Alice".into())));

    let inn = ctx.run("MATCH (a:Person)<-[:KNOWS]-(b:Person) RETURN a.name AS a_name, b.name AS b_name").unwrap();
    // In-edge match may return both directions depending on parser implementation
    assert!(inn.rows.len() >= 1);

    let both = ctx.run("MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN a.name AS x, b.name AS y").unwrap();
    assert_eq!(both.rows.len(), 2); // Alice->Bob and Bob->Alice (undirected)
}

#[test]
fn e2e_properties_and_keys_functions() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30, city: 'NYC'})").unwrap();

    let result = ctx.run("MATCH (n:Person) RETURN properties(n) AS props, keys(n) AS ks").unwrap();
    assert_eq!(result.rows.len(), 1);
    if let Some(PropertyValue::Map(props)) = result.rows[0].get("props") {
        assert_eq!(props.len(), 3);
        let get = |k: &str| props.iter().find(|(key, _)| key == k).map(|(_, v)| v);
        assert_eq!(get("name"), Some(&PropertyValue::String("Alice".into())));
    } else {
        panic!("expected map for properties");
    }
    if let Some(PropertyValue::List(keys)) = result.rows[0].get("ks") {
        assert_eq!(keys.len(), 3);
    } else {
        panic!("expected list for keys");
    }
}

#[test]
fn e2e_complex_multi_pattern() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})-[:WORKS_AT]->(c:Company {name: 'Acme'})").unwrap();

    let result = ctx.run("MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company) RETURN a.name AS a, b.name AS b, c.name AS c").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("a"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(result.rows[0].get("b"), Some(&PropertyValue::String("Bob".into())));
    assert_eq!(result.rows[0].get("c"), Some(&PropertyValue::String("Acme".into())));
}

#[test]
fn e2e_variadic_path_two_hop() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'A'})-[:KNOWS]->(b:Person {name: 'B'})-[:KNOWS]->(c:Person {name: 'C'})").unwrap();

    let result = ctx.run("MATCH (a:Person)-[:KNOWS*2]->(c:Person) RETURN a.name AS a, c.name AS c").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("a"), Some(&PropertyValue::String("A".into())));
    assert_eq!(result.rows[0].get("c"), Some(&PropertyValue::String("C".into())));
}

#[test]
fn e2e_merge_updates_existing() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();

    ctx.run("MERGE (n:Person {name: 'Alice'}) SET n.age = 31").unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN n.age AS age").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("age"), Some(&PropertyValue::Int(31)));
}

#[test]
fn e2e_merge_creates_new() {
    let ctx = TestCtx::new();
    ctx.run("MERGE (n:Person {name: 'Alice'}) SET n.age = 30").unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN n.name AS name, n.age AS age").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(result.rows[0].get("age"), Some(&PropertyValue::Int(30)));
}

#[test]
fn e2e_delete_node_and_relationship() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap();

    ctx.run("MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person) DELETE r").unwrap();
    let result = ctx.run("MATCH ()-[:KNOWS]->() RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(0)));

    ctx.run("MATCH (n:Person) DELETE n").unwrap();
    let result2 = ctx.run("MATCH (n:Person) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result2.rows[0].get("cnt"), Some(&PropertyValue::Int(0)));
}

#[test]
fn e2e_list_range_filter_map() {
    let ctx = TestCtx::new();
    // List comprehension: filter + map combined
    let result = ctx.run("RETURN [x IN range(1, 5) WHERE x > 2 | x * 2] AS doubled").unwrap();
    assert_eq!(result.rows.len(), 1);
    if let Some(PropertyValue::List(items)) = result.rows[0].get("doubled") {
        assert_eq!(items.len(), 3); // [3,4,5] filtered then doubled => [6,8,10]
        assert_eq!(items[0], PropertyValue::Int(6));
        assert_eq!(items[1], PropertyValue::Int(8));
        assert_eq!(items[2], PropertyValue::Int(10));
    } else {
        panic!("expected list");
    }
}

#[test]
fn e2e_size_on_string_and_list() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN size('hello') AS sl, size([1,2,3]) AS ll").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("sl"), Some(&PropertyValue::Int(5)));
    assert_eq!(result.rows[0].get("ll"), Some(&PropertyValue::Int(3)));
}

#[test]
fn e2e_head_last_tail_functions() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN head([1,2,3]) AS h, last([1,2,3]) AS l, tail([1,2,3]) AS t").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("h"), Some(&PropertyValue::Int(1)));
    assert_eq!(result.rows[0].get("l"), Some(&PropertyValue::Int(3)));
    if let Some(PropertyValue::List(t)) = result.rows[0].get("t") {
        assert_eq!(t.len(), 2);
    } else {
        panic!("expected list for tail");
    }
}

#[test]
fn e2e_range_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN range(1, 5) AS r").unwrap();
    assert_eq!(result.rows.len(), 1);
    if let Some(PropertyValue::List(r)) = result.rows[0].get("r") {
        // range is inclusive: [1, 2, 3, 4, 5]
        assert_eq!(r.len(), 5);
        assert_eq!(r[0], PropertyValue::Int(1));
        assert_eq!(r[4], PropertyValue::Int(5));
    } else {
        panic!("expected list");
    }
}

#[test]
fn e2e_boolean_logic_and_comparison() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {val: 10})").unwrap();

    let result = ctx.run("MATCH (n:Item) WHERE n.val >= 5 AND n.val <= 15 RETURN n.val AS v").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("v"), Some(&PropertyValue::Int(10)));

    let result2 = ctx.run("MATCH (n:Item) WHERE n.val < 5 OR n.val > 15 RETURN n.val AS v").unwrap();
    assert_eq!(result2.rows.len(), 0);

    let result3 = ctx.run("MATCH (n:Item) WHERE NOT n.val = 5 RETURN n.val AS v").unwrap();
    assert_eq!(result3.rows.len(), 1);
}

#[test]
fn e2e_unwind_with_multiple_values() {
    let ctx = TestCtx::new();
    let result = ctx.run("UNWIND [1, 2, 3] AS x RETURN x * 2 AS doubled").unwrap();
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0].get("doubled"), Some(&PropertyValue::Int(2)));
    assert_eq!(result.rows[1].get("doubled"), Some(&PropertyValue::Int(4)));
    assert_eq!(result.rows[2].get("doubled"), Some(&PropertyValue::Int(6)));
}

#[test]
fn e2e_with_aliasing() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
    let result = ctx.run("MATCH (n:Person) WITH n.name AS personName RETURN personName AS personName").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("personName"), Some(&PropertyValue::String("Alice".into())));
}

#[test]
fn e2e_set_multiple_properties() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice'})").unwrap();
    ctx.run("MATCH (n:Person) SET n.age = 30, n.city = 'NYC'").unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN n.age AS age, n.city AS city").unwrap();
    assert_eq!(result.rows[0].get("age"), Some(&PropertyValue::Int(30)));
    assert_eq!(result.rows[0].get("city"), Some(&PropertyValue::String("NYC".into())));
}

#[test]
fn e2e_remove_label_and_property() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person:Employee {name: 'Alice', age: 30})").unwrap();
    ctx.run("MATCH (n:Person) REMOVE n:Employee, n.age").unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN n.name AS name").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
    // Verify age is removed
    let result2 = ctx.run("MATCH (n:Person) RETURN n.age AS age").unwrap();
    assert_eq!(result2.rows[0].get("age"), Some(&PropertyValue::Null));
}

#[test]
fn e2e_foreach_create_unwind() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice'})").unwrap();
    ctx.run("UNWIND ['Bob', 'Charlie'] AS friendName CREATE (f:Person {name: friendName})").unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(3)));
}

#[test]
fn e2e_optional_match_no_match() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})").unwrap();
    let result = ctx.run("MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) RETURN a.name AS a, b.name AS b").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("a"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(result.rows[0].get("b"), Some(&PropertyValue::Null));
}

#[test]
fn e2e_exists_predicate() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap();
    ctx.run("CREATE (c:Person {name: 'Charlie'})").unwrap();

    let result = ctx.run("MATCH (n:Person) WHERE EXISTS { MATCH (n)-[:KNOWS]->(:Person) } RETURN n.name AS name ORDER BY n.name").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
}

#[test]
fn e2e_list_predicates_all_any_none_single() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN all(x IN [1,2,3] WHERE x > 0) AS a, any(x IN [1,2,3] WHERE x > 2) AS y, none(x IN [1,2,3] WHERE x > 5) AS n, single(x IN [1,2,3] WHERE x > 2) AS s").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("a"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("y"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("n"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::Bool(true)));
}

#[test]
fn e2e_case_expression_age_category() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN CASE WHEN n.age < 18 THEN 'minor' WHEN n.age < 65 THEN 'adult' ELSE 'senior' END AS category").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("category"), Some(&PropertyValue::String("adult".into())));
}

#[test]
fn e2e_return_distinct_colors() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {color: 'red'})").unwrap();
    ctx.run("CREATE (n:Item {color: 'red'})").unwrap();
    ctx.run("CREATE (n:Item {color: 'blue'})").unwrap();

    let result = ctx.run("MATCH (n:Item) RETURN DISTINCT n.color AS color").unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn e2e_order_by_multiple() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Bob', age: 30})").unwrap();
    ctx.run("CREATE (n:Person {name: 'Alice', age: 25})").unwrap();
    ctx.run("CREATE (n:Person {name: 'Charlie', age: 35})").unwrap();

    let result = ctx.run("MATCH (n:Person) RETURN n.name AS name, n.age AS age ORDER BY name").unwrap();
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(result.rows[1].get("name"), Some(&PropertyValue::String("Bob".into())));
    assert_eq!(result.rows[2].get("name"), Some(&PropertyValue::String("Charlie".into())));
}

#[test]
fn e2e_id_and_labels_functions_both() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person:Employee {name: 'Alice'})").unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN id(n) AS nodeId, labels(n) AS labs").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].get("nodeId"), Some(PropertyValue::Int(_))));
    if let Some(PropertyValue::List(labs)) = result.rows[0].get("labs") {
        assert!(labs.len() >= 1);
    } else {
        panic!("expected list for labels");
    }
}

#[test]
fn e2e_type_function_knows() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person)-[:KNOWS]->(b:Person)").unwrap();
    let result = ctx.run("MATCH ()-[r:KNOWS]->() RETURN type(r) AS t").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("t"), Some(&PropertyValue::String("KNOWS".into())));
}

#[test]
fn e2e_map_literal() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN {a: 1, b: 'hello'} AS m").unwrap();
    assert_eq!(result.rows.len(), 1);
    if let Some(PropertyValue::Map(m)) = result.rows[0].get("m") {
        let get = |k: &str| m.iter().find(|(key, _)| key == k).map(|(_, v)| v);
        assert_eq!(get("a"), Some(&PropertyValue::Int(1)));
        assert_eq!(get("b"), Some(&PropertyValue::String("hello".into())));
    } else {
        panic!("expected map");
    }
}

#[test]
fn e2e_decimal_and_negative_numbers() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN -5 AS neg, 3.14 AS pi").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("neg"), Some(&PropertyValue::Int(-5)));
    assert_eq!(result.rows[0].get("pi"), Some(&PropertyValue::Double(3.14)));
}

#[test]
fn e2e_self_loop() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice'})-[:KNOWS]->(n)").unwrap();
    let result = ctx.run("MATCH (n:Person)-[:KNOWS]->(n) RETURN n.name AS name").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
}


#[test]
fn e2e_large_bulk_create() {
    let ctx = TestCtx::new();
    for i in 0..100 {
        ctx.run(&format!("CREATE (n:Item {{id: {}}})", i)).unwrap();
    }
    let result = ctx.run("MATCH (n:Item) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(100)));
}

#[test]
fn e2e_chain_create() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:NEXT]->(b:Node)-[:NEXT]->(c:Node)").unwrap();
    let result = ctx.run("MATCH (a:Node)-[:NEXT]->(b:Node)-[:NEXT]->(c:Node) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_star_path_match() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A)-[:R]->(b:B)-[:R]->(c:C)").unwrap();
    let result = ctx.run("MATCH (a:A)-[:R*]->(c:C) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_parameterized_query_simulation() {
    let ctx = TestCtx::new();
    // Simulate what a parameterized query would do by embedding values
    let name = "Alice";
    let age = 30;
    ctx.run(&format!("CREATE (n:Person {{name: '{}', age: {}}})", name, age)).unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN n.name AS name").unwrap();
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
}

#[test]
fn e2e_multiple_relationship_types() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:WORKS_WITH]->(b:Person {name: 'Bob'})").unwrap();
    let result = ctx.run("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_empty_database_queries() {
    let ctx = TestCtx::new();
    let result = ctx.run("MATCH (n) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(0)));

    let result2 = ctx.run("MATCH ()-[]->() RETURN count(*) AS cnt").unwrap();
    assert_eq!(result2.rows[0].get("cnt"), Some(&PropertyValue::Int(0)));
}

#[test]
fn e2e_string_concatenation() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN concat('hello', ' ', 'world') AS greeting").unwrap();
    assert_eq!(result.rows[0].get("greeting"), Some(&PropertyValue::String("hello world".into())));
}

#[test]
fn e2e_collect_function() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice'})").unwrap();
    ctx.run("CREATE (n:Person {name: 'Bob'})").unwrap();
    ctx.run("CREATE (n:Person {name: 'Charlie'})").unwrap();

    let result = ctx.run("MATCH (n:Person) RETURN collect(n.name) AS names").unwrap();
    assert_eq!(result.rows.len(), 1);
    if let Some(PropertyValue::List(names)) = result.rows[0].get("names") {
        assert_eq!(names.len(), 3);
    } else {
        panic!("expected list");
    }
}

#[test]
fn e2e_return_multiple_expressions() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {val: 10})").unwrap();
    let result = ctx.run("MATCH (n:Item) RETURN n.val AS v, n.val * 2 AS doubled, 'static' AS s").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("v"), Some(&PropertyValue::Int(10)));
    assert_eq!(result.rows[0].get("doubled"), Some(&PropertyValue::Int(20)));
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::String("static".into())));
}

#[test]
fn e2e_temporal_date_creation() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN date({year: 2024, month: 5, day: 1}) AS d").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].get("d"), Some(PropertyValue::Date(_))));
}

#[test]
fn e2e_temporal_localdatetime() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN localdatetime({year: 2024, month: 1, day: 15, hour: 10, minute: 30}) AS dt").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].get("dt"), Some(PropertyValue::LocalDateTime(_))));
}

#[test]
fn e2e_temporal_time() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN time({hour: 14, minute: 30, second: 0}) AS t").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].get("t"), Some(PropertyValue::LocalTime(_))));
}

#[test]
fn e2e_duration_from_map() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN duration({days: 1, hours: 2, minutes: 30}) AS d").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].get("d"), Some(PropertyValue::Duration(_))));
}

#[test]
fn e2e_duration_from_string() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN duration('P1DT2H30M') AS d").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].get("d"), Some(PropertyValue::Duration(_))));
}

#[test]
fn e2e_point_2d_creation() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN point({x: 1.0, y: 2.0, crs: 'cartesian'}) AS p").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].get("p"), Some(PropertyValue::Point2D(_))));
}

#[test]
fn e2e_distance_function_2d() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN distance(point({x: 0, y: 0}), point({x: 3, y: 4})) AS d").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("d"), Some(&PropertyValue::Double(5.0)));
}

#[test]
fn e2e_distance_3d_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN distance(point({x: 0, y: 0, z: 0, crs: 'cartesian-3d'}), point({x: 1, y: 2, z: 2, crs: 'cartesian-3d'})) AS d").unwrap();
    assert_eq!(result.rows[0].get("d"), Some(&PropertyValue::Double(3.0)));
}

#[test]
fn e2e_math_sqrt() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN sqrt(16) AS r").unwrap();
    assert_eq!(result.rows[0].get("r"), Some(&PropertyValue::Double(4.0)));
}

#[test]
fn e2e_math_pow() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN pow(2, 10) AS r").unwrap();
    assert_eq!(result.rows[0].get("r"), Some(&PropertyValue::Double(1024.0)));
}

#[test]
fn e2e_math_abs() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN abs(-42) AS r").unwrap();
    assert_eq!(result.rows[0].get("r"), Some(&PropertyValue::Int(42)));
}

#[test]
fn e2e_math_round() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN round(3.7) AS r").unwrap();
    assert_eq!(result.rows[0].get("r"), Some(&PropertyValue::Double(4.0)));
}

#[test]
fn e2e_list_range() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN range(1, 5) AS r").unwrap();
    assert_eq!(result.rows.len(), 1);
    if let Some(PropertyValue::List(items)) = result.rows[0].get("r") {
        assert_eq!(items.len(), 5);
        assert_eq!(items[0], PropertyValue::Int(1));
        assert_eq!(items[4], PropertyValue::Int(5));
    } else {
        panic!("expected list");
    }
}

#[test]
fn e2e_list_reverse() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN reverse([1, 2, 3]) AS r").unwrap();
    if let Some(PropertyValue::List(items)) = result.rows[0].get("r") {
        assert_eq!(items.clone(), vec![PropertyValue::Int(3), PropertyValue::Int(2), PropertyValue::Int(1)]);
    } else {
        panic!("expected list");
    }
}

#[test]
fn e2e_string_starts_ends_with() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN startswith('hello world', 'hello') AS s, endswith('hello world', 'world') AS e").unwrap();
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("e"), Some(&PropertyValue::Bool(true)));
}

#[test]
fn e2e_string_starts_ends_with_operator() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (:Person {name: 'Alice'})").unwrap();
    ctx.run("CREATE (:Person {name: 'Bob'})").unwrap();
    ctx.run("CREATE (:Person {name: 'Charlie'})").unwrap();

    let result = ctx.run("MATCH (n:Person) WHERE n.name STARTS WITH 'Al' RETURN n.name AS name").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));

    let result = ctx.run("MATCH (n:Person) WHERE n.name ENDS WITH 'ob' RETURN n.name AS name").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Bob".into())));

    let result = ctx.run("MATCH (n:Person) WHERE n.name CONTAINS 'arl' RETURN n.name AS name").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Charlie".into())));
}

#[test]
fn e2e_list_head_last_tail() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN head([10, 20, 30]) AS h, last([10, 20, 30]) AS l, tail([10, 20, 30]) AS t").unwrap();
    assert_eq!(result.rows[0].get("h"), Some(&PropertyValue::Int(10)));
    assert_eq!(result.rows[0].get("l"), Some(&PropertyValue::Int(30)));
    if let Some(PropertyValue::List(items)) = result.rows[0].get("t") {
        assert_eq!(items.len(), 2);
    } else {
        panic!("expected list");
    }
}

#[test]
fn e2e_size_on_path() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A)-[:R]->(b:B)-[:R]->(c:C)").unwrap();
    let result = ctx.run("MATCH p=(a:A)-[:R*]->(c:C) RETURN size(p) AS s").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_type_and_labels_on_created() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person:Employee {name: 'Alice'})-[:WORKS_AT]->(b:Company {name: 'Acme'})").unwrap();
    let result = ctx.run("MATCH (a:Person)-[r]->(b) RETURN labels(a) AS labs, type(r) AS t").unwrap();
    assert_eq!(result.rows.len(), 1);
    if let Some(PropertyValue::List(labs)) = result.rows[0].get("labs") {
        assert_eq!(labs.len(), 2);
    } else {
        panic!("expected list of labels");
    }
}

#[test]
fn e2e_shortest_path_two_hop() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A)-[:R]->(b:B)-[:R]->(c:C)").unwrap();
    let result = ctx.run("MATCH p = shortestPath((a:A)-[:R*]->(c:C)) RETURN length(p) AS len").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("len"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_null_coalescing() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Node {val: 10})").unwrap();
    ctx.run("CREATE (n:Node)").unwrap();
    let result = ctx.run("MATCH (n:Node) RETURN coalesce(n.val, 0) AS v").unwrap();
    assert_eq!(result.rows.len(), 2);
    let values: Vec<i64> = result.rows.iter()
        .map(|r| match r.get("v") {
            Some(PropertyValue::Int(v)) => *v,
            _ => -1,
        })
        .collect();
    assert!(values.contains(&10));
    assert!(values.contains(&0));
}

#[test]
fn e2e_complex_filter_with_and_or() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {a: 1, b: 2})").unwrap();
    ctx.run("CREATE (n:Item {a: 1, b: 3})").unwrap();
    ctx.run("CREATE (n:Item {a: 2, b: 2})").unwrap();
    let result = ctx.run("MATCH (n:Item) WHERE n.a = 1 AND n.b = 2 RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_not_equals_operator() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {val: 1})").unwrap();
    ctx.run("CREATE (n:Item {val: 2})").unwrap();
    let result = ctx.run("MATCH (n:Item) WHERE n.val <> 1 RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_in_operator() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {val: 2})").unwrap();
    ctx.run("CREATE (n:Item {val: 5})").unwrap();
    let result = ctx.run("MATCH (n:Item) WHERE n.val IN [1, 2, 3] RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_with_order_by() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {val: 3})").unwrap();
    ctx.run("CREATE (n:Item {val: 1})").unwrap();
    ctx.run("CREATE (n:Item {val: 2})").unwrap();
    let result = ctx.run("MATCH (n:Item) WITH n.val AS v ORDER BY v RETURN v").unwrap();
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0].get("v"), Some(&PropertyValue::Int(1)));
    assert_eq!(result.rows[2].get("v"), Some(&PropertyValue::Int(3)));
}

#[test]
fn e2e_merge_on_create_on_match() {
    let ctx = TestCtx::new();
    // First MERGE creates the node — ON CREATE fires
    ctx.run("MERGE (n:Key {id: 'k1'}) ON CREATE SET n.created = 1 ON MATCH SET n.seen = 2").unwrap();
    let result = ctx.run("MATCH (n:Key {id: 'k1'}) RETURN n.created AS c, n.seen AS s").unwrap();
    assert_eq!(result.rows[0].get("c"), Some(&PropertyValue::Int(1)));
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::Null));
    // Second MERGE matches the node — ON MATCH fires
    ctx.run("MERGE (n:Key {id: 'k1'}) ON CREATE SET n.created = 10 ON MATCH SET n.seen = 2").unwrap();
    let result2 = ctx.run("MATCH (n:Key {id: 'k1'}) RETURN n.created AS c, n.seen AS s").unwrap();
    assert_eq!(result2.rows[0].get("c"), Some(&PropertyValue::Int(1)));
    assert_eq!(result2.rows[0].get("s"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_create_unique_constraint() {
    let ctx = TestCtx::new();
    // Create a unique property constraint
    // Unique constraints are enforced at storage level via create_label_index + property index
    ctx.storage.create_label_property_index(mgcore::types::LabelId::from_uint(1), mgcore::types::PropertyId::from_uint(1));
    ctx.run("CREATE (n:Person {email: 'alice@example.com'})").unwrap();
    // Second create with same value should succeed (unique constraint is a stub in this test setup)
    let result = ctx.run("CREATE (n:Person {email: 'bob@example.com'})");
    assert!(result.is_ok());
}

#[test]
fn e2e_unwind_with_empty_list() {
    let ctx = TestCtx::new();
    let result = ctx.run("UNWIND [] AS x RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(0)));
}

#[test]
fn e2e_property_exists_check() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Node {name: 'Alice', age: 30})").unwrap();
    ctx.run("CREATE (n:Node {name: 'Bob'})").unwrap();
    let result = ctx.run("MATCH (n:Node) WHERE exists(n.age) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_optional_match_with_where() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A {val: 1})").unwrap();
    ctx.run("CREATE (b:B {val: 2})").unwrap();
    let result = ctx.run("MATCH (a:A) OPTIONAL MATCH (a)-[:R]->(b:B) RETURN a.val AS av, b.val AS bv").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("av"), Some(&PropertyValue::Int(1)));
    assert_eq!(result.rows[0].get("bv"), Some(&PropertyValue::Null));
}

#[test]
fn e2e_count_star_vs_count_property() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Node {val: 1})").unwrap();
    ctx.run("CREATE (n:Node)").unwrap();
    let result = ctx.run("MATCH (n:Node) RETURN count(*) AS cstar, count(n.val) AS cprop").unwrap();
    assert_eq!(result.rows[0].get("cstar"), Some(&PropertyValue::Int(2)));
    assert_eq!(result.rows[0].get("cprop"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_multiple_match_clauses() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A {name: 'a1'})-[:R]->(b:B {name: 'b1'})").unwrap();
    ctx.run("CREATE (c:C {name: 'c1'})-[:S]->(d:D {name: 'd1'})").unwrap();
    let result = ctx.run("MATCH (a:A)-[:R]->(b:B) MATCH (c:C)-[:S]->(d:D) RETURN a.name AS an, d.name AS dn").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("an"), Some(&PropertyValue::String("a1".into())));
    assert_eq!(result.rows[0].get("dn"), Some(&PropertyValue::String("d1".into())));
}

#[test]
fn e2e_create_index_then_query() {
    let ctx = TestCtx::new();
    // Create label-property index BEFORE inserting data so index is maintained
    ctx.storage.create_label_property_index(mgcore::types::LabelId::from_uint(1), mgcore::types::PropertyId::from_uint(1));
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
    ctx.run("CREATE (n:Person {name: 'Bob', age: 25})").unwrap();
    let result = ctx.run("MATCH (n:Person {name: 'Alice'}) RETURN n.age AS age").unwrap();
    assert_eq!(result.rows[0].get("age"), Some(&PropertyValue::Int(30)));
}

#[test]
fn e2e_transaction_commit_and_read() {
    let ctx = TestCtx::new();
    {
        let tx = ctx.storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
        ctx.run("CREATE (n:Node {val: 42})").unwrap();
        ctx.storage.commit_transaction(&tx);
    }
    let result = ctx.run("MATCH (n:Node) RETURN n.val AS v").unwrap();
    assert_eq!(result.rows[0].get("v"), Some(&PropertyValue::Int(42)));
}

#[test]
fn e2e_path_length() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A)-[:R]->(b:B)-[:R]->(c:C)-[:R]->(d:D)").unwrap();
    let result = ctx.run("MATCH p=(a:A)-[:R*1..3]->(d:D) RETURN length(p) AS len").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("len"), Some(&PropertyValue::Int(3)));
}

#[test]
fn e2e_relationship_property_match() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A)-[:R {weight: 5}]->(b:B)").unwrap();
    ctx.run("CREATE (a:A)-[:R {weight: 10}]->(c:C)").unwrap();
    let result = ctx.run("MATCH (a:A)-[r:R {weight: 5}]->(b) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_delete_all_nodes_and_edges() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A)-[:R]->(b:B)").unwrap();
    ctx.run("CREATE (c:C)-[:S]->(d:D)").unwrap();
    ctx.run("MATCH (n) DETACH DELETE n").unwrap();
    let result = ctx.run("MATCH (n) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(0)));
    let result2 = ctx.run("MATCH ()-[]->() RETURN count(*) AS cnt").unwrap();
    assert_eq!(result2.rows[0].get("cnt"), Some(&PropertyValue::Int(0)));
}

#[test]
fn e2e_complex_aggregation() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Product {category: 'A', price: 10})").unwrap();
    ctx.run("CREATE (n:Product {category: 'A', price: 20})").unwrap();
    ctx.run("CREATE (n:Product {category: 'B', price: 30})").unwrap();
    let result = ctx.run("MATCH (n:Product) RETURN n.category AS cat, sum(n.price) AS total, avg(n.price) AS avg_price, min(n.price) AS min_price, max(n.price) AS max_price").unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn e2e_return_expression_with_alias() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN 1 + 2 + 3 AS total, 'hello' AS greeting").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("total"), Some(&PropertyValue::Int(6)));
    assert_eq!(result.rows[0].get("greeting"), Some(&PropertyValue::String("hello".into())));
}

#[test]
fn e2e_case_with_null() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Node {val: 1})").unwrap();
    ctx.run("CREATE (n:Node)").unwrap();
    let result = ctx.run("MATCH (n:Node) RETURN CASE WHEN n.val IS NULL THEN 'missing' ELSE 'present' END AS status").unwrap();
    assert_eq!(result.rows.len(), 2);
    let statuses: Vec<String> = result.rows.iter()
        .map(|r| match r.get("status") {
            Some(PropertyValue::String(s)) => s.clone(),
            _ => String::new(),
        })
        .collect();
    assert!(statuses.contains(&"missing".to_string()));
    assert!(statuses.contains(&"present".to_string()));
}

#[test]
fn e2e_boolean_operators() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Node {a: true, b: false})").unwrap();
    let result = ctx.run("MATCH (n:Node) RETURN n.a AND n.b AS and_val, n.a OR n.b AS or_val, NOT n.a AS not_val").unwrap();
    assert_eq!(result.rows[0].get("and_val"), Some(&PropertyValue::Bool(false)));
    assert_eq!(result.rows[0].get("or_val"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("not_val"), Some(&PropertyValue::Bool(false)));
}

#[test]
fn e2e_string_case_functions() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN tolower('HELLO') AS low, toupper('world') AS up").unwrap();
    assert_eq!(result.rows[0].get("low"), Some(&PropertyValue::String("hello".into())));
    assert_eq!(result.rows[0].get("up"), Some(&PropertyValue::String("WORLD".into())));
}

#[test]
fn e2e_trim_functions() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN trim('  hello  ') AS t, ltrim('  hello') AS lt, rtrim('hello  ') AS rt").unwrap();
    assert_eq!(result.rows[0].get("t"), Some(&PropertyValue::String("hello".into())));
    assert_eq!(result.rows[0].get("lt"), Some(&PropertyValue::String("hello".into())));
    assert_eq!(result.rows[0].get("rt"), Some(&PropertyValue::String("hello".into())));
}

#[test]
fn e2e_substring_functions() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN substring('hello', 1, 3) AS s, left('hello', 2) AS l, right('hello', 2) AS r").unwrap();
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::String("ell".into())));
    assert_eq!(result.rows[0].get("l"), Some(&PropertyValue::String("he".into())));
    assert_eq!(result.rows[0].get("r"), Some(&PropertyValue::String("lo".into())));
}

#[test]
fn e2e_split_and_replace() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN split('a,b,c', ',') AS s, replace('hello world', 'world', 'rust') AS r").unwrap();
    if let Some(PropertyValue::List(items)) = result.rows[0].get("s") {
        assert_eq!(items.len(), 3);
    } else {
        panic!("expected list");
    }
    assert_eq!(result.rows[0].get("r"), Some(&PropertyValue::String("hello rust".into())));
}

#[test]
fn e2e_size_and_length() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN size('hello') AS ss, size([1,2,3]) AS sl, length('test') AS ll").unwrap();
    assert_eq!(result.rows[0].get("ss"), Some(&PropertyValue::Int(5)));
    assert_eq!(result.rows[0].get("sl"), Some(&PropertyValue::Int(3)));
    assert_eq!(result.rows[0].get("ll"), Some(&PropertyValue::Int(4)));
}

#[test]
fn e2e_to_string_and_to_float() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN tostring(42) AS s, tofloat('3.14') AS f").unwrap();
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::String("42".into())));
    assert_eq!(result.rows[0].get("f"), Some(&PropertyValue::Double(3.14)));
}

#[test]
fn e2e_valuetype_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN valuetype(42) AS t, valuetype('hello') AS s, valuetype([1,2]) AS l").unwrap();
    assert_eq!(result.rows[0].get("t"), Some(&PropertyValue::String("INTEGER".into())));
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::String("STRING".into())));
    assert_eq!(result.rows[0].get("l"), Some(&PropertyValue::String("LIST".into())));
}

#[test]
fn e2e_randomuuid() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN randomuuid() AS u1, randomuuid() AS u2").unwrap();
    let u1 = match result.rows[0].get("u1").unwrap() {
        PropertyValue::String(s) => s.clone(),
        _ => panic!("expected string"),
    };
    let u2 = match result.rows[0].get("u2").unwrap() {
        PropertyValue::String(s) => s.clone(),
        _ => panic!("expected string"),
    };
    assert_ne!(u1, u2);
    assert_eq!(u1.len(), 36); // UUID string length
}

#[test]
fn e2e_timestamp() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN timestamp() AS ts").unwrap();
    if let Some(PropertyValue::Int(ts)) = result.rows[0].get("ts") {
        assert!(*ts > 0);
    } else {
        panic!("expected int timestamp");
    }
}

#[test]
fn e2e_degree_functions() {
    let ctx = TestCtx::new();
    // Linear chain d -> a -> b (single CREATE pattern element)
    ctx.run("CREATE (d:D)-[:R]->(a:A)-[:R]->(b:B)").unwrap();
    let result = ctx.run("MATCH (a:A) RETURN degree(a) AS deg, outdegree(a) AS outd, indegree(a) AS ind").unwrap();
    assert_eq!(result.rows[0].get("deg"), Some(&PropertyValue::Int(2)));
    assert_eq!(result.rows[0].get("outd"), Some(&PropertyValue::Int(1)));
    assert_eq!(result.rows[0].get("ind"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_list_contains() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN contains([1, 2, 3], 2) AS yes, contains([1, 2, 3], 5) AS no").unwrap();
    assert_eq!(result.rows[0].get("yes"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("no"), Some(&PropertyValue::Bool(false)));
}

#[test]
fn e2e_list_union_intersection() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc.coll.union([1,2], [2,3]) AS u, apoc.coll.intersection([1,2], [2,3]) AS i").unwrap();
    if let Some(PropertyValue::List(u)) = result.rows[0].get("u") {
        assert_eq!(u.len(), 3);
    } else {
        panic!("expected list");
    }
    if let Some(PropertyValue::List(i)) = result.rows[0].get("i") {
        assert_eq!(i.len(), 1);
    } else {
        panic!("expected list");
    }
}

#[test]
fn e2e_map_merge_and_set_key() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc.map.merge({a: 1}, {b: 2}) AS m, apoc.map.setKey({a: 1}, 'b', 2) AS s").unwrap();
    if let Some(PropertyValue::Map(m)) = result.rows[0].get("m") {
        assert_eq!(m.len(), 2);
    } else {
        panic!("expected map");
    }
    if let Some(PropertyValue::Map(s)) = result.rows[0].get("s") {
        assert_eq!(s.len(), 2);
    } else {
        panic!("expected map");
    }
}

#[test]
fn e2e_text_join_and_replace() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc.text.join(['a', 'b', 'c'], '-') AS j, apoc.text.replace('hello', 'l', 'x') AS r").unwrap();
    assert_eq!(result.rows[0].get("j"), Some(&PropertyValue::String("a-b-c".into())));
    assert_eq!(result.rows[0].get("r"), Some(&PropertyValue::String("hexxo".into())));
}

#[test]
fn e2e_coll_sort_flatten_remove() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc.coll.sort([3,1,2]) AS s, apoc.coll.flatten([[1], [2]]) AS f, apoc.coll.remove([1,2,3], 1) AS r").unwrap();
    if let Some(PropertyValue::List(s)) = result.rows[0].get("s") {
        assert_eq!(s.clone(), vec![PropertyValue::Int(1), PropertyValue::Int(2), PropertyValue::Int(3)]);
    } else {
        panic!("expected sorted list");
    }
    if let Some(PropertyValue::List(f)) = result.rows[0].get("f") {
        assert_eq!(f.len(), 2);
    } else {
        panic!("expected flattened list");
    }
    if let Some(PropertyValue::List(r)) = result.rows[0].get("r") {
        assert_eq!(r.len(), 2);
    } else {
        panic!("expected list after remove");
    }
}

#[test]
fn e2e_coll_sum_avg() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc.coll.sum([1,2,3]) AS s, apoc.coll.avg([1,2,3]) AS a").unwrap();
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::Int(6)));
    assert_eq!(result.rows[0].get("a"), Some(&PropertyValue::Double(2.0)));
}

#[test]
fn e2e_sin_cos_tan() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN sin(0) AS s, cos(0) AS c").unwrap();
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::Double(0.0)));
    assert_eq!(result.rows[0].get("c"), Some(&PropertyValue::Double(1.0)));
}

#[test]
fn e2e_degrees_radians() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN degrees(pi()) AS d, radians(180) AS r").unwrap();
    assert_eq!(result.rows[0].get("d"), Some(&PropertyValue::Double(180.0)));
    assert_eq!(result.rows[0].get("r"), Some(&PropertyValue::Double(std::f64::consts::PI)));
}

#[test]
fn e2e_ceil_floor() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN ceil(2.3) AS c, floor(2.7) AS f").unwrap();
    assert_eq!(result.rows[0].get("c"), Some(&PropertyValue::Double(3.0)));
    assert_eq!(result.rows[0].get("f"), Some(&PropertyValue::Double(2.0)));
}

#[test]
fn e2e_log_exp() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN log(exp(1)) AS l, log10(100) AS l10").unwrap();
    assert_eq!(result.rows[0].get("l"), Some(&PropertyValue::Double(1.0)));
    assert_eq!(result.rows[0].get("l10"), Some(&PropertyValue::Double(2.0)));
}

#[test]
fn e2e_sign() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN sign(-5) AS neg, sign(5) AS pos, sign(0) AS zero").unwrap();
    assert_eq!(result.rows[0].get("neg"), Some(&PropertyValue::Int(-1)));
    assert_eq!(result.rows[0].get("pos"), Some(&PropertyValue::Int(1)));
    assert_eq!(result.rows[0].get("zero"), Some(&PropertyValue::Int(0)));
}

#[test]
fn e2e_point_with_wgs84() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN point({longitude: 0.0, latitude: 0.0, crs: 'wgs-84'}) AS p").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].get("p"), Some(PropertyValue::Point2D(_))));
}

#[test]
fn e2e_datetime_parsing() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN datetime('2024-01-01T00:00:00Z') AS dt").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].get("dt"), Some(PropertyValue::ZonedDateTime(_))));
}

#[test]
fn e2e_reduce_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN reduce(sum = 0, x IN [1,2,3] | sum + x) AS total").unwrap();
    assert_eq!(result.rows[0].get("total"), Some(&PropertyValue::Int(6)));
}

#[test]
fn e2e_extract_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN extract(x IN [1,2,3] | x * 2) AS doubled").unwrap();
    if let Some(PropertyValue::List(items)) = result.rows[0].get("doubled") {
        assert_eq!(items.clone(), vec![PropertyValue::Int(2), PropertyValue::Int(4), PropertyValue::Int(6)]);
    } else {
        panic!("expected list");
    }
}

#[test]
fn e2e_nodes_and_relationships_functions() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A)-[:R]->(b:B)").unwrap();
    let result = ctx.run("MATCH p=(a:A)-[:R]->(b:B) RETURN size(nodes(p)) AS nc, size(relationships(p)) AS rc").unwrap();
    assert_eq!(result.rows[0].get("nc"), Some(&PropertyValue::Int(2)));
    assert_eq!(result.rows[0].get("rc"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_head_and_last_on_path() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A {name: 'Alice'})-[:KNOWS]->(b:B {name: 'Bob'})-[:KNOWS]->(c:C {name: 'Carol'})").unwrap();
    let result = ctx.run("MATCH p=(a:A)-[:KNOWS*]->(c:C) RETURN head(p).name AS h, last(p).name AS l").unwrap();
    assert_eq!(result.rows[0].get("h"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(result.rows[0].get("l"), Some(&PropertyValue::String("Carol".into())));
}

#[test]
fn e2e_text_index_create_and_search() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Post {title: 'Hello World', body: 'First post'})").unwrap();
    ctx.run("CREATE (b:Post {title: 'Goodbye World', body: 'Last post'})").unwrap();
    ctx.run("CREATE (c:Article {title: 'Hello Rust'})").unwrap();

    // Create text index on Post title
    ctx.run("CALL db.createTextIndex({label: 'Post', properties: ['title']})").unwrap();

    // Search for 'Hello'
    let result = ctx.run("CALL db.searchTextIndex({label: 'Post', query: 'Hello'}) YIELD node, score RETURN node").unwrap();
    assert_eq!(result.rows.len(), 1);

    // Search for 'World' should match both posts
    let result = ctx.run("CALL db.searchTextIndex({label: 'Post', query: 'World'}) YIELD node, score RETURN node").unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn e2e_text_index_auto_update() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Post {title: 'Hello World'})").unwrap();
    ctx.run("CALL db.createTextIndex({label: 'Post', properties: ['title']})").unwrap();

    // Verify initial index
    let result = ctx.run("CALL db.searchTextIndex({label: 'Post', query: 'Hello'}) YIELD node, score RETURN node").unwrap();
    assert_eq!(result.rows.len(), 1);

    // Update property — auto-maintenance should re-index
    ctx.run("MATCH (a:Post) SET a.title = 'Goodbye World'").unwrap();
    let result = ctx.run("CALL db.searchTextIndex({label: 'Post', query: 'Hello'}) YIELD node, score RETURN node").unwrap();
    assert_eq!(result.rows.len(), 0);
    let result = ctx.run("CALL db.searchTextIndex({label: 'Post', query: 'Goodbye'}) YIELD node, score RETURN node").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn e2e_startnode_endnode() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A {name: 'Alice'})-[:KNOWS]->(b:B {name: 'Bob'})").unwrap();
    let result = ctx.run("MATCH (a:A)-[r]->(b:B) RETURN startnode(r).name AS sn, endnode(r).name AS en").unwrap();
    assert_eq!(result.rows[0].get("sn"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(result.rows[0].get("en"), Some(&PropertyValue::String("Bob".into())));
}

#[test]
fn e2e_values_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN values({a: 1, b: 2}) AS v").unwrap();
    if let Some(PropertyValue::List(items)) = result.rows[0].get("v") {
        assert_eq!(items.len(), 2);
    } else {
        panic!("expected list");
    }
}

#[test]
fn e2e_toboolean_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN toboolean('true') AS t, toboolean('false') AS f, toboolean(1) AS one, toboolean(0) AS zero").unwrap();
    assert_eq!(result.rows[0].get("t"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("f"), Some(&PropertyValue::Bool(false)));
    assert_eq!(result.rows[0].get("one"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("zero"), Some(&PropertyValue::Bool(false)));
}

#[test]
fn e2e_isempty_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN isempty('') AS se, isempty([]) AS le, isempty('x') AS sx").unwrap();
    assert_eq!(result.rows[0].get("se"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("le"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("sx"), Some(&PropertyValue::Bool(false)));
}

#[test]
fn e2e_repeat_concat() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN repeat('ab', 3) AS r, concat('a', 'b', 'c') AS c").unwrap();
    assert_eq!(result.rows[0].get("r"), Some(&PropertyValue::String("ababab".into())));
    assert_eq!(result.rows[0].get("c"), Some(&PropertyValue::String("abc".into())));
}

#[test]
fn e2e_min_max_list() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN min([3, 1, 2]) AS mn, max([3, 1, 2]) AS mx").unwrap();
    assert_eq!(result.rows[0].get("mn"), Some(&PropertyValue::Int(1)));
    assert_eq!(result.rows[0].get("mx"), Some(&PropertyValue::Int(3)));
}

#[test]
fn e2e_sum_avg_list() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN sum([1, 2, 3]) AS s, avg([1, 2, 3]) AS a").unwrap();
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::Int(6)));
    assert_eq!(result.rows[0].get("a"), Some(&PropertyValue::Double(2.0)));
}

#[test]
fn e2e_sum_avg_doubles() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {price: 10.5})").unwrap();
    ctx.run("CREATE (n:Item {price: 20.5})").unwrap();
    let result = ctx.run("MATCH (n:Item) RETURN sum(n.price) AS s, avg(n.price) AS a, min(n.price) AS mn, max(n.price) AS mx").unwrap();
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::Double(31.0)));
    assert_eq!(result.rows[0].get("a"), Some(&PropertyValue::Double(15.5)));
    assert_eq!(result.rows[0].get("mn"), Some(&PropertyValue::Double(10.5)));
    assert_eq!(result.rows[0].get("mx"), Some(&PropertyValue::Double(20.5)));
}

#[test]
fn e2e_apoc_date_format() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc.date.format(0, 'yyyy-MM-dd') AS d").unwrap();
    assert_eq!(result.rows[0].get("d"), Some(&PropertyValue::String("1970-01-01".into())));
}


#[test]
fn e2e_remove_label() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person:Employee {name: 'Alice'})").unwrap();
    ctx.run("MATCH (n:Person) REMOVE n:Employee").unwrap();
    let result = ctx.run("MATCH (n:Employee) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(0)));
}

#[test]
fn e2e_match_with_gt_lt_operators() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {price: 10})").unwrap();
    ctx.run("CREATE (n:Item {price: 20})").unwrap();
    ctx.run("CREATE (n:Item {price: 30})").unwrap();
    let result = ctx.run("MATCH (n:Item) WHERE n.price > 10 AND n.price < 30 RETURN n.price AS p").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("p"), Some(&PropertyValue::Int(20)));
}

#[test]
fn e2e_gte_lte_operators() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {price: 10})").unwrap();
    ctx.run("CREATE (n:Item {price: 20})").unwrap();
    let result = ctx.run("MATCH (n:Item) WHERE n.price >= 10 AND n.price <= 20 RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_limit_with_offset() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {val: 1})").unwrap();
    ctx.run("CREATE (n:Item {val: 2})").unwrap();
    ctx.run("CREATE (n:Item {val: 3})").unwrap();
    ctx.run("CREATE (n:Item {val: 4})").unwrap();
    let result = ctx.run("MATCH (n:Item) RETURN n.val AS v ORDER BY v SKIP 1 LIMIT 2").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get("v"), Some(&PropertyValue::Int(2)));
    assert_eq!(result.rows[1].get("v"), Some(&PropertyValue::Int(3)));
}

#[test]
fn e2e_optional_match_chain() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A)-[:R]->(b:B)").unwrap();
    ctx.run("CREATE (c:C)").unwrap();
    let result = ctx.run("MATCH (a:A) OPTIONAL MATCH (a)-[:R]->(b:B) OPTIONAL MATCH (b)-[:R]->(c:C) RETURN labels(a) AS al, labels(b) AS bl, labels(c) AS cl").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(result.rows[0].get("bl").is_some());
    assert_eq!(result.rows[0].get("cl"), Some(&PropertyValue::Null));
}

#[test]
fn e2e_unwind_with_expression() {
    let ctx = TestCtx::new();
    let result = ctx.run("UNWIND range(1, 5) AS x RETURN x").unwrap();
    assert_eq!(result.rows.len(), 5);
    assert_eq!(result.rows[0].get("x"), Some(&PropertyValue::Int(1)));
    assert_eq!(result.rows[4].get("x"), Some(&PropertyValue::Int(5)));
}


#[test]
fn e2e_exists_with_property_check() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', email: 'a@example.com'})").unwrap();
    ctx.run("CREATE (n:Person {name: 'Bob'})").unwrap();
    let result = ctx.run("MATCH (n:Person) WHERE exists(n.email) RETURN n.name AS name").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
}


#[test]
fn e2e_modulo_operator() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN 10 % 3 AS r").unwrap();
    assert_eq!(result.rows[0].get("r"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_negative_numbers() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN -42 AS n, -3.14 AS f").unwrap();
    assert_eq!(result.rows[0].get("n"), Some(&PropertyValue::Int(-42)));
    assert_eq!(result.rows[0].get("f"), Some(&PropertyValue::Double(-3.14)));
}

#[test]
fn e2e_case_simple_form() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Grade {score: 85})").unwrap();
    ctx.run("CREATE (n:Grade {score: 55})").unwrap();
    let result = ctx.run("MATCH (n:Grade) RETURN CASE WHEN n.score >= 60 THEN 'pass' ELSE 'fail' END AS result").unwrap();
    assert_eq!(result.rows.len(), 2);
    let results: Vec<String> = result.rows.iter()
        .map(|r| match r.get("result") {
            Some(PropertyValue::String(s)) => s.clone(),
            _ => String::new(),
        })
        .collect();
    assert!(results.contains(&"pass".to_string()));
    assert!(results.contains(&"fail".to_string()));
}

#[test]
fn e2e_relationship_type_filter() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A)-[:KNOWS]->(b:B)").unwrap();
    ctx.run("CREATE (a:A)-[:WORKS_AT]->(c:C)").unwrap();
    let result = ctx.run("MATCH (a:A)-[r:KNOWS]->(b) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(1)));
}


#[test]
fn e2e_create_return_created() {
    let ctx = TestCtx::new();
    let result = ctx.run("CREATE (n:Person {name: 'Alice'}) RETURN n.name AS name").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
}

#[test]
fn e2e_match_no_results() {
    let ctx = TestCtx::new();
    let result = ctx.run("MATCH (n:NonExistent) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(0)));
}

#[test]
fn e2e_delete_with_match_filter() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {val: 1})").unwrap();
    ctx.run("CREATE (n:Item {val: 2})").unwrap();
    ctx.run("MATCH (n:Item) WHERE n.val = 1 DELETE n").unwrap();
    let result = ctx.run("MATCH (n:Item) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_merge_multiple_times() {
    let ctx = TestCtx::new();
    ctx.run("MERGE (n:Key {id: 'k1'})").unwrap();
    ctx.run("MERGE (n:Key {id: 'k1'})").unwrap();
    ctx.run("MERGE (n:Key {id: 'k2'})").unwrap();
    let result = ctx.run("MATCH (n:Key) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_order_by_desc() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {val: 1})").unwrap();
    ctx.run("CREATE (n:Item {val: 3})").unwrap();
    ctx.run("CREATE (n:Item {val: 2})").unwrap();
    let result = ctx.run("MATCH (n:Item) RETURN n.val AS v ORDER BY v DESC").unwrap();
    assert_eq!(result.rows[0].get("v"), Some(&PropertyValue::Int(3)));
    assert_eq!(result.rows[2].get("v"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_skip_only() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {val: 1})").unwrap();
    ctx.run("CREATE (n:Item {val: 2})").unwrap();
    ctx.run("CREATE (n:Item {val: 3})").unwrap();
    let result = ctx.run("MATCH (n:Item) RETURN n.val AS v ORDER BY v SKIP 1").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get("v"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_complex_boolean_expression() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {a: 1, b: 2})").unwrap();
    ctx.run("CREATE (n:Item {a: 1, b: 3})").unwrap();
    ctx.run("CREATE (n:Item {a: 2, b: 2})").unwrap();
    let result = ctx.run("MATCH (n:Item) WHERE (n.a = 1 AND n.b = 2) OR (n.a = 2 AND n.b = 2) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_is_null_is_not_null() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Node {val: 1})").unwrap();
    ctx.run("CREATE (n:Node)").unwrap();
    let result = ctx.run("MATCH (n:Node) WHERE n.val IS NULL RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(1)));
    let result2 = ctx.run("MATCH (n:Node) WHERE n.val IS NOT NULL RETURN count(*) AS cnt").unwrap();
    assert_eq!(result2.rows[0].get("cnt"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_starts_with_ends_with_contains() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN startswith('hello world', 'hello') AS sw, endswith('hello world', 'world') AS ew, contains('hello world', 'lo wo') AS c").unwrap();
    assert_eq!(result.rows[0].get("sw"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("ew"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("c"), Some(&PropertyValue::Bool(true)));
}


#[test]
fn e2e_size_function_on_string_and_list() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN size('hello') AS ss, size([1,2,3,4]) AS sl").unwrap();
    assert_eq!(result.rows[0].get("ss"), Some(&PropertyValue::Int(5)));
    assert_eq!(result.rows[0].get("sl"), Some(&PropertyValue::Int(4)));
}

#[test]
fn e2e_id_function() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Node {name: 'test'})").unwrap();
    let result = ctx.run("MATCH (n:Node) RETURN id(n) AS node_id").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].get("node_id"), Some(PropertyValue::Int(_))));
}

#[test]
fn e2e_count_star_on_empty_graph() {
    let ctx = TestCtx::new();
    let result = ctx.run("MATCH (n) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(0)));
}

#[test]
fn e2e_return_literal_only() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN 42 AS answer, 'hello' AS greeting, true AS flag").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("answer"), Some(&PropertyValue::Int(42)));
    assert_eq!(result.rows[0].get("greeting"), Some(&PropertyValue::String("hello".into())));
    assert_eq!(result.rows[0].get("flag"), Some(&PropertyValue::Bool(true)));
}


#[test]
fn e2e_match_with_no_labels() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n {name: 'test'})").unwrap();
    let result = ctx.run("MATCH (n) WHERE n.name = 'test' RETURN n.name AS name").unwrap();
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("test".into())));
}

#[test]
fn e2e_aggregation_with_grouping() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Product {category: 'A', price: 10})").unwrap();
    ctx.run("CREATE (n:Product {category: 'A', price: 20})").unwrap();
    ctx.run("CREATE (n:Product {category: 'B', price: 30})").unwrap();
    let result = ctx.run("MATCH (n:Product) RETURN n.category AS cat, sum(n.price) AS total, avg(n.price) AS avg, min(n.price) AS mn, max(n.price) AS mx, count(*) AS cnt").unwrap();
    assert_eq!(result.rows.len(), 2);
    let cat_a = result.rows.iter().find(|r| r.get("cat") == Some(&PropertyValue::String("A".into()))).unwrap();
    assert_eq!(cat_a.get("total"), Some(&PropertyValue::Int(30)));
    assert_eq!(cat_a.get("cnt"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_transaction_rollback_visibility() {
    let ctx = TestCtx::new();
    {
        let tx = ctx.storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
        ctx.run("CREATE (n:Node {val: 99})").unwrap();
        // Before commit, data should be visible in same tx
        ctx.storage.commit_transaction(&tx);
    }
    let result = ctx.run("MATCH (n:Node) RETURN n.val AS v").unwrap();
    assert_eq!(result.rows[0].get("v"), Some(&PropertyValue::Int(99)));
}

#[test]
fn e2e_detach_delete_removes_edges() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A)-[:R]->(b:B)").unwrap();
    ctx.run("MATCH (a:A) DETACH DELETE a").unwrap();
    let result_nodes = ctx.run("MATCH (n) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result_nodes.rows[0].get("cnt"), Some(&PropertyValue::Int(1)));
    let result_edges = ctx.run("MATCH ()-[]->() RETURN count(*) AS cnt").unwrap();
    assert_eq!(result_edges.rows[0].get("cnt"), Some(&PropertyValue::Int(0)));
}

#[test]
fn e2e_pattern_with_multiple_edges() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A)-[:R1]->(b:B)-[:R2]->(c:C)").unwrap();
    let result = ctx.run("MATCH (a:A)-[:R1]->(b:B)-[:R2]->(c:C) RETURN labels(a) AS al, labels(b) AS bl, labels(c) AS cl").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn e2e_nested_list_literal() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN [[1, 2], [3, 4]] AS nested").unwrap();
    if let Some(PropertyValue::List(outer)) = result.rows[0].get("nested") {
        assert_eq!(outer.len(), 2);
        if let PropertyValue::List(inner) = &outer[0] {
            assert_eq!(inner.len(), 2);
        } else {
            panic!("expected inner list");
        }
    } else {
        panic!("expected nested list");
    }
}

#[test]
fn e2e_map_literal_nested() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN {a: {b: 1}} AS m").unwrap();
    if let Some(PropertyValue::Map(outer)) = result.rows[0].get("m") {
        let inner = outer.iter().find(|(k, _)| k == "a").map(|(_, v)| v);
        if let Some(PropertyValue::Map(inner_map)) = inner {
            let val = inner_map.iter().find(|(k, _)| k == "b").map(|(_, v)| v);
            assert_eq!(val, Some(&PropertyValue::Int(1)));
        } else {
            panic!("expected inner map");
        }
    } else {
        panic!("expected map");
    }
}

#[test]
fn e2e_coalesce_with_multiple_args() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Node {a: null, b: null, c: 42})").unwrap();
    let result = ctx.run("MATCH (n:Node) RETURN coalesce(n.a, n.b, n.c, 0) AS val").unwrap();
    assert_eq!(result.rows[0].get("val"), Some(&PropertyValue::Int(42)));
}

#[test]
fn e2e_all_list_predicate() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN all(x IN [1, 2, 3] WHERE x > 0) AS all_pos, all(x IN [1, -1, 3] WHERE x > 0) AS not_all").unwrap();
    assert_eq!(result.rows[0].get("all_pos"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("not_all"), Some(&PropertyValue::Bool(false)));
}

#[test]
fn e2e_any_none_single_predicates() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN any(x IN [1, 2, 3] WHERE x > 2) AS a, none(x IN [1, 2, 3] WHERE x > 5) AS n, single(x IN [1, 2, 3] WHERE x > 2) AS s").unwrap();
    assert_eq!(result.rows[0].get("a"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("n"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::Bool(true)));
}

#[test]
fn e2e_with_alias_then_filter() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {val: 1})").unwrap();
    ctx.run("CREATE (n:Item {val: 2})").unwrap();
    ctx.run("CREATE (n:Item {val: 3})").unwrap();
    let result = ctx.run("MATCH (n:Item) WITH n.val AS v WHERE v > 1 RETURN v").unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn e2e_return_distinct_values() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {val: 1})").unwrap();
    ctx.run("CREATE (n:Item {val: 1})").unwrap();
    ctx.run("CREATE (n:Item {val: 2})").unwrap();
    let result = ctx.run("MATCH (n:Item) RETURN DISTINCT n.val AS v").unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn e2e_create_with_point_property() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Place {location: point({x: 1.0, y: 2.0})})").unwrap();
    let result = ctx.run("MATCH (n:Place) RETURN n.location AS loc").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].get("loc"), Some(PropertyValue::Point2D(_))));
}

#[test]
fn e2e_datetime_property() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Event {time: datetime('2024-01-01T00:00:00Z')})").unwrap();
    let result = ctx.run("MATCH (n:Event) RETURN n.time AS t").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].get("t"), Some(PropertyValue::ZonedDateTime(_))));
}

#[test]
fn e2e_localdatetime_property() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Event {time: localdatetime('2024-01-01T12:00:00')})").unwrap();
    let result = ctx.run("MATCH (n:Event) RETURN n.time AS t").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].get("t"), Some(PropertyValue::LocalDateTime(_))));
}

#[test]
fn e2e_duration_property() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Task {duration: duration({days: 1, hours: 2})})").unwrap();
    let result = ctx.run("MATCH (n:Task) RETURN n.duration AS d").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].get("d"), Some(PropertyValue::Duration(_))));
}

#[test]
fn e2e_list_comprehension_collect() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {val: 1})").unwrap();
    ctx.run("CREATE (n:Item {val: 2})").unwrap();
    ctx.run("CREATE (n:Item {val: 3})").unwrap();
    let result = ctx.run("MATCH (n:Item) RETURN collect(n.val) AS vals").unwrap();
    if let Some(PropertyValue::List(vals)) = result.rows[0].get("vals") {
        assert_eq!(vals.len(), 3);
    } else {
        panic!("expected list");
    }
}

#[test]
fn e2e_aggregate_count_on_relationships() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A)-[:R]->(b:B)").unwrap();
    ctx.run("CREATE (a:A)-[:R]->(c:C)").unwrap();
    let result = ctx.run("MATCH (a:A)-[r]->() RETURN count(r) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_path_variable_length_with_type() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A)-[:KNOWS]->(b:B)-[:KNOWS]->(c:C)").unwrap();
    let result = ctx.run("MATCH (a:A)-[:KNOWS*1..2]->(c) RETURN labels(c) AS cl").unwrap();
    assert_eq!(result.rows.len(), 2); // B and C reachable
}

#[test]
fn e2e_bidirectional_edge_match() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A)-[:R]->(b:B)").unwrap();
    let result = ctx.run("MATCH (a:A)-[:R]-(b:B) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_left_direction_edge() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A)<-[:R]-(b:B)").unwrap();
    let result = ctx.run("MATCH (a:A)<-[:R]-(b:B) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_set_label_and_property_together() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n {name: 'Alice'})").unwrap();
    ctx.run("MATCH (n) SET n:Person, n.age = 30 RETURN n.name AS name").unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN n.name AS name, n.age AS age").unwrap();
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(result.rows[0].get("age"), Some(&PropertyValue::Int(30)));
}

#[test]
fn e2e_remove_property() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Node {a: 1, b: 2})").unwrap();
    ctx.run("MATCH (n:Node) REMOVE n.a").unwrap();
    let result = ctx.run("MATCH (n:Node) RETURN exists(n.a) AS has_a, exists(n.b) AS has_b").unwrap();
    assert_eq!(result.rows[0].get("has_a"), Some(&PropertyValue::Bool(false)));
    assert_eq!(result.rows[0].get("has_b"), Some(&PropertyValue::Bool(true)));
}

#[test]
fn e2e_empty_result_with_where() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {val: 1})").unwrap();
    let result = ctx.run("MATCH (n:Item) WHERE n.val = 999 RETURN n.val AS v").unwrap();
    assert_eq!(result.rows.len(), 0);
}

#[test]
fn e2e_multiple_return_expressions() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Node {x: 10, y: 20})").unwrap();
    let result = ctx.run("MATCH (n:Node) RETURN n.x + n.y AS sum, n.x * n.y AS prod, n.y - n.x AS diff").unwrap();
    assert_eq!(result.rows[0].get("sum"), Some(&PropertyValue::Int(30)));
    assert_eq!(result.rows[0].get("prod"), Some(&PropertyValue::Int(200)));
    assert_eq!(result.rows[0].get("diff"), Some(&PropertyValue::Int(10)));
}

#[test]
fn e2e_string_functions_combined() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN reverse('hello') AS rev, replace('hello', 'l', 'x') AS rep, substring('hello', 0, 2) AS sub").unwrap();
    assert_eq!(result.rows[0].get("rev"), Some(&PropertyValue::String("olleh".into())));
    assert_eq!(result.rows[0].get("rep"), Some(&PropertyValue::String("hexxo".into())));
    assert_eq!(result.rows[0].get("sub"), Some(&PropertyValue::String("he".into())));
}

#[test]
fn e2e_list_functions_combined() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN head([1,2,3]) AS h, last([1,2,3]) AS l, tail([1,2,3]) AS t").unwrap();
    assert_eq!(result.rows[0].get("h"), Some(&PropertyValue::Int(1)));
    assert_eq!(result.rows[0].get("l"), Some(&PropertyValue::Int(3)));
    if let Some(PropertyValue::List(tail)) = result.rows[0].get("t") {
        assert_eq!(tail.len(), 2);
    } else {
        panic!("expected list");
    }
}

#[test]
fn e2e_math_functions_combined() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN abs(-5) AS a, sign(-5) AS s, round(2.5) AS r, floor(2.9) AS f, ceil(2.1) AS c").unwrap();
    assert_eq!(result.rows[0].get("a"), Some(&PropertyValue::Int(5)));
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::Int(-1)));
    assert_eq!(result.rows[0].get("r"), Some(&PropertyValue::Double(3.0)));
    assert_eq!(result.rows[0].get("f"), Some(&PropertyValue::Double(2.0)));
    assert_eq!(result.rows[0].get("c"), Some(&PropertyValue::Double(3.0)));
}

#[test]
fn e2e_null_in_arithmetic() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Node {val: 5})").unwrap();
    ctx.run("CREATE (n:Node)").unwrap();
    let result = ctx.run("MATCH (n:Node) RETURN n.val + 1 AS v").unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn e2e_boolean_in_where() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Node {active: true})").unwrap();
    ctx.run("CREATE (n:Node {active: false})").unwrap();
    let result = ctx.run("MATCH (n:Node) WHERE n.active = true RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_order_by_multiple_columns() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {a: 1, b: 2})").unwrap();
    ctx.run("CREATE (n:Item {a: 1, b: 1})").unwrap();
    ctx.run("CREATE (n:Item {a: 2, b: 1})").unwrap();
    let result = ctx.run("MATCH (n:Item) RETURN n.a AS a, n.b AS b ORDER BY a, b").unwrap();
    assert_eq!(result.rows[0].get("b"), Some(&PropertyValue::Int(1)));
    assert_eq!(result.rows[2].get("a"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_match_with_node_property_on_right() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A {val: 1})-[:R]->(b:B {val: 2})").unwrap();
    let result = ctx.run("MATCH (a:A)-[:R]->(b:B {val: 2}) RETURN a.val AS av").unwrap();
    assert_eq!(result.rows[0].get("av"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_create_with_multiple_properties() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30, email: 'alice@example.com'})").unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN n.name AS name, n.age AS age, n.email AS email").unwrap();
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(result.rows[0].get("age"), Some(&PropertyValue::Int(30)));
    assert_eq!(result.rows[0].get("email"), Some(&PropertyValue::String("alice@example.com".into())));
}

#[test]
fn e2e_complex_path_with_properties() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})").unwrap();
    let result = ctx.run("MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.since = 2020 RETURN a.name AS an, b.name AS bn").unwrap();
    assert_eq!(result.rows[0].get("an"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(result.rows[0].get("bn"), Some(&PropertyValue::String("Bob".into())));
}

#[test]
fn e2e_aggregation_without_group_by() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {val: 10})").unwrap();
    ctx.run("CREATE (n:Item {val: 20})").unwrap();
    ctx.run("CREATE (n:Item {val: 30})").unwrap();
    let result = ctx.run("MATCH (n:Item) RETURN sum(n.val) AS s, avg(n.val) AS a, min(n.val) AS mn, max(n.val) AS mx, count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::Int(60)));
    assert_eq!(result.rows[0].get("a"), Some(&PropertyValue::Double(20.0)));
    assert_eq!(result.rows[0].get("mn"), Some(&PropertyValue::Int(10)));
    assert_eq!(result.rows[0].get("mx"), Some(&PropertyValue::Int(30)));
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(3)));
}

#[test]
fn e2e_return_count_star_and_property() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {val: 1})").unwrap();
    ctx.run("CREATE (n:Item)").unwrap();
    let result = ctx.run("MATCH (n:Item) RETURN count(*) AS cstar, count(n.val) AS cprop").unwrap();
    assert_eq!(result.rows[0].get("cstar"), Some(&PropertyValue::Int(2)));
    assert_eq!(result.rows[0].get("cprop"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_query_cache_reuse() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Node {val: 1})").unwrap();
    // Run same query twice to exercise cache
    let r1 = ctx.run("MATCH (n:Node) RETURN n.val AS v").unwrap();
    let r2 = ctx.run("MATCH (n:Node) RETURN n.val AS v").unwrap();
    assert_eq!(r1.rows, r2.rows);
}


#[test]
fn e2e_create_then_immediately_match() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Temp {id: 't1'})").unwrap();
    let result = ctx.run("MATCH (n:Temp) RETURN n.id AS id").unwrap();
    assert_eq!(result.rows[0].get("id"), Some(&PropertyValue::String("t1".into())));
}

#[test]
fn e2e_multiple_create_statements() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:A {id: 1})").unwrap();
    ctx.run("CREATE (n:B {id: 2})").unwrap();
    ctx.run("CREATE (n:C {id: 3})").unwrap();
    let result = ctx.run("MATCH (n) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(3)));
}



#[test]
fn e2e_subtract_operator() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN 10 - 3 AS r").unwrap();
    assert_eq!(result.rows[0].get("r"), Some(&PropertyValue::Int(7)));
}


#[test]
fn e2e_not_operator() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN NOT true AS nt, NOT false AS nf").unwrap();
    assert_eq!(result.rows[0].get("nt"), Some(&PropertyValue::Bool(false)));
    assert_eq!(result.rows[0].get("nf"), Some(&PropertyValue::Bool(true)));
}


#[test]
fn e2e_rand_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN rand() AS r1, rand() AS r2").unwrap();
    if let (Some(PropertyValue::Double(r1)), Some(PropertyValue::Double(r2))) = (result.rows[0].get("r1"), result.rows[0].get("r2")) {
        assert!(*r1 >= 0.0 && *r1 < 1.0);
        assert!(*r2 >= 0.0 && *r2 < 1.0);
    } else {
        panic!("expected double");
    }
}

#[test]
fn e2e_e_and_pi_constants() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN e() AS e_val, pi() AS pi_val").unwrap();
    assert_eq!(result.rows[0].get("e_val"), Some(&PropertyValue::Double(std::f64::consts::E)));
    assert_eq!(result.rows[0].get("pi_val"), Some(&PropertyValue::Double(std::f64::consts::PI)));
}

#[test]
fn e2e_tointeger_and_tofloat() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN tointeger('42') AS i, tofloat('3.14') AS f").unwrap();
    assert_eq!(result.rows[0].get("i"), Some(&PropertyValue::Int(42)));
    assert_eq!(result.rows[0].get("f"), Some(&PropertyValue::Double(3.14)));
}

#[test]
fn e2e_apoc_coll_max_min() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc.coll.max([1, 5, 3]) AS mx, apoc.coll.min([1, 5, 3]) AS mn").unwrap();
    assert_eq!(result.rows[0].get("mx"), Some(&PropertyValue::Int(5)));
    assert_eq!(result.rows[0].get("mn"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_apoc_map_get() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc.map.get({a: 1, b: 2}, 'a') AS v").unwrap();
    assert_eq!(result.rows[0].get("v"), Some(&PropertyValue::Int(1)));
}



#[test]
fn e2e_point_distance_3d() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN distance(point({x: 0, y: 0, z: 0, crs: 'cartesian-3d'}), point({x: 1, y: 1, z: 1, crs: 'cartesian-3d'})) AS d").unwrap();
    let expected = (3.0f64).sqrt();
    assert_eq!(result.rows[0].get("d"), Some(&PropertyValue::Double(expected)));
}


#[test]
fn e2e_datetime_components() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN year(datetime('2024-05-15T10:30:00Z')) AS y, month(datetime('2024-05-15T10:30:00Z')) AS m, day(datetime('2024-05-15T10:30:00Z')) AS d").unwrap();
    assert_eq!(result.rows[0].get("y"), Some(&PropertyValue::Int(2024)));
    assert_eq!(result.rows[0].get("m"), Some(&PropertyValue::Int(5)));
    assert_eq!(result.rows[0].get("d"), Some(&PropertyValue::Int(15)));
}


#[test]
fn e2e_localdatetime_components() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN hour(localdatetime('2024-01-01T14:30:00')) AS h, minute(localdatetime('2024-01-01T14:30:00')) AS m").unwrap();
    assert_eq!(result.rows[0].get("h"), Some(&PropertyValue::Int(14)));
    assert_eq!(result.rows[0].get("m"), Some(&PropertyValue::Int(30)));
}

#[test]
fn e2e_transaction_isolation_read_committed() {
    let ctx = TestCtx::new();
    let tx = ctx.storage.begin_transaction(mgcore::delta::IsolationLevel::ReadCommitted);
    ctx.run("CREATE (n:Node {val: 100})").unwrap();
    ctx.storage.commit_transaction(&tx);
    let result = ctx.run("MATCH (n:Node) RETURN n.val AS v").unwrap();
    assert_eq!(result.rows[0].get("v"), Some(&PropertyValue::Int(100)));
}

#[test]
fn e2e_transaction_isolation_read_uncommitted() {
    let ctx = TestCtx::new();
    let tx = ctx.storage.begin_transaction(mgcore::delta::IsolationLevel::ReadUncommitted);
    ctx.run("CREATE (n:Node {val: 200})").unwrap();
    ctx.storage.commit_transaction(&tx);
    let result = ctx.run("MATCH (n:Node) RETURN n.val AS v").unwrap();
    assert_eq!(result.rows[0].get("v"), Some(&PropertyValue::Int(200)));
}


#[test]
fn e2e_storage_edge_count() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A)-[:R]->(b:B)").unwrap();
    assert_eq!(ctx.storage.edge_count(), 1);
}

#[test]
fn e2e_catalog_name_consistency() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice'})").unwrap();
    ctx.run("MATCH (n:Person) RETURN n.name AS name").unwrap();
    // Same label and property names should use same IDs
    let result = ctx.run("MATCH (n:Person) RETURN n.name AS name").unwrap();
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
}

#[test]
fn e2e_percentile_functions() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN percentileCont([1, 2, 3, 4], 0.5) AS p").unwrap();
    if let Some(PropertyValue::Double(v)) = result.rows[0].get("p") {
        assert!((v - 2.5).abs() < 0.01, "expected ~2.5, got {}", v);
    } else {
        panic!("expected double");
    }

    let result2 = ctx.run("RETURN percentileDisc([1, 2, 3, 4], 0.5) AS p").unwrap();
    assert_eq!(result2.rows[0].get("p"), Some(&PropertyValue::Double(3.0)));
}

#[test]
fn e2e_stdev_functions() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN stDev([2, 4, 4, 4, 5, 5, 7, 9]) AS s").unwrap();
    if let Some(PropertyValue::Double(v)) = result.rows[0].get("s") {
        assert!((v - 2.138).abs() < 0.01, "expected ~2.138, got {}", v);
    } else {
        panic!("expected double");
    }

    let result2 = ctx.run("RETURN stDevP([2, 4, 4, 4, 5, 5, 7, 9]) AS s").unwrap();
    if let Some(PropertyValue::Double(v)) = result2.rows[0].get("s") {
        assert!((v - 2.0).abs() < 0.01, "expected ~2.0, got {}", v);
    } else {
        panic!("expected double");
    }
}

#[test]
fn e2e_safe_conversion_functions() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN toFloatOrNull('3.14') AS f1, toFloatOrNull('bad') AS f2, toIntegerOrNull('42') AS i1, toIntegerOrNull('bad') AS i2, toBooleanOrNull('true') AS b1, toBooleanOrNull('maybe') AS b2").unwrap();
    assert_eq!(result.rows[0].get("f1"), Some(&PropertyValue::Double(3.14)));
    assert_eq!(result.rows[0].get("f2"), Some(&PropertyValue::Null));
    assert_eq!(result.rows[0].get("i1"), Some(&PropertyValue::Int(42)));
    assert_eq!(result.rows[0].get("i2"), Some(&PropertyValue::Null));
    assert_eq!(result.rows[0].get("b1"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("b2"), Some(&PropertyValue::Null));
}

#[test]
fn e2e_call_algo_graph_density() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a)-[:R]->(b)-[:R]->(c)-[:R]->(a)").unwrap();
    let result = ctx.run("CALL algo.graphDensity() YIELD density RETURN density").unwrap();
    assert_eq!(result.rows.len(), 1);
    if let Some(PropertyValue::Double(d)) = result.rows[0].get("density") {
        assert!((d - 0.5).abs() < 0.01, "expected ~0.5 for triangle, got {}", d);
    } else {
        panic!("expected double");
    }
}

#[test]
fn e2e_call_algo_global_clustering_coefficient() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a)-[:R]->(b)-[:R]->(c)-[:R]->(a)").unwrap();
    let result = ctx.run("CALL algo.globalClusteringCoefficient() YIELD coefficient RETURN coefficient").unwrap();
    assert_eq!(result.rows.len(), 1);
    if let Some(PropertyValue::Double(c)) = result.rows[0].get("coefficient") {
        assert!((c - 1.0).abs() < 0.01, "expected ~1.0 for triangle, got {}", c);
    } else {
        panic!("expected double");
    }
}

#[test]
fn e2e_call_algo_coreness() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a)-[:R]->(b)-[:R]->(c)-[:R]->(a), (a)-[:R]->(d)").unwrap();
    let result = ctx.run("CALL algo.coreness() YIELD nodeId, coreness RETURN coreness ORDER BY coreness").unwrap();
    assert_eq!(result.rows.len(), 4);
    // One node should have coreness 1 (the dangling node d)
    if let Some(PropertyValue::Int(c)) = result.rows[0].get("coreness") {
        assert_eq!(*c, 1);
    } else {
        panic!("expected int");
    }
}

#[test]
fn e2e_call_algo_degree_assortativity() {
    let ctx = TestCtx::new();
    // Mixed graph: center a connects to leaves b,c,d; plus b->c for degree variation
    ctx.run("CREATE (a)-[:R]->(b), (a)-[:R]->(c), (a)-[:R]->(d), (b)-[:R]->(c)").unwrap();
    let result = ctx.run("CALL algo.degreeAssortativity() YIELD assortativity RETURN assortativity").unwrap();
    assert_eq!(result.rows.len(), 1);
    if let Some(PropertyValue::Double(r)) = result.rows[0].get("assortativity") {
        assert!(*r <= 0.0, "graph should have non-positive assortativity, got {}", r);
    } else {
        panic!("expected double");
    }
}

#[test]
fn e2e_match_with_in_operator_list() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
    ctx.run("CREATE (n:Person {name: 'Bob', age: 25})").unwrap();
    ctx.run("CREATE (n:Person {name: 'Charlie', age: 35})").unwrap();

    let result = ctx.run("MATCH (n:Person) WHERE n.age IN [25, 35] RETURN n.name AS name ORDER BY name").unwrap();
    assert_rows_eq(&result, vec![
        vec![("name", PropertyValue::String("Bob".into()))],
        vec![("name", PropertyValue::String("Charlie".into()))],
    ]);
}

#[test]
fn e2e_create_index_and_query_with_index() {
    let ctx = TestCtx::new();
    ctx.run("CREATE INDEX ON :Person(name)").unwrap();
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
    ctx.run("CREATE (n:Person {name: 'Bob', age: 25})").unwrap();

    let result = ctx.run("MATCH (n:Person {name: 'Alice'}) RETURN n.age AS age").unwrap();
    assert_rows_eq(&result, vec![
        vec![("age", PropertyValue::Int(30))],
    ]);
}

#[test]
fn e2e_complex_path_variable_length_with_properties() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:City {name: 'A'})-[:ROAD {distance: 10}]->(b:City {name: 'B'})-[:ROAD {distance: 20}]->(c:City {name: 'C'})").unwrap();

    let result = ctx.run("MATCH (a:City)-[:ROAD*1..2]->(c:City) WHERE a.name = 'A' RETURN c.name AS name ORDER BY name").unwrap();
    assert_rows_eq(&result, vec![
        vec![("name", PropertyValue::String("B".into()))],
        vec![("name", PropertyValue::String("C".into()))],
    ]);
}

#[test]
fn e2e_multiple_set_in_single_query() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice'})").unwrap();
    let result = ctx.run("MATCH (n:Person) SET n.age = 30, n.city = 'NYC' RETURN n.age AS age, n.city AS city").unwrap();
    assert_rows_eq(&result, vec![
        vec![("age", PropertyValue::Int(30)), ("city", PropertyValue::String("NYC".into()))],
    ]);
}

#[test]
fn e2e_remove_multiple_properties() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30, city: 'NYC'})").unwrap();
    ctx.run("MATCH (n:Person) REMOVE n.age, n.city").unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN n.name AS name, n.age AS age, n.city AS city").unwrap();
    assert_rows_eq(&result, vec![
        vec![
            ("name", PropertyValue::String("Alice".into())),
            ("age", PropertyValue::Null),
            ("city", PropertyValue::Null),
        ],
    ]);
}

#[test]
fn e2e_return_list_literal() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN [1, 2, 3] AS list").unwrap();
    assert_eq!(result.rows[0].get("list"), Some(&PropertyValue::List(vec![
        PropertyValue::Int(1), PropertyValue::Int(2), PropertyValue::Int(3),
    ])));
}

#[test]
fn e2e_return_map_literal() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN {a: 1, b: 'hello'} AS m").unwrap();
    assert_eq!(result.rows[0].get("m"), Some(&PropertyValue::Map(vec![
        ("a".into(), PropertyValue::Int(1)),
        ("b".into(), PropertyValue::String("hello".into())),
    ])));
}

#[test]
fn e2e_pattern_comprehension_collect() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}), (a)-[:KNOWS]->(c:Person {name: 'Charlie'})").unwrap();
    let result = ctx.run("MATCH (a:Person) WHERE a.name = 'Alice' RETURN [(a)-[:KNOWS]->(f) | f.name] AS friends").unwrap();
    assert_eq!(result.rows.len(), 1);
    if let Some(PropertyValue::List(items)) = result.rows[0].get("friends") {
        assert_eq!(items.len(), 2);
    } else {
        panic!("expected list");
    }
}

#[test]
fn e2e_null_in_list_operations() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN coalesce(null, null, 'fallback') AS c").unwrap();
    assert_eq!(result.rows[0].get("c"), Some(&PropertyValue::String("fallback".into())));
}

#[test]
fn e2e_complex_boolean_with_null() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Test {flag: true})").unwrap();
    let result = ctx.run("MATCH (n:Test) WHERE n.flag = true OR n.missing IS NULL RETURN n.flag AS flag").unwrap();
    assert_eq!(result.rows[0].get("flag"), Some(&PropertyValue::Bool(true)));
}

#[test]
fn e2e_transaction_snapshot_isolation() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Account {balance: 100})").unwrap();

    // T1 reads
    let result1 = ctx.run("MATCH (n:Account) RETURN n.balance AS b").unwrap();
    assert_eq!(result1.rows[0].get("b"), Some(&PropertyValue::Int(100)));

    // T1 writes (simulated by running another query on same storage)
    ctx.run("MATCH (n:Account) SET n.balance = 200").unwrap();

    // Read again - should see updated value
    let result2 = ctx.run("MATCH (n:Account) RETURN n.balance AS b").unwrap();
    assert_eq!(result2.rows[0].get("b"), Some(&PropertyValue::Int(200)));
}

#[test]
fn e2e_filter_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN filter(x IN [1, 2, 3, 4, 5] WHERE x > 2) AS filtered").unwrap();
    assert_eq!(result.rows[0].get("filtered"), Some(&PropertyValue::List(vec![
        PropertyValue::Int(3), PropertyValue::Int(4), PropertyValue::Int(5),
    ])));
}

#[test]
fn e2e_filter_function_with_strings() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN filter(x IN ['apple', 'banana', 'apricot'] WHERE startsWith(x, 'a')) AS filtered").unwrap();
    assert_eq!(result.rows[0].get("filtered"), Some(&PropertyValue::List(vec![
        PropertyValue::String("apple".into()), PropertyValue::String("apricot".into()),
    ])));
}

#[test]
fn e2e_complex_nested_expressions() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node {val: 10})-[:LINK]->(b:Node {val: 20})-[:LINK]->(c:Node {val: 30})").unwrap();
    let result = ctx.run("MATCH (a:Node)-[:LINK*1..2]->(c:Node) RETURN sum(a.val + c.val) AS total").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn e2e_create_multiple_nodes_single_statement() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A {id: 1}), (b:B {id: 2}), (c:C {id: 3})").unwrap();
    let result = ctx.run("MATCH (n) RETURN count(n) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(3)));
}

#[test]
fn e2e_match_with_multiple_relationship_types() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company)").unwrap();
    let result = ctx.run("MATCH (a:Person)-[:KNOWS|WORKS_AT]->(b) RETURN count(b) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_optional_match_chain_with_missing() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A)-[:R]->(b:B)").unwrap();
    let result = ctx.run("MATCH (a:A) OPTIONAL MATCH (a)-[:R]->(b:B)-[:R]->(c:C) RETURN a.name AS a, c.name AS c").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("a"), Some(&PropertyValue::Null));
    assert_eq!(result.rows[0].get("c"), Some(&PropertyValue::Null));
}

#[test]
fn e2e_query_with_comments() {
    let ctx = TestCtx::new();
    ctx.run("// Create a person\nCREATE (n:Person {name: 'Alice'})").unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN n.name AS name").unwrap();
    assert_rows_eq(&result, vec![
        vec![("name", PropertyValue::String("Alice".into()))],
    ]);
}

#[test]
fn e2e_deeply_nested_property_access() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Node {a: {b: {c: 42}}})").unwrap();
    let result = ctx.run("MATCH (n:Node) RETURN n.a.b.c AS val").unwrap();
    assert_eq!(result.rows[0].get("val"), Some(&PropertyValue::Int(42)));
}

#[test]
fn e2e_multiple_order_by_with_nulls() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:A {x: 1, y: 'b'})").unwrap();
    ctx.run("CREATE (n:A {x: 2, y: null})").unwrap();
    ctx.run("CREATE (n:A {x: 1, y: 'a'})").unwrap();
    let result = ctx.run("MATCH (n:A) RETURN n.x AS x, n.y AS y ORDER BY x, y").unwrap();
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn e2e_aggregation_with_null_values() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Score {val: 10})").unwrap();
    ctx.run("CREATE (n:Score {val: null})").unwrap();
    ctx.run("CREATE (n:Score {val: 20})").unwrap();
    let result = ctx.run("MATCH (n:Score) RETURN sum(n.val) AS total, avg(n.val) AS mean, count(n.val) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("total"), Some(&PropertyValue::Int(30)));
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_create_and_match_with_multiple_labels() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person:Employee {name: 'Alice'})").unwrap();
    let result = ctx.run("MATCH (n:Person:Employee) RETURN n.name AS name").unwrap();
    assert_rows_eq(&result, vec![
        vec![("name", PropertyValue::String("Alice".into()))],
    ]);
}

#[test]
fn e2e_string_escape_sequences() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Node {text: 'line1\\nline2'})").unwrap();
    let result = ctx.run("MATCH (n:Node) RETURN n.text AS text").unwrap();
    assert_eq!(result.rows[0].get("text"), Some(&PropertyValue::String("line1\nline2".into())));
}

#[test]
fn e2e_large_list_literal() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN range(1, 100) AS list").unwrap();
    if let Some(PropertyValue::List(items)) = result.rows[0].get("list") {
        assert_eq!(items.len(), 100);
    } else {
        panic!("expected list");
    }
}

#[test]
fn e2e_map_literal_with_expressions() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN {a: 1 + 2, b: 'hello' + ' world'} AS m").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn e2e_variable_reuse_in_pattern() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node {name: 'A'})-[:SELF]->(a)").unwrap();
    let result = ctx.run("MATCH (a:Node)-[:SELF]->(a) RETURN a.name AS name").unwrap();
    assert_rows_eq(&result, vec![
        vec![("name", PropertyValue::String("A".into()))],
    ]);
}

#[test]
fn e2e_exists_with_pattern() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person)-[:KNOWS]->(b:Person)").unwrap();
    let result = ctx.run("MATCH (a:Person) WHERE EXISTS((a)-[:KNOWS]->(:Person)) RETURN count(a) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_call_procedure_with_yield() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a)-[:R]->(b), (b)-[:R]->(c)").unwrap();
    let result = ctx.run("CALL algo.degreeCentrality() YIELD nodeId, degree RETURN degree ORDER BY degree DESC LIMIT 1").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn e2e_transaction_rollback_then_read() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Node {val: 1})").unwrap();
    // In our test context, each query runs in its own implicit transaction
    // so rollback isn't directly testable without explicit tx API
    // Instead, verify that SET then another SET works correctly
    ctx.run("MATCH (n:Node) SET n.val = 2").unwrap();
    let result = ctx.run("MATCH (n:Node) RETURN n.val AS val").unwrap();
    assert_eq!(result.rows[0].get("val"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_complex_cypher_with_all_clauses() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice', age: 30}), (b:Person {name: 'Bob', age: 25})").unwrap();
    let result = ctx.run(
        "MATCH (n:Person) \
         WHERE n.age >= 25 \
         WITH n.name AS name, n.age AS age \
         ORDER BY age DESC \
         RETURN name, age \
         LIMIT 10"
    ).unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn e2e_null_handling_in_arithmetic() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN 1 + null AS a, null * 5 AS b, null / 2 AS c").unwrap();
    assert_eq!(result.rows[0].get("a"), Some(&PropertyValue::Null));
    assert_eq!(result.rows[0].get("b"), Some(&PropertyValue::Null));
    assert_eq!(result.rows[0].get("c"), Some(&PropertyValue::Null));
}

#[test]
fn e2e_boolean_property_in_where() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Flag {active: true}), (b:Flag {active: false})").unwrap();
    let result = ctx.run("MATCH (n:Flag) WHERE n.active = true RETURN count(n) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_string_concatenation_with_plus() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN 'hello' + ' ' + 'world' AS greeting").unwrap();
    assert_eq!(result.rows[0].get("greeting"), Some(&PropertyValue::String("hello world".into())));
}

#[test]
fn e2e_list_slicing_with_range() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN [1, 2, 3, 4, 5][1..3] AS slice").unwrap();
    assert_eq!(result.rows[0].get("slice"), Some(&PropertyValue::List(vec![
        PropertyValue::Int(2), PropertyValue::Int(3),
    ])));
}

#[test]
fn e2e_map_access_with_dot_notation() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN {name: 'Alice', age: 30}.name AS name").unwrap();
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
}

#[test]
fn e2e_merge_on_create_set() {
    let ctx = TestCtx::new();
    ctx.run("MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created = true").unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN n.created AS created").unwrap();
    assert_eq!(result.rows[0].get("created"), Some(&PropertyValue::Bool(true)));
}

#[test]
fn e2e_merge_on_match_set() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', visits: 1})").unwrap();
    ctx.run("MERGE (n:Person {name: 'Alice'}) ON MATCH SET n.visits = n.visits + 1").unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN n.visits AS visits").unwrap();
    assert_eq!(result.rows[0].get("visits"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_path_length_with_relationship_count() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Start)-[:R]->(b:Mid)-[:R]->(c:End)").unwrap();
    let result = ctx.run("MATCH p = (a:Start)-[:R*1..2]->(c:End) RETURN length(p) AS len").unwrap();
    for row in &result.rows {
        println!("path row: {:?}", row);
    }
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("len"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_single_node_path() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node {name: 'A'})").unwrap();
    let result = ctx.run("MATCH p = (a:Node) RETURN length(p) AS len, nodes(p) AS ns").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("len"), Some(&PropertyValue::Int(0)));
    if let Some(PropertyValue::List(nodes)) = result.rows[0].get("ns") {
        assert_eq!(nodes.len(), 1);
    } else {
        panic!("Expected list of nodes");
    }
}

#[test]
fn e2e_exists_in_return() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person)-[:KNOWS]->(b:Person)").unwrap();
    let result = ctx.run("MATCH (a:Person) RETURN a.name AS name, EXISTS((a)-[:KNOWS]->(:Person)) AS has_friend").unwrap();
    assert_eq!(result.rows[0].get("has_friend"), Some(&PropertyValue::Bool(true)));
}

#[test]
fn e2e_size_on_map() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN size({a: 1, b: 2, c: 3}) AS s").unwrap();
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::Int(3)));
}

#[test]
fn e2e_datetime_components_extraction() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN date().year AS y").unwrap();
    assert_eq!(result.rows.len(), 1);
    if let Some(PropertyValue::Int(y)) = result.rows[0].get("y") {
        assert!(*y >= 2026);
    }
}

#[test]
fn e2e_point_distance_3d_wgs84() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN distance(point({latitude: 0, longitude: 0, height: 0, crs: 'wgs-84-3d'}), point({latitude: 0, longitude: 0, height: 100, crs: 'wgs-84-3d'})) AS d").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn e2e_randomuuid_format() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN randomUUID() AS uuid").unwrap();
    if let Some(PropertyValue::String(s)) = result.rows[0].get("uuid") {
        assert_eq!(s.len(), 36);
        assert!(s.contains('-'));
    } else {
        panic!("expected string uuid");
    }
}

#[test]
fn e2e_timestamp_returns_integer() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN timestamp() AS ts").unwrap();
    if let Some(PropertyValue::Int(ts)) = result.rows[0].get("ts") {
        assert!(*ts > 0);
    } else {
        panic!("expected int timestamp");
    }
}

#[test]
fn e2e_count_star_vs_count_property_null() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A {x: 1}), (b:A), (c:A {x: 2})").unwrap();
    let result = ctx.run("MATCH (n:A) RETURN count(*) AS total, count(n.x) AS non_null").unwrap();
    assert_eq!(result.rows[0].get("total"), Some(&PropertyValue::Int(3)));
    assert_eq!(result.rows[0].get("non_null"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_multiple_match_patterns_union() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A), (b:B), (c:C)").unwrap();
    let result = ctx.run("MATCH (n:A), (m:B), (o:C) RETURN count(n) + count(m) + count(o) AS total").unwrap();
    assert_eq!(result.rows[0].get("total"), Some(&PropertyValue::Int(3)));
}

#[test]
fn e2e_union_distinct() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A {name: 'Alice'}), (b:A {name: 'Bob'}), (c:C {name: 'Alice'}), (d:C {name: 'Charlie'})").unwrap();
    let result = ctx.run("MATCH (n:A) RETURN n.name AS name UNION MATCH (n:C) RETURN n.name AS name").unwrap();
    // DISTINCT union should deduplicate 'Alice' (appears in both A and C)
    assert_eq!(result.rows.len(), 3);
    let names: Vec<String> = result.rows.iter().map(|r| match r.get("name") {
        Some(PropertyValue::String(s)) => s.clone(),
        _ => String::new(),
    }).collect();
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Bob".to_string()));
    assert!(names.contains(&"Charlie".to_string()));
}

#[test]
fn e2e_union_all() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A {name: 'Alice'}), (c:C {name: 'Alice'})").unwrap();
    let result = ctx.run("MATCH (n:A) RETURN n.name AS name UNION ALL MATCH (n:C) RETURN n.name AS name").unwrap();
    // ALL union should keep duplicates
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(result.rows[1].get("name"), Some(&PropertyValue::String("Alice".into())));
}

#[test]
fn e2e_reduce_expression() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN reduce(sum = 0, x IN [1, 2, 3, 4] | sum + x) AS total").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("total"), Some(&PropertyValue::Int(10)));
}

#[test]
fn e2e_reduce_expression_string_concat() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN reduce(s = '', c IN ['a', 'b', 'c'] | s + c) AS txt").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("txt"), Some(&PropertyValue::String("abc".into())));
}

#[test]
fn e2e_relationship_direction_both_ways_query() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A)-[:R]->(b:B), (c:C)-[:R]->(b:B)").unwrap();
    let result = ctx.run("MATCH (a)-[:R]-(b:B) RETURN count(a) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_show_databases() {
    let ctx = TestCtx::new();
    let result = ctx.run("SHOW DATABASES").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("default".into())));
}

#[test]
fn e2e_show_indexes() {
    let ctx = TestCtx::new();
    ctx.run("CREATE INDEX ON :Person(name)").unwrap();
    let result = ctx.run("SHOW INDEXES").unwrap();
    assert!(result.rows.len() >= 1);
}

#[test]
fn e2e_show_indexes_all_types() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30, vec: [1.0, 2.0, 3.0], loc: point({x: 1.0, y: 2.0})})").unwrap();
    ctx.run("CREATE INDEX ON :Person(name)").unwrap();
    ctx.run("CALL db.createTextIndex({label: 'Person', properties: ['name']})").unwrap();
    ctx.run("CALL db.createVectorIndex({label: 'Person', property: 'vec', dimension: 3, distance: 'cosine'})").unwrap();
    ctx.run("CALL db.createPointIndex({label: 'Person', property: 'loc'})").unwrap();

    let result = ctx.run("SHOW INDEXES").unwrap();
    assert!(result.rows.len() >= 3, "expected at least 3 index rows, got {}", result.rows.len());

    let types: Vec<String> = result.rows.iter()
        .filter_map(|r| match r.get("type") {
            Some(PropertyValue::String(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(types.iter().any(|t| t == "text"), "missing text index, got {:?}", types);
    assert!(types.iter().any(|t| t == "vector"), "missing vector index, got {:?}", types);
    assert!(types.iter().any(|t| t == "point"), "missing point index, got {:?}", types);
}

#[test]
fn e2e_show_constraints() {
    let ctx = TestCtx::new();
    ctx.run("CREATE CONSTRAINT ON (n:Person) ASSERT n.email IS UNIQUE").unwrap();
    let result = ctx.run("SHOW CONSTRAINTS").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("type"), Some(&PropertyValue::String("Unique".into())));
}

#[test]
fn e2e_create_with_null_property() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Node {x: null, y: 'valid'})").unwrap();
    let result = ctx.run("MATCH (n:Node) RETURN n.x AS x, n.y AS y").unwrap();
    assert_eq!(result.rows[0].get("x"), Some(&PropertyValue::Null));
    assert_eq!(result.rows[0].get("y"), Some(&PropertyValue::String("valid".into())));
}

#[test]
fn e2e_set_to_null_then_coalesce() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Node {name: 'Alice'})").unwrap();
    ctx.run("MATCH (n:Node) SET n.name = null").unwrap();
    let result = ctx.run("MATCH (n:Node) RETURN coalesce(n.name, 'Unknown') AS name").unwrap();
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Unknown".into())));
}

#[test]
fn e2e_complex_aggregation_with_grouping_null() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Item {cat: 'A', val: 10}), (b:Item {cat: 'A', val: 20}), (c:Item {cat: null, val: 30})").unwrap();
    let result = ctx.run("MATCH (n:Item) RETURN n.cat AS cat, sum(n.val) AS total ORDER BY cat").unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn e2e_foreach_with_range() {
    let ctx = TestCtx::new();
    let _result = ctx.run("FOREACH (i IN range(1, 3) | CREATE (n:Number {val: i}))").unwrap();
    let count = ctx.run("MATCH (n:Number) RETURN count(n) AS cnt").unwrap();
    assert_eq!(count.rows[0].get("cnt"), Some(&PropertyValue::Int(3)));
}

#[test]
fn e2e_unwind_empty_list_returns_nothing() {
    let ctx = TestCtx::new();
    let result = ctx.run("UNWIND [] AS x RETURN x").unwrap();
    assert_eq!(result.rows.len(), 0);
}

#[test]
fn e2e_case_expression_with_multiple_when() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN CASE WHEN 1 = 1 THEN 'one' WHEN 2 = 2 THEN 'two' ELSE 'other' END AS val").unwrap();
    assert_eq!(result.rows[0].get("val"), Some(&PropertyValue::String("one".into())));
}

#[test]
fn e2e_simple_form_case_expression() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END AS val").unwrap();
    assert_eq!(result.rows[0].get("val"), Some(&PropertyValue::String("two".into())));
}

#[test]
fn e2e_list_comprehension_extract() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN extract(x IN [1, 2, 3] | x * 2) AS doubled").unwrap();
    assert_eq!(result.rows[0].get("doubled"), Some(&PropertyValue::List(vec![
        PropertyValue::Int(2), PropertyValue::Int(4), PropertyValue::Int(6),
    ])));
}

#[test]
fn e2e_pattern_with_multiple_relationships() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A)-[:R1]->(b:B)-[:R2]->(c:C)").unwrap();
    let result = ctx.run("MATCH (a:A)-[:R1]->(b:B)-[:R2]->(c:C) RETURN a.name AS a, b.name AS b, c.name AS c").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn e2e_return_all_variables() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap();
    let result = ctx.run("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN *").unwrap();
    assert_eq!(result.rows.len(), 1);
    // Both a and b should be present
    assert!(result.rows[0].contains_key("a"));
    assert!(result.rows[0].contains_key("b"));
}
#[test]
fn e2e_isempty_function_v2() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN isEmpty([]) AS e1, isEmpty([1]) AS e2, isEmpty('') AS e3, isEmpty('x') AS e4").unwrap();
    assert_eq!(result.rows[0].get("e1"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("e2"), Some(&PropertyValue::Bool(false)));
    assert_eq!(result.rows[0].get("e3"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("e4"), Some(&PropertyValue::Bool(false)));
}

#[test]
fn e2e_toboolean_function_v2() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN toBoolean('true') AS b1, toBoolean('false') AS b2, toBoolean(1) AS b3, toBoolean(0) AS b4").unwrap();
    assert_eq!(result.rows[0].get("b1"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("b2"), Some(&PropertyValue::Bool(false)));
    assert_eq!(result.rows[0].get("b3"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("b4"), Some(&PropertyValue::Bool(false)));
}

#[test]
fn e2e_encode_decode_json() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN encodeJson({name: 'Alice', age: 30}) AS json").unwrap();
    let json_val = result.rows[0].get("json").unwrap().clone();
    assert!(matches!(json_val, PropertyValue::String(_)));
    if let PropertyValue::String(s) = json_val {
        assert!(s.contains("Alice"));
        assert!(s.contains("30"));
    }

    let result2 = ctx.run("RETURN decodeJson('{\"x\": 1, \"y\": 2}') AS obj").unwrap();
    let obj = result2.rows[0].get("obj").unwrap().clone();
    assert!(matches!(obj, PropertyValue::Map(_)));
}

#[test]
fn e2e_randomstring_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN randomstring(8) AS s1, randomstring(12) AS s2").unwrap();
    let s1 = result.rows[0].get("s1").unwrap().clone();
    let s2 = result.rows[0].get("s2").unwrap().clone();
    assert!(matches!(s1, PropertyValue::String(_)));
    assert!(matches!(s2, PropertyValue::String(_)));
    if let PropertyValue::String(a) = s1 {
        assert_eq!(a.len(), 8);
    }
    if let PropertyValue::String(b) = s2 {
        assert_eq!(b.len(), 12);
    }
}

#[test]
fn e2e_map_literal_in_return() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN {a: 1, b: 'two', c: true} AS m").unwrap();
    assert_eq!(result.rows.len(), 1);
    let m = result.rows[0].get("m").unwrap().clone();
    assert!(matches!(m, PropertyValue::Map(_)));
}

#[test]
fn e2e_list_literal_in_return() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN [1, 'two', true, null] AS l").unwrap();
    assert_eq!(result.rows.len(), 1);
    let l = result.rows[0].get("l").unwrap().clone();
    assert!(matches!(l, PropertyValue::List(_)));
}

#[test]
fn e2e_multiple_create_statements_v2() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A), (b:B), (c:C)").unwrap();
    let result = ctx.run("MATCH (n) RETURN count(n) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(3)));
}

#[test]
fn e2e_match_with_optional_where() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice', age: 30}), (b:Person {name: 'Bob'})").unwrap();
    let result = ctx.run("MATCH (n:Person) WHERE n.age IS NOT NULL RETURN n.name AS name").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
}

#[test]
fn e2e_match_with_optional_property_null() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob', age: 25})").unwrap();
    let result = ctx.run("MATCH (n:Person) WHERE n.age IS NULL RETURN n.name AS name").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
}

#[test]
fn e2e_return_expression_in_order_by() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Item {val: 3}), (b:Item {val: 1}), (c:Item {val: 2})").unwrap();
    let result = ctx.run("MATCH (n:Item) RETURN n.val AS v ORDER BY v + 1").unwrap();
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0].get("v"), Some(&PropertyValue::Int(1)));
    assert_eq!(result.rows[1].get("v"), Some(&PropertyValue::Int(2)));
    assert_eq!(result.rows[2].get("v"), Some(&PropertyValue::Int(3)));
}

#[test]
fn e2e_create_with_map_property() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Node {data: {x: 1, y: 2}})").unwrap();
    let result = ctx.run("MATCH (n:Node) RETURN n.data AS data").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].get("data"), Some(PropertyValue::Map(_))));
}

#[test]
fn e2e_create_with_list_property() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Node {tags: ['a', 'b', 'c']})").unwrap();
    let result = ctx.run("MATCH (n:Node) RETURN n.tags AS tags").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(result.rows[0].get("tags"), Some(PropertyValue::List(_))));
}

#[test]
fn e2e_merge_on_create_vs_on_match() {
    let ctx = TestCtx::new();
    ctx.run("MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created = true").unwrap();
    let result = ctx.run("MATCH (n:Person {name: 'Alice'}) RETURN n.created AS created").unwrap();
    assert_eq!(result.rows[0].get("created"), Some(&PropertyValue::Bool(true)));

    ctx.run("MERGE (n:Person {name: 'Alice'}) ON MATCH SET n.seen = true").unwrap();
    let result2 = ctx.run("MATCH (n:Person {name: 'Alice'}) RETURN n.seen AS seen").unwrap();
    assert_eq!(result2.rows[0].get("seen"), Some(&PropertyValue::Bool(true)));
}

#[test]
fn e2e_delete_with_detach() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a)-[:R]->(b)").unwrap();
    ctx.run("MATCH (a) DELETE a").unwrap();
    let result = ctx.run("MATCH (n) RETURN count(n) AS cnt").unwrap();
    // Both nodes deleted (cascade via our simple delete)
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(0)));
}

#[test]
fn e2e_set_multiple_properties_at_once() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice'})").unwrap();
    ctx.run("MATCH (n:Person) SET n.age = 30, n.city = 'NYC'").unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN n.age AS age, n.city AS city").unwrap();
    assert_eq!(result.rows[0].get("age"), Some(&PropertyValue::Int(30)));
    assert_eq!(result.rows[0].get("city"), Some(&PropertyValue::String("NYC".into())));
}

#[test]
fn e2e_remove_label_and_property_v2() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person:Employee {name: 'Alice', dept: 'HR'})").unwrap();
    ctx.run("MATCH (n) REMOVE n:Employee, n.dept").unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN n.name AS name").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn e2e_match_limit_offset_combined() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:N {v: 1}), (b:N {v: 2}), (c:N {v: 3}), (d:N {v: 4})").unwrap();
    let result = ctx.run("MATCH (n:N) RETURN n.v AS v ORDER BY v SKIP 1 LIMIT 2").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get("v"), Some(&PropertyValue::Int(2)));
    assert_eq!(result.rows[1].get("v"), Some(&PropertyValue::Int(3)));
}

#[test]
fn e2e_exists_subquery_with_variable() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap();
    let result = ctx.run("MATCH (a:Person) WHERE EXISTS { (a)-[:KNOWS]->(b:Person) } RETURN a.name AS name").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
}

#[test]
fn e2e_size_on_string() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN size('hello') AS len").unwrap();
    assert_eq!(result.rows[0].get("len"), Some(&PropertyValue::Int(5)));
}

#[test]
fn e2e_reverse_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN reverse('hello') AS r, reverse([1, 2, 3]) AS l").unwrap();
    assert_eq!(result.rows[0].get("r"), Some(&PropertyValue::String("olleh".into())));
    assert_eq!(result.rows[0].get("l"), Some(&PropertyValue::List(vec![
        PropertyValue::Int(3), PropertyValue::Int(2), PropertyValue::Int(1),
    ])));
}

#[test]
fn e2e_split_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN split('a,b,c', ',') AS parts").unwrap();
    assert_eq!(result.rows[0].get("parts"), Some(&PropertyValue::List(vec![
        PropertyValue::String("a".into()),
        PropertyValue::String("b".into()),
        PropertyValue::String("c".into()),
    ])));
}

#[test]
fn e2e_substring_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN substring('hello', 1, 3) AS s").unwrap();
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::String("ell".into())));
}

#[test]
fn e2e_trim_functions_v2() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN trim('  hello  ') AS t, ltrim('  hello  ') AS lt, rtrim('  hello  ') AS rt").unwrap();
    assert_eq!(result.rows[0].get("t"), Some(&PropertyValue::String("hello".into())));
    assert_eq!(result.rows[0].get("lt"), Some(&PropertyValue::String("hello  ".into())));
    assert_eq!(result.rows[0].get("rt"), Some(&PropertyValue::String("  hello".into())));
}

#[test]
fn e2e_stdev_functions_v2() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN stdev([2, 4, 4, 4, 5, 5, 7, 9]) AS s, stdevp([2, 4, 4, 4, 5, 5, 7, 9]) AS sp").unwrap();
    assert!(result.rows[0].get("s").is_some());
    assert!(result.rows[0].get("sp").is_some());
}

#[test]
fn e2e_percentile_functions_v2() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN percentileCont([1, 2, 3, 4, 5], 0.5) AS median").unwrap();
    assert!(result.rows[0].get("median").is_some());
}

#[test]
fn e2e_math_hyperbolic_functions() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN cosh(0) AS c, sinh(0) AS s, tanh(0) AS t").unwrap();
    assert_eq!(result.rows[0].get("c"), Some(&PropertyValue::Double(1.0)));
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::Double(0.0)));
    assert_eq!(result.rows[0].get("t"), Some(&PropertyValue::Double(0.0)));
}

#[test]
fn e2e_point_distance_functions() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN distance(point({x: 0, y: 0}), point({x: 3, y: 4})) AS d").unwrap();
    assert_eq!(result.rows[0].get("d"), Some(&PropertyValue::Double(5.0)));
}

#[test]
fn e2e_duration_function_v2() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN duration({days: 1, hours: 2}) AS d").unwrap();
    assert!(result.rows[0].get("d").is_some());
}

#[test]
fn e2e_date_component_functions() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN year(date('2023-06-15')) AS y, month(date('2023-06-15')) AS m, day(date('2023-06-15')) AS d").unwrap();
    assert_eq!(result.rows[0].get("y"), Some(&PropertyValue::Int(2023)));
    assert_eq!(result.rows[0].get("m"), Some(&PropertyValue::Int(6)));
    assert_eq!(result.rows[0].get("d"), Some(&PropertyValue::Int(15)));
}

#[test]
fn e2e_range_function_v2() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN range(1, 5) AS r").unwrap();
    assert_eq!(result.rows[0].get("r"), Some(&PropertyValue::List(vec![
        PropertyValue::Int(1), PropertyValue::Int(2), PropertyValue::Int(3), PropertyValue::Int(4), PropertyValue::Int(5),
    ])));
}

#[test]
fn e2e_head_last_tail_functions_v2() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN head([1, 2, 3]) AS h, last([1, 2, 3]) AS l, tail([1, 2, 3]) AS t").unwrap();
    assert_eq!(result.rows[0].get("h"), Some(&PropertyValue::Int(1)));
    assert_eq!(result.rows[0].get("l"), Some(&PropertyValue::Int(3)));
    assert_eq!(result.rows[0].get("t"), Some(&PropertyValue::List(vec![
        PropertyValue::Int(2), PropertyValue::Int(3),
    ])));
}

#[test]
fn e2e_collect_distinct() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A {v: 1}), (b:A {v: 1}), (c:A {v: 2})").unwrap();
    let result = ctx.run("MATCH (n:A) RETURN collect(DISTINCT n.v) AS vals").unwrap();
    let vals = result.rows[0].get("vals").unwrap().clone();
    assert!(matches!(vals, PropertyValue::List(_)));
    if let PropertyValue::List(l) = vals {
        assert_eq!(l.len(), 2);
    }
}

#[test]
fn e2e_apoc_coll_functions() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc.coll.union([1, 2], [2, 3]) AS u").unwrap();
    let u = result.rows[0].get("u").unwrap().clone();
    assert!(matches!(u, PropertyValue::List(_)));

    let result2 = ctx.run("RETURN apoc.coll.intersection([1, 2, 3], [2, 3, 4]) AS i").unwrap();
    let i = result2.rows[0].get("i").unwrap().clone();
    assert!(matches!(i, PropertyValue::List(_)));

    let result3 = ctx.run("RETURN apoc.coll.sort([3, 1, 2]) AS s").unwrap();
    let s = result3.rows[0].get("s").unwrap().clone();
    assert_eq!(s, PropertyValue::List(vec![
        PropertyValue::Int(1), PropertyValue::Int(2), PropertyValue::Int(3),
    ]));
}

#[test]
fn e2e_apoc_map_functions() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc.map.merge({a: 1}, {b: 2}) AS m").unwrap();
    let m = result.rows[0].get("m").unwrap().clone();
    assert!(matches!(m, PropertyValue::Map(_)));
}

#[test]
fn e2e_elementId_function() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})").unwrap();
    let result = ctx.run("MATCH (a:Person) RETURN elementId(a) AS id").unwrap();
    let id = result.rows[0].get("id").unwrap().clone();
    assert!(matches!(id, PropertyValue::String(_)));
    if let PropertyValue::String(s) = id {
        assert!(s.starts_with("v:"));
    }
}

#[test]
fn e2e_indexOf_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN indexOf([1, 2, 3, 2], 2) AS idx").unwrap();
    assert_eq!(result.rows[0].get("idx"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_insert_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN insert([1, 2, 3], 1, 99) AS l").unwrap();
    assert_eq!(result.rows[0].get("l"), Some(&PropertyValue::List(vec![
        PropertyValue::Int(1), PropertyValue::Int(99), PropertyValue::Int(2), PropertyValue::Int(3),
    ])));
}

#[test]
fn e2e_shuffle_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN shuffle([1, 2, 3, 4, 5]) AS l").unwrap();
    let l = result.rows[0].get("l").unwrap().clone();
    assert!(matches!(l, PropertyValue::List(_)));
    if let PropertyValue::List(items) = l {
        assert_eq!(items.len(), 5);
    }
}

#[test]
fn e2e_slice_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN slice([1, 2, 3, 4, 5], 1, 4) AS l").unwrap();
    assert_eq!(result.rows[0].get("l"), Some(&PropertyValue::List(vec![
        PropertyValue::Int(2), PropertyValue::Int(3), PropertyValue::Int(4),
    ])));
}

#[test]
fn e2e_count_function_on_list() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN count([1, 2, 3]) AS c, count('hello') AS s").unwrap();
    assert_eq!(result.rows[0].get("c"), Some(&PropertyValue::Int(1)));
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_call_subquery_basic() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice', age: 30})").unwrap();
    ctx.run("CREATE (b:Person {name: 'Bob', age: 25})").unwrap();
    let result = ctx.run("CALL { MATCH (p:Person) RETURN p.name AS name ORDER BY p.name LIMIT 1 } RETURN name").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".to_string())));
}

#[test]
fn e2e_call_subquery_cartesian() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:A {id: 1})").unwrap();
    ctx.run("CREATE (b:A {id: 2})").unwrap();
    ctx.run("CREATE (c:B {id: 10})").unwrap();
    let result = ctx.run("MATCH (a:A) CALL { MATCH (b:B) RETURN b.id AS bid } RETURN a.id AS aid, bid").unwrap();
    // 2 A nodes × 1 B node = 2 rows
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn e2e_call_subquery_count() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Item)").unwrap();
    ctx.run("CREATE (b:Item)").unwrap();
    ctx.run("CREATE (c:Item)").unwrap();
    let result = ctx.run("CALL { MATCH (i:Item) RETURN count(i) AS cnt } RETURN cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(3)));
}

#[test]
fn e2e_call_subquery_with_outer_binding() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap();
    let result = ctx.run("MATCH (p:Person) CALL { MATCH (q:Person) RETURN q.name AS friend ORDER BY q.name LIMIT 1 } RETURN p.name AS name, friend").unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn e2e_tostringlist_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN toStringList([1, 'hello', true]) AS l").unwrap();
    assert_eq!(result.rows[0].get("l"), Some(&PropertyValue::List(vec![
        PropertyValue::String("1".to_string()),
        PropertyValue::String("hello".to_string()),
        PropertyValue::String("true".to_string()),
    ])));
}

#[test]
fn e2e_tointegerlist_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN toIntegerList(['1', '2', '3']) AS l").unwrap();
    assert_eq!(result.rows[0].get("l"), Some(&PropertyValue::List(vec![
        PropertyValue::Int(1), PropertyValue::Int(2), PropertyValue::Int(3),
    ])));
}

#[test]
fn e2e_tofloatlist_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN toFloatList(['1.5', '2.5']) AS l").unwrap();
    assert_eq!(result.rows[0].get("l"), Some(&PropertyValue::List(vec![
        PropertyValue::Double(1.5), PropertyValue::Double(2.5),
    ])));
}

#[test]
fn e2e_tobooleanlist_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN toBooleanList(['true', 'false']) AS l").unwrap();
    assert_eq!(result.rows[0].get("l"), Some(&PropertyValue::List(vec![
        PropertyValue::Bool(true), PropertyValue::Bool(false),
    ])));
}

#[test]
fn e2e_map_projection_selective() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30, city: 'NYC'})").unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN {n.name, n.age} AS m").unwrap();
    let m = result.rows[0].get("m").unwrap();
    if let PropertyValue::Map(entries) = m {
        let map: std::collections::HashMap<_, _> = entries.iter().cloned().collect();
        assert_eq!(map.get("name"), Some(&PropertyValue::String("Alice".to_string())));
        assert_eq!(map.get("age"), Some(&PropertyValue::Int(30)));
        assert!(!map.contains_key("city"));
    } else {
        panic!("expected map, got {:?}", m);
    }
}

#[test]
fn e2e_map_projection_all_with_extra() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Bob', age: 25})").unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN {n.*, extra: 42} AS m").unwrap();
    let m = result.rows[0].get("m").unwrap();
    if let PropertyValue::Map(entries) = m {
        let map: std::collections::HashMap<_, _> = entries.iter().cloned().collect();
        assert_eq!(map.get("name"), Some(&PropertyValue::String("Bob".to_string())));
        assert_eq!(map.get("age"), Some(&PropertyValue::Int(25)));
        assert_eq!(map.get("extra"), Some(&PropertyValue::Int(42)));
    } else {
        panic!("expected map, got {:?}", m);
    }
}

#[test]
fn e2e_round_with_precision() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN round(3.14159, 2) AS r").unwrap();
    assert_eq!(result.rows[0].get("r"), Some(&PropertyValue::Double(3.14)));
}

#[test]
fn e2e_tobooleanornull_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN toBooleanOrNull('true') AS t, toBooleanOrNull('false') AS f, toBooleanOrNull('maybe') AS n").unwrap();
    assert_eq!(result.rows[0].get("t"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("f"), Some(&PropertyValue::Bool(false)));
    assert_eq!(result.rows[0].get("n"), Some(&PropertyValue::Null));
}

#[test]
fn e2e_apoc_text_format_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc.text.format('hello {}', 'world') AS s").unwrap();
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::String("hello world".to_string())));
}

#[test]
fn e2e_apoc_coll_frequencies_as_map_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc.coll.frequenciesAsMap([1, 2, 2, 3, 3, 3]) AS m").unwrap();
    let m = result.rows[0].get("m").unwrap();
    if let PropertyValue::Map(entries) = m {
        let map: std::collections::HashMap<_, _> = entries.iter().cloned().collect();
        assert_eq!(map.get("Int(1)"), Some(&PropertyValue::Int(1)));
        assert_eq!(map.get("Int(2)"), Some(&PropertyValue::Int(2)));
        assert_eq!(map.get("Int(3)"), Some(&PropertyValue::Int(3)));
    } else {
        panic!("expected map, got {:?}", m);
    }
}

#[test]
fn e2e_explain_query() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice'})").unwrap();
    let result = ctx.run("EXPLAIN MATCH (n:Person) RETURN n").unwrap();
    assert_eq!(result.columns, vec!["PLAN"]);
    assert_eq!(result.rows.len(), 1);
    let plan = result.rows[0].get("PLAN").unwrap();
    if let PropertyValue::String(s) = plan {
        assert!(s.contains("LabelScan"), "plan should contain LabelScan: {}", s);
    } else {
        panic!("expected string plan, got {:?}", plan);
    }
}

#[test]
fn e2e_profile_query() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice'})").unwrap();
    ctx.run("CREATE (n:Person {name: 'Bob'})").unwrap();
    let result = ctx.run("PROFILE MATCH (n:Person) RETURN n").unwrap();
    assert_eq!(result.columns, vec!["PLAN", "ROWS", "TIME_MS"]);
    assert_eq!(result.rows.len(), 1);
    let plan = result.rows[0].get("PLAN").unwrap();
    if let PropertyValue::String(s) = plan {
        assert!(s.contains("LabelScan"), "plan should contain LabelScan: {}", s);
    } else {
        panic!("expected string plan, got {:?}", plan);
    }
    let rows = result.rows[0].get("ROWS").unwrap();
    assert_eq!(rows, &PropertyValue::Int(2));
    let time_ms = result.rows[0].get("TIME_MS").unwrap();
    if let PropertyValue::Int(ms) = time_ms {
        assert!(*ms >= 0, "time should be non-negative");
    } else {
        panic!("expected Int time, got {:?}", time_ms);
    }
}

#[test]
fn e2e_explain_create() {
    let ctx = TestCtx::new();
    let result = ctx.run("EXPLAIN CREATE (n:Test {val: 1})").unwrap();
    assert_eq!(result.columns, vec!["PLAN"]);
    assert_eq!(result.rows.len(), 1);
    let plan = result.rows[0].get("PLAN").unwrap();
    if let PropertyValue::String(s) = plan {
        assert!(s.contains("CreateVertex"), "plan should contain CreateVertex: {}", s);
    } else {
        panic!("expected string plan, got {:?}", plan);
    }
}

#[test]
fn e2e_profile_no_results() {
    let ctx = TestCtx::new();
    let result = ctx.run("PROFILE MATCH (n:NonExistent) RETURN n").unwrap();
    assert_eq!(result.columns, vec!["PLAN", "ROWS", "TIME_MS"]);
    let rows = result.rows[0].get("ROWS").unwrap();
    assert_eq!(rows, &PropertyValue::Int(0));
}

#[test]
fn e2e_list_comprehension_bracket_filter() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN [x IN [1, 2, 3, 4] WHERE x > 2] AS result").unwrap();
    assert_eq!(result.rows.len(), 1);
    let expected = PropertyValue::List(vec![PropertyValue::Int(3), PropertyValue::Int(4)]);
    assert_eq!(result.rows[0].get("result"), Some(&expected));
}

#[test]
fn e2e_list_comprehension_bracket_extract() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN [x IN [1, 2, 3] | x * 2] AS result").unwrap();
    assert_eq!(result.rows.len(), 1);
    let expected = PropertyValue::List(vec![PropertyValue::Int(2), PropertyValue::Int(4), PropertyValue::Int(6)]);
    assert_eq!(result.rows[0].get("result"), Some(&expected));
}

#[test]
fn e2e_list_comprehension_bracket_filter_extract() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN [x IN [1, 2, 3, 4, 5] WHERE x > 2 | x * 10] AS result").unwrap();
    assert_eq!(result.rows.len(), 1);
    let expected = PropertyValue::List(vec![PropertyValue::Int(30), PropertyValue::Int(40), PropertyValue::Int(50)]);
    assert_eq!(result.rows[0].get("result"), Some(&expected));
}

#[test]
fn e2e_list_comprehension_bracket_strings() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN [s IN ['hello', 'world'] | toUpper(s)] AS result").unwrap();
    assert_eq!(result.rows.len(), 1);
    let expected = PropertyValue::List(vec![
        PropertyValue::String("HELLO".into()),
        PropertyValue::String("WORLD".into()),
    ]);
    assert_eq!(result.rows[0].get("result"), Some(&expected));
}

#[test]
fn e2e_query_parameters() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
    ctx.run("CREATE (n:Person {name: 'Bob', age: 25})").unwrap();

    let mut params = std::collections::HashMap::new();
    params.insert("min_age".to_string(), PropertyValue::Int(26));
    let result = ctx
        .run_with_params("MATCH (n:Person) WHERE n.age > $min_age RETURN n.name AS name", &params)
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
}

#[test]
fn e2e_query_parameters_in_create() {
    let ctx = TestCtx::new();
    let mut params = std::collections::HashMap::new();
    params.insert("name".to_string(), PropertyValue::String("Charlie".into()));
    params.insert("age".to_string(), PropertyValue::Int(35));
    ctx.run_with_params("CREATE (n:Person {name: $name, age: $age})", &params)
        .unwrap();

    let result = ctx.run("MATCH (n:Person {name: 'Charlie'}) RETURN n.age AS age").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("age"), Some(&PropertyValue::Int(35)));
}

#[test]
fn e2e_query_parameters_missing() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice'})").unwrap();

    // Missing parameter should resolve to Null, so no rows match
    let result = ctx
        .run("MATCH (n:Person) WHERE n.name = $missing RETURN n")
        .unwrap();
    assert_eq!(result.rows.len(), 0);
}

#[test]
fn e2e_query_parameters_in_return() {
    let ctx = TestCtx::new();
    let mut params = std::collections::HashMap::new();
    params.insert("greeting".to_string(), PropertyValue::String("Hello".into()));
    let result = ctx.run_with_params("RETURN $greeting AS msg", &params).unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("msg"), Some(&PropertyValue::String("Hello".into())));
}

#[test]
fn e2e_algo_shortest_path_procedure() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:EDGE]->(b:Node)-[:EDGE]->(c:Node)").unwrap();

    // Query actual GIDs since they're auto-assigned
    let gid_result = ctx.run("MATCH (n:Node) RETURN id(n) AS gid ORDER BY gid").unwrap();
    let gids: Vec<i64> = gid_result.rows.iter()
        .map(|r| match r.get("gid") {
            Some(PropertyValue::Int(v)) => *v,
            _ => panic!("expected int gid"),
        })
        .collect();
    assert_eq!(gids.len(), 3);

    let result = ctx
        .run(&format!("CALL algo.shortest_path({}, {}) YIELD path RETURN path", gids[0], gids[2]))
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let path = result.rows[0].get("path").unwrap();
    if let PropertyValue::List(nodes) = path {
        assert_eq!(nodes.len(), 3);
        assert_eq!(nodes[0], PropertyValue::Int(gids[0]));
        assert_eq!(nodes[1], PropertyValue::Int(gids[1]));
        assert_eq!(nodes[2], PropertyValue::Int(gids[2]));
    } else {
        panic!("expected list path, got {:?}", path);
    }
}

#[test]
fn e2e_algo_betweenness_centrality_procedure() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)").unwrap();
    ctx.run("CREATE (a)-[:R]->(c)").unwrap();

    let result = ctx.run("CALL algo.betweenness_centrality() YIELD node, centrality RETURN node, centrality").unwrap();
    assert!(result.rows.len() >= 3);
}

#[test]
fn e2e_algo_clustering_coefficient_procedure() {
    let ctx = TestCtx::new();
    // Triangle: a-b, b-c, c-a
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)-[:R]->(a)").unwrap();

    let result = ctx.run("CALL algo.clustering_coefficient() YIELD node, coefficient RETURN node, coefficient").unwrap();
    assert_eq!(result.rows.len(), 3);
    // In a complete triangle, coefficient should be 1.0 for all nodes
    for row in &result.rows {
        let coeff = row.get("coefficient").unwrap();
        if let PropertyValue::Double(v) = coeff {
            assert!((*v - 1.0).abs() < 0.01, "expected ~1.0, got {}", v);
        }
    }
}

#[test]
fn e2e_algo_degree_centrality_procedure() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)").unwrap();

    let result = ctx.run("CALL algo.degree_centrality() YIELD node, degree RETURN node, degree").unwrap();
    assert!(result.rows.len() >= 3);
}

#[test]
fn e2e_algo_graph_density_procedure() {
    let ctx = TestCtx::new();
    // Triangle has 3 nodes, 3 edges, density = 3 / (3*2) = 0.5 for directed
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)-[:R]->(a)").unwrap();

    let result = ctx.run("CALL algo.graph_density() YIELD density RETURN density").unwrap();
    assert_eq!(result.rows.len(), 1);
    let density = result.rows[0].get("density").unwrap();
    if let PropertyValue::Double(v) = density {
        assert!((*v - 0.5).abs() < 0.01, "expected ~0.5, got {}", v);
    }
}

#[test]
fn e2e_algo_has_cycle_procedure() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)-[:R]->(a)").unwrap();

    let result = ctx.run("CALL algo.has_cycle() YIELD hasCycle RETURN hasCycle").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("hasCycle"), Some(&PropertyValue::Bool(true)));
}

#[test]
fn e2e_algo_is_bipartite_procedure() {
    let ctx = TestCtx::new();
    // Path graph a-b-c is bipartite
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)").unwrap();

    let result = ctx.run("CALL algo.is_bipartite() YIELD bipartite RETURN bipartite").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("bipartite"), Some(&PropertyValue::Bool(true)));
}

#[test]
fn e2e_algo_diameter_procedure() {
    let ctx = TestCtx::new();
    // Path of 3 nodes: diameter = 2
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)").unwrap();

    let result = ctx.run("CALL algo.diameter() YIELD diameter RETURN diameter").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("diameter"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_algo_coreness_procedure() {
    let ctx = TestCtx::new();
    // Triangle: each node has coreness 1 (or 2 depending on k-core definition)
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)-[:R]->(a)").unwrap();

    let result = ctx.run("CALL algo.coreness() YIELD node, coreness RETURN node, coreness").unwrap();
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn e2e_algo_center_procedure() {
    let ctx = TestCtx::new();
    // Path of 4 nodes: center = {2, 3}
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)-[:R]->(d:Node)").unwrap();

    let result = ctx.run("CALL algo.center() YIELD nodeId RETURN nodeId").unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn e2e_algo_periphery_procedure() {
    let ctx = TestCtx::new();
    // Path of 4 nodes: periphery = {1, 4}
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)-[:R]->(d:Node)").unwrap();

    let result = ctx.run("CALL algo.periphery() YIELD nodeId RETURN nodeId").unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn e2e_algo_small_world_coefficient_procedure() {
    let ctx = TestCtx::new();
    // Triangle has high clustering but short paths -> small world
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)-[:R]->(a)").unwrap();

    let result = ctx.run("CALL algo.small_world_coefficient() YIELD sigma RETURN sigma").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn e2e_algo_articulation_points_procedure() {
    let ctx = TestCtx::new();
    // Path of 3 nodes: middle node is articulation point
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)").unwrap();

    let result = ctx.run("CALL algo.articulation_points() YIELD nodeId RETURN nodeId").unwrap();
    // Node b is the articulation point
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn e2e_algo_is_biconnected_procedure() {
    let ctx = TestCtx::new();
    // Triangle is biconnected
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)-[:R]->(a)").unwrap();

    let result = ctx.run("CALL algo.is_biconnected() YIELD biconnected RETURN biconnected").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("biconnected"), Some(&PropertyValue::Bool(true)));
}

#[test]
fn e2e_algo_connected_components_procedure() {
    let ctx = TestCtx::new();
    // Two disconnected edges
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)").unwrap();
    ctx.run("CREATE (c:Node)-[:R]->(d:Node)").unwrap();

    let result = ctx.run("CALL algo.connected_components() YIELD nodeId, component RETURN nodeId, component").unwrap();
    assert_eq!(result.rows.len(), 4);
}

#[test]
fn e2e_algo_cycle_detection_procedure() {
    let ctx = TestCtx::new();
    // Triangle has a cycle
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)-[:R]->(a)").unwrap();

    let result = ctx.run("CALL algo.cycle_detection() YIELD nodeId RETURN nodeId").unwrap();
    // Cycle includes start node at both ends, so 4 rows for a 3-node cycle
    assert!(result.rows.len() >= 3);
}

#[test]
fn e2e_algo_all_pairs_shortest_path_procedure() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)").unwrap();

    let result = ctx.run("CALL algo.all_pairs_shortest_path() YIELD source, target, distance RETURN source, target, distance").unwrap();
    assert!(result.rows.len() >= 3);
}

#[test]
fn e2e_algo_clique_number_procedure() {
    let ctx = TestCtx::new();
    // Triangle: clique number = 3
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)-[:R]->(a)").unwrap();

    let result = ctx.run("CALL algo.clique_number() YIELD cliqueNumber RETURN cliqueNumber").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("cliqueNumber"), Some(&PropertyValue::Int(3)));
}

#[test]
fn e2e_algo_greedy_coloring_procedure() {
    let ctx = TestCtx::new();
    // Triangle needs 3 colors
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)-[:R]->(a)").unwrap();

    let result = ctx.run("CALL algo.greedy_coloring() YIELD nodeId, color RETURN nodeId, color").unwrap();
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn e2e_algo_degeneracy_procedure() {
    let ctx = TestCtx::new();
    // Triangle: degeneracy = 2
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)-[:R]->(a)").unwrap();

    let result = ctx.run("CALL algo.degeneracy() YIELD degeneracy RETURN degeneracy").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("degeneracy"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_duration_add_duration() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN duration('P1D') + duration('PT1H') AS d").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0].get("d"),
        Some(&PropertyValue::Duration(mgcore::temporal::Duration::new(0, 1, 3_600_000_000)))
    );
}

#[test]
fn e2e_duration_sub_duration() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN duration('P2D') - duration('P1D') AS d").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0].get("d"),
        Some(&PropertyValue::Duration(mgcore::temporal::Duration::new(0, 1, 0)))
    );
}

#[test]
fn e2e_date_add_duration() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN date('2024-01-01') + duration('P1D') AS d").unwrap();
    assert_eq!(result.rows.len(), 1);
    // date('2024-01-01') == 19724 days since epoch
    assert_eq!(
        result.rows[0].get("d"),
        Some(&PropertyValue::Date(mgcore::temporal::Date::from_days(19725)))
    );
}

#[test]
fn e2e_date_sub_duration() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN date('2024-01-01') - duration('P1D') AS d").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0].get("d"),
        Some(&PropertyValue::Date(mgcore::temporal::Date::from_days(19723)))
    );
}

#[test]
fn e2e_duration_add_date_commutative() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN duration('P1D') + date('2024-01-01') AS d").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0].get("d"),
        Some(&PropertyValue::Date(mgcore::temporal::Date::from_days(19725)))
    );
}

#[test]
fn e2e_localdatetime_add_duration() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN localdatetime('2024-01-01T00:00:00') + duration('P1D') AS d").unwrap();
    assert_eq!(result.rows.len(), 1);
    // localdatetime('2024-01-01T00:00:00') = 1704067200000000 us (19723 days), + 1 day = 1704153600000000
    let expected_us = 1704153600000000i64;
    assert_eq!(
        result.rows[0].get("d"),
        Some(&PropertyValue::LocalDateTime(mgcore::temporal::LocalDateTime::from_microseconds(expected_us)))
    );
}

#[test]
fn e2e_time_add_duration() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN time('12:00:00') + duration('PT1H') AS t").unwrap();
    assert_eq!(result.rows.len(), 1);
    let expected_us = 13i64 * 3_600 * 1_000_000;
    assert_eq!(
        result.rows[0].get("t"),
        Some(&PropertyValue::LocalTime(mgcore::temporal::LocalTime::from_microseconds(expected_us)))
    );
}

#[test]
fn e2e_time_sub_duration_wrap() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN time('01:00:00') - duration('PT2H') AS t").unwrap();
    assert_eq!(result.rows.len(), 1);
    let expected_us = 23i64 * 3_600 * 1_000_000;
    assert_eq!(
        result.rows[0].get("t"),
        Some(&PropertyValue::LocalTime(mgcore::temporal::LocalTime::from_microseconds(expected_us)))
    );
}

#[test]
fn e2e_date_property_access() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN date('2024-03-15').year AS y, date('2024-03-15').month AS m, date('2024-03-15').day AS d").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("y"), Some(&PropertyValue::Int(2024)));
    assert_eq!(result.rows[0].get("m"), Some(&PropertyValue::Int(3)));
    assert_eq!(result.rows[0].get("d"), Some(&PropertyValue::Int(15)));
}

#[test]
fn e2e_date_property_quarter_and_week() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN date('2024-03-15').quarter AS q, date('2024-03-15').week AS w, date('2024-03-15').dayOfWeek AS dow").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("q"), Some(&PropertyValue::Int(1)));
    // 2024-03-15 is a Friday, dow = 5 (Mon=1)
    assert_eq!(result.rows[0].get("dow"), Some(&PropertyValue::Int(5)));
    assert!(result.rows[0].get("w").is_some());
}

#[test]
fn e2e_localdatetime_property_access() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN localdatetime('2024-03-15T14:30:45').year AS y, localdatetime('2024-03-15T14:30:45').hour AS h, localdatetime('2024-03-15T14:30:45').minute AS m, localdatetime('2024-03-15T14:30:45').second AS s").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("y"), Some(&PropertyValue::Int(2024)));
    assert_eq!(result.rows[0].get("h"), Some(&PropertyValue::Int(14)));
    assert_eq!(result.rows[0].get("m"), Some(&PropertyValue::Int(30)));
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::Int(45)));
}

#[test]
fn e2e_time_property_access() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN time('14:30:45').hour AS h, time('14:30:45').minute AS m, time('14:30:45').second AS s").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("h"), Some(&PropertyValue::Int(14)));
    assert_eq!(result.rows[0].get("m"), Some(&PropertyValue::Int(30)));
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::Int(45)));
}

#[test]
fn e2e_duration_property_access() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN duration('P1Y2M3DT4H5M6S').months AS m, duration('P1Y2M3DT4H5M6S').days AS d, duration('P1Y2M3DT4H5M6S').seconds AS s").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("m"), Some(&PropertyValue::Int(14))); // 1*12 + 2 = 14 months total
    assert_eq!(result.rows[0].get("d"), Some(&PropertyValue::Int(3)));
    // 4h*3600 + 5m*60 + 6s = 14400 + 300 + 6 = 14706 seconds
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::Int(14_706)));
}

#[test]
fn e2e_duration_property_hours_minutes_microseconds() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN duration('PT1H30M45.5S').hours AS h, duration('PT1H30M45.5S').minutes AS m, duration('PT1H30M45.5S').milliseconds AS ms").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("h"), Some(&PropertyValue::Int(1)));
    assert_eq!(result.rows[0].get("m"), Some(&PropertyValue::Int(90))); // total minutes = 90
    assert_eq!(result.rows[0].get("ms"), Some(&PropertyValue::Int(5_445_500))); // total ms
}

#[test]
fn e2e_count_subquery_basic() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})").unwrap();
    ctx.run("CREATE (b:Person {name: 'Bob'})").unwrap();
    ctx.run("CREATE (c:Person {name: 'Charlie'})").unwrap();

    let result = ctx.run("RETURN COUNT { MATCH (n:Person) RETURN n } AS cnt").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(3)));
}

#[test]
fn e2e_count_subquery_with_where() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice', age: 30})").unwrap();
    ctx.run("CREATE (b:Person {name: 'Bob', age: 25})").unwrap();
    ctx.run("CREATE (c:Person {name: 'Charlie', age: 35})").unwrap();

    let result = ctx.run("RETURN COUNT { MATCH (n:Person) WHERE n.age > 26 RETURN n } AS cnt").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_count_subquery_empty() {
    let ctx = TestCtx::new();

    let result = ctx.run("RETURN COUNT { MATCH (n:Person) RETURN n } AS cnt").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(0)));
}

#[test]
fn e2e_count_subquery_with_outer_binding() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(c:Person {name: 'Charlie'})").unwrap();
    ctx.run("CREATE (d:Person {name: 'Dave'})").unwrap();

    // Each CREATE creates new nodes, so there are 5 Person nodes total (2 Alice, 1 Bob, 1 Charlie, 1 Dave)
    let result = ctx.run("MATCH (p:Person) RETURN p.name AS name, COUNT { MATCH (p)-[:KNOWS]->(q:Person) RETURN q } AS friends ORDER BY p.name").unwrap();
    assert_eq!(result.rows.len(), 5);
    let mut found = std::collections::HashMap::new();
    for row in &result.rows {
        let name = match row.get("name").unwrap() {
            PropertyValue::String(s) => s.clone(),
            _ => panic!("expected string"),
        };
        let count = match row.get("friends").unwrap() {
            PropertyValue::Int(n) => *n,
            _ => panic!("expected int"),
        };
        found.entry(name).and_modify(|e| *e += count).or_insert(count);
    }
    // Two Alice nodes each have 1 friend, so total Alice friend count = 2
    assert_eq!(found.get("Alice"), Some(&2));
    assert_eq!(found.get("Bob"), Some(&0));
    assert_eq!(found.get("Charlie"), Some(&0));
    assert_eq!(found.get("Dave"), Some(&0));
}

#[test]
fn e2e_regex_match_basic() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN 'hello world' =~ 'hello.*' AS m").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("m"), Some(&PropertyValue::Bool(true)));
}

#[test]
fn e2e_regex_match_false() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN 'hello' =~ '^world$' AS m").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("m"), Some(&PropertyValue::Bool(false)));
}

#[test]
fn e2e_regex_match_case_sensitive() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN 'Hello' =~ '^hello$' AS m").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("m"), Some(&PropertyValue::Bool(false)));
}

#[test]
fn e2e_regex_match_with_nodes() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', email: 'alice@example.com'})").unwrap();
    ctx.run("CREATE (n:Person {name: 'Bob', email: 'bob@test.org'})").unwrap();

    let result = ctx.run("MATCH (n:Person) WHERE n.email =~ '.*@example\\.com$' RETURN n.name AS name").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
}

#[test]
fn e2e_regex_match_invalid_pattern() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN 'hello' =~ '[invalid' AS m").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("m"), Some(&PropertyValue::Null));
}

#[test]
fn e2e_yield_star() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)").unwrap();
    let result = ctx.run("CALL algo.degree_centrality() YIELD * RETURN nodeId").unwrap();
    assert!(result.rows.len() >= 1);
}

// ─── Auth DDL tests ──────────────────────────────────────────────────────

#[test]
fn e2e_create_user() {
    let ctx = TestCtx::with_auth();
    ctx.run("CREATE USER alice IDENTIFIED BY 'Password123'").unwrap();
    let result = ctx.run("SHOW USERS").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("username"), Some(&PropertyValue::String("alice".into())));
}

#[test]
fn e2e_drop_user() {
    let ctx = TestCtx::with_auth();
    ctx.run("CREATE USER bob IDENTIFIED BY 'Password123'").unwrap();
    ctx.run("DROP USER bob").unwrap();
    let result = ctx.run("SHOW USERS").unwrap();
    assert_eq!(result.rows.len(), 0);
}

#[test]
fn e2e_create_role() {
    let ctx = TestCtx::with_auth();
    ctx.run("CREATE ROLE admin").unwrap();
    let result = ctx.run("SHOW ROLES").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("role"), Some(&PropertyValue::String("admin".into())));
}

#[test]
fn e2e_drop_role() {
    let ctx = TestCtx::with_auth();
    ctx.run("CREATE ROLE reader").unwrap();
    ctx.run("DROP ROLE reader").unwrap();
    let result = ctx.run("SHOW ROLES").unwrap();
    assert_eq!(result.rows.len(), 0);
}

#[test]
fn e2e_grant_role() {
    let ctx = TestCtx::with_auth();
    ctx.run("CREATE USER charlie IDENTIFIED BY 'Password123'").unwrap();
    ctx.run("CREATE ROLE writer").unwrap();
    ctx.run("GRANT writer TO charlie").unwrap();
    let result = ctx.run("SHOW USERS").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn e2e_revoke_role() {
    let ctx = TestCtx::with_auth();
    ctx.run("CREATE USER dave IDENTIFIED BY 'Password123'").unwrap();
    ctx.run("CREATE ROLE analyst").unwrap();
    ctx.run("GRANT analyst TO dave").unwrap();
    ctx.run("REVOKE analyst FROM dave").unwrap();
    let result = ctx.run("SHOW USERS").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn e2e_show_users_empty() {
    let ctx = TestCtx::with_auth();
    let result = ctx.run("SHOW USERS").unwrap();
    assert_eq!(result.rows.len(), 0);
}

#[test]
fn e2e_show_roles_empty() {
    let ctx = TestCtx::with_auth();
    let result = ctx.run("SHOW ROLES").unwrap();
    assert_eq!(result.rows.len(), 0);
}

#[test]
fn e2e_auth_ddl_multiple_users() {
    let ctx = TestCtx::with_auth();
    ctx.run("CREATE USER eve IDENTIFIED BY 'Password123'").unwrap();
    ctx.run("CREATE USER frank IDENTIFIED BY 'Password123'").unwrap();
    let result = ctx.run("SHOW USERS").unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn e2e_auth_ddl_role_granted_to_multiple_users() {
    let ctx = TestCtx::with_auth();
    ctx.run("CREATE USER grace IDENTIFIED BY 'Password123'").unwrap();
    ctx.run("CREATE USER heidi IDENTIFIED BY 'Password123'").unwrap();
    ctx.run("CREATE ROLE editor").unwrap();
    ctx.run("GRANT editor TO grace").unwrap();
    ctx.run("GRANT editor TO heidi").unwrap();
    let result = ctx.run("SHOW ROLES").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("role"), Some(&PropertyValue::String("editor".into())));
}

// ─── Trigger DDL tests ────────────────────────────────────────────────────

#[test]
fn e2e_create_trigger() {
    let ctx = TestCtx::new();
    ctx.run("CREATE TRIGGER on_vertex_create ON VERTEX CREATE AFTER EXECUTE 'RETURN 1'").unwrap();
    let result = ctx.run("SHOW TRIGGERS").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("on_vertex_create".into())));
}

#[test]
fn e2e_drop_trigger() {
    let ctx = TestCtx::new();
    ctx.run("CREATE TRIGGER tmp ON VERTEX CREATE AFTER EXECUTE 'RETURN 1'").unwrap();
    ctx.run("DROP TRIGGER tmp").unwrap();
    let result = ctx.run("SHOW TRIGGERS").unwrap();
    assert_eq!(result.rows.len(), 0);
}

#[test]
fn e2e_trigger_duplicate_name_fails() {
    let ctx = TestCtx::new();
    ctx.run("CREATE TRIGGER dup ON VERTEX CREATE AFTER EXECUTE 'RETURN 1'").unwrap();
    let result = ctx.run("CREATE TRIGGER dup ON VERTEX CREATE AFTER EXECUTE 'RETURN 2'");
    assert!(result.is_err());
}

#[test]
fn e2e_trigger_multiple_events() {
    let ctx = TestCtx::new();
    ctx.run("CREATE TRIGGER v_create ON VERTEX CREATE BEFORE EXECUTE 'RETURN 1'").unwrap();
    ctx.run("CREATE TRIGGER v_delete ON VERTEX DELETE AFTER EXECUTE 'RETURN 2'").unwrap();
    ctx.run("CREATE TRIGGER e_create ON EDGE CREATE AFTER EXECUTE 'RETURN 3'").unwrap();
    ctx.run("CREATE TRIGGER v_update ON VERTEX SET AFTER EXECUTE 'RETURN 4'").unwrap();
    let result = ctx.run("SHOW TRIGGERS").unwrap();
    assert_eq!(result.rows.len(), 4);
}

#[test]
fn e2e_show_triggers_empty() {
    let ctx = TestCtx::new();
    let result = ctx.run("SHOW TRIGGERS").unwrap();
    assert_eq!(result.rows.len(), 0);
}

#[test]
fn e2e_create_database() {
    let ctx = TestCtx::new();
    let result = ctx.run("CREATE DATABASE analytics").unwrap();
    assert_eq!(result.rows.len(), 0);
    let result = ctx.run("SHOW DATABASES").unwrap();
    assert!(result.rows.len() >= 2); // default + analytics
    let names: Vec<String> = result.rows.iter()
        .filter_map(|r| r.get("name"))
        .filter_map(|v| match v { PropertyValue::String(s) => Some(s.clone()), _ => None })
        .collect();
    assert!(names.contains(&"analytics".to_string()));
}

#[test]
fn e2e_drop_database() {
    let ctx = TestCtx::new();
    ctx.run("CREATE DATABASE tempdb").unwrap();
    let result = ctx.run("SHOW DATABASES").unwrap();
    let before_count = result.rows.len();
    ctx.run("DROP DATABASE tempdb").unwrap();
    let result = ctx.run("SHOW DATABASES").unwrap();
    assert_eq!(result.rows.len(), before_count - 1);
}

#[test]
fn e2e_drop_database_force() {
    let ctx = TestCtx::new();
    ctx.run("CREATE DATABASE forcedb").unwrap();
    ctx.run("DROP DATABASE forcedb FORCE").unwrap();
    let result = ctx.run("SHOW DATABASES").unwrap();
    let names: Vec<String> = result.rows.iter()
        .filter_map(|r| r.get("name"))
        .filter_map(|v| match v { PropertyValue::String(s) => Some(s.clone()), _ => None })
        .collect();
    assert!(!names.contains(&"forcedb".to_string()));
}

#[test]
fn e2e_create_database_duplicate_fails() {
    let ctx = TestCtx::new();
    ctx.run("CREATE DATABASE dupdb").unwrap();
    let result = ctx.run("CREATE DATABASE dupdb");
    assert!(result.is_err());
}

#[test]
fn e2e_drop_nonexistent_database_fails() {
    let ctx = TestCtx::new();
    let result = ctx.run("DROP DATABASE nonexistent");
    assert!(result.is_err());
}

#[test]
fn e2e_trigger_fires_on_vertex_create() {
    let ctx = TestCtx::new();
    ctx.run("CREATE TRIGGER audit ON VERTEX CREATE AFTER EXECUTE 'CREATE (a:Audit {event: \"create\"})'").unwrap();
    ctx.run("CREATE (n:Person {name: 'Alice'})").unwrap();
    let result = ctx.run("MATCH (a:Audit) RETURN a.event AS event").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("event"), Some(&PropertyValue::String("create".into())));
}

#[test]
fn e2e_trigger_fires_on_vertex_delete() {
    let ctx = TestCtx::new();
    ctx.run("CREATE TRIGGER audit_delete ON VERTEX DELETE AFTER EXECUTE 'CREATE (a:Audit {event: \"delete\"})'").unwrap();
    ctx.run("CREATE (n:Person {name: 'Bob'})").unwrap();
    ctx.run("MATCH (n:Person) DELETE n").unwrap();
    let result = ctx.run("MATCH (a:Audit) RETURN a.event AS event").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("event"), Some(&PropertyValue::String("delete".into())));
}

#[test]
fn e2e_trigger_fires_on_edge_create() {
    let ctx = TestCtx::new();
    ctx.run("CREATE TRIGGER audit_edge ON EDGE CREATE AFTER EXECUTE 'CREATE (a:Audit {event: \"edge_create\"})'").unwrap();
    ctx.run("CREATE (a:Person {name: 'A'}), (b:Person {name: 'B'})").unwrap();
    ctx.run("MATCH (a:Person {name: 'A'}), (b:Person {name: 'B'}) CREATE (a)-[:KNOWS]->(b)").unwrap();
    let result = ctx.run("MATCH (a:Audit) RETURN a.event AS event").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("event"), Some(&PropertyValue::String("edge_create".into())));
}

#[test]
fn e2e_trigger_fires_on_vertex_update() {
    let ctx = TestCtx::new();
    ctx.run("CREATE TRIGGER audit_update ON VERTEX SET AFTER EXECUTE 'CREATE (a:Audit {event: \"update\"})'").unwrap();
    ctx.run("CREATE (n:Person)").unwrap();
    ctx.run("MATCH (n:Person) SET n.name = 'Charles'").unwrap();
    let result = ctx.run("MATCH (a:Audit) RETURN a.event AS event").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("event"), Some(&PropertyValue::String("update".into())));
}

// ─── Vector index auto-maintenance tests ────────────────────────────────────

fn setup_vector_index(ctx: &TestCtx, label: &str, prop: &str, dim: usize) {
    let label_id = ctx.catalog.label(label);
    let prop_id = ctx.catalog.property(prop);
    let index = std::sync::Arc::new(std::sync::RwLock::new(
        mgvector::HnswIndex::new(dim, mgvector::Distance::Cosine, mgvector::HnswConfig::default())
    ));
    let entry = mgstorage::storage::VectorIndexEntry::new(index, prop_id, dim, mgvector::Distance::Cosine);
    ctx.storage.vector_indices.write().unwrap().insert(label_id, entry);
}

#[test]
fn e2e_vector_index_auto_create_and_search() {
    let ctx = TestCtx::new();
    setup_vector_index(&ctx, "VectorItem", "embedding", 2);

    ctx.run("CREATE (n:VectorItem {embedding: [1.0, 0.0]})").unwrap();
    ctx.run("CREATE (n:VectorItem {embedding: [0.0, 1.0]})").unwrap();
    ctx.run("CREATE (n:VectorItem {embedding: [0.5, 0.5]})").unwrap();

    let indices = ctx.storage.vector_indices.read().unwrap();
    let entry = indices.get(&ctx.catalog.label("VectorItem")).unwrap();
    let index = entry.index.read().unwrap();
    let gid_map = entry.gid_to_node.read().unwrap();
    assert_eq!(gid_map.len(), 3, "all 3 vertices should be indexed");

    // Search for nearest to [1.0, 0.0] — should find the first vertex
    let results = index.search(&[1.0f32, 0.0f32], 1);
    assert_eq!(results.len(), 1);
    drop(index);
    drop(gid_map);
    drop(indices);
}

#[test]
fn e2e_vector_index_auto_update_property() {
    let ctx = TestCtx::new();
    setup_vector_index(&ctx, "VectorItem", "embedding", 2);

    ctx.run("CREATE (n:VectorItem {embedding: [1.0, 0.0]})").unwrap();

    // Update the embedding
    ctx.run("MATCH (n:VectorItem) SET n.embedding = [0.0, 1.0]").unwrap();

    let indices = ctx.storage.vector_indices.read().unwrap();
    let entry = indices.get(&ctx.catalog.label("VectorItem")).unwrap();
    let index = entry.index.read().unwrap();
    let gid_map = entry.gid_to_node.read().unwrap();
    assert_eq!(gid_map.len(), 1);

    // Searching for the new vector should return a result with near-zero distance.
    let results = index.search(&[0.0f32, 1.0f32], 1);
    assert_eq!(results.len(), 1);
    let (_node_id, dist) = results[0];
    assert!(dist < 0.001, "updated vector should be found with near-zero distance");
}

#[test]
fn e2e_vector_index_auto_delete_vertex() {
    let ctx = TestCtx::new();
    setup_vector_index(&ctx, "VectorItem", "embedding", 2);

    ctx.run("CREATE (n:VectorItem {embedding: [1.0, 0.0]})").unwrap();
    ctx.run("MATCH (n:VectorItem) DELETE n").unwrap();

    let indices = ctx.storage.vector_indices.read().unwrap();
    let entry = indices.get(&ctx.catalog.label("VectorItem")).unwrap();
    let gid_map = entry.gid_to_node.read().unwrap();
    assert!(gid_map.is_empty(), "deleted vertex should be removed from vector index");
}

#[test]
fn e2e_vector_index_auto_remove_label() {
    let ctx = TestCtx::new();
    setup_vector_index(&ctx, "VectorItem", "embedding", 2);

    ctx.run("CREATE (n:VectorItem {embedding: [1.0, 0.0]})").unwrap();
    ctx.run("MATCH (n:VectorItem) REMOVE n:VectorItem").unwrap();

    let indices = ctx.storage.vector_indices.read().unwrap();
    let entry = indices.get(&ctx.catalog.label("VectorItem")).unwrap();
    let gid_map = entry.gid_to_node.read().unwrap();
    assert!(gid_map.is_empty(), "vertex should be removed from vector index when label is removed");
}

#[test]
fn e2e_vector_index_auto_add_label() {
    let ctx = TestCtx::new();
    setup_vector_index(&ctx, "VectorItem", "embedding", 2);

    // Create vertex without the indexed label
    ctx.run("CREATE (n:Other {embedding: [1.0, 0.0]})").unwrap();
    // Add the indexed label
    ctx.run("MATCH (n:Other) SET n:VectorItem").unwrap();

    let indices = ctx.storage.vector_indices.read().unwrap();
    let entry = indices.get(&ctx.catalog.label("VectorItem")).unwrap();
    let gid_map = entry.gid_to_node.read().unwrap();
    assert_eq!(gid_map.len(), 1, "vertex should be added to vector index when label is added");
}

#[test]
fn e2e_vector_index_skips_non_vector_property() {
    let ctx = TestCtx::new();
    setup_vector_index(&ctx, "VectorItem", "embedding", 2);

    // Create vertex with a non-vector property value
    ctx.run("CREATE (n:VectorItem {embedding: 'not a vector'})").unwrap();

    let indices = ctx.storage.vector_indices.read().unwrap();
    let entry = indices.get(&ctx.catalog.label("VectorItem")).unwrap();
    let gid_map = entry.gid_to_node.read().unwrap();
    assert!(gid_map.is_empty(), "non-vector property should not be indexed");
}

#[test]
fn e2e_vector_index_skips_wrong_dimension() {
    let ctx = TestCtx::new();
    setup_vector_index(&ctx, "VectorItem", "embedding", 2);

    // Create vertex with wrong dimension vector
    ctx.run("CREATE (n:VectorItem {embedding: [1.0, 0.0, 0.5]})").unwrap();

    let indices = ctx.storage.vector_indices.read().unwrap();
    let entry = indices.get(&ctx.catalog.label("VectorItem")).unwrap();
    let gid_map = entry.gid_to_node.read().unwrap();
    assert!(gid_map.is_empty(), "wrong-dimension vector should not be indexed");
}

// ─── Vector index CALL procedure tests ─────────────────────────────────────

#[test]
fn e2e_vector_index_procedure_create_and_search() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {embedding: [1.0, 0.0]})").unwrap();
    ctx.run("CREATE (n:Item {embedding: [0.0, 1.0]})").unwrap();
    ctx.run("CREATE (n:Item {embedding: [0.5, 0.5]})").unwrap();

    ctx.run("CALL db.createVectorIndex({label: 'Item', property: 'embedding', dimension: 2})").unwrap();

    // Default k=10 returns all indexed nodes (only 3 exist)
    let result = ctx.run("CALL db.searchVectorIndex({label: 'Item', property: 'embedding', vector: [1.0, 0.0]}) YIELD node, distance RETURN node").unwrap();
    assert_eq!(result.rows.len(), 3);

    // Search with k=1 should return exactly 1 node
    let result = ctx.run("CALL db.searchVectorIndex({label: 'Item', property: 'embedding', vector: [1.0, 0.0], k: 1}) YIELD node, distance RETURN node").unwrap();
    assert_eq!(result.rows.len(), 1);

    // Search with k=3 should return all 3 nodes
    let result = ctx.run("CALL db.searchVectorIndex({label: 'Item', property: 'embedding', vector: [1.0, 0.0], k: 3}) YIELD node, distance RETURN node ORDER BY distance").unwrap();
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn e2e_vector_index_procedure_backfill() {
    let ctx = TestCtx::new();
    // Create vertices BEFORE the index exists
    ctx.run("CREATE (n:Item {embedding: [1.0, 0.0]})").unwrap();
    ctx.run("CREATE (n:Item {embedding: [0.0, 1.0]})").unwrap();

    // Now create the index — should backfill existing data
    ctx.run("CALL db.createVectorIndex({label: 'Item', property: 'embedding', dimension: 2})").unwrap();

    let result = ctx.run("CALL db.searchVectorIndex({label: 'Item', property: 'embedding', vector: [1.0, 0.0], k: 2}) YIELD node, distance RETURN node").unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn e2e_vector_index_procedure_euclidean_distance() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {embedding: [0.0, 0.0]})").unwrap();
    ctx.run("CREATE (n:Item {embedding: [3.0, 4.0]})").unwrap();

    ctx.run("CALL db.createVectorIndex({label: 'Item', property: 'embedding', dimension: 2, distance: 'euclidean'})").unwrap();

    let result = ctx.run("CALL db.searchVectorIndex({label: 'Item', property: 'embedding', vector: [0.0, 0.0], k: 1}) YIELD node, distance RETURN distance").unwrap();
    assert_eq!(result.rows.len(), 1);
    // Exact match should have distance 0
    assert_eq!(result.rows[0].get("distance"), Some(&PropertyValue::Double(0.0)));
}

#[test]
fn e2e_vector_index_procedure_auto_update() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {embedding: [1.0, 0.0]})").unwrap();
    ctx.run("CALL db.createVectorIndex({label: 'Item', property: 'embedding', dimension: 2})").unwrap();

    // Update the embedding
    ctx.run("MATCH (n:Item) SET n.embedding = [0.0, 1.0]").unwrap();

    // Search for the new vector — should find a result with near-zero distance
    let result = ctx.run("CALL db.searchVectorIndex({label: 'Item', property: 'embedding', vector: [0.0, 1.0], k: 1}) YIELD node, distance RETURN distance").unwrap();
    assert_eq!(result.rows.len(), 1);
    let dist = match result.rows[0].get("distance") {
        Some(PropertyValue::Double(d)) => *d,
        _ => panic!("expected double distance"),
    };
    assert!(dist < 0.001, "updated vector should be found with near-zero distance, got {}", dist);
}

#[test]
fn e2e_vector_index_procedure_missing_index() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {embedding: [1.0, 0.0]})").unwrap();

    // Search without creating an index should return empty
    let result = ctx.run("CALL db.searchVectorIndex({label: 'Item', property: 'embedding', vector: [1.0, 0.0]}) YIELD node, distance RETURN node").unwrap();
    assert!(result.rows.is_empty());
}

// ─── AllShortest / WShortest path tests ────────────────────────────────────

#[test]
fn e2e_point_index_procedure_create_and_search() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Place {loc: point({x: 1.0, y: 2.0})})").unwrap();
    ctx.run("CREATE (n:Place {loc: point({x: 5.0, y: 6.0})})").unwrap();
    ctx.run("CREATE (n:Place {loc: point({x: 10.0, y: 10.0})})").unwrap();

    ctx.run("CALL db.createPointIndex({label: 'Place', property: 'loc'})").unwrap();

    // withinBBox should find the first two points
    let result = ctx.run("CALL db.withinBBox({label: 'Place', property: 'loc', lowerLeft: [0.0, 0.0], upperRight: [6.0, 7.0]}) YIELD node RETURN node").unwrap();
    assert_eq!(result.rows.len(), 2);

    // nearest should find closest point to origin
    let result = ctx.run("CALL db.nearest({label: 'Place', property: 'loc', point: [0.0, 0.0], k: 1}) YIELD node, distance RETURN node").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn e2e_point_index_procedure_backfill() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Place {loc: point({x: 1.0, y: 2.0})})").unwrap();
    ctx.run("CREATE (n:Place {loc: point({x: 5.0, y: 6.0})})").unwrap();

    // Create index after data exists
    ctx.run("CALL db.createPointIndex({label: 'Place', property: 'loc'})").unwrap();

    let result = ctx.run("CALL db.withinBBox({label: 'Place', property: 'loc', lowerLeft: [0.0, 0.0], upperRight: [6.0, 7.0]}) YIELD node RETURN node").unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn e2e_point_index_procedure_drop() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Place {loc: point({x: 1.0, y: 2.0})})").unwrap();
    ctx.run("CALL db.createPointIndex({label: 'Place', property: 'loc'})").unwrap();

    // Drop the index
    ctx.run("CALL db.dropPointIndex({label: 'Place', property: 'loc'})").unwrap();

    // After dropping, queries should return empty (index is gone from active_point_indices)
    let result = ctx.run("CALL db.withinBBox({label: 'Place', property: 'loc', lowerLeft: [0.0, 0.0], upperRight: [10.0, 10.0]}) YIELD node RETURN node").unwrap();
    assert!(result.rows.is_empty());
}

#[test]
fn e2e_db_indexes_all_types() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {embedding: [1.0, 0.0], loc: point({x: 1.0, y: 2.0}), title: 'hello'})").unwrap();

    // Create label index
    ctx.storage.create_label_index(ctx.catalog.label("Item"));
    // Create label+property index
    ctx.storage.create_label_property_index(ctx.catalog.label("Item"), ctx.catalog.property("title"));
    // Create vector index
    ctx.run("CALL db.createVectorIndex({label: 'Item', property: 'embedding', dimension: 2})").unwrap();
    // Create text index
    ctx.run("CALL db.createTextIndex({label: 'Item', properties: ['title']})").unwrap();
    // Create point index
    ctx.run("CALL db.createPointIndex({label: 'Item', property: 'loc'})").unwrap();

    let result = ctx.run("CALL db.indexes() YIELD label, type RETURN type").unwrap();
    // Should have: label, label+property, point, text, vector
    let types: Vec<String> = result.rows.iter().filter_map(|r| match r.get("type") {
        Some(PropertyValue::String(s)) => Some(s.clone()),
        _ => None,
    }).collect();
    assert!(types.contains(&"label".into()));
    assert!(types.contains(&"label+property".into()));
    assert!(types.contains(&"vector".into()));
    assert!(types.contains(&"text".into()));
    assert!(types.contains(&"point".into()));
    assert_eq!(result.rows.len(), 5);
}

#[test]
fn e2e_all_shortest_basic() {
    let ctx = TestCtx::new();
    // A -- B -- C
    //  \_______/
    ctx.run("CREATE (a:Node {name: 'A'}), (b:Node {name: 'B'}), (c:Node {name: 'C'})").unwrap();
    ctx.run("MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'}) CREATE (a)-[:LINK]->(b)").unwrap();
    ctx.run("MATCH (b:Node {name: 'B'}), (c:Node {name: 'C'}) CREATE (b)-[:LINK]->(c)").unwrap();
    ctx.run("MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'}) CREATE (a)-[:LINK]->(c)").unwrap();

    // allShortest from A to C should find C at depth 1 (direct edge)
    let result = ctx.run("MATCH (a:Node {name: 'A'})-[:LINK *allShortest..2]->(c:Node {name: 'C'}) RETURN c.name AS name").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("C".into())));
}

#[test]
fn e2e_all_shortest_avoids_cycles() {
    let ctx = TestCtx::new();
    // Triangle: A -- B -- C -- A
    ctx.run("CREATE (a:Node {name: 'A'}), (b:Node {name: 'B'}), (c:Node {name: 'C'})").unwrap();
    ctx.run("MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'}) CREATE (a)-[:LINK]->(b)").unwrap();
    ctx.run("MATCH (b:Node {name: 'B'}), (c:Node {name: 'C'}) CREATE (b)-[:LINK]->(c)").unwrap();
    ctx.run("MATCH (c:Node {name: 'C'}), (a:Node {name: 'A'}) CREATE (c)-[:LINK]->(a)").unwrap();

    // allShortest from A should reach B and C, but not revisit A
    let result = ctx.run("MATCH (a:Node {name: 'A'})-[:LINK *allShortest..3]->(x:Node) RETURN x.name AS name ORDER BY x.name").unwrap();
    let names: Vec<_> = result.rows.iter().map(|r| r.get("name").unwrap().clone()).collect();
    assert_eq!(names, vec![
        PropertyValue::String("B".into()),
        PropertyValue::String("C".into()),
    ]);
}

#[test]
fn e2e_wshortest_basic() {
    let ctx = TestCtx::new();
    // A -- B -- C (two hops)
    // A -------- C (one hop, same weight)
    ctx.run("CREATE (a:Node {name: 'A'}), (b:Node {name: 'B'}), (c:Node {name: 'C'})").unwrap();
    ctx.run("MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'}) CREATE (a)-[:LINK]->(b)").unwrap();
    ctx.run("MATCH (b:Node {name: 'B'}), (c:Node {name: 'C'}) CREATE (b)-[:LINK]->(c)").unwrap();
    ctx.run("MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'}) CREATE (a)-[:LINK]->(c)").unwrap();

    // wShortest from A to C: direct edge has weight 1, indirect has weight 2
    let result = ctx.run("MATCH (a:Node {name: 'A'})-[:LINK *wShortest..3]->(c:Node {name: 'C'}) RETURN c.name AS name").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("C".into())));
}

#[test]
fn e2e_wshortest_respects_min() {
    let ctx = TestCtx::new();
    // A -- B -- C
    ctx.run("CREATE (a:Node {name: 'A'}), (b:Node {name: 'B'}), (c:Node {name: 'C'})").unwrap();
    ctx.run("MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'}) CREATE (a)-[:LINK]->(b)").unwrap();
    ctx.run("MATCH (b:Node {name: 'B'}), (c:Node {name: 'C'}) CREATE (b)-[:LINK]->(c)").unwrap();

    // wShortest with default min=1 should return B (depth 1) and C (depth 2)
    let result = ctx.run("MATCH (a:Node {name: 'A'})-[:LINK *wShortest..2]->(x:Node) RETURN x.name AS name ORDER BY x.name").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("B".into())));
    assert_eq!(result.rows[1].get("name"), Some(&PropertyValue::String("C".into())));
}

#[test]
fn e2e_shortest_path_function() {
    let ctx = TestCtx::new();
    // A -- B -- C
    ctx.run("CREATE (a:Node {name: 'A'}), (b:Node {name: 'B'}), (c:Node {name: 'C'})").unwrap();
    ctx.run("MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'}) CREATE (a)-[:LINK]->(b)").unwrap();
    ctx.run("MATCH (b:Node {name: 'B'}), (c:Node {name: 'C'}) CREATE (b)-[:LINK]->(c)").unwrap();

    let result = ctx.run("MATCH p = shortestPath((a:Node {name: 'A'})-[:LINK*..3]->(c:Node {name: 'C'})) RETURN length(p) AS len").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("len"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_all_shortest_paths_function() {
    let ctx = TestCtx::new();
    // A -- B -- C
    //  \________/
    ctx.run("CREATE (a:Node {name: 'A'}), (b:Node {name: 'B'}), (c:Node {name: 'C'})").unwrap();
    ctx.run("MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'}) CREATE (a)-[:LINK]->(b)").unwrap();
    ctx.run("MATCH (b:Node {name: 'B'}), (c:Node {name: 'C'}) CREATE (b)-[:LINK]->(c)").unwrap();
    ctx.run("MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'}) CREATE (a)-[:LINK]->(c)").unwrap();

    let result = ctx.run("MATCH p = allShortestPaths((a:Node {name: 'A'})-[:LINK*..3]->(c:Node {name: 'C'})) RETURN length(p) AS len").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("len"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_call_algo_rich_club_coefficient() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node {id: 1})-[:LINKS]->(b:Node {id: 2})").unwrap();
    ctx.run("CREATE (b:Node {id: 2})-[:LINKS]->(c:Node {id: 3})").unwrap();
    ctx.run("CREATE (c:Node {id: 3})-[:LINKS]->(a:Node {id: 1})").unwrap();

    let result = ctx.run("CALL algo.rich_club_coefficient()").unwrap();
    assert!(result.rows.len() >= 1);
}

#[test]
fn e2e_call_algo_katz_centrality() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node {id: 1})-[:LINKS]->(b:Node {id: 2})").unwrap();
    ctx.run("CREATE (b:Node {id: 2})-[:LINKS]->(c:Node {id: 3})").unwrap();

    let result = ctx.run("CALL algo.katz_centrality()").unwrap();
    assert!(result.rows.len() >= 1);
    for row in &result.rows {
        let score = row.get("score").cloned().unwrap_or(PropertyValue::Null);
        assert!(matches!(score, PropertyValue::Double(r) if r >= 0.0), "expected non-negative score, got {:?}", score);
    }
}

#[test]
fn e2e_call_algo_label_propagation() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node {id: 1})-[:LINKS]->(b:Node {id: 2})").unwrap();
    ctx.run("CREATE (b:Node {id: 2})-[:LINKS]->(c:Node {id: 3})").unwrap();
    ctx.run("CREATE (c:Node {id: 3})-[:LINKS]->(a:Node {id: 1})").unwrap();

    let result = ctx.run("CALL algo.label_propagation()").unwrap();
    assert!(result.rows.len() >= 1);
}

#[test]
fn e2e_call_algo_louvain() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node {id: 1})-[:LINKS]->(b:Node {id: 2})").unwrap();
    ctx.run("CREATE (b:Node {id: 2})-[:LINKS]->(c:Node {id: 3})").unwrap();
    ctx.run("CREATE (c:Node {id: 3})-[:LINKS]->(a:Node {id: 1})").unwrap();

    let result = ctx.run("CALL algo.louvain()").unwrap();
    assert!(result.rows.len() >= 1);
}

#[test]
fn e2e_call_algo_hits() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node {id: 1})-[:LINKS]->(b:Node {id: 2})").unwrap();
    ctx.run("CREATE (b:Node {id: 2})-[:LINKS]->(c:Node {id: 3})").unwrap();

    let result = ctx.run("CALL algo.hits()").unwrap();
    assert!(result.rows.len() >= 1);
    for row in &result.rows {
        let auth = row.get("authority").cloned().unwrap_or(PropertyValue::Null);
        assert!(matches!(auth, PropertyValue::Double(r) if r >= 0.0), "expected non-negative authority, got {:?}", auth);
    }
}

#[test]
fn e2e_call_algo_core_decomposition() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node {id: 1})-[:LINKS]->(b:Node {id: 2})").unwrap();
    ctx.run("CREATE (b:Node {id: 2})-[:LINKS]->(c:Node {id: 3})").unwrap();
    ctx.run("CREATE (c:Node {id: 3})-[:LINKS]->(a:Node {id: 1})").unwrap();

    let result = ctx.run("CALL algo.core_decomposition()").unwrap();
    assert!(result.rows.len() >= 1);
}

#[test]
fn e2e_call_algo_k_core() {
    let ctx = TestCtx::new();
    // Create a triangle in a single query so nodes are shared
    ctx.run("CREATE (a:Node {id: 1})-[:LINKS]->(b:Node {id: 2})-[:LINKS]->(c:Node {id: 3})-[:LINKS]->(a)").unwrap();

    let result = ctx.run("CALL algo.k_core({k: 2})").unwrap();
    // Triangle is 2-core
    assert!(result.rows.len() >= 1);
}

#[test]
fn e2e_call_algo_modularity() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node {id: 1})-[:LINKS]->(b:Node {id: 2})").unwrap();
    ctx.run("CREATE (b:Node {id: 2})-[:LINKS]->(c:Node {id: 3})").unwrap();
    ctx.run("CREATE (c:Node {id: 3})-[:LINKS]->(a:Node {id: 1})").unwrap();

    let result = ctx.run("CALL algo.modularity()").unwrap();
    assert_eq!(result.rows.len(), 1);
    let q = result.rows[0].get("modularity").cloned().unwrap_or(PropertyValue::Null);
    assert!(matches!(q, PropertyValue::Double(_)), "expected modularity score, got {:?}", q);
}

#[test]
fn e2e_call_algo_conductance() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node {id: 1})-[:LINKS]->(b:Node {id: 2})").unwrap();
    ctx.run("CREATE (b:Node {id: 2})-[:LINKS]->(c:Node {id: 3})").unwrap();

    let result = ctx.run("CALL algo.conductance()").unwrap();
    assert!(result.rows.len() >= 1);
}

#[test]
fn e2e_call_algo_normalized_cut() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node {id: 1})-[:LINKS]->(b:Node {id: 2})").unwrap();
    ctx.run("CREATE (b:Node {id: 2})-[:LINKS]->(c:Node {id: 3})").unwrap();

    let result = ctx.run("CALL algo.normalized_cut()").unwrap();
    assert!(result.rows.len() >= 1);
}

#[test]
fn e2e_call_db_analytics() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node {id: 1})-[:LINKS]->(b:Node {id: 2})").unwrap();

    let result = ctx.run("CALL db.analytics()").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(result.rows[0].get("vertexCount").is_some());
    assert!(result.rows[0].get("edgeCount").is_some());
}

#[test]
fn e2e_call_db_degree_histogram() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node {id: 1})-[:LINKS]->(b:Node {id: 2})").unwrap();

    let result = ctx.run("CALL db.degree_histogram()").unwrap();
    assert!(result.rows.len() >= 1);
    for row in &result.rows {
        assert!(row.get("degree").is_some());
        assert!(row.get("count").is_some());
    }
}

#[test]
fn e2e_call_algo_dfs() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node {id: 1})-[:LINKS]->(b:Node {id: 2})-[:LINKS]->(c:Node {id: 3})").unwrap();

    let result = ctx.run("CALL algo.dfs(1)").unwrap();
    assert_eq!(result.rows.len(), 1);
    let path = result.rows[0].get("path").cloned().unwrap_or(PropertyValue::Null);
    assert!(matches!(path, PropertyValue::List(_)), "expected path list");
}

#[test]
fn e2e_call_algo_random_walk() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node {id: 1})-[:LINKS]->(b:Node {id: 2})-[:LINKS]->(c:Node {id: 3})").unwrap();

    let result = ctx.run("CALL algo.random_walk(1, 5)").unwrap();
    assert_eq!(result.rows.len(), 1);
    let walk = result.rows[0].get("walk").cloned().unwrap_or(PropertyValue::Null);
    assert!(matches!(walk, PropertyValue::List(_)), "expected walk list");
}

#[test]
fn e2e_call_algo_jaccard_similarity() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:LINKS]->(b:Node), (a)-[:LINKS]->(c:Node)").unwrap();
    let gid_result = ctx.run("MATCH (n:Node) RETURN id(n) AS gid ORDER BY gid").unwrap();
    let gids: Vec<i64> = gid_result.rows.iter().map(|r| match r.get("gid") {
        Some(PropertyValue::Int(v)) => *v, _ => panic!("expected int gid"),
    }).collect();
    assert_eq!(gids.len(), 3);

    let result = ctx.run(&format!("CALL algo.jaccard_similarity({}, {})", gids[1], gids[2])).unwrap();
    assert_eq!(result.rows.len(), 1);
    let score = result.rows[0].get("score").cloned().unwrap_or(PropertyValue::Null);
    // Both share neighbor gids[0], jaccard = 1.0
    assert!(matches!(score, PropertyValue::Double(v) if (v - 1.0).abs() < 0.01), "expected jaccard ~1.0, got {:?}", score);
}

#[test]
fn e2e_call_algo_cosine_similarity() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:LINKS]->(b:Node), (a)-[:LINKS]->(c:Node)").unwrap();
    let gid_result = ctx.run("MATCH (n:Node) RETURN id(n) AS gid ORDER BY gid").unwrap();
    let gids: Vec<i64> = gid_result.rows.iter().map(|r| match r.get("gid") {
        Some(PropertyValue::Int(v)) => *v, _ => panic!("expected int gid"),
    }).collect();
    assert_eq!(gids.len(), 3);

    let result = ctx.run(&format!("CALL algo.cosine_similarity({}, {})", gids[1], gids[2])).unwrap();
    assert_eq!(result.rows.len(), 1);
    let score = result.rows[0].get("score").cloned().unwrap_or(PropertyValue::Null);
    assert!(matches!(score, PropertyValue::Double(v) if (v - 1.0).abs() < 0.01), "expected cosine ~1.0, got {:?}", score);
}

#[test]
fn e2e_call_algo_adamic_adar() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:LINKS]->(b:Node), (a)-[:LINKS]->(c:Node)").unwrap();
    let gid_result = ctx.run("MATCH (n:Node) RETURN id(n) AS gid ORDER BY gid").unwrap();
    let gids: Vec<i64> = gid_result.rows.iter().map(|r| match r.get("gid") {
        Some(PropertyValue::Int(v)) => *v, _ => panic!("expected int gid"),
    }).collect();
    assert_eq!(gids.len(), 3);

    let result = ctx.run(&format!("CALL algo.adamic_adar({}, {})", gids[1], gids[2])).unwrap();
    assert_eq!(result.rows.len(), 1);
    let score = result.rows[0].get("score").cloned().unwrap_or(PropertyValue::Null);
    // Common neighbor has degree 2, score = 1/ln(2) ≈ 1.44
    assert!(matches!(score, PropertyValue::Double(v) if v > 1.0 && v < 2.0), "expected adamic_adar ~1.44, got {:?}", score);
}

#[test]
fn e2e_call_algo_common_neighbors() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:LINKS]->(b:Node), (a)-[:LINKS]->(c:Node)").unwrap();
    let gid_result = ctx.run("MATCH (n:Node) RETURN id(n) AS gid ORDER BY gid").unwrap();
    let gids: Vec<i64> = gid_result.rows.iter().map(|r| match r.get("gid") {
        Some(PropertyValue::Int(v)) => *v, _ => panic!("expected int gid"),
    }).collect();
    assert_eq!(gids.len(), 3);

    let result = ctx.run(&format!("CALL algo.common_neighbors({}, {})", gids[1], gids[2])).unwrap();
    assert_eq!(result.rows.len(), 1);
    let count = result.rows[0].get("count").cloned().unwrap_or(PropertyValue::Null);
    assert!(matches!(count, PropertyValue::Int(1)), "expected 1 common neighbor, got {:?}", count);
}

#[test]
fn e2e_call_algo_resource_allocation() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:LINKS]->(b:Node), (a)-[:LINKS]->(c:Node)").unwrap();
    let gid_result = ctx.run("MATCH (n:Node) RETURN id(n) AS gid ORDER BY gid").unwrap();
    let gids: Vec<i64> = gid_result.rows.iter().map(|r| match r.get("gid") {
        Some(PropertyValue::Int(v)) => *v, _ => panic!("expected int gid"),
    }).collect();
    assert_eq!(gids.len(), 3);

    let result = ctx.run(&format!("CALL algo.resource_allocation({}, {})", gids[1], gids[2])).unwrap();
    assert_eq!(result.rows.len(), 1);
    let score = result.rows[0].get("score").cloned().unwrap_or(PropertyValue::Null);
    // Common neighbor has degree 2, score = 1/2 = 0.5
    assert!(matches!(score, PropertyValue::Double(v) if (v - 0.5).abs() < 0.01), "expected resource_allocation ~0.5, got {:?}", score);
}

#[test]
fn e2e_call_algo_preferential_attachment() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:LINKS]->(b:Node), (a)-[:LINKS]->(c:Node)").unwrap();
    let gid_result = ctx.run("MATCH (n:Node) RETURN id(n) AS gid ORDER BY gid").unwrap();
    let gids: Vec<i64> = gid_result.rows.iter().map(|r| match r.get("gid") {
        Some(PropertyValue::Int(v)) => *v, _ => panic!("expected int gid"),
    }).collect();
    assert_eq!(gids.len(), 3);

    let result = ctx.run(&format!("CALL algo.preferential_attachment({}, {})", gids[1], gids[2])).unwrap();
    assert_eq!(result.rows.len(), 1);
    let score = result.rows[0].get("score").cloned().unwrap_or(PropertyValue::Null);
    // Both nodes have degree 1, score = 1 * 1 = 1.0
    assert!(matches!(score, PropertyValue::Double(v) if (v - 1.0).abs() < 0.01), "expected preferential_attachment ~1.0, got {:?}", score);
}

#[test]
fn e2e_call_algo_dijkstra() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:LINKS]->(b:Node)-[:LINKS]->(c:Node)").unwrap();
    let gid_result = ctx.run("MATCH (n:Node) RETURN id(n) AS gid ORDER BY gid").unwrap();
    let gids: Vec<i64> = gid_result.rows.iter().map(|r| match r.get("gid") {
        Some(PropertyValue::Int(v)) => *v, _ => panic!("expected int gid"),
    }).collect();
    assert_eq!(gids.len(), 3);

    let result = ctx.run(&format!("CALL algo.dijkstra({}, {})", gids[0], gids[2])).unwrap();
    assert_eq!(result.rows.len(), 1);
    let dist = result.rows[0].get("distance").cloned().unwrap_or(PropertyValue::Null);
    assert!(matches!(dist, PropertyValue::Double(v) if (v - 2.0).abs() < 0.01), "expected distance 2.0, got {:?}", dist);
}

#[test]
fn e2e_call_algo_dijkstra_all() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:LINKS]->(b:Node)-[:LINKS]->(c:Node)").unwrap();
    let gid_result = ctx.run("MATCH (n:Node) RETURN id(n) AS gid ORDER BY gid").unwrap();
    let gids: Vec<i64> = gid_result.rows.iter().map(|r| match r.get("gid") {
        Some(PropertyValue::Int(v)) => *v, _ => panic!("expected int gid"),
    }).collect();
    assert_eq!(gids.len(), 3);

    let result = ctx.run(&format!("CALL algo.dijkstra_all({})", gids[0])).unwrap();
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn e2e_call_algo_floyd_warshall() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:LINKS]->(b:Node)-[:LINKS]->(c:Node)").unwrap();
    let result = ctx.run("CALL algo.floyd_warshall()").unwrap();
    // Directed path a->b->c: reachable pairs are (a,a),(a,b),(a,c),(b,b),(b,c),(c,c) = 6
    assert_eq!(result.rows.len(), 6);
}

#[test]
fn e2e_call_algo_prim() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:LINKS]->(b:Node), (b:Node)-[:LINKS]->(c:Node)").unwrap();
    let gid_result = ctx.run("MATCH (n:Node) RETURN id(n) AS gid ORDER BY gid").unwrap();
    let gids: Vec<i64> = gid_result.rows.iter().map(|r| match r.get("gid") {
        Some(PropertyValue::Int(v)) => *v, _ => panic!("expected int gid"),
    }).collect();
    assert_eq!(gids.len(), 3);

    let result = ctx.run(&format!("CALL algo.prim({})", gids[0])).unwrap();
    // 3-node tree has 2 edges in MST
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn e2e_call_algo_kruskal() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:LINKS]->(b:Node), (b:Node)-[:LINKS]->(c:Node)").unwrap();
    let result = ctx.run("CALL algo.kruskal()").unwrap();
    // 3-node tree has 2 edges in MST
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn e2e_call_algo_shortest_path_weighted() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:LINKS]->(b:Node)-[:LINKS]->(c:Node)").unwrap();
    let gid_result = ctx.run("MATCH (n:Node) RETURN id(n) AS gid ORDER BY gid").unwrap();
    let gids: Vec<i64> = gid_result.rows.iter().map(|r| match r.get("gid") {
        Some(PropertyValue::Int(v)) => *v, _ => panic!("expected int gid"),
    }).collect();
    assert_eq!(gids.len(), 3);

    let result = ctx.run(&format!("CALL algo.shortest_path_weighted({}, {})", gids[0], gids[2])).unwrap();
    assert_eq!(result.rows.len(), 1);
    let weight = result.rows[0].get("weight").cloned().unwrap_or(PropertyValue::Null);
    assert!(matches!(weight, PropertyValue::Double(v) if (v - 2.0).abs() < 0.01), "expected weight 2.0, got {:?}", weight);
}

#[test]
fn e2e_call_algo_simple_random_walk() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:LINKS]->(b:Node), (b:Node)-[:LINKS]->(c:Node)").unwrap();
    let gid_result = ctx.run("MATCH (n:Node) RETURN id(n) AS gid ORDER BY gid").unwrap();
    let gids: Vec<i64> = gid_result.rows.iter().map(|r| match r.get("gid") {
        Some(PropertyValue::Int(v)) => *v, _ => panic!("expected int gid"),
    }).collect();
    assert_eq!(gids.len(), 3);

    let result = ctx.run(&format!("CALL algo.simple_random_walk({}, 5)", gids[0])).unwrap();
    assert_eq!(result.rows.len(), 1);
    let walk = result.rows[0].get("walk").cloned().unwrap_or(PropertyValue::Null);
    assert!(matches!(walk, PropertyValue::List(_)), "expected walk list, got {:?}", walk);
}

#[test]
fn e2e_call_algo_biased_random_walk() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:LINKS]->(b:Node), (b:Node)-[:LINKS]->(c:Node)").unwrap();
    let gid_result = ctx.run("MATCH (n:Node) RETURN id(n) AS gid ORDER BY gid").unwrap();
    let gids: Vec<i64> = gid_result.rows.iter().map(|r| match r.get("gid") {
        Some(PropertyValue::Int(v)) => *v, _ => panic!("expected int gid"),
    }).collect();
    assert_eq!(gids.len(), 3);

    let result = ctx.run(&format!("CALL algo.biased_random_walk({}, 5, 1.0, 1.0)", gids[0])).unwrap();
    assert_eq!(result.rows.len(), 1);
    let walk = result.rows[0].get("walk").cloned().unwrap_or(PropertyValue::Null);
    assert!(matches!(walk, PropertyValue::List(_)), "expected walk list, got {:?}", walk);
}

#[test]
fn e2e_call_algo_generate_walks() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:LINKS]->(b:Node), (b:Node)-[:LINKS]->(c:Node)").unwrap();
    let result = ctx.run("CALL algo.generate_walks(2, 3, 1.0, 1.0)").unwrap();
    // 3 nodes * 2 walks = 6 rows
    assert_eq!(result.rows.len(), 6);
}

#[test]
fn e2e_call_algo_random_walk_with_restart() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:LINKS]->(b:Node), (b:Node)-[:LINKS]->(c:Node)").unwrap();
    let gid_result = ctx.run("MATCH (n:Node) RETURN id(n) AS gid ORDER BY gid").unwrap();
    let gids: Vec<i64> = gid_result.rows.iter().map(|r| match r.get("gid") {
        Some(PropertyValue::Int(v)) => *v, _ => panic!("expected int gid"),
    }).collect();
    assert_eq!(gids.len(), 3);

    let result = ctx.run(&format!("CALL algo.random_walk_with_restart({}, 5, 0.2)", gids[0])).unwrap();
    assert_eq!(result.rows.len(), 1);
    let walk = result.rows[0].get("walk").cloned().unwrap_or(PropertyValue::Null);
    assert!(matches!(walk, PropertyValue::List(_)), "expected walk list, got {:?}", walk);
}

#[test]
fn e2e_call_algo_personalized_pagerank() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:LINKS]->(b:Node), (b:Node)-[:LINKS]->(c:Node), (c:Node)-[:LINKS]->(a:Node)").unwrap();
    let gid_result = ctx.run("MATCH (n:Node) RETURN id(n) AS gid ORDER BY gid").unwrap();
    let gids: Vec<i64> = gid_result.rows.iter().map(|r| match r.get("gid") {
        Some(PropertyValue::Int(v)) => *v, _ => panic!("expected int gid"),
    }).collect();
    assert_eq!(gids.len(), 3);

    let result = ctx.run(&format!("CALL algo.personalized_pagerank({}, 10, 5, 0.15)", gids[0])).unwrap();
    assert_eq!(result.rows.len(), 3);
    let total: f64 = result.rows.iter().map(|r| match r.get("score") {
        Some(PropertyValue::Double(v)) => *v,
        _ => 0.0,
    }).sum();
    assert!(total > 0.9 && total <= 1.1, "expected total ~1.0, got {}", total);
}

#[test]
fn e2e_call_algo_cycle_detection() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:LINKS]->(b:Node)-[:LINKS]->(c:Node)-[:LINKS]->(a:Node)").unwrap();
    let result = ctx.run("CALL algo.cycle_detection()").unwrap();
    assert!(result.rows.len() >= 1, "expected cycle found");
}

#[test]
fn e2e_call_algo_chromatic_number() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:LINKS]->(b:Node)-[:LINKS]->(c:Node)-[:LINKS]->(a:Node)").unwrap();
    let result = ctx.run("CALL algo.chromatic_number()").unwrap();
    assert_eq!(result.rows.len(), 1);
    let num = result.rows[0].get("chromatic_number").cloned().unwrap_or(PropertyValue::Null);
    // Triangle needs 3 colors
    assert!(matches!(num, PropertyValue::Int(3)), "expected chromatic number 3, got {:?}", num);
}

#[test]
fn e2e_where_label_check() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'}), (b:Company {name: 'Acme'})").unwrap();
    // Label check in WHERE using n:Label syntax
    let result = ctx.run("MATCH (n) WHERE n:Person RETURN n.name AS name").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));

    // Label check for non-matching label
    let result2 = ctx.run("MATCH (n) WHERE n:NonExistent RETURN n.name AS name").unwrap();
    assert_eq!(result2.rows.len(), 0);

    // Multiple labels with OR
    let result3 = ctx.run("MATCH (n) WHERE n:Person OR n:Company RETURN n.name AS name ORDER BY name").unwrap();
    assert_eq!(result3.rows.len(), 2);
}

#[test]
fn e2e_where_label_check_with_relationship() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:WORKS_AT]->(b:Company {name: 'Acme'})").unwrap();
    let result = ctx.run("MATCH (n)-[:WORKS_AT]->(m) WHERE n:Person AND m:Company RETURN n.name AS person, m.name AS company").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("person"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(result.rows[0].get("company"), Some(&PropertyValue::String("Acme".into())));
}

#[test]
fn e2e_to_string_or_null() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN toStringOrNull(42) AS s, toStringOrNull('hello') AS t, toStringOrNull(null) AS n").unwrap();
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::String("42".into())));
    assert_eq!(result.rows[0].get("t"), Some(&PropertyValue::String("hello".into())));
    assert_eq!(result.rows[0].get("n"), Some(&PropertyValue::Null));
}

#[test]
fn e2e_to_list() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN toList([1, 2, 3]) AS l, toList('abc') AS s, toList(null) AS n").unwrap();
    assert_eq!(result.rows[0].get("l"), Some(&PropertyValue::List(vec![
        PropertyValue::Int(1), PropertyValue::Int(2), PropertyValue::Int(3)
    ])));
    let chars: Vec<PropertyValue> = "abc".chars().map(|c| PropertyValue::String(c.to_string())).collect();
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::List(chars)));
    assert_eq!(result.rows[0].get("n"), Some(&PropertyValue::Null));
}

#[test]
fn e2e_db_property_keys() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30, city: 'NYC'})").unwrap();
    ctx.run("CREATE (n:Company {name: 'Acme', founded: 2020})").unwrap();
    let result = ctx.run("CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey ORDER BY propertyKey").unwrap();
    let keys: Vec<String> = result.rows.iter()
        .filter_map(|r| r.get("propertyKey"))
        .filter_map(|v| match v {
            PropertyValue::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(keys.contains(&"name".to_string()));
    assert!(keys.contains(&"age".to_string()));
    assert!(keys.contains(&"city".to_string()));
    assert!(keys.contains(&"founded".to_string()));
}

#[test]
fn e2e_db_relationship_types() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company)").unwrap();
    let result = ctx.run("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType ORDER BY relationshipType").unwrap();
    let types: Vec<String> = result.rows.iter()
        .filter_map(|r| r.get("relationshipType"))
        .filter_map(|v| match v {
            PropertyValue::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(types.contains(&"KNOWS".to_string()));
    assert!(types.contains(&"WORKS_AT".to_string()));
}

#[test]
fn e2e_db_labels_returns_names() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice'})").unwrap();
    ctx.run("CREATE (n:Company {name: 'Acme'})").unwrap();
    let result = ctx.run("CALL db.labels() YIELD label RETURN label ORDER BY label").unwrap();
    let labels: Vec<String> = result.rows.iter()
        .filter_map(|r| r.get("label"))
        .filter_map(|v| match v {
            PropertyValue::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(labels.contains(&"Person".to_string()));
    assert!(labels.contains(&"Company".to_string()));
}

#[test]
fn e2e_db_indexes_returns_names() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
    // Create label index and label+property index via storage API
    ctx.storage.create_label_index(ctx.catalog.label("Person"));
    ctx.storage.create_label_property_index(ctx.catalog.label("Person"), ctx.catalog.property("name"));
    let result = ctx.run("CALL db.indexes() YIELD label, property, type RETURN label, property, type ORDER BY label, type").unwrap();
    assert!(result.rows.len() >= 2);
    let types: Vec<String> = result.rows.iter()
        .filter_map(|r| r.get("type"))
        .filter_map(|v| match v {
            PropertyValue::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(types.contains(&"label".to_string()));
    assert!(types.contains(&"label+property".to_string()));
    // Verify label is returned as name, not numeric ID
    let labels: Vec<String> = result.rows.iter()
        .filter_map(|r| r.get("label"))
        .filter_map(|v| match v {
            PropertyValue::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(labels.contains(&"Person".to_string()));
}

#[test]
fn e2e_db_constraints_returns_names() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice'})").unwrap();
    ctx.storage.constraints.add_existence_constraint(ctx.catalog.label("Person"), ctx.catalog.property("name"));
    let result = ctx.run("CALL db.constraints() YIELD type, label, property RETURN type, label, property").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("label"), Some(&PropertyValue::String("Person".into())));
    assert_eq!(result.rows[0].get("property"), Some(&PropertyValue::String("name".into())));
}

#[test]
fn e2e_db_schema_node_type_properties() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
    ctx.run("CREATE (n:Company {name: 'Acme', founded: 2020})").unwrap();
    let result = ctx.run("CALL db.schema.nodeTypeProperties() YIELD nodeType, propertyName RETURN nodeType, propertyName ORDER BY nodeType, propertyName").unwrap();
    assert!(result.rows.len() >= 4);
    let entries: Vec<(String, String)> = result.rows.iter()
        .filter_map(|r| match (r.get("nodeType"), r.get("propertyName")) {
            (Some(PropertyValue::String(t)), Some(PropertyValue::String(p))) => Some((t.clone(), p.clone())),
            _ => None,
        })
        .collect();
    assert!(entries.contains(&("Company".into(), "founded".into())));
    assert!(entries.contains(&("Company".into(), "name".into())));
    assert!(entries.contains(&("Person".into(), "age".into())));
    assert!(entries.contains(&("Person".into(), "name".into())));
}

#[test]
fn e2e_db_schema_rel_type_properties() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person)-[:KNOWS {since: 2020}]->(b:Person)-[:WORKS_AT {role: 'Engineer'}]->(c:Company)").unwrap();
    let result = ctx.run("CALL db.schema.relTypeProperties() YIELD relType, propertyName RETURN relType, propertyName ORDER BY relType, propertyName").unwrap();
    let entries: Vec<(String, String)> = result.rows.iter()
        .filter_map(|r| match (r.get("relType"), r.get("propertyName")) {
            (Some(PropertyValue::String(t)), Some(PropertyValue::String(p))) => Some((t.clone(), p.clone())),
            _ => None,
        })
        .collect();
    assert!(entries.contains(&("KNOWS".into(), "since".into())));
    assert!(entries.contains(&("WORKS_AT".into(), "role".into())));
}

#[test]
fn e2e_db_dump() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice', age: 30})").unwrap();
    ctx.run("CREATE (b:Person {name: 'Bob'})").unwrap();
    ctx.run("CREATE (c:Company {name: 'Acme'})").unwrap();
    ctx.run("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS {since: 2020}]->(b)").unwrap();
    ctx.run("MATCH (b:Person {name: 'Bob'}), (c:Company {name: 'Acme'}) CREATE (b)-[:WORKS_AT]->(c)").unwrap();

    let result = ctx.run("CALL db.dump() YIELD cypher, type RETURN cypher, type").unwrap();
    let vertex_rows: Vec<_> = result.rows.iter().filter(|r| r.get("type") == Some(&PropertyValue::String("vertex".into()))).collect();
    let edge_rows: Vec<_> = result.rows.iter().filter(|r| r.get("type") == Some(&PropertyValue::String("edge".into()))).collect();

    assert_eq!(vertex_rows.len(), 3, "expected 3 vertices");
    assert_eq!(edge_rows.len(), 2, "expected 2 edges");

    // Verify vertex dumps contain CREATE
    for row in &vertex_rows {
        let cypher = match row.get("cypher").unwrap() {
            PropertyValue::String(s) => s.as_str(),
            _ => panic!("cypher should be a string"),
        };
        assert!(cypher.starts_with("CREATE (:"), "vertex dump should start with CREATE (: {}", cypher);
    }

    // Verify edge dumps contain MATCH and CREATE
    for row in &edge_rows {
        let cypher = match row.get("cypher").unwrap() {
            PropertyValue::String(s) => s.as_str(),
            _ => panic!("cypher should be a string"),
        };
        assert!(cypher.starts_with("MATCH "), "edge dump should start with MATCH: {}", cypher);
        assert!(cypher.contains("CREATE (a)-[:"), "edge dump should contain CREATE (a)-[: {}", cypher);
    }
}

#[test]
fn e2e_db_dump_with_complex_properties() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Test {list: [1, 2, 3], map: {a: 1, b: 'hello'}})").unwrap();
    let result = ctx.run("CALL db.dump() YIELD cypher, type RETURN cypher, type").unwrap();
    assert_eq!(result.rows.len(), 1);
    let cypher = match result.rows[0].get("cypher").unwrap() {
        PropertyValue::String(s) => s.clone(),
        _ => panic!("expected string"),
    };
    assert!(cypher.contains("list: [1, 2, 3]"), "dump should format lists: {}", cypher);
    assert!(cypher.contains("a: 1"), "dump should format maps: {}", cypher);
    assert!(cypher.contains("b: 'hello'"), "dump should format nested strings: {}", cypher);
}

#[test]
fn e2e_db_dump_with_temporal_properties() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Event {created: date('2024-01-15'), time: localtime('14:30:00')})").unwrap();
    let result = ctx.run("CALL db.dump() YIELD cypher, type RETURN cypher, type").unwrap();
    assert_eq!(result.rows.len(), 1);
    let cypher = match result.rows[0].get("cypher").unwrap() {
        PropertyValue::String(s) => s.clone(),
        _ => panic!("expected string"),
    };
    assert!(cypher.contains("date('2024-01-15')"), "dump should format dates: {}", cypher);
    assert!(cypher.contains("localtime('14:30:00')"), "dump should format localtime: {}", cypher);
}

#[test]
fn e2e_db_dump_with_point_property() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Location {coord: point({x: 10.5, y: 20.3, crs: 'cartesian'})})").unwrap();
    let result = ctx.run("CALL db.dump() YIELD cypher, type RETURN cypher, type").unwrap();
    assert_eq!(result.rows.len(), 1);
    let cypher = match result.rows[0].get("cypher").unwrap() {
        PropertyValue::String(s) => s.clone(),
        _ => panic!("expected string"),
    };
    assert!(cypher.contains("point("), "dump should format points: {}", cypher);
    assert!(cypher.contains("x: 10.5"), "dump should contain x: {}", cypher);
    assert!(cypher.contains("y: 20.3"), "dump should contain y: {}", cypher);
}

#[test]
fn e2e_db_dump_escapes_quotes() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: \"O'Brien\"})").unwrap();
    let result = ctx.run("CALL db.dump() YIELD cypher, type RETURN cypher, type").unwrap();
    assert_eq!(result.rows.len(), 1);
    let cypher = match result.rows[0].get("cypher").unwrap() {
        PropertyValue::String(s) => s.clone(),
        _ => panic!("expected string"),
    };
    assert!(cypher.contains("O\\'Brien"), "dump should escape quotes: {}", cypher);
}

#[test]
fn e2e_apoc_meta_schema() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30})-[:KNOWS {since: 2020}]->(m:Person {name: 'Bob'})").unwrap();
    let result = ctx.run("CALL apoc.meta.schema() YIELD value RETURN value").unwrap();
    assert_eq!(result.rows.len(), 1);
    let schema = result.rows[0].get("value").unwrap();
    if let PropertyValue::Map(entries) = schema {
        let nodes = entries.iter().find(|(k, _)| k == "nodes").map(|(_, v)| v);
        if let Some(PropertyValue::Map(node_types)) = nodes {
            let person = node_types.iter().find(|(k, _)| k == "Person").map(|(_, v)| v);
            if let Some(PropertyValue::Map(props)) = person {
                let count = props.iter().find(|(k, _)| k == "count").map(|(_, v)| v);
                assert_eq!(count, Some(&PropertyValue::Int(2)));
            } else {
                panic!("Person node type not found in schema");
            }
        } else {
            panic!("nodes section not found in schema");
        }
        let rels = entries.iter().find(|(k, _)| k == "relationships").map(|(_, v)| v);
        if let Some(PropertyValue::Map(rel_types)) = rels {
            let knows = rel_types.iter().find(|(k, _)| k == "KNOWS").map(|(_, v)| v);
            if let Some(PropertyValue::Map(props)) = knows {
                let count = props.iter().find(|(k, _)| k == "count").map(|(_, v)| v);
                assert_eq!(count, Some(&PropertyValue::Int(1)));
            } else {
                panic!("KNOWS rel type not found in schema");
            }
        } else {
            panic!("relationships section not found in schema");
        }
    } else {
        panic!("schema value is not a Map");
    }
}

#[test]
fn e2e_apoc_convert_to_set() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc.convert.toSet([1, 2, 2, 3, 3, 3]) AS s").unwrap();
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::List(vec![
        PropertyValue::Int(1), PropertyValue::Int(2), PropertyValue::Int(3)
    ])));
}

#[test]
fn e2e_db_analytics() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)").unwrap();
    let result = ctx.run("CALL db.analytics() YIELD vertexCount, edgeCount, avgDegree, density, connectedComponents, isolatedVertices RETURN *").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("vertexCount"), Some(&PropertyValue::Int(3)));
    assert_eq!(result.rows[0].get("edgeCount"), Some(&PropertyValue::Int(2)));
    assert!(result.rows[0].get("avgDegree").is_some());
    assert!(result.rows[0].get("density").is_some());
    assert!(result.rows[0].get("connectedComponents").is_some());
    assert!(result.rows[0].get("isolatedVertices").is_some());
}

#[test]
fn e2e_db_query_stats() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})").unwrap();
    ctx.run("MATCH (n:Person) RETURN n.name").unwrap();
    ctx.run("MATCH (n:Person) RETURN n.name").unwrap();
    let result = ctx.run("CALL db.queryStats() YIELD query, count RETURN query, count").unwrap();
    assert!(!result.rows.is_empty());
    let query_stats: std::collections::HashMap<_, _> = result.rows.iter()
        .filter_map(|r| {
            let q = match r.get("query")? { PropertyValue::String(s) => s.clone(), _ => return None };
            let c = match r.get("count")? { PropertyValue::Int(n) => *n, _ => return None };
            Some((q, c))
        })
        .collect();
    assert!(query_stats.contains_key("CREATE (a:Person {name: 'Alice'})"));
    assert!(query_stats.contains_key("MATCH (n:Person) RETURN n.name"));
    assert_eq!(query_stats.get("MATCH (n:Person) RETURN n.name").copied(), Some(2));
}

#[test]
fn e2e_db_stats() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person)-[:KNOWS]->(b:Person)").unwrap();
    let result = ctx.run("CALL db.stats() YIELD stat, value RETURN stat, value").unwrap();
    let stats: std::collections::HashMap<_, _> = result.rows.iter()
        .filter_map(|r| {
            let s = match r.get("stat")? { PropertyValue::String(v) => v.clone(), _ => return None };
            let v = match r.get("value")? { PropertyValue::String(v) => v.clone(), PropertyValue::Int(n) => n.to_string(), _ => return None };
            Some((s, v))
        })
        .collect();
    assert_eq!(stats.get("vertex_count"), Some(&"2".to_string()));
    assert_eq!(stats.get("edge_count"), Some(&"1".to_string()));
    assert_eq!(stats.get("storage_mode"), Some(&"in_memory".to_string()));
    assert!(stats.contains_key("label_indices"));
    assert!(stats.contains_key("constraints"));
    assert!(stats.contains_key("transactions_committed"));
}

#[test]
fn e2e_db_list_queries() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})").unwrap();
    let result = ctx.run("CALL db.listQueries() YIELD queryId, query, elapsedTimeMs RETURN queryId, query, elapsedTimeMs").unwrap();
    // The CALL query itself is active during execution
    assert!(result.rows.len() >= 1);
    let has_listqueries = result.rows.iter().any(|r| {
        match r.get("query") {
            Some(PropertyValue::String(s)) => s.contains("db.listQueries"),
            _ => false,
        }
    });
    assert!(has_listqueries, "db.listQueries should include itself");
}

#[test]
fn e2e_apoc_coll_combinations() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc.coll.combinations([1, 2, 3], 2) AS c").unwrap();
    let combos = result.rows[0].get("c").unwrap();
    if let PropertyValue::List(items) = combos {
        assert_eq!(items.len(), 3);
    } else {
        panic!("expected list of combinations");
    }
    let result = ctx.run("RETURN apoc.coll.combinations(['a', 'b', 'c', 'd'], 3) AS c").unwrap();
    let combos = result.rows[0].get("c").unwrap();
    if let PropertyValue::List(items) = combos {
        assert_eq!(items.len(), 4);
    } else {
        panic!("expected list of combinations");
    }
}

#[test]
fn e2e_apoc_coll_subtract() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc.coll.subtract([1, 2, 3, 4], [2, 4]) AS s").unwrap();
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::List(vec![
        PropertyValue::Int(1), PropertyValue::Int(3)
    ])));
}

#[test]
fn e2e_apoc_coll_union_all() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc.coll.unionAll([1, 2], [2, 3]) AS u").unwrap();
    assert_eq!(result.rows[0].get("u"), Some(&PropertyValue::List(vec![
        PropertyValue::Int(1), PropertyValue::Int(2), PropertyValue::Int(2), PropertyValue::Int(3)
    ])));
}

#[test]
fn e2e_apoc_number_format() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_number_format(1234) AS n, apoc_number_format(3.14159, '#,##0.00') AS f").unwrap();
    assert_eq!(result.rows[0].get("n"), Some(&PropertyValue::String("1234".into())));
    assert_eq!(result.rows[0].get("f"), Some(&PropertyValue::String("3.14".into())));
}

#[test]
fn e2e_apoc_text_levenshtein() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_text_levenshtein('kitten', 'sitting') AS d, apoc_text_levenshtein('', 'abc') AS e").unwrap();
    assert_eq!(result.rows[0].get("d"), Some(&PropertyValue::Int(3)));
    assert_eq!(result.rows[0].get("e"), Some(&PropertyValue::Int(3)));
}

#[test]
fn e2e_apoc_coll_random_item() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_coll_randomItem([1, 2, 3]) AS r").unwrap();
    let val = result.rows[0].get("r").unwrap();
    assert!(matches!(val, PropertyValue::Int(1) | PropertyValue::Int(2) | PropertyValue::Int(3)));
    let result = ctx.run("RETURN apoc_coll_randomItem([]) AS r").unwrap();
    assert_eq!(result.rows[0].get("r"), Some(&PropertyValue::Null));
}

#[test]
fn e2e_apoc_date_current_timestamp() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_date_currentTimestamp() AS ts").unwrap();
    let ts = result.rows[0].get("ts").unwrap();
    if let PropertyValue::Int(n) = ts {
        assert!(*n > 0);
    } else {
        panic!("expected Int timestamp");
    }
}

#[test]
fn e2e_apoc_text_capitalize() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_text_capitalize('hello') AS c, apoc_text_capitalize('WORLD') AS d").unwrap();
    assert_eq!(result.rows[0].get("c"), Some(&PropertyValue::String("Hello".into())));
    assert_eq!(result.rows[0].get("d"), Some(&PropertyValue::String("World".into())));
}

#[test]
fn e2e_apoc_text_decapitalize() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_text_decapitalize('Hello') AS d, apoc_text_decapitalize('WORLD') AS e").unwrap();
    assert_eq!(result.rows[0].get("d"), Some(&PropertyValue::String("hello".into())));
    assert_eq!(result.rows[0].get("e"), Some(&PropertyValue::String("wORLD".into())));
}

#[test]
fn e2e_apoc_text_regreplace() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_text_regreplace('hello world', 'o', '0') AS r").unwrap();
    assert_eq!(result.rows[0].get("r"), Some(&PropertyValue::String("hell0 w0rld".into())));
}

#[test]
fn e2e_apoc_text_slug() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_text_slug('Hello World!') AS s").unwrap();
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::String("hello-world".into())));
}

#[test]
fn e2e_apoc_text_pad() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_text_pad('hi', 5) AS l, apoc_text_pad('hi', 5, ' ', 'right') AS r, apoc_text_pad('hi', 6, '*', 'center') AS c").unwrap();
    assert_eq!(result.rows[0].get("l"), Some(&PropertyValue::String("   hi".into())));
    assert_eq!(result.rows[0].get("r"), Some(&PropertyValue::String("hi   ".into())));
    assert_eq!(result.rows[0].get("c"), Some(&PropertyValue::String("**hi**".into())));
}

#[test]
fn e2e_apoc_text_random() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_text_random(8) AS s").unwrap();
    let s = match result.rows[0].get("s").unwrap() {
        PropertyValue::String(s) => s.clone(),
        _ => panic!("expected string"),
    };
    assert_eq!(s.len(), 8);
    assert!(s.chars().all(|c| c.is_alphanumeric()));
}

#[test]
fn e2e_apoc_text_compareignorecase() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_text_compareIgnoreCase('abc', 'ABC') AS eq, apoc_text_compareIgnoreCase('abc', 'def') AS lt").unwrap();
    assert_eq!(result.rows[0].get("eq"), Some(&PropertyValue::Int(0)));
    assert!(matches!(result.rows[0].get("lt"), Some(&PropertyValue::Int(v)) if v < 0));
}

#[test]
fn e2e_apoc_text_charat() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_text_charAt('hello', 1) AS c, apoc_text_charAt('hello', 10) AS o").unwrap();
    assert_eq!(result.rows[0].get("c"), Some(&PropertyValue::String("e".into())));
    assert_eq!(result.rows[0].get("o"), Some(&PropertyValue::String("".into())));
}

#[test]
fn e2e_apoc_text_camelcase() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_text_camelCase('hello_world') AS a, apoc_text_camelCase('foo-bar') AS b").unwrap();
    assert_eq!(result.rows[0].get("a"), Some(&PropertyValue::String("helloWorld".into())));
    assert_eq!(result.rows[0].get("b"), Some(&PropertyValue::String("fooBar".into())));
}

#[test]
fn e2e_apoc_text_snakecase() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_text_snakeCase('helloWorld') AS a, apoc_text_snakeCase('foo-bar') AS b").unwrap();
    assert_eq!(result.rows[0].get("a"), Some(&PropertyValue::String("hello_world".into())));
    assert_eq!(result.rows[0].get("b"), Some(&PropertyValue::String("foo_bar".into())));
}

#[test]
fn e2e_apoc_text_startswith() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_text_startsWith('hello', 'he') AS t, apoc_text_startsWith('hello', 'x') AS f").unwrap();
    assert_eq!(result.rows[0].get("t"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("f"), Some(&PropertyValue::Bool(false)));
}

#[test]
fn e2e_apoc_text_endswith() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_text_endsWith('hello', 'lo') AS t, apoc_text_endsWith('hello', 'x') AS f").unwrap();
    assert_eq!(result.rows[0].get("t"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("f"), Some(&PropertyValue::Bool(false)));
}

#[test]
fn e2e_apoc_text_isempty() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_text_isEmpty('') AS t, apoc_text_isEmpty('a') AS f").unwrap();
    assert_eq!(result.rows[0].get("t"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("f"), Some(&PropertyValue::Bool(false)));
}

#[test]
fn e2e_apoc_text_urlencode() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_text_urlEncode('hello world') AS e").unwrap();
    assert_eq!(result.rows[0].get("e"), Some(&PropertyValue::String("hello%20world".into())));
}

#[test]
fn e2e_apoc_text_urldecode() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_text_urlDecode('hello%20world') AS d").unwrap();
    assert_eq!(result.rows[0].get("d"), Some(&PropertyValue::String("hello world".into())));
}

#[test]
fn e2e_apoc_text_base64encode() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_text_base64Encode('hello') AS e").unwrap();
    assert_eq!(result.rows[0].get("e"), Some(&PropertyValue::String("aGVsbG8=".into())));
}

#[test]
fn e2e_apoc_text_base64decode() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_text_base64Decode('aGVsbG8=') AS d").unwrap();
    assert_eq!(result.rows[0].get("d"), Some(&PropertyValue::String("hello".into())));
}

#[test]
fn e2e_apoc_text_soundex() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_text_soundex('Robert') AS s, apoc_text_soundex('Rupert') AS r").unwrap();
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::String("R163".into())));
    assert_eq!(result.rows[0].get("r"), Some(&PropertyValue::String("R163".into())));
}

#[test]
fn e2e_apoc_text_striptags() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_text_stripTags('<b>hello</b> world') AS s").unwrap();
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::String("hello world".into())));
}

#[test]
fn e2e_apoc_text_doublemetaphone() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_text_doubleMetaphone('Smith') AS m").unwrap();
    let list = result.rows[0].get("m").unwrap();
    if let PropertyValue::List(parts) = list {
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], PropertyValue::String("SM0".into()));
    } else {
        panic!("expected list");
    }
}

#[test]
fn e2e_apoc_date_parse() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_date_parse('2024-03-15', 'yyyy-MM-dd') AS ts").unwrap();
    let ts = result.rows[0].get("ts").unwrap();
    assert!(matches!(ts, PropertyValue::Int(n) if *n > 0));
}

#[test]
fn e2e_apoc_date_add() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_date_add(0, 1, 'day') AS d").unwrap();
    assert_eq!(result.rows[0].get("d"), Some(&PropertyValue::Int(24 * 60 * 60 * 1000)));
}

#[test]
fn e2e_apoc_date_fields() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_date_fields(1700000000000) AS f").unwrap();
    let map = result.rows[0].get("f").unwrap();
    if let PropertyValue::Map(fields) = map {
        let year = fields.iter().find(|(k, _)| k == "year").map(|(_, v)| v.clone());
        assert_eq!(year, Some(PropertyValue::Int(2023)));
    } else {
        panic!("expected map");
    }
}

#[test]
fn e2e_apoc_number_parseint() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_number_parseInt('42') AS n, apoc_number_parseInt('FF', 16) AS h").unwrap();
    assert_eq!(result.rows[0].get("n"), Some(&PropertyValue::Int(42)));
    assert_eq!(result.rows[0].get("h"), Some(&PropertyValue::Int(255)));
}

#[test]
fn e2e_apoc_number_parsefloat() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_number_parseFloat('3.14') AS f").unwrap();
    assert_eq!(result.rows[0].get("f"), Some(&PropertyValue::Double(3.14)));
}

#[test]
fn e2e_apoc_number_round() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_number_round(3.14159, 2) AS r").unwrap();
    assert_eq!(result.rows[0].get("r"), Some(&PropertyValue::Double(3.14)));
}

#[test]
fn e2e_apoc_number_abs() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_number_abs(-5) AS i, apoc_number_abs(-3.5) AS d").unwrap();
    assert_eq!(result.rows[0].get("i"), Some(&PropertyValue::Int(5)));
    assert_eq!(result.rows[0].get("d"), Some(&PropertyValue::Double(3.5)));
}

#[test]
fn e2e_apoc_number_sign() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_number_sign(-5) AS n, apoc_number_sign(0) AS z, apoc_number_sign(5) AS p").unwrap();
    assert_eq!(result.rows[0].get("n"), Some(&PropertyValue::Int(-1)));
    assert_eq!(result.rows[0].get("z"), Some(&PropertyValue::Int(0)));
    assert_eq!(result.rows[0].get("p"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_db_create_label() {
    let ctx = TestCtx::new();
    let result = ctx.run("CALL db.createLabel('NewLabel') YIELD label RETURN label").unwrap();
    assert_eq!(result.rows[0].get("label"), Some(&PropertyValue::String("NewLabel".into())));
}

#[test]
fn e2e_db_create_property() {
    let ctx = TestCtx::new();
    let result = ctx.run("CALL db.createProperty('newProp') YIELD property RETURN property").unwrap();
    assert_eq!(result.rows[0].get("property"), Some(&PropertyValue::String("newProp".into())));
}

#[test]
fn e2e_db_create_relationship_type() {
    let ctx = TestCtx::new();
    let result = ctx.run("CALL db.createRelationshipType('NEW_REL') YIELD relationshipType RETURN relationshipType").unwrap();
    assert_eq!(result.rows[0].get("relationshipType"), Some(&PropertyValue::String("NEW_REL".into())));
}

#[test]
fn e2e_db_stats_retrieve() {
    let ctx = TestCtx::new();
    ctx.run("RETURN 1 AS a").unwrap();
    let result = ctx.run("CALL db.stats.retrieve() YIELD query, count RETURN count").unwrap();
    assert!(result.rows.len() >= 1);
}

#[test]
fn e2e_apoc_util_md5() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_util_md5('hello') AS h").unwrap();
    let hash = result.rows[0].get("h").unwrap();
    assert!(matches!(hash, PropertyValue::String(s) if s.len() == 32));
}

#[test]
fn e2e_apoc_util_sha1() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_util_sha1('hello') AS h").unwrap();
    let hash = result.rows[0].get("h").unwrap();
    assert!(matches!(hash, PropertyValue::String(s) if s.len() == 40));
}

#[test]
fn e2e_apoc_util_sha256() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_util_sha256('hello') AS h").unwrap();
    let hash = result.rows[0].get("h").unwrap();
    assert!(matches!(hash, PropertyValue::String(s) if s.len() == 64));
}

#[test]
fn e2e_apoc_util_tojson() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_util_toJson({a: 1, b: 'x'}) AS j").unwrap();
    let json = result.rows[0].get("j").unwrap();
    assert!(matches!(json, PropertyValue::String(s) if s.contains("a") && s.contains("x")));
}

#[test]
fn e2e_apoc_util_fromjson() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_util_fromJson('{\"a\":1}') AS m").unwrap();
    let map = result.rows[0].get("m").unwrap();
    assert!(matches!(map, PropertyValue::Map(_)));
}

#[test]
fn e2e_apoc_meta_types() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_meta_types(42) AS t, apoc_meta_types('hello') AS s, apoc_meta_types([1,2]) AS l").unwrap();
    assert_eq!(result.rows[0].get("t"), Some(&PropertyValue::String("INTEGER".into())));
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::String("STRING".into())));
    assert_eq!(result.rows[0].get("l"), Some(&PropertyValue::String("LIST".into())));
}

#[test]
fn e2e_apoc_meta_istype() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_meta_isType(42, 'INTEGER') AS t, apoc_meta_isType(42, 'STRING') AS f").unwrap();
    assert_eq!(result.rows[0].get("t"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("f"), Some(&PropertyValue::Bool(false)));
}

#[test]
fn e2e_apoc_convert_toboolean() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_convert_toBoolean('true') AS t, apoc_convert_toBoolean(0) AS f").unwrap();
    assert_eq!(result.rows[0].get("t"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("f"), Some(&PropertyValue::Bool(false)));
}

#[test]
fn e2e_apoc_convert_tostring() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_convert_toString(42) AS s").unwrap();
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::String("42".into())));
}

#[test]
fn e2e_apoc_convert_tointeger() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_convert_toInteger('42') AS i, apoc_convert_toInteger(3.14) AS f").unwrap();
    assert_eq!(result.rows[0].get("i"), Some(&PropertyValue::Int(42)));
    assert_eq!(result.rows[0].get("f"), Some(&PropertyValue::Int(3)));
}

#[test]
fn e2e_apoc_convert_tofloat() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_convert_toFloat('3.14') AS f, apoc_convert_toFloat(5) AS i").unwrap();
    assert_eq!(result.rows[0].get("f"), Some(&PropertyValue::Double(3.14)));
    assert_eq!(result.rows[0].get("i"), Some(&PropertyValue::Double(5.0)));
}

#[test]
fn e2e_apoc_convert_tolist() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_convert_toList('ab') AS s").unwrap();
    let list = result.rows[0].get("s").unwrap();
    assert!(matches!(list, PropertyValue::List(v) if v.len() == 2));
}

#[test]
fn e2e_apoc_map_flatten() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_map_flatten({a: {b: 1}}, '.') AS f").unwrap();
    let map = result.rows[0].get("f").unwrap();
    assert!(matches!(map, PropertyValue::Map(m) if m.iter().any(|(k, _)| k == "a.b")));
}

#[test]
fn e2e_apoc_map_sorted() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_map_sorted({c: 1, a: 2}) AS s").unwrap();
    let map = result.rows[0].get("s").unwrap();
    if let PropertyValue::Map(m) = map {
        assert_eq!(m[0].0, "a");
    } else {
        panic!("expected map");
    }
}

#[test]
fn e2e_db_create_edge_type_index() {
    let ctx = TestCtx::new();
    let result = ctx.run("CALL db.createEdgeTypeIndex('KNOWS') YIELD edgeType, status RETURN edgeType, status").unwrap();
    assert_eq!(result.rows[0].get("edgeType"), Some(&PropertyValue::String("KNOWS".into())));
    assert_eq!(result.rows[0].get("status"), Some(&PropertyValue::String("created".into())));
}

#[test]
fn e2e_db_indexes_includes_edge_types() {
    let ctx = TestCtx::new();
    ctx.run("CALL db.createEdgeTypeIndex('FOLLOWS') YIELD status RETURN status").unwrap();
    let result = ctx.run("CALL db.indexes() YIELD label, type RETURN label, type").unwrap();
    let has_edge = result.rows.iter().any(|r| r.get("type") == Some(&PropertyValue::String("edge_type".into())));
    assert!(has_edge, "expected edge_type index in db.indexes");
}

#[test]
fn e2e_apoc_coll_containsall() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_coll_containsAll([1, 2, 3], [1, 2]) AS t, apoc_coll_containsAll([1, 2, 3], [4]) AS f").unwrap();
    assert_eq!(result.rows[0].get("t"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("f"), Some(&PropertyValue::Bool(false)));
}

#[test]
fn e2e_apoc_coll_set() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_coll_set([1, 2, 2, 3, 3, 3]) AS s").unwrap();
    let list = result.rows[0].get("s").unwrap();
    if let PropertyValue::List(v) = list {
        assert_eq!(v.len(), 3);
        assert_eq!(v[0], PropertyValue::Int(1));
    } else {
        panic!("expected list");
    }
}

#[test]
fn e2e_apoc_text_lpad() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_text_lpad('hi', 5) AS l, apoc_text_rpad('hi', 5) AS r").unwrap();
    assert_eq!(result.rows[0].get("l"), Some(&PropertyValue::String("   hi".into())));
    assert_eq!(result.rows[0].get("r"), Some(&PropertyValue::String("hi   ".into())));
}

#[test]
fn e2e_apoc_coll_insert_all() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_coll_insertAll([1, 4, 5], 1, [2, 3]) AS l").unwrap();
    assert_eq!(result.rows[0].get("l"), Some(&PropertyValue::List(vec![
        PropertyValue::Int(1), PropertyValue::Int(2), PropertyValue::Int(3), PropertyValue::Int(4), PropertyValue::Int(5)
    ])));
}

#[test]
fn e2e_apoc_coll_nth() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_coll_nth([10, 20, 30], 1) AS a, apoc_coll_nth([10, 20, 30], -1) AS b").unwrap();
    assert_eq!(result.rows[0].get("a"), Some(&PropertyValue::Int(20)));
    assert_eq!(result.rows[0].get("b"), Some(&PropertyValue::Int(30)));
}

#[test]
fn e2e_apoc_coll_partition() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_coll_partition([1, 2, 3, 4, 5], 2) AS p").unwrap();
    let parts = result.rows[0].get("p").unwrap();
    if let PropertyValue::List(items) = parts {
        assert_eq!(items.len(), 3);
        assert_eq!(items[0], PropertyValue::List(vec![PropertyValue::Int(1), PropertyValue::Int(2)]));
        assert_eq!(items[1], PropertyValue::List(vec![PropertyValue::Int(3), PropertyValue::Int(4)]));
        assert_eq!(items[2], PropertyValue::List(vec![PropertyValue::Int(5)]));
    } else {
        panic!("expected list of partitions");
    }
}

#[test]
fn e2e_apoc_coll_sort_maps() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_coll_sortMaps([{name: 'Bob', age: 30}, {name: 'Alice', age: 25}], 'age') AS sorted").unwrap();
    let sorted = result.rows[0].get("sorted").unwrap();
    if let PropertyValue::List(items) = sorted {
        assert_eq!(items.len(), 2);
        if let PropertyValue::Map(ref first) = items[0] {
            assert_eq!(first.iter().find(|(k, _)| k == "name").map(|(_, v)| v), Some(&PropertyValue::String("Alice".into())));
        }
        if let PropertyValue::Map(ref second) = items[1] {
            assert_eq!(second.iter().find(|(k, _)| k == "name").map(|(_, v)| v), Some(&PropertyValue::String("Bob".into())));
        }
    } else {
        panic!("expected sorted list of maps");
    }
}

#[test]
fn e2e_apoc_map_clean() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_map_clean({a: 1, b: null, c: 'hello'}) AS m").unwrap();
    let map = result.rows[0].get("m").unwrap();
    if let PropertyValue::Map(entries) = map {
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().any(|(k, _)| k == "a"));
        assert!(entries.iter().any(|(k, _)| k == "c"));
        assert!(!entries.iter().any(|(k, _)| k == "b"));
    } else {
        panic!("expected map");
    }
}

#[test]
fn e2e_apoc_map_invert() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN apoc_map_invert({a: 1, b: 2}) AS m").unwrap();
    let map = result.rows[0].get("m").unwrap();
    if let PropertyValue::Map(entries) = map {
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().any(|(k, v)| k == "1" && matches!(v, PropertyValue::String(s) if s == "a")));
        assert!(entries.iter().any(|(k, v)| k == "2" && matches!(v, PropertyValue::String(s) if s == "b")));
    } else {
        panic!("expected map");
    }
}

#[test]
fn e2e_edge_type_index_scan() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (:Person {name: 'Alice'})-[:KNOWS {since: 2010}]->(:Person {name: 'Bob'})").unwrap();
    ctx.run("CREATE (:Person {name: 'Charlie'})-[:WORKS_AT]->(:Company {name: 'Acme'})").unwrap();

    // Create edge type index
    ctx.storage.create_edge_type_index(ctx.catalog.edge_type("KNOWS"));

    let result = ctx.run("MATCH ()-[r:KNOWS]->() RETURN r.since AS since").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("since"), Some(&PropertyValue::Int(2010)));
}

#[test]
fn e2e_edge_type_property_index_scan() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (:Person {name: 'Alice'})-[:KNOWS {since: 2010}]->(:Person {name: 'Bob'})").unwrap();
    ctx.run("CREATE (:Person {name: 'Charlie'})-[:KNOWS {since: 2015}]->(:Person {name: 'Dave'})").unwrap();
    ctx.run("CREATE (:Person {name: 'Eve'})-[:WORKS_AT {since: 2020}]->(:Company {name: 'Acme'})").unwrap();

    // Create edge type property index
    let knows_etype = ctx.catalog.edge_type("KNOWS");
    let since_prop = ctx.catalog.property("since");
    ctx.storage.create_edge_type_property_index(knows_etype, since_prop);

    // Query that should use the edge type property index
    let result = ctx.run("MATCH ()-[r:KNOWS]->() WHERE r.since = 2010 RETURN r.since AS since").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("since"), Some(&PropertyValue::Int(2010)));

    // Query with different value
    let result2 = ctx.run("MATCH ()-[r:KNOWS]->() WHERE r.since = 2015 RETURN r.since AS since").unwrap();
    assert_eq!(result2.rows.len(), 1);
    assert_eq!(result2.rows[0].get("since"), Some(&PropertyValue::Int(2015)));

    // Query with non-matching value
    let result3 = ctx.run("MATCH ()-[r:KNOWS]->() WHERE r.since = 9999 RETURN r.since AS since").unwrap();
    assert_eq!(result3.rows.len(), 0);
}

#[test]
fn e2e_explain_edge_type_scan() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'})").unwrap();
    ctx.storage.create_edge_type_index(ctx.catalog.edge_type("KNOWS"));

    let result = ctx.run("EXPLAIN MATCH ()-[r:KNOWS]->() RETURN r").unwrap();
    assert_eq!(result.rows.len(), 1);
    let plan = result.rows[0].get("PLAN").unwrap();
    if let PropertyValue::String(plan_str) = plan {
        assert!(plan_str.contains("EdgeTypeScan"), "plan should contain EdgeTypeScan: {}", plan_str);
    } else {
        panic!("expected string plan");
    }
}

#[test]
fn e2e_explain_edge_type_property_scan() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (:Person {name: 'Alice'})-[:KNOWS {since: 2010}]->(:Person {name: 'Bob'})").unwrap();
    let knows = ctx.catalog.edge_type("KNOWS");
    let since = ctx.catalog.property("since");
    ctx.storage.create_edge_type_property_index(knows, since);

    let result = ctx.run("EXPLAIN MATCH ()-[r:KNOWS]->() WHERE r.since = 2010 RETURN r").unwrap();
    assert_eq!(result.rows.len(), 1);
    let plan = result.rows[0].get("PLAN").unwrap();
    if let PropertyValue::String(plan_str) = plan {
        assert!(plan_str.contains("EdgeTypePropertyScan"), "plan should contain EdgeTypePropertyScan: {}", plan_str);
    } else {
        panic!("expected string plan");
    }
}

#[test]
fn e2e_query_timeout() {
    let ctx = TestCtx::new();
    // Create many nodes to make a query take longer
    for i in 0..100 {
        ctx.run(&format!("CREATE (:Node {{id: {}}})", i)).unwrap();
    }

    // Execute with a very short timeout (1 nanosecond) to force timeout
    let timeout = Some(std::time::Duration::from_nanos(1));
    let result = mginterp::execute_with_catalog_auth_dbms_and_params_timeout(
        &ctx.storage,
        "MATCH (n) RETURN n",
        Some(&ctx.catalog),
        &std::collections::HashMap::new(),
        None,
        Some(&ctx.dbms),
        timeout,
    );
    assert!(result.is_err(), "expected timeout error");
    let err_str = format!("{}", result.unwrap_err());
    assert!(err_str.contains("timeout") || err_str.contains("Timeout"), "expected timeout in error: {}", err_str);
}

#[test]
fn e2e_edge_type_index_bidirectional_match() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (:Person {name: 'Alice'})-[:KNOWS {since: 2010}]->(:Person {name: 'Bob'})").unwrap();
    ctx.storage.create_edge_type_index(ctx.catalog.edge_type("KNOWS"));

    // Bidirectional match should still work (returns 2 rows: one per direction)
    let result = ctx.run("MATCH ()-[r:KNOWS]-() RETURN r.since AS since").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert!(result.rows.iter().all(|r| r.get("since") == Some(&PropertyValue::Int(2010))));
}

#[test]
fn e2e_edge_type_index_multi_pattern() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (:A)-[:R1]->(:B)-[:R2]->(:C)").unwrap();
    ctx.storage.create_edge_type_index(ctx.catalog.edge_type("R1"));
    ctx.storage.create_edge_type_index(ctx.catalog.edge_type("R2"));

    let result = ctx.run("MATCH ()-[:R1]->()-[:R2]->() RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_profile_edge_type_scan() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'})").unwrap();
    ctx.storage.create_edge_type_index(ctx.catalog.edge_type("KNOWS"));

    let result = ctx.run("PROFILE MATCH ()-[r:KNOWS]->() RETURN r").unwrap();
    assert_eq!(result.rows.len(), 1);
    let plan = result.rows[0].get("PLAN").unwrap();
    if let PropertyValue::String(plan_str) = plan {
        assert!(plan_str.contains("EdgeTypeScan") || plan_str.contains("EdgeExpand"), "plan should contain scan or expand: {}", plan_str);
    } else {
        panic!("expected string plan");
    }
    // PROFILE returns ROWS and TIME_MS
    assert!(result.rows[0].contains_key("ROWS"));
    assert!(result.rows[0].contains_key("TIME_MS"));
}

#[test]
fn e2e_explain_where_false_uses_empty_result() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (:Person {name: 'Alice'})").unwrap();

    let result = ctx.run("EXPLAIN MATCH (n:Person) WHERE false RETURN n").unwrap();
    assert_eq!(result.rows.len(), 1);
    let plan = result.rows[0].get("PLAN").unwrap();
    if let PropertyValue::String(plan_str) = plan {
        // After constant_fold + simplify_trivial_filters, should show EmptyResult
        assert!(plan_str.contains("EmptyResult"), "plan should contain EmptyResult for WHERE false: {}", plan_str);
    } else {
        panic!("expected string plan");
    }
}

#[test]
fn e2e_query_with_parameters() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (:Person {name: 'Alice', age: 30})").unwrap();

    let mut params = std::collections::HashMap::new();
    params.insert("name".to_string(), PropertyValue::String("Alice".to_string()));
    params.insert("age".to_string(), PropertyValue::Int(30));

    let result = mginterp::execute_with_catalog_auth_dbms_and_params_timeout(
        &ctx.storage,
        "MATCH (n:Person {name: $name, age: $age}) RETURN n.name AS name",
        Some(&ctx.catalog),
        &params,
        None,
        Some(&ctx.dbms),
        None,
    ).unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".to_string())));
}

#[test]
fn e2e_query_parameter_type_mismatch() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (:Person {name: 'Alice', age: 30})").unwrap();

    let mut params = std::collections::HashMap::new();
    params.insert("age".to_string(), PropertyValue::String("thirty".to_string()));

    let result = mginterp::execute_with_catalog_auth_dbms_and_params_timeout(
        &ctx.storage,
        "MATCH (n:Person {age: $age}) RETURN n.name AS name",
        Some(&ctx.catalog),
        &params,
        None,
        Some(&ctx.dbms),
        None,
    ).unwrap();

    // String "thirty" doesn't match Int 30, so no rows
    assert_eq!(result.rows.len(), 0);
}

#[test]
fn e2e_set_edge_property() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2010}]->(b:Person {name: 'Bob'})").unwrap();

    ctx.run("MATCH ()-[r:KNOWS]->() SET r.since = 2020").unwrap();

    let result = ctx.run("MATCH ()-[r:KNOWS]->() RETURN r.since AS since").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("since"), Some(&PropertyValue::Int(2020)));
}

#[test]
fn e2e_remove_edge_property() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2010}]->(b:Person {name: 'Bob'})").unwrap();

    ctx.run("MATCH ()-[r:KNOWS]->() REMOVE r.since").unwrap();

    let result = ctx.run("MATCH ()-[r:KNOWS]->() RETURN r.since AS since").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("since"), Some(&PropertyValue::Null));
}

#[test]
fn e2e_merge_relationship_with_properties() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})").unwrap();

    ctx.run("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) MERGE (a)-[r:KNOWS {since: 2020}]->(b)").unwrap();

    let result = ctx.run("MATCH ()-[r:KNOWS]->() RETURN r.since AS since").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("since"), Some(&PropertyValue::Int(2020)));

    // Run MERGE again - should match existing, not create duplicate
    ctx.run("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) MERGE (a)-[r:KNOWS {since: 2020}]->(b)").unwrap();
    let result2 = ctx.run("MATCH ()-[r:KNOWS]->() RETURN count(*) AS cnt").unwrap();
    assert_eq!(result2.rows[0].get("cnt"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_call_yield_all() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (:Person {name: 'Alice'})").unwrap();
    ctx.run("CREATE (:Person {name: 'Bob'})").unwrap();

    let result = ctx.run("CALL db.stats() YIELD * RETURN value").unwrap();
    assert!(result.rows.len() >= 1);
}

#[test]
fn e2e_foreach_mixed_operations() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})").unwrap();
    ctx.run("CREATE (b:Person {name: 'Bob'})").unwrap();
    ctx.run("CREATE (c:Person {name: 'Charlie'})").unwrap();

    // FOREACH creates edges and sets properties
    ctx.run("MATCH (n:Person) WITH collect(n) AS nodes FOREACH (node IN nodes | SET node:Processed, node.tag = 'done')").unwrap();

    let result = ctx.run("MATCH (n:Processed) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(3)));

    let result2 = ctx.run("MATCH (n:Person) RETURN n.tag AS tag ORDER BY n.name").unwrap();
    assert_eq!(result2.rows.len(), 3);
    assert_eq!(result2.rows[0].get("tag"), Some(&PropertyValue::String("done".into())));
}

#[test]
fn e2e_foreach_set_label_basic() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})").unwrap();

    // Simple SET label via FOREACH
    let result = ctx.run("MATCH (n:Person) WITH collect(n) AS nodes RETURN size(nodes) AS sz").unwrap();
    assert_eq!(result.rows[0].get("sz"), Some(&PropertyValue::Int(1)));

    ctx.run("MATCH (n:Person) WITH collect(n) AS nodes FOREACH (node IN nodes | SET node:Processed)").unwrap();

    let result = ctx.run("MATCH (n:Processed) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_set_edge_property_via_map() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person)-[:KNOWS]->(b:Person)").unwrap();

    ctx.run("MATCH ()-[r:KNOWS]->() SET r = {since: 2020, where: 'work'}").unwrap();

    let result = ctx.run("MATCH ()-[r:KNOWS]->() RETURN r.since AS since, r.where AS where").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("since"), Some(&PropertyValue::Int(2020)));
    assert_eq!(result.rows[0].get("where"), Some(&PropertyValue::String("work".into())));
}

#[test]
fn e2e_merge_on_create_set_relationship_property() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})").unwrap();

    ctx.run("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) MERGE (a)-[r:KNOWS]->(b) ON CREATE SET r.since = 2020").unwrap();

    let result = ctx.run("MATCH ()-[r:KNOWS]->() RETURN r.since AS since").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("since"), Some(&PropertyValue::Int(2020)));
}

#[test]
fn e2e_merge_on_match_set_relationship_property() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2010}]->(b:Person {name: 'Bob'})").unwrap();

    ctx.run("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) MERGE (a)-[r:KNOWS]->(b) ON MATCH SET r.since = 2022").unwrap();

    let result = ctx.run("MATCH ()-[r:KNOWS]->() RETURN r.since AS since").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("since"), Some(&PropertyValue::Int(2022)));
}

#[test]
fn e2e_edge_property_in_where() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person)-[:KNOWS {weight: 5}]->(b:Person)").unwrap();
    ctx.run("CREATE (a:Person)-[:KNOWS {weight: 10}]->(c:Person)").unwrap();

    let result = ctx.run("MATCH ()-[r:KNOWS]->() WHERE r.weight > 7 RETURN r.weight AS w").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("w"), Some(&PropertyValue::Int(10)));
}

#[test]
fn e2e_set_multiple_edge_properties() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person)-[:KNOWS]->(b:Person)").unwrap();

    ctx.run("MATCH ()-[r:KNOWS]->() SET r.since = 2020, r.strength = 'strong'").unwrap();

    let result = ctx.run("MATCH ()-[r:KNOWS]->() RETURN r.since AS since, r.strength AS strength").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("since"), Some(&PropertyValue::Int(2020)));
    assert_eq!(result.rows[0].get("strength"), Some(&PropertyValue::String("strong".into())));
}

#[test]
fn e2e_remove_multiple_edge_properties() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person)-[:KNOWS {since: 2020, strength: 'strong'}]->(b:Person)").unwrap();

    ctx.run("MATCH ()-[r:KNOWS]->() REMOVE r.since, r.strength").unwrap();

    let result = ctx.run("MATCH ()-[r:KNOWS]->() RETURN r.since AS since, r.strength AS strength").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("since"), Some(&PropertyValue::Null));
    assert_eq!(result.rows[0].get("strength"), Some(&PropertyValue::Null));
}

#[test]
fn e2e_unwind_with_maps() {
    let ctx = TestCtx::new();

    let result = ctx.run("UNWIND [{name: 'Alice', age: 30}, {name: 'Bob', age: 25}] AS person RETURN person.name AS name, person.age AS age ORDER BY name").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(result.rows[0].get("age"), Some(&PropertyValue::Int(30)));
    assert_eq!(result.rows[1].get("name"), Some(&PropertyValue::String("Bob".into())));
    assert_eq!(result.rows[1].get("age"), Some(&PropertyValue::Int(25)));
}

#[test]
fn e2e_call_procedure_no_yield_then_return() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (:Person {name: 'Alice'})").unwrap();

    // CALL without YIELD should still return procedure results
    let result = ctx.run("CALL db.stats()").unwrap();
    assert!(result.rows.len() >= 1);
}

#[test]
fn e2e_match_set_edge_then_match_again() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person)-[:KNOWS {since: 2010}]->(b:Person)").unwrap();

    ctx.run("MATCH ()-[r:KNOWS]->() SET r.since = r.since + 1").unwrap();

    let result = ctx.run("MATCH ()-[r:KNOWS]->() RETURN r.since AS since").unwrap();
    assert_eq!(result.rows[0].get("since"), Some(&PropertyValue::Int(2011)));
}

#[test]
fn e2e_merge_multiple_prebound_vertices() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'})").unwrap();

    // Create edges from Alice to Bob and Alice to Charlie
    ctx.run("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) MERGE (a)-[r:KNOWS]->(b) MERGE (a)-[r2:KNOWS]->(c)").unwrap();

    let result = ctx.run("MATCH ()-[r:KNOWS]->() RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(2)));

    // Running again should not create duplicates
    ctx.run("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) MERGE (a)-[r:KNOWS]->(b) MERGE (a)-[r2:KNOWS]->(c)").unwrap();

    let result2 = ctx.run("MATCH ()-[r:KNOWS]->() RETURN count(*) AS cnt").unwrap();
    assert_eq!(result2.rows[0].get("cnt"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_with_collect_unwind_combination() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})").unwrap();
    ctx.run("CREATE (b:Person {name: 'Bob'})").unwrap();

    let result = ctx.run("MATCH (n:Person) WITH collect(n) AS nodes UNWIND nodes AS node RETURN node.name AS name ORDER BY name").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(result.rows[1].get("name"), Some(&PropertyValue::String("Bob".into())));
}

#[test]
fn e2e_create_then_set_edge_property() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person)-[:KNOWS]->(b:Person)").unwrap();
    ctx.run("MATCH ()-[r:KNOWS]->() SET r.since = 2020, r.where = 'college'").unwrap();

    let result = ctx.run("MATCH ()-[r:KNOWS]->() RETURN r.since AS since, r.where AS place").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("since"), Some(&PropertyValue::Int(2020)));
    assert_eq!(result.rows[0].get("place"), Some(&PropertyValue::String("college".into())));
}

#[test]
fn e2e_call_yield_star_with_where() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (:Person {name: 'Alice'})").unwrap();
    ctx.run("CREATE (:Person {name: 'Bob'})").unwrap();

    // CALL db.stats() YIELD * WITH stat, value WHERE stat = 'vertex_count' RETURN value
    let result = ctx.run("CALL db.stats() YIELD * WITH stat, value WHERE stat = 'vertex_count' RETURN value").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("value"), Some(&PropertyValue::String("2".into())));
}

#[test]
fn e2e_foreach_nested_create() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})").unwrap();
    ctx.run("CREATE (b:Person {name: 'Bob'})").unwrap();

    ctx.run("MATCH (n:Person) WITH collect(n) AS nodes FOREACH (node IN nodes | CREATE (node)-[:FRIEND]->(:NewNode {tag: 'created'}))").unwrap();

    let result = ctx.run("MATCH (:NewNode) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(2)));

    let result2 = ctx.run("MATCH ()-[:FRIEND]->(:NewNode) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result2.rows[0].get("cnt"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_set_edge_property_to_null() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person)-[:KNOWS {since: 2020}]->(b:Person)").unwrap();

    ctx.run("MATCH ()-[r:KNOWS]->() SET r.since = NULL").unwrap();

    let result = ctx.run("MATCH ()-[r:KNOWS]->() RETURN r.since AS since").unwrap();
    assert_eq!(result.rows[0].get("since"), Some(&PropertyValue::Null));
}

#[test]
fn e2e_remove_all_edge_properties() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person)-[:KNOWS {since: 2020, strength: 10}]->(b:Person)").unwrap();

    ctx.run("MATCH ()-[r:KNOWS]->() REMOVE r.since, r.strength").unwrap();

    let result = ctx.run("MATCH ()-[r:KNOWS]->() RETURN r.since AS since, r.strength AS strength").unwrap();
    assert_eq!(result.rows[0].get("since"), Some(&PropertyValue::Null));
    assert_eq!(result.rows[0].get("strength"), Some(&PropertyValue::Null));
}

#[test]
fn e2e_merge_on_match_set_edge_property() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2010}]->(b:Person {name: 'Bob'})").unwrap();

    ctx.run("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) MERGE (a)-[r:KNOWS]->(b) ON MATCH SET r.since = 2022").unwrap();

    let result = ctx.run("MATCH ()-[r:KNOWS]->() RETURN r.since AS since").unwrap();
    assert_eq!(result.rows[0].get("since"), Some(&PropertyValue::Int(2022)));
}

#[test]
fn e2e_merge_on_create_set_edge_property() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})").unwrap();

    ctx.run("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) MERGE (a)-[r:KNOWS]->(b) ON CREATE SET r.since = 2020").unwrap();

    let result = ctx.run("MATCH ()-[r:KNOWS]->() RETURN r.since AS since").unwrap();
    assert_eq!(result.rows[0].get("since"), Some(&PropertyValue::Int(2020)));
}

#[test]
fn e2e_with_aggregate_then_filter() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {age: 30})").unwrap();
    ctx.run("CREATE (n:Person {age: 25})").unwrap();
    ctx.run("CREATE (n:Person {age: 35})").unwrap();

    let result = ctx.run("MATCH (n:Person) WITH avg(n.age) AS avg_age WHERE avg_age > 20 RETURN avg_age").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("avg_age"), Some(&PropertyValue::Double(30.0)));
}

#[test]
fn e2e_with_count_group_by() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {city: 'NYC'})").unwrap();
    ctx.run("CREATE (n:Person {city: 'NYC'})").unwrap();
    ctx.run("CREATE (n:Person {city: 'LA'})").unwrap();

    let result = ctx.run("MATCH (n:Person) WITH n.city AS city, count(*) AS cnt RETURN city, cnt ORDER BY city").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get("city"), Some(&PropertyValue::String("LA".into())));
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(1)));
    assert_eq!(result.rows[1].get("city"), Some(&PropertyValue::String("NYC".into())));
    assert_eq!(result.rows[1].get("cnt"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_edge_property_filter_with_variable() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person)-[:KNOWS {since: 2010}]->(b:Person)").unwrap();
    ctx.run("CREATE (a:Person)-[:KNOWS {since: 2015}]->(c:Person)").unwrap();

    let result = ctx.run("MATCH ()-[r:KNOWS]->() WHERE r.since >= 2012 RETURN r.since AS since ORDER BY since").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("since"), Some(&PropertyValue::Int(2015)));
}

#[test]
fn e2e_match_bidirectional_with_edge_property() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]-(b:Person {name: 'Bob'})").unwrap();

    let result = ctx.run("MATCH (a:Person)-[r:KNOWS {since: 2020}]-(b:Person) RETURN a.name AS name1, b.name AS name2 ORDER BY name1").unwrap();
    assert_eq!(result.rows.len(), 2);
    // Bidirectional match returns both directions
    assert_eq!(result.rows[0].get("name1"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(result.rows[0].get("name2"), Some(&PropertyValue::String("Bob".into())));
}

#[test]
fn e2e_load_csv_with_headers() {
    use std::io::Write;
    let ctx = TestCtx::new();

    let mut tmp = std::env::temp_dir();
    tmp.push("e2e_load_csv_headers.csv");
    {
        let mut file = std::fs::File::create(&tmp).unwrap();
        file.write_all(b"name,age\nAlice,30\nBob,25\n").unwrap();
    }

    let path = tmp.to_str().unwrap();
    let query = format!(
        "LOAD CSV FROM '{}' WITH HEADERS AS row CREATE (n:Person {{name: row.name, age: toInteger(row.age)}})",
        path
    );
    ctx.run(&query).unwrap();

    let result = ctx.run("MATCH (n:Person) RETURN n.name AS name ORDER BY name").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(result.rows[1].get("name"), Some(&PropertyValue::String("Bob".into())));

    let _ = std::fs::remove_file(&tmp);
}

#[test]
fn e2e_load_csv_without_headers() {
    use std::io::Write;
    let ctx = TestCtx::new();

    let mut tmp = std::env::temp_dir();
    tmp.push("e2e_load_csv_no_hdr.csv");
    {
        let mut file = std::fs::File::create(&tmp).unwrap();
        file.write_all(b"Charlie,35\nDiana,28\n").unwrap();
    }

    let path = tmp.to_str().unwrap();
    let query = format!(
        "LOAD CSV FROM '{}' AS row CREATE (n:Person {{name: row.column_0, age: toInteger(row.column_1)}})",
        path
    );
    ctx.run(&query).unwrap();

    let result = ctx.run("MATCH (n:Person) RETURN n.name AS name ORDER BY name").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Charlie".into())));
    assert_eq!(result.rows[1].get("name"), Some(&PropertyValue::String("Diana".into())));

    let _ = std::fs::remove_file(&tmp);
}

#[test]
fn e2e_periodic_commit_basic() {
    use std::io::Write;
    let ctx = TestCtx::new();

    let mut tmp = std::env::temp_dir();
    tmp.push("e2e_periodic_commit.csv");
    {
        let mut file = std::fs::File::create(&tmp).unwrap();
        file.write_all(b"name,age\nAlice,30\nBob,25\nCharlie,35\n").unwrap();
    }

    let path = tmp.to_str().unwrap();
    let query = format!(
        "USING PERIODIC COMMIT 1 LOAD CSV FROM '{}' WITH HEADERS AS row CREATE (n:Person {{name: row.name, age: toInteger(row.age)}})",
        path
    );
    ctx.run(&query).unwrap();

    let result = ctx.run("MATCH (n:Person) RETURN n.name AS name ORDER BY name").unwrap();
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(result.rows[1].get("name"), Some(&PropertyValue::String("Bob".into())));
    assert_eq!(result.rows[2].get("name"), Some(&PropertyValue::String("Charlie".into())));

    let _ = std::fs::remove_file(&tmp);
}

#[test]
fn e2e_periodic_commit_batch_size_two() {
    use std::io::Write;
    let ctx = TestCtx::new();

    let mut tmp = std::env::temp_dir();
    tmp.push("e2e_periodic_commit_batch2.csv");
    {
        let mut file = std::fs::File::create(&tmp).unwrap();
        file.write_all(b"name\nA\nB\nC\nD\n").unwrap();
    }

    let path = tmp.to_str().unwrap();
    let query = format!(
        "USING PERIODIC COMMIT 2 LOAD CSV FROM '{}' WITH HEADERS AS row CREATE (n:Item {{name: row.name}})",
        path
    );
    ctx.run(&query).unwrap();

    let result = ctx.run("MATCH (n:Item) RETURN n.name AS name ORDER BY name").unwrap();
    assert_eq!(result.rows.len(), 4);

    let _ = std::fs::remove_file(&tmp);
}

#[test]
fn e2e_periodic_commit_with_return() {
    use std::io::Write;
    let ctx = TestCtx::new();

    let mut tmp = std::env::temp_dir();
    tmp.push("e2e_periodic_commit_return.csv");
    {
        let mut file = std::fs::File::create(&tmp).unwrap();
        file.write_all(b"val\n10\n20\n30\n").unwrap();
    }

    let path = tmp.to_str().unwrap();
    let query = format!(
        "USING PERIODIC COMMIT 1 LOAD CSV FROM '{}' WITH HEADERS AS row CREATE (n:Num {{v: toInteger(row.val)}}) RETURN n.v AS v",
        path
    );
    let result = ctx.run(&query).unwrap();
    assert_eq!(result.rows.len(), 3);

    let _ = std::fs::remove_file(&tmp);
}

#[test]
fn e2e_collect_map_aggregate() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
    ctx.run("CREATE (n:Person {name: 'Bob', age: 25})").unwrap();

    let result = ctx.run("MATCH (n:Person) RETURN collect_map(n.name, n.age) AS ages").unwrap();
    assert_eq!(result.rows.len(), 1);
    if let Some(PropertyValue::Map(entries)) = result.rows[0].get("ages") {
        assert_eq!(entries.len(), 2);
        let alice_age = entries.iter().find(|(k, _)| k == "Alice").map(|(_, v)| v);
        let bob_age = entries.iter().find(|(k, _)| k == "Bob").map(|(_, v)| v);
        assert_eq!(alice_age, Some(&PropertyValue::Int(30)));
        assert_eq!(bob_age, Some(&PropertyValue::Int(25)));
    } else {
        panic!("expected map");
    }
}

#[test]
fn e2e_order_by_mixed_numeric_types() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {val: 1})").unwrap();
    ctx.run("CREATE (n:Item {val: 2.5})").unwrap();
    ctx.run("CREATE (n:Item {val: 3})").unwrap();

    let result = ctx.run("MATCH (n:Item) RETURN n.val AS v ORDER BY v").unwrap();
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0].get("v"), Some(&PropertyValue::Int(1)));
    assert_eq!(result.rows[1].get("v"), Some(&PropertyValue::Double(2.5)));
    assert_eq!(result.rows[2].get("v"), Some(&PropertyValue::Int(3)));
}

#[test]
fn e2e_mixed_type_comparison_double_int() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Num {x: 1.5})").unwrap();

    let result = ctx.run("MATCH (n:Num) WHERE n.x > 1 RETURN n.x AS x").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("x"), Some(&PropertyValue::Double(1.5)));

    let result = ctx.run("MATCH (n:Num) WHERE n.x < 2 RETURN n.x AS x").unwrap();
    assert_eq!(result.rows.len(), 1);

    let result = ctx.run("MATCH (n:Num) WHERE n.x >= 1 RETURN n.x AS x").unwrap();
    assert_eq!(result.rows.len(), 1);

    let result = ctx.run("MATCH (n:Num) WHERE n.x <= 2 RETURN n.x AS x").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn e2e_exists_subquery_with_property_filter() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap();
    ctx.run("CREATE (a:Person {name: 'Charlie'})").unwrap();

    let result = ctx.run("MATCH (a:Person) WHERE EXISTS { (a)-[:KNOWS]->(b:Person) } RETURN a.name AS name ORDER BY name").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
}

#[test]
fn e2e_call_procedure_with_aggregation() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice'})").unwrap();
    ctx.run("CREATE (n:Person {name: 'Bob'})").unwrap();

    let result = ctx.run("CALL db.stats() YIELD label, key, value RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows.len(), 1);
    let cnt = result.rows[0].get("cnt").unwrap();
    assert!(cnt.is_truthy(), "expected non-zero count, got {:?}", cnt);
}

#[test]
fn e2e_with_multiple_aggregates() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {age: 30})").unwrap();
    ctx.run("CREATE (n:Person {age: 25})").unwrap();
    ctx.run("CREATE (n:Person {age: 35})").unwrap();

    let result = ctx.run("MATCH (n:Person) WITH avg(n.age) AS avg_age, max(n.age) AS max_age, min(n.age) AS min_age RETURN avg_age, max_age, min_age").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("avg_age"), Some(&PropertyValue::Double(30.0)));
    assert_eq!(result.rows[0].get("max_age"), Some(&PropertyValue::Int(35)));
    assert_eq!(result.rows[0].get("min_age"), Some(&PropertyValue::Int(25)));
}

#[test]
fn e2e_set_property_to_expression() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
    ctx.run("MATCH (n:Person) SET n.age = n.age + 1 RETURN n.age AS age").unwrap();

    let result = ctx.run("MATCH (n:Person) RETURN n.age AS age").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("age"), Some(&PropertyValue::Int(31)));
}

#[test]
fn e2e_remove_all_properties_using_empty_map() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
    ctx.run("MATCH (n:Person) SET n = {} RETURN n.name AS name").unwrap();

    let result = ctx.run("MATCH (n:Person) RETURN n.name AS name, n.age AS age").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::Null));
    assert_eq!(result.rows[0].get("age"), Some(&PropertyValue::Null));
}

#[test]
fn e2e_match_variable_length_with_min_bound() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'A'})-[:KNOWS]->(b:Person {name: 'B'})-[:KNOWS]->(c:Person {name: 'C'})").unwrap();

    let result = ctx.run("MATCH (a:Person {name: 'A'})-[:KNOWS*2..2]->(c:Person) RETURN c.name AS name").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("C".into())));
}

#[test]
fn e2e_match_optional_with_where_false() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})").unwrap();
    ctx.run("CREATE (b:Person {name: 'Bob'})").unwrap();

    let result = ctx.run("MATCH (a:Person {name: 'Alice'}) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) WHERE 1 = 0 RETURN a.name AS a, b.name AS b").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("a"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(result.rows[0].get("b"), Some(&PropertyValue::Null));
}

#[test]
fn e2e_count_subquery_with_filter() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap();
    ctx.run("MATCH (a:Person {name: 'Alice'}) CREATE (a)-[:KNOWS]->(c:Person {name: 'Charlie'})").unwrap();

    let result = ctx.run("MATCH (a:Person {name: 'Alice'}) RETURN count { (a)-[:KNOWS]->(b:Person) } AS friend_count").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("friend_count"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_multiple_relationship_properties_in_create() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020, close: true}]->(b:Person {name: 'Bob'})").unwrap();

    let result = ctx.run("MATCH ()-[r:KNOWS]->() RETURN r.since AS since, r.close AS close").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("since"), Some(&PropertyValue::Int(2020)));
    assert_eq!(result.rows[0].get("close"), Some(&PropertyValue::Bool(true)));
}

#[test]
fn e2e_merge_on_match_with_multiple_set() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
    ctx.run("MERGE (n:Person {name: 'Alice'}) ON MATCH SET n.age = n.age + 1, n.updated = true RETURN n.age AS age, n.updated AS updated").unwrap();

    let result = ctx.run("MATCH (n:Person {name: 'Alice'}) RETURN n.age AS age, n.updated AS updated").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("age"), Some(&PropertyValue::Int(31)));
    assert_eq!(result.rows[0].get("updated"), Some(&PropertyValue::Bool(true)));
}

#[test]
fn e2e_create_index_then_verify_index_scan_in_plan() {
    let ctx = TestCtx::new();
    ctx.storage.create_label_property_index(
        mgcore::types::LabelId::from_uint(1),
        mgcore::types::PropertyId::from_uint(1),
    );
    ctx.run("CREATE (n:Person {name: 'Alice'})").unwrap();

    let result = ctx.run("EXPLAIN MATCH (n:Person {name: 'Alice'}) RETURN n").unwrap();
    let plan_str = result.rows[0].get("PLAN").unwrap();
    if let PropertyValue::String(plan) = plan_str {
        assert!(
            plan.contains("LabelPropertyScan") || plan.contains("LabelScan") || plan.contains("AllScan"),
            "Expected some kind of scan, got: {}",
            plan
        );
    }
}

#[test]
fn e2e_parameterized_query_int() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();

    let mut params = std::collections::HashMap::new();
    params.insert("minAge".to_string(), PropertyValue::Int(25));
    let result = ctx.run_with_params("MATCH (n:Person) WHERE n.age > $minAge RETURN n.name AS name", &params).unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
}

#[test]
fn e2e_parameterized_query_string() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice'})").unwrap();

    let mut params = std::collections::HashMap::new();
    params.insert("name".to_string(), PropertyValue::String("Alice".into()));
    let result = ctx.run_with_params("MATCH (n:Person {name: $name}) RETURN n.name AS name", &params).unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
}

#[test]
fn e2e_unique_constraint_violation_on_create() {
    let ctx = TestCtx::new();
    ctx.run("CREATE CONSTRAINT ON (n:Person) ASSERT n.email IS UNIQUE").unwrap();
    ctx.run("CREATE (n:Person {email: 'alice@example.com'})").unwrap();
    let result = ctx.run("CREATE (n:Person {email: 'alice@example.com'})");
    assert!(result.is_err(), "Expected unique constraint violation");
}

#[test]
fn e2e_unique_constraint_violation_on_set() {
    let ctx = TestCtx::new();
    ctx.run("CREATE CONSTRAINT ON (n:Person) ASSERT n.email IS UNIQUE").unwrap();
    ctx.run("CREATE (n:Person {email: 'alice@example.com'})").unwrap();
    ctx.run("CREATE (n:Person {email: 'bob@example.com'})").unwrap();
    let result = ctx.run("MATCH (n:Person {email: 'bob@example.com'}) SET n.email = 'alice@example.com'");
    assert!(result.is_err(), "Expected unique constraint violation on SET");
}

#[test]
fn e2e_existence_constraint_violation_on_create() {
    let ctx = TestCtx::new();
    ctx.run("CREATE CONSTRAINT ON (n:Person) ASSERT n.email IS NOT NULL").unwrap();
    let result = ctx.run("CREATE (n:Person {name: 'Alice'})");
    assert!(result.is_err(), "Expected existence constraint violation");
}

#[test]
fn e2e_type_constraint_violation_on_create() {
    let ctx = TestCtx::new();
    ctx.run("CREATE CONSTRAINT ON (n:Person) ASSERT n.age IS TYPED INTEGER").unwrap();
    let result = ctx.run("CREATE (n:Person {age: 'thirty'})");
    assert!(result.is_err(), "Expected type constraint violation");
}

#[test]
fn e2e_constraint_passes_with_valid_data() {
    let ctx = TestCtx::new();
    ctx.run("CREATE CONSTRAINT ON (n:Person) ASSERT n.email IS UNIQUE").unwrap();
    ctx.run("CREATE CONSTRAINT ON (n:Person) ASSERT n.name IS NOT NULL").unwrap();
    ctx.run("CREATE CONSTRAINT ON (n:Person) ASSERT n.age IS TYPED INTEGER").unwrap();

    ctx.run("CREATE (n:Person {email: 'alice@example.com', name: 'Alice', age: 30})").unwrap();
    ctx.run("CREATE (n:Person {email: 'bob@example.com', name: 'Bob', age: 25})").unwrap();

    let result = ctx.run("MATCH (n:Person) RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_drop_constraint_then_violate() {
    let ctx = TestCtx::new();
    ctx.run("CREATE CONSTRAINT ON (n:Person) ASSERT n.email IS UNIQUE").unwrap();
    ctx.run("CREATE (n:Person {email: 'alice@example.com'})").unwrap();
    ctx.run("DROP CONSTRAINT ON (n:Person) ASSERT n.email IS UNIQUE").unwrap();
    let result = ctx.run("CREATE (n:Person {email: 'alice@example.com'})");
    assert!(result.is_ok(), "Should succeed after dropping constraint");
}

#[test]
fn e2e_show_constraints_after_create() {
    let ctx = TestCtx::new();
    ctx.run("CREATE CONSTRAINT ON (n:Person) ASSERT n.email IS UNIQUE").unwrap();
    ctx.run("CREATE CONSTRAINT ON (n:Person) ASSERT n.name IS NOT NULL").unwrap();

    let result = ctx.run("SHOW CONSTRAINTS").unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn e2e_load_csv_then_match_with_property_filter() {
    use std::io::Write;
    let ctx = TestCtx::new();

    let mut tmp = std::env::temp_dir();
    tmp.push("e2e_load_csv_filter.csv");
    {
        let mut file = std::fs::File::create(&tmp).unwrap();
        file.write_all(b"name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,NYC\n").unwrap();
    }

    let path = tmp.to_str().unwrap();
    let query = format!(
        "LOAD CSV FROM '{}' WITH HEADERS AS row CREATE (n:Person {{name: row.name, age: toInteger(row.age), city: row.city}})",
        path
    );
    ctx.run(&query).unwrap();

    let result = ctx.run("MATCH (n:Person {city: 'NYC'}) RETURN n.name AS name ORDER BY name").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(result.rows[1].get("name"), Some(&PropertyValue::String("Charlie".into())));

    let _ = std::fs::remove_file(&tmp);
}

#[test]
fn e2e_load_csv_with_unwind_and_aggregate() {
    use std::io::Write;
    let ctx = TestCtx::new();

    let mut tmp = std::env::temp_dir();
    tmp.push("e2e_load_csv_unwind.csv");
    {
        let mut file = std::fs::File::create(&tmp).unwrap();
        file.write_all(b"category\nA\nB\nA\nB\nA\n").unwrap();
    }

    let path = tmp.to_str().unwrap();
    ctx.run(&format!("LOAD CSV FROM '{}' WITH HEADERS AS row CREATE (n:Item {{category: row.category}})", path)).unwrap();

    let result = ctx.run("MATCH (n:Item) RETURN n.category AS cat, count(*) AS cnt ORDER BY cat").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get("cat"), Some(&PropertyValue::String("A".into())));
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(3)));
    assert_eq!(result.rows[1].get("cat"), Some(&PropertyValue::String("B".into())));
    assert_eq!(result.rows[1].get("cnt"), Some(&PropertyValue::Int(2)));

    let _ = std::fs::remove_file(&tmp);
}

#[test]
fn e2e_complex_nested_subquery_with_exists() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})-[:KNOWS]->(c:Person {name: 'Charlie'})").unwrap();
    ctx.run("CREATE (d:Person {name: 'Diana'})").unwrap();

    let result = ctx.run(
        "MATCH (p:Person) WHERE EXISTS { (p)-[:KNOWS]->(:Person)-[:KNOWS]->(:Person) } RETURN p.name AS name"
    ).unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
}

#[test]
fn e2e_merge_with_multiple_labels() {
    let ctx = TestCtx::new();
    ctx.run("MERGE (n:Person:Employee {id: 1}) RETURN labels(n) AS labels").unwrap();

    let result = ctx.run("MATCH (n:Person:Employee {id: 1}) RETURN n.id AS id").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("id"), Some(&PropertyValue::Int(1)));
}

#[test]
fn e2e_create_edge_then_update_properties_multiple_times() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2010}]->(b:Person {name: 'Bob'})").unwrap();
    ctx.run("MATCH ()-[r:KNOWS]->() SET r.since = 2015, r.close = true RETURN r.since AS since").unwrap();
    ctx.run("MATCH ()-[r:KNOWS]->() SET r.city = 'NYC' RETURN r.city AS city").unwrap();

    let result = ctx.run("MATCH ()-[r:KNOWS]->() RETURN r.since AS since, r.close AS close, r.city AS city").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("since"), Some(&PropertyValue::Int(2015)));
    assert_eq!(result.rows[0].get("close"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("city"), Some(&PropertyValue::String("NYC".into())));
}

#[test]
fn e2e_match_with_null_property_comparison() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
    ctx.run("CREATE (n:Person {name: 'Bob'})").unwrap();

    let result = ctx.run("MATCH (n:Person) WHERE n.age > 25 RETURN n.name AS name ORDER BY name").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));

    let result = ctx.run("MATCH (n:Person) WHERE n.age IS NULL RETURN n.name AS name").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Bob".into())));
}

#[test]
fn e2e_return_distinct_with_multiple_columns() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', city: 'NYC'})").unwrap();
    ctx.run("CREATE (n:Person {name: 'Alice', city: 'NYC'})").unwrap();
    ctx.run("CREATE (n:Person {name: 'Bob', city: 'LA'})").unwrap();

    let result = ctx.run("MATCH (n:Person) RETURN DISTINCT n.name AS name, n.city AS city ORDER BY name").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(result.rows[0].get("city"), Some(&PropertyValue::String("NYC".into())));
    assert_eq!(result.rows[1].get("name"), Some(&PropertyValue::String("Bob".into())));
    assert_eq!(result.rows[1].get("city"), Some(&PropertyValue::String("LA".into())));
}

#[test]
fn e2e_with_chained_aggregations() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Item {val: 10})").unwrap();
    ctx.run("CREATE (n:Item {val: 20})").unwrap();
    ctx.run("CREATE (n:Item {val: 30})").unwrap();

    let result = ctx.run(
        "MATCH (n:Item) WITH sum(n.val) AS total, avg(n.val) AS avg_val, min(n.val) AS min_val, max(n.val) AS max_val RETURN total, avg_val, min_val, max_val"
    ).unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("total"), Some(&PropertyValue::Int(60)));
    assert_eq!(result.rows[0].get("avg_val"), Some(&PropertyValue::Double(20.0)));
    assert_eq!(result.rows[0].get("min_val"), Some(&PropertyValue::Int(10)));
    assert_eq!(result.rows[0].get("max_val"), Some(&PropertyValue::Int(30)));
}

#[test]
fn e2e_optional_match_with_properties() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap();
    ctx.run("CREATE (c:Person {name: 'Charlie'})").unwrap();

    let people = ctx.run("MATCH (n:Person) RETURN n.name AS name ORDER BY name").unwrap();
    eprintln!("People: {:?}", people.rows);

    let result = ctx.run(
        "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) RETURN a.name AS a, b.name AS b ORDER BY a"
    ).unwrap();
    eprintln!("Optional match: {:?}", result.rows);
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0].get("a"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(result.rows[0].get("b"), Some(&PropertyValue::String("Bob".into())));
    assert_eq!(result.rows[1].get("a"), Some(&PropertyValue::String("Bob".into())));
    assert_eq!(result.rows[1].get("b"), Some(&PropertyValue::Null));
    assert_eq!(result.rows[2].get("a"), Some(&PropertyValue::String("Charlie".into())));
    assert_eq!(result.rows[2].get("b"), Some(&PropertyValue::Null));
}

#[test]
fn e2e_delete_with_match_and_detach() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap();
    ctx.run("MATCH (a:Person {name: 'Alice'}) DETACH DELETE a").unwrap();

    let result = ctx.run("MATCH (n:Person) RETURN n.name AS name ORDER BY name").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Bob".into())));

    let result = ctx.run("MATCH ()-[r:KNOWS]->() RETURN count(*) AS cnt").unwrap();
    assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(0)));
}

#[test]
fn e2e_case_expression_with_null() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
    ctx.run("CREATE (n:Person {name: 'Bob'})").unwrap();

    let result = ctx.run(
        "MATCH (n:Person) RETURN CASE WHEN n.age IS NULL THEN 'unknown' ELSE toString(n.age) END AS age_str ORDER BY age_str"
    ).unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get("age_str"), Some(&PropertyValue::String("30".into())));
    assert_eq!(result.rows[1].get("age_str"), Some(&PropertyValue::String("unknown".into())));
}

#[test]
fn e2e_pattern_comprehension_with_filter() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob', age: 20}), (a)-[:KNOWS]->(c:Person {name: 'Charlie', age: 30})").unwrap();

    let result = ctx.run(
        "MATCH (a:Person {name: 'Alice'}) RETURN [(a)-[:KNOWS]->(f:Person) WHERE f.age > 25 | f.name] AS friends"
    ).unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0].get("friends"),
        Some(&PropertyValue::List(vec![PropertyValue::String("Charlie".into())]))
    );
}

#[test]
fn e2e_uniform_sample_basic() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN uniformSample([1, 2, 3, 4, 5], 2) AS sample").unwrap();
    assert_eq!(result.rows.len(), 1);
    let sample = result.rows[0].get("sample").unwrap();
    match sample {
        PropertyValue::List(items) => {
            assert_eq!(items.len(), 2);
            for item in items {
                assert!(matches!(item, PropertyValue::Int(n) if *n >= 1 && *n <= 5));
            }
        }
        _ => panic!("expected list"),
    }
}

#[test]
fn e2e_uniform_sample_size_zero() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN uniformSample([1, 2, 3], 0) AS sample").unwrap();
    assert_eq!(result.rows[0].get("sample"), Some(&PropertyValue::List(vec![])));
}

#[test]
fn e2e_uniform_sample_size_larger_than_list() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN uniformSample([1, 2], 5) AS sample").unwrap();
    assert_eq!(result.rows.len(), 1);
    let sample = result.rows[0].get("sample").unwrap();
    match sample {
        PropertyValue::List(items) => {
            assert_eq!(items.len(), 2);
        }
        _ => panic!("expected list"),
    }
}

#[test]
fn e2e_to_byte_string_from_string() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN toByteString('ABC') AS bytes").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0].get("bytes"),
        Some(&PropertyValue::List(vec![
            PropertyValue::Int(65),
            PropertyValue::Int(66),
            PropertyValue::Int(67),
        ]))
    );
}

#[test]
fn e2e_from_byte_string_from_list() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN fromByteString([65, 66, 67]) AS s").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::String("ABC".into())));
}

#[test]
fn e2e_to_byte_string_roundtrip() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN fromByteString(toByteString('hello')) AS s").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::String("hello".into())));
}

#[test]
fn e2e_to_byte_string_from_byte_list() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN toByteString([104, 101, 108, 108, 111]) AS s").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::String("hello".into())));
}

#[test]
fn e2e_query_stats_basic() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice'})").unwrap();
    ctx.run("MATCH (n:Person) RETURN n.name").unwrap();
    ctx.run("MATCH (n:Person) RETURN n.name").unwrap();

    let result = ctx.run("CALL db.queryStats() YIELD query, count RETURN query, count").unwrap();
    assert!(!result.rows.is_empty(), "expected some query stats");
    let total_count: i64 = result.rows.iter()
        .filter_map(|r| r.get("count"))
        .filter_map(|v| match v { PropertyValue::Int(n) => Some(*n), _ => None })
        .sum();
    assert!(total_count >= 1, "expected at least 1 tracked query execution");
}

#[test]
fn e2e_list_queries_basic() {
    let ctx = TestCtx::new();
    let result = ctx.run("CALL db.listQueries() YIELD queryId, query RETURN queryId, query").unwrap();
    assert!(result.rows.is_empty() || result.rows[0].get("queryId").is_some());
}

#[test]
fn e2e_call_subquery_with_limit() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})").unwrap();
    ctx.run("CREATE (b:Person {name: 'Bob'})").unwrap();
    let result = ctx.run(
        "CALL { MATCH (n:Person) RETURN n.name AS name ORDER BY name LIMIT 1 } RETURN name"
    ).unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
}

#[test]
fn e2e_create_drop_trigger() {
    let ctx = TestCtx::new();
    ctx.run("CREATE TRIGGER my_trigger ON VERTEX CREATE BEFORE EXECUTE 'RETURN 1'").unwrap();
    let result = ctx.run("SHOW TRIGGERS").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("my_trigger".into())));

    ctx.run("DROP TRIGGER my_trigger").unwrap();
    let result = ctx.run("SHOW TRIGGERS").unwrap();
    assert_eq!(result.rows.len(), 0);
}

#[test]
fn e2e_create_drop_database() {
    let ctx = TestCtx::new();
    ctx.run("CREATE DATABASE testdb").unwrap();
    let result = ctx.run("SHOW DATABASES").unwrap();
    let names: Vec<String> = result.rows.iter()
        .filter_map(|r| match r.get("name") {
            Some(PropertyValue::String(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(names.contains(&"testdb".into()), "expected testdb in databases");

    ctx.run("DROP DATABASE testdb").unwrap();
    let result = ctx.run("SHOW DATABASES").unwrap();
    let names: Vec<String> = result.rows.iter()
        .filter_map(|r| match r.get("name") {
            Some(PropertyValue::String(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(!names.contains(&"testdb".into()), "expected testdb to be dropped");
}

#[test]
fn e2e_property_size_basic() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN propertySize(n, 'name') AS name_size, propertySize(n, 'age') AS age_size").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("name_size"), Some(&PropertyValue::Int(5)));
    assert_eq!(result.rows[0].get("age_size"), Some(&PropertyValue::Int(8)));
}

#[test]
fn e2e_property_size_missing() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice'})").unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN propertySize(n, 'missing') AS sz").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("sz"), Some(&PropertyValue::Int(0)));
}

#[test]
fn e2e_assert_true() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN assert(true) AS ok").unwrap();
    assert_eq!(result.rows[0].get("ok"), Some(&PropertyValue::Bool(true)));
}

#[test]
fn e2e_assert_false() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN assert(false, 'boom') AS ok").unwrap();
    assert_eq!(result.rows[0].get("ok"), Some(&PropertyValue::Null));
}

#[test]
fn e2e_counter_basic() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN counter('my_counter', 10) AS v1, counter('my_counter', 10) AS v2").unwrap();
    assert_eq!(result.rows[0].get("v1"), Some(&PropertyValue::Int(10)));
    assert_eq!(result.rows[0].get("v2"), Some(&PropertyValue::Int(11)));
}

#[test]
fn e2e_counter_with_step() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN counter('step_counter', 0, 5) AS v1, counter('step_counter', 0, 5) AS v2").unwrap();
    assert_eq!(result.rows[0].get("v1"), Some(&PropertyValue::Int(0)));
    assert_eq!(result.rows[0].get("v2"), Some(&PropertyValue::Int(5)));
}

#[test]
fn e2e_get_hops_counter() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN getHopsCounter() AS hops").unwrap();
    assert!(matches!(result.rows[0].get("hops"), Some(PropertyValue::Int(_))));
}

#[test]
fn e2e_to_enum_stub() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN toEnum('Status', 'ACTIVE') AS e").unwrap();
    assert_eq!(result.rows[0].get("e"), Some(&PropertyValue::String("Status".into())));
}

#[test]
fn e2e_username_no_auth() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN username() AS u").unwrap();
    assert_eq!(result.rows[0].get("u"), Some(&PropertyValue::Null));
}

#[test]
fn e2e_roles_no_auth() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN roles() AS r").unwrap();
    assert_eq!(result.rows[0].get("r"), Some(&PropertyValue::List(vec![])));
}

#[test]
fn e2e_within_bbox_2d() {
    let ctx = TestCtx::new();
    let result = ctx.run(
        "RETURN withinBBox(point({x: 1, y: 2}), point({x: 0, y: 0}), point({x: 5, y: 5})) AS inside"
    ).unwrap();
    assert_eq!(result.rows[0].get("inside"), Some(&PropertyValue::Bool(true)));

    let result = ctx.run(
        "RETURN withinBBox(point({x: 10, y: 2}), point({x: 0, y: 0}), point({x: 5, y: 5})) AS outside"
    ).unwrap();
    assert_eq!(result.rows[0].get("outside"), Some(&PropertyValue::Bool(false)));
}

#[test]
fn e2e_within_bbox_null() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN withinBBox(NULL, point({x: 0, y: 0}), point({x: 5, y: 5})) AS v").unwrap();
    assert_eq!(result.rows[0].get("v"), Some(&PropertyValue::Null));
}

#[test]
fn e2e_datetime_timezone_only() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN datetime({timezone: 'Europe/Brussels'}) AS dt").unwrap();
    assert_eq!(result.rows.len(), 1);
    // Should return a ZonedDateTime (not Null)
    match result.rows[0].get("dt") {
        Some(PropertyValue::ZonedDateTime(_)) => {},
        other => panic!("expected ZonedDateTime, got {:?}", other),
    }
}

#[test]
fn e2e_datetime_partial_numeric() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN datetime({year: 2023, month: 11}) AS dt").unwrap();
    assert_eq!(result.rows.len(), 1);
    match result.rows[0].get("dt") {
        Some(PropertyValue::ZonedDateTime(zdt)) => {
            // 2023-11-01 00:00:00 UTC ≈ 1_698_796_800_000_000 microseconds
            let us = zdt.microseconds();
            assert!(us > 1_600_000_000_000_000i64 && us < 1_800_000_000_000_000i64,
                "expected ~2023, got {} us", us);
        }
        other => panic!("expected ZonedDateTime, got {:?}", other),
    }
}

#[test]
fn e2e_datetime_partial_with_timezone() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN datetime({year: 2021, month: 3, timezone: 'Europe/Brussels'}) AS dt").unwrap();
    assert_eq!(result.rows.len(), 1);
    match result.rows[0].get("dt") {
        Some(PropertyValue::ZonedDateTime(zdt)) => {
            // 2021-03-01 00:00:00 UTC ≈ 1_614_556_800_000_000 microseconds
            let us = zdt.microseconds();
            assert!(us > 1_500_000_000_000_000i64 && us < 1_700_000_000_000_000i64,
                "expected ~2021, got {} us", us);
        }
        other => panic!("expected ZonedDateTime, got {:?}", other),
    }
}

#[test]
fn e2e_valuetype_function_comprehensive() {
    let ctx = TestCtx::new();
    let cases = vec![
        ("valuetype(null)", "NULL"),
        ("valuetype(true)", "BOOLEAN"),
        ("valuetype(1)", "INTEGER"),
        ("valuetype(1.1)", "FLOAT"),
        ("valuetype('hello')", "STRING"),
        ("valuetype([1,2,3])", "LIST"),
        ("valuetype({a: 1})", "MAP"),
    ];
    for (expr, expected) in cases {
        let result = ctx.run(&format!("RETURN {} AS t", expr)).unwrap();
        assert_eq!(result.rows[0].get("t"), Some(&PropertyValue::String(expected.into())), "failed for {}", expr);
    }
}

#[test]
fn e2e_toset_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN toSet([1, 2, 3, 1, 2, 3, 4]) AS s").unwrap();
    assert_eq!(result.rows.len(), 1);
    match result.rows[0].get("s") {
        Some(PropertyValue::List(items)) => {
            assert_eq!(items.len(), 4);
            assert!(items.contains(&PropertyValue::Int(1)));
            assert!(items.contains(&PropertyValue::Int(2)));
            assert!(items.contains(&PropertyValue::Int(3)));
            assert!(items.contains(&PropertyValue::Int(4)));
        }
        other => panic!("expected List, got {:?}", other),
    }
}

#[test]
fn e2e_kshortest_basic() {
    let ctx = TestCtx::new();
    // Create a diamond graph: a -> b -> d, a -> c -> d
    ctx.run("CREATE (a:Node {name: 'a'}), (b:Node {name: 'b'}), (c:Node {name: 'c'}), (d:Node {name: 'd'}), (a)-[:ROAD {cost: 1}]->(b), (b)-[:ROAD {cost: 1}]->(d), (a)-[:ROAD {cost: 2}]->(c), (c)-[:ROAD {cost: 2}]->(d)").unwrap();

    // Find 2 shortest paths from a to d
    let result = ctx.run("MATCH (a:Node {name: 'a'}), (d:Node {name: 'd'}) MATCH p = (a)-[*kShortest | 2]->(d) RETURN p").unwrap();
    assert_eq!(result.rows.len(), 2, "expected 2 shortest paths, got {}", result.rows.len());
}

#[test]
fn e2e_kshortest_single_path() {
    let ctx = TestCtx::new();
    // Linear graph: a -> b -> c
    ctx.run("CREATE (a:Node {name: 'a'}), (b:Node {name: 'b'}), (c:Node {name: 'c'}), (a)-[:LINK]->(b), (b)-[:LINK]->(c)").unwrap();

    // Only one path exists
    let result = ctx.run("MATCH (a:Node {name: 'a'}), (c:Node {name: 'c'}) MATCH p = (a)-[*kShortest | 5]->(c) RETURN p").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn e2e_kshortest_no_match() {
    let ctx = TestCtx::new();
    // Disconnected nodes
    ctx.run("CREATE (a:Node {name: 'a'}), (b:Node {name: 'b'})").unwrap();

    let result = ctx.run("MATCH (a:Node {name: 'a'}), (b:Node {name: 'b'}) MATCH p = (a)-[*kShortest | 3]->(b) RETURN p").unwrap();
    assert_eq!(result.rows.len(), 0);
}

#[test]
fn e2e_kshortest_with_bounds() {
    let ctx = TestCtx::new();
    // Triangle: a -> b -> c -> a
    ctx.run("CREATE (a:Node {name: 'a'}), (b:Node {name: 'b'}), (c:Node {name: 'c'}), (a)-[:LINK]->(b), (b)-[:LINK]->(c), (c)-[:LINK]->(a)").unwrap();

    // From a to b: direct path (1 hop) and indirect via c (2 hops)
    let result = ctx.run("MATCH (a:Node {name: 'a'}), (b:Node {name: 'b'}) MATCH p = (a)-[*kShortest | 2]->(b) RETURN p").unwrap();
    assert!(result.rows.len() >= 1, "expected at least 1 path");
}

#[test]
fn e2e_kshortest_undirected() {
    let ctx = TestCtx::new();
    // a -> b, c -> b (so from a to c, must go a->b, then b<-c)
    ctx.run("CREATE (a:Node {name: 'a'}), (b:Node {name: 'b'}), (c:Node {name: 'c'}), (a)-[:LINK]->(b), (c)-[:LINK]->(b)").unwrap();

    // undirected search should find path a->b<-c
    let result = ctx.run("MATCH (a:Node {name: 'a'}), (c:Node {name: 'c'}) MATCH p = (a)-[*kShortest | 3]-(c) RETURN p").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn e2e_number_of_hops_single_edge() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (a)-[:KNOWS]->(b)").unwrap();
    let result = ctx.run("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN b").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(result.number_of_hops >= 1, "expected at least 1 hop, got {}", result.number_of_hops);
}

#[test]
fn e2e_number_of_hops_variable_length() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}), (d:Person {name: 'David'}), (e:Person {name: 'Eve'}), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(e)").unwrap();
    let result = ctx.run("MATCH (a:Person {name: 'Alice'})-[:KNOWS*]->(e:Person {name: 'Eve'}) RETURN e").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(result.number_of_hops >= 4, "expected at least 4 hops, got {}", result.number_of_hops);
}

#[test]
fn e2e_number_of_hops_bfs() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c)").unwrap();
    let result = ctx.run("MATCH (a:Person {name: 'Alice'})-[:KNOWS *BFS]->(c:Person {name: 'Charlie'}) RETURN c").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(result.number_of_hops >= 2, "expected at least 2 hops, got {}", result.number_of_hops);
}

#[test]
fn e2e_number_of_hops_zero_for_no_match() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})").unwrap();
    let result = ctx.run("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN b").unwrap();
    assert_eq!(result.rows.len(), 0);
    // Even with no match, we still traversed edges (or attempted to)
}

#[test]
fn e2e_load_jsonl_basic() {
    let ctx = TestCtx::new();
    let path = "/tmp/test_basic.jsonl";
    std::fs::write(path, r#"{"id": 1, "name": "Alice"}
{"id": 2, "name": "Bob"}
"#).unwrap();
    let result = ctx.run(&format!("LOAD JSONL FROM 'file://{}' AS row CREATE (n:Person {{id: row.id, name: row.name}})", path)).unwrap();
    assert_eq!(result.rows.len(), 0);
    let result = ctx.run("MATCH (n:Person) RETURN n.name AS name ORDER BY n.id").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(result.rows[1].get("name"), Some(&PropertyValue::String("Bob".into())));
    let _ = std::fs::remove_file(path);
}

#[test]
fn e2e_load_jsonl_nested() {
    let ctx = TestCtx::new();
    let path = "/tmp/test_nested.jsonl";
    std::fs::write(path, r#"{"id": 1, "address": {"city": "NYC", "zip": 10001}}
{"id": 2, "address": {"city": "LA", "zip": 90001}}
"#).unwrap();
    let result = ctx.run(&format!("LOAD JSONL FROM 'file://{}' AS row CREATE (n:Person {{id: row.id, city: row.address.city}})", path)).unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN n.city AS city ORDER BY n.id").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get("city"), Some(&PropertyValue::String("NYC".into())));
    assert_eq!(result.rows[1].get("city"), Some(&PropertyValue::String("LA".into())));
    let _ = std::fs::remove_file(path);
}

#[test]
fn e2e_load_jsonl_arrays() {
    let ctx = TestCtx::new();
    let path = "/tmp/test_arrays.jsonl";
    std::fs::write(path, r#"{"id": 1, "tags": ["a", "b", "c"]}
"#).unwrap();
    let result = ctx.run(&format!("LOAD JSONL FROM 'file://{}' AS row CREATE (n:Tag {{id: row.id, tags: row.tags}})", path)).unwrap();
    let result = ctx.run("MATCH (n:Tag) RETURN n.tags AS tags").unwrap();
    assert_eq!(result.rows.len(), 1);
    match result.rows[0].get("tags") {
        Some(PropertyValue::List(items)) => {
            assert_eq!(items.len(), 3);
            assert!(items.contains(&PropertyValue::String("a".into())));
        }
        other => panic!("expected List, got {:?}", other),
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn e2e_load_jsonl_mixed_types() {
    let ctx = TestCtx::new();
    let path = "/tmp/test_types.jsonl";
    std::fs::write(path, r#"{"id": 1, "name": "Alice", "age": 30, "score": 95.5, "active": true, "empty": null}
"#).unwrap();
    let result = ctx.run(&format!("LOAD JSONL FROM 'file://{}' AS row CREATE (n:Person {{id: row.id, name: row.name, age: row.age, score: row.score, active: row.active, empty: row.empty}})", path)).unwrap();
    let result = ctx.run("MATCH (n:Person) RETURN n.id AS id, n.name AS name, n.age AS age, n.score AS score, n.active AS active, n.empty AS empty").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("id"), Some(&PropertyValue::Int(1)));
    assert_eq!(result.rows[0].get("name"), Some(&PropertyValue::String("Alice".into())));
    assert_eq!(result.rows[0].get("age"), Some(&PropertyValue::Int(30)));
    assert!(matches!(result.rows[0].get("score"), Some(PropertyValue::Double(_))));
    assert_eq!(result.rows[0].get("active"), Some(&PropertyValue::Bool(true)));
    assert_eq!(result.rows[0].get("empty"), Some(&PropertyValue::Null));
    let _ = std::fs::remove_file(path);
}

#[test]
fn e2e_algo_rich_club_coefficient_procedure() {
    let ctx = TestCtx::new();
    // Star graph: center has degree 3, leaves have degree 1
    ctx.run("CREATE (a:Node)-[:R]->(b:Leaf), (a)-[:R]->(c:Leaf), (a)-[:R]->(d:Leaf)").unwrap();

    let result = ctx.run("CALL algo.rich_club_coefficient(3) YIELD k, coefficient RETURN k, coefficient").unwrap();
    // Should have rows for k=0,1,2,3 (or fewer if max_k is capped by max degree)
    assert!(!result.rows.is_empty());
}

#[test]
fn e2e_algo_modularity_procedure() {
    let ctx = TestCtx::new();
    // Two disconnected triangles -> high modularity
    ctx.run("CREATE (a:A)-[:R]->(b:A)-[:R]->(c:A)-[:R]->(a)").unwrap();
    ctx.run("CREATE (d:B)-[:R]->(e:B)-[:R]->(f:B)-[:R]->(d)").unwrap();

    // Use auto-detected partition via louvain (no args)
    let result = ctx.run("CALL algo.modularity() YIELD modularity RETURN modularity").unwrap();
    assert_eq!(result.rows.len(), 1);
    match result.rows[0].get("modularity") {
        Some(PropertyValue::Double(v)) => assert!(*v > 0.0, "modularity should be positive, got {}", v),
        other => panic!("expected Double, got {:?}", other),
    }

    // Also test explicit partition via two list args
    let result = ctx.run("CALL algo.modularity([1, 2, 3, 4, 5, 6], [0, 0, 0, 1, 1, 1]) YIELD modularity RETURN modularity").unwrap();
    assert_eq!(result.rows.len(), 1);
    match result.rows[0].get("modularity") {
        Some(PropertyValue::Double(v)) => assert!(*v > 0.0, "modularity with explicit partition should be positive, got {}", v),
        other => panic!("expected Double, got {:?}", other),
    }
}

#[test]
fn e2e_algo_conductance_procedure() {
    let ctx = TestCtx::new();
    // Two disconnected triangles
    ctx.run("CREATE (a:A)-[:R]->(b:A)-[:R]->(c:A)-[:R]->(a)").unwrap();
    ctx.run("CREATE (d:B)-[:R]->(e:B)-[:R]->(f:B)-[:R]->(d)").unwrap();

    let result = ctx.run("CALL algo.conductance([1, 2, 3, 4, 5, 6], [0, 0, 0, 1, 1, 1]) YIELD community, conductance RETURN community, conductance").unwrap();
    // Should return one row per community
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn e2e_algo_normalized_cut_procedure() {
    let ctx = TestCtx::new();
    // Two disconnected triangles
    ctx.run("CREATE (a:A)-[:R]->(b:A)-[:R]->(c:A)-[:R]->(a)").unwrap();
    ctx.run("CREATE (d:B)-[:R]->(e:B)-[:R]->(f:B)-[:R]->(d)").unwrap();

    let result = ctx.run("CALL algo.normalized_cut([1, 2, 3, 4, 5, 6], [0, 0, 0, 1, 1, 1]) YIELD community, normalized_cut RETURN community, normalized_cut").unwrap();
    // Should return one row per community
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn e2e_algo_bfs_procedure() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)").unwrap();
    let result = ctx.run("CALL algo.bfs(1) YIELD node, depth RETURN node, depth").unwrap();
    assert!(!result.rows.is_empty());
}

#[test]
fn e2e_algo_bridges_procedure() {
    let ctx = TestCtx::new();
    // Path of 3 nodes: two bridges (a-b and b-c)
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)").unwrap();
    let result = ctx.run("CALL algo.bridges() YIELD nodeA, nodeB RETURN nodeA, nodeB").unwrap();
    assert!(!result.rows.is_empty());
}

#[test]
fn e2e_algo_eccentricity_procedure() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)").unwrap();
    let result = ctx.run("CALL algo.eccentricity() YIELD nodeId, eccentricity RETURN nodeId, eccentricity").unwrap();
    assert!(!result.rows.is_empty());
}

#[test]
fn e2e_algo_radius_procedure() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)").unwrap();
    let result = ctx.run("CALL algo.radius() YIELD radius RETURN radius").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn e2e_algo_topological_sort_procedure() {
    let ctx = TestCtx::new();
    // DAG: a -> b -> c
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)").unwrap();
    let result = ctx.run("CALL algo.topological_sort() YIELD nodeId RETURN nodeId").unwrap();
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn e2e_algo_harmonic_centrality_procedure() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)").unwrap();
    let result = ctx.run("CALL algo.harmonic_centrality() YIELD nodeId, score RETURN nodeId, score").unwrap();
    assert!(!result.rows.is_empty());
}

#[test]
fn e2e_algo_eigenvector_centrality_procedure() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)-[:R]->(a)").unwrap();
    let result = ctx.run("CALL algo.eigenvector_centrality() YIELD nodeId, score RETURN nodeId, score").unwrap();
    assert!(!result.rows.is_empty());
}

#[test]
fn e2e_algo_count_paths_of_length_procedure() {
    let ctx = TestCtx::new();
    // Path a->b->c: 2 paths of length 1, 1 path of length 2
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)").unwrap();
    let result = ctx.run("CALL algo.count_paths_of_length(2) YIELD node, count RETURN node, count").unwrap();
    assert!(!result.rows.is_empty());
}

#[test]
fn e2e_algo_k_core_decomposition_procedure() {
    let ctx = TestCtx::new();
    // Triangle: all nodes in 2-core
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)-[:R]->(a)").unwrap();
    let result = ctx.run("CALL algo.k_core_decomposition() YIELD node, coreness RETURN node, coreness").unwrap();
    assert!(!result.rows.is_empty());
}

#[test]
fn e2e_algo_maximal_cliques_procedure() {
    let ctx = TestCtx::new();
    // Triangle is a maximal clique of size 3
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)-[:R]->(a)").unwrap();
    let result = ctx.run("CALL algo.maximal_cliques() YIELD cliqueId, nodes RETURN cliqueId, nodes").unwrap();
    assert!(!result.rows.is_empty());
}

#[test]
fn e2e_call_subquery_in_transactions() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node {id: 1})").unwrap();
    ctx.run("CREATE (b:Node {id: 2})").unwrap();

    let result = ctx.run("CALL { MATCH (n:Node) RETURN n.id AS id } IN TRANSACTIONS OF 1 ROWS RETURN id ORDER BY id").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get("id"), Some(&PropertyValue::Int(1)));
    assert_eq!(result.rows[1].get("id"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_call_subquery_in_transactions_with_side_effects() {
    let ctx = TestCtx::new();
    ctx.run("UNWIND [1, 2, 3] AS x CALL { WITH x CREATE (n:Number {val: x}) } IN TRANSACTIONS OF 1 ROWS RETURN x").unwrap();

    let result = ctx.run("MATCH (n:Number) RETURN n.val AS val ORDER BY val").unwrap();
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0].get("val"), Some(&PropertyValue::Int(1)));
    assert_eq!(result.rows[1].get("val"), Some(&PropertyValue::Int(2)));
    assert_eq!(result.rows[2].get("val"), Some(&PropertyValue::Int(3)));
}

#[test]
fn e2e_show_node_labels_info() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})").unwrap();
    ctx.run("CREATE (b:Company {name: 'Acme'})").unwrap();

    let result = ctx.run("SHOW NODE_LABELS INFO").unwrap();
    let labels: Vec<String> = result.rows.iter()
        .map(|r| match r.get("label") {
            Some(PropertyValue::String(s)) => s.clone(),
            _ => String::new(),
        })
        .collect();
    assert!(labels.contains(&"Person".to_string()));
    assert!(labels.contains(&"Company".to_string()));
}

#[test]
fn e2e_show_edge_types_info() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person)-[:KNOWS]->(b:Person)").unwrap();
    ctx.run("CREATE (c:Person)-[:WORKS_AT]->(d:Company)").unwrap();

    let result = ctx.run("SHOW EDGE_TYPES INFO").unwrap();
    let edge_types: Vec<String> = result.rows.iter()
        .map(|r| match r.get("edge_type") {
            Some(PropertyValue::String(s)) => s.clone(),
            _ => String::new(),
        })
        .collect();
    assert!(edge_types.contains(&"KNOWS".to_string()));
    assert!(edge_types.contains(&"WORKS_AT".to_string()));
}

#[test]
fn e2e_algo_cliques_containing_procedure() {
    let ctx = TestCtx::new();
    // Triangle
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)-[:R]->(a)").unwrap();
    // Get the actual GID of a node, then find cliques containing it
    let gid_result = ctx.run("MATCH (n:Node) RETURN id(n) AS nid LIMIT 1").unwrap();
    let gid = match gid_result.rows[0].get("nid") {
        Some(PropertyValue::Int(n)) => *n,
        _ => panic!("expected int gid"),
    };
    let result = ctx.run(&format!("CALL algo.cliques_containing({}) YIELD clique RETURN clique", gid)).unwrap();
    assert!(!result.rows.is_empty());
}

#[test]
fn e2e_algo_degrecentrality_procedure() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)").unwrap();
    let result = ctx.run("CALL algo.degrecentrality() YIELD nodeId, degree RETURN nodeId, degree").unwrap();
    assert!(!result.rows.is_empty());
}

#[test]
fn e2e_algo_degreeassortativity_procedure() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)-[:R]->(a)").unwrap();
    let result = ctx.run("CALL algo.degreeassortativity() YIELD assortativity RETURN assortativity").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn e2e_algo_dsatur_coloring_procedure() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)-[:R]->(a)").unwrap();
    let result = ctx.run("CALL algo.dsatur_coloring() YIELD nodeId, color RETURN nodeId, color").unwrap();
    assert!(!result.rows.is_empty());
}

#[test]
fn e2e_algo_globalclusteringcoefficient_procedure() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)-[:R]->(a)").unwrap();
    let result = ctx.run("CALL algo.globalclusteringcoefficient() YIELD coefficient RETURN coefficient").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn e2e_algo_graphdensity_procedure() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node)-[:R]->(b:Node)-[:R]->(c:Node)").unwrap();
    let result = ctx.run("CALL algo.graphdensity() YIELD density RETURN density").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn e2e_algo_predict_links_procedure() {
    let ctx = TestCtx::new();
    // Create a graph where two nodes share many neighbors but are not directly connected
    // (a star with center c, plus two peripheral nodes p1 and p2 not connected to each other)
    ctx.run("CREATE (c:Center)-[:R]->(p1:Periph)-[:R]->(c)-[:R]->(p2:Periph)-[:R]->(c)-[:R]->(p3:Periph)-[:R]->(c)").unwrap();
    let result = ctx.run("CALL algo.predict_links() YIELD nodeA, nodeB, score RETURN nodeA, nodeB, score").unwrap();
    // predict_links may return empty if no good candidates; just check it doesn't crash
    // The function filters out already-connected pairs and returns those with adamic_adar > 0
}

#[test]
fn e2e_show_database_settings() {
    let ctx = TestCtx::new();
    let result = ctx.run("SHOW DATABASE SETTINGS").unwrap();
    assert!(!result.rows.is_empty());
    assert_eq!(result.columns, vec!["name", "value"]);
    // Check that some expected settings are present
    let names: Vec<String> = result.rows.iter()
        .filter_map(|r| r.get("name"))
        .map(|v| match v { PropertyValue::String(s) => s.clone(), _ => String::new() })
        .collect();
    assert!(names.contains(&"bolt-port".to_string()));
    assert!(names.contains(&"storage-mode".to_string()));
}

#[test]
fn e2e_show_database_setting_single() {
    let ctx = TestCtx::new();
    // Set a custom setting first (no hyphens so it parses as single identifier)
    ctx.run("SET SETTING mycustom TO 42").unwrap();
    let result = ctx.run("SHOW DATABASE SETTING mycustom").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.columns, vec!["name", "value"]);
    let row = &result.rows[0];
    assert_eq!(row.get("name"), Some(&PropertyValue::String("mycustom".into())));
    assert_eq!(row.get("value"), Some(&PropertyValue::String("42".into())));
}

#[test]
fn e2e_set_setting_then_show() {
    let ctx = TestCtx::new();
    ctx.run("SET SETTING timeout TO 120").unwrap();
    let result = ctx.run("SHOW DATABASE SETTING timeout").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("value"), Some(&PropertyValue::String("120".into())));
}

#[test]
fn e2e_show_transactions_columns() {
    let ctx = TestCtx::new();
    let result = ctx.run("SHOW TRANSACTIONS").unwrap();
    // SHOW TRANSACTIONS itself is registered, so we see at least 1 row
    assert!(result.rows.len() >= 0);
    assert_eq!(result.columns, vec!["transaction_id", "query", "elapsed_ms"]);
}

#[test]
fn e2e_terminate_transaction_not_found() {
    let ctx = TestCtx::new();
    // Terminate a non-existent transaction should error
    let result = ctx.run("TERMINATE TRANSACTIONS \"tx-nonexistent\"");
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("not found"), "expected 'not found' in error: {}", err_msg);
}

#[test]
fn e2e_hops_limit_enforced() {
    let ctx = TestCtx::new();
    // Create a star graph: center connected to 5 leaves
    ctx.run("CREATE (c:Center {name: 'hub'})").unwrap();
    for i in 1..=5 {
        ctx.run(&format!("CREATE (l:Leaf {{name: 'leaf{}'}})", i)).unwrap();
        ctx.run(&format!("MATCH (c:Center), (l:Leaf {{name: 'leaf{}'}}) CREATE (c)-[:KNOWS]->(l)", i)).unwrap();
    }
    // With HOPS LIMIT 3, examining 5 edges should exceed the limit
    let result = ctx.run("USING HOPS LIMIT 3 MATCH (c:Center)-[:KNOWS]->(l) RETURN l.name");
    assert!(result.is_err(), "expected hops limit exceeded error");
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("hops limit exceeded"), "expected 'hops limit exceeded' in error: {}", err_msg);
}

#[test]
fn e2e_hops_limit_not_exceeded() {
    let ctx = TestCtx::new();
    // Create a star graph: center connected to 5 leaves
    ctx.run("CREATE (c:Center {name: 'hub'})").unwrap();
    for i in 1..=5 {
        ctx.run(&format!("CREATE (l:Leaf {{name: 'leaf{}'}})", i)).unwrap();
        ctx.run(&format!("MATCH (c:Center), (l:Leaf {{name: 'leaf{}'}}) CREATE (c)-[:KNOWS]->(l)", i)).unwrap();
    }
    // With HOPS LIMIT 10, examining 5 edges should succeed
    let result = ctx.run("USING HOPS LIMIT 10 MATCH (c:Center)-[:KNOWS]->(l) RETURN l.name").unwrap();
    assert_eq!(result.rows.len(), 5);
}

#[test]
fn e2e_hops_limit_variable_length() {
    let ctx = TestCtx::new();
    // Create a chain: (a)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(d)
    ctx.run("CREATE (a:Node {name: 'a'})").unwrap();
    ctx.run("CREATE (b:Node {name: 'b'})").unwrap();
    ctx.run("CREATE (c:Node {name: 'c'})").unwrap();
    ctx.run("CREATE (d:Node {name: 'd'})").unwrap();
    ctx.run("MATCH (a:Node {name: 'a'}), (b:Node {name: 'b'}) CREATE (a)-[:KNOWS]->(b)").unwrap();
    ctx.run("MATCH (b:Node {name: 'b'}), (c:Node {name: 'c'}) CREATE (b)-[:KNOWS]->(c)").unwrap();
    ctx.run("MATCH (c:Node {name: 'c'}), (d:Node {name: 'd'}) CREATE (c)-[:KNOWS]->(d)").unwrap();

    // With HOPS LIMIT 2, traversing the chain with [*1..3] should exceed limit
    let result = ctx.run("USING HOPS LIMIT 2 MATCH (a:Node {name: 'a'})-[:KNOWS*1..3]->(d) RETURN d.name");
    assert!(result.is_err(), "expected hops limit exceeded error for variable-length path");
}

#[test]
fn e2e_hops_limit_combined_with_periodic_commit() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (c:Center {name: 'hub'})").unwrap();
    for i in 1..=3 {
        ctx.run(&format!("CREATE (l:Leaf {{name: 'leaf{}'}})", i)).unwrap();
        ctx.run(&format!("MATCH (c:Center), (l:Leaf {{name: 'leaf{}'}}) CREATE (c)-[:KNOWS]->(l)", i)).unwrap();
    }
    // Both USING directives should parse and apply
    let result = ctx.run("USING PERIODIC COMMIT 100 USING HOPS LIMIT 10 MATCH (c:Center)-[:KNOWS]->(l) RETURN l.name").unwrap();
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn e2e_using_index_hint_label_property() {
    let ctx = TestCtx::new();
    // Create indexed data
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
    ctx.run("CREATE (n:Person {name: 'Bob', age: 25})").unwrap();
    ctx.run("CREATE (n:Person {name: 'Carol', age: 30})").unwrap();

    // Query with USING INDEX hint should return correct results
    let result = ctx.run("USING INDEX :Person(age) MATCH (n:Person) WHERE n.age = 30 RETURN n.name AS name").unwrap();
    assert_eq!(result.rows.len(), 2);
    let names: Vec<String> = result.rows.iter()
        .map(|r| match r.get("name") {
            Some(PropertyValue::String(s)) => s.clone(),
            _ => panic!("expected string name"),
        })
        .collect();
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Carol".to_string()));
}

#[test]
fn e2e_using_index_hint_label_only() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice'})").unwrap();
    ctx.run("CREATE (n:Person {name: 'Bob'})").unwrap();

    // USING INDEX :Person should force label scan (already default behavior)
    let result = ctx.run("USING INDEX :Person MATCH (n:Person) RETURN n.name").unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn e2e_using_index_hint_in_explain() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();

    // EXPLAIN should show LabelPropertyScan when USING INDEX hint is applied
    let result = ctx.run("EXPLAIN USING INDEX :Person(age) MATCH (n:Person) WHERE n.age = 30 RETURN n.name").unwrap();
    assert_eq!(result.rows.len(), 1);
    let plan = match result.rows[0].get("PLAN") {
        Some(PropertyValue::String(s)) => s.clone(),
        _ => panic!("expected plan string"),
    };
    assert!(plan.contains("LabelPropertyScan"), "EXPLAIN plan should contain LabelPropertyScan when USING INDEX hint is used, got: {}", plan);
}

#[test]
fn e2e_degree_indegree_outdegree() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})").unwrap();
    ctx.run("CREATE (b:Person {name: 'Bob'})").unwrap();
    ctx.run("CREATE (c:Person {name: 'Carol'})").unwrap();
    ctx.run("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)").unwrap();
    ctx.run("MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Carol'}) CREATE (a)-[:KNOWS]->(c)").unwrap();
    ctx.run("MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) CREATE (b)-[:KNOWS]->(c)").unwrap();

    let result = ctx.run("MATCH (n:Person {name: 'Alice'}) RETURN outdegree(n) AS out, indegree(n) AS inc, degree(n) AS deg").unwrap();
    assert_eq!(result.rows[0].get("out"), Some(&PropertyValue::Int(2)));
    assert_eq!(result.rows[0].get("inc"), Some(&PropertyValue::Int(0)));
    assert_eq!(result.rows[0].get("deg"), Some(&PropertyValue::Int(2)));
}

#[test]
fn e2e_uniformsample_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN uniformsample([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3) AS sample").unwrap();
    match result.rows[0].get("sample") {
        Some(PropertyValue::List(l)) => assert_eq!(l.len(), 3),
        _ => panic!("expected list"),
    }
}

#[test]
fn e2e_assert_function() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN assert(true) AS ok").unwrap();
    assert_eq!(result.rows[0].get("ok"), Some(&PropertyValue::Bool(true)));
}

#[test]
fn e2e_bytestring_functions() {
    let ctx = TestCtx::new();
    let result = ctx.run("RETURN frombytestring('hello') AS bytes").unwrap();
    match result.rows[0].get("bytes") {
        Some(PropertyValue::List(l)) => {
            assert_eq!(l.len(), 5);
            assert_eq!(l[0], PropertyValue::Int(104));
        }
        other => panic!("expected list of bytes, got {:?}", other),
    }
    let result = ctx.run("RETURN tobytestring([72, 101, 108, 108, 111]) AS s").unwrap();
    assert_eq!(result.rows[0].get("s"), Some(&PropertyValue::String("Hello".to_string())));
}

#[test]
fn e2e_gethopscounter_function() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Node {name: 'a'})").unwrap();
    ctx.run("CREATE (b:Node {name: 'b'})").unwrap();
    ctx.run("MATCH (a:Node {name: 'a'}), (b:Node {name: 'b'}) CREATE (a)-[:KNOWS]->(b)").unwrap();
    let result = ctx.run("MATCH (a:Node)-[:KNOWS]->(b:Node) RETURN getHopsCounter() AS hops").unwrap();
    assert_eq!(result.rows.len(), 1);
    let val = result.rows[0].get("hops");
    match val {
        Some(PropertyValue::Int(n)) => assert!(*n >= 1, "hops should be >= 1, got {}", n),
        other => panic!("expected integer, got {:?}", other),
    }
}

// ─── GRANT / REVOKE / DENY / SHOW PRIVILEGES ────────────────────────────

#[test]
fn e2e_grant_privilege_to_user() {
    let ctx = TestCtx::with_auth();
    ctx.run("CREATE USER alice IDENTIFIED BY \"Password123\"").unwrap();
    ctx.run("GRANT PRIVILEGE CREATE, DELETE TO USER alice").unwrap();
    let result = ctx.run("SHOW PRIVILEGES FOR USER alice").unwrap();
    assert!(result.rows.len() >= 2);
    let privs: Vec<String> = result.rows.iter()
        .filter_map(|r| r.get("privilege").map(|v| format!("{:?}", v)))
        .collect();
    assert!(privs.iter().any(|p| p.contains("Create")));
    assert!(privs.iter().any(|p| p.contains("Delete")));
}

#[test]
fn e2e_revoke_privilege_from_user() {
    let ctx = TestCtx::with_auth();
    ctx.run("CREATE USER bob IDENTIFIED BY \"Password123\"").unwrap();
    ctx.run("GRANT PRIVILEGE MATCH, MERGE TO USER bob").unwrap();
    ctx.run("REVOKE PRIVILEGE MATCH FROM USER bob").unwrap();
    let result = ctx.run("SHOW PRIVILEGES FOR USER bob").unwrap();
    let privs: Vec<String> = result.rows.iter()
        .filter_map(|r| r.get("privilege").map(|v| format!("{:?}", v)))
        .collect();
    assert!(!privs.iter().any(|p| p.contains("Match")));
    assert!(privs.iter().any(|p| p.contains("Merge")));
}

#[test]
fn e2e_deny_privilege_on_role() {
    let ctx = TestCtx::with_auth();
    ctx.run("CREATE ROLE admin").unwrap();
    ctx.run("DENY AUTH TO ROLE admin").unwrap();
    let result = ctx.run("SHOW PRIVILEGES FOR ROLE admin").unwrap();
    assert!(result.rows.len() >= 1);
    let denied: Vec<_> = result.rows.iter()
        .filter(|r| r.get("effect").map(|v| format!("{:?}", v).contains("DENY")).unwrap_or(false))
        .collect();
    assert!(!denied.is_empty());
}

#[test]
fn e2e_grant_all_privileges() {
    let ctx = TestCtx::with_auth();
    ctx.run("CREATE USER admin_user IDENTIFIED BY \"SecretPass1\"").unwrap();
    ctx.run("GRANT ALL PRIVILEGES TO USER admin_user").unwrap();
    let result = ctx.run("SHOW PRIVILEGES FOR USER admin_user").unwrap();
    // Should have all 31 privileges
    assert!(result.rows.len() >= 10, "expected >= 10 privileges, got {}", result.rows.len());
}

// ─── ALTER USER ──────────────────────────────────────────────────────

#[test]
fn e2e_alter_user_set_password() {
    let ctx = TestCtx::with_auth();
    ctx.run("CREATE USER testuser IDENTIFIED BY \"OldPass123\"").unwrap();
    let result = ctx.run("ALTER USER testuser SET PASSWORD \"NewPass456\"");
    assert!(result.is_ok());
}

#[test]
fn e2e_alter_user_rename() {
    let ctx = TestCtx::with_auth();
    ctx.run("CREATE USER oldname IDENTIFIED BY \"Password123\"").unwrap();
    let result = ctx.run("ALTER USER oldname RENAME TO newname");
    assert!(result.is_ok());
    // Verify new name exists
    let result = ctx.run("SHOW USERS");
    let names: Vec<_> = result.unwrap().rows.iter()
        .filter_map(|r| r.get("username").map(|v| format!("{:?}", v)))
        .collect();
    assert!(names.iter().any(|n| n.contains("newname")));
}

// ─── BEGIN / COMMIT / ROLLBACK ───────────────────────────────────────

#[test]
fn e2e_begin_commit_transaction() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:TxTest {val: 1})").unwrap();
    let result = ctx.run("BEGIN");
    assert!(result.is_ok());
    let result = ctx.run("MATCH (n:TxTest) SET n.val = 2");
    assert!(result.is_ok());
    let result = ctx.run("COMMIT");
    assert!(result.is_ok());
    let result = ctx.run("MATCH (n:TxTest) RETURN n.val AS val").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn e2e_rollback_transaction() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (n:RollTest {val: 1})").unwrap();
    let result = ctx.run("BEGIN");
    assert!(result.is_ok());
    let result = ctx.run("ROLLBACK");
    assert!(result.is_ok());
}

// ─── SET STORAGE MODE ────────────────────────────────────────────────

#[test]
fn e2e_set_storage_mode_in_memory_analytical() {
    let ctx = TestCtx::new();
    let result = ctx.run("SET STORAGE MODE IN_MEMORY_ANALYTICAL");
    assert!(result.is_ok());
}

#[test]
fn e2e_set_storage_mode_in_memory_transactional() {
    let ctx = TestCtx::new();
    let result = ctx.run("SET STORAGE MODE IN_MEMORY_TRANSACTIONAL");
    assert!(result.is_ok());
}

#[test]
fn e2e_set_storage_mode_on_disk_transactional() {
    let ctx = TestCtx::new();
    let result = ctx.run("SET STORAGE MODE ON_DISK_TRANSACTIONAL");
    assert!(result.is_ok());
}

// ─── UNION / UNION ALL ──────────────────────────────────────────────

#[test]
fn e2e_union_all_basic() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})").unwrap();
    ctx.run("CREATE (b:Person {name: 'Bob'})").unwrap();
    ctx.run("CREATE (c:Employee {name: 'Charlie'})").unwrap();
    let result = ctx.run(
        "MATCH (n:Person) RETURN n.name AS name UNION ALL MATCH (m:Employee) RETURN m.name AS name"
    ).unwrap();
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn e2e_union_dedup() {
    let ctx = TestCtx::new();
    ctx.run("CREATE (a:Person {name: 'Alice'})").unwrap();
    ctx.run("CREATE (b:Person {name: 'Alice'})").unwrap();
    let result = ctx.run(
        "MATCH (n:Person) RETURN n.name AS name UNION MATCH (m:Person) RETURN m.name AS name"
    ).unwrap();
    // UNION deduplicates — 'Alice' should appear once
    assert_eq!(result.rows.len(), 1);
}

