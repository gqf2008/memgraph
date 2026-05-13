# Integration Tests

End-to-end tests exercising the full Memgraph Rust query stack:
parser → semantic analysis → planner → interpreter → storage.

## Run

```bash
# All integration tests
cd /Users/sqb/Documents/GitHub/memgraph/rust
cargo test -p integration-tests

# Specific test
cargo test -p integration-tests e2e_create_and_match_vertex

# With output
cargo test -p integration-tests -- --nocapture
```

## Coverage

| Feature | Tests |
|---|---|
| CREATE vertex / relationship | `e2e_create_and_match_vertex`, `e2e_create_and_match_relationship` |
| MATCH + WHERE | `e2e_match_with_where`, `e2e_match_with_where_and_relationship` |
| Aggregation | `e2e_aggregation_count`, `e2e_aggregation_sum_avg_min_max`, `e2e_aggregation_collect`, `e2e_aggregation_grouping` |
| Variable-length paths | `e2e_variable_length_path`, `e2e_variable_length_unbounded` |
| OPTIONAL MATCH | `e2e_optional_match_found`, `e2e_optional_match_not_found` |
| SET / REMOVE | `e2e_set_and_remove` |
| DELETE / DETACH DELETE | `e2e_delete_vertex`, `e2e_delete_detach_vertex` |
| MERGE | `e2e_merge_existing`, `e2e_merge_new` |
| UNWIND / WITH | `e2e_unwind`, `e2e_with_clause` |
| ORDER BY / SKIP / LIMIT | `e2e_order_by_skip_limit` |
| EXISTS subquery | `e2e_exists_subquery` |
| List predicates | `e2e_list_predicate_all`, `e2e_list_predicate_any`, `e2e_list_predicate_none`, `e2e_list_predicate_single` |
| FOREACH | `e2e_foreach_create`, `e2e_foreach_set` |
| Multi-pattern | `e2e_multi_pattern_shared_variable` |
| Directional edges | `e2e_match_left_directional_edge`, `e2e_match_bidirectional_edge` |
| CASE expression | `e2e_case_expression`, `e2e_case_expression_simple_form` |
| Functions | `e2e_string_functions`, `e2e_list_functions`, `e2e_id_and_labels_functions`, `e2e_type_function`, `e2e_properties_function`, `e2e_keys_function`, `e2e_coalesce_function` |
| Arithmetic | `e2e_arithmetic_expressions` |
| NULL checks | `e2e_is_null` |
