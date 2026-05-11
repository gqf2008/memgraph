//! Physical plan executor for mginterp.
//!
//! Executes a `mgplanner::PhysicalPlan` tree against the storage engine,
//! producing a `QueryResult`. This bridges the query optimizer's output
//! with the storage layer.

use std::collections::HashMap;
use std::sync::Arc;

use mgcore::delta::IsolationLevel;
use mgcore::property_value::{EdgeRefValue, PropertyValue, VertexRef};
use mgparser::ast::{Direction, Expression};
use mgplanner::{PhysicalOp, PhysicalPlan};
use mgstorage::storage::Storage;
use mgstorage::transaction::Transaction;

use crate::{
    eval_expression_with_storage, set_active_transaction, ExecError, QueryResult, ResultRow,
};

/// Execute a physical plan and return the query result.
pub fn execute_physical_plan(
    storage: &Storage,
    plan: &PhysicalPlan,
) -> Result<QueryResult, ExecError> {
    let (tx, _guard) = match crate::active_transaction() {
        Some(existing) => (existing, None),
        None => {
            let new_tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
            let guard = set_active_transaction(Some(new_tx.clone()));
            (new_tx, Some(guard))
        }
    };

    execute_op(storage, &plan.op, &tx)
}

fn execute_op(
    storage: &Storage,
    op: &PhysicalOp,
    tx: &Arc<Transaction>,
) -> Result<QueryResult, ExecError> {
    match op {
        PhysicalOp::Produce { items, child } => {
            let child_rows = execute_plan_rows(storage, &child.op, tx)?;
            let mut rows = Vec::new();
            let columns: Vec<String>;
            if items.is_empty() {
                // RETURN * — return all bound variables
                let mut all_keys: Vec<String> = Vec::new();
                for row in &child_rows {
                    for key in row.keys() {
                        if !all_keys.contains(key) && !key.starts_with("__") {
                            all_keys.push(key.clone());
                        }
                    }
                }
                all_keys.sort();
                columns = all_keys.clone();
                for row in child_rows {
                    let mut result_row = ResultRow::new();
                    for key in &columns {
                        if let Some(val) = row.get(key) {
                            result_row.insert(key.clone(), val.clone());
                        }
                    }
                    rows.push(result_row);
                }
            } else {
                columns = items
                    .iter()
                    .enumerate()
                    .map(|(i, item)| {
                        item.alias
                            .clone()
                            .or_else(|| {
                                if let Expression::Identifier(name) = &item.expression {
                                    Some(name.clone())
                                } else {
                                    None
                                }
                            })
                            .unwrap_or_else(|| format!("column_{}", i))
                    })
                    .collect();
                for row in child_rows {
                    let mut result_row = ResultRow::new();
                    for (i, item) in items.iter().enumerate() {
                        let val = eval_expression_with_storage(&item.expression, &row, Some(storage));
                        result_row.insert(columns[i].clone(), val);
                    }
                    rows.push(result_row);
                }
            }

            Ok(QueryResult {
                columns,
                rows,
                number_of_hops: crate::reset_hops(),
            })
        }
        PhysicalOp::HashAggregate {
            group_by,
            aggregates,
            child: _,
        }
        | PhysicalOp::StreamingAggregate {
            group_by,
            aggregates,
            child: _,
        }
        | PhysicalOp::Aggregate {
            group_by,
            aggregates,
            child: _,
        } => {
            let rows = execute_plan_rows(storage, op, tx)?;
            let mut columns = Vec::new();
            for (i, (_expr, alias)) in group_by.iter().enumerate() {
                let col = alias.clone().unwrap_or_else(|| format!("column_{}", i));
                columns.push(col);
            }
            for (i, (_expr, alias)) in aggregates.iter().enumerate() {
                let col = alias.clone().unwrap_or_else(|| format!("column_{}", group_by.len() + i));
                columns.push(col);
            }
            Ok(QueryResult {
                columns,
                rows,
                number_of_hops: crate::reset_hops(),
            })
        }
        _ => {
            let rows = execute_plan_rows(storage, op, tx)?;
            Ok(QueryResult {
                columns: vec![],
                rows,
                number_of_hops: 0,
            })
        }
    }
}

fn execute_plan_rows(
    storage: &Storage,
    op: &PhysicalOp,
    tx: &Arc<Transaction>,
) -> Result<Vec<ResultRow>, ExecError> {
    match op {
        PhysicalOp::SeqScan { alias, label } => {
            let gids = if let Some(label) = label {
                storage.vertices_by_label(*label)
            } else {
                storage.all_vertex_gids()
            };
            let mut rows = Vec::new();
            for gid in gids {
                crate::check_query_timeout()?;
                if let Some(v) = storage.get_vertex(gid, tx) {
                    let mut row = ResultRow::new();
                    let alias_name = alias.clone().unwrap_or_else(|| "_".to_string());
                    row.insert(
                        alias_name,
                        PropertyValue::Vertex(VertexRef::new(v.gid, v.labels, v.properties)),
                    );
                    rows.push(row);
                }
            }
            Ok(rows)
        }
        PhysicalOp::IndexSeek {
            alias,
            label,
            property,
            value,
        } => {
            let gids = storage.vertices_by_label_property(*label, *property, value);
            let mut rows = Vec::new();
            for gid in gids {
                crate::check_query_timeout()?;
                if let Some(v) = storage.get_vertex(gid, tx) {
                    let mut row = ResultRow::new();
                    let alias_name = alias.clone().unwrap_or_else(|| "_".to_string());
                    row.insert(
                        alias_name,
                        PropertyValue::Vertex(VertexRef::new(v.gid, v.labels, v.properties)),
                    );
                    rows.push(row);
                }
            }
            Ok(rows)
        }
        PhysicalOp::EdgeTypeScan { alias, edge_type } => {
            let gids = storage.edges_by_type(*edge_type);
            let mut rows = Vec::new();
            for gid in gids {
                crate::check_query_timeout()?;
                if let Some(e) = storage.get_edge(gid, tx) {
                    let mut row = ResultRow::new();
                    let alias_name = alias.clone().unwrap_or_else(|| "_".to_string());
                    row.insert(
                        alias_name,
                        PropertyValue::Edge(EdgeRefValue::new(
                            e.gid,
                            e.edge_type,
                            e.from_vertex,
                            e.to_vertex,
                            e.properties,
                        )),
                    );
                    rows.push(row);
                }
            }
            Ok(rows)
        }
        PhysicalOp::EdgeTypePropertyScan {
            alias,
            edge_type,
            property,
            value,
        } => {
            let gids = storage.edges_by_type_property_value(*edge_type, *property, value);
            let mut rows = Vec::new();
            for gid in gids {
                crate::check_query_timeout()?;
                if let Some(e) = storage.get_edge(gid, tx) {
                    let mut row = ResultRow::new();
                    let alias_name = alias.clone().unwrap_or_else(|| "_".to_string());
                    row.insert(
                        alias_name,
                        PropertyValue::Edge(EdgeRefValue::new(
                            e.gid,
                            e.edge_type,
                            e.from_vertex,
                            e.to_vertex,
                            e.properties,
                        )),
                    );
                    rows.push(row);
                }
            }
            Ok(rows)
        }
        PhysicalOp::Filter { condition, child } => {
            let child_rows = execute_plan_rows(storage, &child.op, tx)?;
            let mut rows = Vec::new();
            for row in child_rows {
                crate::check_query_timeout()?;
                let val = eval_expression_with_storage(condition, &row, Some(storage));
                if val.is_truthy() {
                    rows.push(row);
                }
            }
            Ok(rows)
        }
        PhysicalOp::Project { expressions, child } => {
            let child_rows = execute_plan_rows(storage, &child.op, tx)?;
            let mut rows = Vec::new();
            for row in child_rows {
                crate::check_query_timeout()?;
                let mut new_row = ResultRow::new();
                for (i, expr) in expressions.iter().enumerate() {
                    let val = eval_expression_with_storage(expr, &row, Some(storage));
                    new_row.insert(format!("col_{}", i), val);
                }
                rows.push(new_row);
            }
            Ok(rows)
        }
        PhysicalOp::Sort { key, child } => {
            let mut child_rows = execute_plan_rows(storage, &child.op, tx)?;

            crate::check_query_timeout()?;

            // Fast path: single sort key avoids Vec allocation per row.
            if key.len() == 1 {
                let item = &key[0];
                let mut keyed: Vec<(ResultRow, PropertyValue)> = child_rows
                    .drain(..)
                    .map(|row| {
                        let k = eval_expression_with_storage(&item.expression, &row, Some(storage));
                        (row, k)
                    })
                    .collect();

                keyed.sort_unstable_by(|(_, a), (_, b)| {
                    match crate::compare(a, b) {
                        std::cmp::Ordering::Equal => std::cmp::Ordering::Equal,
                        ord => {
                            if item.ascending {
                                ord
                            } else {
                                ord.reverse()
                            }
                        }
                    }
                });

                return Ok(keyed.into_iter().map(|(row, _)| row).collect());
            }

            let mut keyed: Vec<_> = child_rows
                .drain(..)
                .map(|row| {
                    let keys: Vec<PropertyValue> = key
                        .iter()
                        .map(|o| eval_expression_with_storage(&o.expression, &row, Some(storage)))
                        .collect();
                    (row, keys)
                })
                .collect();

            keyed.sort_unstable_by(|(_, keys_a), (_, keys_b)| {
                for (i, item) in key.iter().enumerate() {
                    match crate::compare(&keys_a[i], &keys_b[i]) {
                        std::cmp::Ordering::Equal => continue,
                        ord => return if item.ascending { ord } else { ord.reverse() },
                    }
                }
                std::cmp::Ordering::Equal
            });

            Ok(keyed.into_iter().map(|(row, _)| row).collect())
        }
        PhysicalOp::Limit { count, child } => {
            let mut child_rows = execute_plan_rows(storage, &child.op, tx)?;
            child_rows.truncate(*count);
            Ok(child_rows)
        }
        PhysicalOp::TopN { key, count, child } => {
            let mut child_rows = execute_plan_rows(storage, &child.op, tx)?;
            let mut keyed: Vec<_> = child_rows
                .drain(..)
                .map(|row| {
                    let keys: Vec<PropertyValue> = key
                        .iter()
                        .map(|o| eval_expression_with_storage(&o.expression, &row, Some(storage)))
                        .collect();
                    (row, keys)
                })
                .collect();

            crate::check_query_timeout()?;

            let compare_keys = |keys_a: &Vec<PropertyValue>, keys_b: &Vec<PropertyValue>| {
                for (i, item) in key.iter().enumerate() {
                    match crate::compare(&keys_a[i], &keys_b[i]) {
                        std::cmp::Ordering::Equal => continue,
                        ord => return if item.ascending { ord } else { ord.reverse() },
                    }
                }
                std::cmp::Ordering::Equal
            };

            if *count < keyed.len() {
                let k = *count;
                keyed.select_nth_unstable_by(k, |(_, a), (_, b)| compare_keys(a, b));
                keyed[..k].sort_by(|(_, a), (_, b)| compare_keys(a, b));
                keyed.truncate(k);
            } else {
                keyed.sort_by(|(_, a), (_, b)| compare_keys(a, b));
            }

            Ok(keyed.into_iter().map(|(row, _)| row).collect())
        }
        PhysicalOp::Skip { count, child } => {
            let mut child_rows = execute_plan_rows(storage, &child.op, tx)?;
            if *count < child_rows.len() {
                Ok(child_rows.split_off(*count))
            } else {
                Ok(vec![])
            }
        }
        PhysicalOp::NestedLoopJoin { left, right } => {
            let left_rows = execute_plan_rows(storage, &left.op, tx)?;
            let right_rows = execute_plan_rows(storage, &right.op, tx)?;
            let mut rows = Vec::new();
            for l in left_rows {
                crate::check_query_timeout()?;
                for r in &right_rows {
                    let mut merged = l.clone();
                    let mut conflict = false;
                    for (k, v) in r {
                        if let Some(existing_v) = merged.get(k) {
                            if existing_v != v {
                                conflict = true;
                                break;
                            }
                        }
                        merged.insert(k.clone(), v.clone());
                    }
                    if !conflict {
                        rows.push(merged);
                    }
                }
            }
            Ok(rows)
        }
        PhysicalOp::HashJoin {
            left,
            right,
            left_key,
            right_key,
        } => {
            let left_rows = execute_plan_rows(storage, &left.op, tx)?;
            let right_rows = execute_plan_rows(storage, &right.op, tx)?;

            let mut hash_table: HashMap<String, Vec<ResultRow>> = HashMap::new();
            for row in left_rows {
                let key_val = eval_expression_with_storage(
                    &Expression::Identifier(left_key.clone()),
                    &row,
                    Some(storage),
                );
                let key_str = format!("{:?}", key_val);
                hash_table.entry(key_str).or_default().push(row);
            }

            let mut rows = Vec::new();
            for r in right_rows {
                crate::check_query_timeout()?;
                let key_val = eval_expression_with_storage(
                    &Expression::Identifier(right_key.clone()),
                    &r,
                    Some(storage),
                );
                let key_str = format!("{:?}", key_val);
                if let Some(left_matches) = hash_table.get(&key_str) {
                    for l in left_matches {
                        let mut merged = l.clone();
                        let mut conflict = false;
                        for (k, v) in &r {
                            if let Some(existing_v) = merged.get(k) {
                                if existing_v != v {
                                    conflict = true;
                                    break;
                                }
                            }
                            merged.insert(k.clone(), v.clone());
                        }
                        if !conflict {
                            rows.push(merged);
                        }
                    }
                }
            }
            Ok(rows)
        }
        PhysicalOp::HashAggregate {
            group_by,
            aggregates,
            child,
        }
        | PhysicalOp::StreamingAggregate {
            group_by,
            aggregates,
            child,
        }
        | PhysicalOp::Aggregate {
            group_by,
            aggregates,
            child,
        } => {
            let child_rows = execute_plan_rows(storage, &child.op, tx)?;

            // Empty input: aggregates still produce one row (e.g. count(*) → 0)
            if child_rows.is_empty() {
                let mut result_row = ResultRow::new();
                for (expr, alias) in group_by.iter() {
                    let col = alias.clone().or_else(|| {
                        if let Expression::Identifier(name) = expr {
                            Some(name.clone())
                        } else {
                            None
                        }
                    }).unwrap_or_else(|| format!("group_{}", result_row.len()));
                    result_row.insert(col, PropertyValue::Null);
                }
                for (i, (expr, alias)) in aggregates.iter().enumerate() {
                    let val = crate::eval_aggregate(storage, expr, &[]);
                    let col = alias.clone().unwrap_or_else(|| format!("column_{}", group_by.len() + i));
                    result_row.insert(col, val);
                }
                return Ok(vec![result_row]);
            }

            let mut groups: HashMap<String, (Vec<PropertyValue>, Vec<ResultRow>)> = HashMap::new();

            for row in child_rows {
                crate::check_query_timeout()?;
                let key_vals: Vec<PropertyValue> = group_by
                    .iter()
                    .map(|(expr, _alias)| eval_expression_with_storage(expr, &row, Some(storage)))
                    .collect();
                let key_str = format!("{:?}", key_vals);
                groups
                    .entry(key_str)
                    .or_insert_with(|| (key_vals, Vec::new()))
                    .1
                    .push(row);
            }

            let mut rows = Vec::new();
            for (_key, (key_vals, group_rows)) in groups {
                let mut result_row = ResultRow::new();
                // Insert group-by columns using alias or identifier name
                for ((expr, alias), val) in group_by.iter().zip(key_vals.iter()) {
                    let col = alias.clone().or_else(|| {
                        if let Expression::Identifier(name) = expr {
                            Some(name.clone())
                        } else {
                            None
                        }
                    }).unwrap_or_else(|| format!("group_{}", result_row.len()));
                    result_row.insert(col, val.clone());
                }
                for (i, (expr, alias)) in aggregates.iter().enumerate() {
                    let val = crate::eval_aggregate(storage, expr, &group_rows);
                    let col = alias.clone().unwrap_or_else(|| format!("column_{}", group_by.len() + i));
                    result_row.insert(col, val);
                }
                rows.push(result_row);
            }
            Ok(rows)
        }
        PhysicalOp::Produce { items, child } => {
            let child_rows = execute_plan_rows(storage, &child.op, tx)?;
            let mut rows = Vec::new();
            for row in child_rows {
                let mut result_row = ResultRow::new();
                for item in items {
                    let val = eval_expression_with_storage(&item.expression, &row, Some(storage));
                    let col = item
                        .alias
                        .clone()
                        .or_else(|| {
                            if let Expression::Identifier(name) = &item.expression {
                                Some(name.clone())
                            } else {
                                None
                            }
                        })
                        .unwrap_or_else(|| format!("column_{:?}", item.expression));
                    result_row.insert(col, val);
                }
                rows.push(result_row);
            }
            Ok(rows)
        }
        PhysicalOp::EdgeExpand {
            from_alias,
            edge_alias,
            to_alias,
            direction,
            edge_type,
            child,
        } => {
            let child_rows = execute_plan_rows(storage, &child.op, tx)?;
            let mut rows = Vec::new();
            for row in child_rows {
                crate::check_query_timeout()?;
                let from_val = row.get(from_alias).cloned().unwrap_or(PropertyValue::Null);
                let from_gid = match from_val {
                    PropertyValue::Vertex(vr) => vr.gid,
                    PropertyValue::Int(gid_int) => mgcore::types::Gid::from(gid_int as u64),
                    _ => continue,
                };
                let edge_gids: Vec<mgcore::types::Gid> = match direction {
                    mgparser::ast::Direction::Right => storage.vertex_out_edge_gids(from_gid),
                    mgparser::ast::Direction::Left => storage.vertex_in_edge_gids(from_gid),
                    mgparser::ast::Direction::Either => {
                        let mut gids = storage.vertex_out_edge_gids(from_gid);
                        gids.extend(storage.vertex_in_edge_gids(from_gid));
                        let seen: std::collections::HashSet<_> = gids.iter().copied().collect();
                        seen.into_iter().collect()
                    }
                };
                for edge_gid in &edge_gids {
                    crate::increment_hops(1)?;
                    if let Some(edge) = storage.get_edge(*edge_gid, tx) {
                        if edge_type.map_or(false, |et| edge.edge_type != et) {
                            continue;
                        }
                        let other_gid = if edge.from_vertex == from_gid {
                            edge.to_vertex
                        } else {
                            edge.from_vertex
                        };
                        if let Some(target) = storage.get_vertex(other_gid, tx) {
                            let mut new_row = row.clone();
                            if let Some(ref ea) = edge_alias {
                                new_row.insert(
                                    ea.clone(),
                                    PropertyValue::Edge(EdgeRefValue::new(
                                        edge.gid,
                                        edge.edge_type,
                                        edge.from_vertex,
                                        edge.to_vertex,
                                        edge.properties.clone(),
                                    )),
                                );
                            }
                            if let Some(ref ta) = to_alias {
                                new_row.insert(
                                    ta.clone(),
                                    PropertyValue::Vertex(VertexRef::new(
                                        target.gid,
                                        target.labels.clone(),
                                        target.properties.clone(),
                                    )),
                                );
                            }
                            rows.push(new_row);
                        }
                    }
                }
            }
            Ok(rows)
        }
        PhysicalOp::Distinct { child } => {
            let child_rows = execute_plan_rows(storage, &child.op, tx)?;
            let mut seen = std::collections::HashSet::new();
            let mut rows = Vec::new();
            for row in child_rows {
                crate::check_query_timeout()?;
                // Use the sorted key-value string representation for dedup
                let mut entries: Vec<_> = row.iter().collect();
                entries.sort_by(|a, b| a.0.cmp(b.0));
                let key = format!("{:?}", entries);
                if seen.insert(key) {
                    rows.push(row);
                }
            }
            Ok(rows)
        }
        _ => Err(ExecError::Runtime(format!(
            "unsupported physical operator: {:?}",
            op
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mgcore::property_value::PropertyValue;
    use mgcore::types::PropertyId;
    use mgparser::ast::{Expression, OrderByItem};
    use mgplanner::{PhysicalPlan, PlanCost};

    #[test]
    fn test_topn_basic() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);

        for i in 0..10 {
            let gid = mgcore::types::Gid::from(i as u64);
            let _ = storage.create_vertex(&tx, gid);
            let _ =
                storage.vertex_set_property(&tx, gid, PropertyId::from(0), PropertyValue::Int(i));
        }

        let _guard = set_active_transaction(Some(tx.clone()));

        let rows = execute_plan_rows(
            &storage,
            &PhysicalOp::TopN {
                key: vec![OrderByItem {
                    expression: Expression::Property {
                        object: Box::new(Expression::Identifier("n".to_string())),
                        key: PropertyId::from(0),
                    },
                    ascending: false,
                }],
                count: 3,
                child: Box::new(PhysicalPlan {
                    op: PhysicalOp::SeqScan {
                        alias: Some("n".to_string()),
                        label: None,
                    },
                    cost: PlanCost::default(),
                    cardinality: 10.0,
                }),
            },
            &tx,
        )
        .unwrap();

        assert_eq!(rows.len(), 3);
        let scores: Vec<i64> = rows
            .iter()
            .map(|r| match r.get("n") {
                Some(PropertyValue::Vertex(vr)) => match vr.properties.get(PropertyId::from(0)) {
                    PropertyValue::Int(v) => *v,
                    _ => -1,
                },
                _ => -1,
            })
            .collect();
        assert_eq!(scores, vec![9, 8, 7]);
    }

    #[test]
    fn test_topn_ascending() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);

        for i in 0..5 {
            let gid = mgcore::types::Gid::from(i as u64);
            let _ = storage.create_vertex(&tx, gid);
            let _ = storage.vertex_set_property(
                &tx,
                gid,
                PropertyId::from(0),
                PropertyValue::Int(10 - i),
            );
        }

        let _guard = set_active_transaction(Some(tx.clone()));

        let rows = execute_plan_rows(
            &storage,
            &PhysicalOp::TopN {
                key: vec![OrderByItem {
                    expression: Expression::Property {
                        object: Box::new(Expression::Identifier("n".to_string())),
                        key: PropertyId::from(0),
                    },
                    ascending: true,
                }],
                count: 2,
                child: Box::new(PhysicalPlan {
                    op: PhysicalOp::SeqScan {
                        alias: Some("n".to_string()),
                        label: None,
                    },
                    cost: PlanCost::default(),
                    cardinality: 5.0,
                }),
            },
            &tx,
        )
        .unwrap();

        assert_eq!(rows.len(), 2);
        let scores: Vec<i64> = rows
            .iter()
            .map(|r| match r.get("n") {
                Some(PropertyValue::Vertex(vr)) => match vr.properties.get(PropertyId::from(0)) {
                    PropertyValue::Int(v) => *v,
                    _ => -1,
                },
                _ => -1,
            })
            .collect();
        // Vertex values are 10, 9, 8, 7, 6; ascending top 2 = 6, 7
        assert_eq!(scores, vec![6, 7]);
    }

    #[test]
    fn test_topn_count_greater_than_rows() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);

        for i in 0..3 {
            let gid = mgcore::types::Gid::from(i as u64);
            let _ = storage.create_vertex(&tx, gid);
            let _ =
                storage.vertex_set_property(&tx, gid, PropertyId::from(0), PropertyValue::Int(i));
        }

        let _guard = set_active_transaction(Some(tx.clone()));

        let rows = execute_plan_rows(
            &storage,
            &PhysicalOp::TopN {
                key: vec![OrderByItem {
                    expression: Expression::Property {
                        object: Box::new(Expression::Identifier("n".to_string())),
                        key: PropertyId::from(0),
                    },
                    ascending: true,
                }],
                count: 10,
                child: Box::new(PhysicalPlan {
                    op: PhysicalOp::SeqScan {
                        alias: Some("n".to_string()),
                        label: None,
                    },
                    cost: PlanCost::default(),
                    cardinality: 3.0,
                }),
            },
            &tx,
        )
        .unwrap();

        assert_eq!(rows.len(), 3);
    }
}
