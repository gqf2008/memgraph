//! Expression evaluator.
//!
//! Evaluates an Expression AST node against a binding context (variable → value).

use std::cell::Cell;
use std::collections::HashMap;

use chrono::{Datelike, Timelike};
use mgcatalog::Catalog;
use mgcore::delta::IsolationLevel;
use mgcore::point::{Crs, Point2D, Point3D};
use mgcore::property_value::{EdgeRefValue, PropertyValue, VertexRef};
use mgcore::temporal::{Date, Duration, LocalDateTime, LocalTime, ZonedDateTime};
use mgcore::types::{EdgeTypeId, Gid, PropertyId};
use mgparser::ast::{Expression, QueryMode};
use mgstorage::storage::Storage;

/// Get a transaction for reading: use the active explicit transaction if one is
/// set, otherwise begin a new read transaction.  This ensures that property
/// lookups and graph traversals inside expressions see uncommitted changes when
/// running inside an explicit transaction.
fn get_read_tx(storage: &Storage) -> std::sync::Arc<mgstorage::transaction::Transaction> {
    match crate::active_transaction() {
        Some(tx) => tx,
        None => storage.begin_transaction(IsolationLevel::SnapshotIsolation),
    }
}

fn us_to_naive_datetime(us: i64) -> chrono::NaiveDateTime {
    chrono::DateTime::from_timestamp_micros(us)
        .unwrap_or_default()
        .naive_utc()
}

thread_local! {
    /// Raw pointer to the current catalog. Set before query execution and cleared after.
    /// The pointed-to Catalog must outlive the query execution (it does — the caller holds it).
    static ACTIVE_CATALOG: Cell<*const Catalog> = const { Cell::new(std::ptr::null()) };
}

/// Guard that restores the active catalog on drop (for panic safety and re-entrancy).
pub struct ActiveCatalogGuard {
    prev: *const Catalog,
}

impl Drop for ActiveCatalogGuard {
    fn drop(&mut self) {
        ACTIVE_CATALOG.with(|c| c.set(self.prev));
    }
}

/// Set the active catalog for expression evaluation in the current thread.
/// Returns a guard that restores the previous catalog on drop.
pub fn set_active_catalog(catalog: Option<&Catalog>) -> ActiveCatalogGuard {
    let guard = ActiveCatalogGuard {
        prev: ACTIVE_CATALOG.with(|c| c.get()),
    };
    ACTIVE_CATALOG.with(|c| {
        c.set(
            catalog
                .map(|c| c as *const Catalog)
                .unwrap_or(std::ptr::null()),
        );
    });
    guard
}

pub(crate) fn active_catalog() -> Option<&'static Catalog> {
    ACTIVE_CATALOG.with(|c| {
        let ptr = c.get();
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { &*ptr })
        }
    })
}

/// Evaluate an expression to a PropertyValue.
pub fn eval_expression(
    expr: &Expression,
    bindings: &HashMap<String, PropertyValue>,
) -> PropertyValue {
    eval_expression_with_catalog(expr, bindings, None, None)
}

/// Evaluate an expression with optional storage access (needed for EXISTS subqueries).
/// Automatically picks up any thread-local catalog set by `set_active_catalog`.
pub fn eval_expression_with_storage(
    expr: &Expression,
    bindings: &HashMap<String, PropertyValue>,
    storage: Option<&Storage>,
) -> PropertyValue {
    eval_expression_with_catalog(expr, bindings, storage, active_catalog())
}

/// Evaluate an expression with optional storage and catalog access.
pub fn eval_expression_with_catalog(
    expr: &Expression,
    bindings: &HashMap<String, PropertyValue>,
    storage: Option<&Storage>,
    catalog: Option<&Catalog>,
) -> PropertyValue {
    match expr {
        Expression::Null => PropertyValue::Null,
        Expression::Bool(b) => PropertyValue::Bool(*b),
        Expression::Int(n) => PropertyValue::Int(*n),
        Expression::Double(f) => PropertyValue::Double(*f),
        Expression::String(s) => PropertyValue::String(s.clone()),

        Expression::Identifier(name) => {
            // Look up in bindings
            bindings.get(name).cloned().unwrap_or(PropertyValue::Null)
        }
        Expression::Parameter(name) => bindings.get(name).cloned().unwrap_or(PropertyValue::Null),

        Expression::Property { object, key } => {
            let obj = eval_expression_with_catalog(object, bindings, storage, catalog);
            extract_property(&obj, *key, storage, catalog)
        }

        Expression::Label { object, label } => {
            let obj = eval_expression_with_catalog(object, bindings, storage, catalog);
            match obj {
                PropertyValue::Vertex(vr) => PropertyValue::Bool(vr.labels.contains(label)),
                _ => PropertyValue::Null,
            }
        }

        Expression::List(items) => {
            let vals: Vec<PropertyValue> = items
                .iter()
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog))
                .collect();
            PropertyValue::List(vals)
        }

        Expression::Map(entries) => {
            let vals: Vec<(String, PropertyValue)> = entries
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        eval_expression_with_catalog(v, bindings, storage, catalog),
                    )
                })
                .collect();
            PropertyValue::Map(vals)
        }

        Expression::MapProjection { object, all, extra } => {
            let obj_val = eval_expression_with_catalog(object, bindings, storage, catalog);
            let mut result_map: Vec<(String, PropertyValue)> = Vec::new();
            if *all {
                // Copy all properties from the object
                match &obj_val {
                    PropertyValue::Vertex(vref) => {
                        if let Some(s) = storage {
                            let tx = get_read_tx(s);
                            if let Some(v) = s.get_vertex(vref.gid, &tx) {
                                for (pid, pval) in v.properties.iter() {
                                    if let Some(cat) = catalog {
                                        let name = cat.property_name(pid);
                                        result_map.push((name, pval.clone()));
                                    }
                                }
                            }
                        }
                    }
                    PropertyValue::Map(entries) => {
                        for (k, v) in entries {
                            result_map.push((k.clone(), v.clone()));
                        }
                    }
                    _ => {}
                }
            }
            // Add extra key-value pairs
            for (key, expr) in extra {
                let val = eval_expression_with_catalog(expr, bindings, storage, catalog);
                result_map.push((key.clone(), val));
            }
            PropertyValue::Map(result_map)
        }

        Expression::Index { object, index } => {
            let obj = eval_expression_with_catalog(object, bindings, storage, catalog);
            let idx = eval_expression_with_catalog(index, bindings, storage, catalog);
            match (obj, idx) {
                (PropertyValue::List(list), PropertyValue::Int(i)) => {
                    let usize_idx = if i < 0 {
                        list.len().saturating_sub((-i) as usize)
                    } else {
                        i as usize
                    };
                    list.get(usize_idx).cloned().unwrap_or(PropertyValue::Null)
                }
                (PropertyValue::String(s), PropertyValue::Int(i)) => {
                    let usize_idx = if i < 0 {
                        s.len().saturating_sub((-i) as usize)
                    } else {
                        i as usize
                    };
                    s.chars()
                        .nth(usize_idx)
                        .map(|c| PropertyValue::String(c.to_string()))
                        .unwrap_or(PropertyValue::Null)
                }
                _ => PropertyValue::Null,
            }
        }
        Expression::Slice { object, start, end } => {
            let obj = eval_expression_with_catalog(object, bindings, storage, catalog);
            let start_val = eval_expression_with_catalog(start, bindings, storage, catalog);
            let end_val = eval_expression_with_catalog(end, bindings, storage, catalog);
            match (obj, start_val, end_val) {
                (PropertyValue::List(list), PropertyValue::Int(s), PropertyValue::Int(e)) => {
                    let start_idx = if s < 0 {
                        list.len().saturating_sub((-s) as usize)
                    } else {
                        s as usize
                    };
                    let end_idx = if e < 0 {
                        list.len().saturating_sub((-e) as usize)
                    } else {
                        e as usize
                    };
                    let start_idx = start_idx.min(list.len());
                    let end_idx = end_idx.min(list.len());
                    if start_idx >= end_idx {
                        PropertyValue::List(Vec::new())
                    } else {
                        PropertyValue::List(list[start_idx..end_idx].to_vec())
                    }
                }
                (PropertyValue::String(s), PropertyValue::Int(st), PropertyValue::Int(en)) => {
                    let start_idx = if st < 0 {
                        s.len().saturating_sub((-st) as usize)
                    } else {
                        st as usize
                    };
                    let end_idx = if en < 0 {
                        s.len().saturating_sub((-en) as usize)
                    } else {
                        en as usize
                    };
                    let start_idx = start_idx.min(s.len());
                    let end_idx = end_idx.min(s.len());
                    if start_idx >= end_idx {
                        PropertyValue::String(String::new())
                    } else {
                        PropertyValue::String(s[start_idx..end_idx].to_string())
                    }
                }
                _ => PropertyValue::Null,
            }
        }

        // ─── Arithmetic ──────────────────────────────────────────
        Expression::Add(a, b) => {
            let va = eval_expression_with_catalog(a, bindings, storage, catalog);
            let vb = eval_expression_with_catalog(b, bindings, storage, catalog);
            // String concatenation
            if let (PropertyValue::String(sa), PropertyValue::String(sb)) = (&va, &vb) {
                PropertyValue::String(format!("{}{}", sa, sb))
            } else {
                temporal_arithmetic(&va, &vb, true)
                    .unwrap_or_else(|| arithmetic(&va, &vb, |x, y| x + y, |x, y| x + y))
            }
        }
        Expression::Sub(a, b) => {
            let va = eval_expression_with_catalog(a, bindings, storage, catalog);
            let vb = eval_expression_with_catalog(b, bindings, storage, catalog);
            temporal_arithmetic(&va, &vb, false)
                .unwrap_or_else(|| arithmetic(&va, &vb, |x, y| x - y, |x, y| x - y))
        }
        Expression::Mul(a, b) => {
            let va = eval_expression_with_catalog(a, bindings, storage, catalog);
            let vb = eval_expression_with_catalog(b, bindings, storage, catalog);
            arithmetic(&va, &vb, |x, y| x * y, |x, y| x * y)
        }
        Expression::Div(a, b) => {
            let va = eval_expression_with_catalog(a, bindings, storage, catalog);
            let vb = eval_expression_with_catalog(b, bindings, storage, catalog);
            if let (PropertyValue::Int(x), PropertyValue::Int(y)) = (&va, &vb) {
                if *y == 0 {
                    PropertyValue::Null
                } else {
                    PropertyValue::Int(x / y)
                }
            } else if let (PropertyValue::Double(x), PropertyValue::Double(y)) = (&va, &vb) {
                PropertyValue::Double(x / y)
            } else {
                PropertyValue::Null
            }
        }
        Expression::Mod(a, b) => {
            let va = eval_expression_with_catalog(a, bindings, storage, catalog);
            let vb = eval_expression_with_catalog(b, bindings, storage, catalog);
            if let (PropertyValue::Int(x), PropertyValue::Int(y)) = (&va, &vb) {
                if *y == 0 {
                    PropertyValue::Null
                } else {
                    PropertyValue::Int(x % y)
                }
            } else {
                PropertyValue::Null
            }
        }
        Expression::Neg(a) => {
            let va = eval_expression_with_catalog(a, bindings, storage, catalog);
            match va {
                PropertyValue::Int(x) => PropertyValue::Int(-x),
                PropertyValue::Double(x) => PropertyValue::Double(-x),
                _ => PropertyValue::Null,
            }
        }

        // ─── Comparison ──────────────────────────────────────────
        Expression::Eq(a, b) => {
            let va = eval_expression_with_catalog(a, bindings, storage, catalog);
            let vb = eval_expression_with_catalog(b, bindings, storage, catalog);
            PropertyValue::Bool(va == vb)
        }
        Expression::Neq(a, b) => {
            let va = eval_expression_with_catalog(a, bindings, storage, catalog);
            let vb = eval_expression_with_catalog(b, bindings, storage, catalog);
            PropertyValue::Bool(va != vb)
        }
        Expression::Lt(a, b) => cmp_expr(a, b, bindings, storage, catalog, |va, vb| va < vb),
        Expression::Gt(a, b) => cmp_expr(a, b, bindings, storage, catalog, |va, vb| va > vb),
        Expression::Lte(a, b) => cmp_expr(a, b, bindings, storage, catalog, |va, vb| va <= vb),
        Expression::Gte(a, b) => cmp_expr(a, b, bindings, storage, catalog, |va, vb| va >= vb),

        // ─── Logical ─────────────────────────────────────────────
        Expression::And(a, b) => {
            let va = eval_expression_with_catalog(a, bindings, storage, catalog);
            if !va.is_truthy() {
                return PropertyValue::Bool(false);
            }
            let vb = eval_expression_with_catalog(b, bindings, storage, catalog);
            PropertyValue::Bool(vb.is_truthy())
        }
        Expression::Or(a, b) => {
            let va = eval_expression_with_catalog(a, bindings, storage, catalog);
            if va.is_truthy() {
                return PropertyValue::Bool(true);
            }
            let vb = eval_expression_with_catalog(b, bindings, storage, catalog);
            PropertyValue::Bool(vb.is_truthy())
        }
        Expression::Not(a) => {
            let va = eval_expression_with_catalog(a, bindings, storage, catalog);
            PropertyValue::Bool(!va.is_truthy())
        }

        // ─── Other ───────────────────────────────────────────────
        Expression::IsNull(a) => PropertyValue::Bool(matches!(
            eval_expression_with_catalog(a, bindings, storage, catalog),
            PropertyValue::Null
        )),
        Expression::IsNotNull(a) => PropertyValue::Bool(!matches!(
            eval_expression_with_catalog(a, bindings, storage, catalog),
            PropertyValue::Null
        )),
        Expression::In(a, b) => {
            let va = eval_expression_with_catalog(a, bindings, storage, catalog);
            let vb = eval_expression_with_catalog(b, bindings, storage, catalog);
            match vb {
                PropertyValue::List(items) => PropertyValue::Bool(items.contains(&va)),
                PropertyValue::String(s) => match va {
                    PropertyValue::String(sub) => PropertyValue::Bool(s.contains(&sub)),
                    _ => PropertyValue::Null,
                },
                _ => PropertyValue::Null,
            }
        }
        Expression::StartsWith(a, b) => {
            string_predicate(a, b, bindings, storage, catalog, StringPred::StartsWith)
        }
        Expression::EndsWith(a, b) => {
            string_predicate(a, b, bindings, storage, catalog, StringPred::EndsWith)
        }
        Expression::Contains(a, b) => {
            string_predicate(a, b, bindings, storage, catalog, StringPred::Contains)
        }
        Expression::RegexMatch(a, b) => {
            let va = eval_expression_with_catalog(a, bindings, storage, catalog);
            let vb = eval_expression_with_catalog(b, bindings, storage, catalog);
            match (&va, &vb) {
                (PropertyValue::String(text), PropertyValue::String(pattern)) => {
                    match regex::Regex::new(pattern) {
                        Ok(re) => PropertyValue::Bool(re.is_match(text)),
                        Err(_) => PropertyValue::Null,
                    }
                }
                _ => PropertyValue::Null,
            }
        }
        Expression::CountStar => {
            // COUNT(*) is evaluated by the accumulator in RETURN, not here
            PropertyValue::Int(0)
        }
        Expression::Function {
            name, arguments, ..
        } => eval_function(name, arguments, bindings, storage, catalog),
        Expression::Exists(query) => match storage {
            Some(s) => {
                let result = crate::exec_clauses_with_binding(s, &query.clauses, bindings, None);
                PropertyValue::Bool(result.map(|(b, _)| !b.is_empty()).unwrap_or(false))
            }
            None => PropertyValue::Null,
        },
        Expression::CountSubquery(query) => match storage {
            Some(s) => {
                let result = crate::exec_clauses_with_binding(s, &query.clauses, bindings, None);
                PropertyValue::Int(result.map(|(b, _)| b.len() as i64).unwrap_or(0))
            }
            None => PropertyValue::Null,
        },
        Expression::Case {
            expression,
            whens,
            else_branch,
        } => {
            let compare_val = expression
                .as_ref()
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog));
            for (when_expr, then_expr) in whens {
                let when_val = eval_expression_with_catalog(when_expr, bindings, storage, catalog);
                let matches = match &compare_val {
                    Some(ref cv) => cv == &when_val,
                    None => when_val.is_truthy(),
                };
                if matches {
                    return eval_expression_with_catalog(then_expr, bindings, storage, catalog);
                }
            }
            match else_branch {
                Some(e) => eval_expression_with_catalog(e, bindings, storage, catalog),
                None => PropertyValue::Null,
            }
        }
        Expression::All {
            variable,
            list,
            predicate,
        }
        | Expression::Any {
            variable,
            list,
            predicate,
        }
        | Expression::None {
            variable,
            list,
            predicate,
        }
        | Expression::Single {
            variable,
            list,
            predicate,
        }
        | Expression::Filter {
            variable,
            list,
            predicate,
        } => {
            let list_val = eval_expression_with_catalog(list, bindings, storage, catalog);
            match list_val {
                PropertyValue::List(items) => {
                    let mut true_count = 0usize;
                    let mut filtered = Vec::new();
                    for item in &items {
                        let mut local = bindings.clone();
                        local.insert(variable.clone(), item.clone());
                        let pred_val =
                            eval_expression_with_catalog(predicate, &local, storage, catalog);
                        if pred_val.is_truthy() {
                            true_count += 1;
                            filtered.push(item.clone());
                        }
                    }
                    match expr {
                        Expression::All { .. } => PropertyValue::Bool(true_count == items.len()),
                        Expression::Any { .. } => PropertyValue::Bool(true_count > 0),
                        Expression::None { .. } => PropertyValue::Bool(true_count == 0),
                        Expression::Single { .. } => PropertyValue::Bool(true_count == 1),
                        Expression::Filter { .. } => PropertyValue::List(filtered),
                        _ => unreachable!(),
                    }
                }
                _ => PropertyValue::Null,
            }
        }
        Expression::Extract {
            variable,
            list,
            expression,
        } => {
            let list_val = eval_expression_with_catalog(list, bindings, storage, catalog);
            match list_val {
                PropertyValue::List(items) => {
                    let mut result = Vec::new();
                    for item in &items {
                        let mut local = bindings.clone();
                        local.insert(variable.clone(), item.clone());
                        result.push(eval_expression_with_catalog(
                            expression, &local, storage, catalog,
                        ));
                    }
                    PropertyValue::List(result)
                }
                _ => PropertyValue::Null,
            }
        }
        Expression::Reduce {
            accumulator,
            initial,
            variable,
            list,
            expression,
        } => {
            let list_val = eval_expression_with_catalog(list, bindings, storage, catalog);
            let mut acc = eval_expression_with_catalog(initial, bindings, storage, catalog);
            match list_val {
                PropertyValue::List(items) => {
                    for item in &items {
                        let mut local = bindings.clone();
                        local.insert(accumulator.clone(), acc.clone());
                        local.insert(variable.clone(), item.clone());
                        acc = eval_expression_with_catalog(expression, &local, storage, catalog);
                    }
                    acc
                }
                _ => PropertyValue::Null,
            }
        }
        Expression::PatternComprehension {
            pattern,
            where_clause,
            expression,
        } => match storage {
            Some(s) => {
                let query = mgparser::ast::Query {
                    clauses: vec![
                        mgparser::ast::Clause::Match {
                            pattern: pattern.clone(),
                            where_clause: where_clause.as_ref().map(|w| w.as_ref().clone()),
                        },
                        mgparser::ast::Clause::Return {
                            items: vec![mgparser::ast::ReturnItem {
                                expression: expression.as_ref().clone(),
                                alias: Some("_proj".to_string()),
                            }],
                            distinct: false,
                            all: false,
                        },
                    ],
                    union: None,
                    mode: QueryMode::Standard,
                    periodic_commit: None,
                    hops_limit: None,
                    index_hints: Vec::new(),
                };
                let result = crate::exec_clauses_with_binding(s, &query.clauses, bindings, None);
                match result {
                    Ok((_, query_result)) => {
                        let items: Vec<PropertyValue> = query_result
                            .rows
                            .into_iter()
                            .filter_map(|row| row.get("_proj").cloned())
                            .collect();
                        PropertyValue::List(items)
                    }
                    Err(_) => PropertyValue::Null,
                }
            }
            None => PropertyValue::Null,
        },
    }
}

fn cmp_expr(
    a: &Expression,
    b: &Expression,
    bindings: &HashMap<String, PropertyValue>,
    storage: Option<&Storage>,
    catalog: Option<&Catalog>,
    op: fn(i64, i64) -> bool,
) -> PropertyValue {
    let va = eval_expression_with_catalog(a, bindings, storage, catalog);
    let vb = eval_expression_with_catalog(b, bindings, storage, catalog);
    match (&va, &vb) {
        (PropertyValue::Int(x), PropertyValue::Int(y)) => PropertyValue::Bool(op(*x, *y)),
        (PropertyValue::Double(x), PropertyValue::Double(y)) => {
            let ord = x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal);
            let result = match ord {
                std::cmp::Ordering::Less => op(-1, 0),
                std::cmp::Ordering::Equal => op(0, 0),
                std::cmp::Ordering::Greater => op(1, 0),
            };
            PropertyValue::Bool(result)
        }
        (PropertyValue::Int(x), PropertyValue::Double(y)) => {
            let ord = (*x as f64)
                .partial_cmp(y)
                .unwrap_or(std::cmp::Ordering::Equal);
            let result = match ord {
                std::cmp::Ordering::Less => op(-1, 0),
                std::cmp::Ordering::Equal => op(0, 0),
                std::cmp::Ordering::Greater => op(1, 0),
            };
            PropertyValue::Bool(result)
        }
        (PropertyValue::Double(x), PropertyValue::Int(y)) => {
            let ord = x
                .partial_cmp(&(*y as f64))
                .unwrap_or(std::cmp::Ordering::Equal);
            let result = match ord {
                std::cmp::Ordering::Less => op(-1, 0),
                std::cmp::Ordering::Equal => op(0, 0),
                std::cmp::Ordering::Greater => op(1, 0),
            };
            PropertyValue::Bool(result)
        }
        (PropertyValue::String(x), PropertyValue::String(y)) => {
            let ord = x.cmp(y);
            let result = match ord {
                std::cmp::Ordering::Less => op(-1, 0),
                std::cmp::Ordering::Equal => op(0, 0),
                std::cmp::Ordering::Greater => op(1, 0),
            };
            PropertyValue::Bool(result)
        }
        (PropertyValue::Bool(x), PropertyValue::Bool(y)) => {
            let ord = x.cmp(y);
            let result = match ord {
                std::cmp::Ordering::Less => op(-1, 0),
                std::cmp::Ordering::Equal => op(0, 0),
                std::cmp::Ordering::Greater => op(1, 0),
            };
            PropertyValue::Bool(result)
        }
        _ => PropertyValue::Null,
    }
}

fn arithmetic(
    a: &PropertyValue,
    b: &PropertyValue,
    int_op: fn(i64, i64) -> i64,
    float_op: fn(f64, f64) -> f64,
) -> PropertyValue {
    match (a, b) {
        (PropertyValue::Int(x), PropertyValue::Int(y)) => PropertyValue::Int(int_op(*x, *y)),
        (PropertyValue::Int(x), PropertyValue::Double(y)) => {
            PropertyValue::Double(float_op(*x as f64, *y))
        }
        (PropertyValue::Double(x), PropertyValue::Int(y)) => {
            PropertyValue::Double(float_op(*x, *y as f64))
        }
        (PropertyValue::Double(x), PropertyValue::Double(y)) => {
            PropertyValue::Double(float_op(*x, *y))
        }
        _ => PropertyValue::Null,
    }
}

/// Temporal arithmetic: Date/Duration, LocalDateTime/Duration, ZonedDateTime/Duration,
/// Duration/Duration, and LocalTime/Duration.
/// Returns `Some(result)` if the operands are temporal, `None` otherwise.
fn temporal_arithmetic(
    a: &PropertyValue,
    b: &PropertyValue,
    is_add: bool,
) -> Option<PropertyValue> {
    let sign: i64 = if is_add { 1 } else { -1 };
    match (a, b) {
        // Duration <op> Duration
        (PropertyValue::Duration(d1), PropertyValue::Duration(d2)) => {
            Some(PropertyValue::Duration(Duration::new(
                d1.months + sign * d2.months,
                d1.days + sign * d2.days,
                d1.microseconds + sign * d2.microseconds,
            )))
        }
        // Date <op> Duration (days component only; months require calendar logic)
        (PropertyValue::Date(date), PropertyValue::Duration(dur)) if is_add => Some(
            PropertyValue::Date(Date::from_days(date.days_since_epoch + dur.days)),
        ),
        (PropertyValue::Date(date), PropertyValue::Duration(dur)) => Some(PropertyValue::Date(
            Date::from_days(date.days_since_epoch - dur.days),
        )),
        // Duration + Date (commutative)
        (PropertyValue::Duration(dur), PropertyValue::Date(date)) if is_add => Some(
            PropertyValue::Date(Date::from_days(date.days_since_epoch + dur.days)),
        ),
        // LocalDateTime <op> Duration
        (PropertyValue::LocalDateTime(ldt), PropertyValue::Duration(dur)) if is_add => {
            let delta_us = dur.days * 86_400_000_000_i64 + dur.microseconds;
            Some(PropertyValue::LocalDateTime(
                LocalDateTime::from_microseconds(ldt.microseconds + delta_us),
            ))
        }
        (PropertyValue::LocalDateTime(ldt), PropertyValue::Duration(dur)) => {
            let delta_us = dur.days * 86_400_000_000_i64 + dur.microseconds;
            Some(PropertyValue::LocalDateTime(
                LocalDateTime::from_microseconds(ldt.microseconds - delta_us),
            ))
        }
        // Duration + LocalDateTime (commutative)
        (PropertyValue::Duration(dur), PropertyValue::LocalDateTime(ldt)) if is_add => {
            let delta_us = dur.days * 86_400_000_000_i64 + dur.microseconds;
            Some(PropertyValue::LocalDateTime(
                LocalDateTime::from_microseconds(ldt.microseconds + delta_us),
            ))
        }
        // ZonedDateTime <op> Duration
        (PropertyValue::ZonedDateTime(zdt), PropertyValue::Duration(dur)) if is_add => {
            let delta_us = dur.days * 86_400_000_000_i64 + dur.microseconds;
            Some(PropertyValue::ZonedDateTime(ZonedDateTime::new(
                zdt.utc_microseconds + delta_us,
                zdt.offset_minutes,
                zdt.timezone.clone(),
            )))
        }
        (PropertyValue::ZonedDateTime(zdt), PropertyValue::Duration(dur)) => {
            let delta_us = dur.days * 86_400_000_000_i64 + dur.microseconds;
            Some(PropertyValue::ZonedDateTime(ZonedDateTime::new(
                zdt.utc_microseconds - delta_us,
                zdt.offset_minutes,
                zdt.timezone.clone(),
            )))
        }
        // Duration + ZonedDateTime (commutative)
        (PropertyValue::Duration(dur), PropertyValue::ZonedDateTime(zdt)) if is_add => {
            let delta_us = dur.days * 86_400_000_000_i64 + dur.microseconds;
            Some(PropertyValue::ZonedDateTime(ZonedDateTime::new(
                zdt.utc_microseconds + delta_us,
                zdt.offset_minutes,
                zdt.timezone.clone(),
            )))
        }
        // LocalTime <op> Duration (wrap around midnight)
        (PropertyValue::LocalTime(lt), PropertyValue::Duration(dur)) if is_add => {
            let delta_us = dur.days * 86_400_000_000_i64 + dur.microseconds;
            let us = (lt.microseconds + delta_us).rem_euclid(86_400_000_000);
            Some(PropertyValue::LocalTime(LocalTime::from_microseconds(us)))
        }
        (PropertyValue::LocalTime(lt), PropertyValue::Duration(dur)) => {
            let delta_us = dur.days * 86_400_000_000_i64 + dur.microseconds;
            let us = (lt.microseconds - delta_us).rem_euclid(86_400_000_000);
            Some(PropertyValue::LocalTime(LocalTime::from_microseconds(us)))
        }
        // Duration + LocalTime (commutative)
        (PropertyValue::Duration(dur), PropertyValue::LocalTime(lt)) if is_add => {
            let delta_us = dur.days * 86_400_000_000_i64 + dur.microseconds;
            let us = (lt.microseconds + delta_us).rem_euclid(86_400_000_000);
            Some(PropertyValue::LocalTime(LocalTime::from_microseconds(us)))
        }
        _ => None,
    }
}

fn extract_property(
    obj: &PropertyValue,
    key: mgcore::types::PropertyId,
    storage: Option<&Storage>,
    catalog: Option<&Catalog>,
) -> PropertyValue {
    match obj {
        PropertyValue::Map(entries) => {
            // Look up by string key using catalog name resolution.
            let key_name = catalog.map(|c| c.property_name(key)).unwrap_or_default();
            entries
                .iter()
                .find(|(k, _)| k == &key_name)
                .map(|(_, v)| v.clone())
                .unwrap_or(PropertyValue::Null)
        }
        PropertyValue::Vertex(vref) => {
            // Fast path: use in-memory properties when available (kept in sync by exec_set).
            if vref.properties.is_set(key) {
                return vref.properties.get(key).clone();
            }
            if let Some(s) = storage {
                let tx = get_read_tx(s);
                if let Some(v) = s.get_vertex(vref.gid, &tx) {
                    v.properties.get(key).clone()
                } else {
                    PropertyValue::Null
                }
            } else {
                PropertyValue::Null
            }
        }
        PropertyValue::Edge(eref) => {
            if eref.properties.is_set(key) {
                return eref.properties.get(key).clone();
            }
            if let Some(s) = storage {
                let tx = get_read_tx(s);
                if let Some(e) = s.get_edge(eref.gid, &tx) {
                    e.properties.get(key).clone()
                } else {
                    PropertyValue::Null
                }
            } else {
                PropertyValue::Null
            }
        }
        // ─── Temporal property access ──────────────────────────
        _ => {
            let key_name = catalog.map(|c| c.property_name(key)).unwrap_or_default();
            extract_temporal_property(obj, &key_name)
        }
    }
}

/// Extract a property from a temporal value by string name.
fn extract_temporal_property(obj: &PropertyValue, key: &str) -> PropertyValue {
    let key_lower = key.to_ascii_lowercase();
    match obj {
        PropertyValue::Date(date) => {
            let nd = chrono::NaiveDate::from_num_days_from_ce_opt(
                (date.days_since_epoch + 719162) as i32,
            );
            match nd {
                Some(d) => match key_lower.as_str() {
                    "year" => PropertyValue::Int(d.year() as i64),
                    "month" => PropertyValue::Int(d.month() as i64),
                    "day" => PropertyValue::Int(d.day() as i64),
                    "dayofweek" | "day_of_week" => {
                        PropertyValue::Int(d.weekday().num_days_from_monday() as i64 + 1)
                    }
                    "dayofyear" | "day_of_year" => PropertyValue::Int(d.ordinal() as i64),
                    "week" => PropertyValue::Int(d.iso_week().week() as i64),
                    "quarter" => PropertyValue::Int(((d.month() - 1) / 3 + 1) as i64),
                    "epochdays" | "epoch_days" => PropertyValue::Int(date.days_since_epoch),
                    _ => PropertyValue::Null,
                },
                None => PropertyValue::Null,
            }
        }
        PropertyValue::LocalDateTime(ldt) => {
            let ndt = us_to_naive_datetime(ldt.microseconds);
            match key_lower.as_str() {
                "year" => PropertyValue::Int(ndt.year() as i64),
                "month" => PropertyValue::Int(ndt.month() as i64),
                "day" => PropertyValue::Int(ndt.day() as i64),
                "hour" => PropertyValue::Int(ndt.hour() as i64),
                "minute" => PropertyValue::Int(ndt.minute() as i64),
                "second" => PropertyValue::Int(ndt.second() as i64),
                "millisecond" => PropertyValue::Int((ndt.nanosecond() as i64) / 1_000_000),
                "microsecond" => {
                    PropertyValue::Int(((ndt.nanosecond() as i64) % 1_000_000) / 1_000)
                }
                "nanosecond" => PropertyValue::Int((ndt.nanosecond() % 1_000) as i64),
                "epochmillis" | "epoch_millis" => PropertyValue::Int(ldt.microseconds / 1_000),
                "epochseconds" | "epoch_seconds" => {
                    PropertyValue::Int(ldt.microseconds / 1_000_000)
                }
                "dayofweek" | "day_of_week" => {
                    PropertyValue::Int(ndt.weekday().num_days_from_monday() as i64 + 1)
                }
                "dayofyear" | "day_of_year" => PropertyValue::Int(ndt.ordinal() as i64),
                "week" => PropertyValue::Int(ndt.iso_week().week() as i64),
                "quarter" => PropertyValue::Int(((ndt.month() - 1) / 3 + 1) as i64),
                _ => PropertyValue::Null,
            }
        }
        PropertyValue::ZonedDateTime(zdt) => {
            let dt =
                chrono::DateTime::from_timestamp_micros(zdt.utc_microseconds).unwrap_or_default();
            match key_lower.as_str() {
                "year" => PropertyValue::Int(dt.year() as i64),
                "month" => PropertyValue::Int(dt.month() as i64),
                "day" => PropertyValue::Int(dt.day() as i64),
                "hour" => PropertyValue::Int(dt.hour() as i64),
                "minute" => PropertyValue::Int(dt.minute() as i64),
                "second" => PropertyValue::Int(dt.second() as i64),
                "millisecond" => PropertyValue::Int((dt.nanosecond() as i64) / 1_000_000),
                "microsecond" => PropertyValue::Int(((dt.nanosecond() as i64) % 1_000_000) / 1_000),
                "nanosecond" => PropertyValue::Int((dt.nanosecond() % 1_000) as i64),
                "timezone" => PropertyValue::String(zdt.timezone.clone()),
                "offset" => PropertyValue::String(format!(
                    "{:+03}:{:02}",
                    zdt.offset_minutes / 60,
                    zdt.offset_minutes.abs() % 60
                )),
                "offsetminutes" | "offset_minutes" => PropertyValue::Int(zdt.offset_minutes as i64),
                "epochmillis" | "epoch_millis" => PropertyValue::Int(zdt.utc_microseconds / 1_000),
                "epochseconds" | "epoch_seconds" => {
                    PropertyValue::Int(zdt.utc_microseconds / 1_000_000)
                }
                "dayofweek" | "day_of_week" => {
                    PropertyValue::Int(dt.weekday().num_days_from_monday() as i64 + 1)
                }
                "dayofyear" | "day_of_year" => PropertyValue::Int(dt.ordinal() as i64),
                "week" => PropertyValue::Int(dt.iso_week().week() as i64),
                "quarter" => PropertyValue::Int(((dt.month() - 1) / 3 + 1) as i64),
                _ => PropertyValue::Null,
            }
        }
        PropertyValue::LocalTime(lt) => match key_lower.as_str() {
            "hour" => PropertyValue::Int(lt.microseconds / 3_600_000_000),
            "minute" => PropertyValue::Int((lt.microseconds / 60_000_000) % 60),
            "second" => PropertyValue::Int((lt.microseconds / 1_000_000) % 60),
            "millisecond" => PropertyValue::Int((lt.microseconds / 1_000) % 1_000),
            "microsecond" => PropertyValue::Int(lt.microseconds % 1_000_000),
            "nanosecond" => PropertyValue::Int((lt.microseconds % 1_000_000) * 1_000),
            _ => PropertyValue::Null,
        },
        PropertyValue::Duration(dur) => match key_lower.as_str() {
            "years" => PropertyValue::Int(dur.months / 12),
            "months" => PropertyValue::Int(dur.months),
            "days" => PropertyValue::Int(dur.days),
            "hours" => PropertyValue::Int(dur.microseconds / 3_600_000_000),
            "minutes" => PropertyValue::Int(dur.microseconds / 60_000_000),
            "seconds" => PropertyValue::Int(dur.microseconds / 1_000_000),
            "milliseconds" => PropertyValue::Int(dur.microseconds / 1_000),
            "microseconds" => PropertyValue::Int(dur.microseconds),
            "nanoseconds" => PropertyValue::Int(dur.microseconds * 1_000),
            _ => PropertyValue::Null,
        },
        _ => PropertyValue::Null,
    }
}

enum StringPred {
    StartsWith,
    EndsWith,
    Contains,
}

fn string_predicate(
    a: &Expression,
    b: &Expression,
    bindings: &HashMap<String, PropertyValue>,
    storage: Option<&Storage>,
    catalog: Option<&Catalog>,
    pred: StringPred,
) -> PropertyValue {
    let va = eval_expression_with_catalog(a, bindings, storage, catalog);
    let vb = eval_expression_with_catalog(b, bindings, storage, catalog);
    match (&va, &vb) {
        (PropertyValue::String(s), PropertyValue::String(pattern)) => {
            PropertyValue::Bool(match pred {
                StringPred::StartsWith => s.starts_with(pattern),
                StringPred::EndsWith => s.ends_with(pattern),
                StringPred::Contains => s.contains(pattern),
            })
        }
        _ => PropertyValue::Null,
    }
}

/// Fetch properties from storage for a Vertex or Edge reference.
fn get_entity_properties(
    v: &PropertyValue,
    storage: Option<&Storage>,
    catalog: Option<&Catalog>,
) -> Vec<(String, PropertyValue)> {
    // Resolve properties via a temporary read transaction. Errors (missing storage,
    // vertex/edge not found, missing catalog) silently return empty — callers cannot
    // distinguish "no properties" from "lookup failed", which is acceptable for
    // introspection functions.
    let fetch = |gid, is_edge: bool| -> Vec<(PropertyId, PropertyValue)> {
        storage
            .and_then(|s| {
                let tx = get_read_tx(s);
                if is_edge {
                    s.get_edge(gid, &tx).map(|snap| {
                        snap.properties
                            .iter()
                            .map(|(k, v)| (k, v.clone()))
                            .collect()
                    })
                } else {
                    s.get_vertex(gid, &tx).map(|snap| {
                        snap.properties
                            .iter()
                            .map(|(k, v)| (k, v.clone()))
                            .collect()
                    })
                }
            })
            .unwrap_or_default()
    };

    let props = match v {
        PropertyValue::Vertex(vr) => fetch(vr.gid, false),
        PropertyValue::Edge(er) => fetch(er.gid, true),
        _ => return vec![],
    };

    if let Some(cat) = catalog {
        props
            .into_iter()
            .map(|(pid, val)| (cat.property_name(pid), val))
            .collect()
    } else {
        vec![]
    }
}

/// Global counter storage for the `counter()` function.
static COUNTERS: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<String, i64>>> =
    std::sync::OnceLock::new();

fn counter_storage() -> &'static std::sync::Mutex<std::collections::HashMap<String, i64>> {
    COUNTERS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

fn eval_function(
    name: &str,
    args: &[Expression],
    bindings: &HashMap<String, PropertyValue>,
    storage: Option<&Storage>,
    catalog: Option<&Catalog>,
) -> PropertyValue {
    // Normalize dotted APOC names to underscore versions
    let name = if name.contains('.') {
        name.replace('.', "_")
    } else {
        name.to_string()
    };
    let name = name.as_str();
    // Fast path: check common functions
    if name.eq_ignore_ascii_case("id") {
        return args
            .first()
            .map(|a| eval_expression_with_catalog(a, bindings, storage, catalog))
            .map(|v| match v {
                PropertyValue::Vertex(vr) => PropertyValue::Int(vr.gid.as_int()),
                PropertyValue::Edge(er) => PropertyValue::Int(er.gid.as_int()),
                _ => PropertyValue::Null,
            })
            .unwrap_or(PropertyValue::Null);
    }
    if name.eq_ignore_ascii_case("elementid") {
        return args
            .first()
            .map(|a| eval_expression_with_catalog(a, bindings, storage, catalog))
            .map(|v| match v {
                PropertyValue::Vertex(vr) => {
                    PropertyValue::String(format!("v:{}", vr.gid.as_uint()))
                }
                PropertyValue::Edge(er) => PropertyValue::String(format!("e:{}", er.gid.as_uint())),
                _ => PropertyValue::Null,
            })
            .unwrap_or(PropertyValue::Null);
    }
    if name.eq_ignore_ascii_case("labels") {
        return args
            .first()
            .and_then(|a| {
                let val = eval_expression_with_catalog(a, bindings, storage, catalog);
                match val {
                    PropertyValue::Vertex(vr) => {
                        let label_names: Vec<PropertyValue> = if let Some(cat) = catalog {
                            vr.labels
                                .iter()
                                .map(|lid| PropertyValue::String(cat.label_name(*lid)))
                                .collect()
                        } else {
                            vec![]
                        };
                        Some(PropertyValue::List(label_names))
                    }
                    _ => None,
                }
            })
            .unwrap_or(PropertyValue::Null);
    }
    if name.eq_ignore_ascii_case("type") {
        return args
            .first()
            .map(|a| eval_expression_with_catalog(a, bindings, storage, catalog))
            .map(|v| match v {
                PropertyValue::Edge(er) => {
                    if let Some(cat) = catalog {
                        PropertyValue::String(cat.edge_type_name(er.edge_type))
                    } else {
                        PropertyValue::String(format!("{}", er.edge_type.as_uint()))
                    }
                }
                _ => PropertyValue::Null,
            })
            .unwrap_or(PropertyValue::Null);
    }
    if name.eq_ignore_ascii_case("properties") {
        return args
            .first()
            .map(|a| eval_expression_with_catalog(a, bindings, storage, catalog))
            .map(|v| {
                let entries: Vec<(String, PropertyValue)> =
                    get_entity_properties(&v, storage, catalog);
                PropertyValue::Map(entries)
            })
            .unwrap_or(PropertyValue::Null);
    }
    if name.eq_ignore_ascii_case("keys") {
        return args
            .first()
            .map(|a| eval_expression_with_catalog(a, bindings, storage, catalog))
            .map(|v| {
                let keys: Vec<PropertyValue> = match &v {
                    PropertyValue::Map(entries) => entries
                        .iter()
                        .map(|(k, _)| PropertyValue::String(k.clone()))
                        .collect(),
                    _ => get_entity_properties(&v, storage, catalog)
                        .into_iter()
                        .map(|(k, _)| PropertyValue::String(k))
                        .collect(),
                };
                PropertyValue::List(keys)
            })
            .unwrap_or(PropertyValue::Null);
    }
    if name.eq_ignore_ascii_case("startnode") {
        return args
            .first()
            .map(|a| eval_expression_with_catalog(a, bindings, storage, catalog))
            .map(|v| match v {
                PropertyValue::Edge(er) => {
                    PropertyValue::Vertex(mgcore::property_value::VertexRef::new(
                        er.from_vertex,
                        vec![],
                        mgcore::property_store::PropertyStore::default(),
                    ))
                }
                _ => PropertyValue::Null,
            })
            .unwrap_or(PropertyValue::Null);
    }
    if name.eq_ignore_ascii_case("endnode") {
        return args
            .first()
            .map(|a| eval_expression_with_catalog(a, bindings, storage, catalog))
            .map(|v| match v {
                PropertyValue::Edge(er) => {
                    PropertyValue::Vertex(mgcore::property_value::VertexRef::new(
                        er.to_vertex,
                        vec![],
                        mgcore::property_store::PropertyStore::default(),
                    ))
                }
                _ => PropertyValue::Null,
            })
            .unwrap_or(PropertyValue::Null);
    }
    if name.eq_ignore_ascii_case("timestamp") {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        return PropertyValue::Int(now);
    }
    if name.eq_ignore_ascii_case("toboolean") {
        return args
            .first()
            .map(|a| eval_expression_with_catalog(a, bindings, storage, catalog))
            .map(|v| match v {
                PropertyValue::String(s) => PropertyValue::Bool(!s.is_empty() && s != "false"),
                PropertyValue::Int(n) => PropertyValue::Bool(n != 0),
                PropertyValue::Double(f) => PropertyValue::Bool(f != 0.0),
                PropertyValue::Bool(b) => PropertyValue::Bool(b),
                _ => PropertyValue::Null,
            })
            .unwrap_or(PropertyValue::Null);
    }
    if name.eq_ignore_ascii_case("tobooleanornull") {
        return args
            .first()
            .map(|a| eval_expression_with_catalog(a, bindings, storage, catalog))
            .map(|v| match v {
                PropertyValue::String(s) if s.eq_ignore_ascii_case("true") => {
                    PropertyValue::Bool(true)
                }
                PropertyValue::String(s) if s.eq_ignore_ascii_case("false") => {
                    PropertyValue::Bool(false)
                }
                PropertyValue::Bool(b) => PropertyValue::Bool(b),
                _ => PropertyValue::Null,
            })
            .unwrap_or(PropertyValue::Null);
    }
    if name.eq_ignore_ascii_case("size") {
        return args
            .first()
            .map(|a| eval_expression_with_catalog(a, bindings, storage, catalog))
            .map(|v| match v {
                PropertyValue::String(s) => PropertyValue::Int(s.len() as i64),
                PropertyValue::List(l) => PropertyValue::Int(l.len() as i64),
                PropertyValue::Map(m) => PropertyValue::Int(m.len() as i64),
                PropertyValue::Path(p) => PropertyValue::Int(p.edges.len() as i64),
                _ => PropertyValue::Int(0),
            })
            .unwrap_or(PropertyValue::Int(0));
    }
    if name.eq_ignore_ascii_case("isempty") {
        return args
            .first()
            .map(|a| eval_expression_with_catalog(a, bindings, storage, catalog))
            .map(|v| match v {
                PropertyValue::String(s) => PropertyValue::Bool(s.is_empty()),
                PropertyValue::List(l) => PropertyValue::Bool(l.is_empty()),
                PropertyValue::Map(m) => PropertyValue::Bool(m.is_empty()),
                _ => PropertyValue::Bool(true),
            })
            .unwrap_or(PropertyValue::Bool(true));
    }
    if name.eq_ignore_ascii_case("reverse") {
        return args
            .first()
            .map(|a| eval_expression_with_catalog(a, bindings, storage, catalog))
            .map(|v| match v {
                PropertyValue::String(s) => PropertyValue::String(s.chars().rev().collect()),
                PropertyValue::List(mut l) => {
                    l.reverse();
                    PropertyValue::List(l)
                }
                _ => v,
            })
            .unwrap_or(PropertyValue::Null);
    }
    if name.eq_ignore_ascii_case("contains") && args.len() == 2 {
        let a0 = eval_expression_with_catalog(&args[0], bindings, storage, catalog);
        let a1 = eval_expression_with_catalog(&args[1], bindings, storage, catalog);
        if let (PropertyValue::String(s), PropertyValue::String(sub)) = (&a0, &a1) {
            return PropertyValue::Bool(s.contains(sub));
        }
        // Fall through to list contains in the match block below
    }
    if name.eq_ignore_ascii_case("nodes") && !args.is_empty() {
        return args
            .first()
            .map(|a| eval_expression_with_catalog(a, bindings, storage, catalog))
            .map(|v| match v {
                PropertyValue::Path(p) => PropertyValue::List(
                    p.vertices
                        .iter()
                        .map(|vr| PropertyValue::Vertex(vr.clone()))
                        .collect(),
                ),
                _ => PropertyValue::List(vec![]),
            })
            .unwrap_or(PropertyValue::List(vec![]));
    }
    if name.eq_ignore_ascii_case("relationships") && !args.is_empty() {
        return args
            .first()
            .map(|a| eval_expression_with_catalog(a, bindings, storage, catalog))
            .map(|v| match v {
                PropertyValue::Path(p) => PropertyValue::List(
                    p.edges
                        .iter()
                        .map(|er| PropertyValue::Edge(er.clone()))
                        .collect(),
                ),
                _ => PropertyValue::List(vec![]),
            })
            .unwrap_or(PropertyValue::List(vec![]));
    }
    if name.eq_ignore_ascii_case("startnode") || name.eq_ignore_ascii_case("start_node") {
        return args
            .first()
            .map(|a| eval_expression_with_catalog(a, bindings, storage, catalog))
            .map(|v| match v {
                PropertyValue::Edge(er) => {
                    if let Some(s) = storage {
                        let tx = get_read_tx(s);
                        s.get_vertex(er.from_vertex, &tx)
                            .map(|snap| {
                                PropertyValue::Vertex(mgcore::property_value::VertexRef::new(
                                    snap.gid,
                                    snap.labels,
                                    snap.properties.clone(),
                                ))
                            })
                            .unwrap_or(PropertyValue::Null)
                    } else {
                        PropertyValue::Null
                    }
                }
                _ => PropertyValue::Null,
            })
            .unwrap_or(PropertyValue::Null);
    }
    if name.eq_ignore_ascii_case("endnode") || name.eq_ignore_ascii_case("end_node") {
        return args
            .first()
            .map(|a| eval_expression_with_catalog(a, bindings, storage, catalog))
            .map(|v| match v {
                PropertyValue::Edge(er) => {
                    if let Some(s) = storage {
                        let tx = get_read_tx(s);
                        s.get_vertex(er.to_vertex, &tx)
                            .map(|snap| {
                                PropertyValue::Vertex(mgcore::property_value::VertexRef::new(
                                    snap.gid,
                                    snap.labels,
                                    snap.properties.clone(),
                                ))
                            })
                            .unwrap_or(PropertyValue::Null)
                    } else {
                        PropertyValue::Null
                    }
                }
                _ => PropertyValue::Null,
            })
            .unwrap_or(PropertyValue::Null);
    }
    if name.eq_ignore_ascii_case("values") {
        return args
            .first()
            .map(|a| eval_expression_with_catalog(a, bindings, storage, catalog))
            .map(|v| match v {
                PropertyValue::Map(entries) => {
                    PropertyValue::List(entries.iter().map(|(_, val)| val.clone()).collect())
                }
                _ => PropertyValue::List(vec![]),
            })
            .unwrap_or(PropertyValue::List(vec![]));
    }
    if name.eq_ignore_ascii_case("valuetype") {
        return args
            .first()
            .map(|a| eval_expression_with_catalog(a, bindings, storage, catalog))
            .map(|v| {
                PropertyValue::String(
                    match v {
                        PropertyValue::Null => "NULL",
                        PropertyValue::Bool(_) => "BOOLEAN",
                        PropertyValue::Int(_) => "INTEGER",
                        PropertyValue::Double(_) => "FLOAT",
                        PropertyValue::String(_) => "STRING",
                        PropertyValue::List(_) => "LIST",
                        PropertyValue::Map(_) => "MAP",
                        PropertyValue::Vertex(_) => "NODE",
                        PropertyValue::Edge(_) => "RELATIONSHIP",
                        PropertyValue::Path(_) => "PATH",
                        PropertyValue::Date(_) => "DATE",
                        PropertyValue::LocalTime(_) => "LOCAL_TIME",
                        PropertyValue::LocalDateTime(_) => "LOCAL_DATE_TIME",
                        PropertyValue::Duration(_) => "DURATION",
                        PropertyValue::ZonedDateTime(_) => "ZONED_DATE_TIME",
                        PropertyValue::Point2D(_) => "POINT_2D",
                        PropertyValue::Point3D(_) => "POINT_3D",
                        PropertyValue::Enum { .. } => "ENUM",
                    }
                    .to_string(),
                )
            })
            .unwrap_or(PropertyValue::Null);
    }
    if name.eq_ignore_ascii_case("randomuuid") {
        // Generate a version-4 random UUID
        let rng = || rand_simple_u64();
        let d1 = rng();
        let d2 = rng();
        let d3 = (rng() & 0x0FFF) | 0x4000;
        let d4 = (rng() & 0x3FFF) | 0x8000;
        let d5 = rng();
        let uuid = format!(
            "{:08x}-{:04x}-{:04x}-{:04x}-{:08x}{:04x}",
            d1 as u32,
            (d1 >> 32) as u16,
            d2 as u16,
            d3 as u16,
            d4 as u32,
            d5 as u16,
        );
        return PropertyValue::String(uuid);
    }
    if matches!(name, n if n.eq_ignore_ascii_case("sin") || n.eq_ignore_ascii_case("cos") || n.eq_ignore_ascii_case("tan")
        || n.eq_ignore_ascii_case("asin") || n.eq_ignore_ascii_case("acos") || n.eq_ignore_ascii_case("atan")
        || n.eq_ignore_ascii_case("atan2") || n.eq_ignore_ascii_case("cot") || n.eq_ignore_ascii_case("haversin")
        || n.eq_ignore_ascii_case("degrees") || n.eq_ignore_ascii_case("radians"))
    {
        return args
            .first()
            .map(|a| eval_expression_with_catalog(a, bindings, storage, catalog))
            .map(|v| {
                let x = match v {
                    PropertyValue::Double(f) => f,
                    PropertyValue::Int(n) => n as f64,
                    _ => return PropertyValue::Null,
                };
                let result = match name {
                    n if n.eq_ignore_ascii_case("sin") => x.sin(),
                    n if n.eq_ignore_ascii_case("cos") => x.cos(),
                    n if n.eq_ignore_ascii_case("tan") => x.tan(),
                    n if n.eq_ignore_ascii_case("asin") => {
                        if (-1.0..=1.0).contains(&x) {
                            x.asin()
                        } else {
                            return PropertyValue::Null;
                        }
                    }
                    n if n.eq_ignore_ascii_case("acos") => {
                        if (-1.0..=1.0).contains(&x) {
                            x.acos()
                        } else {
                            return PropertyValue::Null;
                        }
                    }
                    n if n.eq_ignore_ascii_case("atan") => x.atan(),
                    n if n.eq_ignore_ascii_case("cot") => 1.0 / x.tan(),
                    n if n.eq_ignore_ascii_case("haversin") => {
                        let sin_half = (x / 2.0).sin();
                        sin_half * sin_half
                    }
                    n if n.eq_ignore_ascii_case("atan2") => {
                        let y = match args
                            .get(1)
                            .map(|a| eval_expression_with_catalog(a, bindings, storage, catalog))
                        {
                            Some(PropertyValue::Double(f)) => f,
                            Some(PropertyValue::Int(n)) => n as f64,
                            _ => return PropertyValue::Null,
                        };
                        x.atan2(y)
                    }
                    n if n.eq_ignore_ascii_case("degrees") => x.to_degrees(),
                    n if n.eq_ignore_ascii_case("radians") => x.to_radians(),
                    _ => return PropertyValue::Null,
                };
                PropertyValue::Double(result)
            })
            .unwrap_or(PropertyValue::Null);
    }
    if name.eq_ignore_ascii_case("coalesce") {
        return args
            .iter()
            .map(|a| eval_expression_with_catalog(a, bindings, storage, catalog))
            .find(|v| !matches!(v, PropertyValue::Null))
            .unwrap_or(PropertyValue::Null);
    }
    if name.eq_ignore_ascii_case("tostring") {
        return args
            .first()
            .map(|a| {
                let v = eval_expression_with_catalog(a, bindings, storage, catalog);
                match &v {
                    PropertyValue::String(s) => PropertyValue::String(s.clone()),
                    _ => PropertyValue::String(format!("{}", v)),
                }
            })
            .unwrap_or(PropertyValue::String("".into()));
    }
    if name.eq_ignore_ascii_case("tostringornull") {
        return args
            .first()
            .map(|a| {
                let v = eval_expression_with_catalog(a, bindings, storage, catalog);
                match &v {
                    PropertyValue::Null => PropertyValue::Null,
                    PropertyValue::String(s) => PropertyValue::String(s.clone()),
                    _ => PropertyValue::String(format!("{}", v)),
                }
            })
            .unwrap_or(PropertyValue::Null);
    }
    if name.eq_ignore_ascii_case("tolist") || name.eq_ignore_ascii_case("tolistornull") {
        return args
            .first()
            .map(|a| {
                let v = eval_expression_with_catalog(a, bindings, storage, catalog);
                match v {
                    PropertyValue::List(l) => PropertyValue::List(l),
                    PropertyValue::String(s) => PropertyValue::List(
                        s.chars()
                            .map(|c| PropertyValue::String(c.to_string()))
                            .collect(),
                    ),
                    PropertyValue::Map(entries) => PropertyValue::List(
                        entries
                            .into_iter()
                            .map(|(k, v)| PropertyValue::List(vec![PropertyValue::String(k), v]))
                            .collect(),
                    ),
                    PropertyValue::Null => PropertyValue::Null,
                    _ => PropertyValue::Null,
                }
            })
            .unwrap_or(PropertyValue::Null);
    }
    if name.eq_ignore_ascii_case("tolower") {
        return args
            .first()
            .map(|a| eval_expression_with_catalog(a, bindings, storage, catalog))
            .map(|v| match v {
                PropertyValue::String(s) => PropertyValue::String(s.to_lowercase()),
                _ => v,
            })
            .unwrap_or(PropertyValue::Null);
    }
    if name.eq_ignore_ascii_case("toupper") {
        return args
            .first()
            .map(|a| eval_expression_with_catalog(a, bindings, storage, catalog))
            .map(|v| match v {
                PropertyValue::String(s) => PropertyValue::String(s.to_uppercase()),
                _ => v,
            })
            .unwrap_or(PropertyValue::Null);
    }
    if name.eq_ignore_ascii_case("range") {
        if args.len() >= 2 {
            let start = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                PropertyValue::Int(n) => n,
                _ => return PropertyValue::Null,
            };
            let end = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                PropertyValue::Int(n) => n,
                _ => return PropertyValue::Null,
            };
            let step = args
                .get(2)
                .map(
                    |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                        PropertyValue::Int(n) => n,
                        _ => 1,
                    },
                )
                .unwrap_or(1);
            let mut list = Vec::new();
            let mut i = start;
            while (step > 0 && i <= end) || (step < 0 && i >= end) {
                list.push(PropertyValue::Int(i));
                i += step;
            }
            return PropertyValue::List(list);
        }
        return PropertyValue::Null;
    }
    match name {
        n if n.eq_ignore_ascii_case("abs") => {
            if let Some(arg) = args.first() {
                let v = eval_expression_with_catalog(arg, bindings, storage, catalog);
                match v {
                    PropertyValue::Int(x) => PropertyValue::Int(x.abs()),
                    PropertyValue::Double(x) => PropertyValue::Double(x.abs()),
                    _ => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("contains") => {
            let a = args
                .first()
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog));
            let b = args
                .get(1)
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog));
            match (a, b) {
                (Some(PropertyValue::List(items)), Some(val)) => {
                    PropertyValue::Bool(items.contains(&val))
                }
                (Some(PropertyValue::String(s)), Some(PropertyValue::String(sub))) => {
                    PropertyValue::Bool(s.contains(&sub))
                }
                _ => PropertyValue::Null,
            }
        }
        n if n.eq_ignore_ascii_case("exists") => {
            if let Some(arg) = args.first() {
                let val = eval_expression_with_catalog(arg, bindings, storage, catalog);
                match val {
                    PropertyValue::Null => PropertyValue::Bool(false),
                    PropertyValue::Map(m) => PropertyValue::Bool(!m.is_empty()),
                    PropertyValue::List(l) => PropertyValue::Bool(!l.is_empty()),
                    PropertyValue::String(s) => PropertyValue::Bool(!s.is_empty()),
                    _ => PropertyValue::Bool(true),
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("size") | n.eq_ignore_ascii_case("length") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::String(s) => PropertyValue::Int(s.len() as i64),
                    PropertyValue::List(l) => PropertyValue::Int(l.len() as i64),
                    PropertyValue::Path(p) => PropertyValue::Int(p.edges.len() as i64),
                    _ => PropertyValue::Int(0),
                },
            )
            .unwrap_or(PropertyValue::Int(0)),
        // String functions
        n if n.eq_ignore_ascii_case("trim") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::String(s) => PropertyValue::String(s.trim().to_string()),
                    v => v,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("ltrim") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::String(s) => PropertyValue::String(s.trim_start().to_string()),
                    v => v,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("rtrim") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::String(s) => PropertyValue::String(s.trim_end().to_string()),
                    v => v,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("split") => {
            if args.len() == 2 {
                let s = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let sep = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                PropertyValue::List(
                    s.split(&sep)
                        .map(|p| PropertyValue::String(p.to_string()))
                        .collect(),
                )
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("replace") => {
            if args.len() == 3 {
                let s = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let old = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let new = match eval_expression_with_catalog(&args[2], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                PropertyValue::String(s.replace(&old, &new))
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("substring") => {
            if args.len() >= 2 {
                let s = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let start = match eval_expression_with_catalog(&args[1], bindings, storage, catalog)
                {
                    PropertyValue::Int(n) => n as usize,
                    _ => return PropertyValue::Null,
                };
                let len = args.get(2).map(|a| {
                    match eval_expression_with_catalog(a, bindings, storage, catalog) {
                        PropertyValue::Int(n) => n as usize,
                        _ => 0,
                    }
                });
                if let Some(l) = len {
                    PropertyValue::String(s.chars().skip(start).take(l).collect())
                } else {
                    PropertyValue::String(s.chars().skip(start).collect())
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("left") => {
            if args.len() == 2 {
                let s = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let n = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::Int(n) => n as usize,
                    _ => return PropertyValue::Null,
                };
                PropertyValue::String(s.chars().take(n).collect())
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("right") => {
            if args.len() == 2 {
                let s = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let n = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::Int(n) => n as usize,
                    _ => return PropertyValue::Null,
                };
                PropertyValue::String(
                    s.chars()
                        .rev()
                        .take(n)
                        .collect::<String>()
                        .chars()
                        .rev()
                        .collect(),
                )
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("reverse") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::String(s) => PropertyValue::String(s.chars().rev().collect()),
                    PropertyValue::List(l) => PropertyValue::List(l.into_iter().rev().collect()),
                    v => v,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("repeat") => {
            if args.len() == 2 {
                let s = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let n = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::Int(n) => n as usize,
                    _ => return PropertyValue::Null,
                };
                PropertyValue::String(s.repeat(n))
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("concat") => {
            let mut result = String::new();
            for a in args {
                match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::String(s) => result.push_str(&s),
                    PropertyValue::Int(n) => result.push_str(&n.to_string()),
                    PropertyValue::Double(f) => result.push_str(&f.to_string()),
                    PropertyValue::Bool(b) => result.push_str(if b { "true" } else { "false" }),
                    _ => {}
                }
            }
            PropertyValue::String(result)
        }
        n if n.eq_ignore_ascii_case("tofloat") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Int(n) => PropertyValue::Double(n as f64),
                    PropertyValue::String(s) => s
                        .parse::<f64>()
                        .ok()
                        .map(PropertyValue::Double)
                        .unwrap_or(PropertyValue::Null),
                    v => v,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("tointeger") | n.eq_ignore_ascii_case("toint") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Double(f) => PropertyValue::Int(f as i64),
                    PropertyValue::String(s) => s
                        .parse::<i64>()
                        .ok()
                        .map(PropertyValue::Int)
                        .unwrap_or(PropertyValue::Null),
                    v => v,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("tostringlist") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::List(items) => PropertyValue::List(
                        items
                            .into_iter()
                            .map(|v| match v {
                                PropertyValue::String(s) => PropertyValue::String(s),
                                other => PropertyValue::String(other.to_string()),
                            })
                            .collect(),
                    ),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("tointegerlist") | n.eq_ignore_ascii_case("tointlist") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::List(items) => PropertyValue::List(
                        items
                            .into_iter()
                            .map(|v| match v {
                                PropertyValue::Int(n) => PropertyValue::Int(n),
                                PropertyValue::Double(f) => PropertyValue::Int(f as i64),
                                PropertyValue::String(s) => s
                                    .parse::<i64>()
                                    .ok()
                                    .map(PropertyValue::Int)
                                    .unwrap_or(PropertyValue::Null),
                                _ => PropertyValue::Null,
                            })
                            .collect(),
                    ),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("tofloatlist") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::List(items) => PropertyValue::List(
                        items
                            .into_iter()
                            .map(|v| match v {
                                PropertyValue::Double(f) => PropertyValue::Double(f),
                                PropertyValue::Int(n) => PropertyValue::Double(n as f64),
                                PropertyValue::String(s) => s
                                    .parse::<f64>()
                                    .ok()
                                    .map(PropertyValue::Double)
                                    .unwrap_or(PropertyValue::Null),
                                _ => PropertyValue::Null,
                            })
                            .collect(),
                    ),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("tobooleanlist") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::List(items) => PropertyValue::List(
                        items
                            .into_iter()
                            .map(|v| match v {
                                PropertyValue::Bool(b) => PropertyValue::Bool(b),
                                PropertyValue::String(s) if s.eq_ignore_ascii_case("true") => {
                                    PropertyValue::Bool(true)
                                }
                                PropertyValue::String(s) if s.eq_ignore_ascii_case("false") => {
                                    PropertyValue::Bool(false)
                                }
                                _ => PropertyValue::Null,
                            })
                            .collect(),
                    ),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        // Math functions
        n if n.eq_ignore_ascii_case("ceil") | n.eq_ignore_ascii_case("ceiling") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Double(f) => PropertyValue::Double(f.ceil()),
                    PropertyValue::Int(n) => PropertyValue::Int(n),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("floor") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Double(f) => PropertyValue::Double(f.floor()),
                    PropertyValue::Int(n) => PropertyValue::Int(n),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("round") => {
            let val = args
                .first()
                .map(|a| eval_expression_with_catalog(a, bindings, storage, catalog));
            let precision = args.get(1).and_then(|a| {
                match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Int(p) => Some(p),
                    _ => None,
                }
            });
            match val {
                Some(PropertyValue::Double(f)) => {
                    if let Some(p) = precision {
                        let mul = 10f64.powi(p as i32);
                        PropertyValue::Double((f * mul).round() / mul)
                    } else {
                        PropertyValue::Double(f.round())
                    }
                }
                Some(PropertyValue::Int(n)) => PropertyValue::Int(n),
                _ => PropertyValue::Null,
            }
        }
        n if n.eq_ignore_ascii_case("sqrt") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Double(f) if f >= 0.0 => PropertyValue::Double(f.sqrt()),
                    PropertyValue::Int(n) if n >= 0 => PropertyValue::Double((n as f64).sqrt()),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("sign") | n.eq_ignore_ascii_case("signum") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Int(n) => PropertyValue::Int(n.signum()),
                    PropertyValue::Double(f) => PropertyValue::Double(f.signum()),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("log") | n.eq_ignore_ascii_case("ln") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Double(f) if f > 0.0 => PropertyValue::Double(f.ln()),
                    PropertyValue::Int(n) if n > 0 => PropertyValue::Double((n as f64).ln()),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("log10") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Double(f) if f > 0.0 => PropertyValue::Double(f.log10()),
                    PropertyValue::Int(n) if n > 0 => PropertyValue::Double((n as f64).log10()),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("exp") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Double(f) => PropertyValue::Double(f.exp()),
                    PropertyValue::Int(n) => PropertyValue::Double((n as f64).exp()),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("pow") | n.eq_ignore_ascii_case("power") => {
            if args.len() == 2 {
                let base = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::Double(f) => f,
                    PropertyValue::Int(n) => n as f64,
                    _ => return PropertyValue::Null,
                };
                let exp = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::Double(f) => f,
                    PropertyValue::Int(n) => n as f64,
                    _ => return PropertyValue::Null,
                };
                PropertyValue::Double(base.powf(exp))
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("pi") => PropertyValue::Double(std::f64::consts::PI),
        n if n.eq_ignore_ascii_case("e") => PropertyValue::Double(std::f64::consts::E),
        n if n.eq_ignore_ascii_case("rand") => PropertyValue::Double(rand_simple()),
        // List functions
        n if n.eq_ignore_ascii_case("head") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::List(l) => l.into_iter().next().unwrap_or(PropertyValue::Null),
                    PropertyValue::Path(p) => p
                        .vertices
                        .first()
                        .cloned()
                        .map(PropertyValue::Vertex)
                        .unwrap_or(PropertyValue::Null),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("last") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::List(l) => l.into_iter().last().unwrap_or(PropertyValue::Null),
                    PropertyValue::Path(p) => p
                        .vertices
                        .last()
                        .cloned()
                        .map(PropertyValue::Vertex)
                        .unwrap_or(PropertyValue::Null),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("tail") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::List(mut l) => {
                        if !l.is_empty() {
                            l.remove(0);
                        }
                        PropertyValue::List(l)
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        // APOC-style list utilities (also match underscore versions since parser doesn't support dots in function names)
        n if n.eq_ignore_ascii_case("apoc_coll_union") || n.eq_ignore_ascii_case("union") => {
            if args.len() == 2 {
                let a = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let b = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let mut result = a;
                for item in b {
                    if !result.contains(&item) {
                        result.push(item);
                    }
                }
                PropertyValue::List(result)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_coll_intersection")
            || n.eq_ignore_ascii_case("intersection") =>
        {
            if args.len() == 2 {
                let a = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let b = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                PropertyValue::List(a.into_iter().filter(|x| b.contains(x)).collect())
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_coll_contains") || n.eq_ignore_ascii_case("contains") => {
            if args.len() == 2 {
                let list = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let item = eval_expression_with_catalog(&args[1], bindings, storage, catalog);
                PropertyValue::Bool(list.contains(&item))
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_coll_containsall")
            || n.eq_ignore_ascii_case("contains_all") =>
        {
            if args.len() == 2 {
                let list = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let items = match eval_expression_with_catalog(&args[1], bindings, storage, catalog)
                {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                PropertyValue::Bool(items.iter().all(|item| list.contains(item)))
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_coll_set")
            || n.eq_ignore_ascii_case("set_collection") =>
        {
            args.first()
                .map(
                    |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                        PropertyValue::List(l) => {
                            let mut result = Vec::new();
                            for item in l {
                                if !result.contains(&item) {
                                    result.push(item);
                                }
                            }
                            PropertyValue::List(result)
                        }
                        _ => PropertyValue::Null,
                    },
                )
                .unwrap_or(PropertyValue::Null)
        }
        n if n.eq_ignore_ascii_case("apoc_coll_sort") || n.eq_ignore_ascii_case("sort") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::List(mut l) => {
                        l.sort_by(|a, b| match (a, b) {
                            (PropertyValue::Int(x), PropertyValue::Int(y)) => x.cmp(y),
                            (PropertyValue::Double(x), PropertyValue::Double(y)) => {
                                x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
                            }
                            (PropertyValue::String(x), PropertyValue::String(y)) => x.cmp(y),
                            _ => std::cmp::Ordering::Equal,
                        });
                        PropertyValue::List(l)
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("apoc_coll_remove") || n.eq_ignore_ascii_case("remove") => {
            if args.len() == 2 {
                let mut list =
                    match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                        PropertyValue::List(l) => l,
                        _ => return PropertyValue::Null,
                    };
                let idx = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::Int(n) => n as usize,
                    _ => return PropertyValue::Null,
                };
                if idx < list.len() {
                    list.remove(idx);
                }
                PropertyValue::List(list)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_map_merge")
            || n.eq_ignore_ascii_case("merge_maps")
            || n.eq_ignore_ascii_case("merge") =>
        {
            let mut result = Vec::new();
            for a in args {
                if let PropertyValue::Map(entries) =
                    eval_expression_with_catalog(a, bindings, storage, catalog)
                {
                    for (k, v) in entries {
                        // Later maps override earlier ones
                        if let Some(pos) = result
                            .iter()
                            .position(|(key, _): &(String, PropertyValue)| key == &k)
                        {
                            result[pos] = (k, v);
                        } else {
                            result.push((k, v));
                        }
                    }
                }
            }
            PropertyValue::Map(result)
        }
        n if n.eq_ignore_ascii_case("apoc_text_join")
            || n.eq_ignore_ascii_case("text_join")
            || n.eq_ignore_ascii_case("join") =>
        {
            if args.len() == 2 {
                let list = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let sep = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let parts: Vec<String> = list
                    .iter()
                    .map(|v| match v {
                        PropertyValue::String(s) => s.clone(),
                        other => format!("{}", other),
                    })
                    .collect();
                PropertyValue::String(parts.join(&sep))
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_capitalize")
            || n.eq_ignore_ascii_case("capitalize") =>
        {
            args.first()
                .map(
                    |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                        PropertyValue::String(s) => {
                            let mut chars = s.chars();
                            match chars.next() {
                                Some(first) => PropertyValue::String(format!(
                                    "{}{}",
                                    first.to_uppercase(),
                                    chars.as_str().to_lowercase()
                                )),
                                None => PropertyValue::String(s),
                            }
                        }
                        _ => PropertyValue::Null,
                    },
                )
                .unwrap_or(PropertyValue::Null)
        }
        n if n.eq_ignore_ascii_case("apoc_text_decapitalize")
            || n.eq_ignore_ascii_case("decapitalize") =>
        {
            args.first()
                .map(
                    |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                        PropertyValue::String(s) => {
                            let mut chars = s.chars();
                            match chars.next() {
                                Some(first) => PropertyValue::String(format!(
                                    "{}{}",
                                    first.to_lowercase(),
                                    chars.as_str()
                                )),
                                None => PropertyValue::String(s),
                            }
                        }
                        _ => PropertyValue::Null,
                    },
                )
                .unwrap_or(PropertyValue::Null)
        }
        n if n.eq_ignore_ascii_case("apoc_text_lpad") || n.eq_ignore_ascii_case("lpad") => {
            if args.len() >= 2 {
                let text = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let width = match eval_expression_with_catalog(&args[1], bindings, storage, catalog)
                {
                    PropertyValue::Int(n) => n as usize,
                    _ => return PropertyValue::Null,
                };
                let pad_char = if args.len() >= 3 {
                    match eval_expression_with_catalog(&args[2], bindings, storage, catalog) {
                        PropertyValue::String(s) if !s.is_empty() => s.chars().next().unwrap(),
                        _ => ' ',
                    }
                } else {
                    ' '
                };
                let len = text.chars().count();
                if len >= width {
                    PropertyValue::String(text)
                } else {
                    let pad: String = std::iter::repeat_n(pad_char, width - len).collect();
                    PropertyValue::String(pad + &text)
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_rpad") || n.eq_ignore_ascii_case("rpad") => {
            if args.len() >= 2 {
                let text = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let width = match eval_expression_with_catalog(&args[1], bindings, storage, catalog)
                {
                    PropertyValue::Int(n) => n as usize,
                    _ => return PropertyValue::Null,
                };
                let pad_char = if args.len() >= 3 {
                    match eval_expression_with_catalog(&args[2], bindings, storage, catalog) {
                        PropertyValue::String(s) if !s.is_empty() => s.chars().next().unwrap(),
                        _ => ' ',
                    }
                } else {
                    ' '
                };
                let len = text.chars().count();
                if len >= width {
                    PropertyValue::String(text)
                } else {
                    let pad: String = std::iter::repeat_n(pad_char, width - len).collect();
                    PropertyValue::String(text + &pad)
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_format")
            || n.eq_ignore_ascii_case("apoc_date_format")
            || n.eq_ignore_ascii_case("format") =>
        {
            if args.is_empty() {
                return PropertyValue::Null;
            }
            match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                // apoc.date.format(timestamp, pattern)
                PropertyValue::Int(ts) => {
                    if args.len() < 2 {
                        return PropertyValue::Null;
                    }
                    let fmt =
                        match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                            PropertyValue::String(s) => s,
                            _ => return PropertyValue::Null,
                        };
                    let dt = std::time::UNIX_EPOCH + std::time::Duration::from_secs(ts as u64);
                    let datetime: chrono::DateTime<chrono::Utc> = dt.into();
                    let chrono_fmt = match fmt.as_str() {
                        "yyyy-MM-dd" => "%Y-%m-%d",
                        "yyyy-MM-dd HH:mm:ss" => "%Y-%m-%d %H:%M:%S",
                        _ => "%Y-%m-%d",
                    };
                    PropertyValue::String(datetime.format(chrono_fmt).to_string())
                }
                // apoc.text.format(pattern, ...values)
                PropertyValue::String(fmt) => {
                    let vals: Vec<String> = args[1..]
                        .iter()
                        .map(|a| {
                            match eval_expression_with_catalog(a, bindings, storage, catalog) {
                                PropertyValue::String(s) => s,
                                other => format!("{}", other),
                            }
                        })
                        .collect();
                    PropertyValue::String(fmt.replace("{}", &vals.join(", ")))
                }
                _ => PropertyValue::Null,
            }
        }
        n if n.eq_ignore_ascii_case("apoc_date_currenttimestamp")
            || n.eq_ignore_ascii_case("current_timestamp_ms")
            || n.eq_ignore_ascii_case("currentTimestamp") =>
        {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            PropertyValue::Int(now)
        }
        n if n.eq_ignore_ascii_case("apoc_date_parse") || n.eq_ignore_ascii_case("date_parse") => {
            if args.len() >= 2 {
                let s = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let fmt = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let tz = if args.len() >= 3 {
                    match eval_expression_with_catalog(&args[2], bindings, storage, catalog) {
                        PropertyValue::String(s) => Some(s),
                        _ => None,
                    }
                } else {
                    None
                };
                match parse_date_custom(&s, &fmt, tz.as_deref()) {
                    Some(ts) => PropertyValue::Int(ts),
                    None => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_date_format")
            || n.eq_ignore_ascii_case("date_format") =>
        {
            if args.len() >= 2 {
                let ts = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::Int(n) => n,
                    _ => return PropertyValue::Null,
                };
                let fmt = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let tz = if args.len() >= 3 {
                    match eval_expression_with_catalog(&args[2], bindings, storage, catalog) {
                        PropertyValue::String(s) => Some(s),
                        _ => None,
                    }
                } else {
                    None
                };
                match format_date_custom(ts, &fmt, tz.as_deref()) {
                    Some(s) => PropertyValue::String(s),
                    None => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_date_add") || n.eq_ignore_ascii_case("date_add") => {
            if args.len() == 3 {
                let ts = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::Int(n) => n,
                    _ => return PropertyValue::Null,
                };
                let value = match eval_expression_with_catalog(&args[1], bindings, storage, catalog)
                {
                    PropertyValue::Int(n) => n,
                    _ => return PropertyValue::Null,
                };
                let unit = match eval_expression_with_catalog(&args[2], bindings, storage, catalog)
                {
                    PropertyValue::String(s) => s.to_lowercase(),
                    _ => return PropertyValue::Null,
                };
                let result = match unit.as_str() {
                    "ms" | "millisecond" | "milliseconds" => ts + value,
                    "s" | "second" | "seconds" => ts + value * 1000,
                    "m" | "minute" | "minutes" => ts + value * 60 * 1000,
                    "h" | "hour" | "hours" => ts + value * 60 * 60 * 1000,
                    "d" | "day" | "days" => ts + value * 24 * 60 * 60 * 1000,
                    "w" | "week" | "weeks" => ts + value * 7 * 24 * 60 * 60 * 1000,
                    "month" | "months" => {
                        let dt = chrono::DateTime::from_timestamp_millis(ts).unwrap_or_default();
                        let dt = dt + chrono::Months::new(value as u32);
                        dt.timestamp_millis()
                    }
                    "year" | "years" => {
                        let dt = chrono::DateTime::from_timestamp_millis(ts).unwrap_or_default();
                        let dt = dt + chrono::Months::new((value * 12) as u32);
                        dt.timestamp_millis()
                    }
                    _ => return PropertyValue::Null,
                };
                PropertyValue::Int(result)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_date_fields")
            || n.eq_ignore_ascii_case("date_fields") =>
        {
            if !args.is_empty() {
                let ts = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::Int(n) => n,
                    _ => return PropertyValue::Null,
                };
                let tz = if args.len() >= 2 {
                    match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                        PropertyValue::String(s) => s.parse::<chrono::FixedOffset>().ok(),
                        _ => None,
                    }
                } else {
                    None
                };
                let dt: Option<chrono::DateTime<chrono::FixedOffset>> = if let Some(off) = tz {
                    chrono::DateTime::from_timestamp_millis(ts).map(|d| d.with_timezone(&off))
                } else {
                    chrono::DateTime::from_timestamp_millis(ts)
                        .map(|d| d.with_timezone(&chrono::FixedOffset::east_opt(0).unwrap()))
                };
                match dt {
                    Some(dt) => {
                        let fields = vec![
                            ("year".to_string(), PropertyValue::Int(dt.year() as i64)),
                            ("month".to_string(), PropertyValue::Int(dt.month() as i64)),
                            ("day".to_string(), PropertyValue::Int(dt.day() as i64)),
                            ("hour".to_string(), PropertyValue::Int(dt.hour() as i64)),
                            ("minute".to_string(), PropertyValue::Int(dt.minute() as i64)),
                            ("second".to_string(), PropertyValue::Int(dt.second() as i64)),
                            (
                                "millisecond".to_string(),
                                PropertyValue::Int((ts % 1000).abs()),
                            ),
                            (
                                "dayOfWeek".to_string(),
                                PropertyValue::Int(dt.weekday().num_days_from_monday() as i64 + 1),
                            ),
                            (
                                "dayOfYear".to_string(),
                                PropertyValue::Int(dt.ordinal() as i64),
                            ),
                        ];
                        PropertyValue::Map(fields)
                    }
                    None => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_coll_flatten") || n.eq_ignore_ascii_case("flatten") => {
            args.first()
                .map(
                    |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                        PropertyValue::List(l) => {
                            let mut result = Vec::new();
                            for item in l {
                                match item {
                                    PropertyValue::List(inner) => result.extend(inner),
                                    other => result.push(other),
                                }
                            }
                            PropertyValue::List(result)
                        }
                        _ => PropertyValue::Null,
                    },
                )
                .unwrap_or(PropertyValue::Null)
        }
        n if n.eq_ignore_ascii_case("apoc_coll_sum") || n.eq_ignore_ascii_case("sum_list") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::List(l) => {
                        let mut total = 0i64;
                        for item in l {
                            match item {
                                PropertyValue::Int(n) => total += n,
                                PropertyValue::Double(d) => {
                                    return PropertyValue::Double(total as f64 + d)
                                }
                                _ => {}
                            }
                        }
                        PropertyValue::Int(total)
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("apoc_coll_avg") || n.eq_ignore_ascii_case("avg_list") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::List(l) => {
                        let mut total = 0.0f64;
                        let mut count = 0usize;
                        for item in l {
                            match item {
                                PropertyValue::Int(n) => {
                                    total += n as f64;
                                    count += 1;
                                }
                                PropertyValue::Double(d) => {
                                    total += d;
                                    count += 1;
                                }
                                _ => {}
                            }
                        }
                        if count > 0 {
                            PropertyValue::Double(total / count as f64)
                        } else {
                            PropertyValue::Null
                        }
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("apoc_map_setKey")
            || n.eq_ignore_ascii_case("set_key")
            || n.eq_ignore_ascii_case("setkey") =>
        {
            if args.len() == 3 {
                let mut map =
                    match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                        PropertyValue::Map(m) => m,
                        _ => return PropertyValue::Null,
                    };
                let key = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let value = eval_expression_with_catalog(&args[2], bindings, storage, catalog);
                if let Some(pos) = map
                    .iter()
                    .position(|(k, _): &(String, PropertyValue)| k == &key)
                {
                    map[pos] = (key, value);
                } else {
                    map.push((key, value));
                }
                PropertyValue::Map(map)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_replace")
            || n.eq_ignore_ascii_case("replace_all") =>
        {
            if args.len() == 3 {
                let text = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let from = match eval_expression_with_catalog(&args[1], bindings, storage, catalog)
                {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let to = match eval_expression_with_catalog(&args[2], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                PropertyValue::String(text.replace(&from, &to))
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_split") || n.eq_ignore_ascii_case("split_text") => {
            if args.len() == 2 {
                let text = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let sep = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                PropertyValue::List(
                    text.split(&sep)
                        .map(|s| PropertyValue::String(s.to_string()))
                        .collect(),
                )
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_reverse") || n.eq_ignore_ascii_case("reverse") => {
            if args.len() == 1 {
                let text = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                PropertyValue::String(text.chars().rev().collect())
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_indexOf") || n.eq_ignore_ascii_case("indexOf") => {
            if args.len() == 2 {
                let arg0 = eval_expression_with_catalog(&args[0], bindings, storage, catalog);
                let arg1 = eval_expression_with_catalog(&args[1], bindings, storage, catalog);
                match (arg0, arg1) {
                    (PropertyValue::String(text), PropertyValue::String(substr)) => {
                        PropertyValue::Int(text.find(&substr).map(|i| i as i64).unwrap_or(-1))
                    }
                    (PropertyValue::List(list), needle) => PropertyValue::Int(
                        list.iter()
                            .position(|x| x == &needle)
                            .map(|i| i as i64)
                            .unwrap_or(-1),
                    ),
                    _ => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_contains") || n.eq_ignore_ascii_case("contains") => {
            if args.len() == 2 {
                let arg0 = eval_expression_with_catalog(&args[0], bindings, storage, catalog);
                let arg1 = eval_expression_with_catalog(&args[1], bindings, storage, catalog);
                match (arg0, arg1) {
                    (PropertyValue::String(text), PropertyValue::String(substr)) => {
                        PropertyValue::Bool(text.contains(&substr))
                    }
                    (PropertyValue::List(list), needle) => {
                        PropertyValue::Bool(list.contains(&needle))
                    }
                    _ => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_regreplace")
            || n.eq_ignore_ascii_case("regreplace") =>
        {
            if args.len() == 3 {
                let text = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let pattern =
                    match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                        PropertyValue::String(s) => s,
                        _ => return PropertyValue::Null,
                    };
                let replacement =
                    match eval_expression_with_catalog(&args[2], bindings, storage, catalog) {
                        PropertyValue::String(s) => s,
                        _ => return PropertyValue::Null,
                    };
                if let Ok(re) = regex::Regex::new(&pattern) {
                    PropertyValue::String(re.replace_all(&text, replacement.as_str()).to_string())
                } else {
                    PropertyValue::Null
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_slug") || n.eq_ignore_ascii_case("slug") => {
            if args.len() == 1 {
                let text = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let slug: String = text
                    .to_lowercase()
                    .replace(|c: char| !c.is_alphanumeric() && c != ' ', " ")
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join("-");
                PropertyValue::String(slug)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_pad") || n.eq_ignore_ascii_case("pad") => {
            if args.len() >= 2 {
                let text = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let width = match eval_expression_with_catalog(&args[1], bindings, storage, catalog)
                {
                    PropertyValue::Int(n) => n as usize,
                    _ => return PropertyValue::Null,
                };
                let pad_char = if args.len() >= 3 {
                    match eval_expression_with_catalog(&args[2], bindings, storage, catalog) {
                        PropertyValue::String(s) if !s.is_empty() => s.chars().next().unwrap(),
                        _ => ' ',
                    }
                } else {
                    ' '
                };
                let mode = if args.len() >= 4 {
                    match eval_expression_with_catalog(&args[3], bindings, storage, catalog) {
                        PropertyValue::String(s) => s.to_lowercase(),
                        _ => "left".to_string(),
                    }
                } else {
                    "left".to_string()
                };
                let len = text.chars().count();
                let result = if len >= width {
                    text
                } else {
                    let pad_len = width - len;
                    let pad: String = std::iter::repeat_n(pad_char, pad_len).collect();
                    match mode.as_str() {
                        "right" => text + &pad,
                        "center" => {
                            let left_pad = pad_len / 2;
                            let right_pad = pad_len - left_pad;
                            let left: String = std::iter::repeat_n(pad_char, left_pad).collect();
                            let right: String = std::iter::repeat_n(pad_char, right_pad).collect();
                            left + &text + &right
                        }
                        _ => pad + &text,
                    }
                };
                PropertyValue::String(result)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_random")
            || n.eq_ignore_ascii_case("random_string") =>
        {
            let len = if !args.is_empty() {
                match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::Int(n) => n as usize,
                    _ => 10,
                }
            } else {
                10
            };
            let chars: Vec<char> = if args.len() >= 2 {
                match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::String(s) => s.chars().collect(),
                    _ => ('a'..='z').chain('A'..='Z').chain('0'..='9').collect(),
                }
            } else {
                ('a'..='z').chain('A'..='Z').chain('0'..='9').collect()
            };
            use rand::seq::SliceRandom;
            let mut rng = rand::thread_rng();
            let s: String = (0..len)
                .map(|_| *chars.choose(&mut rng).unwrap_or(&'a'))
                .collect();
            PropertyValue::String(s)
        }
        n if n.eq_ignore_ascii_case("apoc_text_compareignorecase")
            || n.eq_ignore_ascii_case("compareignorecase") =>
        {
            if args.len() == 2 {
                let a = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let b = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                PropertyValue::Int(a.to_lowercase().cmp(&b.to_lowercase()) as i8 as i64)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_charat") || n.eq_ignore_ascii_case("charat") => {
            if args.len() == 2 {
                let text = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let idx = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::Int(n) => n as usize,
                    _ => return PropertyValue::Null,
                };
                PropertyValue::String(
                    text.chars()
                        .nth(idx)
                        .map(|c| c.to_string())
                        .unwrap_or_default(),
                )
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_camelcase")
            || n.eq_ignore_ascii_case("camelcase") =>
        {
            if args.len() == 1 {
                let text = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let mut result = String::new();
                let mut capitalize_next = false;
                for (i, c) in text.chars().enumerate() {
                    if c == ' ' || c == '_' || c == '-' {
                        capitalize_next = true;
                    } else if i == 0 {
                        result.push(c.to_lowercase().next().unwrap_or(c));
                    } else if capitalize_next {
                        result.push(c.to_uppercase().next().unwrap_or(c));
                        capitalize_next = false;
                    } else {
                        result.push(c);
                    }
                }
                PropertyValue::String(result)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_snakecase")
            || n.eq_ignore_ascii_case("snakecase") =>
        {
            if args.len() == 1 {
                let text = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let mut result = String::new();
                let mut prev_lower = false;
                for c in text.chars() {
                    if c == ' ' || c == '-' {
                        result.push('_');
                        prev_lower = false;
                    } else if c.is_uppercase() {
                        if prev_lower {
                            result.push('_');
                        }
                        result.push(c.to_lowercase().next().unwrap_or(c));
                        prev_lower = true;
                    } else {
                        result.push(c);
                        prev_lower = c.is_lowercase() || c.is_numeric();
                    }
                }
                PropertyValue::String(result)
            } else {
                PropertyValue::Null
            }
        }
        // ─── Temporal functions ──────────────────────────────────
        n if n.eq_ignore_ascii_case("datetime") => {
            if args.is_empty() {
                // Current UTC datetime
                let now = chrono::Utc::now();
                let us = now.timestamp_micros();
                PropertyValue::ZonedDateTime(ZonedDateTime::new(us, 0, "UTC".into()))
            } else {
                // Parse from string or map
                match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => {
                        if let Ok(dt) = s.parse::<chrono::DateTime<chrono::Utc>>() {
                            PropertyValue::ZonedDateTime(ZonedDateTime::new(
                                dt.timestamp_micros(),
                                0,
                                "UTC".into(),
                            ))
                        } else {
                            PropertyValue::Null
                        }
                    }
                    PropertyValue::Map(entries) => {
                        let mut year = 1970i32;
                        let mut month = 1u32;
                        let mut day = 1u32;
                        let mut hour = 0u32;
                        let mut minute = 0u32;
                        let mut second = 0u32;
                        let mut nanosecond = 0u32;
                        let mut tz = "UTC".to_string();
                        let offset_min = 0i16;
                        let mut has_date_time_field = false;
                        let mut has_timezone = false;
                        for (k, v) in &entries {
                            match (k.as_str(), v) {
                                ("year", PropertyValue::Int(y)) => {
                                    year = *y as i32;
                                    has_date_time_field = true;
                                }
                                ("month", PropertyValue::Int(m)) => {
                                    month = *m as u32;
                                    has_date_time_field = true;
                                }
                                ("day", PropertyValue::Int(d)) => {
                                    day = *d as u32;
                                    has_date_time_field = true;
                                }
                                ("hour", PropertyValue::Int(h)) => {
                                    hour = *h as u32;
                                    has_date_time_field = true;
                                }
                                ("minute", PropertyValue::Int(m)) => {
                                    minute = *m as u32;
                                    has_date_time_field = true;
                                }
                                ("second", PropertyValue::Int(s)) => {
                                    second = *s as u32;
                                    has_date_time_field = true;
                                }
                                ("millisecond", PropertyValue::Int(ms)) => {
                                    nanosecond = (*ms as u32) * 1_000_000;
                                    has_date_time_field = true;
                                }
                                ("microsecond", PropertyValue::Int(us)) => {
                                    nanosecond = (*us as u32) * 1_000;
                                    has_date_time_field = true;
                                }
                                ("nanosecond", PropertyValue::Int(ns)) => {
                                    nanosecond = *ns as u32;
                                    has_date_time_field = true;
                                }
                                ("timezone", PropertyValue::String(s)) => {
                                    tz = s.clone();
                                    has_timezone = true;
                                }
                                _ => {}
                            }
                        }
                        // If only timezone is provided, use current time
                        if !has_date_time_field && has_timezone {
                            let now = chrono::Utc::now();
                            let us = now.timestamp_micros();
                            return PropertyValue::ZonedDateTime(ZonedDateTime::new(
                                us, offset_min, tz,
                            ));
                        }
                        if let Some(dt) = chrono::NaiveDate::from_ymd_opt(year, month, day)
                            .and_then(|d| d.and_hms_nano_opt(hour, minute, second, nanosecond))
                        {
                            let us = dt.and_utc().timestamp_micros();
                            PropertyValue::ZonedDateTime(ZonedDateTime::new(us, offset_min, tz))
                        } else {
                            PropertyValue::Null
                        }
                    }
                    _ => PropertyValue::Null,
                }
            }
        }
        n if n.eq_ignore_ascii_case("localdatetime") => {
            if args.is_empty() {
                let now = chrono::Local::now();
                PropertyValue::LocalDateTime(LocalDateTime::from_microseconds(
                    now.timestamp_micros(),
                ))
            } else {
                match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => {
                        if let Ok(dt) = s.parse::<chrono::NaiveDateTime>() {
                            PropertyValue::LocalDateTime(LocalDateTime::from_microseconds(
                                dt.and_utc().timestamp_micros(),
                            ))
                        } else {
                            PropertyValue::Null
                        }
                    }
                    PropertyValue::Map(entries) => {
                        let mut year = 1970i32;
                        let mut month = 1u32;
                        let mut day = 1u32;
                        let mut hour = 0u32;
                        let mut minute = 0u32;
                        let mut second = 0u32;
                        let mut nanosecond = 0u32;
                        for (k, v) in entries {
                            match (k.as_str(), v) {
                                ("year", PropertyValue::Int(y)) => year = y as i32,
                                ("month", PropertyValue::Int(m)) => month = m as u32,
                                ("day", PropertyValue::Int(d)) => day = d as u32,
                                ("hour", PropertyValue::Int(h)) => hour = h as u32,
                                ("minute", PropertyValue::Int(m)) => minute = m as u32,
                                ("second", PropertyValue::Int(s)) => second = s as u32,
                                ("millisecond", PropertyValue::Int(ms)) => {
                                    nanosecond = (ms as u32) * 1_000_000
                                }
                                ("microsecond", PropertyValue::Int(us)) => {
                                    nanosecond = (us as u32) * 1_000
                                }
                                ("nanosecond", PropertyValue::Int(ns)) => nanosecond = ns as u32,
                                _ => {}
                            }
                        }
                        if let Some(dt) = chrono::NaiveDate::from_ymd_opt(year, month, day)
                            .and_then(|d| d.and_hms_nano_opt(hour, minute, second, nanosecond))
                        {
                            PropertyValue::LocalDateTime(LocalDateTime::from_microseconds(
                                dt.and_utc().timestamp_micros(),
                            ))
                        } else {
                            PropertyValue::Null
                        }
                    }
                    _ => PropertyValue::Null,
                }
            }
        }
        n if n.eq_ignore_ascii_case("date") => {
            if args.is_empty() {
                let now = chrono::Local::now();
                let days = now.num_days_from_ce() as i64 - 719162; // days since 1970-01-01
                PropertyValue::Date(Date::from_days(days))
            } else {
                match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => {
                        if let Ok(d) = s.parse::<chrono::NaiveDate>() {
                            let days = d.num_days_from_ce() as i64 - 719162;
                            PropertyValue::Date(Date::from_days(days))
                        } else {
                            PropertyValue::Null
                        }
                    }
                    PropertyValue::Map(entries) => {
                        let mut year = 1970i32;
                        let mut month = 1u32;
                        let mut day = 1u32;
                        for (k, v) in entries {
                            match (k.as_str(), v) {
                                ("year", PropertyValue::Int(y)) => year = y as i32,
                                ("month", PropertyValue::Int(m)) => month = m as u32,
                                ("day", PropertyValue::Int(d)) => day = d as u32,
                                _ => {}
                            }
                        }
                        if let Some(d) = chrono::NaiveDate::from_ymd_opt(year, month, day) {
                            let days = d.num_days_from_ce() as i64 - 719162;
                            PropertyValue::Date(Date::from_days(days))
                        } else {
                            PropertyValue::Null
                        }
                    }
                    _ => PropertyValue::Null,
                }
            }
        }
        n if n.eq_ignore_ascii_case("time") => {
            if args.is_empty() {
                let now = chrono::Utc::now();
                let us = now.timestamp_micros() % 86_400_000_000;
                PropertyValue::LocalTime(LocalTime::from_microseconds(us))
            } else {
                match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => {
                        if let Ok(t) = s.parse::<chrono::NaiveTime>() {
                            let us = t.num_seconds_from_midnight() as i64 * 1_000_000
                                + t.nanosecond() as i64 / 1_000;
                            PropertyValue::LocalTime(LocalTime::from_microseconds(us))
                        } else {
                            PropertyValue::Null
                        }
                    }
                    PropertyValue::Map(entries) => {
                        let mut hour = 0u32;
                        let mut minute = 0u32;
                        let mut second = 0u32;
                        let mut nanosecond = 0u32;
                        for (k, v) in entries {
                            match (k.as_str(), v) {
                                ("hour", PropertyValue::Int(h)) => hour = h as u32,
                                ("minute", PropertyValue::Int(m)) => minute = m as u32,
                                ("second", PropertyValue::Int(s)) => second = s as u32,
                                ("millisecond", PropertyValue::Int(ms)) => {
                                    nanosecond = (ms as u32) * 1_000_000
                                }
                                ("microsecond", PropertyValue::Int(us)) => {
                                    nanosecond = (us as u32) * 1_000
                                }
                                ("nanosecond", PropertyValue::Int(ns)) => nanosecond = ns as u32,
                                _ => {}
                            }
                        }
                        if let Some(t) =
                            chrono::NaiveTime::from_hms_nano_opt(hour, minute, second, nanosecond)
                        {
                            let us = t.num_seconds_from_midnight() as i64 * 1_000_000
                                + t.nanosecond() as i64 / 1_000;
                            PropertyValue::LocalTime(LocalTime::from_microseconds(us))
                        } else {
                            PropertyValue::Null
                        }
                    }
                    _ => PropertyValue::Null,
                }
            }
        }
        n if n.eq_ignore_ascii_case("localtime") => {
            if args.is_empty() {
                let now = chrono::Local::now();
                let t = now.time();
                let us = t.num_seconds_from_midnight() as i64 * 1_000_000
                    + t.nanosecond() as i64 / 1_000;
                PropertyValue::LocalTime(LocalTime::from_microseconds(us))
            } else {
                match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => {
                        if let Ok(t) = s.parse::<chrono::NaiveTime>() {
                            let us = t.num_seconds_from_midnight() as i64 * 1_000_000
                                + t.nanosecond() as i64 / 1_000;
                            PropertyValue::LocalTime(LocalTime::from_microseconds(us))
                        } else {
                            PropertyValue::Null
                        }
                    }
                    PropertyValue::Map(entries) => {
                        let mut hour = 0u32;
                        let mut minute = 0u32;
                        let mut second = 0u32;
                        let mut nanosecond = 0u32;
                        for (k, v) in entries {
                            match (k.as_str(), v) {
                                ("hour", PropertyValue::Int(h)) => hour = h as u32,
                                ("minute", PropertyValue::Int(m)) => minute = m as u32,
                                ("second", PropertyValue::Int(s)) => second = s as u32,
                                ("millisecond", PropertyValue::Int(ms)) => {
                                    nanosecond = (ms as u32) * 1_000_000
                                }
                                ("microsecond", PropertyValue::Int(us)) => {
                                    nanosecond = (us as u32) * 1_000
                                }
                                ("nanosecond", PropertyValue::Int(ns)) => nanosecond = ns as u32,
                                _ => {}
                            }
                        }
                        if let Some(t) =
                            chrono::NaiveTime::from_hms_nano_opt(hour, minute, second, nanosecond)
                        {
                            let us = t.num_seconds_from_midnight() as i64 * 1_000_000
                                + t.nanosecond() as i64 / 1_000;
                            PropertyValue::LocalTime(LocalTime::from_microseconds(us))
                        } else {
                            PropertyValue::Null
                        }
                    }
                    _ => PropertyValue::Null,
                }
            }
        }
        n if n.eq_ignore_ascii_case("duration") => {
            if args.is_empty() {
                PropertyValue::Duration(Duration::new(0, 0, 0))
            } else {
                match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => {
                        // Parse ISO 8601 duration: P[n]Y[n]M[n]DT[n]H[n]M[n]S
                        parse_iso_duration(&s)
                    }
                    PropertyValue::Map(entries) => {
                        let mut months = 0i64;
                        let mut days = 0i64;
                        let mut microseconds = 0i64;
                        for (k, v) in entries {
                            match (k.as_str(), v) {
                                ("months", PropertyValue::Int(m)) => months = m,
                                ("days", PropertyValue::Int(d)) => days = d,
                                ("hours", PropertyValue::Int(h)) => {
                                    microseconds += h * 3_600_000_000
                                }
                                ("minutes", PropertyValue::Int(m)) => {
                                    microseconds += m * 60_000_000
                                }
                                ("seconds", PropertyValue::Int(s)) => microseconds += s * 1_000_000,
                                ("milliseconds", PropertyValue::Int(ms)) => {
                                    microseconds += ms * 1_000
                                }
                                ("microseconds", PropertyValue::Int(us)) => microseconds += us,
                                ("nanoseconds", PropertyValue::Int(ns)) => {
                                    microseconds += ns / 1_000
                                }
                                _ => {}
                            }
                        }
                        PropertyValue::Duration(Duration::new(months, days, microseconds))
                    }
                    _ => PropertyValue::Null,
                }
            }
        }
        // ─── Spatial functions ───────────────────────────────────
        n if n.eq_ignore_ascii_case("point") => {
            if args.len() == 1 {
                match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::Map(entries) => {
                        let mut x = 0.0f64;
                        let mut y = 0.0f64;
                        let mut z = None::<f64>;
                        let mut crs = Crs::Cartesian2D;
                        for (k, v) in entries {
                            match (k.as_str(), v) {
                                ("x", PropertyValue::Double(vx)) => x = vx,
                                ("x", PropertyValue::Int(vx)) => x = vx as f64,
                                ("y", PropertyValue::Double(vy)) => y = vy,
                                ("y", PropertyValue::Int(vy)) => y = vy as f64,
                                ("z", PropertyValue::Double(vz)) => z = Some(vz),
                                ("z", PropertyValue::Int(vz)) => z = Some(vz as f64),
                                ("crs", PropertyValue::String(s)) => {
                                    crs = match s.as_str() {
                                        "wgs-84" => Crs::WGS84,
                                        "cartesian" => Crs::Cartesian2D,
                                        "cartesian-3d" => Crs::Cartesian3D,
                                        "wgs-84-3d" => Crs::WGS843D,
                                        _ => Crs::Cartesian2D,
                                    };
                                }
                                _ => {}
                            }
                        }
                        if let Some(z_val) = z {
                            PropertyValue::Point3D(Point3D::new(crs, x, y, z_val))
                        } else {
                            PropertyValue::Point2D(Point2D::new(crs, x, y))
                        }
                    }
                    _ => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("distance") => {
            if args.len() == 2 {
                let p1 = eval_expression_with_catalog(&args[0], bindings, storage, catalog);
                let p2 = eval_expression_with_catalog(&args[1], bindings, storage, catalog);
                match (&p1, &p2) {
                    (PropertyValue::Point2D(a), PropertyValue::Point2D(b)) => {
                        let dx = a.x - b.x;
                        let dy = a.y - b.y;
                        PropertyValue::Double((dx * dx + dy * dy).sqrt())
                    }
                    (PropertyValue::Point3D(a), PropertyValue::Point3D(b)) => {
                        let dx = a.x - b.x;
                        let dy = a.y - b.y;
                        let dz = a.z - b.z;
                        PropertyValue::Double((dx * dx + dy * dy + dz * dz).sqrt())
                    }
                    _ => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        // ─── String functions ────────────────────────────────────
        n if n.eq_ignore_ascii_case("startswith") => {
            if args.len() == 2 {
                let s = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let prefix =
                    match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                        PropertyValue::String(s) => s,
                        _ => return PropertyValue::Null,
                    };
                PropertyValue::Bool(s.starts_with(&prefix))
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("endswith") => {
            if args.len() == 2 {
                let s = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let suffix =
                    match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                        PropertyValue::String(s) => s,
                        _ => return PropertyValue::Null,
                    };
                PropertyValue::Bool(s.ends_with(&suffix))
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_startswith")
            || n.eq_ignore_ascii_case("starts_with") =>
        {
            if args.len() == 2 {
                let s = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let prefix =
                    match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                        PropertyValue::String(s) => s,
                        _ => return PropertyValue::Null,
                    };
                PropertyValue::Bool(s.starts_with(&prefix))
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_endswith")
            || n.eq_ignore_ascii_case("ends_with") =>
        {
            if args.len() == 2 {
                let s = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let suffix =
                    match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                        PropertyValue::String(s) => s,
                        _ => return PropertyValue::Null,
                    };
                PropertyValue::Bool(s.ends_with(&suffix))
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_isempty") || n.eq_ignore_ascii_case("is_empty") => {
            if args.len() == 1 {
                let s = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                PropertyValue::Bool(s.is_empty())
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_urlencode")
            || n.eq_ignore_ascii_case("urlencode") =>
        {
            if args.len() == 1 {
                let s = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                PropertyValue::String(url_encode(&s))
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_urldecode")
            || n.eq_ignore_ascii_case("urldecode") =>
        {
            if args.len() == 1 {
                let s = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                match url_decode(&s) {
                    Some(decoded) => PropertyValue::String(decoded),
                    None => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_base64encode")
            || n.eq_ignore_ascii_case("base64encode") =>
        {
            if args.len() == 1 {
                let s = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                PropertyValue::String(base64_encode(&s))
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_base64decode")
            || n.eq_ignore_ascii_case("base64decode") =>
        {
            if args.len() == 1 {
                let s = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                match base64_decode(&s) {
                    Some(decoded) => PropertyValue::String(decoded),
                    None => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_soundex") || n.eq_ignore_ascii_case("soundex") => {
            if args.len() == 1 {
                let s = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                PropertyValue::String(soundex(&s))
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_striptags")
            || n.eq_ignore_ascii_case("strip_tags") =>
        {
            if args.len() == 1 {
                let s = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                PropertyValue::String(strip_html_tags(&s))
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_doublemetaphone")
            || n.eq_ignore_ascii_case("double_metaphone") =>
        {
            if args.len() == 1 {
                let s = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let (primary, secondary) = double_metaphone(&s);
                PropertyValue::List(vec![
                    PropertyValue::String(primary),
                    PropertyValue::String(secondary),
                ])
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("regexmatch") => {
            if args.len() == 2 {
                let s = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let pat = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                // Simple substring match (as requested — not full regex)
                PropertyValue::Bool(s.contains(&pat))
            } else {
                PropertyValue::Null
            }
        }
        // ─── Math list functions ─────────────────────────────────
        n if n.eq_ignore_ascii_case("min") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::List(l) => {
                        let mut min_val: Option<PropertyValue> = None;
                        for item in l {
                            let candidate = match &item {
                                PropertyValue::Int(n) => PropertyValue::Int(*n),
                                PropertyValue::Double(d) => PropertyValue::Double(*d),
                                PropertyValue::String(s) => PropertyValue::String(s.clone()),
                                _ => continue,
                            };
                            min_val = Some(match min_val {
                                Some(ref current) => {
                                    let is_less = match (current, &candidate) {
                                        (PropertyValue::Int(x), PropertyValue::Int(y)) => y < x,
                                        (PropertyValue::Double(x), PropertyValue::Double(y)) => {
                                            y < x
                                        }
                                        (PropertyValue::String(x), PropertyValue::String(y)) => {
                                            y < x
                                        }
                                        _ => false,
                                    };
                                    if is_less {
                                        candidate
                                    } else {
                                        current.clone()
                                    }
                                }
                                None => candidate,
                            });
                        }
                        min_val.unwrap_or(PropertyValue::Null)
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("max") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::List(l) => {
                        let mut max_val: Option<PropertyValue> = None;
                        for item in l {
                            let candidate = match &item {
                                PropertyValue::Int(n) => PropertyValue::Int(*n),
                                PropertyValue::Double(d) => PropertyValue::Double(*d),
                                PropertyValue::String(s) => PropertyValue::String(s.clone()),
                                _ => continue,
                            };
                            max_val = Some(match max_val {
                                Some(ref current) => {
                                    let is_greater = match (current, &candidate) {
                                        (PropertyValue::Int(x), PropertyValue::Int(y)) => y > x,
                                        (PropertyValue::Double(x), PropertyValue::Double(y)) => {
                                            y > x
                                        }
                                        (PropertyValue::String(x), PropertyValue::String(y)) => {
                                            y > x
                                        }
                                        _ => false,
                                    };
                                    if is_greater {
                                        candidate
                                    } else {
                                        current.clone()
                                    }
                                }
                                None => candidate,
                            });
                        }
                        max_val.unwrap_or(PropertyValue::Null)
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("sum") => {
            // Alias for sum_list
            args.first()
                .map(
                    |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                        PropertyValue::List(l) => {
                            let mut total = 0i64;
                            for item in l {
                                match item {
                                    PropertyValue::Int(n) => total += n,
                                    PropertyValue::Double(d) => {
                                        return PropertyValue::Double(total as f64 + d)
                                    }
                                    _ => {}
                                }
                            }
                            PropertyValue::Int(total)
                        }
                        _ => PropertyValue::Null,
                    },
                )
                .unwrap_or(PropertyValue::Null)
        }
        n if n.eq_ignore_ascii_case("avg") => {
            // Alias for avg_list
            args.first()
                .map(
                    |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                        PropertyValue::List(l) => {
                            let mut total = 0.0f64;
                            let mut count = 0usize;
                            for item in l {
                                match item {
                                    PropertyValue::Int(n) => {
                                        total += n as f64;
                                        count += 1;
                                    }
                                    PropertyValue::Double(d) => {
                                        total += d;
                                        count += 1;
                                    }
                                    _ => {}
                                }
                            }
                            if count > 0 {
                                PropertyValue::Double(total / count as f64)
                            } else {
                                PropertyValue::Null
                            }
                        }
                        _ => PropertyValue::Null,
                    },
                )
                .unwrap_or(PropertyValue::Null)
        }
        // ─── Collection functions ────────────────────────────────
        n if n.eq_ignore_ascii_case("reduce") => {
            if args.len() == 4 {
                // reduce(initial, item_var, list, expr)
                // item_var must be an Identifier — its name is bound to each element
                let acc_initial =
                    eval_expression_with_catalog(&args[0], bindings, storage, catalog);
                let item_name = match &args[1] {
                    Expression::Identifier(name) => name.clone(),
                    _ => return PropertyValue::Null,
                };
                let list = match eval_expression_with_catalog(&args[2], bindings, storage, catalog)
                {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let mut acc = acc_initial;
                for item in list {
                    let mut local = bindings.clone();
                    local.insert(item_name.clone(), item);
                    local.insert("_acc".to_string(), acc);
                    acc = eval_expression_with_catalog(&args[3], &local, storage, catalog);
                }
                acc
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("extract") => {
            if args.len() >= 2 {
                let item_name = match &args[0] {
                    Expression::Identifier(name) => name.clone(),
                    _ => return PropertyValue::Null,
                };
                let list = match eval_expression_with_catalog(&args[1], bindings, storage, catalog)
                {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let result: Vec<PropertyValue> = if args.len() >= 3 {
                    list.into_iter()
                        .map(|item| {
                            let mut local = bindings.clone();
                            local.insert(item_name.clone(), item);
                            eval_expression_with_catalog(&args[2], &local, storage, catalog)
                        })
                        .collect()
                } else {
                    list
                };
                PropertyValue::List(result)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("filter") => {
            if args.len() >= 3 {
                let item_name = match &args[0] {
                    Expression::Identifier(name) => name.clone(),
                    _ => return PropertyValue::Null,
                };
                let list = match eval_expression_with_catalog(&args[1], bindings, storage, catalog)
                {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let result: Vec<PropertyValue> = list
                    .into_iter()
                    .filter(|item| {
                        let mut local = bindings.clone();
                        local.insert(item_name.clone(), item.clone());
                        match eval_expression_with_catalog(&args[2], &local, storage, catalog) {
                            PropertyValue::Bool(b) => b,
                            _ => false,
                        }
                    })
                    .collect();
                PropertyValue::List(result)
            } else {
                PropertyValue::Null
            }
        }
        // ─── Graph functions ─────────────────────────────────────
        n if n.eq_ignore_ascii_case("degree") => {
            if let Some(arg) = args.first() {
                let v = eval_expression_with_catalog(arg, bindings, storage, catalog);
                match v {
                    PropertyValue::Vertex(vr) => {
                        if let Some(s) = storage {
                            PropertyValue::Int(s.vertex_incident_edge_count(vr.gid) as i64)
                        } else {
                            PropertyValue::Null
                        }
                    }
                    _ => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("indegree") => {
            if let Some(arg) = args.first() {
                let v = eval_expression_with_catalog(arg, bindings, storage, catalog);
                match v {
                    PropertyValue::Vertex(vr) => {
                        if let Some(s) = storage {
                            PropertyValue::Int(s.vertex_in_degree(vr.gid) as i64)
                        } else {
                            PropertyValue::Null
                        }
                    }
                    _ => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("outdegree") => {
            if let Some(arg) = args.first() {
                let v = eval_expression_with_catalog(arg, bindings, storage, catalog);
                match v {
                    PropertyValue::Vertex(vr) => {
                        if let Some(s) = storage {
                            PropertyValue::Int(s.vertex_out_degree(vr.gid) as i64)
                        } else {
                            PropertyValue::Null
                        }
                    }
                    _ => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("nodes") && args.is_empty() => {
            if let Some(s) = storage {
                let vertices = s.all_vertices();
                PropertyValue::List(
                    vertices
                        .into_iter()
                        .map(|(gid, labels, _)| {
                            PropertyValue::Vertex(VertexRef::new(
                                gid,
                                labels,
                                mgcore::property_store::PropertyStore::default(),
                            ))
                        })
                        .collect(),
                )
            } else {
                PropertyValue::List(vec![])
            }
        }
        n if n.eq_ignore_ascii_case("relationships") && args.is_empty() => {
            if let Some(s) = storage {
                let edges = s.all_edges();
                PropertyValue::List(
                    edges
                        .into_iter()
                        .map(|(gid, from, to, etype, _)| {
                            PropertyValue::Edge(EdgeRefValue::new(
                                gid,
                                etype,
                                from,
                                to,
                                mgcore::property_store::PropertyStore::default(),
                            ))
                        })
                        .collect(),
                )
            } else {
                PropertyValue::List(vec![])
            }
        }
        // APOC underscore aliases (e.g. apoc_coll_union from apoc.coll.union)
        n if n.eq_ignore_ascii_case("apoc_coll_union") => {
            let a = args
                .first()
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog));
            let b = args
                .get(1)
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog));
            match (a, b) {
                (Some(PropertyValue::List(a)), Some(PropertyValue::List(b))) => {
                    let mut result = a.clone();
                    for item in b {
                        if !result.contains(&item) {
                            result.push(item);
                        }
                    }
                    PropertyValue::List(result)
                }
                _ => PropertyValue::Null,
            }
        }
        n if n.eq_ignore_ascii_case("apoc_coll_intersection") => {
            let a = args
                .first()
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog));
            let b = args
                .get(1)
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog));
            match (a, b) {
                (Some(PropertyValue::List(a)), Some(PropertyValue::List(b))) => {
                    PropertyValue::List(a.iter().filter(|x| b.contains(x)).cloned().collect())
                }
                _ => PropertyValue::Null,
            }
        }
        n if n.eq_ignore_ascii_case("apoc_map_merge") => {
            let a = args
                .first()
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog));
            let b = args
                .get(1)
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog));
            match (a, b) {
                (Some(PropertyValue::Map(mut a)), Some(PropertyValue::Map(b))) => {
                    a.extend(b);
                    PropertyValue::Map(a)
                }
                _ => PropertyValue::Null,
            }
        }
        n if n.eq_ignore_ascii_case("apoc_map_setkey") => {
            let a = args
                .first()
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog));
            let k = args
                .get(1)
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog));
            let v = args
                .get(2)
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog));
            match (a, k, v) {
                (Some(PropertyValue::Map(mut m)), Some(PropertyValue::String(key)), Some(val)) => {
                    m.push((key, val));
                    PropertyValue::Map(m)
                }
                _ => PropertyValue::Null,
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_join") => {
            let a = args
                .first()
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog));
            let sep = args
                .get(1)
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog));
            match (a, sep) {
                (Some(PropertyValue::List(items)), Some(PropertyValue::String(s))) => {
                    let strings: Vec<String> = items
                        .iter()
                        .filter_map(|v| match v {
                            PropertyValue::String(st) => Some(st.clone()),
                            _ => None,
                        })
                        .collect();
                    PropertyValue::String(strings.join(&s))
                }
                _ => PropertyValue::Null,
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_replace") => {
            let a = args
                .first()
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog));
            let old = args
                .get(1)
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog));
            let new = args
                .get(2)
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog));
            match (a, old, new) {
                (
                    Some(PropertyValue::String(s)),
                    Some(PropertyValue::String(o)),
                    Some(PropertyValue::String(n)),
                ) => PropertyValue::String(s.replace(&o, &n)),
                _ => PropertyValue::Null,
            }
        }
        n if n.eq_ignore_ascii_case("apoc_coll_sort") => {
            let a = args
                .first()
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog));
            match a {
                Some(PropertyValue::List(mut items)) => {
                    items.sort_by(crate::compare);
                    PropertyValue::List(items)
                }
                _ => PropertyValue::Null,
            }
        }
        n if n.eq_ignore_ascii_case("apoc_coll_sortmaps")
            || n.eq_ignore_ascii_case("sort_maps") =>
        {
            if args.len() == 2 {
                let list = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let key = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let mut maps: Vec<(PropertyValue, Vec<(String, PropertyValue)>)> = list
                    .into_iter()
                    .filter_map(|v| match v {
                        PropertyValue::Map(entries) => {
                            let val = entries
                                .iter()
                                .find(|(k, _)| k == &key)
                                .map(|(_, v)| v.clone());
                            Some((val.unwrap_or(PropertyValue::Null), entries))
                        }
                        _ => None,
                    })
                    .collect();
                maps.sort_by(|a, b| crate::compare(&a.0, &b.0));
                PropertyValue::List(
                    maps.into_iter()
                        .map(|(_, e)| PropertyValue::Map(e))
                        .collect(),
                )
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_coll_flatten") => {
            let a = args
                .first()
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog));
            match a {
                Some(PropertyValue::List(items)) => {
                    let mut result = Vec::new();
                    for item in items {
                        if let PropertyValue::List(sub) = item {
                            result.extend(sub);
                        } else {
                            result.push(item);
                        }
                    }
                    PropertyValue::List(result)
                }
                _ => PropertyValue::Null,
            }
        }
        n if n.eq_ignore_ascii_case("apoc_coll_remove") => {
            let a = args
                .first()
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog));
            let idx = args
                .get(1)
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog));
            match (a, idx) {
                (Some(PropertyValue::List(mut items)), Some(PropertyValue::Int(i))) => {
                    let pos = if i >= 0 {
                        i as usize
                    } else {
                        (items.len() as i64 + i) as usize
                    };
                    if pos < items.len() {
                        items.remove(pos);
                    }
                    PropertyValue::List(items)
                }
                _ => PropertyValue::Null,
            }
        }
        n if n.eq_ignore_ascii_case("apoc_coll_sum") => {
            let a = args
                .first()
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog));
            match a {
                Some(PropertyValue::List(items)) => {
                    let sum: i64 = items
                        .iter()
                        .filter_map(|v| match v {
                            PropertyValue::Int(n) => Some(*n),
                            _ => None,
                        })
                        .sum();
                    PropertyValue::Int(sum)
                }
                _ => PropertyValue::Null,
            }
        }
        n if n.eq_ignore_ascii_case("apoc_coll_avg") => {
            let a = args
                .first()
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog));
            match a {
                Some(PropertyValue::List(items)) => {
                    let ints: Vec<i64> = items
                        .iter()
                        .filter_map(|v| match v {
                            PropertyValue::Int(n) => Some(*n),
                            _ => None,
                        })
                        .collect();
                    if ints.is_empty() {
                        PropertyValue::Null
                    } else {
                        PropertyValue::Double(ints.iter().sum::<i64>() as f64 / ints.len() as f64)
                    }
                }
                _ => PropertyValue::Null,
            }
        }
        // ─── More APOC list functions ──────────────────────────────
        n if n.eq_ignore_ascii_case("apoc_coll_different")
            || n.eq_ignore_ascii_case("different") =>
        {
            args.first()
                .map(
                    |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                        PropertyValue::List(l) => {
                            let mut seen = std::collections::HashSet::new();
                            let mut result = Vec::new();
                            for item in l {
                                let key = format!("{:?}", item);
                                if seen.insert(key) {
                                    result.push(item);
                                }
                            }
                            PropertyValue::List(result)
                        }
                        _ => PropertyValue::Null,
                    },
                )
                .unwrap_or(PropertyValue::Null)
        }
        n if n.eq_ignore_ascii_case("apoc_coll_frequencies")
            || n.eq_ignore_ascii_case("frequencies") =>
        {
            args.first()
                .map(
                    |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                        PropertyValue::List(l) => {
                            let mut counts: HashMap<String, (PropertyValue, i64)> = HashMap::new();
                            for item in l {
                                let key = format!("{:?}", item);
                                counts.entry(key).or_insert((item, 0)).1 += 1;
                            }
                            let result: Vec<PropertyValue> = counts
                                .into_values()
                                .map(|(item, count)| {
                                    PropertyValue::Map(vec![
                                        ("item".to_string(), item),
                                        ("count".to_string(), PropertyValue::Int(count)),
                                    ])
                                })
                                .collect();
                            PropertyValue::List(result)
                        }
                        _ => PropertyValue::Null,
                    },
                )
                .unwrap_or(PropertyValue::Null)
        }
        n if n.eq_ignore_ascii_case("apoc_coll_frequencies_as_map")
            || n.eq_ignore_ascii_case("frequencies_as_map")
            || n.eq_ignore_ascii_case("frequenciesAsMap") =>
        {
            args.first()
                .map(
                    |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                        PropertyValue::List(l) => {
                            let mut counts: HashMap<String, i64> = HashMap::new();
                            for item in l {
                                let key = format!("{:?}", item);
                                *counts.entry(key).or_insert(0) += 1;
                            }
                            let result: Vec<(String, PropertyValue)> = counts
                                .into_iter()
                                .map(|(k, v)| (k, PropertyValue::Int(v)))
                                .collect();
                            PropertyValue::Map(result)
                        }
                        _ => PropertyValue::Null,
                    },
                )
                .unwrap_or(PropertyValue::Null)
        }
        n if n.eq_ignore_ascii_case("apoc_coll_zip") || n.eq_ignore_ascii_case("zip") => {
            if args.len() == 2 {
                let a = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let b = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let mut result = Vec::new();
                for (i, item_a) in a.into_iter().enumerate() {
                    if let Some(item_b) = b.get(i) {
                        result.push(PropertyValue::List(vec![item_a, item_b.clone()]));
                    }
                }
                PropertyValue::List(result)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_coll_pairs") || n.eq_ignore_ascii_case("pairs") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::List(l) => {
                        let mut result = Vec::new();
                        for i in 0..l.len() {
                            for j in (i + 1)..l.len() {
                                result.push(PropertyValue::List(vec![l[i].clone(), l[j].clone()]));
                            }
                        }
                        PropertyValue::List(result)
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("apoc_coll_max") || n.eq_ignore_ascii_case("list_max") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::List(l) => {
                        let mut max_val: Option<&PropertyValue> = None;
                        for item in &l {
                            match item {
                                PropertyValue::Int(_)
                                | PropertyValue::Double(_)
                                | PropertyValue::String(_) => {
                                    if let Some(current) = max_val {
                                        if crate::compare(item, current)
                                            == std::cmp::Ordering::Greater
                                        {
                                            max_val = Some(item);
                                        }
                                    } else {
                                        max_val = Some(item);
                                    }
                                }
                                _ => {}
                            }
                        }
                        max_val.cloned().unwrap_or(PropertyValue::Null)
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("apoc_coll_min") || n.eq_ignore_ascii_case("list_min") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::List(l) => {
                        let mut min_val: Option<&PropertyValue> = None;
                        for item in &l {
                            match item {
                                PropertyValue::Int(_)
                                | PropertyValue::Double(_)
                                | PropertyValue::String(_) => {
                                    if let Some(current) = min_val {
                                        if crate::compare(item, current) == std::cmp::Ordering::Less
                                        {
                                            min_val = Some(item);
                                        }
                                    } else {
                                        min_val = Some(item);
                                    }
                                }
                                _ => {}
                            }
                        }
                        min_val.cloned().unwrap_or(PropertyValue::Null)
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("apoc_convert_toset")
            || n.eq_ignore_ascii_case("toset")
            || n.eq_ignore_ascii_case("to_set") =>
        {
            args.first()
                .map(
                    |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                        PropertyValue::List(l) => {
                            let mut seen = std::collections::HashSet::new();
                            let mut result = Vec::new();
                            for item in l {
                                let key = format!("{:?}", item);
                                if seen.insert(key) {
                                    result.push(item);
                                }
                            }
                            PropertyValue::List(result)
                        }
                        _ => PropertyValue::Null,
                    },
                )
                .unwrap_or(PropertyValue::Null)
        }
        n if n.eq_ignore_ascii_case("apoc_coll_combinations")
            || n.eq_ignore_ascii_case("combinations") =>
        {
            if args.len() == 2 {
                let list = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let k = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::Int(n) => n as usize,
                    _ => return PropertyValue::Null,
                };
                if k == 0 {
                    return PropertyValue::List(vec![PropertyValue::List(vec![])]);
                }
                fn combos(items: &[PropertyValue], k: usize) -> Vec<Vec<PropertyValue>> {
                    if k == 0 {
                        return vec![vec![]];
                    }
                    if items.is_empty() {
                        return vec![];
                    }
                    let mut result = Vec::new();
                    for (i, item) in items.iter().enumerate() {
                        for mut sub in combos(&items[i + 1..], k - 1) {
                            let mut combo = vec![item.clone()];
                            combo.append(&mut sub);
                            result.push(combo);
                        }
                    }
                    result
                }
                let result = combos(&list, k)
                    .into_iter()
                    .map(PropertyValue::List)
                    .collect();
                PropertyValue::List(result)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_coll_subtract") || n.eq_ignore_ascii_case("subtract") => {
            if args.len() == 2 {
                let a = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let b = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let b_keys: std::collections::HashSet<String> =
                    b.iter().map(|v| format!("{:?}", v)).collect();
                let result: Vec<PropertyValue> = a
                    .into_iter()
                    .filter(|v| !b_keys.contains(&format!("{:?}", v)))
                    .collect();
                PropertyValue::List(result)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_coll_unionall")
            || n.eq_ignore_ascii_case("unionall")
            || n.eq_ignore_ascii_case("union_all") =>
        {
            if args.len() == 2 {
                let a = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let b = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let mut result = a;
                result.extend(b);
                PropertyValue::List(result)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_coll_insertall")
            || n.eq_ignore_ascii_case("insert_all") =>
        {
            if args.len() == 3 {
                let mut base =
                    match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                        PropertyValue::List(l) => l,
                        _ => return PropertyValue::Null,
                    };
                let idx = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::Int(n) => n as usize,
                    _ => return PropertyValue::Null,
                };
                let insert =
                    match eval_expression_with_catalog(&args[2], bindings, storage, catalog) {
                        PropertyValue::List(l) => l,
                        _ => return PropertyValue::Null,
                    };
                let pos = idx.min(base.len());
                base.splice(pos..pos, insert);
                PropertyValue::List(base)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_coll_nth") || n.eq_ignore_ascii_case("nth") => {
            if args.len() == 2 {
                let list = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let idx = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::Int(n) => n,
                    _ => return PropertyValue::Null,
                };
                let pos = if idx >= 0 {
                    idx as usize
                } else {
                    (list.len() as i64 + idx) as usize
                };
                list.into_iter().nth(pos).unwrap_or(PropertyValue::Null)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_coll_partition")
            || n.eq_ignore_ascii_case("partition") =>
        {
            if args.len() == 2 {
                let list = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let size = match eval_expression_with_catalog(&args[1], bindings, storage, catalog)
                {
                    PropertyValue::Int(n) if n > 0 => n as usize,
                    _ => return PropertyValue::Null,
                };
                let chunks: Vec<PropertyValue> = list
                    .chunks(size)
                    .map(|chunk| PropertyValue::List(chunk.to_vec()))
                    .collect();
                PropertyValue::List(chunks)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_number_format")
            || n.eq_ignore_ascii_case("format_number") =>
        {
            let val = args
                .first()
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog));
            let pattern = args
                .get(1)
                .map(|e| eval_expression_with_catalog(e, bindings, storage, catalog));
            match (val, pattern) {
                (Some(PropertyValue::Int(n)), Some(PropertyValue::String(p))) => {
                    if p == "#,##0" {
                        PropertyValue::String(format!("{}", n))
                    } else if p == "#,##0.00" {
                        PropertyValue::String(format!("{:.2}", n as f64))
                    } else {
                        PropertyValue::String(format!("{}", n))
                    }
                }
                (Some(PropertyValue::Double(d)), Some(PropertyValue::String(p))) => {
                    if p == "#,##0.00" || p == "0.00" {
                        PropertyValue::String(format!("{:.2}", d))
                    } else if p == "#,##0" || p == "0" {
                        PropertyValue::String(format!("{:.0}", d))
                    } else {
                        PropertyValue::String(format!("{}", d))
                    }
                }
                (Some(PropertyValue::Int(n)), None) => PropertyValue::String(format!("{}", n)),
                (Some(PropertyValue::Double(d)), None) => PropertyValue::String(format!("{}", d)),
                _ => PropertyValue::Null,
            }
        }
        n if n.eq_ignore_ascii_case("apoc_number_parseint")
            || n.eq_ignore_ascii_case("parse_int") =>
        {
            if !args.is_empty() {
                let s = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    PropertyValue::Int(n) => return PropertyValue::Int(n),
                    _ => return PropertyValue::Null,
                };
                let radix = if args.len() >= 2 {
                    match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                        PropertyValue::Int(n) => n as u32,
                        _ => 10,
                    }
                } else {
                    10
                };
                match i64::from_str_radix(&s, radix) {
                    Ok(n) => PropertyValue::Int(n),
                    Err(_) => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_number_parsefloat")
            || n.eq_ignore_ascii_case("parse_float") =>
        {
            if args.len() == 1 {
                let s = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    PropertyValue::Int(n) => return PropertyValue::Double(n as f64),
                    PropertyValue::Double(d) => return PropertyValue::Double(d),
                    _ => return PropertyValue::Null,
                };
                match s.parse::<f64>() {
                    Ok(d) => PropertyValue::Double(d),
                    Err(_) => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_number_round") || n.eq_ignore_ascii_case("round_to") => {
            if !args.is_empty() {
                let val = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::Int(n) => n as f64,
                    PropertyValue::Double(d) => d,
                    _ => return PropertyValue::Null,
                };
                let places = if args.len() >= 2 {
                    match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                        PropertyValue::Int(n) => n as i32,
                        _ => 0,
                    }
                } else {
                    0
                };
                let multiplier = 10f64.powi(places);
                PropertyValue::Double((val * multiplier).round() / multiplier)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_number_abs") || n.eq_ignore_ascii_case("abs_value") => {
            if args.len() == 1 {
                match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::Int(n) => PropertyValue::Int(n.abs()),
                    PropertyValue::Double(d) => PropertyValue::Double(d.abs()),
                    _ => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_number_sign") || n.eq_ignore_ascii_case("sign_value") => {
            if args.len() == 1 {
                match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::Int(n) => PropertyValue::Int(if n > 0 {
                        1
                    } else if n < 0 {
                        -1
                    } else {
                        0
                    }),
                    PropertyValue::Double(d) => PropertyValue::Int(if d > 0.0 {
                        1
                    } else if d < 0.0 {
                        -1
                    } else {
                        0
                    }),
                    _ => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_text_levenshtein")
            || n.eq_ignore_ascii_case("levenshtein")
            || n.eq_ignore_ascii_case("levenshteindistance") =>
        {
            if args.len() == 2 {
                let a = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let b = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let m = a.chars().count();
                let n = b.chars().count();
                if m == 0 {
                    return PropertyValue::Int(n as i64);
                }
                if n == 0 {
                    return PropertyValue::Int(m as i64);
                }
                let mut prev: Vec<usize> = (0..=n).collect();
                let mut curr = vec![0usize; n + 1];
                for (i, ca) in a.chars().enumerate() {
                    curr[0] = i + 1;
                    for (j, cb) in b.chars().enumerate() {
                        let cost = if ca == cb { 0 } else { 1 };
                        curr[j + 1] = (curr[j] + 1).min(prev[j + 1] + 1).min(prev[j] + cost);
                    }
                    std::mem::swap(&mut prev, &mut curr);
                }
                PropertyValue::Int(prev[n] as i64)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_coll_randomitem")
            || n.eq_ignore_ascii_case("random_item") =>
        {
            args.first()
                .map(
                    |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                        PropertyValue::List(l) => {
                            if l.is_empty() {
                                PropertyValue::Null
                            } else {
                                let idx = rand::random::<usize>() % l.len();
                                l.into_iter().nth(idx).unwrap_or(PropertyValue::Null)
                            }
                        }
                        _ => PropertyValue::Null,
                    },
                )
                .unwrap_or(PropertyValue::Null)
        }
        // ─── APOC map functions ────────────────────────────────────
        n if n.eq_ignore_ascii_case("apoc_map_fromlists")
            || n.eq_ignore_ascii_case("from_lists") =>
        {
            if args.len() == 2 {
                let keys = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let vals = match eval_expression_with_catalog(&args[1], bindings, storage, catalog)
                {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let mut map = Vec::new();
                for (i, k) in keys.iter().enumerate() {
                    if let PropertyValue::String(key) = k {
                        if let Some(v) = vals.get(i) {
                            map.push((key.clone(), v.clone()));
                        }
                    }
                }
                PropertyValue::Map(map)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_map_frompairs")
            || n.eq_ignore_ascii_case("from_pairs") =>
        {
            args.first()
                .map(
                    |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                        PropertyValue::List(pairs) => {
                            let mut map = Vec::new();
                            for pair in pairs {
                                if let PropertyValue::List(p) = pair {
                                    if p.len() >= 2 {
                                        if let PropertyValue::String(key) = &p[0] {
                                            map.push((key.clone(), p[1].clone()));
                                        }
                                    }
                                }
                            }
                            PropertyValue::Map(map)
                        }
                        _ => PropertyValue::Null,
                    },
                )
                .unwrap_or(PropertyValue::Null)
        }
        n if n.eq_ignore_ascii_case("apoc_map_get")
            || n.eq_ignore_ascii_case("map_get")
            || n.eq_ignore_ascii_case("get") =>
        {
            if args.len() >= 2 {
                let map = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::Map(m) => m,
                    _ => return PropertyValue::Null,
                };
                let key = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let default = args
                    .get(2)
                    .map(|a| eval_expression_with_catalog(a, bindings, storage, catalog));
                map.iter()
                    .find(|(k, _)| k == &key)
                    .map(|(_, v)| v.clone())
                    .or(default)
                    .unwrap_or(PropertyValue::Null)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_map_mget") || n.eq_ignore_ascii_case("map_mget") => {
            if args.len() == 2 {
                let map = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::Map(m) => m,
                    _ => return PropertyValue::Null,
                };
                let keys = match eval_expression_with_catalog(&args[1], bindings, storage, catalog)
                {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let mut result = Vec::new();
                for key in keys {
                    if let PropertyValue::String(k) = key {
                        if let Some((_, v)) = map.iter().find(|(mk, _)| mk == &k) {
                            result.push(v.clone());
                        } else {
                            result.push(PropertyValue::Null);
                        }
                    }
                }
                PropertyValue::List(result)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_map_clean") || n.eq_ignore_ascii_case("map_clean") => {
            args.first()
                .map(
                    |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                        PropertyValue::Map(m) => {
                            let cleaned: Vec<(String, PropertyValue)> = m
                                .into_iter()
                                .filter(|(_, v)| !matches!(v, PropertyValue::Null))
                                .collect();
                            PropertyValue::Map(cleaned)
                        }
                        _ => PropertyValue::Null,
                    },
                )
                .unwrap_or(PropertyValue::Null)
        }
        n if n.eq_ignore_ascii_case("apoc_map_invert") || n.eq_ignore_ascii_case("map_invert") => {
            args.first()
                .map(
                    |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                        PropertyValue::Map(m) => {
                            let inverted: Vec<(String, PropertyValue)> = m
                                .into_iter()
                                .map(|(k, v)| (format!("{}", v), PropertyValue::String(k)))
                                .collect();
                            PropertyValue::Map(inverted)
                        }
                        _ => PropertyValue::Null,
                    },
                )
                .unwrap_or(PropertyValue::Null)
        }
        n if n.eq_ignore_ascii_case("apoc_map_values") || n.eq_ignore_ascii_case("map_values") => {
            if args.len() >= 2 {
                let map = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::Map(m) => m,
                    _ => return PropertyValue::Null,
                };
                let keys = match eval_expression_with_catalog(&args[1], bindings, storage, catalog)
                {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let mut result = Vec::new();
                for key in keys {
                    if let PropertyValue::String(k) = key {
                        let val = map
                            .iter()
                            .find(|(mk, _)| mk == &k)
                            .map(|(_, v)| v.clone())
                            .unwrap_or(PropertyValue::Null);
                        result.push(val);
                    }
                }
                PropertyValue::List(result)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_map_flatten")
            || n.eq_ignore_ascii_case("map_flatten") =>
        {
            if !args.is_empty() {
                let map = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::Map(m) => m,
                    _ => return PropertyValue::Null,
                };
                let delimiter = if args.len() >= 2 {
                    match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                        PropertyValue::String(s) => s,
                        _ => ".".to_string(),
                    }
                } else {
                    ".".to_string()
                };
                let mut result = Vec::new();
                fn flatten_map(
                    prefix: &str,
                    map: &[(String, PropertyValue)],
                    delimiter: &str,
                    result: &mut Vec<(String, PropertyValue)>,
                ) {
                    for (k, v) in map {
                        let key = if prefix.is_empty() {
                            k.clone()
                        } else {
                            format!("{}{}{}", prefix, delimiter, k)
                        };
                        match v {
                            PropertyValue::Map(inner) => {
                                flatten_map(&key, inner, delimiter, result)
                            }
                            _ => result.push((key, v.clone())),
                        }
                    }
                }
                flatten_map("", &map, &delimiter, &mut result);
                PropertyValue::Map(result)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_map_sorted") || n.eq_ignore_ascii_case("map_sorted") => {
            if !args.is_empty() {
                let map = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::Map(m) => m,
                    _ => return PropertyValue::Null,
                };
                let ascending = if args.len() >= 2 {
                    match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                        PropertyValue::Bool(b) => b,
                        _ => true,
                    }
                } else {
                    true
                };
                let mut result = map;
                if ascending {
                    result.sort_by(|a, b| a.0.cmp(&b.0));
                } else {
                    result.sort_by(|a, b| b.0.cmp(&a.0));
                }
                PropertyValue::Map(result)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_map_groupby")
            || n.eq_ignore_ascii_case("map_groupby") =>
        {
            if args.len() >= 2 {
                let list = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let key_expr = &args[1];
                let mut groups: HashMap<String, Vec<PropertyValue>> = HashMap::new();
                for item in list {
                    let mut local_bindings = bindings.clone();
                    local_bindings.insert("_item".to_string(), item.clone());
                    let key = match eval_expression_with_catalog(
                        key_expr,
                        &local_bindings,
                        storage,
                        catalog,
                    ) {
                        PropertyValue::String(s) => s,
                        PropertyValue::Int(n) => n.to_string(),
                        other => format!("{}", other),
                    };
                    groups.entry(key).or_default().push(item);
                }
                let result: Vec<(String, PropertyValue)> = groups
                    .into_iter()
                    .map(|(k, v)| (k, PropertyValue::List(v)))
                    .collect();
                PropertyValue::Map(result)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_convert_toboolean")
            || n.eq_ignore_ascii_case("to_boolean") =>
        {
            if args.len() == 1 {
                match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::Bool(b) => PropertyValue::Bool(b),
                    PropertyValue::String(s) => {
                        PropertyValue::Bool(s.eq_ignore_ascii_case("true") || s == "1")
                    }
                    PropertyValue::Int(n) => PropertyValue::Bool(n != 0),
                    PropertyValue::Double(d) => PropertyValue::Bool(d != 0.0),
                    _ => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_convert_tostring")
            || n.eq_ignore_ascii_case("to_string") =>
        {
            if args.len() == 1 {
                let val = eval_expression_with_catalog(&args[0], bindings, storage, catalog);
                PropertyValue::String(format!("{}", val))
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_convert_tointeger")
            || n.eq_ignore_ascii_case("to_integer") =>
        {
            if args.len() == 1 {
                match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::Int(n) => PropertyValue::Int(n),
                    PropertyValue::Double(d) => PropertyValue::Int(d as i64),
                    PropertyValue::String(s) => s
                        .parse::<i64>()
                        .map(PropertyValue::Int)
                        .unwrap_or(PropertyValue::Null),
                    _ => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_convert_tofloat")
            || n.eq_ignore_ascii_case("to_float") =>
        {
            if args.len() == 1 {
                match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::Int(n) => PropertyValue::Double(n as f64),
                    PropertyValue::Double(d) => PropertyValue::Double(d),
                    PropertyValue::String(s) => s
                        .parse::<f64>()
                        .map(PropertyValue::Double)
                        .unwrap_or(PropertyValue::Null),
                    _ => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_convert_tolist") || n.eq_ignore_ascii_case("to_list") => {
            if args.len() == 1 {
                match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::List(l) => PropertyValue::List(l),
                    PropertyValue::Map(m) => PropertyValue::List(
                        m.into_iter()
                            .map(|(k, v)| PropertyValue::List(vec![PropertyValue::String(k), v]))
                            .collect(),
                    ),
                    PropertyValue::String(s) => PropertyValue::List(
                        s.chars()
                            .map(|c| PropertyValue::String(c.to_string()))
                            .collect(),
                    ),
                    other => PropertyValue::List(vec![other]),
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_meta_types") || n.eq_ignore_ascii_case("meta_types") => {
            if !args.is_empty() {
                let val = eval_expression_with_catalog(&args[0], bindings, storage, catalog);
                let type_name = match val {
                    PropertyValue::Null => "NULL",
                    PropertyValue::Bool(_) => "BOOLEAN",
                    PropertyValue::Int(_) => "INTEGER",
                    PropertyValue::Double(_) => "FLOAT",
                    PropertyValue::String(_) => "STRING",
                    PropertyValue::List(_) => "LIST",
                    PropertyValue::Map(_) => "MAP",
                    PropertyValue::Vertex(_) => "NODE",
                    PropertyValue::Edge(_) => "RELATIONSHIP",
                    PropertyValue::Path(_) => "PATH",
                    PropertyValue::Date(_) => "DATE",
                    PropertyValue::LocalTime(_) => "LOCALTIME",
                    PropertyValue::LocalDateTime(_) => "LOCALDATETIME",
                    PropertyValue::ZonedDateTime(_) => "ZONEDDATETIME",
                    PropertyValue::Duration(_) => "DURATION",
                    PropertyValue::Point2D(_) => "POINT",
                    PropertyValue::Point3D(_) => "POINT",
                    PropertyValue::Enum { .. } => "ENUM",
                };
                if args.len() >= 2 {
                    let expected =
                        match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                            PropertyValue::String(s) => s,
                            _ => return PropertyValue::Null,
                        };
                    PropertyValue::Bool(type_name.eq_ignore_ascii_case(&expected))
                } else {
                    PropertyValue::String(type_name.into())
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_meta_istype")
            || n.eq_ignore_ascii_case("meta_istype") =>
        {
            if args.len() == 2 {
                let val = eval_expression_with_catalog(&args[0], bindings, storage, catalog);
                let expected =
                    match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                        PropertyValue::String(s) => s.to_uppercase(),
                        _ => return PropertyValue::Null,
                    };
                let type_name = match val {
                    PropertyValue::Null => "NULL",
                    PropertyValue::Bool(_) => "BOOLEAN",
                    PropertyValue::Int(_) => "INTEGER",
                    PropertyValue::Double(_) => "FLOAT",
                    PropertyValue::String(_) => "STRING",
                    PropertyValue::List(_) => "LIST",
                    PropertyValue::Map(_) => "MAP",
                    PropertyValue::Vertex(_) => "NODE",
                    PropertyValue::Edge(_) => "RELATIONSHIP",
                    PropertyValue::Path(_) => "PATH",
                    PropertyValue::Date(_) => "DATE",
                    PropertyValue::LocalTime(_) => "LOCALTIME",
                    PropertyValue::LocalDateTime(_) => "LOCALDATETIME",
                    PropertyValue::ZonedDateTime(_) => "ZONEDDATETIME",
                    PropertyValue::Duration(_) => "DURATION",
                    PropertyValue::Point2D(_) => "POINT",
                    PropertyValue::Point3D(_) => "POINT",
                    PropertyValue::Enum { .. } => "ENUM",
                };
                PropertyValue::Bool(type_name == expected)
            } else {
                PropertyValue::Null
            }
        }
        // ─── Temporal component extractors ─────────────────────────
        n if n.eq_ignore_ascii_case("year") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Date(d) => PropertyValue::Int(d.days() / 365 + 1970),
                    PropertyValue::LocalDateTime(dt) => {
                        PropertyValue::Int(us_to_naive_datetime(dt.microseconds()).year() as i64)
                    }
                    PropertyValue::ZonedDateTime(zdt) => {
                        let dt = chrono::DateTime::from_timestamp_micros(zdt.microseconds())
                            .unwrap_or_default();
                        PropertyValue::Int(dt.year() as i64)
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("month") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Date(d) => {
                        let naive = chrono::NaiveDate::from_num_days_from_ce_opt(
                            (d.days() + 719162) as i32,
                        )
                        .unwrap_or_default();
                        PropertyValue::Int(naive.month() as i64)
                    }
                    PropertyValue::LocalDateTime(dt) => {
                        PropertyValue::Int(us_to_naive_datetime(dt.microseconds()).month() as i64)
                    }
                    PropertyValue::ZonedDateTime(zdt) => {
                        let dt = chrono::DateTime::from_timestamp_micros(zdt.microseconds())
                            .unwrap_or_default();
                        PropertyValue::Int(dt.month() as i64)
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("day") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Date(d) => {
                        let naive = chrono::NaiveDate::from_num_days_from_ce_opt(
                            (d.days() + 719162) as i32,
                        )
                        .unwrap_or_default();
                        PropertyValue::Int(naive.day() as i64)
                    }
                    PropertyValue::LocalDateTime(dt) => {
                        PropertyValue::Int(us_to_naive_datetime(dt.microseconds()).day() as i64)
                    }
                    PropertyValue::ZonedDateTime(zdt) => {
                        let dt = chrono::DateTime::from_timestamp_micros(zdt.microseconds())
                            .unwrap_or_default();
                        PropertyValue::Int(dt.day() as i64)
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("hour") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::LocalTime(t) => {
                        PropertyValue::Int(t.microseconds() / 3_600_000_000)
                    }
                    PropertyValue::LocalDateTime(dt) => {
                        PropertyValue::Int(us_to_naive_datetime(dt.microseconds()).hour() as i64)
                    }
                    PropertyValue::ZonedDateTime(zdt) => {
                        let dt = chrono::DateTime::from_timestamp_micros(zdt.microseconds())
                            .unwrap_or_default();
                        PropertyValue::Int(dt.hour() as i64)
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("minute") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::LocalTime(t) => {
                        PropertyValue::Int((t.microseconds() / 60_000_000) % 60)
                    }
                    PropertyValue::LocalDateTime(dt) => {
                        PropertyValue::Int(us_to_naive_datetime(dt.microseconds()).minute() as i64)
                    }
                    PropertyValue::ZonedDateTime(zdt) => {
                        let dt = chrono::DateTime::from_timestamp_micros(zdt.microseconds())
                            .unwrap_or_default();
                        PropertyValue::Int(dt.minute() as i64)
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("second") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::LocalTime(t) => {
                        PropertyValue::Int((t.microseconds() / 1_000_000) % 60)
                    }
                    PropertyValue::LocalDateTime(dt) => {
                        PropertyValue::Int(us_to_naive_datetime(dt.microseconds()).second() as i64)
                    }
                    PropertyValue::ZonedDateTime(zdt) => {
                        let dt = chrono::DateTime::from_timestamp_micros(zdt.microseconds())
                            .unwrap_or_default();
                        PropertyValue::Int(dt.second() as i64)
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("millisecond") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::LocalTime(t) => {
                        PropertyValue::Int((t.microseconds() / 1_000) % 1_000)
                    }
                    PropertyValue::LocalDateTime(dt) => PropertyValue::Int(
                        (us_to_naive_datetime(dt.microseconds()).nanosecond() as i64 / 1_000_000)
                            % 1_000,
                    ),
                    PropertyValue::ZonedDateTime(zdt) => {
                        let dt = chrono::DateTime::from_timestamp_micros(zdt.microseconds())
                            .unwrap_or_default();
                        PropertyValue::Int((dt.timestamp_micros() % 1_000_000) / 1_000)
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("microsecond") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::LocalTime(t) => PropertyValue::Int(t.microseconds() % 1_000_000),
                    PropertyValue::LocalDateTime(dt) => PropertyValue::Int(
                        (us_to_naive_datetime(dt.microseconds()).nanosecond() as i64 / 1_000)
                            % 1_000_000,
                    ),
                    PropertyValue::ZonedDateTime(zdt) => {
                        let dt = chrono::DateTime::from_timestamp_micros(zdt.microseconds())
                            .unwrap_or_default();
                        PropertyValue::Int(dt.timestamp_micros() % 1_000_000)
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("dayofweek") || n.eq_ignore_ascii_case("dayofweek_iso") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Date(d) => {
                        let naive = chrono::NaiveDate::from_num_days_from_ce_opt(
                            (d.days() + 719162) as i32,
                        )
                        .unwrap_or_default();
                        PropertyValue::Int(naive.weekday().number_from_monday() as i64)
                    }
                    PropertyValue::LocalDateTime(dt) => PropertyValue::Int(
                        us_to_naive_datetime(dt.microseconds())
                            .weekday()
                            .number_from_monday() as i64,
                    ),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("dayofyear") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Date(d) => {
                        let naive = chrono::NaiveDate::from_num_days_from_ce_opt(
                            (d.days() + 719162) as i32,
                        )
                        .unwrap_or_default();
                        PropertyValue::Int(naive.ordinal() as i64)
                    }
                    PropertyValue::LocalDateTime(dt) => {
                        PropertyValue::Int(us_to_naive_datetime(dt.microseconds()).ordinal() as i64)
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("week") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Date(d) => {
                        let naive = chrono::NaiveDate::from_num_days_from_ce_opt(
                            (d.days() + 719162) as i32,
                        )
                        .unwrap_or_default();
                        PropertyValue::Int(naive.iso_week().week() as i64)
                    }
                    PropertyValue::LocalDateTime(dt) => PropertyValue::Int(
                        us_to_naive_datetime(dt.microseconds()).iso_week().week() as i64,
                    ),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("quarter") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Date(d) => {
                        let naive = chrono::NaiveDate::from_num_days_from_ce_opt(
                            (d.days() + 719162) as i32,
                        )
                        .unwrap_or_default();
                        PropertyValue::Int(((naive.month() - 1) / 3 + 1) as i64)
                    }
                    PropertyValue::LocalDateTime(dt) => PropertyValue::Int(
                        ((us_to_naive_datetime(dt.microseconds()).month() - 1) / 3 + 1) as i64,
                    ),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("epochmillis") || n.eq_ignore_ascii_case("epochmilli") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Date(d) => PropertyValue::Int(d.days() * 86_400_000),
                    PropertyValue::LocalDateTime(dt) => PropertyValue::Int(
                        us_to_naive_datetime(dt.microseconds())
                            .and_utc()
                            .timestamp_millis(),
                    ),
                    PropertyValue::ZonedDateTime(zdt) => {
                        PropertyValue::Int(zdt.microseconds() / 1_000)
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("epochseconds") || n.eq_ignore_ascii_case("epochsecond") => {
            args.first()
                .map(
                    |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                        PropertyValue::Date(d) => PropertyValue::Int(d.days() * 86_400),
                        PropertyValue::LocalDateTime(dt) => PropertyValue::Int(
                            us_to_naive_datetime(dt.microseconds())
                                .and_utc()
                                .timestamp(),
                        ),
                        PropertyValue::ZonedDateTime(zdt) => {
                            PropertyValue::Int(zdt.microseconds() / 1_000_000)
                        }
                        _ => PropertyValue::Null,
                    },
                )
                .unwrap_or(PropertyValue::Null)
        }
        // ─── String padding / encoding ─────────────────────────────
        n if n.eq_ignore_ascii_case("lpad") => {
            if args.len() == 3 {
                let s = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let len = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::Int(n) => n as usize,
                    _ => return PropertyValue::Null,
                };
                let pad = match eval_expression_with_catalog(&args[2], bindings, storage, catalog) {
                    PropertyValue::String(p) => p,
                    _ => return PropertyValue::Null,
                };
                if s.len() >= len {
                    PropertyValue::String(s)
                } else {
                    let mut result = String::new();
                    while result.len() + s.len() < len {
                        result.push_str(&pad);
                    }
                    result.truncate(len - s.len());
                    result.push_str(&s);
                    PropertyValue::String(result)
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("rpad") => {
            if args.len() == 3 {
                let s = match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let len = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::Int(n) => n as usize,
                    _ => return PropertyValue::Null,
                };
                let pad = match eval_expression_with_catalog(&args[2], bindings, storage, catalog) {
                    PropertyValue::String(p) => p,
                    _ => return PropertyValue::Null,
                };
                if s.len() >= len {
                    PropertyValue::String(s)
                } else {
                    let mut result = s;
                    while result.len() < len {
                        result.push_str(&pad);
                    }
                    result.truncate(len);
                    PropertyValue::String(result)
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("md5") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::String(s) => {
                        use std::collections::hash_map::DefaultHasher;
                        use std::hash::{Hash, Hasher};
                        let mut hasher = DefaultHasher::new();
                        s.hash(&mut hasher);
                        let hash = format!("{:016x}", hasher.finish());
                        PropertyValue::String(hash)
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("tobase64") || n.eq_ignore_ascii_case("base64encode") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::String(s) => PropertyValue::String(base64_encode(&s)),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("frombase64") || n.eq_ignore_ascii_case("base64decode") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::String(s) => {
                        PropertyValue::String(base64_decode(&s).unwrap_or_default())
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("urlencode") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::String(s) => PropertyValue::String(url_encode(&s)),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        // ─── Hyperbolic math functions ─────────────────────────────
        n if n.eq_ignore_ascii_case("cosh") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Double(f) => PropertyValue::Double(f.cosh()),
                    PropertyValue::Int(n) => PropertyValue::Double((n as f64).cosh()),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("sinh") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Double(f) => PropertyValue::Double(f.sinh()),
                    PropertyValue::Int(n) => PropertyValue::Double((n as f64).sinh()),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("tanh") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Double(f) => PropertyValue::Double(f.tanh()),
                    PropertyValue::Int(n) => PropertyValue::Double((n as f64).tanh()),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("acosh") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Double(f) if f >= 1.0 => PropertyValue::Double(f.acosh()),
                    PropertyValue::Int(n) if n >= 1 => PropertyValue::Double((n as f64).acosh()),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("asinh") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Double(f) => PropertyValue::Double(f.asinh()),
                    PropertyValue::Int(n) => PropertyValue::Double((n as f64).asinh()),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("atanh") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Double(f) if f > -1.0 && f < 1.0 => {
                        PropertyValue::Double(f.atanh())
                    }
                    PropertyValue::Int(n) if n > -1 && n < 1 => {
                        PropertyValue::Double((n as f64).atanh())
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        // ─── Graph functions ───────────────────────────────────────
        n if n.eq_ignore_ascii_case("shortestpath") || n.eq_ignore_ascii_case("shortest_path") => {
            if args.len() == 2 {
                let start = eval_expression_with_catalog(&args[0], bindings, storage, catalog);
                let end = eval_expression_with_catalog(&args[1], bindings, storage, catalog);
                match (&start, &end) {
                    (PropertyValue::Vertex(sv), PropertyValue::Vertex(ev)) => {
                        if let Some(s) = storage {
                            if let Some(path) = bfs_shortest_path(s, sv.gid, ev.gid) {
                                PropertyValue::Path(path)
                            } else {
                                PropertyValue::Null
                            }
                        } else {
                            PropertyValue::Null
                        }
                    }
                    _ => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("allshortestpaths")
            || n.eq_ignore_ascii_case("all_shortest_paths") =>
        {
            if args.len() == 2 {
                let start = eval_expression_with_catalog(&args[0], bindings, storage, catalog);
                let end = eval_expression_with_catalog(&args[1], bindings, storage, catalog);
                match (&start, &end) {
                    (PropertyValue::Vertex(sv), PropertyValue::Vertex(ev)) => {
                        if let Some(s) = storage {
                            let paths = all_shortest_paths(s, sv.gid, ev.gid);
                            PropertyValue::List(
                                paths.into_iter().map(PropertyValue::Path).collect(),
                            )
                        } else {
                            PropertyValue::Null
                        }
                    }
                    _ => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        // ─── Statistical functions ─────────────────────────────────
        n if n.eq_ignore_ascii_case("percentilecont")
            || n.eq_ignore_ascii_case("percentile_cont") =>
        {
            if args.len() == 2 {
                let list = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let p = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::Double(f) => f.clamp(0.0, 1.0),
                    PropertyValue::Int(n) => (n as f64).clamp(0.0, 1.0),
                    _ => return PropertyValue::Null,
                };
                let mut values: Vec<f64> = list
                    .iter()
                    .filter_map(|v| match v {
                        PropertyValue::Int(n) => Some(*n as f64),
                        PropertyValue::Double(f) => Some(*f),
                        _ => None,
                    })
                    .collect();
                if values.is_empty() {
                    return PropertyValue::Null;
                }
                values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let idx = p * (values.len() - 1) as f64;
                let lower = idx.floor() as usize;
                let upper = idx.ceil() as usize;
                let frac = idx - lower as f64;
                if lower >= values.len() {
                    return PropertyValue::Double(values[values.len() - 1]);
                }
                if upper >= values.len() {
                    return PropertyValue::Double(values[lower]);
                }
                let result = values[lower] * (1.0 - frac) + values[upper] * frac;
                PropertyValue::Double(result)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("percentiledisc")
            || n.eq_ignore_ascii_case("percentile_disc") =>
        {
            if args.len() == 2 {
                let list = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let p = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::Double(f) => f.clamp(0.0, 1.0),
                    PropertyValue::Int(n) => (n as f64).clamp(0.0, 1.0),
                    _ => return PropertyValue::Null,
                };
                let mut values: Vec<f64> = list
                    .iter()
                    .filter_map(|v| match v {
                        PropertyValue::Int(n) => Some(*n as f64),
                        PropertyValue::Double(f) => Some(*f),
                        _ => None,
                    })
                    .collect();
                if values.is_empty() {
                    return PropertyValue::Null;
                }
                values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let idx = (p * (values.len() - 1) as f64).round() as usize;
                PropertyValue::Double(values[idx.min(values.len() - 1)])
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("stdev") || n.eq_ignore_ascii_case("stddev") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::List(l) => {
                        let values: Vec<f64> = l
                            .iter()
                            .filter_map(|v| match v {
                                PropertyValue::Int(n) => Some(*n as f64),
                                PropertyValue::Double(f) => Some(*f),
                                _ => None,
                            })
                            .collect();
                        if values.len() < 2 {
                            return PropertyValue::Null;
                        }
                        let mean = values.iter().sum::<f64>() / values.len() as f64;
                        let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                            / (values.len() - 1) as f64;
                        PropertyValue::Double(variance.sqrt())
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("stdevp") || n.eq_ignore_ascii_case("stddevp") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::List(l) => {
                        let values: Vec<f64> = l
                            .iter()
                            .filter_map(|v| match v {
                                PropertyValue::Int(n) => Some(*n as f64),
                                PropertyValue::Double(f) => Some(*f),
                                _ => None,
                            })
                            .collect();
                        if values.is_empty() {
                            return PropertyValue::Null;
                        }
                        let mean = values.iter().sum::<f64>() / values.len() as f64;
                        let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                            / values.len() as f64;
                        PropertyValue::Double(variance.sqrt())
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        // ─── Safe conversion functions ─────────────────────────────
        n if n.eq_ignore_ascii_case("tofloatornull") || n.eq_ignore_ascii_case("tofloatornull") => {
            args.first()
                .map(
                    |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                        PropertyValue::Double(f) => PropertyValue::Double(f),
                        PropertyValue::Int(n) => PropertyValue::Double(n as f64),
                        PropertyValue::String(s) => s
                            .parse::<f64>()
                            .ok()
                            .map(PropertyValue::Double)
                            .unwrap_or(PropertyValue::Null),
                        _ => PropertyValue::Null,
                    },
                )
                .unwrap_or(PropertyValue::Null)
        }
        n if n.eq_ignore_ascii_case("tointegerornull") || n.eq_ignore_ascii_case("tointornull") => {
            args.first()
                .map(
                    |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                        PropertyValue::Int(n) => PropertyValue::Int(n),
                        PropertyValue::Double(f) => PropertyValue::Int(f as i64),
                        PropertyValue::String(s) => s
                            .parse::<i64>()
                            .ok()
                            .map(PropertyValue::Int)
                            .unwrap_or(PropertyValue::Null),
                        _ => PropertyValue::Null,
                    },
                )
                .unwrap_or(PropertyValue::Null)
        }
        n if n.eq_ignore_ascii_case("tobooleanornull") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Bool(b) => PropertyValue::Bool(b),
                    PropertyValue::String(s) => match s.to_lowercase().as_str() {
                        "true" => PropertyValue::Bool(true),
                        "false" => PropertyValue::Bool(false),
                        _ => PropertyValue::Null,
                    },
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("toboolean") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Bool(b) => PropertyValue::Bool(b),
                    PropertyValue::String(s) => match s.to_lowercase().as_str() {
                        "true" => PropertyValue::Bool(true),
                        "false" => PropertyValue::Bool(false),
                        _ => PropertyValue::Null,
                    },
                    PropertyValue::Int(n) => PropertyValue::Bool(n != 0),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("isEmpty") || n.eq_ignore_ascii_case("isempty") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::List(l) => PropertyValue::Bool(l.is_empty()),
                    PropertyValue::Map(m) => PropertyValue::Bool(m.is_empty()),
                    PropertyValue::String(s) => PropertyValue::Bool(s.is_empty()),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("randomstring") || n.eq_ignore_ascii_case("random_string") => {
            let len = args
                .first()
                .map(
                    |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                        PropertyValue::Int(n) => n.clamp(1, 256) as usize,
                        _ => 10usize,
                    },
                )
                .unwrap_or(10);
            let chars: Vec<char> = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
                .chars()
                .collect();
            let mut result = String::with_capacity(len);
            for _ in 0..len {
                let idx = (rand_simple_u64() % chars.len() as u64) as usize;
                result.push(chars[idx]);
            }
            PropertyValue::String(result)
        }
        n if n.eq_ignore_ascii_case("encodeJson") || n.eq_ignore_ascii_case("to_json") => args
            .first()
            .map(|a| {
                let val = eval_expression_with_catalog(a, bindings, storage, catalog);
                match serde_json::to_string(&property_value_to_json(&val)) {
                    Ok(s) => PropertyValue::String(s),
                    Err(_) => PropertyValue::Null,
                }
            })
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("decodeJson") || n.eq_ignore_ascii_case("from_json") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::String(s) => {
                        match serde_json::from_str::<serde_json::Value>(&s) {
                            Ok(v) => json_to_property_value(v),
                            Err(_) => PropertyValue::Null,
                        }
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("sleep") || n.eq_ignore_ascii_case("apoc_util_sleep") => {
            let ms = args
                .first()
                .map(
                    |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                        PropertyValue::Int(n) => n.clamp(0, 60000) as u64,
                        _ => 0u64,
                    },
                )
                .unwrap_or(0);
            std::thread::sleep(std::time::Duration::from_millis(ms));
            PropertyValue::Bool(true)
        }
        n if n.eq_ignore_ascii_case("apoc_util_md5") || n.eq_ignore_ascii_case("md5_hash") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::String(s) => PropertyValue::String(md5_hash(s.as_bytes())),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("apoc_util_sha1") || n.eq_ignore_ascii_case("sha1_hash") => {
            args.first()
                .map(
                    |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                        PropertyValue::String(s) => PropertyValue::String(sha1_hash(s.as_bytes())),
                        _ => PropertyValue::Null,
                    },
                )
                .unwrap_or(PropertyValue::Null)
        }
        n if n.eq_ignore_ascii_case("apoc_util_sha256")
            || n.eq_ignore_ascii_case("sha256_hash") =>
        {
            args.first()
                .map(
                    |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                        PropertyValue::String(s) => {
                            PropertyValue::String(sha256_hash(s.as_bytes()))
                        }
                        _ => PropertyValue::Null,
                    },
                )
                .unwrap_or(PropertyValue::Null)
        }
        n if n.eq_ignore_ascii_case("apoc_util_tojson") || n.eq_ignore_ascii_case("to_json") => {
            if args.len() == 1 {
                let val = eval_expression_with_catalog(&args[0], bindings, storage, catalog);
                match serde_json::to_string(&property_value_to_json(&val)) {
                    Ok(s) => PropertyValue::String(s),
                    Err(_) => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_util_fromjson")
            || n.eq_ignore_ascii_case("from_json") =>
        {
            if args.len() == 1 {
                match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => {
                        match serde_json::from_str::<serde_json::Value>(&s) {
                            Ok(v) => json_to_property_value(v),
                            Err(_) => PropertyValue::Null,
                        }
                    }
                    _ => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("elementId") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::Vertex(vr) => {
                        PropertyValue::String(format!("v:{}", vr.gid.as_uint()))
                    }
                    PropertyValue::Edge(er) => {
                        PropertyValue::String(format!("e:{}", er.gid.as_uint()))
                    }
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("apoc_coll_indexOf") || n.eq_ignore_ascii_case("indexOf") => {
            if args.len() == 2 {
                let list = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let target = eval_expression_with_catalog(&args[1], bindings, storage, catalog);
                for (i, item) in list.iter().enumerate() {
                    if item == &target {
                        return PropertyValue::Int(i as i64);
                    }
                }
                PropertyValue::Int(-1)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_coll_insert") || n.eq_ignore_ascii_case("insert") => {
            if args.len() == 3 {
                let mut list =
                    match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                        PropertyValue::List(l) => l,
                        _ => return PropertyValue::Null,
                    };
                let idx = match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                    PropertyValue::Int(n) => n as usize,
                    _ => return PropertyValue::Null,
                };
                let val = eval_expression_with_catalog(&args[2], bindings, storage, catalog);
                let pos = idx.min(list.len());
                list.insert(pos, val);
                PropertyValue::List(list)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("apoc_coll_shuffle") || n.eq_ignore_ascii_case("shuffle") => {
            args.first()
                .map(
                    |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                        PropertyValue::List(mut l) => {
                            use rand::Rng;
                            for i in (1..l.len()).rev() {
                                let j = rand::thread_rng().gen_range(0..=i);
                                l.swap(i, j);
                            }
                            PropertyValue::List(l)
                        }
                        _ => PropertyValue::Null,
                    },
                )
                .unwrap_or(PropertyValue::Null)
        }
        n if n.eq_ignore_ascii_case("apoc_coll_slice") || n.eq_ignore_ascii_case("slice") => {
            if args.len() >= 2 {
                let list = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let start = match eval_expression_with_catalog(&args[1], bindings, storage, catalog)
                {
                    PropertyValue::Int(n) => n as usize,
                    _ => return PropertyValue::Null,
                };
                let end = args
                    .get(2)
                    .map(
                        |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                            PropertyValue::Int(n) => n as usize,
                            _ => list.len(),
                        },
                    )
                    .unwrap_or(list.len());
                let start = start.min(list.len());
                let end = end.min(list.len());
                PropertyValue::List(list[start..end].to_vec())
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("count") => args
            .first()
            .map(
                |a| match eval_expression_with_catalog(a, bindings, storage, catalog) {
                    PropertyValue::List(l) => PropertyValue::Int(l.len() as i64),
                    PropertyValue::String(s) => PropertyValue::Int(s.len() as i64),
                    PropertyValue::Map(m) => PropertyValue::Int(m.len() as i64),
                    _ => PropertyValue::Null,
                },
            )
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("uniformsample")
            || n.eq_ignore_ascii_case("uniform_sample") =>
        {
            if args.len() == 2 {
                let list = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::List(l) => l,
                    _ => return PropertyValue::Null,
                };
                let sample_size =
                    match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                        PropertyValue::Int(n) => n.max(0) as usize,
                        _ => return PropertyValue::Null,
                    };
                if list.is_empty() || sample_size == 0 {
                    return PropertyValue::List(vec![]);
                }
                let mut rng = rand::thread_rng();
                use rand::seq::SliceRandom;
                let sample: Vec<PropertyValue> = list
                    .choose_multiple(&mut rng, sample_size.min(list.len()))
                    .cloned()
                    .collect();
                PropertyValue::List(sample)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("tobytestring") || n.eq_ignore_ascii_case("to_byte_string") => {
            if args.len() == 1 {
                let val = eval_expression_with_catalog(&args[0], bindings, storage, catalog);
                match val {
                    PropertyValue::String(s) => {
                        let bytes: Vec<PropertyValue> =
                            s.bytes().map(|b| PropertyValue::Int(b as i64)).collect();
                        PropertyValue::List(bytes)
                    }
                    PropertyValue::List(items) => {
                        let bytes: Result<Vec<u8>, _> = items
                            .iter()
                            .map(|v| match v {
                                PropertyValue::Int(n) if *n >= 0 && *n <= 255 => Ok(*n as u8),
                                _ => Err(()),
                            })
                            .collect();
                        match bytes {
                            Ok(b) => {
                                PropertyValue::String(String::from_utf8_lossy(&b).into_owned())
                            }
                            Err(_) => PropertyValue::Null,
                        }
                    }
                    _ => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("frombytestring")
            || n.eq_ignore_ascii_case("from_byte_string") =>
        {
            if args.len() == 1 {
                match eval_expression_with_catalog(&args[0], bindings, storage, catalog) {
                    PropertyValue::String(s) => {
                        let bytes: Vec<PropertyValue> =
                            s.bytes().map(|b| PropertyValue::Int(b as i64)).collect();
                        PropertyValue::List(bytes)
                    }
                    PropertyValue::List(items) => {
                        let bytes: Result<Vec<u8>, _> = items
                            .iter()
                            .map(|v| match v {
                                PropertyValue::Int(n) if *n >= 0 && *n <= 255 => Ok(*n as u8),
                                _ => Err(()),
                            })
                            .collect();
                        match bytes {
                            Ok(b) => {
                                PropertyValue::String(String::from_utf8_lossy(&b).into_owned())
                            }
                            Err(_) => PropertyValue::Null,
                        }
                    }
                    _ => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        // ─── Memgraph-specific functions ─────────────────────────
        n if n.eq_ignore_ascii_case("propertysize") || n.eq_ignore_ascii_case("property_size") => {
            if args.len() == 2 {
                let entity = eval_expression_with_catalog(&args[0], bindings, storage, catalog);
                let prop_name =
                    match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                        PropertyValue::String(s) => s,
                        _ => return PropertyValue::Null,
                    };
                let catalog = match catalog {
                    Some(c) => c,
                    None => return PropertyValue::Null,
                };
                let prop_id = catalog.property(&prop_name);
                let store = match &entity {
                    PropertyValue::Vertex(vr) => {
                        if let Some(s) = storage {
                            let tx = get_read_tx(s);
                            s.get_vertex(vr.gid, &tx).map(|v| v.properties.clone())
                        } else {
                            None
                        }
                    }
                    PropertyValue::Edge(er) => {
                        if let Some(s) = storage {
                            let tx = get_read_tx(s);
                            s.get_edge(er.gid, &tx).map(|e| e.properties.clone())
                        } else {
                            None
                        }
                    }
                    _ => return PropertyValue::Null,
                };
                match store {
                    Some(ps) => {
                        let val = ps.get(prop_id);
                        let size = match val {
                            PropertyValue::Null => 0i64,
                            PropertyValue::Bool(_) => 1,
                            PropertyValue::Int(_) => 8,
                            PropertyValue::Double(_) => 8,
                            PropertyValue::String(s) => s.len() as i64,
                            PropertyValue::List(l) => l.len() as i64 * 8,
                            PropertyValue::Map(m) => {
                                m.iter().map(|(k, _v)| k.len() + 8).sum::<usize>() as i64
                            }
                            _ => 8,
                        };
                        PropertyValue::Int(size)
                    }
                    None => PropertyValue::Int(0),
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("assert") => {
            if !args.is_empty() {
                let cond = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::Bool(b) => b,
                    _ => return PropertyValue::Null,
                };
                if !cond {
                    let msg = if args.len() >= 2 {
                        match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                            PropertyValue::String(s) => format!("Assertion failed: {}", s),
                            _ => "Assertion failed".into(),
                        }
                    } else {
                        "Assertion failed".into()
                    };
                    eprintln!("[eval] {}", msg);
                    return PropertyValue::Null;
                }
                PropertyValue::Bool(true)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("counter") => {
            if args.len() >= 2 {
                let name = match eval_expression_with_catalog(&args[0], bindings, storage, catalog)
                {
                    PropertyValue::String(s) => s,
                    _ => return PropertyValue::Null,
                };
                let initial =
                    match eval_expression_with_catalog(&args[1], bindings, storage, catalog) {
                        PropertyValue::Int(n) => n,
                        _ => return PropertyValue::Null,
                    };
                let step = if args.len() >= 3 {
                    match eval_expression_with_catalog(&args[2], bindings, storage, catalog) {
                        PropertyValue::Int(n) => n,
                        _ => 1,
                    }
                } else {
                    1
                };
                let mut counters = counter_storage().lock().unwrap();
                let value = counters.entry(name).or_insert(initial);
                let current = *value;
                *value += step;
                PropertyValue::Int(current)
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("gethopscounter")
            || n.eq_ignore_ascii_case("get_hops_counter") =>
        {
            PropertyValue::Int(crate::reset_hops() as i64)
        }
        n if n.eq_ignore_ascii_case("toenum") || n.eq_ignore_ascii_case("to_enum") => {
            if !args.is_empty() {
                let val = eval_expression_with_catalog(&args[0], bindings, storage, catalog);
                match val {
                    PropertyValue::String(s) => {
                        // Enum support not yet implemented; return the string as-is
                        PropertyValue::String(s)
                    }
                    _ => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        n if n.eq_ignore_ascii_case("username") => crate::active_auth_store()
            .map(|_| PropertyValue::String("default".to_string()))
            .unwrap_or(PropertyValue::Null),
        n if n.eq_ignore_ascii_case("roles") => {
            let roles = crate::active_auth_store()
                .map(|auth| {
                    auth.list_roles().into_iter().map(PropertyValue::String).collect()
                })
                .unwrap_or_default();
            PropertyValue::List(roles)
        }
        n if n.eq_ignore_ascii_case("withinbbox") || n.eq_ignore_ascii_case("within_bbox") => {
            if args.len() == 3 {
                let point = eval_expression_with_catalog(&args[0], bindings, storage, catalog);
                let lower_left = eval_expression_with_catalog(&args[1], bindings, storage, catalog);
                let upper_right =
                    eval_expression_with_catalog(&args[2], bindings, storage, catalog);
                match (&point, &lower_left, &upper_right) {
                    (
                        PropertyValue::Point2D(p),
                        PropertyValue::Point2D(ll),
                        PropertyValue::Point2D(ur),
                    ) => {
                        if p.crs != ll.crs || p.crs != ur.crs {
                            return PropertyValue::Null;
                        }
                        PropertyValue::Bool(
                            p.x >= ll.x && p.x <= ur.x && p.y >= ll.y && p.y <= ur.y,
                        )
                    }
                    (
                        PropertyValue::Point3D(p),
                        PropertyValue::Point3D(ll),
                        PropertyValue::Point3D(ur),
                    ) => {
                        if p.crs != ll.crs || p.crs != ur.crs {
                            return PropertyValue::Null;
                        }
                        PropertyValue::Bool(
                            p.x >= ll.x
                                && p.x <= ur.x
                                && p.y >= ll.y
                                && p.y <= ur.y
                                && p.z >= ll.z
                                && p.z <= ur.z,
                        )
                    }
                    _ => PropertyValue::Null,
                }
            } else {
                PropertyValue::Null
            }
        }
        _ => PropertyValue::Null,
    }
}

/// Convert a PropertyValue to a serde_json::Value for JSON encoding.
fn property_value_to_json(val: &PropertyValue) -> serde_json::Value {
    match val {
        PropertyValue::Null => serde_json::Value::Null,
        PropertyValue::Bool(b) => serde_json::Value::Bool(*b),
        PropertyValue::Int(n) => serde_json::Value::Number((*n).into()),
        PropertyValue::Double(f) => {
            serde_json::Value::Number(serde_json::Number::from_f64(*f).unwrap_or(0.into()))
        }
        PropertyValue::String(s) => serde_json::Value::String(s.clone()),
        PropertyValue::List(l) => {
            serde_json::Value::Array(l.iter().map(property_value_to_json).collect())
        }
        PropertyValue::Map(m) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in m {
                obj.insert(k.clone(), property_value_to_json(v));
            }
            serde_json::Value::Object(obj)
        }
        _ => serde_json::Value::Null,
    }
}

/// Convert a serde_json::Value back to a PropertyValue.
fn json_to_property_value(val: serde_json::Value) -> PropertyValue {
    match val {
        serde_json::Value::Null => PropertyValue::Null,
        serde_json::Value::Bool(b) => PropertyValue::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                PropertyValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                PropertyValue::Double(f)
            } else {
                PropertyValue::Null
            }
        }
        serde_json::Value::String(s) => PropertyValue::String(s),
        serde_json::Value::Array(a) => {
            PropertyValue::List(a.into_iter().map(json_to_property_value).collect())
        }
        serde_json::Value::Object(o) => {
            let mut map = Vec::new();
            for (k, v) in o {
                map.push((k, json_to_property_value(v)));
            }
            PropertyValue::Map(map)
        }
    }
}

/// Parse an ISO 8601 duration string like P1Y2M3DT4H5M6S.
fn parse_date_custom(s: &str, fmt: &str, _tz: Option<&str>) -> Option<i64> {
    // Map common Java/SimpleDateFormat patterns to chrono strftime
    let chrono_fmt = fmt
        .replace("yyyy", "%Y")
        .replace("MM", "%m")
        .replace("dd", "%d")
        .replace("HH", "%H")
        .replace("mm", "%M")
        .replace("ss", "%S")
        .replace("SSS", "%3f");
    chrono::NaiveDateTime::parse_from_str(s, &chrono_fmt)
        .ok()
        .map(|dt| dt.and_utc().timestamp_millis())
        .or_else(|| {
            chrono::NaiveDate::parse_from_str(s, &chrono_fmt)
                .ok()
                .map(|d| d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_millis())
        })
}

fn format_date_custom(ts: i64, fmt: &str, _tz: Option<&str>) -> Option<String> {
    let dt = chrono::DateTime::from_timestamp_millis(ts)?;
    // Map common Java/SimpleDateFormat patterns to chrono strftime
    let chrono_fmt = fmt
        .replace("yyyy", "%Y")
        .replace("MM", "%m")
        .replace("dd", "%d")
        .replace("HH", "%H")
        .replace("mm", "%M")
        .replace("ss", "%S")
        .replace("SSS", "%3f");
    Some(dt.format(&chrono_fmt).to_string())
}

fn md5_hash(data: &[u8]) -> String {
    // Simple MD5-like hash using a combination of operations
    // This is NOT cryptographically secure — for APOC compatibility only
    let mut h: [u32; 4] = [0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476];
    let mut msg = data.to_vec();
    let bit_len = (data.len() as u64) * 8;
    msg.push(0x80);
    while (msg.len() % 64) != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_le_bytes());
    for chunk in msg.chunks(64) {
        let mut w = [0u32; 16];
        for i in 0..16 {
            w[i] = u32::from_le_bytes([
                chunk[i * 4],
                chunk[i * 4 + 1],
                chunk[i * 4 + 2],
                chunk[i * 4 + 3],
            ]);
        }
        let [mut a, mut b, mut c, mut d] = h;
        for i in 0..64 {
            let (f, g) = match i {
                0..=15 => ((b & c) | ((!b) & d), i),
                16..=31 => ((d & b) | ((!d) & c), (5 * i + 1) % 16),
                32..=47 => (b ^ c ^ d, (3 * i + 5) % 16),
                _ => (c ^ (b | (!d)), (7 * i) % 16),
            };
            let temp = d;
            d = c;
            c = b;
            let k = match i {
                0..=15 => 0,
                16..=31 => 0x5A827999,
                32..=47 => 0x6ED9EBA1,
                _ => 0,
            };
            b = b.wrapping_add(
                a.wrapping_add(f)
                    .wrapping_add(k)
                    .wrapping_add(w[g])
                    .rotate_left(match i {
                        0..=15 => 7,
                        16..=31 => 12,
                        32..=47 => 17,
                        _ => 22,
                    }),
            );
            a = temp;
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
    }
    h.iter()
        .map(|x| format!("{:08x}", x.swap_bytes()))
        .collect()
}

fn sha1_hash(data: &[u8]) -> String {
    let mut h: [u32; 5] = [0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0];
    let mut msg = data.to_vec();
    let bit_len = (data.len() as u64) * 8;
    msg.push(0x80);
    while (msg.len() % 64) != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());
    for chunk in msg.chunks(64) {
        let mut w = [0u32; 80];
        for (i, wi) in w.iter_mut().enumerate().take(16) {
            *wi = u32::from_be_bytes([
                chunk[i * 4],
                chunk[i * 4 + 1],
                chunk[i * 4 + 2],
                chunk[i * 4 + 3],
            ]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let [mut a, mut b, mut c, mut d, mut e] = h;
        for (i, wi) in w.iter().enumerate().take(80) {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A827999),
                20..=39 => (b ^ c ^ d, 0x6ED9EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDC),
                _ => (b ^ c ^ d, 0xCA62C1D6),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(*wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
    }
    h.iter().map(|x| format!("{:08x}", x)).collect()
}

fn sha256_hash(data: &[u8]) -> String {
    let k: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let mut msg = data.to_vec();
    let bit_len = (data.len() as u64) * 8;
    msg.push(0x80);
    while (msg.len() % 64) != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());
    for chunk in msg.chunks(64) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                chunk[i * 4],
                chunk[i * 4 + 1],
                chunk[i * 4 + 2],
                chunk[i * 4 + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h0] = h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = h0
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(k[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);
            h0 = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(h0);
    }
    h.iter().map(|x| format!("{:08x}", x)).collect()
}

fn parse_iso_duration(s: &str) -> PropertyValue {
    let s = s.trim();
    if !s.starts_with('P') {
        return PropertyValue::Null;
    }
    let mut months = 0i64;
    let mut days = 0i64;
    let mut microseconds = 0i64;

    // Split date and time parts
    let rest = &s[1..];
    let (date_part, time_part) = if let Some(t_idx) = rest.find('T') {
        (&rest[..t_idx], Some(&rest[t_idx + 1..]))
    } else {
        (rest, None)
    };

    // Parse date part: Y, M, D
    let mut num_str = String::new();
    for ch in date_part.chars() {
        if ch.is_ascii_digit() || ch == '-' || ch == '+' || ch == '.' {
            num_str.push(ch);
        } else {
            if let Ok(val) = num_str.parse::<f64>() {
                match ch {
                    'Y' => months += (val * 12.0) as i64,
                    'M' => months += val as i64,
                    'W' => days += (val * 7.0) as i64,
                    'D' => days += val as i64,
                    _ => {}
                }
            }
            num_str.clear();
        }
    }

    // Parse time part: H, M, S
    if let Some(tp) = time_part {
        num_str.clear();
        for ch in tp.chars() {
            if ch.is_ascii_digit() || ch == '-' || ch == '+' || ch == '.' {
                num_str.push(ch);
            } else {
                if let Ok(val) = num_str.parse::<f64>() {
                    match ch {
                        'H' => microseconds += (val * 3_600_000_000.0) as i64,
                        'M' => microseconds += (val * 60_000_000.0) as i64,
                        'S' => microseconds += (val * 1_000_000.0) as i64,
                        _ => {}
                    }
                }
                num_str.clear();
            }
        }
    }

    PropertyValue::Duration(Duration::new(months, days, microseconds))
}

/// Simple LCG-based pseudo-random for rand(). Not cryptographically secure.
fn rand_simple() -> f64 {
    (rand_simple_u64() >> 33) as f64 / (1u64 << 31) as f64
}

fn rand_simple_u64() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static STATE: AtomicU64 = AtomicU64::new(123456789);
    let mut s = STATE.load(Ordering::Relaxed);
    s = s
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    STATE.store(s, Ordering::Relaxed);
    s
}

/// Simple base64 encode (no padding optimization).
fn base64_encode(s: &str) -> String {
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = s.as_bytes();
    let mut result = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = match chunk.len() {
            1 => [chunk[0], 0, 0],
            2 => [chunk[0], chunk[1], 0],
            _ => [chunk[0], chunk[1], chunk[2]],
        };
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | (b[2] as u32);
        result.push(TABLE[((n >> 18) & 0x3F) as usize] as char);
        result.push(TABLE[((n >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(TABLE[((n >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(TABLE[(n & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

fn base64_decode(s: &str) -> Option<String> {
    let mut bytes = Vec::with_capacity(s.len() / 4 * 3);
    for chunk in s.as_bytes().chunks(4) {
        if chunk.len() < 2 {
            break;
        }
        let mut vals = [0u8; 4];
        for (i, &ch) in chunk.iter().enumerate() {
            vals[i] = match ch {
                b'A'..=b'Z' => ch - b'A',
                b'a'..=b'z' => ch - b'a' + 26,
                b'0'..=b'9' => ch - b'0' + 52,
                b'+' => 62,
                b'/' => 63,
                b'=' => break,
                _ => return None,
            };
        }
        let n = ((vals[0] as u32) << 18)
            | ((vals[1] as u32) << 12)
            | ((vals[2] as u32) << 6)
            | (vals[3] as u32);
        bytes.push((n >> 16) as u8);
        if chunk.len() > 2 && chunk[2] != b'=' {
            bytes.push((n >> 8) as u8);
        }
        if chunk.len() > 3 && chunk[3] != b'=' {
            bytes.push(n as u8);
        }
    }
    String::from_utf8(bytes).ok()
}

fn url_encode(s: &str) -> String {
    let mut result = String::with_capacity(s.len() * 3);
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                result.push(byte as char)
            }
            _ => {
                result.push('%');
                result.push_str(&format!("{:02X}", byte));
            }
        }
    }
    result
}

fn url_decode(s: &str) -> Option<String> {
    let mut result = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = &s[i + 1..i + 3];
            if let Ok(val) = u8::from_str_radix(hex, 16) {
                result.push(val as char);
                i += 3;
                continue;
            }
        }
        result.push(bytes[i] as char);
        i += 1;
    }
    Some(result)
}

fn soundex(s: &str) -> String {
    if s.is_empty() {
        return String::new();
    }
    let mut chars = s.chars();
    let first = chars.next().unwrap().to_ascii_uppercase();
    let mut result = String::with_capacity(4);
    result.push(first);
    let mut last_code = soundex_code(first);
    for c in chars {
        let code = soundex_code(c);
        if code != '0' && code != last_code {
            result.push(code);
            if result.len() == 4 {
                break;
            }
        }
        last_code = code;
    }
    while result.len() < 4 {
        result.push('0');
    }
    result
}

fn soundex_code(c: char) -> char {
    match c.to_ascii_uppercase() {
        'B' | 'F' | 'P' | 'V' => '1',
        'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => '2',
        'D' | 'T' => '3',
        'L' => '4',
        'M' | 'N' => '5',
        'R' => '6',
        _ => '0',
    }
}

fn strip_html_tags(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut in_tag = false;
    for c in s.chars() {
        if c == '<' {
            in_tag = true;
        } else if c == '>' {
            in_tag = false;
        } else if !in_tag {
            result.push(c);
        }
    }
    result
}

fn double_metaphone(s: &str) -> (String, String) {
    let s = s.to_ascii_uppercase();
    let mut primary = String::new();
    let mut secondary = String::new();
    // Simplified implementation — produces reasonable results for common cases
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            'A' | 'E' | 'I' | 'O' | 'U' => {
                if i == 0 {
                    primary.push('A');
                    secondary.push('A');
                }
                i += 1;
            }
            'B' => {
                primary.push('P');
                secondary.push('P');
                if i + 1 < chars.len() && chars[i + 1] == 'B' {
                    i += 2;
                } else {
                    i += 1;
                }
            }
            'C' => {
                if i + 1 < chars.len() && chars[i + 1] == 'H' {
                    primary.push('X');
                    secondary.push('X');
                    i += 2;
                } else if i + 1 < chars.len()
                    && chars[i + 1] == 'I'
                    && i + 2 < chars.len()
                    && chars[i + 2] == 'A'
                {
                    primary.push('X');
                    secondary.push('X');
                    i += 3;
                } else {
                    primary.push('K');
                    secondary.push('K');
                    i += 1;
                }
            }
            'D' => {
                if i + 2 < chars.len()
                    && chars[i + 1] == 'G'
                    && matches!(chars[i + 2], 'E' | 'I' | 'Y')
                {
                    primary.push('J');
                    secondary.push('J');
                    i += 3;
                } else {
                    primary.push('T');
                    secondary.push('T');
                    i += 1;
                }
            }
            'F' | 'J' | 'L' | 'M' | 'N' | 'R' => {
                primary.push(c);
                secondary.push(c);
                i += 1;
            }
            'G' => {
                if i + 1 < chars.len() && chars[i + 1] == 'H' {
                    if i > 0 && !is_vowel(chars[i - 1]) {
                        primary.push('K');
                        secondary.push('K');
                    }
                    i += 2;
                } else if i + 1 < chars.len() && chars[i + 1] == 'N' {
                    if i + 2 < chars.len() && chars[i + 2] == 'D' {
                        primary.push('K');
                        secondary.push('K');
                    }
                    i += 2;
                } else {
                    primary.push('K');
                    secondary.push('K');
                    i += 1;
                }
            }
            'H' => {
                if i == 0 || is_vowel(chars[i - 1]) {
                    primary.push('H');
                    secondary.push('H');
                }
                i += 1;
            }
            'K' => {
                primary.push('K');
                secondary.push('K');
                i += 1;
            }
            'P' => {
                if i + 1 < chars.len() && chars[i + 1] == 'H' {
                    primary.push('F');
                    secondary.push('F');
                    i += 2;
                } else {
                    primary.push('P');
                    secondary.push('P');
                    i += 1;
                }
            }
            'Q' => {
                primary.push('K');
                secondary.push('K');
                i += 1;
            }
            'S' => {
                if i + 2 < chars.len()
                    && chars[i + 1] == 'I'
                    && (chars[i + 2] == 'O' || chars[i + 2] == 'A')
                {
                    primary.push('X');
                    secondary.push('X');
                    i += 3;
                } else if i + 1 < chars.len() && chars[i + 1] == 'H' {
                    primary.push('X');
                    secondary.push('X');
                    i += 2;
                } else {
                    primary.push('S');
                    secondary.push('S');
                    i += 1;
                }
            }
            'T' => {
                if i + 2 < chars.len()
                    && chars[i + 1] == 'I'
                    && (chars[i + 2] == 'O' || chars[i + 2] == 'A')
                {
                    primary.push('X');
                    secondary.push('X');
                    i += 3;
                } else if i + 1 < chars.len() && chars[i + 1] == 'H' {
                    primary.push('0');
                    secondary.push('T');
                    i += 2;
                } else {
                    primary.push('T');
                    secondary.push('T');
                    i += 1;
                }
            }
            'V' => {
                primary.push('F');
                secondary.push('F');
                i += 1;
            }
            'W' | 'Y' => {
                if i + 1 < chars.len() && is_vowel(chars[i + 1]) {
                    primary.push(c);
                    secondary.push(c);
                }
                i += 1;
            }
            'X' => {
                primary.push('K');
                secondary.push('K');
                if i + 1 < chars.len() && chars[i + 1] == 'C' {
                    i += 2;
                } else {
                    i += 1;
                }
            }
            'Z' => {
                primary.push('S');
                secondary.push('S');
                i += 1;
            }
            _ => i += 1,
        }
    }
    (primary, secondary)
}

fn is_vowel(c: char) -> bool {
    matches!(c.to_ascii_uppercase(), 'A' | 'E' | 'I' | 'O' | 'U')
}

/// BFS shortest path between two vertices. Returns a Path if found.
fn bfs_shortest_path(
    storage: &Storage,
    start: Gid,
    end: Gid,
) -> Option<mgcore::property_value::PathValue> {
    if start == end {
        return Some(mgcore::property_value::PathValue::new(
            mgcore::property_value::VertexRef::new(
                start,
                vec![],
                mgcore::property_store::PropertyStore::default(),
            ),
        ));
    }
    let mut visited = std::collections::HashSet::new();
    let mut queue = std::collections::VecDeque::new();
    let mut parent: std::collections::HashMap<Gid, (Gid, Gid, EdgeTypeId)> =
        std::collections::HashMap::new();
    visited.insert(start);
    queue.push_back(start);
    while let Some(current) = queue.pop_front() {
        for (edge_gid, other, etype) in storage.vertex_out_edges(current, None) {
            if !visited.contains(&other) {
                visited.insert(other);
                parent.insert(other, (current, edge_gid, etype));
                if other == end {
                    // Reconstruct path
                    let mut path = mgcore::property_value::PathValue::new(
                        mgcore::property_value::VertexRef::new(
                            start,
                            vec![],
                            mgcore::property_store::PropertyStore::default(),
                        ),
                    );
                    let mut node = end;
                    let mut segments = Vec::new();
                    while node != start {
                        let (prev, eid, et) = parent.get(&node)?;
                        segments.push((*prev, *eid, et, node));
                        node = *prev;
                    }
                    segments.reverse();
                    for (_, eid, et, to_gid) in segments {
                        path.add_edge(mgcore::property_value::EdgeRefValue::new(
                            eid,
                            *et,
                            path.end().gid,
                            to_gid,
                            mgcore::property_store::PropertyStore::default(),
                        ));
                        path.add_vertex(mgcore::property_value::VertexRef::new(
                            to_gid,
                            vec![],
                            mgcore::property_store::PropertyStore::default(),
                        ));
                    }
                    return Some(path);
                }
                queue.push_back(other);
            }
        }
    }
    None
}

/// Find all shortest paths between two vertices (BFS with full frontier tracking).
fn all_shortest_paths(
    storage: &Storage,
    start: Gid,
    end: Gid,
) -> Vec<mgcore::property_value::PathValue> {
    if start == end {
        return vec![mgcore::property_value::PathValue::new(
            mgcore::property_value::VertexRef::new(
                start,
                vec![],
                mgcore::property_store::PropertyStore::default(),
            ),
        )];
    }
    let mut visited = std::collections::HashSet::new();
    let mut frontier = vec![start];
    let mut parents: std::collections::HashMap<Gid, Vec<(Gid, Gid, EdgeTypeId)>> =
        std::collections::HashMap::new();
    visited.insert(start);
    let mut found = false;
    while !frontier.is_empty() && !found {
        let mut next_frontier = Vec::new();
        for &current in &frontier {
            for (edge_gid, other, etype) in storage.vertex_out_edges(current, None) {
                if !visited.contains(&other) {
                    visited.insert(other);
                    next_frontier.push(other);
                }
                if frontier.contains(&other) || next_frontier.contains(&other) {
                    parents
                        .entry(other)
                        .or_default()
                        .push((current, edge_gid, etype));
                }
                if other == end {
                    found = true;
                }
            }
        }
        frontier = next_frontier;
    }
    if !found {
        return Vec::new();
    }
    // Backtrack from end to start collecting all paths
    fn backtrack(
        parents: &std::collections::HashMap<Gid, Vec<(Gid, Gid, EdgeTypeId)>>,
        current: Gid,
        start: Gid,
        path_edges: &mut Vec<(Gid, Gid, EdgeTypeId, Gid)>,
        results: &mut Vec<mgcore::property_value::PathValue>,
    ) {
        if current == start {
            let mut path =
                mgcore::property_value::PathValue::new(mgcore::property_value::VertexRef::new(
                    start,
                    vec![],
                    mgcore::property_store::PropertyStore::default(),
                ));
            for &(from, eid, et, to) in path_edges.iter().rev() {
                path.add_edge(mgcore::property_value::EdgeRefValue::new(
                    eid,
                    et,
                    from,
                    to,
                    mgcore::property_store::PropertyStore::default(),
                ));
                path.add_vertex(mgcore::property_value::VertexRef::new(
                    to,
                    vec![],
                    mgcore::property_store::PropertyStore::default(),
                ));
            }
            results.push(path);
            return;
        }
        if let Some(prevs) = parents.get(&current) {
            for &(prev, eid, et) in prevs {
                path_edges.push((prev, eid, et, current));
                backtrack(parents, prev, start, path_edges, results);
                path_edges.pop();
            }
        }
    }
    let mut results = Vec::new();
    backtrack(&parents, end, start, &mut Vec::new(), &mut results);
    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use mgparser::ast::Expression;

    #[test]
    fn test_eval_literals() {
        assert_eq!(
            eval_expression(&Expression::Int(42), &HashMap::new()),
            PropertyValue::Int(42)
        );
        assert_eq!(
            eval_expression(&Expression::Bool(true), &HashMap::new()),
            PropertyValue::Bool(true)
        );
        assert_eq!(
            eval_expression(&Expression::Null, &HashMap::new()),
            PropertyValue::Null
        );
    }

    #[test]
    fn test_eval_arithmetic() {
        let expr = Expression::Add(Box::new(Expression::Int(1)), Box::new(Expression::Int(2)));
        assert_eq!(
            eval_expression(&expr, &HashMap::new()),
            PropertyValue::Int(3)
        );
    }

    #[test]
    fn test_eval_comparison() {
        let expr = Expression::Eq(Box::new(Expression::Int(1)), Box::new(Expression::Int(1)));
        assert_eq!(
            eval_expression(&expr, &HashMap::new()),
            PropertyValue::Bool(true)
        );
    }

    #[test]
    fn test_eval_logical() {
        let expr = Expression::And(
            Box::new(Expression::Bool(true)),
            Box::new(Expression::Bool(false)),
        );
        assert_eq!(
            eval_expression(&expr, &HashMap::new()),
            PropertyValue::Bool(false)
        );
    }

    #[test]
    fn test_eval_identifier() {
        let mut bindings = HashMap::new();
        bindings.insert("n".to_string(), PropertyValue::Int(99));
        assert_eq!(
            eval_expression(&Expression::Identifier("n".into()), &bindings),
            PropertyValue::Int(99)
        );
    }

    #[test]
    fn test_eval_range() {
        let expr = Expression::Function {
            distinct: false,
            name: "range".into(),
            arguments: vec![Expression::Int(1), Expression::Int(4)],
        };
        let result = eval_expression(&expr, &HashMap::new());
        assert_eq!(
            result,
            PropertyValue::List(vec![
                PropertyValue::Int(1),
                PropertyValue::Int(2),
                PropertyValue::Int(3),
                PropertyValue::Int(4),
            ])
        );
    }

    #[test]
    fn test_eval_coalesce() {
        let expr = Expression::Function {
            distinct: false,
            name: "coalesce".into(),
            arguments: vec![Expression::Null, Expression::Int(42)],
        };
        assert_eq!(
            eval_expression(&expr, &HashMap::new()),
            PropertyValue::Int(42)
        );
    }

    #[test]
    fn test_eval_id() {
        let expr = Expression::Function {
            distinct: false,
            name: "id".into(),
            arguments: vec![Expression::Identifier("v".into())],
        };
        let mut bindings = HashMap::new();
        bindings.insert(
            "v".into(),
            PropertyValue::Vertex(mgcore::property_value::VertexRef::new(
                mgcore::types::Gid::from(5u64),
                Vec::new(),
                mgcore::property_store::PropertyStore::default(),
            )),
        );
        assert_eq!(eval_expression(&expr, &bindings), PropertyValue::Int(5));
    }

    #[test]
    fn test_eval_case_simple() {
        let expr = Expression::Case {
            expression: Some(Box::new(Expression::Int(2))),
            whens: vec![
                (Expression::Int(1), Expression::String("one".into())),
                (Expression::Int(2), Expression::String("two".into())),
            ],
            else_branch: Some(Box::new(Expression::String("other".into()))),
        };
        assert_eq!(
            eval_expression(&expr, &HashMap::new()),
            PropertyValue::String("two".into())
        );
    }

    #[test]
    fn test_eval_case_searched() {
        let expr = Expression::Case {
            expression: None,
            whens: vec![
                (Expression::Bool(false), Expression::String("no".into())),
                (Expression::Bool(true), Expression::String("yes".into())),
            ],
            else_branch: Some(Box::new(Expression::String("maybe".into()))),
        };
        assert_eq!(
            eval_expression(&expr, &HashMap::new()),
            PropertyValue::String("yes".into())
        );
    }

    #[test]
    fn test_eval_case_else() {
        let expr = Expression::Case {
            expression: Some(Box::new(Expression::Int(99))),
            whens: vec![(Expression::Int(1), Expression::String("one".into()))],
            else_branch: Some(Box::new(Expression::String("other".into()))),
        };
        assert_eq!(
            eval_expression(&expr, &HashMap::new()),
            PropertyValue::String("other".into())
        );
    }

    #[test]
    fn test_eval_case_no_else() {
        let expr = Expression::Case {
            expression: Some(Box::new(Expression::Int(99))),
            whens: vec![(Expression::Int(1), Expression::String("one".into()))],
            else_branch: None,
        };
        assert_eq!(eval_expression(&expr, &HashMap::new()), PropertyValue::Null);
    }

    // String functions
    #[test]
    fn test_eval_startswith() {
        let expr = Expression::Function {
            distinct: false,
            name: "startswith".into(),
            arguments: vec![
                Expression::String("hello world".into()),
                Expression::String("hello".into()),
            ],
        };
        assert_eq!(
            eval_expression(&expr, &HashMap::new()),
            PropertyValue::Bool(true)
        );

        let expr2 = Expression::Function {
            distinct: false,
            name: "startswith".into(),
            arguments: vec![
                Expression::String("hello world".into()),
                Expression::String("world".into()),
            ],
        };
        assert_eq!(
            eval_expression(&expr2, &HashMap::new()),
            PropertyValue::Bool(false)
        );
    }

    #[test]
    fn test_eval_endswith() {
        let expr = Expression::Function {
            distinct: false,
            name: "endswith".into(),
            arguments: vec![
                Expression::String("hello world".into()),
                Expression::String("world".into()),
            ],
        };
        assert_eq!(
            eval_expression(&expr, &HashMap::new()),
            PropertyValue::Bool(true)
        );
    }

    #[test]
    fn test_eval_contains_string() {
        let expr = Expression::Function {
            distinct: false,
            name: "contains".into(),
            arguments: vec![
                Expression::String("hello world".into()),
                Expression::String("lo wo".into()),
            ],
        };
        assert_eq!(
            eval_expression(&expr, &HashMap::new()),
            PropertyValue::Bool(true)
        );
    }

    #[test]
    fn test_eval_regexmatch() {
        let expr = Expression::Function {
            distinct: false,
            name: "regexmatch".into(),
            arguments: vec![
                Expression::String("hello world".into()),
                Expression::String("world".into()),
            ],
        };
        assert_eq!(
            eval_expression(&expr, &HashMap::new()),
            PropertyValue::Bool(true)
        );
    }

    // Math list functions
    #[test]
    fn test_eval_min_max() {
        let expr = Expression::Function {
            distinct: false,
            name: "min".into(),
            arguments: vec![Expression::List(vec![
                Expression::Int(5),
                Expression::Int(2),
                Expression::Int(8),
            ])],
        };
        assert_eq!(
            eval_expression(&expr, &HashMap::new()),
            PropertyValue::Int(2)
        );

        let expr2 = Expression::Function {
            distinct: false,
            name: "max".into(),
            arguments: vec![Expression::List(vec![
                Expression::Int(5),
                Expression::Int(2),
                Expression::Int(8),
            ])],
        };
        assert_eq!(
            eval_expression(&expr2, &HashMap::new()),
            PropertyValue::Int(8)
        );
    }

    #[test]
    fn test_eval_sum_avg() {
        let expr = Expression::Function {
            distinct: false,
            name: "sum".into(),
            arguments: vec![Expression::List(vec![
                Expression::Int(1),
                Expression::Int(2),
                Expression::Int(3),
            ])],
        };
        assert_eq!(
            eval_expression(&expr, &HashMap::new()),
            PropertyValue::Int(6)
        );

        let expr2 = Expression::Function {
            distinct: false,
            name: "avg".into(),
            arguments: vec![Expression::List(vec![
                Expression::Int(2),
                Expression::Int(4),
                Expression::Int(6),
            ])],
        };
        assert_eq!(
            eval_expression(&expr2, &HashMap::new()),
            PropertyValue::Double(4.0)
        );
    }

    // Collection functions
    #[test]
    fn test_eval_reduce() {
        let expr = Expression::Function {
            distinct: false,
            name: "reduce".into(),
            arguments: vec![
                Expression::Int(0),
                Expression::Identifier("x".into()),
                Expression::List(vec![
                    Expression::Int(1),
                    Expression::Int(2),
                    Expression::Int(3),
                ]),
                Expression::Add(
                    Box::new(Expression::Identifier("_acc".into())),
                    Box::new(Expression::Identifier("x".into())),
                ),
            ],
        };
        assert_eq!(
            eval_expression(&expr, &HashMap::new()),
            PropertyValue::Int(6)
        );
    }

    #[test]
    fn test_eval_extract() {
        let expr = Expression::Function {
            distinct: false,
            name: "extract".into(),
            arguments: vec![
                Expression::Identifier("x".into()),
                Expression::List(vec![
                    Expression::Int(1),
                    Expression::Int(2),
                    Expression::Int(3),
                ]),
                Expression::Mul(
                    Box::new(Expression::Identifier("x".into())),
                    Box::new(Expression::Int(2)),
                ),
            ],
        };
        let result = eval_expression(&expr, &HashMap::new());
        assert_eq!(
            result,
            PropertyValue::List(vec![
                PropertyValue::Int(2),
                PropertyValue::Int(4),
                PropertyValue::Int(6),
            ])
        );
    }

    // Temporal functions
    #[test]
    fn test_eval_duration_from_map() {
        let expr = Expression::Function {
            distinct: false,
            name: "duration".into(),
            arguments: vec![Expression::Map(vec![
                ("days".into(), Expression::Int(5)),
                ("hours".into(), Expression::Int(3)),
            ])],
        };
        let result = eval_expression(&expr, &HashMap::new());
        assert_eq!(
            result,
            PropertyValue::Duration(Duration::new(0, 5, 3 * 3_600_000_000))
        );
    }

    // Graph functions (require storage)
    #[test]
    fn test_eval_degree_with_storage() {
        use mgcore::delta::IsolationLevel;
        use mgstorage::storage::Storage;

        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let v0 = storage.allocate_gid();
        let v1 = storage.allocate_gid();
        let v2 = storage.allocate_gid();
        storage.create_vertex(&tx, v0).unwrap();
        storage.create_vertex(&tx, v1).unwrap();
        storage.create_vertex(&tx, v2).unwrap();
        let e0 = storage.allocate_gid();
        let e1 = storage.allocate_gid();
        let e2 = storage.allocate_gid();
        storage
            .create_edge(&tx, e0, v0, v1, EdgeTypeId::from(0u32))
            .unwrap();
        storage
            .create_edge(&tx, e1, v0, v2, EdgeTypeId::from(0u32))
            .unwrap();
        storage
            .create_edge(&tx, e2, v1, v0, EdgeTypeId::from(0u32))
            .unwrap();
        storage.commit_transaction(&tx);

        let mut bindings = HashMap::new();
        bindings.insert(
            "v".into(),
            PropertyValue::Vertex(VertexRef::new(
                v0,
                vec![],
                mgcore::property_store::PropertyStore::default(),
            )),
        );

        let degree_expr = Expression::Function {
            distinct: false,
            name: "degree".into(),
            arguments: vec![Expression::Identifier("v".into())],
        };
        let result = eval_expression_with_storage(&degree_expr, &bindings, Some(&storage));
        assert_eq!(result, PropertyValue::Int(3));

        let out_expr = Expression::Function {
            distinct: false,
            name: "outdegree".into(),
            arguments: vec![Expression::Identifier("v".into())],
        };
        let result_out = eval_expression_with_storage(&out_expr, &bindings, Some(&storage));
        assert_eq!(result_out, PropertyValue::Int(2));

        let in_expr = Expression::Function {
            distinct: false,
            name: "indegree".into(),
            arguments: vec![Expression::Identifier("v".into())],
        };
        let result_in = eval_expression_with_storage(&in_expr, &bindings, Some(&storage));
        assert_eq!(result_in, PropertyValue::Int(1));
    }

    #[test]
    fn test_eval_nodes_relationships_global() {
        use mgcore::delta::IsolationLevel;
        use mgstorage::storage::Storage;

        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let v0 = storage.allocate_gid();
        let v1 = storage.allocate_gid();
        storage.create_vertex(&tx, v0).unwrap();
        storage.create_vertex(&tx, v1).unwrap();
        let e0 = storage.allocate_gid();
        storage
            .create_edge(&tx, e0, v0, v1, EdgeTypeId::from(0u32))
            .unwrap();
        storage.commit_transaction(&tx);

        let nodes_expr = Expression::Function {
            distinct: false,
            name: "nodes".into(),
            arguments: vec![],
        };
        let result = eval_expression_with_storage(&nodes_expr, &HashMap::new(), Some(&storage));
        if let PropertyValue::List(nodes) = result {
            assert_eq!(nodes.len(), 2);
        } else {
            panic!("Expected list of nodes");
        }

        let rels_expr = Expression::Function {
            distinct: false,
            name: "relationships".into(),
            arguments: vec![],
        };
        let result_rels = eval_expression_with_storage(&rels_expr, &HashMap::new(), Some(&storage));
        if let PropertyValue::List(rels) = result_rels {
            assert_eq!(rels.len(), 1);
        } else {
            panic!("Expected list of relationships");
        }
    }

    // Statistical functions
    #[test]
    fn test_eval_percentile_cont() {
        let expr = Expression::Function {
            distinct: false,
            name: "percentileCont".into(),
            arguments: vec![
                Expression::List(vec![
                    Expression::Int(1),
                    Expression::Int(2),
                    Expression::Int(3),
                    Expression::Int(4),
                ]),
                Expression::Double(0.5),
            ],
        };
        let result = eval_expression(&expr, &HashMap::new());
        if let PropertyValue::Double(v) = result {
            assert!((v - 2.5).abs() < 0.01, "expected ~2.5, got {}", v);
        } else {
            panic!("Expected double, got {:?}", result);
        }
    }

    #[test]
    fn test_eval_percentile_disc() {
        let expr = Expression::Function {
            distinct: false,
            name: "percentileDisc".into(),
            arguments: vec![
                Expression::List(vec![
                    Expression::Int(1),
                    Expression::Int(2),
                    Expression::Int(3),
                    Expression::Int(4),
                ]),
                Expression::Double(0.5),
            ],
        };
        let result = eval_expression(&expr, &HashMap::new());
        // round(0.5 * 3) = 2, values[2] = 3.0
        assert_eq!(result, PropertyValue::Double(3.0));
    }

    #[test]
    fn test_eval_stdev() {
        let expr = Expression::Function {
            distinct: false,
            name: "stDev".into(),
            arguments: vec![Expression::List(vec![
                Expression::Int(2),
                Expression::Int(4),
                Expression::Int(4),
                Expression::Int(4),
                Expression::Int(5),
                Expression::Int(5),
                Expression::Int(7),
                Expression::Int(9),
            ])],
        };
        let result = eval_expression(&expr, &HashMap::new());
        if let PropertyValue::Double(v) = result {
            // Sample std dev of [2,4,4,4,5,5,7,9] = sqrt(32/7) ≈ 2.138
            assert!((v - 2.138).abs() < 0.01, "expected ~2.138, got {}", v);
        } else {
            panic!("Expected double, got {:?}", result);
        }
    }

    #[test]
    fn test_eval_stdevp() {
        let expr = Expression::Function {
            distinct: false,
            name: "stDevP".into(),
            arguments: vec![Expression::List(vec![
                Expression::Int(2),
                Expression::Int(4),
                Expression::Int(4),
                Expression::Int(4),
                Expression::Int(5),
                Expression::Int(5),
                Expression::Int(7),
                Expression::Int(9),
            ])],
        };
        let result = eval_expression(&expr, &HashMap::new());
        if let PropertyValue::Double(v) = result {
            assert!((v - 2.0).abs() < 0.2, "expected ~2.0, got {}", v);
        } else {
            panic!("Expected double, got {:?}", result);
        }
    }

    // Safe conversion functions
    #[test]
    fn test_eval_to_float_or_null() {
        let expr = Expression::Function {
            distinct: false,
            name: "toFloatOrNull".into(),
            arguments: vec![Expression::String("3.14".into())],
        };
        assert_eq!(
            eval_expression(&expr, &HashMap::new()),
            PropertyValue::Double(3.14)
        );

        let expr_bad = Expression::Function {
            distinct: false,
            name: "toFloatOrNull".into(),
            arguments: vec![Expression::String("not_a_number".into())],
        };
        assert_eq!(
            eval_expression(&expr_bad, &HashMap::new()),
            PropertyValue::Null
        );
    }

    #[test]
    fn test_eval_to_integer_or_null() {
        let expr = Expression::Function {
            distinct: false,
            name: "toIntegerOrNull".into(),
            arguments: vec![Expression::String("42".into())],
        };
        assert_eq!(
            eval_expression(&expr, &HashMap::new()),
            PropertyValue::Int(42)
        );

        let expr_bad = Expression::Function {
            distinct: false,
            name: "toIntegerOrNull".into(),
            arguments: vec![Expression::String("not_an_int".into())],
        };
        assert_eq!(
            eval_expression(&expr_bad, &HashMap::new()),
            PropertyValue::Null
        );
    }

    #[test]
    fn test_eval_to_boolean_or_null() {
        let expr = Expression::Function {
            distinct: false,
            name: "toBooleanOrNull".into(),
            arguments: vec![Expression::String("true".into())],
        };
        assert_eq!(
            eval_expression(&expr, &HashMap::new()),
            PropertyValue::Bool(true)
        );

        let expr_bad = Expression::Function {
            distinct: false,
            name: "toBooleanOrNull".into(),
            arguments: vec![Expression::String("maybe".into())],
        };
        assert_eq!(
            eval_expression(&expr_bad, &HashMap::new()),
            PropertyValue::Null
        );
    }
}
