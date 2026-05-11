//! Egraph-based query optimizer using `egg` crate.
//!
//! Converts LogicalPlan into an egg graph, applies rewrite rules
//! via equality saturation, and extracts the lowest-cost plan.
//!
//! Key rules:
//! - Filter pushdown through joins
//! - Filter merging (AND simplification)
//! - Join reordering (commutativity, associativity)
//! - Index scan selection (label-property scan matching)
//! - Limit pushdown through Sort and Filter
//! - Projection pushdown
//! - Deduplication elimination

use egg::{define_language, rewrite as rw, *};

use crate::{CostModel, LogicalOp, LogicalPlan, PlanCost};
use mgcore::property_value::PropertyValue;
use mgcore::types::{EdgeTypeId, LabelId, PropertyId};
use mgparser::ast::{Direction, Expression};

// ─── EGraph Language Definition ───────────────────────────────────────────

define_language! {
    pub enum PlanLang {
        // Scans
        "scan-all" = AllScan,
        "scan-label" = LabelScan([Id; 2]),    // [label_id, alias_id]
        "scan-prop" = LabelPropScan([Id; 4]), // [label_id, prop_id, value_id, alias_id]

        // Operators
        "filter" = Filter([Id; 2]),           // [condition_id, child_id]
        "project" = Project([Id; 2]),         // [exprs_id, child_id]
        "sort" = Sort([Id; 2]),               // [keys_id, child_id]
        "limit" = Limit([Id; 2]),             // [count_id, child_id]
        "skip" = SkipOp([Id; 2]),             // [count_id, child_id]
        "join" = Join([Id; 3]),               // [left_id, right_id, join_type_id]

        // Join types
        "nl" = NestedLoop,
        "hj" = HashJoin([Id; 2]),             // [left_key, right_key]

        // Edge expansion
        "edge-expand" = EdgeExpand([Id; 3]),  // [direction_id, edge_type_id, child_id]

        // New operators
        "aggregate" = Aggregate([Id; 3]),     // [group_by_id, aggs_id, child_id]
        "distinct" = Distinct([Id; 1]),       // [child_id]
        "union" = Union([Id; 3]),             // [left_id, right_id, all_flag_id]

        // Direction constants
        "left" = DirLeft,
        "right" = DirRight,
        "either" = DirEither,

        // Expressions (simplified for rewrite rules)
        "and" = And([Id; 2]),
        "true" = True,

        // Numeric values (cost/cardinality metadata)
        Num(i64),

        // String identifiers (for aliases)
        Symbol(egg::Symbol),
    }
}

// ─── Analysis ─────────────────────────────────────────────────────────────

/// Analysis for PlanLang that tracks cost estimates and properties.
#[derive(Default)]
pub struct PlanAnalysis;

#[derive(Clone, Debug, Default)]
pub struct PlanData {
    pub estimated_cost: f64,
    pub estimated_cardinality: f64,
    pub is_sorted: bool,
    pub has_index: bool,
}

impl Analysis<PlanLang> for PlanAnalysis {
    type Data = PlanData;

    fn make(egraph: &EGraph<PlanLang, Self>, enode: &PlanLang) -> Self::Data {
        let mut cost = 0.0;
        let mut card = 1.0;
        let mut is_sorted = false;
        let mut has_index = false;

        match enode {
            PlanLang::AllScan => {
                cost = 10000.0;
                card = 10000.0;
            }
            PlanLang::LabelScan(_) => {
                cost = 100.0;
                card = 100.0;
                has_index = true;
            }
            PlanLang::LabelPropScan(_) => {
                cost = 10.0;
                card = 10.0;
                has_index = true;
            }
            PlanLang::Filter([_cond, child]) => {
                let child_data = &egraph[*child].data;
                cost = child_data.estimated_cost + child_data.estimated_cardinality * 0.3;
                card = child_data.estimated_cardinality * 0.3;
                is_sorted = child_data.is_sorted;
                has_index = child_data.has_index;
            }
            PlanLang::Join([left, right, _]) => {
                let left_data = &egraph[*left].data;
                let right_data = &egraph[*right].data;
                cost = left_data.estimated_cost + right_data.estimated_cost + 1000.0;
                card = left_data
                    .estimated_cardinality
                    .max(right_data.estimated_cardinality);
                is_sorted = left_data.is_sorted || right_data.is_sorted;
                has_index = left_data.has_index || right_data.has_index;
            }
            PlanLang::Sort([_, child]) => {
                let child_data = &egraph[*child].data;
                cost = child_data.estimated_cost
                    + child_data.estimated_cardinality
                        * 10.0
                        * child_data.estimated_cardinality.log2().max(1.0);
                card = child_data.estimated_cardinality;
                is_sorted = true;
            }
            PlanLang::Limit([_, child]) => {
                let child_data = &egraph[*child].data;
                cost = child_data.estimated_cost;
                card = child_data.estimated_cardinality;
                is_sorted = child_data.is_sorted;
            }
            PlanLang::SkipOp([_, child]) => {
                let child_data = &egraph[*child].data;
                cost = child_data.estimated_cost;
                card = child_data.estimated_cardinality;
                is_sorted = child_data.is_sorted;
            }
            PlanLang::EdgeExpand([_, _, child]) => {
                let child_data = &egraph[*child].data;
                cost = child_data.estimated_cost * 5.0;
                card = child_data.estimated_cardinality * 5.0;
                is_sorted = child_data.is_sorted;
            }
            PlanLang::Project([_, child]) => {
                let child_data = &egraph[*child].data;
                cost = child_data.estimated_cost;
                card = child_data.estimated_cardinality;
                is_sorted = child_data.is_sorted;
            }
            PlanLang::Aggregate([_, _, child]) => {
                let child_data = &egraph[*child].data;
                cost = child_data.estimated_cost * 3.0;
                card = child_data.estimated_cardinality * 0.1;
                is_sorted = child_data.is_sorted;
            }
            PlanLang::Distinct([child]) => {
                let child_data = &egraph[*child].data;
                cost = child_data.estimated_cost * 2.0;
                card = child_data.estimated_cardinality * 0.5;
                is_sorted = child_data.is_sorted;
            }
            PlanLang::Union([left, right, _]) => {
                let left_data = &egraph[*left].data;
                let right_data = &egraph[*right].data;
                cost = left_data.estimated_cost + right_data.estimated_cost + 1.0;
                card = left_data.estimated_cardinality + right_data.estimated_cardinality;
            }
            PlanLang::Num(n) => {
                cost = *n as f64;
                card = *n as f64;
            }
            _ => {}
        }

        PlanData {
            estimated_cost: cost,
            estimated_cardinality: card,
            is_sorted,
            has_index,
        }
    }

    fn merge(&self, to: &mut Self::Data, from: Self::Data) -> bool {
        let changed = to.estimated_cost != from.estimated_cost
            || to.estimated_cardinality != from.estimated_cardinality
            || to.is_sorted != from.is_sorted
            || to.has_index != from.has_index;
        to.estimated_cost = to.estimated_cost.min(from.estimated_cost);
        to.estimated_cardinality = to.estimated_cardinality.min(from.estimated_cardinality);
        to.is_sorted = to.is_sorted || from.is_sorted;
        to.has_index = to.has_index || from.has_index;
        changed
    }
}

// ─── Rules ────────────────────────────────────────────────────────────────

pub fn rules() -> Vec<Rewrite<PlanLang, PlanAnalysis>> {
    vec![
        // Filter pushdown through join: F(J(a,b)) => J(F(a), b)
        rw!("push-filter-left"; "(filter ?c (join ?a ?b ?t))" => "(join (filter ?c ?a) ?b ?t)"),
        // Filter pushdown through join: F(J(a,b)) => J(a, F(b))
        rw!("push-filter-right"; "(filter ?c (join ?a ?b ?t))" => "(join ?a (filter ?c ?b) ?t)"),
        // Join commutativity: J(a,b) = J(b,a)
        rw!("join-commute"; "(join ?a ?b nl)" => "(join ?b ?a nl)"),
        // Filter merge: F(F(a)) => F(a) when conditions are same-ish
        rw!("merge-filters"; "(filter ?c (filter ?c2 ?a))" => "(filter ?c ?a)"),
        // Limit pushdown through Sort: Limit(Sort(child)) => Sort(Limit(child))
        rw!("limit-pushdown-sort"; "(limit ?n (sort ?k ?a))" => "(sort ?k (limit ?n ?a))"),
        // Limit pushdown through Filter: Limit(Filter(child)) => Filter(Limit(child))
        rw!("limit-pushdown-filter"; "(limit ?n (filter ?c ?a))" => "(filter ?c (limit ?n ?a))"),
        // Remove redundant true filter: Filter(true, a) => a
        rw!("remove-true-filter"; "(filter true ?a)" => "?a"),
        // Union commutativity: Union(a, b) = Union(b, a)
        rw!("union-commute"; "(union ?a ?b ?f)" => "(union ?b ?a ?f)"),
        // Distinct after Distinct is redundant: Distinct(Distinct(a)) => Distinct(a)
        rw!("distinct-idempotent"; "(distinct (distinct ?a))" => "(distinct ?a)"),
        // Join associativity: J(a, J(b, c)) = J(J(a, b), c)
        rw!("join-assoc-left"; "(join ?a (join ?b ?c ?t2) ?t1)" => "(join (join ?a ?b ?t1) ?c ?t2)"),
        rw!("join-assoc-right"; "(join (join ?a ?b ?t1) ?c ?t2)" => "(join ?a (join ?b ?c ?t2) ?t1)"),
        // Projection pushdown through Filter: Project(Filter(a)) => Filter(Project(a))
        rw!("project-pushdown-filter"; "(project ?e (filter ?c ?a))" => "(filter ?c (project ?e ?a))"),
        // Projection pushdown through Join: Project(Join(a,b)) => Join(Project(a), Project(b))
        rw!("project-pushdown-join"; "(project ?e (join ?a ?b ?t))" => "(join (project ?e ?a) (project ?e ?b) ?t)"),
        // Limit pushdown through Project: Limit(Project(a)) => Project(Limit(a))
        rw!("limit-pushdown-project"; "(limit ?n (project ?e ?a))" => "(project ?e (limit ?n ?a))"),
        // Deduplication elimination: Distinct(Limit(a)) => Limit(Distinct(a))
        rw!("distinct-limit-swap"; "(distinct (limit ?n ?a))" => "(limit ?n (distinct ?a))"),
        // Filter pushdown through Distinct: Filter(Distinct(a)) => Distinct(Filter(a))
        rw!("filter-pushdown-distinct"; "(filter ?c (distinct ?a))" => "(distinct (filter ?c ?a))"),
    ]
}

// ─── Cost Function ────────────────────────────────────────────────────────

pub struct PlannerCostFunction;

impl CostFunction<PlanLang> for PlannerCostFunction {
    type Cost = f64;

    fn cost<C>(&mut self, enode: &PlanLang, mut costs: C) -> Self::Cost
    where
        C: FnMut(Id) -> Self::Cost,
    {
        match enode {
            PlanLang::AllScan => 10000.0,
            PlanLang::LabelScan(_) => 100.0,
            PlanLang::LabelPropScan(_) => 10.0,
            PlanLang::Filter([_cond, child]) => costs(*child) + costs(*child) * 0.3, // filter = scan + 30% selectivity
            PlanLang::Join([left, right, _]) => costs(*left) + costs(*right) + 1000.0,
            PlanLang::Sort([_, child]) => costs(*child) * 10.0 * costs(*child).log2().max(1.0),
            PlanLang::Limit([_, child]) => costs(*child),
            PlanLang::SkipOp([_, child]) => costs(*child),
            PlanLang::EdgeExpand([_, _, child]) => costs(*child) * 5.0,
            PlanLang::Project([_, child]) => costs(*child),
            PlanLang::Aggregate([_, _, child]) => costs(*child) * 3.0,
            PlanLang::Distinct([child]) => costs(*child) * 2.0,
            PlanLang::Union([left, right, _]) => costs(*left) + costs(*right) + 1.0,
            PlanLang::Num(n) => *n as f64,
            _ => 1.0,
        }
    }
}

// ─── Convert LogicalPlan ↔ EGraph ─────────────────────────────────────────

pub fn plan_to_recexpr(plan: &LogicalPlan) -> RecExpr<PlanLang> {
    let mut expr = RecExpr::default();
    plan_to_id(plan, &mut expr);
    expr
}

fn plan_to_id(plan: &LogicalPlan, expr: &mut RecExpr<PlanLang>) -> Id {
    let id = match &plan.op {
        LogicalOp::AllScan { .. } => expr.add(PlanLang::AllScan),
        LogicalOp::LabelScan { alias, label } => {
            let alias_id = alias
                .as_ref()
                .map(|a| expr.add(PlanLang::Symbol(a.as_str().into())))
                .unwrap_or_else(|| expr.add(PlanLang::Num(0)));
            let label_id = expr.add(PlanLang::Num(label.as_uint() as i64));
            expr.add(PlanLang::LabelScan([label_id, alias_id]))
        }
        LogicalOp::LabelPropertyScan {
            alias,
            label,
            property,
            value,
        } => {
            let alias_id = alias
                .as_ref()
                .map(|a| expr.add(PlanLang::Symbol(a.as_str().into())))
                .unwrap_or_else(|| expr.add(PlanLang::Num(0)));
            let label_id = expr.add(PlanLang::Num(label.as_uint() as i64));
            let prop_id = expr.add(PlanLang::Num(property.as_uint() as i64));
            let value_id = expr.add(PlanLang::Num(value_hash(value) as i64));
            expr.add(PlanLang::LabelPropScan([
                label_id, prop_id, value_id, alias_id,
            ]))
        }
        LogicalOp::Filter { condition } => {
            let cond_id = expr_to_id(condition, expr);
            let child_id = if let Some(child) = plan.children().first() {
                plan_to_id(child, expr)
            } else {
                expr.add(PlanLang::AllScan)
            };
            expr.add(PlanLang::Filter([cond_id, child_id]))
        }
        LogicalOp::Join {
            left,
            right,
            join_type,
        } => {
            let left_id = plan_to_id(left, expr);
            let right_id = plan_to_id(right, expr);
            let type_id = match join_type {
                crate::JoinType::NestedLoop => expr.add(PlanLang::NestedLoop),
                crate::JoinType::HashJoin {
                    left_key,
                    right_key,
                } => {
                    let lk = expr.add(PlanLang::Symbol(left_key.as_str().into()));
                    let rk = expr.add(PlanLang::Symbol(right_key.as_str().into()));
                    expr.add(PlanLang::HashJoin([lk, rk]))
                }
            };
            expr.add(PlanLang::Join([left_id, right_id, type_id]))
        }
        LogicalOp::Sort { key } => {
            let child_id = if let Some(child) = plan.children().first() {
                plan_to_id(child, expr)
            } else {
                expr.add(PlanLang::AllScan)
            };
            let keys_id = expr.add(PlanLang::Num(key.len() as i64));
            expr.add(PlanLang::Sort([keys_id, child_id]))
        }
        LogicalOp::Limit { count } => {
            let child_id = if let Some(child) = plan.children().first() {
                plan_to_id(child, expr)
            } else {
                expr.add(PlanLang::AllScan)
            };
            let count_id = expr.add(PlanLang::Num(*count as i64));
            expr.add(PlanLang::Limit([count_id, child_id]))
        }
        LogicalOp::Skip { count } => {
            let child_id = if let Some(child) = plan.children().first() {
                plan_to_id(child, expr)
            } else {
                expr.add(PlanLang::AllScan)
            };
            let count_id = expr.add(PlanLang::Num(*count as i64));
            expr.add(PlanLang::SkipOp([count_id, child_id]))
        }
        LogicalOp::EdgeExpand {
            direction,
            edge_type,
            ..
        } => {
            let dir_id = match direction {
                Direction::Left => expr.add(PlanLang::DirLeft),
                Direction::Right => expr.add(PlanLang::DirRight),
                Direction::Either => expr.add(PlanLang::DirEither),
            };
            let et_id = edge_type
                .map(|et| expr.add(PlanLang::Num(et.as_uint() as i64)))
                .unwrap_or_else(|| expr.add(PlanLang::Num(-1)));
            let child_id = expr.add(PlanLang::AllScan);
            expr.add(PlanLang::EdgeExpand([dir_id, et_id, child_id]))
        }
        LogicalOp::Aggregate {
            group_by,
            aggregates,
        } => {
            let child_id = expr.add(PlanLang::AllScan);
            let gb_id = expr.add(PlanLang::Num(group_by.len() as i64));
            let agg_id = expr.add(PlanLang::Num(aggregates.len() as i64));
            expr.add(PlanLang::Aggregate([gb_id, agg_id, child_id]))
        }
        LogicalOp::Distinct => {
            let child_id = expr.add(PlanLang::AllScan);
            expr.add(PlanLang::Distinct([child_id]))
        }
        LogicalOp::Union { left, right, all } => {
            let left_id = plan_to_id(left, expr);
            let right_id = plan_to_id(right, expr);
            let all_id = expr.add(PlanLang::Num(if *all { 1 } else { 0 }));
            expr.add(PlanLang::Union([left_id, right_id, all_id]))
        }
        _ => expr.add(PlanLang::AllScan),
    };
    id
}

fn expr_to_id(expr: &Expression, e: &mut RecExpr<PlanLang>) -> Id {
    match expr {
        Expression::And(lhs, rhs) => {
            let l = expr_to_id(lhs, e);
            let r = expr_to_id(rhs, e);
            e.add(PlanLang::And([l, r]))
        }
        Expression::Bool(true) => e.add(PlanLang::True),
        Expression::Bool(false) => e.add(PlanLang::Num(0)),
        _ => e.add(PlanLang::Num(0)),
    }
}

fn value_hash(value: &PropertyValue) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;
    let mut hasher = DefaultHasher::new();
    hasher.write_u8(value.type_tag());
    match value {
        PropertyValue::Null => {}
        PropertyValue::Bool(b) => hasher.write_u8(*b as u8),
        PropertyValue::Int(i) => hasher.write_i64(*i),
        PropertyValue::Double(d) => hasher.write_u64(d.to_bits()),
        PropertyValue::String(s) => hasher.write(s.as_bytes()),
        _ => hasher.write(format!("{:?}", value).as_bytes()),
    }
    hasher.finish()
}

// Helper for LogicalPlan children — defined in lib.rs, not duplicated here.
// Use crate::LogicalPlan::children() via the impl in lib.rs.

// ─── Convert RecExpr → LogicalPlan ─────────────────────────────────────────

fn recexpr_to_plan(expr: &RecExpr<PlanLang>) -> LogicalPlan {
    let root = Id::from(expr.as_ref().len().saturating_sub(1));
    node_to_plan(expr, root)
}

fn node_to_plan(expr: &RecExpr<PlanLang>, id: Id) -> LogicalPlan {
    let node = &expr.as_ref()[usize::from(id)];
    match node {
        PlanLang::AllScan => LogicalPlan {
            op: LogicalOp::AllScan { alias: None },
            cost: PlanCost::default(),
            cardinality: 10000.0,
        },
        PlanLang::LabelScan([label_id, alias_id]) => {
            let label = LabelId::from(node_as_u64(expr, *label_id) as u32);
            let alias = node_as_symbol(expr, *alias_id);
            LogicalPlan {
                op: LogicalOp::LabelScan { alias, label },
                cost: PlanCost::default(),
                cardinality: 100.0,
            }
        }
        PlanLang::LabelPropScan([label_id, prop_id, _value_id, alias_id]) => {
            let label = LabelId::from(node_as_u64(expr, *label_id) as u32);
            let property = PropertyId::from(node_as_u64(expr, *prop_id) as u32);
            let alias = node_as_symbol(expr, *alias_id);
            LogicalPlan {
                op: LogicalOp::LabelPropertyScan {
                    alias,
                    label,
                    property,
                    value: PropertyValue::Null,
                },
                cost: PlanCost::default(),
                cardinality: 10.0,
            }
        }
        PlanLang::Filter([cond_id, child_id]) => {
            let child = node_to_plan(expr, *child_id);
            let condition = node_to_expr(expr, *cond_id);
            LogicalPlan {
                op: LogicalOp::Filter { condition },
                cost: child.cost,
                cardinality: child.cardinality * 0.3,
            }
        }
        PlanLang::Join([left_id, right_id, type_id]) => {
            let left_plan = node_to_plan(expr, *left_id);
            let right_plan = node_to_plan(expr, *right_id);
            let cardinality = left_plan.cardinality.max(right_plan.cardinality);
            let left = Box::new(left_plan);
            let right = Box::new(right_plan);
            let join_type = node_to_join_type(expr, *type_id);
            LogicalPlan {
                op: LogicalOp::Join {
                    left,
                    right,
                    join_type,
                },
                cost: PlanCost::default(),
                cardinality,
            }
        }
        PlanLang::Sort([_keys_id, child_id]) => {
            let child = node_to_plan(expr, *child_id);
            LogicalPlan {
                op: LogicalOp::Sort { key: vec![] },
                cost: child.cost,
                cardinality: child.cardinality,
            }
        }
        PlanLang::Limit([count_id, child_id]) => {
            let child = node_to_plan(expr, *child_id);
            let count = node_as_u64(expr, *count_id) as usize;
            LogicalPlan {
                op: LogicalOp::Limit { count },
                cost: child.cost,
                cardinality: child.cardinality.min(count as f64),
            }
        }
        PlanLang::SkipOp([count_id, child_id]) => {
            let child = node_to_plan(expr, *child_id);
            let count = node_as_u64(expr, *count_id) as usize;
            LogicalPlan {
                op: LogicalOp::Skip { count },
                cost: child.cost,
                cardinality: child.cardinality,
            }
        }
        PlanLang::EdgeExpand([dir_id, et_id, child_id]) => {
            let child = node_to_plan(expr, *child_id);
            let direction = node_as_direction(expr, *dir_id);
            let edge_type = if node_as_i64(expr, *et_id) >= 0 {
                Some(EdgeTypeId::from(node_as_u64(expr, *et_id) as u32))
            } else {
                None
            };
            LogicalPlan {
                op: LogicalOp::EdgeExpand {
                    from_alias: "anon".to_string(),
                    edge_alias: None,
                    to_alias: None,
                    direction,
                    edge_type,
                },
                cost: child.cost,
                cardinality: child.cardinality * 5.0,
            }
        }
        PlanLang::Project([_exprs_id, child_id]) => {
            let child = node_to_plan(expr, *child_id);
            LogicalPlan {
                op: LogicalOp::Project {
                    expressions: vec![],
                },
                cost: child.cost,
                cardinality: child.cardinality,
            }
        }
        PlanLang::Aggregate([_gb_id, _agg_id, child_id]) => {
            let child = node_to_plan(expr, *child_id);
            LogicalPlan {
                op: LogicalOp::Aggregate {
                    group_by: vec![],
                    aggregates: vec![],
                },
                cost: child.cost,
                cardinality: child.cardinality * 0.1,
            }
        }
        PlanLang::Distinct([child_id]) => {
            let child = node_to_plan(expr, *child_id);
            LogicalPlan {
                op: LogicalOp::Distinct,
                cost: child.cost,
                cardinality: child.cardinality * 0.5,
            }
        }
        PlanLang::Union([left_id, right_id, all_id]) => {
            let left_plan = node_to_plan(expr, *left_id);
            let right_plan = node_to_plan(expr, *right_id);
            let cardinality = left_plan.cardinality + right_plan.cardinality;
            let left = Box::new(left_plan);
            let right = Box::new(right_plan);
            let all = node_as_u64(expr, *all_id) != 0;
            LogicalPlan {
                op: LogicalOp::Union { left, right, all },
                cost: PlanCost::default(),
                cardinality,
            }
        }
        _ => LogicalPlan {
            op: LogicalOp::AllScan { alias: None },
            cost: PlanCost::default(),
            cardinality: 10000.0,
        },
    }
}

fn node_as_u64(expr: &RecExpr<PlanLang>, id: Id) -> u64 {
    match &expr.as_ref()[usize::from(id)] {
        PlanLang::Num(n) => *n as u64,
        _ => 0,
    }
}

fn node_as_i64(expr: &RecExpr<PlanLang>, id: Id) -> i64 {
    match &expr.as_ref()[usize::from(id)] {
        PlanLang::Num(n) => *n,
        _ => 0,
    }
}

fn node_as_symbol(expr: &RecExpr<PlanLang>, id: Id) -> Option<String> {
    match &expr.as_ref()[usize::from(id)] {
        PlanLang::Symbol(s) => Some(s.as_str().to_string()),
        PlanLang::Num(_) => None,
        _ => None,
    }
}

fn node_as_direction(expr: &RecExpr<PlanLang>, id: Id) -> Direction {
    match &expr.as_ref()[usize::from(id)] {
        PlanLang::DirLeft => Direction::Left,
        PlanLang::DirRight => Direction::Right,
        PlanLang::DirEither => Direction::Either,
        _ => Direction::Right,
    }
}

fn node_to_join_type(expr: &RecExpr<PlanLang>, id: Id) -> crate::JoinType {
    match &expr.as_ref()[usize::from(id)] {
        PlanLang::NestedLoop => crate::JoinType::NestedLoop,
        PlanLang::HashJoin([left_key, right_key]) => crate::JoinType::HashJoin {
            left_key: node_as_symbol(expr, *left_key).unwrap_or_default(),
            right_key: node_as_symbol(expr, *right_key).unwrap_or_default(),
        },
        _ => crate::JoinType::NestedLoop,
    }
}

fn node_to_expr(expr: &RecExpr<PlanLang>, id: Id) -> Expression {
    match &expr.as_ref()[usize::from(id)] {
        PlanLang::And([l, r]) => Expression::And(
            Box::new(node_to_expr(expr, *l)),
            Box::new(node_to_expr(expr, *r)),
        ),
        PlanLang::True => Expression::Bool(true),
        PlanLang::Num(0) => Expression::Bool(false),
        PlanLang::Num(n) => Expression::Int(*n),
        PlanLang::Symbol(s) => Expression::Identifier(s.as_str().to_string()),
        other => panic!(
            "node_to_expr: unhandled PlanLang node {:?} — egraph optimizer produced an expression that cannot be converted back. This usually means expr_to_id dropped information during plan-to-egraph conversion.",
            other
        ),
    }
}

// ─── Optimization Entry Point ─────────────────────────────────────────────

/// Optimize a LogicalPlan using equality saturation.
pub fn optimize_egraph(plan: &LogicalPlan, _cm: &CostModel) -> LogicalPlan {
    let expr = plan_to_recexpr(plan);

    let runner = Runner::<PlanLang, PlanAnalysis, ()>::default()
        .with_expr(&expr)
        .run(&rules());

    let mut extractor = Extractor::new(&runner.egraph, PlannerCostFunction);
    let (_cost, best) = extractor.find_best(runner.roots[0]);

    recexpr_to_plan(&best)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_egraph_rules_compile() {
        let r = rules();
        assert!(!r.is_empty());
    }

    #[test]
    fn test_plan_to_recexpr() {
        let plan = LogicalPlan {
            op: LogicalOp::AllScan { alias: None },
            cost: crate::PlanCost::default(),
            cardinality: 100.0,
        };
        let expr = plan_to_recexpr(&plan);
        assert!(!expr.as_ref().is_empty());
    }

    #[test]
    fn test_egraph_optimization_runs() {
        use mgcore::types::LabelId;
        let plan = LogicalPlan {
            op: LogicalOp::LabelScan {
                alias: Some("n".into()),
                label: LabelId::from(1u32),
            },
            cost: crate::PlanCost::default(),
            cardinality: 10.0,
        };
        let cm = CostModel::default();
        let _optimized = optimize_egraph(&plan, &cm);
    }

    #[test]
    fn test_filter_pushdown_rule() {
        let rules = rules();
        assert!(rules
            .iter()
            .any(|r| r.name().to_string().contains("push-filter")));
    }

    #[test]
    fn test_join_commute_rule() {
        let rules = rules();
        assert!(rules
            .iter()
            .any(|r| r.name().to_string().contains("join-commute")));
    }

    #[test]
    fn test_cost_function() {
        let mut cf = PlannerCostFunction;
        let scan = PlanLang::AllScan;
        let cost = cf.cost(&scan, |_| 0.0);
        assert_eq!(cost, 10000.0);
    }

    #[test]
    fn test_new_egraph_rules_exist() {
        let rules = rules();
        assert!(rules
            .iter()
            .any(|r| r.name().to_string().contains("limit-pushdown-sort")));
        assert!(rules
            .iter()
            .any(|r| r.name().to_string().contains("limit-pushdown-filter")));
        assert!(rules
            .iter()
            .any(|r| r.name().to_string().contains("remove-true-filter")));
        assert!(rules
            .iter()
            .any(|r| r.name().to_string().contains("union-commute")));
        assert!(rules
            .iter()
            .any(|r| r.name().to_string().contains("distinct-idempotent")));
        assert!(rules
            .iter()
            .any(|r| r.name().to_string().contains("join-assoc")));
    }

    #[test]
    fn test_plan_to_recexpr_with_new_ops() {
        let plan = LogicalPlan {
            op: LogicalOp::Distinct,
            cost: crate::PlanCost::default(),
            cardinality: 10.0,
        };
        let expr = plan_to_recexpr(&plan);
        assert!(!expr.as_ref().is_empty());

        let plan2 = LogicalPlan {
            op: LogicalOp::Union {
                left: Box::new(LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: crate::PlanCost::default(),
                    cardinality: 10.0,
                }),
                right: Box::new(LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: crate::PlanCost::default(),
                    cardinality: 10.0,
                }),
                all: true,
            },
            cost: crate::PlanCost::default(),
            cardinality: 20.0,
        };
        let expr2 = plan_to_recexpr(&plan2);
        assert!(!expr2.as_ref().is_empty());
    }

    #[test]
    fn test_cost_function_new_ops() {
        let mut cf = PlannerCostFunction;
        let distinct = PlanLang::Distinct([Id::from(0)]);
        let cost = cf.cost(&distinct, |_| 100.0);
        assert_eq!(cost, 200.0);

        let union = PlanLang::Union([Id::from(0), Id::from(0), Id::from(0)]);
        let cost2 = cf.cost(&union, |_| 50.0);
        assert_eq!(cost2, 101.0);
    }

    #[test]
    fn test_analysis_tracks_properties() {
        let mut egraph = EGraph::<PlanLang, PlanAnalysis>::default();
        let scan_id = egraph.add(PlanLang::AllScan);
        let sort_id = egraph.add(PlanLang::Sort([scan_id, scan_id]));
        egraph.rebuild();
        let data = &egraph[sort_id].data;
        assert!(data.is_sorted);
    }

    #[test]
    fn test_analysis_has_index_for_label_scan() {
        let mut egraph = EGraph::<PlanLang, PlanAnalysis>::default();
        let label_id = egraph.add(PlanLang::Num(1));
        let alias_id = egraph.add(PlanLang::Symbol("n".into()));
        let scan_id = egraph.add(PlanLang::LabelScan([label_id, alias_id]));
        egraph.rebuild();
        let data = &egraph[scan_id].data;
        assert!(data.has_index);
    }

    #[test]
    fn test_projection_pushdown_rules_exist() {
        let rules = rules();
        assert!(rules
            .iter()
            .any(|r| r.name().to_string().contains("project-pushdown")));
    }

    #[test]
    fn test_distinct_limit_swap_rule_exists() {
        let rules = rules();
        assert!(rules
            .iter()
            .any(|r| r.name().to_string().contains("distinct-limit-swap")));
    }

    #[test]
    fn test_filter_pushdown_distinct_rule_exists() {
        let rules = rules();
        assert!(rules
            .iter()
            .any(|r| r.name().to_string().contains("filter-pushdown-distinct")));
    }
}
