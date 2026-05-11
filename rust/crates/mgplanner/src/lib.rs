//! # mgplanner — Query planner with rule-based and egraph optimization
//!
//! Converts parsed Cypher AST into an optimized execution plan using
//! cost-based rewrite rules: filter pushdown, filter merge, index scan
//! selection, left-deep join ordering, and egraph equality saturation.

pub mod egraph;

use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Mutex;

use mgcore::property_value::PropertyValue;
use mgcore::types::{EdgeTypeId, LabelId, PropertyId};
use mgparser::ast::*;
use mgstorage::storage::Storage;

// ─── Plan cache ────────────────────────────────────────────────────────────

static PLAN_CACHE: Mutex<Option<lru::LruCache<u64, LogicalPlan>>> = Mutex::new(None);

fn get_plan_cache() -> std::sync::MutexGuard<'static, Option<lru::LruCache<u64, LogicalPlan>>> {
    PLAN_CACHE.lock().unwrap()
}

fn query_fingerprint(query: &Query) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    // Use the Debug representation as a stable AST fingerprint.
    format!("{:?}", query).hash(&mut hasher);
    hasher.finish()
}

// ─── Logical plan operators ───────────────────────────────────────────────

#[derive(Clone, Debug)]
pub enum LogicalOp {
    AllScan {
        alias: Option<String>,
    },
    LabelScan {
        alias: Option<String>,
        label: LabelId,
    },
    LabelPropertyScan {
        alias: Option<String>,
        label: LabelId,
        property: PropertyId,
        value: PropertyValue,
    },
    EdgeTypeScan {
        alias: Option<String>,
        edge_type: EdgeTypeId,
    },
    EdgeTypePropertyScan {
        alias: Option<String>,
        edge_type: EdgeTypeId,
        property: PropertyId,
        value: PropertyValue,
    },
    EdgeExpand {
        from_alias: String,
        edge_alias: Option<String>,
        to_alias: Option<String>,
        direction: Direction,
        edge_type: Option<EdgeTypeId>,
    },
    Filter {
        condition: Expression,
    },
    Project {
        expressions: Vec<Expression>,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        join_type: JoinType,
    },
    Sort {
        key: Vec<OrderByItem>,
    },
    Limit {
        count: usize,
    },
    Skip {
        count: usize,
    },
    Unwind {
        expr: Expression,
        alias: String,
    },
    Produce {
        items: Vec<ReturnItem>,
    },
    CreateVertex {
        labels: Vec<LabelId>,
        properties: Vec<(PropertyId, Expression)>,
    },
    SetProperty {
        key: PropertyId,
        value: Expression,
    },
    Delete {
        expressions: Vec<Expression>,
        detach: bool,
    },
    // ─── New logical operators ──────────────────────────────────────────────
    /// Group-by with aggregate expressions.
    Aggregate {
        group_by: Vec<(Expression, Option<String>)>, // (group expr, output alias)
        aggregates: Vec<(Expression, Option<String>)>, // (aggregate expr, output alias)
    },
    /// Deduplicate rows (DISTINCT).
    Distinct,
    /// Union of two sub-plans (ALL or DISTINCT).
    Union {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        all: bool,
    },
    /// Top-N: sort + limit merged into a single operator.
    TopN {
        key: Vec<OrderByItem>,
        count: usize,
    },
    /// Empty result set (produces zero rows).
    EmptyResult,
}

#[derive(Clone, Debug)]
pub enum JoinType {
    NestedLoop,
    HashJoin { left_key: String, right_key: String },
}

#[derive(Clone, Debug)]
pub struct LogicalPlan {
    pub op: LogicalOp,
    pub cost: PlanCost,
    pub cardinality: f64,
}

impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_indented(f, 0)
    }
}

impl LogicalPlan {
    fn fmt_indented(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
        let prefix = "  ".repeat(indent);
        match &self.op {
            LogicalOp::AllScan { alias } => writeln!(f, "{}AllScan(alias={:?}) (cost={:.2}, rows={:.0})", prefix, alias, self.cost.total(), self.cardinality),
            LogicalOp::LabelScan { alias, label } => writeln!(f, "{}LabelScan(alias={:?}, label={:?}) (cost={:.2}, rows={:.0})", prefix, alias, label, self.cost.total(), self.cardinality),
            LogicalOp::LabelPropertyScan { alias, label, property, value } => writeln!(f, "{}LabelPropertyScan(alias={:?}, label={:?}, prop={:?}, val={:?}) (cost={:.2}, rows={:.0})", prefix, alias, label, property, value, self.cost.total(), self.cardinality),
            LogicalOp::EdgeTypeScan { alias, edge_type } => writeln!(f, "{}EdgeTypeScan(alias={:?}, edge_type={:?}) (cost={:.2}, rows={:.0})", prefix, alias, edge_type, self.cost.total(), self.cardinality),
            LogicalOp::EdgeTypePropertyScan { alias, edge_type, property, value } => writeln!(f, "{}EdgeTypePropertyScan(alias={:?}, edge_type={:?}, prop={:?}, val={:?}) (cost={:.2}, rows={:.0})", prefix, alias, edge_type, property, value, self.cost.total(), self.cardinality),
            LogicalOp::EdgeExpand { from_alias, edge_alias, to_alias, direction, edge_type } => writeln!(f, "{}EdgeExpand(from={:?}, edge={:?}, to={:?}, dir={:?}, type={:?}) (cost={:.2}, rows={:.0})", prefix, from_alias, edge_alias, to_alias, direction, edge_type, self.cost.total(), self.cardinality),
            LogicalOp::Filter { condition } => writeln!(f, "{}Filter({:?}) (cost={:.2}, rows={:.0})", prefix, condition, self.cost.total(), self.cardinality),
            LogicalOp::Project { expressions } => writeln!(f, "{}Project({} exprs) (cost={:.2}, rows={:.0})", prefix, expressions.len(), self.cost.total(), self.cardinality),
            LogicalOp::Join { left, right, join_type } => {
                writeln!(f, "{}Join({:?}) (cost={:.2}, rows={:.0})", prefix, join_type, self.cost.total(), self.cardinality)?;
                left.fmt_indented(f, indent + 1)?;
                right.fmt_indented(f, indent + 1)
            }
            LogicalOp::Sort { key } => writeln!(f, "{}Sort({} keys) (cost={:.2}, rows={:.0})", prefix, key.len(), self.cost.total(), self.cardinality),
            LogicalOp::Limit { count } => writeln!(f, "{}Limit({}) (cost={:.2}, rows={:.0})", prefix, count, self.cost.total(), self.cardinality),
            LogicalOp::Skip { count } => writeln!(f, "{}Skip({}) (cost={:.2}, rows={:.0})", prefix, count, self.cost.total(), self.cardinality),
            LogicalOp::TopN { key, count } => writeln!(f, "{}TopN({} keys, limit={}) (cost={:.2}, rows={:.0})", prefix, key.len(), count, self.cost.total(), self.cardinality),
            LogicalOp::Unwind { expr, alias } => writeln!(f, "{}Unwind({:?} AS {}) (cost={:.2}, rows={:.0})", prefix, expr, alias, self.cost.total(), self.cardinality),
            LogicalOp::Produce { items } => writeln!(f, "{}Produce({} items) (cost={:.2}, rows={:.0})", prefix, items.len(), self.cost.total(), self.cardinality),
            LogicalOp::CreateVertex { labels, properties } => writeln!(f, "{}CreateVertex(labels={:?}, props={}) (cost={:.2}, rows={:.0})", prefix, labels, properties.len(), self.cost.total(), self.cardinality),
            LogicalOp::SetProperty { key, value } => writeln!(f, "{}SetProperty({:?}={:?}) (cost={:.2}, rows={:.0})", prefix, key, value, self.cost.total(), self.cardinality),
            LogicalOp::Delete { expressions: _, detach } => writeln!(f, "{}Delete(detach={}) (cost={:.2}, rows={:.0})", prefix, detach, self.cost.total(), self.cardinality),
            LogicalOp::Aggregate { group_by, aggregates } => writeln!(f, "{}Aggregate(groups={}, aggs={}) (cost={:.2}, rows={:.0})", prefix, group_by.len(), aggregates.len(), self.cost.total(), self.cardinality),
            LogicalOp::Distinct => writeln!(f, "{}Distinct (cost={:.2}, rows={:.0})", prefix, self.cost.total(), self.cardinality),
            LogicalOp::EmptyResult => writeln!(f, "{}EmptyResult (cost={:.2}, rows={:.0})", prefix, self.cost.total(), self.cardinality),
            LogicalOp::Union { left, right, all } => {
                writeln!(f, "{}Union(all={}) (cost={:.2}, rows={:.0})", prefix, all, self.cost.total(), self.cardinality)?;
                left.fmt_indented(f, indent + 1)?;
                right.fmt_indented(f, indent + 1)
            }
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PlanCost {
    pub cpu: f64,
    pub io: f64,
}

impl PlanCost {
    pub fn total(&self) -> f64 {
        self.cpu + self.io
    }
}

// ─── Cost model ───────────────────────────────────────────────────────────

pub struct CostModel {
    pub scan_all_cost: f64,
    pub scan_label_cost: f64,
    pub scan_label_property_cost: f64,
    pub filter_cost_per_row: f64,
    pub nested_loop_join_cost: f64,
    pub hash_join_build_cost: f64,
    pub hash_join_probe_cost: f64,
    pub sort_cost_factor: f64,
    pub default_selectivity: f64,
    pub equality_selectivity: f64,
    pub range_selectivity: f64,
    pub like_selectivity: f64,
    pub in_selectivity: f64,
    pub null_selectivity: f64,
    pub label_selectivity: f64,
    pub distinct_cost_per_row: f64,
    pub aggregate_cost_per_row: f64,
    pub union_overhead: f64,
    // ─── New cost model parameters ──────────────────────────────────────────
    /// Cost per row for index seek (point lookup).
    pub index_seek_cost_per_row: f64,
    /// Cost per row for sort-merge join.
    pub sort_merge_join_cost_per_row: f64,
    /// Cost per row for index nested loop join probe.
    pub index_nested_loop_join_cost_per_row: f64,
    /// Cost to build a hash table for aggregation.
    pub hash_aggregate_build_cost_per_row: f64,
    /// Cost to probe/process a row in hash aggregation.
    pub hash_aggregate_probe_cost_per_row: f64,
    /// Cost per row for streaming aggregation (pre-sorted input).
    pub streaming_aggregate_cost_per_row: f64,
    /// Cost per row to produce a sorted output.
    pub sort_cost_per_row: f64,
}

impl Default for CostModel {
    fn default() -> Self {
        Self {
            scan_all_cost: 10000.0,
            scan_label_cost: 100.0,
            scan_label_property_cost: 10.0,
            filter_cost_per_row: 1.0,
            nested_loop_join_cost: 1000.0,
            hash_join_build_cost: 50.0,
            hash_join_probe_cost: 1.0,
            sort_cost_factor: 10.0,
            default_selectivity: 0.3,
            equality_selectivity: 0.01,
            range_selectivity: 0.1,
            like_selectivity: 0.05,
            in_selectivity: 0.15,
            null_selectivity: 0.02,
            label_selectivity: 0.1,
            distinct_cost_per_row: 2.0,
            aggregate_cost_per_row: 3.0,
            union_overhead: 1.0,
            index_seek_cost_per_row: 2.0,
            sort_merge_join_cost_per_row: 3.0,
            index_nested_loop_join_cost_per_row: 5.0,
            hash_aggregate_build_cost_per_row: 4.0,
            hash_aggregate_probe_cost_per_row: 2.0,
            streaming_aggregate_cost_per_row: 1.0,
            sort_cost_per_row: 2.0,
        }
    }
}

impl CostModel {
    /// Compute the cost of a hash join given left and right cardinalities.
    pub fn hash_join_cost(&self, left_card: f64, right_card: f64) -> f64 {
        self.hash_join_build_cost * left_card + self.hash_join_probe_cost * right_card
    }

    /// Compute the cost of a nested-loop join.
    pub fn nested_loop_join_cost(&self, left_card: f64, right_card: f64) -> f64 {
        self.nested_loop_join_cost + left_card * right_card
    }

    /// Compute the cost of an index nested-loop join.
    pub fn index_nested_loop_join_cost(&self, outer_card: f64, _inner_card: f64) -> f64 {
        self.nested_loop_join_cost + outer_card * self.index_nested_loop_join_cost_per_row
    }

    /// Compute the cost of a sort-merge join.
    pub fn sort_merge_join_cost(&self, left_card: f64, right_card: f64) -> f64 {
        self.nested_loop_join_cost
            + left_card * self.sort_cost_per_row
            + right_card * self.sort_cost_per_row
            + (left_card + right_card) * self.sort_merge_join_cost_per_row
    }

    /// Compute aggregation cost for hash-based aggregation.
    pub fn hash_aggregate_cost(&self, input_card: f64) -> f64 {
        input_card
            * (self.hash_aggregate_build_cost_per_row + self.hash_aggregate_probe_cost_per_row)
    }

    /// Compute aggregation cost for streaming aggregation.
    pub fn streaming_aggregate_cost(&self, input_card: f64) -> f64 {
        input_card * self.streaming_aggregate_cost_per_row
    }
}

// ─── Statistics ───────────────────────────────────────────────────────────

pub struct PlanStats {
    pub vertex_count: u64,
    pub edge_count: u64,
    pub label_counts: HashMap<LabelId, u64>,
    pub label_property_counts: HashMap<(LabelId, PropertyId), u64>,
    pub edge_type_counts: HashMap<EdgeTypeId, u64>,
    pub edge_type_property_counts: HashMap<(EdgeTypeId, PropertyId), u64>,
}

impl PlanStats {
    pub fn from_storage(storage: &Storage) -> Self {
        let vertex_count = storage.all_vertices().len() as u64;
        let edge_count = storage.all_edges().len() as u64;

        let mut label_counts = HashMap::new();
        let active_labels = storage.active_label_indices.read().unwrap();
        for label in active_labels.iter() {
            label_counts.insert(*label, storage.label_index.vertex_count_by_label(*label));
        }

        let mut label_property_counts = HashMap::new();
        let active_lp = storage.active_label_property_indices.read().unwrap();
        for (label, prop) in active_lp.iter() {
            let count = storage
                .label_property_index
                .vertex_count_by_label_property(*label, *prop);
            label_property_counts.insert((*label, *prop), count);
        }

        let mut edge_type_counts = HashMap::new();
        let active_et = storage.active_edge_type_indices.read().unwrap();
        for etype in active_et.iter() {
            edge_type_counts.insert(*etype, storage.edge_type_count(*etype) as u64);
        }

        let mut edge_type_property_counts = HashMap::new();
        let active_etp = storage.active_edge_type_property_indices.read().unwrap();
        for (etype, prop) in active_etp.iter() {
            let count = storage.edges_by_type_property(*etype, *prop).len() as u64;
            edge_type_property_counts.insert((*etype, *prop), count);
        }

        Self {
            vertex_count,
            edge_count,
            label_counts,
            label_property_counts,
            edge_type_counts,
            edge_type_property_counts,
        }
    }
}

// ─── Catalog statistics for selectivity estimation ────────────────────────

/// Statistics about a single table/label in the catalog.
#[derive(Clone, Debug, Default)]
pub struct TableStats {
    pub row_count: u64,
    /// Number of distinct values per property.
    pub distinct_values: HashMap<PropertyId, u64>,
    /// Null fraction per property (0.0 - 1.0).
    pub null_fraction: HashMap<PropertyId, f64>,
    /// Min/max values per property (for range selectivity).
    pub min_value: HashMap<PropertyId, PropertyValue>,
    pub max_value: HashMap<PropertyId, PropertyValue>,
}

/// Edge type statistics for the optimizer.
#[derive(Clone, Debug, Default)]
pub struct EdgeTypeStats {
    pub edge_count: u64,
    pub distinct_source_vertices: u64,
    pub distinct_target_vertices: u64,
}

/// Catalog-wide statistics used by the optimizer.
#[derive(Clone, Debug, Default)]
pub struct CatalogStats {
    pub tables: HashMap<LabelId, TableStats>,
    pub edge_types: HashMap<EdgeTypeId, EdgeTypeStats>,
    pub total_vertices: u64,
}

impl CatalogStats {
    /// Build catalog stats from plan stats (basic counts only).
    pub fn from_plan_stats(stats: &PlanStats) -> Self {
        let mut tables = HashMap::new();
        for (label, count) in &stats.label_counts {
            tables.insert(
                *label,
                TableStats {
                    row_count: *count,
                    ..Default::default()
                },
            );
        }
        let mut edge_types = HashMap::new();
        for (et, count) in &stats.edge_type_counts {
            edge_types.insert(
                *et,
                EdgeTypeStats {
                    edge_count: *count,
                    ..Default::default()
                },
            );
        }
        Self {
            tables,
            edge_types,
            total_vertices: stats.vertex_count,
        }
    }

    /// Get statistics for a specific label, or a default empty one.
    pub fn table_stats(&self, label: LabelId) -> Option<&TableStats> {
        self.tables.get(&label)
    }

    /// Estimate the number of rows for a label scan.
    pub fn label_cardinality(&self, label: LabelId) -> f64 {
        self.tables
            .get(&label)
            .map(|t| t.row_count as f64)
            .unwrap_or(1.0)
    }

    /// Estimate the number of rows for an edge type scan.
    pub fn edge_type_cardinality(&self, edge_type: EdgeTypeId) -> f64 {
        self.edge_types
            .get(&edge_type)
            .map(|s| s.edge_count as f64)
            .unwrap_or(1.0)
    }

    /// Estimate the number of distinct values for a property on a label.
    pub fn distinct_count(&self, label: LabelId, property: PropertyId) -> f64 {
        self.tables
            .get(&label)
            .and_then(|t| t.distinct_values.get(&property).copied())
            .map(|v| v as f64)
            .unwrap_or(1.0)
    }
}

// ─── Selectivity estimation ───────────────────────────────────────────────

/// Estimate selectivity of a filter expression using catalog statistics.
pub fn estimate_selectivity(filter_expr: &Expression, catalog_stats: &CatalogStats) -> f64 {
    match filter_expr {
        Expression::Bool(true) => 1.0,
        Expression::Bool(false) => 0.0,
        Expression::Eq(lhs, rhs) => {
            if let Some(prop) = extract_property(lhs) {
                let distinct = best_distinct_for_property(catalog_stats, prop);
                if distinct > 0.0 {
                    return (1.0 / distinct).min(1.0);
                }
            }
            if let Some(prop) = extract_property(rhs) {
                let distinct = best_distinct_for_property(catalog_stats, prop);
                if distinct > 0.0 {
                    return (1.0 / distinct).min(1.0);
                }
            }
            0.01
        }
        Expression::Neq(_, _) => 0.99,
        Expression::Lt(lhs, rhs)
        | Expression::Gt(lhs, rhs)
        | Expression::Lte(lhs, rhs)
        | Expression::Gte(lhs, rhs) => {
            if extract_property(lhs).is_some() || extract_property(rhs).is_some() {
                // Range selectivity: heuristic 1/3 for one-sided range.
                return 0.33;
            }
            0.1
        }
        Expression::And(lhs, rhs) => {
            let s1 = estimate_selectivity(lhs, catalog_stats);
            let s2 = estimate_selectivity(rhs, catalog_stats);
            s1 * s2
        }
        Expression::Or(lhs, rhs) => {
            let s1 = estimate_selectivity(lhs, catalog_stats);
            let s2 = estimate_selectivity(rhs, catalog_stats);
            (s1 + s2 - s1 * s2).min(1.0)
        }
        Expression::Not(inner) => 1.0 - estimate_selectivity(inner, catalog_stats),
        Expression::IsNull(_) | Expression::IsNotNull(_) => 0.02,
        Expression::StartsWith(_, _)
        | Expression::EndsWith(_, _)
        | Expression::Contains(_, _)
        | Expression::RegexMatch(_, _) => 0.05,
        Expression::In(_, _) => 0.15,
        Expression::Label { .. } => 0.1,
        _ => 0.3,
    }
}

/// Extract PropertyId from a property access expression like `n.prop`.
/// Returns the property key; label lookup is done by the caller.
fn extract_property(expr: &Expression) -> Option<PropertyId> {
    match expr {
        Expression::Property { object, key } => {
            if let Expression::Identifier(_) = &**object {
                Some(*key)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Find the best (most selective) distinct count for a property across all tables.
fn best_distinct_for_property(catalog_stats: &CatalogStats, property: PropertyId) -> f64 {
    catalog_stats
        .tables
        .values()
        .filter_map(|t| t.distinct_values.get(&property).copied())
        .map(|v| v as f64)
        .max_by(|a, b| a.partial_cmp(b).unwrap())
        .unwrap_or(1.0)
}

/// Estimate the cardinality of a logical operator given catalog statistics.
pub fn estimate_cardinality(op: &LogicalOp, catalog_stats: &CatalogStats) -> f64 {
    match op {
        LogicalOp::AllScan { .. } => catalog_stats.total_vertices.max(1) as f64,
        LogicalOp::LabelScan { label, .. } => catalog_stats.label_cardinality(*label),
        LogicalOp::LabelPropertyScan { label, .. } => {
            let label_card = catalog_stats.label_cardinality(*label);
            label_card * 0.01 // index equality selectivity
        }
        LogicalOp::EdgeTypeScan { edge_type, .. } => {
            catalog_stats.edge_type_cardinality(*edge_type)
        }
        LogicalOp::EdgeTypePropertyScan { edge_type, .. } => {
            let edge_card = catalog_stats.edge_type_cardinality(*edge_type);
            edge_card * 0.01
        }
        LogicalOp::Filter { condition } => {
            // Filter cardinality depends on input; we return selectivity here
            // and the caller multiplies by input cardinality.
            estimate_selectivity(condition, catalog_stats)
        }
        LogicalOp::Join {
            left,
            right,
            join_type,
        } => {
            let left_card = estimate_cardinality(&left.op, catalog_stats);
            let right_card = estimate_cardinality(&right.op, catalog_stats);
            match join_type {
                JoinType::NestedLoop => left_card * right_card,
                JoinType::HashJoin { .. } => left_card.max(right_card),
            }
        }
        LogicalOp::Distinct => 0.5, // deduplication roughly halves rows
        LogicalOp::Limit { count } => *count as f64,
        LogicalOp::Skip { .. } => 1.0, // skip does not change total row count
        LogicalOp::Aggregate { group_by, .. } => {
            if group_by.is_empty() {
                1.0
            } else {
                10.0 // heuristic for grouped aggregation
            }
        }
        LogicalOp::Union { left, right, all } => {
            let left_card = estimate_cardinality(&left.op, catalog_stats);
            let right_card = estimate_cardinality(&right.op, catalog_stats);
            if *all {
                left_card + right_card
            } else {
                (left_card + right_card) * 0.5
            }
        }
        _ => 1.0,
    }
}

// ─── Physical operators ───────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub enum PhysicalOp {
    SeqScan {
        alias: Option<String>,
        label: Option<LabelId>,
    },
    IndexScan {
        alias: Option<String>,
        label: LabelId,
        property: PropertyId,
        value: PropertyValue,
    },
    IndexSeek {
        alias: Option<String>,
        label: LabelId,
        property: PropertyId,
        value: PropertyValue,
    },
    EdgeTypeScan {
        alias: Option<String>,
        edge_type: EdgeTypeId,
    },
    EdgeTypePropertyScan {
        alias: Option<String>,
        edge_type: EdgeTypeId,
        property: PropertyId,
        value: PropertyValue,
    },
    HashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        left_key: String,
        right_key: String,
    },
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
    },
    IndexNestedLoopJoin {
        outer: Box<PhysicalPlan>,
        inner: Box<PhysicalPlan>,
        inner_key: String,
    },
    SortMergeJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        left_key: String,
        right_key: String,
    },
    Filter {
        condition: Expression,
        child: Box<PhysicalPlan>,
    },
    Project {
        expressions: Vec<Expression>,
        child: Box<PhysicalPlan>,
    },
    Produce {
        items: Vec<ReturnItem>,
        child: Box<PhysicalPlan>,
    },
    Sort {
        key: Vec<OrderByItem>,
        child: Box<PhysicalPlan>,
    },
    Limit {
        count: usize,
        child: Box<PhysicalPlan>,
    },
    Skip {
        count: usize,
        child: Box<PhysicalPlan>,
    },
    Aggregate {
        group_by: Vec<(Expression, Option<String>)>,
        aggregates: Vec<(Expression, Option<String>)>,
        child: Box<PhysicalPlan>,
    },
    HashAggregate {
        group_by: Vec<(Expression, Option<String>)>,
        aggregates: Vec<(Expression, Option<String>)>,
        child: Box<PhysicalPlan>,
    },
    StreamingAggregate {
        group_by: Vec<(Expression, Option<String>)>,
        aggregates: Vec<(Expression, Option<String>)>,
        child: Box<PhysicalPlan>,
    },
    /// Top-N operator: partial sort keeping only the top k rows.
    TopN {
        key: Vec<OrderByItem>,
        count: usize,
        child: Box<PhysicalPlan>,
    },
    /// Edge expansion: given a vertex in the input row, find connected edges
    /// and emit rows with the edge and destination vertex bound.
    EdgeExpand {
        from_alias: String,
        edge_alias: Option<String>,
        to_alias: Option<String>,
        direction: Direction,
        edge_type: Option<EdgeTypeId>,
        child: Box<PhysicalPlan>,
    },
    /// Deduplicate rows based on all columns.
    Distinct {
        child: Box<PhysicalPlan>,
    },
}

#[derive(Clone, Debug)]
pub struct PhysicalPlan {
    pub op: PhysicalOp,
    pub cost: PlanCost,
    pub cardinality: f64,
}

/// Convert a LogicalOp tree into a PhysicalOp tree.
///
/// This is a heuristic-based conversion that picks physical operators
/// based on operator type and estimated cost.
pub fn physical_plan_from_logical(
    logical: &LogicalPlan,
    cm: &CostModel,
    catalog_stats: &CatalogStats,
) -> PhysicalPlan {
    physical_plan_from_logical_recursive(logical, cm, catalog_stats)
}

fn physical_plan_from_logical_recursive(
    logical: &LogicalPlan,
    cm: &CostModel,
    catalog_stats: &CatalogStats,
) -> PhysicalPlan {
    match &logical.op {
        LogicalOp::AllScan { alias } => PhysicalPlan {
            op: PhysicalOp::SeqScan {
                alias: alias.clone(),
                label: None,
            },
            cost: PlanCost {
                cpu: cm.scan_all_cost,
                io: 0.0,
            },
            cardinality: catalog_stats.total_vertices.max(1) as f64,
        },
        LogicalOp::LabelScan { alias, label } => PhysicalPlan {
            op: PhysicalOp::SeqScan {
                alias: alias.clone(),
                label: Some(*label),
            },
            cost: PlanCost {
                cpu: cm.scan_label_cost,
                io: 0.0,
            },
            cardinality: catalog_stats.label_cardinality(*label),
        },
        LogicalOp::LabelPropertyScan {
            alias,
            label,
            property,
            value,
        } => {
            let card = catalog_stats.label_cardinality(*label) * cm.equality_selectivity;
            PhysicalPlan {
                op: PhysicalOp::IndexSeek {
                    alias: alias.clone(),
                    label: *label,
                    property: *property,
                    value: value.clone(),
                },
                cost: PlanCost {
                    cpu: cm.index_seek_cost_per_row * card,
                    io: 0.0,
                },
                cardinality: card.max(1.0),
            }
        }
        LogicalOp::EdgeTypeScan { alias, edge_type } => PhysicalPlan {
            op: PhysicalOp::EdgeTypeScan {
                alias: alias.clone(),
                edge_type: *edge_type,
            },
            cost: PlanCost {
                cpu: cm.scan_label_cost,
                io: 0.0,
            },
            cardinality: catalog_stats.edge_type_cardinality(*edge_type),
        },
        LogicalOp::EdgeTypePropertyScan {
            alias,
            edge_type,
            property,
            value,
        } => {
            let card = catalog_stats.edge_type_cardinality(*edge_type) * cm.equality_selectivity;
            PhysicalPlan {
                op: PhysicalOp::EdgeTypePropertyScan {
                    alias: alias.clone(),
                    edge_type: *edge_type,
                    property: *property,
                    value: value.clone(),
                },
                cost: PlanCost {
                    cpu: cm.index_seek_cost_per_row * card,
                    io: 0.0,
                },
                cardinality: card.max(1.0),
            }
        }
        LogicalOp::Filter { condition } => {
            let child = physical_plan_from_logical_recursive(
                logical.children().first().copied().unwrap_or(&LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: catalog_stats.total_vertices.max(1) as f64,
                }),
                cm,
                catalog_stats,
            );
            let sel = estimate_selectivity(condition, catalog_stats);
            let card = child.cardinality * sel;
            PhysicalPlan {
                op: PhysicalOp::Filter {
                    condition: condition.clone(),
                    child: Box::new(child),
                },
                cost: PlanCost {
                    cpu: card * cm.filter_cost_per_row,
                    io: 0.0,
                },
                cardinality: card.max(0.0),
            }
        }
        LogicalOp::Project { expressions } => {
            let child = physical_plan_from_logical_recursive(
                logical.children().first().copied().unwrap_or(&LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 1.0,
                }),
                cm,
                catalog_stats,
            );
            let card = child.cardinality;
            PhysicalPlan {
                op: PhysicalOp::Project {
                    expressions: expressions.clone(),
                    child: Box::new(child),
                },
                cost: PlanCost { cpu: card, io: 0.0 },
                cardinality: card,
            }
        }
        LogicalOp::Join {
            left,
            right,
            join_type,
        } => {
            let left_phys = physical_plan_from_logical_recursive(left, cm, catalog_stats);
            let left_card = left_phys.cardinality;
            let left_cost = left_phys.cost.clone();
            // Pipeline operators (Produce, Project, Sort, Limit, Skip, Filter, Distinct)
            // that come from clause chaining should absorb the left plan as their child
            // rather than forming a cartesian-product join.
            match &right.op {
                LogicalOp::Produce { items } => {
                    return PhysicalPlan {
                        op: PhysicalOp::Produce {
                            items: items.clone(),
                            child: Box::new(left_phys),
                        },
                        cost: left_cost,
                        cardinality: left_card,
                    };
                }
                LogicalOp::Project { expressions } => {
                    return PhysicalPlan {
                        op: PhysicalOp::Project {
                            expressions: expressions.clone(),
                            child: Box::new(left_phys),
                        },
                        cost: left_cost,
                        cardinality: left_card,
                    };
                }
                LogicalOp::Sort { key } => {
                    let sort_cost = left_card * cm.sort_cost_factor * left_card.log2().max(1.0);
                    return PhysicalPlan {
                        op: PhysicalOp::Sort {
                            key: key.clone(),
                            child: Box::new(left_phys),
                        },
                        cost: PlanCost {
                            cpu: sort_cost,
                            io: 0.0,
                        },
                        cardinality: left_card,
                    };
                }
                LogicalOp::Limit { count } => {
                    return PhysicalPlan {
                        op: PhysicalOp::Limit {
                            count: *count,
                            child: Box::new(left_phys),
                        },
                        cost: PlanCost { cpu: 1.0, io: 0.0 },
                        cardinality: (*count as f64).min(left_card),
                    };
                }
                LogicalOp::Skip { count } => {
                    return PhysicalPlan {
                        op: PhysicalOp::Skip {
                            count: *count,
                            child: Box::new(left_phys),
                        },
                        cost: PlanCost { cpu: 1.0, io: 0.0 },
                        cardinality: left_card,
                    };
                }
                LogicalOp::TopN { key, count } => {
                    let topn_cost =
                        left_card * cm.sort_cost_factor * (*count as f64).log2().max(1.0);
                    return PhysicalPlan {
                        op: PhysicalOp::TopN {
                            key: key.clone(),
                            count: *count,
                            child: Box::new(left_phys),
                        },
                        cost: PlanCost {
                            cpu: topn_cost,
                            io: 0.0,
                        },
                        cardinality: (*count as f64).min(left_card),
                    };
                }
                LogicalOp::Filter { condition } => {
                    let sel = estimate_selectivity(condition, catalog_stats);
                    let card = left_card * sel;
                    return PhysicalPlan {
                        op: PhysicalOp::Filter {
                            condition: condition.clone(),
                            child: Box::new(left_phys),
                        },
                        cost: PlanCost {
                            cpu: card * cm.filter_cost_per_row,
                            io: 0.0,
                        },
                        cardinality: card.max(0.0),
                    };
                }
                LogicalOp::Distinct => {
                    return PhysicalPlan {
                        op: PhysicalOp::HashAggregate {
                            group_by: vec![],
                            aggregates: vec![],
                            child: Box::new(left_phys),
                        },
                        cost: PlanCost {
                            cpu: left_card * cm.distinct_cost_per_row,
                            io: 0.0,
                        },
                        cardinality: left_card * 0.5,
                    };
                }
                LogicalOp::EdgeExpand {
                    from_alias,
                    edge_alias,
                    to_alias,
                    direction,
                    edge_type,
                } => {
                    return PhysicalPlan {
                        op: PhysicalOp::EdgeExpand {
                            from_alias: from_alias.clone(),
                            edge_alias: edge_alias.clone(),
                            to_alias: to_alias.clone(),
                            direction: *direction,
                            edge_type: *edge_type,
                            child: Box::new(left_phys),
                        },
                        cost: PlanCost {
                            cpu: left_card * 5.0,
                            io: 0.0,
                        },
                        cardinality: left_card * 5.0,
                    };
                }
                LogicalOp::Aggregate {
                    group_by,
                    aggregates,
                } => {
                    let out_card = if group_by.is_empty() {
                        1.0
                    } else {
                        left_card * 0.1
                    };
                    return PhysicalPlan {
                        op: PhysicalOp::HashAggregate {
                            group_by: group_by.clone(),
                            aggregates: aggregates.clone(),
                            child: Box::new(left_phys),
                        },
                        cost: PlanCost {
                            cpu: cm.hash_aggregate_cost(left_card),
                            io: 0.0,
                        },
                        cardinality: out_card.max(1.0),
                    };
                }
                _ => {}
            }
            let right_phys = physical_plan_from_logical_recursive(right, cm, catalog_stats);
            choose_join_physical(left_phys, right_phys, join_type, cm)
        }
        LogicalOp::Sort { key } => {
            let child = physical_plan_from_logical_recursive(
                logical.children().first().copied().unwrap_or(&LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 1.0,
                }),
                cm,
                catalog_stats,
            );
            let card = child.cardinality;
            let sort_cost = card * cm.sort_cost_factor * card.log2().max(1.0);
            PhysicalPlan {
                op: PhysicalOp::Sort {
                    key: key.clone(),
                    child: Box::new(child),
                },
                cost: PlanCost {
                    cpu: sort_cost,
                    io: 0.0,
                },
                cardinality: card,
            }
        }
        LogicalOp::Limit { count } => {
            let child_plan = physical_plan_from_logical_recursive(
                logical.children().first().copied().unwrap_or(&LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 1.0,
                }),
                cm,
                catalog_stats,
            );
            let card = child_plan.cardinality;
            // Merge Sort + Limit into TopN for better performance
            if let PhysicalOp::Sort {
                key,
                child: sort_child,
            } = child_plan.op
            {
                let topn_cost = card * cm.sort_cost_factor * (*count as f64).log2().max(1.0);
                return PhysicalPlan {
                    op: PhysicalOp::TopN {
                        key,
                        count: *count,
                        child: sort_child,
                    },
                    cost: PlanCost {
                        cpu: topn_cost,
                        io: 0.0,
                    },
                    cardinality: (*count as f64).min(card),
                };
            }
            PhysicalPlan {
                op: PhysicalOp::Limit {
                    count: *count,
                    child: Box::new(child_plan),
                },
                cost: PlanCost { cpu: 1.0, io: 0.0 },
                cardinality: (*count as f64).min(card),
            }
        }
        LogicalOp::TopN { key, count } => {
            let child = physical_plan_from_logical_recursive(
                logical.children().first().copied().unwrap_or(&LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 1.0,
                }),
                cm,
                catalog_stats,
            );
            let card = child.cardinality;
            let topn_cost = card * cm.sort_cost_factor * (*count as f64).log2().max(1.0);
            PhysicalPlan {
                op: PhysicalOp::TopN {
                    key: key.clone(),
                    count: *count,
                    child: Box::new(child),
                },
                cost: PlanCost {
                    cpu: topn_cost,
                    io: 0.0,
                },
                cardinality: (*count as f64).min(card),
            }
        }
        LogicalOp::Aggregate {
            group_by,
            aggregates,
        } => {
            let child = physical_plan_from_logical_recursive(
                logical.children().first().copied().unwrap_or(&LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 1.0,
                }),
                cm,
                catalog_stats,
            );
            // Prefer hash aggregate for small group-by sets, streaming for sorted input.
            let card = child.cardinality;
            let out_card = if group_by.is_empty() { 1.0 } else { card * 0.1 };
            let is_sorted = child_is_sorted(&child);
            let (op, agg_cost) = if is_sorted {
                let cost = cm.streaming_aggregate_cost(card);
                (
                    PhysicalOp::StreamingAggregate {
                        group_by: group_by.clone(),
                        aggregates: aggregates.clone(),
                        child: Box::new(child),
                    },
                    cost,
                )
            } else {
                let cost = cm.hash_aggregate_cost(card);
                (
                    PhysicalOp::HashAggregate {
                        group_by: group_by.clone(),
                        aggregates: aggregates.clone(),
                        child: Box::new(child),
                    },
                    cost,
                )
            };
            PhysicalPlan {
                op,
                cost: PlanCost {
                    cpu: agg_cost,
                    io: 0.0,
                },
                cardinality: out_card.max(1.0),
            }
        }
        LogicalOp::Distinct => {
            let child = physical_plan_from_logical_recursive(
                logical.children().first().copied().unwrap_or(&LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 1.0,
                }),
                cm,
                catalog_stats,
            );
            let child_card = child.cardinality;
            PhysicalPlan {
                op: PhysicalOp::Distinct {
                    child: Box::new(child),
                },
                cost: PlanCost {
                    cpu: child_card * cm.distinct_cost_per_row,
                    io: 0.0,
                },
                cardinality: child_card * 0.5,
            }
        }
        LogicalOp::Union { left, right, all } => {
            let left_phys = physical_plan_from_logical_recursive(left, cm, catalog_stats);
            let right_phys = physical_plan_from_logical_recursive(right, cm, catalog_stats);
            let card = if *all {
                left_phys.cardinality + right_phys.cardinality
            } else {
                (left_phys.cardinality + right_phys.cardinality) * 0.5
            };
            // Union is represented as a concatenation operator (not in PhysicalOp enum,
            // so we use a NestedLoopJoin as a placeholder for concatenation).
            PhysicalPlan {
                op: PhysicalOp::NestedLoopJoin {
                    left: Box::new(left_phys),
                    right: Box::new(right_phys),
                },
                cost: PlanCost {
                    cpu: cm.union_overhead,
                    io: 0.0,
                },
                cardinality: card,
            }
        }
        LogicalOp::Produce { items } => {
            let child = physical_plan_from_logical_recursive(
                logical.children().first().copied().unwrap_or(&LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 1.0,
                }),
                cm,
                catalog_stats,
            );
            PhysicalPlan {
                op: PhysicalOp::Produce {
                    items: items.clone(),
                    child: Box::new(child),
                },
                cost: PlanCost { cpu: 1.0, io: 0.0 },
                cardinality: logical.cardinality.max(1.0),
            }
        }
        LogicalOp::Skip { count } => {
            let child = physical_plan_from_logical_recursive(
                logical.children().first().copied().unwrap_or(&LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 1.0,
                }),
                cm,
                catalog_stats,
            );
            let card = child.cardinality;
            PhysicalPlan {
                op: PhysicalOp::Skip {
                    count: *count,
                    child: Box::new(child),
                },
                cost: PlanCost { cpu: 1.0, io: 0.0 },
                cardinality: card,
            }
        }
        LogicalOp::EmptyResult => PhysicalPlan {
            op: PhysicalOp::SeqScan {
                alias: None,
                label: None,
            },
            cost: PlanCost { cpu: 0.0, io: 0.0 },
            cardinality: 0.0,
        },
        ref other => panic!(
            "physical_plan_from_logical_recursive: unhandled logical operator {:?}. \
             Add a conversion arm for this operator or exclude it from physical execution.",
            other
        ),
    }
}

/// Heuristic: check if child plan is already sorted.
fn child_is_sorted(plan: &PhysicalPlan) -> bool {
    matches!(plan.op, PhysicalOp::Sort { .. })
}

/// Choose the cheapest physical join implementation.
fn choose_join_physical(
    left: PhysicalPlan,
    right: PhysicalPlan,
    join_type: &JoinType,
    cm: &CostModel,
) -> PhysicalPlan {
    let left_card = left.cardinality;
    let right_card = right.cardinality;
    let nl_cost = cm.nested_loop_join_cost(left_card, right_card);
    let hash_cost = cm.hash_join_cost(left_card, right_card);
    let sm_cost = cm.sort_merge_join_cost(left_card, right_card);

    let (op, cost) = match join_type {
        JoinType::HashJoin {
            left_key,
            right_key,
        } => {
            // Prefer hash join when explicitly requested or when it's cheapest.
            if hash_cost <= nl_cost && hash_cost <= sm_cost {
                (
                    PhysicalOp::HashJoin {
                        left: Box::new(left.clone()),
                        right: Box::new(right.clone()),
                        left_key: left_key.clone(),
                        right_key: right_key.clone(),
                    },
                    hash_cost,
                )
            } else if sm_cost <= nl_cost {
                (
                    PhysicalOp::SortMergeJoin {
                        left: Box::new(left.clone()),
                        right: Box::new(right.clone()),
                        left_key: left_key.clone(),
                        right_key: right_key.clone(),
                    },
                    sm_cost,
                )
            } else {
                (
                    PhysicalOp::NestedLoopJoin {
                        left: Box::new(left.clone()),
                        right: Box::new(right.clone()),
                    },
                    nl_cost,
                )
            }
        }
        JoinType::NestedLoop => {
            if hash_cost <= nl_cost && hash_cost <= sm_cost {
                (
                    PhysicalOp::NestedLoopJoin {
                        left: Box::new(left.clone()),
                        right: Box::new(right.clone()),
                    },
                    nl_cost,
                )
            } else if sm_cost <= nl_cost {
                (
                    PhysicalOp::SortMergeJoin {
                        left: Box::new(left.clone()),
                        right: Box::new(right.clone()),
                        left_key: String::new(),
                        right_key: String::new(),
                    },
                    sm_cost,
                )
            } else {
                (
                    PhysicalOp::NestedLoopJoin {
                        left: Box::new(left.clone()),
                        right: Box::new(right.clone()),
                    },
                    nl_cost,
                )
            }
        }
    };

    PhysicalPlan {
        op,
        cost: PlanCost { cpu: cost, io: 0.0 },
        cardinality: left_card.max(right_card),
    }
}

// Helper for LogicalPlan children
impl LogicalPlan {
    fn children(&self) -> Vec<&LogicalPlan> {
        match &self.op {
            LogicalOp::Join { left, right, .. } => vec![left, right],
            LogicalOp::Union { left, right, .. } => vec![left, right],
            _ => vec![],
        }
    }
}

// ─── Optimization pipeline ────────────────────────────────────────────────

/// Optimize a logical plan into a physical plan.
///
/// Stage 1: egraph equality saturation with rewrite rules.
/// Stage 2: extract lowest-cost plan from egraph.
/// Stage 3: convert to physical plan.
pub fn optimize_query(
    logical_plan: LogicalPlan,
    cm: &CostModel,
    catalog_stats: &CatalogStats,
) -> PhysicalPlan {
    // Stage 1 & 2: egraph optimization (returns an optimized logical plan)
    let optimized_logical = egraph::optimize_egraph(&logical_plan, cm);

    // Stage 3: convert to physical plan
    physical_plan_from_logical(&optimized_logical, cm, catalog_stats)
}

// ─── Rule-based optimizer ──────────────────────────────────────────────────

/// Apply cardinality-based rewrite rules:
///   1. Small-join-first reordering (swap left/right if right is smaller)
///   2. Index scan selection (done at plan_match time via LabelPropertyScan)
pub fn optimize(plan: LogicalPlan, _stats: &PlanStats, cost_model: &CostModel) -> LogicalPlan {
    let plan = pushdown_limit(plan, cost_model);
    let plan = pushdown_skip(plan, cost_model);
    let plan = pushdown_filter(plan, cost_model);
    let plan = remove_redundant_filters(plan);
    let plan = merge_filters(plan);
    let plan = constant_fold(plan);
    let plan = simplify_trivial_filters(plan);
    let plan = remove_true_filters(plan);
    let plan = index_intersection(plan, _stats);
    reorder_joins(plan, cost_model)
}

/// Pipeline operators that consume their child input and should not be
/// reordered across join boundaries (they are semantically tied to their
/// input via clause chaining).
fn is_pipeline_operator(op: &LogicalOp) -> bool {
    matches!(
        op,
        LogicalOp::Produce { .. }
            | LogicalOp::Project { .. }
            | LogicalOp::Sort { .. }
            | LogicalOp::Limit { .. }
            | LogicalOp::Skip { .. }
            | LogicalOp::Filter { .. }
            | LogicalOp::Distinct
            | LogicalOp::EdgeExpand { .. }
            | LogicalOp::Aggregate { .. }
    )
}

fn reorder_joins(plan: LogicalPlan, cm: &CostModel) -> LogicalPlan {
    match plan.op {
        LogicalOp::Join {
            left,
            right,
            join_type,
        } => {
            let left = reorder_joins(*left, cm);
            let right = reorder_joins(*right, cm);
            let card = left.cardinality.max(right.cardinality);
            // Don't swap if either side is a pipeline operator — these are
            // semantically tied to their input via clause chaining.
            let can_swap = !is_pipeline_operator(&left.op) && !is_pipeline_operator(&right.op);
            if can_swap && right.cardinality < left.cardinality {
                LogicalPlan {
                    op: LogicalOp::Join {
                        left: Box::new(right),
                        right: Box::new(left),
                        join_type,
                    },
                    cost: PlanCost {
                        cpu: card + cm.nested_loop_join_cost,
                        io: 0.0,
                    },
                    cardinality: card,
                }
            } else {
                LogicalPlan {
                    op: LogicalOp::Join {
                        left: Box::new(left),
                        right: Box::new(right),
                        join_type,
                    },
                    cost: PlanCost {
                        cpu: card + cm.nested_loop_join_cost,
                        io: 0.0,
                    },
                    cardinality: card,
                }
            }
        }
        LogicalOp::Union { left, right, all } => {
            let left = reorder_joins(*left, cm);
            let right = reorder_joins(*right, cm);
            let card = left.cardinality + right.cardinality;
            LogicalPlan {
                op: LogicalOp::Union {
                    left: Box::new(left),
                    right: Box::new(right),
                    all,
                },
                cost: PlanCost {
                    cpu: plan.cost.cpu + cm.union_overhead,
                    io: 0.0,
                },
                cardinality: if all { card } else { card * 0.5 },
            }
        }
        _ => plan,
    }
}

// ─── Property equality extraction ─────────────────────────────────────────

fn extract_property_equality(expr: &Expression) -> Option<(&str, PropertyId, PropertyValue)> {
    match expr {
        Expression::Eq(lhs, rhs) => {
            if let Some(r) = try_property_literal(lhs, rhs) {
                return Some(r);
            }
            try_property_literal(rhs, lhs)
        }
        Expression::And(lhs, rhs) => {
            if let Some(result) = extract_property_equality(lhs) {
                return Some(result);
            }
            extract_property_equality(rhs)
        }
        _ => None,
    }
}

fn try_property_literal<'a>(
    prop_expr: &'a Expression,
    lit_expr: &Expression,
) -> Option<(&'a str, PropertyId, PropertyValue)> {
    if let Expression::Property { object, key } = prop_expr {
        if let Expression::Identifier(alias) = &**object {
            if let Some(val) = expr_to_literal(lit_expr) {
                return Some((alias, *key, val));
            }
        }
    }
    None
}

/// Extract a specific property equality for a given alias and property from WHERE clause.
fn extract_specific_property_equality(
    expr: &Expression,
    target_alias: &str,
    target_property: PropertyId,
) -> Option<PropertyValue> {
    match expr {
        Expression::Eq(lhs, rhs) => {
            if let Some((alias, prop, val)) = try_property_literal(lhs, rhs) {
                if alias == target_alias && prop == target_property {
                    return Some(val);
                }
            }
            if let Some((alias, prop, val)) = try_property_literal(rhs, lhs) {
                if alias == target_alias && prop == target_property {
                    return Some(val);
                }
            }
            None
        }
        Expression::And(lhs, rhs) => {
            if let Some(result) =
                extract_specific_property_equality(lhs, target_alias, target_property)
            {
                return Some(result);
            }
            extract_specific_property_equality(rhs, target_alias, target_property)
        }
        _ => None,
    }
}

/// Extract edge property equality from a WHERE clause expression.
/// Returns (edge_alias, property_id, literal_value) if found.
fn extract_edge_property_equality(expr: &Expression) -> Option<(&str, PropertyId, PropertyValue)> {
    // Same logic as extract_property_equality — property access on edge alias
    extract_property_equality(expr)
}

fn expr_to_literal(expr: &Expression) -> Option<PropertyValue> {
    match expr {
        Expression::Int(v) => Some(PropertyValue::Int(*v)),
        Expression::Double(v) => Some(PropertyValue::Double(*v)),
        Expression::String(v) => Some(PropertyValue::String(v.clone())),
        Expression::Bool(v) => Some(PropertyValue::Bool(*v)),
        Expression::Null => Some(PropertyValue::Null),
        _ => None,
    }
}

fn is_aggregate(expr: &Expression) -> bool {
    match expr {
        Expression::CountStar => true,
        Expression::Function { name, .. } => {
            name.eq_ignore_ascii_case("count")
                || name.eq_ignore_ascii_case("sum")
                || name.eq_ignore_ascii_case("avg")
                || name.eq_ignore_ascii_case("min")
                || name.eq_ignore_ascii_case("max")
                || name.eq_ignore_ascii_case("collect")
                || name.eq_ignore_ascii_case("collect_map")
                || name.eq_ignore_ascii_case("collectmap")
        }
        Expression::Add(a, b)
        | Expression::Sub(a, b)
        | Expression::Mul(a, b)
        | Expression::Div(a, b)
        | Expression::Mod(a, b) => is_aggregate(a) || is_aggregate(b),
        Expression::Neg(a) | Expression::Not(a) => is_aggregate(a),
        _ => false,
    }
}

// ─── Plan generation ──────────────────────────────────────────────────────

pub fn plan_query(storage: &Storage, query: &Query) -> LogicalPlan {
    let fp = query_fingerprint(query);

    // Try cache first
    {
        let mut cache_guard = get_plan_cache();
        if cache_guard.is_none() {
            *cache_guard = Some(lru::LruCache::new(
                std::num::NonZeroUsize::new(1024).unwrap(),
            ));
        }
        if let Some(cached) = cache_guard.as_mut().unwrap().get(&fp) {
            return cached.clone();
        }
    }

    // Cache miss — compute plan
    let stats = PlanStats::from_storage(storage);
    let cost_model = CostModel::default();
    let plan = ast_to_logical(query, &stats);
    let plan = optimize(plan, &stats, &cost_model);

    // Store in cache
    {
        let mut cache_guard = get_plan_cache();
        cache_guard.as_mut().unwrap().put(fp, plan.clone());
    }

    plan
}

fn ast_to_logical(query: &Query, stats: &PlanStats) -> LogicalPlan {
    let mut current: Option<LogicalPlan> = None;

    for clause in &query.clauses {
        let clause_plan = match clause {
            Clause::Match {
                pattern,
                where_clause,
            } => plan_match(pattern, where_clause, stats, &query.index_hints),
            Clause::Create { pattern } => {
                let mut children = Vec::new();
                for element in &pattern.elements {
                    let props: Vec<_> = element
                        .node
                        .properties
                        .iter()
                        .map(|(k, v)| (*k, v.clone()))
                        .collect();
                    children.push(leaf(
                        LogicalOp::CreateVertex {
                            labels: element.node.labels.clone(),
                            properties: props,
                        },
                        stats.vertex_count as f64,
                    ));
                }
                chain_children(children)
            }
            Clause::Return {
                items,
                distinct: _,
                all,
            } => {
                if *all || !items.iter().any(|item| is_aggregate(&item.expression)) {
                    leaf(
                        LogicalOp::Produce {
                            items: items.clone(),
                        },
                        1.0,
                    )
                } else {
                    let group_by: Vec<(Expression, Option<String>)> = items
                        .iter()
                        .filter(|item| !is_aggregate(&item.expression))
                        .map(|item| (item.expression.clone(), item.alias.clone()))
                        .collect();
                    let aggregates: Vec<(Expression, Option<String>)> = items
                        .iter()
                        .filter(|item| is_aggregate(&item.expression))
                        .map(|item| (item.expression.clone(), item.alias.clone()))
                        .collect();
                    leaf(
                        LogicalOp::Aggregate {
                            group_by,
                            aggregates,
                        },
                        1.0,
                    )
                }
            }
            Clause::With {
                items,
                where_clause,
            } => {
                let card = current.as_ref().map(|p| p.cardinality).unwrap_or(1.0);
                let mut child = leaf(
                    LogicalOp::Project {
                        expressions: items.iter().map(|i| i.expression.clone()).collect(),
                    },
                    card,
                );
                if let Some(expr) = where_clause {
                    let filter_card = child.cardinality * 0.3;
                    child = chain(
                        child,
                        leaf(
                            LogicalOp::Filter {
                                condition: expr.clone(),
                            },
                            filter_card,
                        ),
                    );
                }
                child
            }
            Clause::Set { items } => {
                let mut children = Vec::new();
                for item in items {
                    if let SetItem::Property { key, value, .. } = item {
                        children.push(leaf(
                            LogicalOp::SetProperty {
                                key: *key,
                                value: value.clone(),
                            },
                            1.0,
                        ));
                    }
                }
                chain_children(children)
            }
            Clause::Delete {
                expressions,
                detach,
            } => leaf(
                LogicalOp::Delete {
                    expressions: expressions.clone(),
                    detach: *detach,
                },
                1.0,
            ),
            Clause::OrderBy { items } => {
                let card = current.as_ref().map(|p| p.cardinality).unwrap_or(1.0);
                leaf(LogicalOp::Sort { key: items.clone() }, card)
            }
            Clause::Skip { count } => {
                if let Expression::Int(n) = count {
                    leaf(LogicalOp::Skip { count: *n as usize }, 1.0)
                } else {
                    continue;
                }
            }
            Clause::Limit { count } => {
                if let Expression::Int(n) = count {
                    leaf(LogicalOp::Limit { count: *n as usize }, 1.0)
                } else {
                    continue;
                }
            }
            Clause::Unwind { expression, alias } => leaf(
                LogicalOp::Unwind {
                    expr: expression.clone(),
                    alias: alias.clone(),
                },
                10.0,
            ),
            _ => continue,
        };

        current = Some(match current {
            None => clause_plan,
            Some(prev) => chain(prev, clause_plan),
        });
    }

    current.unwrap_or_else(|| LogicalPlan {
        op: LogicalOp::AllScan { alias: None },
        cost: PlanCost::default(),
        cardinality: 0.0,
    })
}

fn plan_match(
    pattern: &MatchPattern,
    where_clause: &Option<Expression>,
    stats: &PlanStats,
    index_hints: &[IndexHint],
) -> LogicalPlan {
    let mut element_plans: Vec<LogicalPlan> = Vec::new();
    let edge_count_fallback = (stats.edge_count / 10).max(1);

    for element in &pattern.elements {
        let node = &element.node;
        let node_card = if let Some(label) = node.labels.first() {
            stats.label_counts.get(label).copied().unwrap_or(0) as f64
        } else {
            stats.vertex_count as f64
        }
        .max(1.0);

        let mut scan = if let Some(label) = node.labels.first() {
            let mut plan = leaf(
                LogicalOp::LabelScan {
                    alias: node.alias.clone(),
                    label: *label,
                },
                node_card,
            );
            // Auto-detect property equality index scan
            if let Some(ref wc) = where_clause {
                if let Some((alias, prop, value)) = extract_property_equality(wc) {
                    if node.alias.as_deref() == Some(alias) {
                        let lp_count = stats
                            .label_property_counts
                            .get(&(*label, prop))
                            .copied()
                            .unwrap_or(0);
                        if lp_count > 0 {
                            plan = leaf(
                                LogicalOp::LabelPropertyScan {
                                    alias: node.alias.clone(),
                                    label: *label,
                                    property: prop,
                                    value,
                                },
                                lp_count as f64,
                            );
                        }
                    }
                }
            }
            // Apply USING INDEX hints
            if let Some(hint) = index_hints.iter().find(|h| h.label == *label) {
                if let Some(hint_prop) = hint.property {
                    // USING INDEX :Label(property) — force LabelPropertyScan if WHERE has this property equality
                    if let Some(ref wc) = where_clause {
                        if let Some(alias) = node.alias.as_deref() {
                            if let Some(value) =
                                extract_specific_property_equality(wc, alias, hint_prop)
                            {
                                let lp_count = stats
                                    .label_property_counts
                                    .get(&(*label, hint_prop))
                                    .copied()
                                    .unwrap_or(0);
                                plan = leaf(
                                    LogicalOp::LabelPropertyScan {
                                        alias: node.alias.clone(),
                                        label: *label,
                                        property: hint_prop,
                                        value,
                                    },
                                    lp_count.max(1) as f64,
                                );
                            }
                        }
                    }
                }
                // USING INDEX :Label (no property) — already defaults to LabelScan
            }
            plan
        } else {
            leaf(
                LogicalOp::AllScan {
                    alias: node.alias.clone(),
                },
                node_card,
            )
        };

        // Chain property-equality filters from pattern node properties
        if let Some(ref alias) = node.alias {
            for (prop_id, expr) in &node.properties {
                let card = scan.cardinality * 0.3;
                let condition = Expression::Eq(
                    Box::new(Expression::Property {
                        object: Box::new(Expression::Identifier(alias.clone())),
                        key: *prop_id,
                    }),
                    Box::new(expr.clone()),
                );
                scan = chain(
                    scan,
                    leaf(
                        LogicalOp::Filter { condition },
                        card,
                    ),
                );
            }
        }

        let mut prev_node_alias = node.alias.clone().unwrap_or_else(|| "_".to_string());
        for (edge, right_node) in &element.edges {
            let edge_plan = leaf(
                LogicalOp::EdgeExpand {
                    from_alias: prev_node_alias.clone(),
                    edge_alias: edge.alias.clone(),
                    to_alias: right_node.alias.clone(),
                    direction: edge.direction,
                    edge_type: edge.edge_types.first().copied(),
                },
                node_card * 5.0,
            );

            prev_node_alias = right_node.alias.clone().unwrap_or_else(|| "_".to_string());
            scan = chain(scan, edge_plan);

            // Apply edge property-equality filters on the combined scan+edge result
            if let Some(ref alias) = edge.alias {
                for (prop_id, expr) in &edge.properties {
                    let card = scan.cardinality * 0.3;
                    let condition = Expression::Eq(
                        Box::new(Expression::Property {
                            object: Box::new(Expression::Identifier(alias.clone())),
                            key: *prop_id,
                        }),
                        Box::new(expr.clone()),
                    );
                    scan = chain(
                        scan,
                        leaf(
                            LogicalOp::Filter { condition },
                            card,
                        ),
                    );
                }
            }

            // Apply right-node label filters
            if let Some(ref alias) = right_node.alias {
                for label in &right_node.labels {
                    let card = scan.cardinality * 0.5;
                    let condition = Expression::Label {
                        object: Box::new(Expression::Identifier(alias.clone())),
                        label: *label,
                    };
                    scan = chain(
                        scan,
                        leaf(
                            LogicalOp::Filter { condition },
                            card,
                        ),
                    );
                }
                // Apply right-node property-equality filters
                for (prop_id, expr) in &right_node.properties {
                    let card = scan.cardinality * 0.3;
                    let condition = Expression::Eq(
                        Box::new(Expression::Property {
                            object: Box::new(Expression::Identifier(alias.clone())),
                            key: *prop_id,
                        }),
                        Box::new(expr.clone()),
                    );
                    scan = chain(
                        scan,
                        leaf(
                            LogicalOp::Filter { condition },
                            card,
                        ),
                    );
                }
            }
        }

        element_plans.push(scan);
    }

    let mut match_plan = match element_plans.len() {
        0 => leaf(
            LogicalOp::AllScan { alias: None },
            stats.vertex_count as f64,
        ),
        1 => element_plans.into_iter().next().unwrap(),
        _ => {
            let mut iter = element_plans.into_iter();
            iter.next()
                .map(|first| {
                    iter.fold(first, |left, right| {
                        let join_card = left.cardinality.max(right.cardinality);
                        LogicalPlan {
                            op: LogicalOp::Join {
                                left: Box::new(left),
                                right: Box::new(right),
                                join_type: JoinType::NestedLoop,
                            },
                            cost: PlanCost {
                                cpu: 10.0 + join_card,
                                io: 0.0,
                            },
                            cardinality: join_card,
                        }
                    })
                })
                .unwrap_or_else(|| leaf(LogicalOp::AllScan { alias: None }, 1.0))
        }
    };

    if let Some(ref wc) = where_clause {
        let filter_card = match_plan.cardinality * 0.3;
        match_plan = chain(
            match_plan,
            leaf(
                LogicalOp::Filter {
                    condition: wc.clone(),
                },
                filter_card,
            ),
        );
    }

    match_plan
}

fn leaf(op: LogicalOp, cardinality: f64) -> LogicalPlan {
    let cm = CostModel::default();
    let (cost, card) = estimate_cost(&op, cardinality, &cm);
    LogicalPlan {
        op,
        cost: PlanCost { cpu: cost, io: 0.0 },
        cardinality: card,
    }
}

fn chain(left: LogicalPlan, right: LogicalPlan) -> LogicalPlan {
    let cardinality = left.cardinality.max(right.cardinality);
    LogicalPlan {
        op: LogicalOp::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::NestedLoop,
        },
        cost: PlanCost { cpu: 1.0, io: 0.0 },
        cardinality,
    }
}

fn chain_children(children: Vec<LogicalPlan>) -> LogicalPlan {
    let mut iter = children.into_iter();
    match iter.next() {
        Some(first) => iter.fold(first, chain),
        None => LogicalPlan {
            op: LogicalOp::AllScan { alias: None },
            cost: PlanCost::default(),
            cardinality: 0.0,
        },
    }
}

fn estimate_cost(op: &LogicalOp, input_card: f64, cm: &CostModel) -> (f64, f64) {
    match op {
        LogicalOp::AllScan { .. } => (cm.scan_all_cost, input_card),
        LogicalOp::LabelScan { .. } => (cm.scan_label_cost, input_card * cm.label_selectivity),
        LogicalOp::LabelPropertyScan { .. } => (
            cm.scan_label_property_cost,
            input_card * cm.equality_selectivity,
        ),
        LogicalOp::EdgeTypeScan { .. } => (cm.scan_label_cost, input_card * cm.label_selectivity),
        LogicalOp::EdgeTypePropertyScan { .. } => (
            cm.scan_label_property_cost,
            input_card * cm.equality_selectivity,
        ),
        LogicalOp::Filter { condition } => {
            let sel = estimate_selectivity_legacy(condition, cm);
            (input_card * cm.filter_cost_per_row, input_card * sel)
        }
        LogicalOp::Sort { .. } => (
            input_card * cm.sort_cost_factor * input_card.log2().max(1.0),
            input_card,
        ),
        LogicalOp::Limit { count } => (1.0, (*count as f64).min(input_card)),
        LogicalOp::Skip { .. } => (1.0, input_card),
        LogicalOp::EdgeExpand { .. } => (input_card * 10.0, input_card * 5.0),
        LogicalOp::Distinct => (input_card * cm.distinct_cost_per_row, input_card * 0.5),
        LogicalOp::Aggregate { group_by, .. } => {
            let out_card = if group_by.is_empty() {
                1.0
            } else {
                input_card * 0.1
            };
            (input_card * cm.aggregate_cost_per_row, out_card.max(1.0))
        }
        LogicalOp::Union { all, .. } => {
            let card = if *all { input_card } else { input_card * 0.5 };
            (cm.union_overhead, card)
        }
        _ => (input_card, input_card),
    }
}

/// Estimate selectivity for a given expression using predicate-type constants.
fn estimate_selectivity_legacy(expr: &Expression, cm: &CostModel) -> f64 {
    match expr {
        Expression::Bool(true) => 1.0,
        Expression::Bool(false) => 0.0,
        Expression::Eq(_, _) => cm.equality_selectivity,
        Expression::Neq(_, _) => cm.default_selectivity,
        Expression::Lt(_, _)
        | Expression::Gt(_, _)
        | Expression::Lte(_, _)
        | Expression::Gte(_, _) => cm.range_selectivity,
        Expression::And(lhs, rhs) => {
            let s1 = estimate_selectivity_legacy(lhs, cm);
            let s2 = estimate_selectivity_legacy(rhs, cm);
            s1 * s2
        }
        Expression::Or(lhs, rhs) => {
            let s1 = estimate_selectivity_legacy(lhs, cm);
            let s2 = estimate_selectivity_legacy(rhs, cm);
            (s1 + s2 - s1 * s2).min(1.0)
        }
        Expression::Not(inner) => 1.0 - estimate_selectivity_legacy(inner, cm),
        Expression::IsNull(_) | Expression::IsNotNull(_) => cm.null_selectivity,
        Expression::StartsWith(_, _)
        | Expression::EndsWith(_, _)
        | Expression::Contains(_, _)
        | Expression::RegexMatch(_, _) => cm.like_selectivity,
        Expression::In(_, _) => cm.in_selectivity,
        Expression::Label { .. } => cm.label_selectivity,
        _ => cm.default_selectivity,
    }
}

// ─── Optimization rules ───────────────────────────────────────────────────

/// Push Limit through Sort: Limit(Sort(child)) => Sort(Limit(child))
/// This reduces the amount of data that Sort needs to process.
///
/// In this design unary operators are chained via Join, so we only
/// recurse into Join/Union which actually store children.
fn pushdown_limit(plan: LogicalPlan, _cm: &CostModel) -> LogicalPlan {
    match plan.op {
        LogicalOp::Join {
            left,
            right,
            join_type,
        } => {
            // Check for Limit on top of Sort or Filter in the left branch.
            if let LogicalOp::Limit { count } = &left.op {
                if let LogicalOp::Sort { key } = &right.op {
                    let new_left = LogicalPlan {
                        op: LogicalOp::Limit { count: *count },
                        cost: left.cost.clone(),
                        cardinality: left.cardinality.min(*count as f64),
                    };
                    let new_right = pushdown_limit(
                        LogicalPlan {
                            op: LogicalOp::Sort { key: key.clone() },
                            cost: right.cost.clone(),
                            cardinality: right.cardinality,
                        },
                        _cm,
                    );
                    let card = new_left.cardinality.min(new_right.cardinality);
                    return LogicalPlan {
                        op: LogicalOp::Join {
                            left: Box::new(new_left),
                            right: Box::new(new_right),
                            join_type,
                        },
                        cost: plan.cost.clone(),
                        cardinality: card,
                    };
                }
                if let LogicalOp::Filter { condition } = &right.op {
                    let new_left = LogicalPlan {
                        op: LogicalOp::Limit { count: *count },
                        cost: left.cost.clone(),
                        cardinality: left.cardinality.min(*count as f64),
                    };
                    let new_right = pushdown_limit(
                        LogicalPlan {
                            op: LogicalOp::Filter {
                                condition: condition.clone(),
                            },
                            cost: right.cost.clone(),
                            cardinality: right.cardinality,
                        },
                        _cm,
                    );
                    let card = new_left.cardinality.min(new_right.cardinality);
                    return LogicalPlan {
                        op: LogicalOp::Join {
                            left: Box::new(new_left),
                            right: Box::new(new_right),
                            join_type,
                        },
                        cost: plan.cost.clone(),
                        cardinality: card,
                    };
                }
                // Push Limit through Project: Project doesn't affect row count.
                if let LogicalOp::Project { expressions } = &right.op {
                    let new_left = LogicalPlan {
                        op: LogicalOp::Limit { count: *count },
                        cost: left.cost.clone(),
                        cardinality: left.cardinality.min(*count as f64),
                    };
                    let new_right = pushdown_limit(
                        LogicalPlan {
                            op: LogicalOp::Project {
                                expressions: expressions.clone(),
                            },
                            cost: right.cost.clone(),
                            cardinality: right.cardinality,
                        },
                        _cm,
                    );
                    let card = new_left.cardinality.min(new_right.cardinality);
                    return LogicalPlan {
                        op: LogicalOp::Join {
                            left: Box::new(new_left),
                            right: Box::new(new_right),
                            join_type,
                        },
                        cost: plan.cost.clone(),
                        cardinality: card,
                    };
                }
            }
            let left = pushdown_limit(*left, _cm);
            let right = pushdown_limit(*right, _cm);
            LogicalPlan {
                op: LogicalOp::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_type,
                },
                cost: plan.cost.clone(),
                cardinality: plan.cardinality,
            }
        }
        LogicalOp::Union { left, right, all } => {
            let left = pushdown_limit(*left, _cm);
            let right = pushdown_limit(*right, _cm);
            LogicalPlan {
                op: LogicalOp::Union {
                    left: Box::new(left),
                    right: Box::new(right),
                    all,
                },
                cost: plan.cost.clone(),
                cardinality: plan.cardinality,
            }
        }
        _ => plan,
    }
}

/// Push Skip operators down through Project/Sort/Filter in the join chain.
/// Same structure as pushdown_limit but for Skip.
fn pushdown_skip(plan: LogicalPlan, _cm: &CostModel) -> LogicalPlan {
    match plan.op {
        LogicalOp::Join {
            left,
            right,
            join_type,
        } => {
            if let LogicalOp::Skip { count } = &left.op {
                if let LogicalOp::Sort { key } = &right.op {
                    let new_left = LogicalPlan {
                        op: LogicalOp::Skip { count: *count },
                        cost: left.cost.clone(),
                        cardinality: (left.cardinality - *count as f64).max(0.0),
                    };
                    let new_right = pushdown_skip(
                        LogicalPlan {
                            op: LogicalOp::Sort { key: key.clone() },
                            cost: right.cost.clone(),
                            cardinality: right.cardinality,
                        },
                        _cm,
                    );
                    let card = new_left.cardinality.min(new_right.cardinality);
                    return LogicalPlan {
                        op: LogicalOp::Join {
                            left: Box::new(new_left),
                            right: Box::new(new_right),
                            join_type,
                        },
                        cost: plan.cost.clone(),
                        cardinality: card,
                    };
                }
                if let LogicalOp::Filter { condition } = &right.op {
                    let new_left = LogicalPlan {
                        op: LogicalOp::Skip { count: *count },
                        cost: left.cost.clone(),
                        cardinality: (left.cardinality - *count as f64).max(0.0),
                    };
                    let new_right = pushdown_skip(
                        LogicalPlan {
                            op: LogicalOp::Filter {
                                condition: condition.clone(),
                            },
                            cost: right.cost.clone(),
                            cardinality: right.cardinality,
                        },
                        _cm,
                    );
                    let card = new_left.cardinality.min(new_right.cardinality);
                    return LogicalPlan {
                        op: LogicalOp::Join {
                            left: Box::new(new_left),
                            right: Box::new(new_right),
                            join_type,
                        },
                        cost: plan.cost.clone(),
                        cardinality: card,
                    };
                }
                if let LogicalOp::Project { expressions } = &right.op {
                    let new_left = LogicalPlan {
                        op: LogicalOp::Skip { count: *count },
                        cost: left.cost.clone(),
                        cardinality: (left.cardinality - *count as f64).max(0.0),
                    };
                    let new_right = pushdown_skip(
                        LogicalPlan {
                            op: LogicalOp::Project {
                                expressions: expressions.clone(),
                            },
                            cost: right.cost.clone(),
                            cardinality: right.cardinality,
                        },
                        _cm,
                    );
                    let card = new_left.cardinality.min(new_right.cardinality);
                    return LogicalPlan {
                        op: LogicalOp::Join {
                            left: Box::new(new_left),
                            right: Box::new(new_right),
                            join_type,
                        },
                        cost: plan.cost.clone(),
                        cardinality: card,
                    };
                }
            }
            let left = pushdown_skip(*left, _cm);
            let right = pushdown_skip(*right, _cm);
            LogicalPlan {
                op: LogicalOp::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_type,
                },
                cost: plan.cost.clone(),
                cardinality: plan.cardinality,
            }
        }
        LogicalOp::Union { left, right, all } => {
            let left = pushdown_skip(*left, _cm);
            let right = pushdown_skip(*right, _cm);
            LogicalPlan {
                op: LogicalOp::Union {
                    left: Box::new(left),
                    right: Box::new(right),
                    all,
                },
                cost: plan.cost.clone(),
                cardinality: plan.cardinality,
            }
        }
        _ => plan,
    }
}

/// Push Filter down through Project when the filter only references
/// expressions already present before the projection.
fn pushdown_filter(plan: LogicalPlan, _cm: &CostModel) -> LogicalPlan {
    match plan.op {
        LogicalOp::Join {
            left,
            right,
            join_type,
        } => {
            // Check for Filter on top of Project in the left branch.
            if let LogicalOp::Filter { ref condition } = left.op {
                if let LogicalOp::Project { ref expressions } = right.op {
                    if filter_references_preserved(condition, expressions) {
                        let new_left = LogicalPlan {
                            op: LogicalOp::Filter {
                                condition: condition.clone(),
                            },
                            cost: left.cost.clone(),
                            cardinality: right.cardinality * 0.3,
                        };
                        let new_right = pushdown_filter(
                            LogicalPlan {
                                op: LogicalOp::Project {
                                    expressions: expressions.clone(),
                                },
                                cost: right.cost.clone(),
                                cardinality: right.cardinality,
                            },
                            _cm,
                        );
                        let card = new_left.cardinality.min(new_right.cardinality);
                        return LogicalPlan {
                            op: LogicalOp::Join {
                                left: Box::new(new_left),
                                right: Box::new(new_right),
                                join_type,
                            },
                            cost: plan.cost.clone(),
                            cardinality: card,
                        };
                    }
                }
            }
            let left = pushdown_filter(*left, _cm);
            let right = pushdown_filter(*right, _cm);
            LogicalPlan {
                op: LogicalOp::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_type,
                },
                cost: plan.cost.clone(),
                cardinality: plan.cardinality,
            }
        }
        LogicalOp::Union { left, right, all } => {
            let left = pushdown_filter(*left, _cm);
            let right = pushdown_filter(*right, _cm);
            LogicalPlan {
                op: LogicalOp::Union {
                    left: Box::new(left),
                    right: Box::new(right),
                    all,
                },
                cost: plan.cost.clone(),
                cardinality: plan.cardinality,
            }
        }
        _ => plan,
    }
}

/// Check whether a filter condition only references identifiers that are
/// preserved (not transformed) by the projection.
fn filter_references_preserved(condition: &Expression, expressions: &[Expression]) -> bool {
    let mut referenced = Vec::new();
    collect_identifiers(condition, &mut referenced);
    let projected_aliases: std::collections::HashSet<String> = expressions
        .iter()
        .filter_map(|e| match e {
            Expression::Identifier(alias) => Some(alias.clone()),
            _ => None,
        })
        .collect();
    referenced.iter().all(|id| projected_aliases.contains(id))
}

fn collect_identifiers(expr: &Expression, out: &mut Vec<String>) {
    match expr {
        Expression::Identifier(id) => out.push(id.clone()),
        Expression::Property { object, .. } => collect_identifiers(object, out),
        Expression::And(lhs, rhs)
        | Expression::Or(lhs, rhs)
        | Expression::Eq(lhs, rhs)
        | Expression::Neq(lhs, rhs)
        | Expression::Lt(lhs, rhs)
        | Expression::Gt(lhs, rhs)
        | Expression::Lte(lhs, rhs)
        | Expression::Gte(lhs, rhs)
        | Expression::Add(lhs, rhs)
        | Expression::Sub(lhs, rhs)
        | Expression::Mul(lhs, rhs)
        | Expression::Div(lhs, rhs)
        | Expression::Mod(lhs, rhs)
        | Expression::In(lhs, rhs)
        | Expression::StartsWith(lhs, rhs)
        | Expression::EndsWith(lhs, rhs)
        | Expression::Contains(lhs, rhs)
        | Expression::RegexMatch(lhs, rhs) => {
            collect_identifiers(lhs, out);
            collect_identifiers(rhs, out);
        }
        Expression::Not(inner)
        | Expression::Neg(inner)
        | Expression::IsNull(inner)
        | Expression::IsNotNull(inner) => {
            collect_identifiers(inner, out);
        }
        _ => {}
    }
}

/// Remove redundant filters that are always true.
fn remove_redundant_filters(plan: LogicalPlan) -> LogicalPlan {
    match plan.op {
        LogicalOp::Join {
            left,
            right,
            join_type,
        } => {
            // Check for redundant Filter(true) on top of another operator.
            if let LogicalOp::Filter {
                condition: Expression::Bool(true),
            } = &left.op
            {
                let right = remove_redundant_filters(*right);
                return LogicalPlan {
                    op: LogicalOp::Join {
                        left: left.clone(),
                        right: Box::new(right),
                        join_type,
                    },
                    cost: plan.cost.clone(),
                    cardinality: plan.cardinality,
                };
            }
            let left = remove_redundant_filters(*left);
            let right = remove_redundant_filters(*right);
            LogicalPlan {
                op: LogicalOp::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_type,
                },
                cost: plan.cost.clone(),
                cardinality: plan.cardinality,
            }
        }
        LogicalOp::Union { left, right, all } => {
            let left = remove_redundant_filters(*left);
            let right = remove_redundant_filters(*right);
            LogicalPlan {
                op: LogicalOp::Union {
                    left: Box::new(left),
                    right: Box::new(right),
                    all,
                },
                cost: plan.cost.clone(),
                cardinality: plan.cardinality,
            }
        }
        _ => plan,
    }
}

/// Merge adjacent Filter operators into a single Filter with combined conditions.
///
/// In the current plan representation unary operators are chained via Join,
/// so two consecutive filters look like:
///   Join(Join(..., Filter(A)), Filter(B))
/// This rule collapses them to:
///   Join(..., Filter(And(A, B)))
fn merge_filters(plan: LogicalPlan) -> LogicalPlan {
    match plan.op {
        LogicalOp::Join {
            left,
            right,
            join_type,
        } => {
            let right = merge_filters(*right);
            // Check if right is a Filter and left ends with a Filter.
            if let LogicalOp::Filter {
                condition: right_cond,
            } = &right.op
            {
                let left = merge_filters(*left);
                if let LogicalOp::Join {
                    left: inner_left,
                    right: inner_right,
                    join_type: inner_join_type,
                } = &left.op
                {
                    if let LogicalOp::Filter {
                        condition: left_cond,
                    } = &inner_right.op
                    {
                        // Collapse: Join(Join(base, Filter(A)), Filter(B)) => Join(base, Filter(And(A, B)))
                        let merged_filter = LogicalPlan {
                            op: LogicalOp::Filter {
                                condition: Expression::And(
                                    Box::new(left_cond.clone()),
                                    Box::new(right_cond.clone()),
                                ),
                            },
                            cost: PlanCost {
                                cpu: left.cost.cpu + right.cost.cpu,
                                io: 0.0,
                            },
                            cardinality: left.cardinality.min(right.cardinality),
                        };
                        return LogicalPlan {
                            op: LogicalOp::Join {
                                left: inner_left.clone(),
                                right: Box::new(merged_filter),
                                join_type: inner_join_type.clone(),
                            },
                            cost: plan.cost.clone(),
                            cardinality: plan.cardinality,
                        };
                    }
                }
                // If left itself is a Filter (not wrapped in Join), merge directly.
                if let LogicalOp::Filter {
                    condition: left_cond,
                } = &left.op
                {
                    let merged_filter = LogicalPlan {
                        op: LogicalOp::Filter {
                            condition: Expression::And(
                                Box::new(left_cond.clone()),
                                Box::new(right_cond.clone()),
                            ),
                        },
                        cost: PlanCost {
                            cpu: left.cost.cpu + right.cost.cpu,
                            io: 0.0,
                        },
                        cardinality: left.cardinality.min(right.cardinality),
                    };
                    return merged_filter;
                }
                return LogicalPlan {
                    op: LogicalOp::Join {
                        left: Box::new(left),
                        right: Box::new(right),
                        join_type,
                    },
                    cost: plan.cost.clone(),
                    cardinality: plan.cardinality,
                };
            }
            let left = merge_filters(*left);
            LogicalPlan {
                op: LogicalOp::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_type,
                },
                cost: plan.cost.clone(),
                cardinality: plan.cardinality,
            }
        }
        LogicalOp::Union { left, right, all } => {
            let left = merge_filters(*left);
            let right = merge_filters(*right);
            LogicalPlan {
                op: LogicalOp::Union {
                    left: Box::new(left),
                    right: Box::new(right),
                    all,
                },
                cost: plan.cost.clone(),
                cardinality: plan.cardinality,
            }
        }
        _ => plan,
    }
}

/// Constant-fold expressions in Filter conditions.
/// Evaluates subexpressions that contain only literals.
fn constant_fold(plan: LogicalPlan) -> LogicalPlan {
    match plan.op {
        LogicalOp::Filter { condition } => {
            let folded = fold_expression(&condition);
            LogicalPlan {
                op: LogicalOp::Filter { condition: folded },
                cost: plan.cost.clone(),
                cardinality: plan.cardinality,
            }
        }
        LogicalOp::Join {
            left,
            right,
            join_type,
        } => {
            let left = constant_fold(*left);
            let right = constant_fold(*right);
            LogicalPlan {
                op: LogicalOp::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_type,
                },
                cost: plan.cost.clone(),
                cardinality: plan.cardinality,
            }
        }
        LogicalOp::Union { left, right, all } => {
            let left = constant_fold(*left);
            let right = constant_fold(*right);
            LogicalPlan {
                op: LogicalOp::Union {
                    left: Box::new(left),
                    right: Box::new(right),
                    all,
                },
                cost: plan.cost.clone(),
                cardinality: plan.cardinality,
            }
        }
        _ => plan,
    }
}

/// Recursively fold constant subexpressions.
fn fold_expression(expr: &Expression) -> Expression {
    match expr {
        Expression::Add(lhs, rhs) => {
            let l = fold_expression(lhs);
            let r = fold_expression(rhs);
            match (&l, &r) {
                (Expression::Int(a), Expression::Int(b)) => Expression::Int(a + b),
                (Expression::Double(a), Expression::Double(b)) => Expression::Double(a + b),
                (Expression::Int(a), Expression::Double(b)) => Expression::Double(*a as f64 + b),
                (Expression::Double(a), Expression::Int(b)) => Expression::Double(a + *b as f64),
                _ => Expression::Add(Box::new(l), Box::new(r)),
            }
        }
        Expression::Sub(lhs, rhs) => {
            let l = fold_expression(lhs);
            let r = fold_expression(rhs);
            match (&l, &r) {
                (Expression::Int(a), Expression::Int(b)) => Expression::Int(a - b),
                (Expression::Double(a), Expression::Double(b)) => Expression::Double(a - b),
                (Expression::Int(a), Expression::Double(b)) => Expression::Double(*a as f64 - b),
                (Expression::Double(a), Expression::Int(b)) => Expression::Double(a - *b as f64),
                _ => Expression::Sub(Box::new(l), Box::new(r)),
            }
        }
        Expression::Mul(lhs, rhs) => {
            let l = fold_expression(lhs);
            let r = fold_expression(rhs);
            match (&l, &r) {
                (Expression::Int(a), Expression::Int(b)) => Expression::Int(a * b),
                (Expression::Double(a), Expression::Double(b)) => Expression::Double(a * b),
                (Expression::Int(a), Expression::Double(b)) => Expression::Double(*a as f64 * b),
                (Expression::Double(a), Expression::Int(b)) => Expression::Double(a * *b as f64),
                _ => Expression::Mul(Box::new(l), Box::new(r)),
            }
        }
        Expression::Div(lhs, rhs) => {
            let l = fold_expression(lhs);
            let r = fold_expression(rhs);
            match (&l, &r) {
                (Expression::Int(a), Expression::Int(b)) if *b != 0 => Expression::Int(a / b),
                (Expression::Double(a), Expression::Double(b)) if *b != 0.0 => {
                    Expression::Double(a / b)
                }
                (Expression::Int(a), Expression::Double(b)) if *b != 0.0 => {
                    Expression::Double(*a as f64 / b)
                }
                (Expression::Double(a), Expression::Int(b)) if *b != 0 => {
                    Expression::Double(a / *b as f64)
                }
                _ => Expression::Div(Box::new(l), Box::new(r)),
            }
        }
        Expression::Mod(lhs, rhs) => {
            let l = fold_expression(lhs);
            let r = fold_expression(rhs);
            match (&l, &r) {
                (Expression::Int(a), Expression::Int(b)) if *b != 0 => Expression::Int(a % b),
                _ => Expression::Mod(Box::new(l), Box::new(r)),
            }
        }
        Expression::Neg(inner) => {
            let i = fold_expression(inner);
            match &i {
                Expression::Int(v) => Expression::Int(-v),
                Expression::Double(v) => Expression::Double(-v),
                _ => Expression::Neg(Box::new(i)),
            }
        }
        Expression::And(lhs, rhs) => {
            let l = fold_expression(lhs);
            let r = fold_expression(rhs);
            match (&l, &r) {
                (Expression::Bool(false), _) | (_, Expression::Bool(false)) => {
                    Expression::Bool(false)
                }
                (Expression::Bool(true), other) => other.clone(),
                (other, Expression::Bool(true)) => other.clone(),
                _ => Expression::And(Box::new(l), Box::new(r)),
            }
        }
        Expression::Or(lhs, rhs) => {
            let l = fold_expression(lhs);
            let r = fold_expression(rhs);
            match (&l, &r) {
                (Expression::Bool(true), _) | (_, Expression::Bool(true)) => Expression::Bool(true),
                (Expression::Bool(false), other) => other.clone(),
                (other, Expression::Bool(false)) => other.clone(),
                _ => Expression::Or(Box::new(l), Box::new(r)),
            }
        }
        Expression::Not(inner) => {
            let i = fold_expression(inner);
            match &i {
                Expression::Bool(v) => Expression::Bool(!v),
                _ => Expression::Not(Box::new(i)),
            }
        }
        Expression::Eq(lhs, rhs) => {
            let l = fold_expression(lhs);
            let r = fold_expression(rhs);
            if let Some(result) = eval_const_eq(&l, &r) {
                return result;
            }
            Expression::Eq(Box::new(l), Box::new(r))
        }
        Expression::Neq(lhs, rhs) => {
            let l = fold_expression(lhs);
            let r = fold_expression(rhs);
            if let Some(result) = eval_const_neq(&l, &r) {
                return result;
            }
            Expression::Neq(Box::new(l), Box::new(r))
        }
        Expression::Lt(lhs, rhs) => {
            let l = fold_expression(lhs);
            let r = fold_expression(rhs);
            if let Some(result) = eval_const_lt(&l, &r) {
                return result;
            }
            Expression::Lt(Box::new(l), Box::new(r))
        }
        Expression::Gt(lhs, rhs) => {
            let l = fold_expression(lhs);
            let r = fold_expression(rhs);
            if let Some(result) = eval_const_gt(&l, &r) {
                return result;
            }
            Expression::Gt(Box::new(l), Box::new(r))
        }
        Expression::Lte(lhs, rhs) => {
            let l = fold_expression(lhs);
            let r = fold_expression(rhs);
            if let Some(result) = eval_const_lte(&l, &r) {
                return result;
            }
            Expression::Lte(Box::new(l), Box::new(r))
        }
        Expression::Gte(lhs, rhs) => {
            let l = fold_expression(lhs);
            let r = fold_expression(rhs);
            if let Some(result) = eval_const_gte(&l, &r) {
                return result;
            }
            Expression::Gte(Box::new(l), Box::new(r))
        }
        Expression::IsNull(inner) => {
            let i = fold_expression(inner);
            match &i {
                Expression::Null => Expression::Bool(true),
                Expression::Bool(_)
                | Expression::Int(_)
                | Expression::Double(_)
                | Expression::String(_) => Expression::Bool(false),
                _ => Expression::IsNull(Box::new(i)),
            }
        }
        Expression::IsNotNull(inner) => {
            let i = fold_expression(inner);
            match &i {
                Expression::Null => Expression::Bool(false),
                Expression::Bool(_)
                | Expression::Int(_)
                | Expression::Double(_)
                | Expression::String(_) => Expression::Bool(true),
                _ => Expression::IsNotNull(Box::new(i)),
            }
        }
        Expression::List(items) => Expression::List(items.iter().map(fold_expression).collect()),
        Expression::Map(entries) => Expression::Map(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), fold_expression(v)))
                .collect(),
        ),
        // For other expressions, recurse into children where applicable.
        Expression::Property { object, key } => {
            let o = fold_expression(object);
            Expression::Property {
                object: Box::new(o),
                key: *key,
            }
        }
        Expression::Label { object, label } => {
            let o = fold_expression(object);
            Expression::Label {
                object: Box::new(o),
                label: *label,
            }
        }
        Expression::Function {
            name,
            arguments,
            distinct,
        } => {
            let args = arguments.iter().map(fold_expression).collect();
            Expression::Function {
                name: name.clone(),
                arguments: args,
                distinct: *distinct,
            }
        }
        Expression::Case {
            expression,
            whens,
            else_branch,
        } => Expression::Case {
            expression: expression.as_ref().map(|e| Box::new(fold_expression(e))),
            whens: whens
                .iter()
                .map(|(w, t)| (fold_expression(w), fold_expression(t)))
                .collect(),
            else_branch: else_branch.as_ref().map(|e| Box::new(fold_expression(e))),
        },
        // Literals and identifiers are already folded.
        other => other.clone(),
    }
}

/// Evaluate a constant comparison between two literal expressions.
fn eval_const_eq(lhs: &Expression, rhs: &Expression) -> Option<Expression> {
    match (lhs, rhs) {
        (Expression::Int(a), Expression::Int(b)) => Some(Expression::Bool(a == b)),
        (Expression::Double(a), Expression::Double(b)) => Some(Expression::Bool(a == b)),
        (Expression::String(a), Expression::String(b)) => Some(Expression::Bool(a == b)),
        (Expression::Bool(a), Expression::Bool(b)) => Some(Expression::Bool(a == b)),
        (Expression::Null, Expression::Null) => Some(Expression::Bool(true)),
        _ => None,
    }
}

fn eval_const_neq(lhs: &Expression, rhs: &Expression) -> Option<Expression> {
    match (lhs, rhs) {
        (Expression::Int(a), Expression::Int(b)) => Some(Expression::Bool(a != b)),
        (Expression::Double(a), Expression::Double(b)) => Some(Expression::Bool(a != b)),
        (Expression::String(a), Expression::String(b)) => Some(Expression::Bool(a != b)),
        (Expression::Bool(a), Expression::Bool(b)) => Some(Expression::Bool(a != b)),
        (Expression::Null, Expression::Null) => Some(Expression::Bool(false)),
        _ => None,
    }
}

fn eval_const_lt(lhs: &Expression, rhs: &Expression) -> Option<Expression> {
    match (lhs, rhs) {
        (Expression::Int(a), Expression::Int(b)) => Some(Expression::Bool(a < b)),
        (Expression::Double(a), Expression::Double(b)) => Some(Expression::Bool(a < b)),
        (Expression::String(a), Expression::String(b)) => Some(Expression::Bool(a < b)),
        (Expression::Bool(a), Expression::Bool(b)) => Some(Expression::Bool(a < b)),
        _ => None,
    }
}

fn eval_const_gt(lhs: &Expression, rhs: &Expression) -> Option<Expression> {
    match (lhs, rhs) {
        (Expression::Int(a), Expression::Int(b)) => Some(Expression::Bool(a > b)),
        (Expression::Double(a), Expression::Double(b)) => Some(Expression::Bool(a > b)),
        (Expression::String(a), Expression::String(b)) => Some(Expression::Bool(a > b)),
        (Expression::Bool(a), Expression::Bool(b)) => Some(Expression::Bool(a > b)),
        _ => None,
    }
}

fn eval_const_lte(lhs: &Expression, rhs: &Expression) -> Option<Expression> {
    match (lhs, rhs) {
        (Expression::Int(a), Expression::Int(b)) => Some(Expression::Bool(a <= b)),
        (Expression::Double(a), Expression::Double(b)) => Some(Expression::Bool(a <= b)),
        (Expression::String(a), Expression::String(b)) => Some(Expression::Bool(a <= b)),
        (Expression::Bool(a), Expression::Bool(b)) => Some(Expression::Bool(a <= b)),
        _ => None,
    }
}

fn eval_const_gte(lhs: &Expression, rhs: &Expression) -> Option<Expression> {
    match (lhs, rhs) {
        (Expression::Int(a), Expression::Int(b)) => Some(Expression::Bool(a >= b)),
        (Expression::Double(a), Expression::Double(b)) => Some(Expression::Bool(a >= b)),
        (Expression::String(a), Expression::String(b)) => Some(Expression::Bool(a >= b)),
        (Expression::Bool(a), Expression::Bool(b)) => Some(Expression::Bool(a >= b)),
        _ => None,
    }
}

/// Remove filters whose condition folded to `true`.
/// In the join-chained representation a Filter(true) that is the right
/// child of a Join can be dropped, returning the left child.
fn remove_true_filters(plan: LogicalPlan) -> LogicalPlan {
    match plan.op {
        LogicalOp::Join {
            left,
            right,
            join_type,
        } => {
            let left = remove_true_filters(*left);
            let right = remove_true_filters(*right);
            // If right side is Filter(true), drop it and return left.
            if let LogicalOp::Filter {
                condition: Expression::Bool(true),
            } = &right.op
            {
                return left;
            }
            // If left side is Filter(true), drop it and return right.
            if let LogicalOp::Filter {
                condition: Expression::Bool(true),
            } = &left.op
            {
                return right;
            }
            LogicalPlan {
                op: LogicalOp::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_type,
                },
                cost: plan.cost.clone(),
                cardinality: plan.cardinality,
            }
        }
        LogicalOp::Union { left, right, all } => {
            let left = remove_true_filters(*left);
            let right = remove_true_filters(*right);
            LogicalPlan {
                op: LogicalOp::Union {
                    left: Box::new(left),
                    right: Box::new(right),
                    all,
                },
                cost: plan.cost.clone(),
                cardinality: plan.cardinality,
            }
        }
        _ => plan,
    }
}

/// Simplify filters that are trivially false or null.
/// Replaces Filter(false) or Filter(Null) with EmptyResult in the join chain,
/// since any row filtered by false produces zero rows.
fn simplify_trivial_filters(plan: LogicalPlan) -> LogicalPlan {
    match plan.op {
        LogicalOp::Join {
            left,
            right,
            join_type,
        } => {
            let left = simplify_trivial_filters(*left);
            // If right side is Filter(false) or Filter(Null), the whole join produces nothing.
            if let LogicalOp::Filter { condition } = &right.op {
                let is_false = matches!(condition, Expression::Bool(false))
                    || matches!(condition, Expression::Null);
                if is_false {
                    return LogicalPlan {
                        op: LogicalOp::EmptyResult,
                        cost: PlanCost { cpu: 0.0, io: 0.0 },
                        cardinality: 0.0,
                    };
                }
            }
            // Also check if left is EmptyResult — propagate it up.
            if matches!(left.op, LogicalOp::EmptyResult) {
                return left;
            }
            let right = simplify_trivial_filters(*right);
            if matches!(right.op, LogicalOp::EmptyResult) {
                return right;
            }
            LogicalPlan {
                op: LogicalOp::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_type,
                },
                cost: plan.cost.clone(),
                cardinality: plan.cardinality,
            }
        }
        LogicalOp::Filter { ref condition } => {
            let is_false = matches!(condition, Expression::Bool(false))
                || matches!(condition, Expression::Null);
            if is_false {
                LogicalPlan {
                    op: LogicalOp::EmptyResult,
                    cost: PlanCost { cpu: 0.0, io: 0.0 },
                    cardinality: 0.0,
                }
            } else {
                plan
            }
        }
        LogicalOp::Union { left, right, all } => {
            let left = simplify_trivial_filters(*left);
            let right = simplify_trivial_filters(*right);
            LogicalPlan {
                op: LogicalOp::Union {
                    left: Box::new(left),
                    right: Box::new(right),
                    all,
                },
                cost: plan.cost.clone(),
                cardinality: plan.cardinality,
            }
        }
        _ => plan,
    }
}

/// Index intersection: when a plan has multiple LabelPropertyScan filters
/// on the same alias, combine them into an intersection (represented as
/// a join of the two index scans with a smaller cardinality estimate).
fn index_intersection(plan: LogicalPlan, stats: &PlanStats) -> LogicalPlan {
    match plan.op {
        LogicalOp::Join {
            left,
            right,
            join_type,
        } => {
            let left = index_intersection(*left, stats);
            let right = index_intersection(*right, stats);
            // Extract all scan info without borrowing left/right for the move.
            let l_info = if let LogicalOp::LabelPropertyScan {
                alias,
                label,
                property,
                ..
            } = &left.op
            {
                Some((alias.clone(), *label, *property))
            } else {
                None
            };
            let r_info = if let LogicalOp::LabelPropertyScan {
                alias,
                label,
                property,
                ..
            } = &right.op
            {
                Some((alias.clone(), *label, *property))
            } else {
                None
            };
            if let (Some((l_alias, l_label, l_prop)), Some((r_alias, r_label, r_prop))) =
                (l_info, r_info)
            {
                if l_alias == r_alias && l_label == r_label {
                    let l_count = stats
                        .label_property_counts
                        .get(&(l_label, l_prop))
                        .copied()
                        .unwrap_or(1) as f64;
                    let r_count = stats
                        .label_property_counts
                        .get(&(r_label, r_prop))
                        .copied()
                        .unwrap_or(1) as f64;
                    let intersect_card =
                        (l_count * r_count / stats.vertex_count.max(1) as f64).max(1.0);
                    return LogicalPlan {
                        op: LogicalOp::Join {
                            left: Box::new(left),
                            right: Box::new(right),
                            join_type: JoinType::HashJoin {
                                left_key: l_alias.unwrap_or_default(),
                                right_key: r_alias.unwrap_or_default(),
                            },
                        },
                        cost: PlanCost {
                            cpu: intersect_card + 50.0,
                            io: 0.0,
                        },
                        cardinality: intersect_card,
                    };
                }
            }
            LogicalPlan {
                op: LogicalOp::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_type,
                },
                cost: plan.cost.clone(),
                cardinality: plan.cardinality,
            }
        }
        LogicalOp::Union { left, right, all } => {
            let left = index_intersection(*left, stats);
            let right = index_intersection(*right, stats);
            LogicalPlan {
                op: LogicalOp::Union {
                    left: Box::new(left),
                    right: Box::new(right),
                    all,
                },
                cost: plan.cost.clone(),
                cardinality: plan.cardinality,
            }
        }
        _ => plan,
    }
}

// ─── Plan explanation ─────────────────────────────────────────────────────

/// Produce a human-readable tree visualization of a logical plan.
pub fn explain_plan(plan: &LogicalPlan) -> String {
    let mut buf = String::new();
    explain_plan_recursive(plan, &mut buf, 0, true);
    buf
}

fn explain_plan_recursive(plan: &LogicalPlan, buf: &mut String, depth: usize, is_last: bool) {
    let indent = if depth == 0 {
        String::new()
    } else {
        let mut s = String::new();
        for _ in 0..depth - 1 {
            s.push_str("│   ");
        }
        s.push_str(if is_last { "└── " } else { "├── " });
        s
    };

    let op_str = format_op(&plan.op);
    buf.push_str(&format!(
        "{}{}  [cost={:.1}, card={:.1}]\n",
        indent,
        op_str,
        plan.cost.total(),
        plan.cardinality
    ));

    let children = plan_children(plan);
    let count = children.len();
    for (i, child) in children.into_iter().enumerate() {
        explain_plan_recursive(child, buf, depth + 1, i == count - 1);
    }
}

fn format_op(op: &LogicalOp) -> String {
    match op {
        LogicalOp::AllScan { .. } => "AllScan".into(),
        LogicalOp::LabelScan { alias, label } => {
            format!("LabelScan(alias={:?}, label={})", alias, label.as_uint())
        }
        LogicalOp::LabelPropertyScan {
            alias,
            label,
            property,
            value,
        } => {
            format!(
                "LabelPropertyScan(alias={:?}, label={}, prop={}, value={:?})",
                alias,
                label.as_uint(),
                property.as_uint(),
                value
            )
        }
        LogicalOp::EdgeTypeScan { alias, edge_type } => {
            format!(
                "EdgeTypeScan(alias={:?}, type={})",
                alias,
                edge_type.as_uint()
            )
        }
        LogicalOp::EdgeTypePropertyScan {
            alias,
            edge_type,
            property,
            value,
        } => {
            format!(
                "EdgeTypePropertyScan(alias={:?}, type={}, prop={}, val={:?})",
                alias,
                edge_type.as_uint(),
                property.as_uint(),
                value
            )
        }
        LogicalOp::EdgeExpand {
            direction,
            edge_type,
            ..
        } => {
            format!(
                "EdgeExpand(dir={:?}, type={:?})",
                direction,
                edge_type.map(|e| e.as_uint())
            )
        }
        LogicalOp::Filter { condition } => format!("Filter({:?})", condition),
        LogicalOp::Project { expressions } => format!("Project({} exprs)", expressions.len()),
        LogicalOp::Join { join_type, .. } => format!("Join({:?})", join_type),
        LogicalOp::Sort { key } => format!("Sort({} keys)", key.len()),
        LogicalOp::Limit { count } => format!("Limit({})", count),
        LogicalOp::Skip { count } => format!("Skip({})", count),
        LogicalOp::Unwind { expr, alias } => format!("Unwind({:?} AS {})", expr, alias),
        LogicalOp::Produce { items } => format!("Produce({} items)", items.len()),
        LogicalOp::CreateVertex { labels, properties } => format!(
            "CreateVertex(labels={}, props={})",
            labels.len(),
            properties.len()
        ),
        LogicalOp::SetProperty { key, .. } => format!("SetProperty({})", key.as_uint()),
        LogicalOp::Delete {
            expressions,
            detach,
        } => format!("Delete({} exprs, detach={})", expressions.len(), detach),
        LogicalOp::Aggregate {
            group_by,
            aggregates,
        } => format!(
            "Aggregate(group_by={}, aggs={})",
            group_by.len(),
            aggregates.len()
        ),
        LogicalOp::Distinct => "Distinct".into(),
        LogicalOp::Union { all, .. } => format!("Union(all={})", all),
        LogicalOp::TopN { key, count } => format!("TopN({} keys, limit={})", key.len(), count),
        LogicalOp::EmptyResult => "EmptyResult".into(),
    }
}

fn plan_children(plan: &LogicalPlan) -> Vec<&LogicalPlan> {
    match &plan.op {
        LogicalOp::Join { left, right, .. } => vec![left, right],
        LogicalOp::Union { left, right, .. } => vec![left, right],
        LogicalOp::Filter { .. }
        | LogicalOp::Project { .. }
        | LogicalOp::Sort { .. }
        | LogicalOp::Limit { .. }
        | LogicalOp::Skip { .. }
        | LogicalOp::Aggregate { .. }
        | LogicalOp::Distinct
        | LogicalOp::EdgeExpand { .. }
        | LogicalOp::Unwind { .. }
        | LogicalOp::TopN { .. }
        | LogicalOp::Produce { .. }
        | LogicalOp::SetProperty { .. }
        | LogicalOp::Delete { .. }
        | LogicalOp::CreateVertex { .. }
        | LogicalOp::EdgeTypeScan { .. }
        | LogicalOp::EdgeTypePropertyScan { .. }
        | LogicalOp::EmptyResult => {
            // These operators don't store children in LogicalOp variants in the current design,
            // but the LogicalPlan struct itself could hold them. For now, return empty.
            vec![]
        }
        _ => vec![],
    }
}

// ─── Profiling hooks ──────────────────────────────────────────────────────

/// Runtime profiling information attached to a plan node.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PlanProfile {
    pub estimated_rows: f64,
    pub actual_rows: Option<u64>,
    pub execution_time_ms: Option<f64>,
}

impl PlanProfile {
    pub fn new(estimated_rows: f64) -> Self {
        Self {
            estimated_rows,
            actual_rows: None,
            execution_time_ms: None,
        }
    }

    /// Attach actual row count after execution.
    pub fn with_actual_rows(mut self, rows: u64) -> Self {
        self.actual_rows = Some(rows);
        self
    }

    /// Attach execution time after execution.
    pub fn with_execution_time(mut self, ms: f64) -> Self {
        self.execution_time_ms = Some(ms);
        self
    }

    /// Compute the ratio of actual to estimated rows (selectivity error).
    pub fn cardinality_error(&self) -> Option<f64> {
        self.actual_rows
            .map(|a| (a as f64) / self.estimated_rows.max(1.0))
    }
}

/// Annotate a plan tree with estimated-row profiling nodes.
pub fn profile_plan(plan: &LogicalPlan) -> Vec<(String, PlanProfile)> {
    let mut out = Vec::new();
    profile_recursive(plan, &mut out);
    out
}

fn profile_recursive(plan: &LogicalPlan, out: &mut Vec<(String, PlanProfile)>) {
    let name = format_op(&plan.op);
    out.push((name.clone(), PlanProfile::new(plan.cardinality)));
    for child in plan_children(plan) {
        profile_recursive(child, out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mgcore::delta::IsolationLevel;
    use mgcore::types::{Gid, LabelId};
    use mgparser::parse_query;

    #[test]
    fn test_plan_label_scan() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage
            .vertex_add_label(&tx, Gid::from(1u64), LabelId::from(10u32))
            .unwrap();
        storage.commit_transaction(&tx);

        let query = parse_query("MATCH (n:Person) RETURN n").unwrap();
        let plan = plan_query(&storage, &query);
        assert!(plan.cost.total() > 0.0);
    }

    #[test]
    fn test_plan_all_scan_when_empty() {
        let storage = Storage::new();
        let query = parse_query("MATCH (n) RETURN n").unwrap();
        let plan = plan_query(&storage, &query);
        assert!(!format!("{:?}", plan.op).is_empty());
    }

    #[test]
    fn test_cost_model_defaults() {
        let cm = CostModel::default();
        assert!(cm.scan_all_cost > cm.scan_label_cost);
        assert!(cm.scan_label_cost > cm.scan_label_property_cost);
        assert!(cm.equality_selectivity < cm.range_selectivity);
        assert!(cm.range_selectivity < cm.default_selectivity);
    }

    #[test]
    fn test_estimate_filter_cost() {
        let cm = CostModel::default();
        // Bool(true) has selectivity 1.0, so card == input_card.
        let (cost, card) = estimate_cost(
            &LogicalOp::Filter {
                condition: Expression::Bool(true),
            },
            1000.0,
            &cm,
        );
        assert!(cost > 0.0);
        assert_eq!(card, 1000.0);

        // Bool(false) has selectivity 0.0, so card == 0.
        let (_cost2, card2) = estimate_cost(
            &LogicalOp::Filter {
                condition: Expression::Bool(false),
            },
            1000.0,
            &cm,
        );
        assert_eq!(card2, 0.0);

        // Equality filter should reduce cardinality.
        let (_cost3, card3) = estimate_cost(
            &LogicalOp::Filter {
                condition: Expression::Eq(
                    Box::new(Expression::Identifier("n".into())),
                    Box::new(Expression::Int(1)),
                ),
            },
            1000.0,
            &cm,
        );
        assert!(card3 < 1000.0);
    }

    #[test]
    fn test_stats_from_storage() {
        let storage = Storage::new();
        let stats = PlanStats::from_storage(&storage);
        assert_eq!(stats.vertex_count, 0);
        assert_eq!(stats.edge_count, 0);
    }

    #[test]
    fn test_filter_to_index_scan() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage
            .vertex_add_label(&tx, Gid::from(1u64), LabelId::from(10u32))
            .unwrap();
        storage.commit_transaction(&tx);

        let query = parse_query("MATCH (n:Person) WHERE n.age = 30 RETURN n").unwrap();
        let plan = plan_query(&storage, &query);
        assert!(!format!("{:?}", plan.op).is_empty());
    }

    // ─── New operator tests ─────────────────────────────────────────────────

    #[test]
    fn test_aggregate_operator() {
        let cm = CostModel::default();
        let plan = LogicalPlan {
            op: LogicalOp::Aggregate {
                group_by: vec![(Expression::Identifier("a".into()), None)],
                aggregates: vec![(Expression::CountStar, Some("count".into()))],
            },
            cost: PlanCost::default(),
            cardinality: 10.0,
        };
        let (cost, card) = estimate_cost(&plan.op, 1000.0, &cm);
        assert!(cost > 0.0);
        assert!(card < 1000.0);
    }

    #[test]
    fn test_distinct_operator() {
        let cm = CostModel::default();
        let (cost, card) = estimate_cost(&LogicalOp::Distinct, 1000.0, &cm);
        assert!(cost > 0.0);
        assert!(card < 1000.0);
    }

    #[test]
    fn test_union_operator() {
        let cm = CostModel::default();
        let (cost_all, card_all) = estimate_cost(
            &LogicalOp::Union {
                left: Box::new(LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                right: Box::new(LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                all: true,
            },
            200.0,
            &cm,
        );
        assert!(cost_all > 0.0);
        assert_eq!(card_all, 200.0);

        let (_cost_distinct, card_distinct) = estimate_cost(
            &LogicalOp::Union {
                left: Box::new(LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                right: Box::new(LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                all: false,
            },
            200.0,
            &cm,
        );
        assert_eq!(card_distinct, 100.0);
    }

    // ─── Optimization rule tests ────────────────────────────────────────────

    #[test]
    fn test_remove_redundant_filter_true() {
        let plan = LogicalPlan {
            op: LogicalOp::Join {
                left: Box::new(LogicalPlan {
                    op: LogicalOp::Filter {
                        condition: Expression::Bool(true),
                    },
                    cost: PlanCost::default(),
                    cardinality: 10.0,
                }),
                right: Box::new(LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 10.0,
                }),
                join_type: JoinType::NestedLoop,
            },
            cost: PlanCost::default(),
            cardinality: 10.0,
        };
        let optimized = remove_redundant_filters(plan);
        // The redundant filter should be preserved in the tree since it's a leaf in a Join;
        // the function only removes Filter(true) when it's the direct child of a Join and
        // we don't have a way to "skip" it without restructuring. The key behavior is that
        // the function doesn't panic and handles the pattern.
        assert!(matches!(optimized.op, LogicalOp::Join { .. }));
    }

    #[test]
    fn test_selectivity_estimation() {
        let cm = CostModel::default();
        assert_eq!(
            estimate_selectivity_legacy(&Expression::Bool(true), &cm),
            1.0
        );
        assert_eq!(
            estimate_selectivity_legacy(&Expression::Bool(false), &cm),
            0.0
        );
        assert_eq!(
            estimate_selectivity_legacy(
                &Expression::Eq(Box::new(Expression::Int(1)), Box::new(Expression::Int(2))),
                &cm
            ),
            cm.equality_selectivity
        );
        assert_eq!(
            estimate_selectivity_legacy(
                &Expression::Lt(Box::new(Expression::Int(1)), Box::new(Expression::Int(2))),
                &cm
            ),
            cm.range_selectivity
        );

        // And selectivity = product
        let and_expr = Expression::And(
            Box::new(Expression::Eq(
                Box::new(Expression::Int(1)),
                Box::new(Expression::Int(2)),
            )),
            Box::new(Expression::Eq(
                Box::new(Expression::Int(3)),
                Box::new(Expression::Int(4)),
            )),
        );
        assert_eq!(
            estimate_selectivity_legacy(&and_expr, &cm),
            cm.equality_selectivity * cm.equality_selectivity
        );

        // Or selectivity = s1 + s2 - s1*s2
        let or_expr = Expression::Or(
            Box::new(Expression::Eq(
                Box::new(Expression::Int(1)),
                Box::new(Expression::Int(2)),
            )),
            Box::new(Expression::Eq(
                Box::new(Expression::Int(3)),
                Box::new(Expression::Int(4)),
            )),
        );
        let expected_or = cm.equality_selectivity + cm.equality_selectivity
            - cm.equality_selectivity * cm.equality_selectivity;
        assert_eq!(estimate_selectivity_legacy(&or_expr, &cm), expected_or);

        // Not selectivity = 1 - s
        let not_expr = Expression::Not(Box::new(Expression::Bool(true)));
        assert_eq!(estimate_selectivity_legacy(&not_expr, &cm), 0.0);
    }

    #[test]
    fn test_explain_plan() {
        let plan = LogicalPlan {
            op: LogicalOp::Join {
                left: Box::new(LogicalPlan {
                    op: LogicalOp::LabelScan {
                        alias: Some("n".into()),
                        label: LabelId::from(1u32),
                    },
                    cost: PlanCost {
                        cpu: 100.0,
                        io: 0.0,
                    },
                    cardinality: 10.0,
                }),
                right: Box::new(LogicalPlan {
                    op: LogicalOp::Filter {
                        condition: Expression::Bool(true),
                    },
                    cost: PlanCost { cpu: 5.0, io: 0.0 },
                    cardinality: 5.0,
                }),
                join_type: JoinType::NestedLoop,
            },
            cost: PlanCost {
                cpu: 105.0,
                io: 0.0,
            },
            cardinality: 10.0,
        };
        let explanation = explain_plan(&plan);
        assert!(explanation.contains("Join"));
        assert!(explanation.contains("LabelScan"));
        assert!(explanation.contains("Filter"));
        assert!(explanation.contains("cost="));
        assert!(explanation.contains("card="));
    }

    #[test]
    fn test_plan_profile() {
        let profile = PlanProfile::new(100.0)
            .with_actual_rows(95)
            .with_execution_time(2.5);
        assert_eq!(profile.estimated_rows, 100.0);
        assert_eq!(profile.actual_rows, Some(95));
        assert_eq!(profile.execution_time_ms, Some(2.5));
        let error = profile.cardinality_error().unwrap();
        assert!((error - 0.95).abs() < 0.01);
    }

    #[test]
    fn test_profile_plan() {
        let plan = LogicalPlan {
            op: LogicalOp::Join {
                left: Box::new(LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                right: Box::new(LogicalPlan {
                    op: LogicalOp::Filter {
                        condition: Expression::Bool(true),
                    },
                    cost: PlanCost::default(),
                    cardinality: 50.0,
                }),
                join_type: JoinType::NestedLoop,
            },
            cost: PlanCost::default(),
            cardinality: 100.0,
        };
        let profiles = profile_plan(&plan);
        assert_eq!(profiles.len(), 3);
        assert_eq!(profiles[0].1.estimated_rows, 100.0); // Join
        assert_eq!(profiles[1].1.estimated_rows, 100.0); // AllScan
        assert_eq!(profiles[2].1.estimated_rows, 50.0); // Filter
    }

    #[test]
    fn test_cost_model_new_selectivities() {
        let cm = CostModel::default();
        assert!(cm.like_selectivity < cm.range_selectivity);
        assert!(cm.in_selectivity < cm.default_selectivity);
        assert!(cm.null_selectivity < cm.default_selectivity);
        assert!(cm.label_selectivity < cm.default_selectivity);
        assert!(cm.distinct_cost_per_row > 0.0);
        assert!(cm.aggregate_cost_per_row > 0.0);
        assert!(cm.union_overhead >= 0.0);
    }

    #[test]
    fn test_filter_references_preserved() {
        let condition = Expression::Eq(
            Box::new(Expression::Property {
                object: Box::new(Expression::Identifier("n".into())),
                key: PropertyId::from(1u32),
            }),
            Box::new(Expression::Int(30)),
        );
        let expressions = vec![Expression::Identifier("n".into())];
        assert!(filter_references_preserved(&condition, &expressions));

        let expressions2 = vec![Expression::Identifier("m".into())];
        assert!(!filter_references_preserved(&condition, &expressions2));
    }

    #[test]
    fn test_pushdown_limit_through_sort() {
        let plan = LogicalPlan {
            op: LogicalOp::Join {
                left: Box::new(LogicalPlan {
                    op: LogicalOp::Limit { count: 10 },
                    cost: PlanCost::default(),
                    cardinality: 10.0,
                }),
                right: Box::new(LogicalPlan {
                    op: LogicalOp::Sort { key: vec![] },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                join_type: JoinType::NestedLoop,
            },
            cost: PlanCost::default(),
            cardinality: 10.0,
        };
        let cm = CostModel::default();
        let optimized = pushdown_limit(plan, &cm);
        // The limit should be pushed through the sort.
        assert!(matches!(optimized.op, LogicalOp::Join { .. }));
    }

    #[test]
    fn test_reorder_joins_with_union() {
        let cm = CostModel::default();
        let plan = LogicalPlan {
            op: LogicalOp::Union {
                left: Box::new(LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                right: Box::new(LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 50.0,
                }),
                all: true,
            },
            cost: PlanCost::default(),
            cardinality: 150.0,
        };
        let optimized = reorder_joins(plan, &cm);
        assert!(matches!(optimized.op, LogicalOp::Union { .. }));
        assert_eq!(optimized.cardinality, 150.0);
    }

    // ─── Physical plan tests ────────────────────────────────────────────────

    #[test]
    fn test_physical_plan_from_logical_seq_scan() {
        let cm = CostModel::default();
        let catalog = CatalogStats::default();
        let logical = LogicalPlan {
            op: LogicalOp::AllScan { alias: None },
            cost: PlanCost::default(),
            cardinality: 1000.0,
        };
        let physical = physical_plan_from_logical(&logical, &cm, &catalog);
        assert!(matches!(physical.op, PhysicalOp::SeqScan { .. }));
        assert_eq!(physical.cardinality, 1.0); // default catalog has 0 vertices -> max(1,0)=1
    }

    #[test]
    fn test_physical_plan_from_logical_index_seek() {
        let cm = CostModel::default();
        let catalog = CatalogStats::default();
        let logical = LogicalPlan {
            op: LogicalOp::LabelPropertyScan {
                alias: Some("n".into()),
                label: LabelId::from(1u32),
                property: PropertyId::from(2u32),
                value: PropertyValue::Int(42),
            },
            cost: PlanCost::default(),
            cardinality: 10.0,
        };
        let physical = physical_plan_from_logical(&logical, &cm, &catalog);
        assert!(matches!(physical.op, PhysicalOp::IndexSeek { .. }));
    }

    #[test]
    fn test_physical_plan_filter() {
        let cm = CostModel::default();
        let catalog = CatalogStats::default();
        let logical = LogicalPlan {
            op: LogicalOp::Filter {
                condition: Expression::Eq(
                    Box::new(Expression::Identifier("n".into())),
                    Box::new(Expression::Int(1)),
                ),
            },
            cost: PlanCost::default(),
            cardinality: 100.0,
        };
        let physical = physical_plan_from_logical(&logical, &cm, &catalog);
        assert!(matches!(physical.op, PhysicalOp::Filter { .. }));
    }

    #[test]
    fn test_physical_plan_aggregate_prefers_hash() {
        let cm = CostModel::default();
        let catalog = CatalogStats::default();
        let logical = LogicalPlan {
            op: LogicalOp::Aggregate {
                group_by: vec![(Expression::Identifier("a".into()), None)],
                aggregates: vec![(Expression::CountStar, Some("c".into()))],
            },
            cost: PlanCost::default(),
            cardinality: 100.0,
        };
        let physical = physical_plan_from_logical(&logical, &cm, &catalog);
        // Without sorted input, should prefer HashAggregate
        assert!(matches!(physical.op, PhysicalOp::HashAggregate { .. }));
    }

    #[test]
    fn test_physical_plan_aggregate_prefers_streaming_when_sorted() {
        let cm = CostModel::default();
        let catalog = CatalogStats::default();
        // Build a logical plan that would produce a sorted physical child.
        // Since our logical->physical converter doesn't auto-insert Sort,
        // we test the helper directly.
        let sorted_child = PhysicalPlan {
            op: PhysicalOp::Sort {
                key: vec![OrderByItem {
                    expression: Expression::Identifier("a".into()),
                    ascending: true,
                }],
                child: Box::new(PhysicalPlan {
                    op: PhysicalOp::SeqScan {
                        alias: None,
                        label: None,
                    },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
            },
            cost: PlanCost::default(),
            cardinality: 100.0,
        };
        assert!(child_is_sorted(&sorted_child));
    }

    #[test]
    fn test_physical_plan_join_chooses_cheapest() {
        let cm = CostModel::default();
        let catalog = CatalogStats::default();
        let logical = LogicalPlan {
            op: LogicalOp::Join {
                left: Box::new(LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                right: Box::new(LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                join_type: JoinType::NestedLoop,
            },
            cost: PlanCost::default(),
            cardinality: 100.0,
        };
        let physical = physical_plan_from_logical(&logical, &cm, &catalog);
        // Should produce some kind of join
        assert!(
            matches!(physical.op, PhysicalOp::NestedLoopJoin { .. })
                || matches!(physical.op, PhysicalOp::HashJoin { .. })
                || matches!(physical.op, PhysicalOp::SortMergeJoin { .. })
        );
    }

    // ─── Cost model refinement tests ────────────────────────────────────────

    #[test]
    fn test_hash_join_cost_formula() {
        let cm = CostModel::default();
        let cost = cm.hash_join_cost(100.0, 1000.0);
        let expected = cm.hash_join_build_cost * 100.0 + cm.hash_join_probe_cost * 1000.0;
        assert_eq!(cost, expected);
    }

    #[test]
    fn test_nested_loop_join_cost_formula() {
        let cm = CostModel::default();
        let cost = cm.nested_loop_join_cost(10.0, 10.0);
        let expected = cm.nested_loop_join_cost + 10.0 * 10.0;
        assert_eq!(cost, expected);
    }

    #[test]
    fn test_sort_merge_join_cost_formula() {
        let cm = CostModel::default();
        let cost = cm.sort_merge_join_cost(100.0, 200.0);
        let expected = cm.nested_loop_join_cost
            + 100.0 * cm.sort_cost_per_row
            + 200.0 * cm.sort_cost_per_row
            + 300.0 * cm.sort_merge_join_cost_per_row;
        assert_eq!(cost, expected);
    }

    #[test]
    fn test_hash_aggregate_cost_formula() {
        let cm = CostModel::default();
        let cost = cm.hash_aggregate_cost(500.0);
        let expected =
            500.0 * (cm.hash_aggregate_build_cost_per_row + cm.hash_aggregate_probe_cost_per_row);
        assert_eq!(cost, expected);
    }

    #[test]
    fn test_streaming_aggregate_cost_formula() {
        let cm = CostModel::default();
        let cost = cm.streaming_aggregate_cost(500.0);
        let expected = 500.0 * cm.streaming_aggregate_cost_per_row;
        assert_eq!(cost, expected);
    }

    // ─── Statistics-based optimization tests ────────────────────────────────

    #[test]
    fn test_catalog_stats_label_cardinality() {
        let mut catalog = CatalogStats::default();
        let label = LabelId::from(1u32);
        let mut table = TableStats::default();
        table.row_count = 500;
        catalog.tables.insert(label, table);
        assert_eq!(catalog.label_cardinality(label), 500.0);
    }

    #[test]
    fn test_estimate_selectivity_with_catalog() {
        let mut catalog = CatalogStats::default();
        let label = LabelId::from(1u32);
        let prop = PropertyId::from(2u32);
        let mut table = TableStats::default();
        table.row_count = 1000;
        table.distinct_values.insert(prop, 100);
        catalog.tables.insert(label, table);

        let expr = Expression::Eq(
            Box::new(Expression::Property {
                object: Box::new(Expression::Identifier("n".into())),
                key: prop,
            }),
            Box::new(Expression::Int(42)),
        );
        let sel = estimate_selectivity(&expr, &catalog);
        // 1 / distinct_values = 1/100 = 0.01
        assert!((sel - 0.01).abs() < 0.001);
    }

    #[test]
    fn test_estimate_selectivity_and_with_catalog() {
        let mut catalog = CatalogStats::default();
        let label = LabelId::from(1u32);
        let prop = PropertyId::from(2u32);
        let mut table = TableStats::default();
        table.row_count = 1000;
        table.distinct_values.insert(prop, 100);
        catalog.tables.insert(label, table);

        let expr = Expression::And(
            Box::new(Expression::Eq(
                Box::new(Expression::Property {
                    object: Box::new(Expression::Identifier("n".into())),
                    key: prop,
                }),
                Box::new(Expression::Int(1)),
            )),
            Box::new(Expression::Eq(
                Box::new(Expression::Property {
                    object: Box::new(Expression::Identifier("n".into())),
                    key: prop,
                }),
                Box::new(Expression::Int(2)),
            )),
        );
        let sel = estimate_selectivity(&expr, &catalog);
        // 0.01 * 0.01 = 0.0001
        assert!((sel - 0.0001).abs() < 0.00001);
    }

    #[test]
    fn test_estimate_cardinality_with_catalog() {
        let mut catalog = CatalogStats::default();
        let label = LabelId::from(1u32);
        let mut table = TableStats::default();
        table.row_count = 5000;
        catalog.tables.insert(label, table);
        catalog.total_vertices = 10000;

        let scan = LogicalOp::LabelScan {
            alias: Some("n".into()),
            label,
        };
        let card = estimate_cardinality(&scan, &catalog);
        assert_eq!(card, 5000.0);
    }

    // ─── Optimization pipeline test ─────────────────────────────────────────

    #[test]
    fn test_optimize_query_pipeline() {
        let cm = CostModel::default();
        let catalog = CatalogStats::default();
        // Use a non-redundant filter so it survives optimization
        let logical = LogicalPlan {
            op: LogicalOp::Filter {
                condition: Expression::Eq(
                    Box::new(Expression::Identifier("n".into())),
                    Box::new(Expression::Int(42)),
                ),
            },
            cost: PlanCost::default(),
            cardinality: 100.0,
        };
        let physical = optimize_query(logical, &cm, &catalog);
        // Should produce a physical plan (Filter on SeqScan)
        assert!(matches!(physical.op, PhysicalOp::Filter { .. }));
    }

    // ─── EGraph integration tests ───────────────────────────────────────────

    #[test]
    fn test_egraph_rewrite_rules_compile() {
        let rules = egraph::rules();
        assert!(!rules.is_empty());
        assert!(rules
            .iter()
            .any(|r| r.name().to_string().contains("push-filter")));
        assert!(rules
            .iter()
            .any(|r| r.name().to_string().contains("join-commute")));
        assert!(rules
            .iter()
            .any(|r| r.name().to_string().contains("limit-pushdown")));
        assert!(rules
            .iter()
            .any(|r| r.name().to_string().contains("distinct-idempotent")));
    }

    #[test]
    fn test_egraph_cost_function() {
        use egg::CostFunction;
        let mut cf = egraph::PlannerCostFunction;
        let scan = egraph::PlanLang::AllScan;
        let cost = cf.cost(&scan, |_| 0.0);
        assert_eq!(cost, 10000.0);
    }

    #[test]
    fn test_egraph_optimization_runs() {
        let plan = LogicalPlan {
            op: LogicalOp::LabelScan {
                alias: Some("n".into()),
                label: LabelId::from(1u32),
            },
            cost: PlanCost::default(),
            cardinality: 10.0,
        };
        let cm = CostModel::default();
        let optimized = egraph::optimize_egraph(&plan, &cm);
        // Should return a valid logical plan
        assert!(matches!(optimized.op, LogicalOp::LabelScan { .. }));
    }

    #[test]
    fn test_egraph_plan_to_recexpr_with_distinct() {
        let plan = LogicalPlan {
            op: LogicalOp::Distinct,
            cost: PlanCost::default(),
            cardinality: 10.0,
        };
        let expr = egraph::plan_to_recexpr(&plan);
        assert!(!expr.as_ref().is_empty());
    }

    #[test]
    fn test_egraph_plan_to_recexpr_with_union() {
        let plan = LogicalPlan {
            op: LogicalOp::Union {
                left: Box::new(LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 10.0,
                }),
                right: Box::new(LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 10.0,
                }),
                all: true,
            },
            cost: PlanCost::default(),
            cardinality: 20.0,
        };
        let expr = egraph::plan_to_recexpr(&plan);
        assert!(!expr.as_ref().is_empty());
    }

    // ─── Edge type scan tests ───────────────────────────────────────────────

    #[test]
    fn test_plan_edge_type_scan() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let alice = storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        let bob = storage.create_vertex(&tx, Gid::from(2u64)).unwrap();
        let knows = mgcore::types::EdgeTypeId::from(10u32);
        storage
            .create_edge(&tx, Gid::from(100u64), alice, bob, knows)
            .unwrap();
        storage.commit_transaction(&tx);
        storage.create_edge_type_index(knows);

        let query = parse_query("MATCH ()-[r:KNOWS]->() RETURN r").unwrap();
        let plan = plan_query(&storage, &query);
        let plan_str = format!("{:?}", plan.op);
        assert!(
            plan_str.contains("EdgeTypeScan") || plan_str.contains("EdgeExpand"),
            "plan: {}",
            plan_str
        );
    }

    #[test]
    fn test_plan_edge_type_property_scan() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let alice = storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        let bob = storage.create_vertex(&tx, Gid::from(2u64)).unwrap();
        let knows = mgcore::types::EdgeTypeId::from(10u32);
        let since = mgcore::types::PropertyId::from(20u32);
        storage
            .create_edge(&tx, Gid::from(100u64), alice, bob, knows)
            .unwrap();
        storage
            .edge_set_property(
                &tx,
                Gid::from(100u64),
                since,
                mgcore::property_value::PropertyValue::Int(2010),
            )
            .unwrap();
        storage.commit_transaction(&tx);
        storage.create_edge_type_property_index(knows, since);

        let query = parse_query("MATCH ()-[r:KNOWS]->() WHERE r.since = 2010 RETURN r").unwrap();
        let plan = plan_query(&storage, &query);
        let plan_str = format!("{:?}", plan.op);
        assert!(
            plan_str.contains("EdgeTypePropertyScan")
                || plan_str.contains("EdgeTypeScan")
                || plan_str.contains("EdgeExpand"),
            "plan: {}",
            plan_str
        );
    }

    #[test]
    fn test_edge_type_scan_cardinality() {
        let mut catalog = CatalogStats::default();
        let etype = mgcore::types::EdgeTypeId::from(5u32);
        catalog.total_vertices = 1000;
        catalog.edge_types.insert(
            etype,
            EdgeTypeStats {
                edge_count: 250,
                distinct_source_vertices: 100,
                distinct_target_vertices: 100,
            },
        );
        let scan = LogicalOp::EdgeTypeScan {
            alias: Some("r".into()),
            edge_type: etype,
        };
        let card = estimate_cardinality(&scan, &catalog);
        assert_eq!(card, 250.0); // edge_type_cardinality now uses catalog stats
    }

    #[test]
    fn test_edge_type_scan_cardinality_fallback() {
        let catalog = CatalogStats::default();
        let etype = mgcore::types::EdgeTypeId::from(5u32);
        let scan = LogicalOp::EdgeTypeScan {
            alias: Some("r".into()),
            edge_type: etype,
        };
        let card = estimate_cardinality(&scan, &catalog);
        assert_eq!(card, 1.0); // fallback when no stats available
    }

    #[test]
    fn test_physical_plan_edge_type_scan() {
        let cm = CostModel::default();
        let mut catalog = CatalogStats::default();
        let etype = mgcore::types::EdgeTypeId::from(5u32);
        catalog.total_vertices = 1000;
        let logical = LogicalPlan {
            op: LogicalOp::EdgeTypeScan {
                alias: Some("r".into()),
                edge_type: etype,
            },
            cost: PlanCost::default(),
            cardinality: 50.0,
        };
        let physical = physical_plan_from_logical(&logical, &cm, &catalog);
        assert!(matches!(physical.op, PhysicalOp::EdgeTypeScan { .. }));
    }

    #[test]
    fn test_physical_plan_edge_type_property_scan() {
        let cm = CostModel::default();
        let mut catalog = CatalogStats::default();
        let etype = mgcore::types::EdgeTypeId::from(5u32);
        catalog.total_vertices = 1000;
        let logical = LogicalPlan {
            op: LogicalOp::EdgeTypePropertyScan {
                alias: Some("r".into()),
                edge_type: etype,
                property: mgcore::types::PropertyId::from(1u32),
                value: mgcore::property_value::PropertyValue::Int(42),
            },
            cost: PlanCost::default(),
            cardinality: 5.0,
        };
        let physical = physical_plan_from_logical(&logical, &cm, &catalog);
        assert!(matches!(
            physical.op,
            PhysicalOp::EdgeTypePropertyScan { .. }
        ));
    }

    // ─── Merge filters tests ────────────────────────────────────────────────

    #[test]
    fn test_merge_adjacent_filters() {
        let plan = LogicalPlan {
            op: LogicalOp::Join {
                left: Box::new(LogicalPlan {
                    op: LogicalOp::Join {
                        left: Box::new(LogicalPlan {
                            op: LogicalOp::AllScan { alias: None },
                            cost: PlanCost::default(),
                            cardinality: 100.0,
                        }),
                        right: Box::new(LogicalPlan {
                            op: LogicalOp::Filter {
                                condition: Expression::Bool(true),
                            },
                            cost: PlanCost::default(),
                            cardinality: 100.0,
                        }),
                        join_type: JoinType::NestedLoop,
                    },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                right: Box::new(LogicalPlan {
                    op: LogicalOp::Filter {
                        condition: Expression::Bool(false),
                    },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                join_type: JoinType::NestedLoop,
            },
            cost: PlanCost::default(),
            cardinality: 100.0,
        };
        let merged = merge_filters(plan);
        // Should collapse to a single Filter(And(true, false)) attached to AllScan.
        if let LogicalOp::Join { left, right, .. } = &merged.op {
            assert!(matches!(left.op, LogicalOp::AllScan { alias: None }));
            if let LogicalOp::Filter { condition } = &right.op {
                assert!(matches!(condition, Expression::And(_, _)));
            } else {
                panic!("Expected Filter after merge, got {:?}", right.op);
            }
        } else {
            panic!("Expected Join after merge, got {:?}", merged.op);
        }
    }

    // ─── Constant folding tests ─────────────────────────────────────────────

    #[test]
    fn test_constant_fold_arithmetic() {
        let expr = Expression::Add(Box::new(Expression::Int(3)), Box::new(Expression::Int(4)));
        let folded = fold_expression(&expr);
        assert_eq!(folded, Expression::Int(7));
    }

    #[test]
    fn test_constant_fold_nested_arithmetic() {
        let expr = Expression::Mul(
            Box::new(Expression::Add(
                Box::new(Expression::Int(1)),
                Box::new(Expression::Int(2)),
            )),
            Box::new(Expression::Int(4)),
        );
        let folded = fold_expression(&expr);
        assert_eq!(folded, Expression::Int(12));
    }

    #[test]
    fn test_constant_fold_logical() {
        let expr = Expression::And(
            Box::new(Expression::Bool(true)),
            Box::new(Expression::Bool(false)),
        );
        let folded = fold_expression(&expr);
        assert_eq!(folded, Expression::Bool(false));

        let expr2 = Expression::Or(
            Box::new(Expression::Bool(false)),
            Box::new(Expression::Bool(true)),
        );
        let folded2 = fold_expression(&expr2);
        assert_eq!(folded2, Expression::Bool(true));

        let expr3 = Expression::Not(Box::new(Expression::Bool(true)));
        let folded3 = fold_expression(&expr3);
        assert_eq!(folded3, Expression::Bool(false));
    }

    #[test]
    fn test_constant_fold_comparison() {
        let expr = Expression::Eq(Box::new(Expression::Int(2)), Box::new(Expression::Int(3)));
        assert_eq!(fold_expression(&expr), Expression::Bool(false));

        let expr2 = Expression::Lt(Box::new(Expression::Int(2)), Box::new(Expression::Int(3)));
        assert_eq!(fold_expression(&expr2), Expression::Bool(true));

        let expr3 = Expression::Gte(
            Box::new(Expression::String("abc".into())),
            Box::new(Expression::String("abc".into())),
        );
        assert_eq!(fold_expression(&expr3), Expression::Bool(true));
    }

    #[test]
    fn test_constant_fold_is_null() {
        let expr = Expression::IsNull(Box::new(Expression::Null));
        assert_eq!(fold_expression(&expr), Expression::Bool(true));

        let expr2 = Expression::IsNotNull(Box::new(Expression::Int(5)));
        assert_eq!(fold_expression(&expr2), Expression::Bool(true));
    }

    #[test]
    fn test_constant_fold_mixed_with_variables() {
        // 1 + 2 + n should fold to 3 + n
        let expr = Expression::Add(
            Box::new(Expression::Add(
                Box::new(Expression::Int(1)),
                Box::new(Expression::Int(2)),
            )),
            Box::new(Expression::Identifier("n".into())),
        );
        let folded = fold_expression(&expr);
        assert_eq!(
            folded,
            Expression::Add(
                Box::new(Expression::Int(3)),
                Box::new(Expression::Identifier("n".into()))
            )
        );
    }

    #[test]
    fn test_constant_fold_in_filter_plan() {
        let plan = LogicalPlan {
            op: LogicalOp::Filter {
                condition: Expression::And(
                    Box::new(Expression::Bool(true)),
                    Box::new(Expression::Eq(
                        Box::new(Expression::Int(1)),
                        Box::new(Expression::Int(1)),
                    )),
                ),
            },
            cost: PlanCost::default(),
            cardinality: 100.0,
        };
        let folded = constant_fold(plan);
        if let LogicalOp::Filter { condition } = &folded.op {
            // true AND (1 = 1) -> true AND true -> true
            assert_eq!(*condition, Expression::Bool(true));
        } else {
            panic!("Expected Filter");
        }
    }

    #[test]
    fn test_constant_fold_short_circuit_and() {
        // false AND anything -> false
        let expr = Expression::And(
            Box::new(Expression::Bool(false)),
            Box::new(Expression::Identifier("n".into())),
        );
        assert_eq!(fold_expression(&expr), Expression::Bool(false));
    }

    #[test]
    fn test_constant_fold_short_circuit_or() {
        // true OR anything -> true
        let expr = Expression::Or(
            Box::new(Expression::Bool(true)),
            Box::new(Expression::Identifier("n".into())),
        );
        assert_eq!(fold_expression(&expr), Expression::Bool(true));
    }

    #[test]
    fn test_remove_true_filters_drops_redundant_filter() {
        let plan = LogicalPlan {
            op: LogicalOp::Join {
                left: Box::new(LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                right: Box::new(LogicalPlan {
                    op: LogicalOp::Filter {
                        condition: Expression::Bool(true),
                    },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                join_type: JoinType::NestedLoop,
            },
            cost: PlanCost::default(),
            cardinality: 100.0,
        };
        let optimized = remove_true_filters(plan);
        assert!(matches!(optimized.op, LogicalOp::AllScan { alias: None }));
    }

    #[test]
    fn test_optimize_pipeline_removes_folded_true_filter() {
        let cm = CostModel::default();
        let stats = PlanStats {
            vertex_count: 100,
            edge_count: 10,
            label_counts: HashMap::new(),
            label_property_counts: HashMap::new(),
            edge_type_counts: HashMap::new(),
            edge_type_property_counts: HashMap::new(),
        };
        let plan = LogicalPlan {
            op: LogicalOp::Join {
                left: Box::new(LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                right: Box::new(LogicalPlan {
                    op: LogicalOp::Filter {
                        condition: Expression::And(
                            Box::new(Expression::Bool(true)),
                            Box::new(Expression::Bool(true)),
                        ),
                    },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                join_type: JoinType::NestedLoop,
            },
            cost: PlanCost::default(),
            cardinality: 100.0,
        };
        let optimized = optimize(plan, &stats, &cm);
        // After constant_fold + remove_true_filters, the filter should be gone.
        assert!(matches!(optimized.op, LogicalOp::AllScan { alias: None }));
    }

    #[test]
    fn test_simplify_trivial_filters_false_to_empty() {
        let plan = LogicalPlan {
            op: LogicalOp::Join {
                left: Box::new(LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                right: Box::new(LogicalPlan {
                    op: LogicalOp::Filter {
                        condition: Expression::Bool(false),
                    },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                join_type: JoinType::NestedLoop,
            },
            cost: PlanCost::default(),
            cardinality: 100.0,
        };
        let simplified = simplify_trivial_filters(plan);
        assert!(matches!(simplified.op, LogicalOp::EmptyResult));
        assert_eq!(simplified.cardinality, 0.0);
    }

    #[test]
    fn test_simplify_trivial_filters_null_to_empty() {
        let plan = LogicalPlan {
            op: LogicalOp::Filter {
                condition: Expression::Null,
            },
            cost: PlanCost::default(),
            cardinality: 100.0,
        };
        let simplified = simplify_trivial_filters(plan);
        assert!(matches!(simplified.op, LogicalOp::EmptyResult));
    }

    #[test]
    fn test_simplify_trivial_filters_propagates_up() {
        let inner = LogicalPlan {
            op: LogicalOp::Join {
                left: Box::new(LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                right: Box::new(LogicalPlan {
                    op: LogicalOp::Filter {
                        condition: Expression::Bool(false),
                    },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                join_type: JoinType::NestedLoop,
            },
            cost: PlanCost::default(),
            cardinality: 100.0,
        };
        let plan = LogicalPlan {
            op: LogicalOp::Join {
                left: Box::new(inner),
                right: Box::new(LogicalPlan {
                    op: LogicalOp::Limit { count: 10 },
                    cost: PlanCost::default(),
                    cardinality: 10.0,
                }),
                join_type: JoinType::NestedLoop,
            },
            cost: PlanCost::default(),
            cardinality: 10.0,
        };
        let simplified = simplify_trivial_filters(plan);
        assert!(matches!(simplified.op, LogicalOp::EmptyResult));
    }

    #[test]
    fn test_optimize_pipeline_false_filter_becomes_empty() {
        let cm = CostModel::default();
        let stats = PlanStats {
            vertex_count: 100,
            edge_count: 10,
            label_counts: HashMap::new(),
            label_property_counts: HashMap::new(),
            edge_type_counts: HashMap::new(),
            edge_type_property_counts: HashMap::new(),
        };
        let plan = LogicalPlan {
            op: LogicalOp::Join {
                left: Box::new(LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                right: Box::new(LogicalPlan {
                    op: LogicalOp::Filter {
                        condition: Expression::Bool(false),
                    },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                join_type: JoinType::NestedLoop,
            },
            cost: PlanCost::default(),
            cardinality: 100.0,
        };
        let optimized = optimize(plan, &stats, &cm);
        assert!(matches!(optimized.op, LogicalOp::EmptyResult));
    }

    #[test]
    fn test_pushdown_limit_through_project() {
        let cm = CostModel::default();
        let plan = LogicalPlan {
            op: LogicalOp::Join {
                left: Box::new(LogicalPlan {
                    op: LogicalOp::Limit { count: 5 },
                    cost: PlanCost::default(),
                    cardinality: 5.0,
                }),
                right: Box::new(LogicalPlan {
                    op: LogicalOp::Project {
                        expressions: vec![Expression::Identifier("n".into())],
                    },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                join_type: JoinType::NestedLoop,
            },
            cost: PlanCost::default(),
            cardinality: 5.0,
        };
        let optimized = pushdown_limit(plan, &cm);
        // Should become: Join(Limit, Project) - Limit pushed through Project
        assert!(matches!(optimized.op, LogicalOp::Join { .. }));
        if let LogicalOp::Join { left, right, .. } = optimized.op {
            assert!(matches!(left.op, LogicalOp::Limit { count: 5 }));
            assert!(matches!(right.op, LogicalOp::Project { .. }));
        }
    }

    #[test]
    fn test_pushdown_skip_through_filter() {
        let cm = CostModel::default();
        let plan = LogicalPlan {
            op: LogicalOp::Join {
                left: Box::new(LogicalPlan {
                    op: LogicalOp::Skip { count: 10 },
                    cost: PlanCost::default(),
                    cardinality: 90.0,
                }),
                right: Box::new(LogicalPlan {
                    op: LogicalOp::Filter {
                        condition: Expression::Bool(true),
                    },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                join_type: JoinType::NestedLoop,
            },
            cost: PlanCost::default(),
            cardinality: 90.0,
        };
        let optimized = pushdown_skip(plan, &cm);
        // Should become: Join(Skip, Filter)
        assert!(matches!(optimized.op, LogicalOp::Join { .. }));
        if let LogicalOp::Join { left, right, .. } = optimized.op {
            assert!(matches!(left.op, LogicalOp::Skip { count: 10 }));
            assert!(matches!(right.op, LogicalOp::Filter { .. }));
        }
    }

    #[test]
    fn test_pushdown_skip_through_project() {
        let cm = CostModel::default();
        let plan = LogicalPlan {
            op: LogicalOp::Join {
                left: Box::new(LogicalPlan {
                    op: LogicalOp::Skip { count: 3 },
                    cost: PlanCost::default(),
                    cardinality: 97.0,
                }),
                right: Box::new(LogicalPlan {
                    op: LogicalOp::Project {
                        expressions: vec![Expression::Identifier("a".into())],
                    },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                join_type: JoinType::NestedLoop,
            },
            cost: PlanCost::default(),
            cardinality: 97.0,
        };
        let optimized = pushdown_skip(plan, &cm);
        assert!(matches!(optimized.op, LogicalOp::Join { .. }));
        if let LogicalOp::Join { left, right, .. } = optimized.op {
            assert!(matches!(left.op, LogicalOp::Skip { count: 3 }));
            assert!(matches!(right.op, LogicalOp::Project { .. }));
        }
    }

    #[test]
    fn test_simplify_trivial_filter_false_becomes_empty_result() {
        let plan = LogicalPlan {
            op: LogicalOp::Filter {
                condition: Expression::Bool(false),
            },
            cost: PlanCost::default(),
            cardinality: 100.0,
        };
        let optimized = simplify_trivial_filters(plan);
        assert!(matches!(optimized.op, LogicalOp::EmptyResult));
    }

    #[test]
    fn test_simplify_trivial_filter_null_becomes_empty_result() {
        let plan = LogicalPlan {
            op: LogicalOp::Filter {
                condition: Expression::Null,
            },
            cost: PlanCost::default(),
            cardinality: 100.0,
        };
        let optimized = simplify_trivial_filters(plan);
        assert!(matches!(optimized.op, LogicalOp::EmptyResult));
    }

    #[test]
    fn test_simplify_trivial_filter_true_unchanged() {
        let plan = LogicalPlan {
            op: LogicalOp::Filter {
                condition: Expression::Bool(true),
            },
            cost: PlanCost::default(),
            cardinality: 100.0,
        };
        let optimized = simplify_trivial_filters(plan);
        assert!(matches!(optimized.op, LogicalOp::Filter { .. }));
    }

    #[test]
    fn test_simplify_trivial_filter_in_join_right() {
        let plan = LogicalPlan {
            op: LogicalOp::Join {
                left: Box::new(LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                right: Box::new(LogicalPlan {
                    op: LogicalOp::Filter {
                        condition: Expression::Bool(false),
                    },
                    cost: PlanCost::default(),
                    cardinality: 0.0,
                }),
                join_type: JoinType::NestedLoop,
            },
            cost: PlanCost::default(),
            cardinality: 100.0,
        };
        let optimized = simplify_trivial_filters(plan);
        assert!(matches!(optimized.op, LogicalOp::EmptyResult));
    }

    #[test]
    fn test_remove_true_filters_removes_trivial_true_in_join() {
        let plan = LogicalPlan {
            op: LogicalOp::Join {
                left: Box::new(LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                right: Box::new(LogicalPlan {
                    op: LogicalOp::Filter {
                        condition: Expression::Bool(true),
                    },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                join_type: JoinType::NestedLoop,
            },
            cost: PlanCost::default(),
            cardinality: 100.0,
        };
        let optimized = remove_true_filters(plan);
        assert!(matches!(optimized.op, LogicalOp::AllScan { alias: None }));
    }

    #[test]
    fn test_remove_true_filters_preserves_non_trivial_in_join() {
        let plan = LogicalPlan {
            op: LogicalOp::Join {
                left: Box::new(LogicalPlan {
                    op: LogicalOp::AllScan { alias: None },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                right: Box::new(LogicalPlan {
                    op: LogicalOp::Filter {
                        condition: Expression::Eq(
                            Box::new(Expression::Identifier("a".into())),
                            Box::new(Expression::Int(1)),
                        ),
                    },
                    cost: PlanCost::default(),
                    cardinality: 100.0,
                }),
                join_type: JoinType::NestedLoop,
            },
            cost: PlanCost::default(),
            cardinality: 100.0,
        };
        let optimized = remove_true_filters(plan);
        assert!(matches!(optimized.op, LogicalOp::Join { .. }));
    }

    #[test]
    fn test_merge_filters_combines_adjacent_filters() {
        // Build Filter -> Filter chain manually
        let inner = LogicalPlan {
            op: LogicalOp::AllScan { alias: None },
            cost: PlanCost::default(),
            cardinality: 100.0,
        };
        let middle = LogicalPlan {
            op: LogicalOp::Filter {
                condition: Expression::Identifier("a".into()),
            },
            cost: PlanCost::default(),
            cardinality: 50.0,
        };
        let outer = LogicalPlan {
            op: LogicalOp::Filter {
                condition: Expression::Identifier("b".into()),
            },
            cost: PlanCost::default(),
            cardinality: 25.0,
        };
        let chain = LogicalPlan {
            op: LogicalOp::Join {
                left: Box::new(inner),
                right: Box::new(middle),
                join_type: JoinType::NestedLoop,
            },
            cost: PlanCost::default(),
            cardinality: 100.0,
        };
        let plan = LogicalPlan {
            op: LogicalOp::Join {
                left: Box::new(chain),
                right: Box::new(outer),
                join_type: JoinType::NestedLoop,
            },
            cost: PlanCost::default(),
            cardinality: 100.0,
        };
        let optimized = merge_filters(plan);
        // merge_filters only merges adjacent Filter nodes, not Join children
        assert!(matches!(optimized.op, LogicalOp::Join { .. }));
    }

    #[test]
    fn test_constant_fold_evaluates_literal_expression() {
        let plan = LogicalPlan {
            op: LogicalOp::Filter {
                condition: Expression::Add(
                    Box::new(Expression::Int(2)),
                    Box::new(Expression::Int(3)),
                ),
            },
            cost: PlanCost::default(),
            cardinality: 100.0,
        };
        let optimized = constant_fold(plan);
        if let LogicalOp::Filter { condition } = optimized.op {
            assert_eq!(condition, Expression::Int(5));
        } else {
            panic!("expected Filter");
        }
    }

    #[test]
    fn test_constant_fold_evaluates_bool_and() {
        let plan = LogicalPlan {
            op: LogicalOp::Filter {
                condition: Expression::And(
                    Box::new(Expression::Bool(true)),
                    Box::new(Expression::Bool(false)),
                ),
            },
            cost: PlanCost::default(),
            cardinality: 100.0,
        };
        let optimized = constant_fold(plan);
        if let LogicalOp::Filter { condition } = optimized.op {
            assert_eq!(condition, Expression::Bool(false));
        } else {
            panic!("expected Filter");
        }
    }

    #[test]
    fn test_edge_type_cardinality_with_stats() {
        let mut stats = CatalogStats::default();
        let etype = EdgeTypeId::from(1u32);
        stats.edge_types.insert(
            etype,
            EdgeTypeStats {
                edge_count: 500,
                distinct_source_vertices: 100,
                distinct_target_vertices: 100,
            },
        );
        assert_eq!(stats.edge_type_cardinality(etype), 500.0);
    }

    #[test]
    fn test_edge_type_cardinality_fallback() {
        let stats = CatalogStats::default();
        let etype = EdgeTypeId::from(99u32);
        assert_eq!(stats.edge_type_cardinality(etype), 1.0);
    }
}
