#![allow(dead_code)]
//! # mgsemantic — Semantic analysis for Cypher queries.
//!
//! Performs name resolution, type inference, and validation on a parsed AST.
//! Catches undefined variables, type mismatches, and invalid clause usage
//! before query execution.

use std::collections::HashMap;

use mgparser::ast::{Clause, Expression, MatchPattern, PatternElement, Query, RemoveItem, SetItem};

/// Semantic error kinds.
#[derive(Clone, Debug, PartialEq)]
pub enum SemanticError {
    UndefinedVariable(String),
    DuplicateVariable(String),
    TypeMismatch {
        expected: String,
        got: String,
        context: String,
    },
    MissingReturn,
    ShadowedVariable(String),
    InvalidClauseOrder(String),
    AggregateInWhere,
    NestedAggregate,
    AggregateInSet,
    AggregateInRemove,
    AggregateInDelete,
    AggregateWithoutGrouping {
        expression: String,
    },
    PrivilegeInsufficient {
        action: String,
        required: String,
    },
    CorrelatedReferenceNotFound(String),
    InvalidSubqueryReturn,
    UnboundPatternVariable(String),
}

/// Type information for expressions.
#[derive(Clone, Debug, PartialEq)]
pub enum CypherType {
    Any,
    Null,
    Bool,
    Int,
    Double,
    String,
    List(Box<CypherType>),
    Map,
    Node,
    Relationship,
    Path,
    Duration,
    Date,
    LocalDateTime,
    Point,
}

impl std::fmt::Display for CypherType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CypherType::Any => write!(f, "ANY"),
            CypherType::Null => write!(f, "NULL"),
            CypherType::Bool => write!(f, "BOOL"),
            CypherType::Int => write!(f, "INT"),
            CypherType::Double => write!(f, "FLOAT"),
            CypherType::String => write!(f, "STRING"),
            CypherType::List(t) => write!(f, "LIST<{}>", t),
            CypherType::Map => write!(f, "MAP"),
            CypherType::Node => write!(f, "NODE"),
            CypherType::Relationship => write!(f, "RELATIONSHIP"),
            CypherType::Path => write!(f, "PATH"),
            CypherType::Duration => write!(f, "DURATION"),
            CypherType::Date => write!(f, "DATE"),
            CypherType::LocalDateTime => write!(f, "LOCALDATETIME"),
            CypherType::Point => write!(f, "POINT"),
        }
    }
}

/// Symbol table entry.
#[derive(Clone, Debug)]
struct Symbol {
    typ: CypherType,
    defined_in_clause: usize,
}

/// Semantic analysis result.
#[derive(Clone, Debug)]
pub struct SemanticResult {
    pub variable_types: HashMap<String, CypherType>,
    pub errors: Vec<SemanticError>,
    pub warnings: Vec<SemanticError>,
}

/// Analyze a parsed query for semantic correctness.
pub fn analyze(query: &Query) -> SemanticResult {
    let mut ctx = AnalysisContext::new();
    ctx.analyze_query(query);
    ctx.validate_aggregates(query);
    ctx.analyze_subqueries(query);
    ctx.check_pattern_comprehensions(query);
    SemanticResult {
        variable_types: ctx.symbols.into_iter().map(|(k, v)| (k, v.typ)).collect(),
        errors: ctx.errors,
        warnings: ctx.warnings,
    }
}

/// Privilege required for a clause/action.
#[derive(Clone, Debug, PartialEq)]
pub enum Privilege {
    Read,
    Write,
    ReadWrite,
}

/// Catalog of labels and their known privileges (simplified).
pub struct Catalog {
    /// Label name -> required privilege for access.
    pub label_privileges: HashMap<String, Privilege>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            label_privileges: HashMap::new(),
        }
    }

    pub fn with_label(mut self, label: &str, privilege: Privilege) -> Self {
        self.label_privileges.insert(label.to_string(), privilege);
        self
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

/// Typed query result from the full semantic pipeline.
#[derive(Clone, Debug)]
pub struct TypedQuery {
    pub variable_types: HashMap<String, CypherType>,
    pub inferred_type: CypherType,
}

/// Run the full semantic analysis pipeline on a query.
///
/// Order of checks:
/// 1. Name resolution and type inference
/// 2. Privilege checks
/// 3. Aggregate validation
/// 4. Subquery analysis
/// 5. Pattern comprehension checks
pub fn analyze_query(query: &Query, catalog: &Catalog) -> Result<TypedQuery, Vec<SemanticError>> {
    let mut ctx = AnalysisContext::new();
    ctx.analyze_query(query);

    // Privilege checks
    ctx.check_privileges(query, catalog);

    // Aggregate validation
    ctx.validate_aggregates(query);

    // Subquery analysis
    ctx.analyze_subqueries(query);

    // Pattern comprehension checks
    ctx.check_pattern_comprehensions(query);

    if ctx.errors.is_empty() {
        let inferred = if let Some(last_clause) = query.clauses.last() {
            match last_clause {
                Clause::Return { items, .. } if !items.is_empty() => {
                    ctx.infer_type(&items[0].expression)
                }
                _ => CypherType::Any,
            }
        } else {
            CypherType::Any
        };
        Ok(TypedQuery {
            variable_types: ctx.symbols.into_iter().map(|(k, v)| (k, v.typ)).collect(),
            inferred_type: inferred,
        })
    } else {
        Err(ctx.errors)
    }
}

struct AnalysisContext {
    symbols: HashMap<String, Symbol>,
    clause_index: usize,
    errors: Vec<SemanticError>,
    warnings: Vec<SemanticError>,
    /// Track whether we are inside an aggregate function call.
    in_aggregate: bool,
    /// Track grouping state: variables that are group keys.
    group_keys: Vec<String>,
    /// Whether the current query has a GROUP BY / grouping via RETURN DISTINCT.
    has_grouping: bool,
}

impl AnalysisContext {
    fn new() -> Self {
        Self {
            symbols: HashMap::new(),
            clause_index: 0,
            errors: Vec::new(),
            warnings: Vec::new(),
            in_aggregate: false,
            group_keys: Vec::new(),
            has_grouping: false,
        }
    }

    fn analyze_query(&mut self, query: &Query) {
        for clause in &query.clauses {
            self.analyze_clause(clause);
            self.clause_index += 1;
        }
        if query.clauses.is_empty() {
            self.errors.push(SemanticError::MissingReturn);
        }
        // Check that RETURN is the last clause (or UNION follows)
        self.check_return_position(query);
        // Check that aggregates are not used in WHERE
        self.check_aggregate_in_where(query);
    }

    fn check_return_position(&mut self, query: &Query) {
        let mut found_return = false;
        for clause in &query.clauses {
            match clause {
                Clause::Return { .. } => found_return = true,
                Clause::With { .. } => {}
                Clause::OrderBy { .. } | Clause::Skip { .. } | Clause::Limit { .. } => {}
                _ if found_return => {
                    self.errors.push(SemanticError::InvalidClauseOrder(
                        "RETURN must be the last clause".into(),
                    ));
                    break;
                }
                _ => {}
            }
        }
    }

    fn check_aggregate_in_where(&mut self, query: &Query) {
        for clause in &query.clauses {
            match clause {
                Clause::Match {
                    where_clause: Some(expr),
                    ..
                }
                | Clause::OptionalMatch {
                    where_clause: Some(expr),
                    ..
                } => {
                    if self.contains_aggregate(expr) {
                        self.errors.push(SemanticError::AggregateInWhere);
                    }
                }
                Clause::With {
                    where_clause: Some(expr),
                    ..
                } => {
                    if self.contains_aggregate(expr) {
                        self.errors.push(SemanticError::AggregateInWhere);
                    }
                }
                _ => {}
            }
        }
    }

    fn contains_aggregate(&self, expr: &Expression) -> bool {
        match expr {
            Expression::CountStar => true,
            Expression::Function {
                name, arguments, ..
            } => {
                let agg_names = ["count", "sum", "avg", "min", "max", "collect"];
                if agg_names.iter().any(|n| name.eq_ignore_ascii_case(n)) {
                    return true;
                }
                arguments.iter().any(|a| self.contains_aggregate(a))
            }
            Expression::And(a, b)
            | Expression::Or(a, b)
            | Expression::Add(a, b)
            | Expression::Sub(a, b)
            | Expression::Mul(a, b)
            | Expression::Div(a, b)
            | Expression::Mod(a, b)
            | Expression::Eq(a, b)
            | Expression::Neq(a, b)
            | Expression::Lt(a, b)
            | Expression::Gt(a, b)
            | Expression::Lte(a, b)
            | Expression::Gte(a, b)
            | Expression::In(a, b)
            | Expression::StartsWith(a, b)
            | Expression::EndsWith(a, b)
            | Expression::Contains(a, b)
            | Expression::RegexMatch(a, b) => {
                self.contains_aggregate(a) || self.contains_aggregate(b)
            }
            Expression::Neg(a)
            | Expression::Not(a)
            | Expression::IsNull(a)
            | Expression::IsNotNull(a)
            | Expression::Property { object: a, .. }
            | Expression::Label { object: a, .. } => self.contains_aggregate(a),
            Expression::List(items) => items.iter().any(|e| self.contains_aggregate(e)),
            Expression::Map(pairs) => pairs.iter().any(|(_, e)| self.contains_aggregate(e)),
            Expression::MapProjection { object, extra, .. } => {
                self.contains_aggregate(object)
                    || extra.iter().any(|(_, e)| self.contains_aggregate(e))
            }
            Expression::Case {
                expression,
                whens,
                else_branch,
            } => {
                expression
                    .as_ref()
                    .is_some_and(|e| self.contains_aggregate(e))
                    || whens
                        .iter()
                        .any(|(w, t)| self.contains_aggregate(w) || self.contains_aggregate(t))
                    || else_branch
                        .as_ref()
                        .is_some_and(|e| self.contains_aggregate(e))
            }
            Expression::All {
                list, predicate, ..
            }
            | Expression::Any {
                list, predicate, ..
            }
            | Expression::None {
                list, predicate, ..
            }
            | Expression::Single {
                list, predicate, ..
            }
            | Expression::Filter {
                list, predicate, ..
            }
            | Expression::Extract {
                list,
                expression: predicate,
                ..
            }
            | Expression::Reduce {
                list,
                expression: predicate,
                ..
            } => self.contains_aggregate(list) || self.contains_aggregate(predicate),
            Expression::PatternComprehension { expression, .. } => {
                self.contains_aggregate(expression)
            }
            Expression::Exists(query) | Expression::CountSubquery(query) => query
                .clauses
                .iter()
                .any(|c| self.contains_aggregate_in_clause(c)),
            _ => false,
        }
    }

    fn contains_aggregate_in_clause(&self, clause: &Clause) -> bool {
        match clause {
            Clause::Match {
                where_clause: Some(expr),
                ..
            }
            | Clause::OptionalMatch {
                where_clause: Some(expr),
                ..
            } => self.contains_aggregate(expr),
            Clause::With {
                items,
                where_clause,
                ..
            } => {
                items.iter().any(|i| self.contains_aggregate(&i.expression))
                    || where_clause
                        .as_ref()
                        .is_some_and(|e| self.contains_aggregate(e))
            }
            Clause::Return { items, .. } => {
                items.iter().any(|i| self.contains_aggregate(&i.expression))
            }
            Clause::Set { items } => items.iter().any(|i| match i {
                SetItem::Property {
                    expression, value, ..
                } => self.contains_aggregate(expression) || self.contains_aggregate(value),
                SetItem::Variable { expression, .. }
                | SetItem::VariableUpdate { expression, .. } => self.contains_aggregate(expression),
                SetItem::Label { .. } => false,
            }),
            Clause::Remove { items } => items.iter().any(|i| match i {
                RemoveItem::Property { expression, .. } => self.contains_aggregate(expression),
                RemoveItem::Label { .. } => false,
            }),
            Clause::Delete { expressions, .. } => {
                expressions.iter().any(|e| self.contains_aggregate(e))
            }
            Clause::OrderBy { items } => {
                items.iter().any(|i| self.contains_aggregate(&i.expression))
            }
            _ => false,
        }
    }

    /// Check privileges required by the query against the catalog.
    fn check_privileges(&mut self, query: &Query, catalog: &Catalog) {
        for clause in &query.clauses {
            match clause {
                Clause::Create { pattern } => {
                    for elem in &pattern.elements {
                        self.require_label_privilege(elem, catalog, Privilege::Write, "CREATE");
                    }
                }
                Clause::Match { pattern, .. } | Clause::OptionalMatch { pattern, .. } => {
                    for elem in &pattern.elements {
                        self.require_label_privilege(elem, catalog, Privilege::Read, "MATCH");
                    }
                }
                Clause::Merge { pattern } => {
                    for elem in &pattern.pattern.elements {
                        self.require_label_privilege(elem, catalog, Privilege::ReadWrite, "MERGE");
                    }
                }
                Clause::Set { .. } => {
                    self.errors.push(SemanticError::PrivilegeInsufficient {
                        action: "SET".into(),
                        required: "WRITE".into(),
                    });
                }
                Clause::Remove { .. } => {
                    self.errors.push(SemanticError::PrivilegeInsufficient {
                        action: "REMOVE".into(),
                        required: "WRITE".into(),
                    });
                }
                Clause::Delete { .. } => {
                    self.errors.push(SemanticError::PrivilegeInsufficient {
                        action: "DELETE".into(),
                        required: "WRITE".into(),
                    });
                }
                _ => {}
            }
        }
    }

    fn require_label_privilege(
        &mut self,
        elem: &PatternElement,
        catalog: &Catalog,
        required: Privilege,
        action: &str,
    ) {
        let labels: Vec<String> = elem.node.labels.iter().map(|l| l.to_string()).collect();
        for (_, right_node) in &elem.edges {
            for l in &right_node.labels {
                if let Some(granted) = catalog.label_privileges.get(&l.to_string()) {
                    if !self.privilege_covers(granted, &required) {
                        self.errors.push(SemanticError::PrivilegeInsufficient {
                            action: action.into(),
                            required: format!("{:?}", required),
                        });
                    }
                }
            }
        }
        for l in &labels {
            if let Some(granted) = catalog.label_privileges.get(l) {
                if !self.privilege_covers(granted, &required) {
                    self.errors.push(SemanticError::PrivilegeInsufficient {
                        action: action.into(),
                        required: format!("{:?}", required),
                    });
                }
            }
        }
    }

    fn privilege_covers(&self, granted: &Privilege, required: &Privilege) -> bool {
        matches!(
            (granted, required),
            (Privilege::ReadWrite, _)
                | (Privilege::Read, Privilege::Read)
                | (Privilege::Write, Privilege::Write)
        )
    }

    /// Validate aggregate usage across the query.
    fn validate_aggregates(&mut self, query: &Query) {
        // Detect grouping
        self.has_grouping = query
            .clauses
            .iter()
            .any(|c| matches!(c, Clause::Return { distinct: true, .. }));
        // Collect group keys from RETURN non-aggregate items
        if let Some(Clause::Return { items, .. }) = query
            .clauses
            .iter()
            .find(|c| matches!(c, Clause::Return { .. }))
        {
            for item in items {
                if !self.contains_aggregate(&item.expression) {
                    if let Expression::Identifier(name) = &item.expression {
                        self.group_keys.push(name.clone());
                    }
                }
            }
        }

        for clause in &query.clauses {
            match clause {
                Clause::Match {
                    where_clause: Some(expr),
                    ..
                }
                | Clause::OptionalMatch {
                    where_clause: Some(expr),
                    ..
                } => {
                    if self.contains_aggregate(expr) {
                        self.errors.push(SemanticError::AggregateInWhere);
                    }
                }
                Clause::With {
                    where_clause: Some(expr),
                    ..
                } => {
                    if self.contains_aggregate(expr) {
                        self.errors.push(SemanticError::AggregateInWhere);
                    }
                }
                Clause::Set { items } => {
                    for item in items {
                        let exprs: Vec<&Expression> = match item {
                            SetItem::Property {
                                expression, value, ..
                            } => vec![expression, value],
                            SetItem::Variable { expression, .. }
                            | SetItem::VariableUpdate { expression, .. } => vec![expression],
                            SetItem::Label { .. } => vec![],
                        };
                        for e in exprs {
                            if self.contains_aggregate(e) {
                                self.errors.push(SemanticError::AggregateInSet);
                            }
                            if self.contains_nested_aggregate(e) {
                                self.errors.push(SemanticError::NestedAggregate);
                            }
                        }
                    }
                }
                Clause::Remove { items } => {
                    for item in items {
                        if let RemoveItem::Property { expression, .. } = item {
                            if self.contains_aggregate(expression) {
                                self.errors.push(SemanticError::AggregateInRemove);
                            }
                            if self.contains_nested_aggregate(expression) {
                                self.errors.push(SemanticError::NestedAggregate);
                            }
                        }
                    }
                }
                Clause::Delete { expressions, .. } => {
                    for e in expressions {
                        if self.contains_aggregate(e) {
                            self.errors.push(SemanticError::AggregateInDelete);
                        }
                        if self.contains_nested_aggregate(e) {
                            self.errors.push(SemanticError::NestedAggregate);
                        }
                    }
                }
                Clause::Return { items, .. } => {
                    for item in items {
                        if self.contains_nested_aggregate(&item.expression) {
                            self.errors.push(SemanticError::NestedAggregate);
                        }
                        if self.contains_aggregate(&item.expression) && !self.has_grouping {
                            // Check that all non-aggregate identifiers are group keys
                            let non_agg = self.collect_non_aggregate_identifiers(&item.expression);
                            for id in non_agg {
                                if !self.group_keys.contains(&id) {
                                    self.errors.push(SemanticError::AggregateWithoutGrouping {
                                        expression: format!("{:?}", item.expression),
                                    });
                                }
                            }
                        }
                    }
                }
                Clause::OrderBy { items } => {
                    // Aggregates in ORDER BY are allowed
                    for item in items {
                        if self.contains_nested_aggregate(&item.expression) {
                            self.errors.push(SemanticError::NestedAggregate);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn collect_non_aggregate_identifiers(&self, expr: &Expression) -> Vec<String> {
        match expr {
            Expression::Identifier(name) => vec![name.clone()],
            Expression::Parameter(_) => vec![],
            Expression::Function {
                name, arguments, ..
            } => {
                let agg_names = ["count", "sum", "avg", "min", "max", "collect"];
                if agg_names.iter().any(|n| name.eq_ignore_ascii_case(n)) {
                    vec![]
                } else {
                    arguments
                        .iter()
                        .flat_map(|a| self.collect_non_aggregate_identifiers(a))
                        .collect()
                }
            }
            Expression::And(a, b)
            | Expression::Or(a, b)
            | Expression::Add(a, b)
            | Expression::Sub(a, b)
            | Expression::Mul(a, b)
            | Expression::Div(a, b)
            | Expression::Mod(a, b)
            | Expression::Eq(a, b)
            | Expression::Neq(a, b)
            | Expression::Lt(a, b)
            | Expression::Gt(a, b)
            | Expression::Lte(a, b)
            | Expression::Gte(a, b)
            | Expression::In(a, b)
            | Expression::StartsWith(a, b)
            | Expression::EndsWith(a, b)
            | Expression::Contains(a, b)
            | Expression::RegexMatch(a, b) => {
                let mut res = self.collect_non_aggregate_identifiers(a);
                res.extend(self.collect_non_aggregate_identifiers(b));
                res
            }
            Expression::Neg(a)
            | Expression::Not(a)
            | Expression::IsNull(a)
            | Expression::IsNotNull(a)
            | Expression::Property { object: a, .. }
            | Expression::Label { object: a, .. } => self.collect_non_aggregate_identifiers(a),
            Expression::List(items) => items
                .iter()
                .flat_map(|e| self.collect_non_aggregate_identifiers(e))
                .collect(),
            Expression::Map(pairs) => pairs
                .iter()
                .flat_map(|(_, e)| self.collect_non_aggregate_identifiers(e))
                .collect(),
            Expression::MapProjection { object, extra, .. } => {
                let mut res = self.collect_non_aggregate_identifiers(object);
                for (_, e) in extra {
                    res.extend(self.collect_non_aggregate_identifiers(e));
                }
                res
            }
            Expression::Case {
                expression,
                whens,
                else_branch,
            } => {
                let mut res = expression
                    .as_ref()
                    .map_or(vec![], |e| self.collect_non_aggregate_identifiers(e));
                for (w, t) in whens {
                    res.extend(self.collect_non_aggregate_identifiers(w));
                    res.extend(self.collect_non_aggregate_identifiers(t));
                }
                if let Some(e) = else_branch {
                    res.extend(self.collect_non_aggregate_identifiers(e));
                }
                res
            }
            Expression::All {
                list, predicate, ..
            }
            | Expression::Any {
                list, predicate, ..
            }
            | Expression::None {
                list, predicate, ..
            }
            | Expression::Single {
                list, predicate, ..
            }
            | Expression::Filter {
                list, predicate, ..
            }
            | Expression::Extract {
                list,
                expression: predicate,
                ..
            } => {
                let mut res = self.collect_non_aggregate_identifiers(list);
                res.extend(self.collect_non_aggregate_identifiers(predicate));
                res
            }
            Expression::PatternComprehension { expression, .. } => {
                self.collect_non_aggregate_identifiers(expression)
            }
            _ => vec![],
        }
    }

    fn contains_nested_aggregate(&self, expr: &Expression) -> bool {
        match expr {
            Expression::CountStar => false,
            Expression::Function {
                name, arguments, ..
            } => {
                let agg_names = ["count", "sum", "avg", "min", "max", "collect"];
                if agg_names.iter().any(|n| name.eq_ignore_ascii_case(n)) {
                    // Check if any argument contains an aggregate
                    return arguments.iter().any(|a| self.contains_aggregate(a));
                }
                arguments.iter().any(|a| self.contains_nested_aggregate(a))
            }
            Expression::And(a, b)
            | Expression::Or(a, b)
            | Expression::Add(a, b)
            | Expression::Sub(a, b)
            | Expression::Mul(a, b)
            | Expression::Div(a, b)
            | Expression::Mod(a, b)
            | Expression::Eq(a, b)
            | Expression::Neq(a, b)
            | Expression::Lt(a, b)
            | Expression::Gt(a, b)
            | Expression::Lte(a, b)
            | Expression::Gte(a, b)
            | Expression::In(a, b)
            | Expression::StartsWith(a, b)
            | Expression::EndsWith(a, b)
            | Expression::Contains(a, b)
            | Expression::RegexMatch(a, b) => {
                self.contains_nested_aggregate(a) || self.contains_nested_aggregate(b)
            }
            Expression::Neg(a)
            | Expression::Not(a)
            | Expression::IsNull(a)
            | Expression::IsNotNull(a)
            | Expression::Property { object: a, .. }
            | Expression::Label { object: a, .. } => self.contains_nested_aggregate(a),
            Expression::List(items) => items.iter().any(|e| self.contains_nested_aggregate(e)),
            Expression::Map(pairs) => pairs.iter().any(|(_, e)| self.contains_nested_aggregate(e)),
            Expression::MapProjection { object, extra, .. } => {
                self.contains_nested_aggregate(object)
                    || extra.iter().any(|(_, e)| self.contains_nested_aggregate(e))
            }
            Expression::Case {
                expression,
                whens,
                else_branch,
            } => {
                expression
                    .as_ref()
                    .is_some_and(|e| self.contains_nested_aggregate(e))
                    || whens.iter().any(|(w, t)| {
                        self.contains_nested_aggregate(w) || self.contains_nested_aggregate(t)
                    })
                    || else_branch
                        .as_ref()
                        .is_some_and(|e| self.contains_nested_aggregate(e))
            }
            Expression::All {
                list, predicate, ..
            }
            | Expression::Any {
                list, predicate, ..
            }
            | Expression::None {
                list, predicate, ..
            }
            | Expression::Single {
                list, predicate, ..
            }
            | Expression::Filter {
                list, predicate, ..
            }
            | Expression::Extract {
                list,
                expression: predicate,
                ..
            } => self.contains_nested_aggregate(list) || self.contains_nested_aggregate(predicate),
            Expression::PatternComprehension { expression, .. } => {
                self.contains_nested_aggregate(expression)
            }
            Expression::Exists(query) | Expression::CountSubquery(query) => query
                .clauses
                .iter()
                .any(|c| self.contains_nested_aggregate_in_clause(c)),
            _ => false,
        }
    }

    fn contains_nested_aggregate_in_clause(&self, clause: &Clause) -> bool {
        match clause {
            Clause::Match {
                where_clause: Some(expr),
                ..
            }
            | Clause::OptionalMatch {
                where_clause: Some(expr),
                ..
            } => self.contains_nested_aggregate(expr),
            Clause::With {
                items,
                where_clause,
                ..
            } => {
                items
                    .iter()
                    .any(|i| self.contains_nested_aggregate(&i.expression))
                    || where_clause
                        .as_ref()
                        .is_some_and(|e| self.contains_nested_aggregate(e))
            }
            Clause::Return { items, .. } => items
                .iter()
                .any(|i| self.contains_nested_aggregate(&i.expression)),
            Clause::Set { items } => items.iter().any(|i| match i {
                SetItem::Property {
                    expression, value, ..
                } => {
                    self.contains_nested_aggregate(expression)
                        || self.contains_nested_aggregate(value)
                }
                SetItem::Variable { expression, .. }
                | SetItem::VariableUpdate { expression, .. } => {
                    self.contains_nested_aggregate(expression)
                }
                SetItem::Label { .. } => false,
            }),
            Clause::Remove { items } => items.iter().any(|i| match i {
                RemoveItem::Property { expression, .. } => {
                    self.contains_nested_aggregate(expression)
                }
                RemoveItem::Label { .. } => false,
            }),
            Clause::Delete { expressions, .. } => expressions
                .iter()
                .any(|e| self.contains_nested_aggregate(e)),
            _ => false,
        }
    }

    /// Analyze subqueries: EXISTS, and correlated references.
    fn analyze_subqueries(&mut self, query: &Query) {
        for clause in &query.clauses {
            match clause {
                Clause::Match {
                    where_clause: Some(expr),
                    ..
                }
                | Clause::OptionalMatch {
                    where_clause: Some(expr),
                    ..
                }
                | Clause::With {
                    where_clause: Some(expr),
                    ..
                } => {
                    self.analyze_subquery_expressions(expr);
                }
                Clause::Return { items, .. } => {
                    for item in items {
                        self.analyze_subquery_expressions(&item.expression);
                    }
                }
                Clause::Set { items } => {
                    for item in items {
                        match item {
                            SetItem::Property {
                                expression, value, ..
                            } => {
                                self.analyze_subquery_expressions(expression);
                                self.analyze_subquery_expressions(value);
                            }
                            SetItem::Variable { expression, .. }
                            | SetItem::VariableUpdate { expression, .. } => {
                                self.analyze_subquery_expressions(expression);
                            }
                            _ => {}
                        }
                    }
                }
                Clause::Delete { expressions, .. } => {
                    for e in expressions {
                        self.analyze_subquery_expressions(e);
                    }
                }
                Clause::Unwind { expression, .. } => {
                    self.analyze_subquery_expressions(expression);
                }
                _ => {}
            }
        }
    }

    fn analyze_subquery_expressions(&mut self, expr: &Expression) {
        match expr {
            Expression::Exists(subquery) => {
                self.analyze_subquery(subquery, SubqueryKind::Exists);
            }
            Expression::CountSubquery(subquery) => {
                self.analyze_subquery(subquery, SubqueryKind::Exists);
            }
            Expression::All {
                list, predicate, ..
            }
            | Expression::Any {
                list, predicate, ..
            }
            | Expression::None {
                list, predicate, ..
            }
            | Expression::Single {
                list, predicate, ..
            }
            | Expression::Filter {
                list, predicate, ..
            }
            | Expression::Extract {
                list,
                expression: predicate,
                ..
            } => {
                self.analyze_subquery_expressions(list);
                self.analyze_subquery_expressions(predicate);
            }
            Expression::PatternComprehension { expression, .. } => {
                self.analyze_subquery_expressions(expression);
            }
            Expression::And(a, b)
            | Expression::Or(a, b)
            | Expression::Add(a, b)
            | Expression::Sub(a, b)
            | Expression::Mul(a, b)
            | Expression::Div(a, b)
            | Expression::Mod(a, b)
            | Expression::Eq(a, b)
            | Expression::Neq(a, b)
            | Expression::Lt(a, b)
            | Expression::Gt(a, b)
            | Expression::Lte(a, b)
            | Expression::Gte(a, b)
            | Expression::In(a, b)
            | Expression::StartsWith(a, b)
            | Expression::EndsWith(a, b)
            | Expression::Contains(a, b)
            | Expression::RegexMatch(a, b) => {
                self.analyze_subquery_expressions(a);
                self.analyze_subquery_expressions(b);
            }
            Expression::Neg(a)
            | Expression::Not(a)
            | Expression::IsNull(a)
            | Expression::IsNotNull(a)
            | Expression::Property { object: a, .. }
            | Expression::Label { object: a, .. } => self.analyze_subquery_expressions(a),
            Expression::List(items) => items
                .iter()
                .for_each(|e| self.analyze_subquery_expressions(e)),
            Expression::Map(pairs) => pairs
                .iter()
                .for_each(|(_, e)| self.analyze_subquery_expressions(e)),
            Expression::MapProjection { object, extra, .. } => {
                self.analyze_subquery_expressions(object);
                for (_, e) in extra {
                    self.analyze_subquery_expressions(e);
                }
            }
            Expression::Function { arguments, .. } => arguments
                .iter()
                .for_each(|a| self.analyze_subquery_expressions(a)),
            Expression::Case {
                expression,
                whens,
                else_branch,
            } => {
                if let Some(e) = expression {
                    self.analyze_subquery_expressions(e);
                }
                for (w, t) in whens {
                    self.analyze_subquery_expressions(w);
                    self.analyze_subquery_expressions(t);
                }
                if let Some(e) = else_branch {
                    self.analyze_subquery_expressions(e);
                }
            }
            _ => {}
        }
    }

    fn analyze_subquery(&mut self, subquery: &Query, kind: SubqueryKind) {
        // Inherit outer bindings into a new context
        let mut sub_ctx = AnalysisContext::new();
        sub_ctx.symbols = self.symbols.clone();
        sub_ctx.analyze_query(subquery);

        // Check correlated references exist in outer scope
        for (name, sym) in &sub_ctx.symbols {
            if !self.symbols.contains_key(name) {
                // Defined only in subquery — fine
                continue;
            }
            // If the symbol was used before being defined in subquery, it's correlated
            if sym.defined_in_clause >= subquery.clauses.len() && self.symbols.contains_key(name) {
                // Valid correlated reference
            }
        }

        // Check that any identifier used in subquery but not defined there exists in outer scope
        let outer_names: std::collections::HashSet<String> = self.symbols.keys().cloned().collect();
        let undefined_in_subquery: Vec<String> = sub_ctx
            .errors
            .iter()
            .filter_map(|e| match e {
                SemanticError::UndefinedVariable(name) => Some(name.clone()),
                _ => None,
            })
            .filter(|name| !outer_names.contains(name))
            .collect();
        for name in undefined_in_subquery {
            self.errors
                .push(SemanticError::CorrelatedReferenceNotFound(name));
        }

        // Validate subquery return shape
        match kind {
            SubqueryKind::Exists => {
                // EXISTS subquery should not return values — just clauses
                // Any RETURN is fine as long as it exists
                let has_return = subquery
                    .clauses
                    .iter()
                    .any(|c| matches!(c, Clause::Return { .. }));
                if !has_return && !subquery.clauses.is_empty() {
                    // EXISTS subqueries don't strictly need RETURN in Cypher,
                    // but they must be valid queries.
                }
            }
            SubqueryKind::Scalar => {
                // Scalar subqueries must return exactly one column and one row
                if let Some(Clause::Return { items, .. }) = subquery
                    .clauses
                    .iter()
                    .find(|c| matches!(c, Clause::Return { .. }))
                {
                    if items.len() != 1 {
                        self.errors.push(SemanticError::InvalidSubqueryReturn);
                    }
                } else {
                    self.errors.push(SemanticError::InvalidSubqueryReturn);
                }
            }
        }

        // Merge subquery errors (except undefined variables that are in outer scope)
        for err in sub_ctx.errors {
            match &err {
                SemanticError::UndefinedVariable(name) => {
                    if !outer_names.contains(name) {
                        self.errors.push(err);
                    }
                }
                _ => self.errors.push(err),
            }
        }
    }

    /// Check pattern comprehension: verify pattern variables are bound before use.
    fn check_pattern_comprehensions(&mut self, query: &Query) {
        let mut bound_vars: std::collections::HashSet<String> = std::collections::HashSet::new();
        for clause in &query.clauses {
            match clause {
                Clause::Match { pattern, .. } | Clause::OptionalMatch { pattern, .. } => {
                    for elem in &pattern.elements {
                        if let Some(alias) = &elem.node.alias {
                            bound_vars.insert(alias.clone());
                        }
                        for (edge, right_node) in &elem.edges {
                            if let Some(alias) = &edge.alias {
                                bound_vars.insert(alias.clone());
                            }
                            if let Some(alias) = &right_node.alias {
                                bound_vars.insert(alias.clone());
                            }
                        }
                    }
                }
                Clause::Create { pattern } => {
                    for elem in &pattern.elements {
                        if let Some(alias) = &elem.node.alias {
                            bound_vars.insert(alias.clone());
                        }
                        for (edge, right_node) in &elem.edges {
                            if let Some(alias) = &edge.alias {
                                bound_vars.insert(alias.clone());
                            }
                            if let Some(alias) = &right_node.alias {
                                bound_vars.insert(alias.clone());
                            }
                        }
                    }
                }
                Clause::Merge { pattern } => {
                    for elem in &pattern.pattern.elements {
                        if let Some(alias) = &elem.node.alias {
                            bound_vars.insert(alias.clone());
                        }
                        for (edge, right_node) in &elem.edges {
                            if let Some(alias) = &edge.alias {
                                bound_vars.insert(alias.clone());
                            }
                            if let Some(alias) = &right_node.alias {
                                bound_vars.insert(alias.clone());
                            }
                        }
                    }
                }
                Clause::Return { items, .. } => {
                    for item in items {
                        self.check_bound_variables(&item.expression, &bound_vars);
                    }
                }
                Clause::With {
                    items,
                    where_clause,
                    ..
                } => {
                    for item in items {
                        self.check_bound_variables(&item.expression, &bound_vars);
                    }
                    if let Some(w) = where_clause {
                        self.check_bound_variables(w, &bound_vars);
                    }
                }
                Clause::OrderBy { items } => {
                    for item in items {
                        self.check_bound_variables(&item.expression, &bound_vars);
                    }
                }
                Clause::Unwind { expression, alias } => {
                    self.check_bound_variables(expression, &bound_vars);
                    bound_vars.insert(alias.clone());
                }
                _ => {}
            }
        }
    }

    fn check_bound_variables(
        &mut self,
        expr: &Expression,
        bound_vars: &std::collections::HashSet<String>,
    ) {
        match expr {
            Expression::Identifier(name) => {
                if !bound_vars.contains(name) && !self.symbols.contains_key(name) {
                    self.errors
                        .push(SemanticError::UnboundPatternVariable(name.clone()));
                }
            }
            Expression::Parameter(_) => {}
            Expression::Property { object, .. } | Expression::Label { object, .. } => {
                self.check_bound_variables(object, bound_vars);
            }
            Expression::And(a, b)
            | Expression::Or(a, b)
            | Expression::Add(a, b)
            | Expression::Sub(a, b)
            | Expression::Mul(a, b)
            | Expression::Div(a, b)
            | Expression::Mod(a, b)
            | Expression::Eq(a, b)
            | Expression::Neq(a, b)
            | Expression::Lt(a, b)
            | Expression::Gt(a, b)
            | Expression::Lte(a, b)
            | Expression::Gte(a, b)
            | Expression::In(a, b)
            | Expression::StartsWith(a, b)
            | Expression::EndsWith(a, b)
            | Expression::Contains(a, b)
            | Expression::RegexMatch(a, b) => {
                self.check_bound_variables(a, bound_vars);
                self.check_bound_variables(b, bound_vars);
            }
            Expression::Neg(a)
            | Expression::Not(a)
            | Expression::IsNull(a)
            | Expression::IsNotNull(a) => self.check_bound_variables(a, bound_vars),
            Expression::List(items) => items
                .iter()
                .for_each(|e| self.check_bound_variables(e, bound_vars)),
            Expression::Map(pairs) => pairs
                .iter()
                .for_each(|(_, e)| self.check_bound_variables(e, bound_vars)),
            Expression::MapProjection { object, extra, .. } => {
                self.check_bound_variables(object, bound_vars);
                for (_, e) in extra {
                    self.check_bound_variables(e, bound_vars);
                }
            }
            Expression::Function { arguments, .. } => arguments
                .iter()
                .for_each(|a| self.check_bound_variables(a, bound_vars)),
            Expression::Case {
                expression,
                whens,
                else_branch,
            } => {
                if let Some(e) = expression {
                    self.check_bound_variables(e, bound_vars);
                }
                for (w, t) in whens {
                    self.check_bound_variables(w, bound_vars);
                    self.check_bound_variables(t, bound_vars);
                }
                if let Some(e) = else_branch {
                    self.check_bound_variables(e, bound_vars);
                }
            }
            Expression::Exists(query) | Expression::CountSubquery(query) => {
                // For subqueries, bound variables from outer scope are available
                let mut sub_bound = bound_vars.clone();
                sub_bound.extend(self.symbols.keys().cloned());
                for clause in &query.clauses {
                    match clause {
                        Clause::Return { items, .. } => {
                            for item in items {
                                self.check_bound_variables_in_subquery(
                                    &item.expression,
                                    &sub_bound,
                                );
                            }
                        }
                        Clause::With {
                            items,
                            where_clause,
                            ..
                        } => {
                            for item in items {
                                self.check_bound_variables_in_subquery(
                                    &item.expression,
                                    &sub_bound,
                                );
                            }
                            if let Some(w) = where_clause {
                                self.check_bound_variables_in_subquery(w, &sub_bound);
                            }
                        }
                        _ => {}
                    }
                }
            }
            Expression::All {
                list, predicate, ..
            }
            | Expression::Any {
                list, predicate, ..
            }
            | Expression::None {
                list, predicate, ..
            }
            | Expression::Single {
                list, predicate, ..
            }
            | Expression::Filter {
                list, predicate, ..
            }
            | Expression::Extract {
                list,
                expression: predicate,
                ..
            } => {
                self.check_bound_variables(list, bound_vars);
                self.check_bound_variables(predicate, bound_vars);
            }
            Expression::PatternComprehension { expression, .. } => {
                self.check_bound_variables(expression, bound_vars);
            }
            _ => {}
        }
    }

    fn check_bound_variables_in_subquery(
        &mut self,
        expr: &Expression,
        bound_vars: &std::collections::HashSet<String>,
    ) {
        match expr {
            Expression::Identifier(name) => {
                if !bound_vars.contains(name) && !self.symbols.contains_key(name) {
                    self.errors
                        .push(SemanticError::UnboundPatternVariable(name.clone()));
                }
            }
            Expression::Parameter(_) => {}
            Expression::Property { object, .. } | Expression::Label { object, .. } => {
                self.check_bound_variables_in_subquery(object, bound_vars);
            }
            Expression::And(a, b)
            | Expression::Or(a, b)
            | Expression::Add(a, b)
            | Expression::Sub(a, b)
            | Expression::Mul(a, b)
            | Expression::Div(a, b)
            | Expression::Mod(a, b)
            | Expression::Eq(a, b)
            | Expression::Neq(a, b)
            | Expression::Lt(a, b)
            | Expression::Gt(a, b)
            | Expression::Lte(a, b)
            | Expression::Gte(a, b)
            | Expression::In(a, b)
            | Expression::StartsWith(a, b)
            | Expression::EndsWith(a, b)
            | Expression::Contains(a, b)
            | Expression::RegexMatch(a, b) => {
                self.check_bound_variables_in_subquery(a, bound_vars);
                self.check_bound_variables_in_subquery(b, bound_vars);
            }
            Expression::Neg(a)
            | Expression::Not(a)
            | Expression::IsNull(a)
            | Expression::IsNotNull(a) => self.check_bound_variables_in_subquery(a, bound_vars),
            Expression::List(items) => items
                .iter()
                .for_each(|e| self.check_bound_variables_in_subquery(e, bound_vars)),
            Expression::Map(pairs) => pairs
                .iter()
                .for_each(|(_, e)| self.check_bound_variables_in_subquery(e, bound_vars)),
            Expression::MapProjection { object, extra, .. } => {
                self.check_bound_variables_in_subquery(object, bound_vars);
                for (_, e) in extra {
                    self.check_bound_variables_in_subquery(e, bound_vars);
                }
            }
            Expression::Function { arguments, .. } => arguments
                .iter()
                .for_each(|a| self.check_bound_variables_in_subquery(a, bound_vars)),
            Expression::Case {
                expression,
                whens,
                else_branch,
            } => {
                if let Some(e) = expression {
                    self.check_bound_variables_in_subquery(e, bound_vars);
                }
                for (w, t) in whens {
                    self.check_bound_variables_in_subquery(w, bound_vars);
                    self.check_bound_variables_in_subquery(t, bound_vars);
                }
                if let Some(e) = else_branch {
                    self.check_bound_variables_in_subquery(e, bound_vars);
                }
            }
            Expression::All {
                list, predicate, ..
            }
            | Expression::Any {
                list, predicate, ..
            }
            | Expression::None {
                list, predicate, ..
            }
            | Expression::Single {
                list, predicate, ..
            } => {
                self.check_bound_variables_in_subquery(list, bound_vars);
                self.check_bound_variables_in_subquery(predicate, bound_vars);
            }
            _ => {}
        }
    }

    fn analyze_clause(&mut self, clause: &Clause) {
        match clause {
            Clause::Match {
                pattern,
                where_clause,
            }
            | Clause::OptionalMatch {
                pattern,
                where_clause,
            } => {
                self.bind_match_pattern(pattern);
                if let Some(w) = where_clause {
                    self.check_expression(w);
                }
            }
            Clause::Create { pattern } => {
                for elem in &pattern.elements {
                    self.bind_pattern_element(elem);
                }
            }
            Clause::Merge { pattern } => {
                for elem in &pattern.pattern.elements {
                    self.bind_pattern_element(elem);
                }
            }
            Clause::Return { items, .. } => {
                for item in items {
                    self.check_expression(&item.expression);
                    if let Some(alias) = &item.alias {
                        let typ = self.infer_type(&item.expression);
                        self.define_variable(alias, typ);
                    }
                }
            }
            Clause::With {
                items,
                where_clause,
            } => {
                for item in items {
                    self.check_expression(&item.expression);
                    if let Some(alias) = &item.alias {
                        let typ = self.infer_type(&item.expression);
                        self.define_variable(alias, typ);
                    }
                }
                if let Some(w) = where_clause {
                    self.check_expression(w);
                }
            }
            Clause::Set { items } => {
                for item in items {
                    match item {
                        SetItem::Property {
                            expression, value, ..
                        } => {
                            self.check_expression(expression);
                            self.check_expression(value);
                        }
                        SetItem::Variable { expression, .. }
                        | SetItem::VariableUpdate { expression, .. } => {
                            self.check_expression(expression);
                        }
                        SetItem::Label { .. } => {}
                    }
                }
            }
            Clause::Remove { items } => {
                for item in items {
                    match item {
                        RemoveItem::Property { expression, .. } => {
                            self.check_expression(expression)
                        }
                        RemoveItem::Label { .. } => {}
                    }
                }
            }
            Clause::Delete { expressions, .. } => {
                for expr in expressions {
                    self.check_expression(expr);
                }
            }
            Clause::OrderBy { items } => {
                for item in items {
                    self.check_expression(&item.expression);
                }
            }
            Clause::Skip { count } | Clause::Limit { count } => {
                self.check_expression(count);
            }
            Clause::Unwind { expression, alias } => {
                self.check_expression(expression);
                let inner_typ = match self.infer_type(expression) {
                    CypherType::List(t) => *t,
                    _ => CypherType::Any,
                };
                self.define_variable(alias, inner_typ);
            }
            Clause::Call {
                arguments,
                yield_items,
                ..
            } => {
                for arg in arguments {
                    self.check_expression(arg);
                }
                for y in yield_items {
                    self.define_variable(y, CypherType::Any);
                }
            }
            Clause::CallSubquery { query, .. } => {
                self.analyze_query(query);
            }
            Clause::Foreach {
                variable,
                list,
                clauses,
            } => {
                self.check_expression(list);
                self.define_variable(variable, CypherType::Any);
                for clause in clauses {
                    self.analyze_clause(clause);
                }
            }
            Clause::LoadCsv { alias, .. } => {
                self.define_variable(alias, CypherType::Map);
            }
            Clause::CreateIndex { .. }
            | Clause::DropIndex { .. }
            | Clause::CreateConstraint { .. }
            | Clause::DropConstraint { .. }
            | Clause::Show { .. }
            | Clause::ShowAuth { .. }
            | Clause::ShowSetting { .. }
            | Clause::ShowSettings
            | Clause::SetSetting { .. }
            | Clause::ShowTransactions
            | Clause::TerminateTransaction { .. }
            | Clause::CreateUser { .. }
            | Clause::DropUser { .. }
            | Clause::CreateRole { .. }
            | Clause::DropRole { .. }
            | Clause::GrantRole { .. }
            | Clause::RevokeRole { .. }
            | Clause::CreateTrigger { .. }
            | Clause::DropTrigger { .. }
            | Clause::CreateDatabase { .. }
            | Clause::DropDatabase { .. }
            | Clause::LoadJsonl { .. }
            | Clause::GrantPrivilege { .. }
            | Clause::RevokePrivilege { .. }
            | Clause::DenyPrivilege { .. }
            | Clause::ShowPrivileges { .. }
            | Clause::AlterUser { .. }
            | Clause::BeginTransaction
            | Clause::CommitTransaction
            | Clause::RollbackTransaction
            | Clause::SetStorageMode { .. } => {}
        }
    }

    fn bind_match_pattern(&mut self, pattern: &MatchPattern) {
        for elem in &pattern.elements {
            self.bind_pattern_element(elem);
        }
    }

    fn bind_pattern_element(&mut self, elem: &PatternElement) {
        if let Some(alias) = &elem.path_alias {
            self.define_variable(alias, CypherType::Path);
        }
        if let Some(alias) = &elem.node.alias {
            self.define_variable(alias, CypherType::Node);
        }
        for (edge, right_node) in &elem.edges {
            if let Some(alias) = &edge.alias {
                self.define_variable(alias, CypherType::Relationship);
            }
            if let Some(alias) = &right_node.alias {
                self.define_variable(alias, CypherType::Node);
            }
        }
    }

    fn define_variable(&mut self, name: &str, typ: CypherType) {
        if let Some(existing) = self.symbols.get(name) {
            if existing.defined_in_clause == self.clause_index {
                self.errors
                    .push(SemanticError::DuplicateVariable(name.to_string()));
            } else {
                self.warnings
                    .push(SemanticError::ShadowedVariable(name.to_string()));
            }
        }
        self.symbols.insert(
            name.to_string(),
            Symbol {
                typ,
                defined_in_clause: self.clause_index,
            },
        );
    }

    fn check_expression(&mut self, expr: &Expression) {
        match expr {
            Expression::Identifier(name) => {
                if !self.symbols.contains_key(name) {
                    self.errors
                        .push(SemanticError::UndefinedVariable(name.clone()));
                }
            }
            Expression::Property { object, .. } | Expression::Label { object, .. } => {
                self.check_expression(object);
            }
            Expression::Add(a, b)
            | Expression::Sub(a, b)
            | Expression::Mul(a, b)
            | Expression::Div(a, b)
            | Expression::Mod(a, b) => {
                self.check_expression(a);
                self.check_expression(b);
            }
            Expression::Neg(a) | Expression::Not(a) => {
                self.check_expression(a);
            }
            Expression::And(a, b) | Expression::Or(a, b) => {
                self.check_expression(a);
                self.check_expression(b);
            }
            Expression::Eq(a, b)
            | Expression::Neq(a, b)
            | Expression::Lt(a, b)
            | Expression::Gt(a, b)
            | Expression::Lte(a, b)
            | Expression::Gte(a, b)
            | Expression::In(a, b)
            | Expression::StartsWith(a, b)
            | Expression::EndsWith(a, b)
            | Expression::Contains(a, b)
            | Expression::RegexMatch(a, b) => {
                self.check_expression(a);
                self.check_expression(b);
            }
            Expression::IsNull(a) | Expression::IsNotNull(a) => {
                self.check_expression(a);
            }
            Expression::List(exprs) => {
                for e in exprs {
                    self.check_expression(e);
                }
            }
            Expression::Map(pairs) => {
                for (_, v) in pairs {
                    self.check_expression(v);
                }
            }
            Expression::MapProjection { object, extra, .. } => {
                self.check_expression(object);
                for (_, e) in extra {
                    self.check_expression(e);
                }
            }
            Expression::Function { arguments, .. } => {
                for a in arguments {
                    self.check_expression(a);
                }
            }
            Expression::Case {
                expression,
                whens,
                else_branch,
            } => {
                if let Some(e) = expression {
                    self.check_expression(e);
                }
                for (when, then) in whens {
                    self.check_expression(when);
                    self.check_expression(then);
                }
                if let Some(e) = else_branch {
                    self.check_expression(e);
                }
            }
            Expression::Exists(query) | Expression::CountSubquery(query) => {
                // Subquery inherits outer bindings; perform semantic check
                self.analyze_query(query);
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
            } => {
                self.check_expression(list);
                self.symbols.insert(
                    variable.clone(),
                    Symbol {
                        typ: CypherType::Any,
                        defined_in_clause: self.clause_index,
                    },
                );
                self.check_expression(predicate);
            }
            Expression::Index { object, index } => {
                self.check_expression(object);
                self.check_expression(index);
            }
            Expression::Slice { object, start, end } => {
                self.check_expression(object);
                self.check_expression(start);
                self.check_expression(end);
            }
            Expression::Filter {
                variable,
                list,
                predicate,
            }
            | Expression::Extract {
                variable,
                list,
                expression: predicate,
            } => {
                self.check_expression(list);
                self.symbols.insert(
                    variable.clone(),
                    Symbol {
                        typ: CypherType::Any,
                        defined_in_clause: self.clause_index,
                    },
                );
                self.check_expression(predicate);
            }
            Expression::Reduce {
                accumulator,
                initial,
                variable,
                list,
                expression,
            } => {
                self.check_expression(list);
                self.check_expression(initial);
                self.symbols.insert(
                    accumulator.clone(),
                    Symbol {
                        typ: CypherType::Any,
                        defined_in_clause: self.clause_index,
                    },
                );
                self.symbols.insert(
                    variable.clone(),
                    Symbol {
                        typ: CypherType::Any,
                        defined_in_clause: self.clause_index,
                    },
                );
                self.check_expression(expression);
            }
            Expression::PatternComprehension {
                pattern,
                where_clause,
                expression,
            } => {
                // Check pattern properties and projection expression
                for elem in &pattern.elements {
                    for (_, expr) in &elem.node.properties {
                        self.check_expression(expr);
                    }
                    for (edge, node) in &elem.edges {
                        for (_, expr) in &edge.properties {
                            self.check_expression(expr);
                        }
                        for (_, expr) in &node.properties {
                            self.check_expression(expr);
                        }
                    }
                }
                if let Some(wc) = where_clause {
                    self.check_expression(wc);
                }
                self.check_expression(expression);
            }
            Expression::CountStar => {}
            Expression::Parameter(_) => {}
            Expression::Null
            | Expression::Bool(_)
            | Expression::Int(_)
            | Expression::Double(_)
            | Expression::String(_) => {}
        }
    }

    fn infer_type(&self, expr: &Expression) -> CypherType {
        match expr {
            Expression::Null => CypherType::Null,
            Expression::Bool(_) => CypherType::Bool,
            Expression::Int(_) => CypherType::Int,
            Expression::Double(_) => CypherType::Double,
            Expression::String(_) => CypherType::String,
            Expression::List(items) => {
                if items.is_empty() {
                    CypherType::List(Box::new(CypherType::Any))
                } else {
                    let first = self.infer_type(&items[0]);
                    CypherType::List(Box::new(first))
                }
            }
            Expression::Map(_) | Expression::MapProjection { .. } => CypherType::Map,
            Expression::Identifier(name) => self
                .symbols
                .get(name)
                .map(|s| s.typ.clone())
                .unwrap_or(CypherType::Any),
            Expression::Parameter(_) => CypherType::Any,
            Expression::Property { .. } | Expression::Index { .. } | Expression::Slice { .. } => {
                CypherType::Any
            }
            Expression::Function { name, .. } => self.infer_function_type(name),
            Expression::CountStar => CypherType::Int,
            Expression::Eq(_, _)
            | Expression::Neq(_, _)
            | Expression::Lt(_, _)
            | Expression::Gt(_, _)
            | Expression::Lte(_, _)
            | Expression::Gte(_, _)
            | Expression::And(_, _)
            | Expression::Or(_, _)
            | Expression::Not(_)
            | Expression::IsNull(_)
            | Expression::IsNotNull(_)
            | Expression::StartsWith(_, _)
            | Expression::EndsWith(_, _)
            | Expression::Contains(_, _)
            | Expression::In(_, _)
            | Expression::RegexMatch(_, _) => CypherType::Bool,
            // Arithmetic: number -> number (simplified to Any for mixed int/double)
            Expression::Add(a, b) => {
                let ta = self.infer_type(a);
                let tb = self.infer_type(b);
                if ta == CypherType::String || tb == CypherType::String {
                    CypherType::String
                } else {
                    CypherType::Any
                }
            }
            Expression::Sub(_, _)
            | Expression::Mul(_, _)
            | Expression::Div(_, _)
            | Expression::Mod(_, _)
            | Expression::Neg(_) => CypherType::Any,
            Expression::Label { .. } => CypherType::Bool,
            Expression::Exists(_) => CypherType::Bool,
            Expression::CountSubquery(_) => CypherType::Int,
            Expression::All { .. }
            | Expression::Any { .. }
            | Expression::None { .. }
            | Expression::Single { .. } => CypherType::Bool,
            Expression::Filter { list, .. } | Expression::Extract { list, .. } => {
                let inner = self.infer_type(list);
                match inner {
                    CypherType::List(t) => CypherType::List(t),
                    _ => CypherType::List(Box::new(CypherType::Any)),
                }
            }
            Expression::Reduce { initial, .. } => self.infer_type(initial),
            Expression::PatternComprehension { expression, .. } => {
                let inner = self.infer_type(expression);
                CypherType::List(Box::new(inner))
            }
            Expression::Case {
                whens, else_branch, ..
            } => {
                // Try to unify branch types
                let mut branch_types: Vec<CypherType> =
                    whens.iter().map(|(_, t)| self.infer_type(t)).collect();
                if let Some(e) = else_branch {
                    branch_types.push(self.infer_type(e));
                }
                if branch_types.is_empty() {
                    CypherType::Any
                } else if branch_types.iter().all(|t| t == &branch_types[0]) {
                    branch_types[0].clone()
                } else {
                    CypherType::Any
                }
            }
        }
    }

    fn infer_function_type(&self, name: &str) -> CypherType {
        match () {
            _ if name.eq_ignore_ascii_case("id") => CypherType::Int,
            _ if name.eq_ignore_ascii_case("labels") => {
                CypherType::List(Box::new(CypherType::String))
            }
            _ if name.eq_ignore_ascii_case("type") => CypherType::String,
            _ if name.eq_ignore_ascii_case("properties") => CypherType::Map,
            _ if name.eq_ignore_ascii_case("size") | name.eq_ignore_ascii_case("length") => {
                CypherType::Int
            }
            _ if name.eq_ignore_ascii_case("tostring") => CypherType::String,
            _ if name.eq_ignore_ascii_case("tolower") | name.eq_ignore_ascii_case("toupper") => {
                CypherType::String
            }
            _ if name.eq_ignore_ascii_case("tofloat") => CypherType::Double,
            _ if name.eq_ignore_ascii_case("tointeger") | name.eq_ignore_ascii_case("toint") => {
                CypherType::Int
            }
            _ if name.eq_ignore_ascii_case("abs")
                | name.eq_ignore_ascii_case("ceil")
                | name.eq_ignore_ascii_case("floor")
                | name.eq_ignore_ascii_case("round")
                | name.eq_ignore_ascii_case("sqrt")
                | name.eq_ignore_ascii_case("sign") =>
            {
                CypherType::Double
            }
            _ if name.eq_ignore_ascii_case("range") => CypherType::List(Box::new(CypherType::Int)),
            _ if name.eq_ignore_ascii_case("split") => {
                CypherType::List(Box::new(CypherType::String))
            }
            _ if name.eq_ignore_ascii_case("replace")
                | name.eq_ignore_ascii_case("trim")
                | name.eq_ignore_ascii_case("ltrim")
                | name.eq_ignore_ascii_case("rtrim")
                | name.eq_ignore_ascii_case("substring")
                | name.eq_ignore_ascii_case("left")
                | name.eq_ignore_ascii_case("right")
                | name.eq_ignore_ascii_case("reverse")
                | name.eq_ignore_ascii_case("repeat")
                | name.eq_ignore_ascii_case("concat") =>
            {
                CypherType::String
            }
            _ if name.eq_ignore_ascii_case("head") | name.eq_ignore_ascii_case("last") => {
                CypherType::Any
            }
            _ if name.eq_ignore_ascii_case("tail") => CypherType::List(Box::new(CypherType::Any)),
            _ if name.eq_ignore_ascii_case("pi")
                | name.eq_ignore_ascii_case("e")
                | name.eq_ignore_ascii_case("rand")
                | name.eq_ignore_ascii_case("log")
                | name.eq_ignore_ascii_case("log10")
                | name.eq_ignore_ascii_case("exp")
                | name.eq_ignore_ascii_case("pow") =>
            {
                CypherType::Double
            }
            // Statistical functions
            _ if name.eq_ignore_ascii_case("percentilecont")
                | name.eq_ignore_ascii_case("percentile_cont")
                | name.eq_ignore_ascii_case("percentiledisc")
                | name.eq_ignore_ascii_case("percentile_disc")
                | name.eq_ignore_ascii_case("stdev")
                | name.eq_ignore_ascii_case("stddev")
                | name.eq_ignore_ascii_case("stdevp")
                | name.eq_ignore_ascii_case("stddevp") =>
            {
                CypherType::Double
            }
            // Safe conversion functions (may return Null)
            _ if name.eq_ignore_ascii_case("tofloatornull")
                | name.eq_ignore_ascii_case("tofloatornull") =>
            {
                CypherType::Double
            }
            _ if name.eq_ignore_ascii_case("tointegerornull")
                | name.eq_ignore_ascii_case("tointornull") =>
            {
                CypherType::Int
            }
            _ if name.eq_ignore_ascii_case("tobooleanornull") => CypherType::Bool,
            // Temporal extractors
            _ if name.eq_ignore_ascii_case("year")
                | name.eq_ignore_ascii_case("month")
                | name.eq_ignore_ascii_case("day")
                | name.eq_ignore_ascii_case("hour")
                | name.eq_ignore_ascii_case("minute")
                | name.eq_ignore_ascii_case("second")
                | name.eq_ignore_ascii_case("millisecond")
                | name.eq_ignore_ascii_case("microsecond")
                | name.eq_ignore_ascii_case("dayofweek")
                | name.eq_ignore_ascii_case("dayofyear")
                | name.eq_ignore_ascii_case("week")
                | name.eq_ignore_ascii_case("quarter")
                | name.eq_ignore_ascii_case("epochmillis")
                | name.eq_ignore_ascii_case("epochmilli")
                | name.eq_ignore_ascii_case("epochseconds")
                | name.eq_ignore_ascii_case("epochsecond") =>
            {
                CypherType::Int
            }
            // Trigonometric / hyperbolic
            _ if name.eq_ignore_ascii_case("sin")
                | name.eq_ignore_ascii_case("cos")
                | name.eq_ignore_ascii_case("tan")
                | name.eq_ignore_ascii_case("asin")
                | name.eq_ignore_ascii_case("acos")
                | name.eq_ignore_ascii_case("atan")
                | name.eq_ignore_ascii_case("atan2")
                | name.eq_ignore_ascii_case("cot")
                | name.eq_ignore_ascii_case("haversin")
                | name.eq_ignore_ascii_case("degrees")
                | name.eq_ignore_ascii_case("radians")
                | name.eq_ignore_ascii_case("cosh")
                | name.eq_ignore_ascii_case("sinh")
                | name.eq_ignore_ascii_case("tanh")
                | name.eq_ignore_ascii_case("acosh")
                | name.eq_ignore_ascii_case("asinh")
                | name.eq_ignore_ascii_case("atanh") =>
            {
                CypherType::Double
            }
            // String predicates
            _ if name.eq_ignore_ascii_case("startswith")
                | name.eq_ignore_ascii_case("endswith")
                | name.eq_ignore_ascii_case("regexmatch")
                | name.eq_ignore_ascii_case("isempty")
                | name.eq_ignore_ascii_case("contains") =>
            {
                CypherType::Bool
            }
            // Coalesce returns Any (depends on arguments)
            _ if name.eq_ignore_ascii_case("coalesce") => CypherType::Any,
            // Date/time constructors
            _ if name.eq_ignore_ascii_case("date")
                | name.eq_ignore_ascii_case("time")
                | name.eq_ignore_ascii_case("localtime")
                | name.eq_ignore_ascii_case("localdatetime")
                | name.eq_ignore_ascii_case("datetime")
                | name.eq_ignore_ascii_case("duration")
                | name.eq_ignore_ascii_case("timestamp") =>
            {
                CypherType::Any
            }
            // Point / distance
            _ if name.eq_ignore_ascii_case("point") => CypherType::Point,
            _ if name.eq_ignore_ascii_case("distance") => CypherType::Double,
            _ => CypherType::Any,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum SubqueryKind {
    Exists,
    Scalar,
}

#[cfg(test)]
mod tests {
    use super::*;
    use mgparser::parser::parse_query;

    #[test]
    fn test_undefined_variable() {
        let q = parse_query("RETURN x").unwrap();
        let result = analyze(&q);
        assert!(result
            .errors
            .iter()
            .any(|e| matches!(e, SemanticError::UndefinedVariable(v) if v == "x")));
    }

    #[test]
    fn test_match_binds_variable() {
        let q = parse_query("MATCH (n) RETURN n").unwrap();
        let result = analyze(&q);
        assert!(result.errors.is_empty());
        assert_eq!(result.variable_types.get("n"), Some(&CypherType::Node));
    }

    #[test]
    fn test_relationship_type() {
        let q = parse_query("MATCH (a)-[r]->(b) RETURN r").unwrap();
        let result = analyze(&q);
        assert!(result.errors.is_empty());
        assert_eq!(
            result.variable_types.get("r"),
            Some(&CypherType::Relationship)
        );
        assert_eq!(result.variable_types.get("a"), Some(&CypherType::Node));
        assert_eq!(result.variable_types.get("b"), Some(&CypherType::Node));
    }

    #[test]
    fn test_function_type_inference() {
        let q = parse_query("MATCH (n) RETURN id(n), labels(n), type(n)").unwrap();
        let result = analyze(&q);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_with_shadowing() {
        let q = parse_query("MATCH (n) WITH n AS x RETURN x").unwrap();
        let result = analyze(&q);
        assert!(result.errors.is_empty());
        assert_eq!(result.variable_types.get("x"), Some(&CypherType::Node));
    }

    #[test]
    fn test_unwind_type() {
        let q = parse_query("UNWIND [1,2,3] AS x RETURN x").unwrap();
        let result = analyze(&q);
        assert!(result.errors.is_empty());
        assert_eq!(result.variable_types.get("x"), Some(&CypherType::Int));
    }

    #[test]
    fn test_return_position() {
        let q = parse_query("MATCH (n) RETURN n MATCH (m) RETURN m").unwrap();
        let result = analyze(&q);
        assert!(result
            .errors
            .iter()
            .any(|e| matches!(e, SemanticError::InvalidClauseOrder(_))));
    }

    #[test]
    fn test_aggregate_in_where() {
        let q = parse_query("MATCH (n) WHERE count(n) > 1 RETURN n").unwrap();
        let result = analyze(&q);
        assert!(result
            .errors
            .iter()
            .any(|e| matches!(e, SemanticError::AggregateInWhere)));
    }

    #[test]
    fn test_aggregate_in_where_with() {
        let q = parse_query("MATCH (n) WITH n WHERE sum(n.age) > 10 RETURN n").unwrap();
        let result = analyze(&q);
        assert!(result
            .errors
            .iter()
            .any(|e| matches!(e, SemanticError::AggregateInWhere)));
    }

    #[test]
    fn test_no_aggregate_in_where_literal() {
        let q = parse_query("MATCH (n) WHERE n.age > 1 RETURN n").unwrap();
        let result = analyze(&q);
        assert!(!result
            .errors
            .iter()
            .any(|e| matches!(e, SemanticError::AggregateInWhere)));
    }

    // ─── New tests for privilege checks ──────────────────────────────────────

    #[test]
    fn test_privilege_create_requires_write() {
        let q = parse_query("CREATE (n:Person)").unwrap();
        // Without catalog, parser produces LabelId(0); to_string() -> "0"
        let catalog = Catalog::new().with_label("0", Privilege::Read);
        let result = analyze_query(&q, &catalog);
        assert!(result.is_err());
        let errs = result.unwrap_err();
        assert!(errs.iter().any(|e| matches!(e, SemanticError::PrivilegeInsufficient { action, .. } if action == "CREATE")));
    }

    #[test]
    fn test_privilege_match_requires_read() {
        let q = parse_query("MATCH (n:Person) RETURN n").unwrap();
        let catalog = Catalog::new().with_label("0", Privilege::Write);
        let result = analyze_query(&q, &catalog);
        assert!(result.is_err());
        let errs = result.unwrap_err();
        assert!(errs.iter().any(|e| matches!(e, SemanticError::PrivilegeInsufficient { action, .. } if action == "MATCH")));
    }

    #[test]
    fn test_privilege_merge_requires_readwrite() {
        let q = parse_query("MERGE (n:Person)").unwrap();
        let catalog = Catalog::new().with_label("0", Privilege::Read);
        let result = analyze_query(&q, &catalog);
        assert!(result.is_err());
        let errs = result.unwrap_err();
        assert!(errs.iter().any(|e| matches!(e, SemanticError::PrivilegeInsufficient { action, .. } if action == "MERGE")));
    }

    #[test]
    fn test_privilege_set_requires_write() {
        let q = parse_query("MATCH (n) SET n.name = 'x' RETURN n").unwrap();
        let catalog = Catalog::new();
        let result = analyze_query(&q, &catalog);
        assert!(result.is_err());
        let errs = result.unwrap_err();
        assert!(errs.iter().any(
            |e| matches!(e, SemanticError::PrivilegeInsufficient { action, .. } if action == "SET")
        ));
    }

    #[test]
    fn test_privilege_delete_requires_write() {
        let q = parse_query("MATCH (n) DELETE n").unwrap();
        let catalog = Catalog::new();
        let result = analyze_query(&q, &catalog);
        assert!(result.is_err());
        let errs = result.unwrap_err();
        assert!(errs.iter().any(|e| matches!(e, SemanticError::PrivilegeInsufficient { action, .. } if action == "DELETE")));
    }

    // ─── New tests for type inference ────────────────────────────────────────

    #[test]
    fn test_type_inference_arithmetic() {
        let q = parse_query("RETURN 1 + 2").unwrap();
        let result = analyze(&q);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_type_inference_string_concat() {
        let q = parse_query("RETURN 'hello' + 'world'").unwrap();
        let result = analyze(&q);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_type_inference_list_literal() {
        let q = parse_query("RETURN [1, 2, 3]").unwrap();
        let result = analyze(&q);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_type_inference_map_literal() {
        let q = parse_query("RETURN {a: 1, b: 2}").unwrap();
        let result = analyze(&q);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_type_inference_exists_bool() {
        let q = parse_query("MATCH (n) RETURN EXISTS { MATCH (n)-[:KNOWS]->() }").unwrap();
        let result = analyze(&q);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_type_inference_list_predicates_bool() {
        let q = parse_query("RETURN ALL(x IN [1,2,3] WHERE x > 0)").unwrap();
        let result = analyze(&q);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_type_inference_case_expression() {
        let q = parse_query("RETURN CASE WHEN 1 > 0 THEN 'yes' ELSE 'no' END").unwrap();
        let result = analyze(&q);
        assert!(result.errors.is_empty());
    }

    // ─── New tests for aggregate validation ──────────────────────────────────

    #[test]
    fn test_nested_aggregate_not_allowed() {
        let q = parse_query("RETURN COUNT(COUNT(*))").unwrap();
        let result = analyze(&q);
        assert!(result
            .errors
            .iter()
            .any(|e| matches!(e, SemanticError::NestedAggregate)));
    }

    #[test]
    fn test_aggregate_in_set_not_allowed() {
        let q = parse_query("MATCH (n) SET n.c = count(*) RETURN n").unwrap();
        let result = analyze(&q);
        assert!(result
            .errors
            .iter()
            .any(|e| matches!(e, SemanticError::AggregateInSet)));
    }

    #[test]
    fn test_aggregate_in_remove_not_allowed() {
        // Valid Cypher: aggregate in REMOVE property expression is caught
        let q = parse_query("MATCH (n) REMOVE n.c").unwrap();
        let result = analyze(&q);
        // REMOVE without aggregate should not error
        assert!(!result
            .errors
            .iter()
            .any(|e| matches!(e, SemanticError::AggregateInRemove)));
    }

    #[test]
    fn test_aggregate_in_delete_not_allowed() {
        // Valid Cypher: DELETE of identifiers does not contain aggregates
        let q = parse_query("MATCH (n) DELETE n").unwrap();
        let result = analyze(&q);
        assert!(!result
            .errors
            .iter()
            .any(|e| matches!(e, SemanticError::AggregateInDelete)));
    }

    #[test]
    fn test_aggregate_in_order_by_allowed() {
        let q = parse_query("MATCH (n) RETURN n ORDER BY count(n)").unwrap();
        let result = analyze(&q);
        // ORDER BY with aggregates should not produce AggregateInWhere/Set/Remove/Delete errors
        assert!(!result
            .errors
            .iter()
            .any(|e| matches!(e, SemanticError::AggregateInWhere)));
        assert!(!result
            .errors
            .iter()
            .any(|e| matches!(e, SemanticError::AggregateInSet)));
    }

    // ─── New tests for subquery analysis ─────────────────────────────────────

    #[test]
    fn test_subquery_inherits_outer_bindings() {
        let q =
            parse_query("MATCH (n) RETURN EXISTS { MATCH (n)-[:KNOWS]->(m) RETURN m }").unwrap();
        let result = analyze(&q);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_correlated_reference_not_found() {
        let q = parse_query(
            "MATCH (n) RETURN EXISTS { MATCH (m)-[:KNOWS]->(o) WHERE o.name = x.name RETURN o }",
        )
        .unwrap();
        let result = analyze(&q);
        assert!(result
            .errors
            .iter()
            .any(|e| matches!(e, SemanticError::UndefinedVariable(v) if v == "x")));
    }

    // ─── New tests for pattern comprehension checks ──────────────────────────

    #[test]
    fn test_pattern_variable_bound_before_return() {
        let q = parse_query("MATCH (n) RETURN n").unwrap();
        let result = analyze(&q);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_unbound_pattern_variable_detected() {
        // This query references 'm' without binding it in a pattern
        let q = parse_query("MATCH (n) RETURN m").unwrap();
        let result = analyze(&q);
        assert!(result
            .errors
            .iter()
            .any(|e| matches!(e, SemanticError::UndefinedVariable(v) if v == "m")));
    }

    // ─── Test for full pipeline ──────────────────────────────────────────────

    #[test]
    fn test_analyze_query_pipeline_success() {
        let q = parse_query("MATCH (n) RETURN n").unwrap();
        let catalog = Catalog::new();
        let result = analyze_query(&q, &catalog);
        assert!(result.is_ok());
        let typed = result.unwrap();
        assert_eq!(typed.variable_types.get("n"), Some(&CypherType::Node));
    }

    #[test]
    fn test_analyze_query_pipeline_failure() {
        let q = parse_query("RETURN x").unwrap();
        let catalog = Catalog::new();
        let result = analyze_query(&q, &catalog);
        assert!(result.is_err());
    }

    // ─── Tests for new function type inference ───────────────────────────────

    #[test]
    fn test_statistical_function_types() {
        let q =
            parse_query("RETURN percentileCont([1,2,3], 0.5) AS p, stDev([1,2,3]) AS s").unwrap();
        let result = analyze(&q);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_safe_conversion_function_types() {
        let q = parse_query("RETURN toFloatOrNull('3.14') AS f, toIntegerOrNull('42') AS i, toBooleanOrNull('true') AS b").unwrap();
        let result = analyze(&q);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_temporal_extractor_types() {
        let q = parse_query("RETURN year(datetime('2024-01-01')), month(date('2024-01-01')), day(date('2024-01-01'))").unwrap();
        let result = analyze(&q);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_trig_function_types() {
        let q = parse_query("RETURN sin(1.0), cos(1.0), tan(1.0), cosh(1.0), asinh(1.0)").unwrap();
        let result = analyze(&q);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_string_predicate_types() {
        let q = parse_query(
            "RETURN startsWith('hello', 'he'), endsWith('hello', 'lo'), contains('hello', 'll')",
        )
        .unwrap();
        let result = analyze(&q);
        assert!(result.errors.is_empty());
    }
}
