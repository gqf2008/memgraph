//! Cypher AST types — query representation.
//!
//! Mirrors the C++ AST in `src/query/frontend/ast/ast.hpp`.

use mgcore::types::{EdgeTypeId, LabelId, PropertyId};
use std::hash::{Hash, Hasher};

// ─── Fingerprint machinery ───────────────────────────────────────────────

/// Fast zero-allocation structural hash for AST nodes.
pub trait Fingerprint {
    fn fingerprint(&self) -> u64;
}

impl Fingerprint for String {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut h);
        h.finish()
    }
}

impl Fingerprint for bool {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut h);
        h.finish()
    }
}

impl Fingerprint for i64 {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut h);
        h.finish()
    }
}

impl Fingerprint for f64 {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.to_bits().hash(&mut h);
        h.finish()
    }
}

impl Fingerprint for usize {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut h);
        h.finish()
    }
}

impl<T: Fingerprint> Fingerprint for Vec<T> {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.len().hash(&mut h);
        for item in self {
            item.fingerprint().hash(&mut h);
        }
        h.finish()
    }
}

impl<T: Fingerprint> Fingerprint for Option<T> {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        match self {
            None => 0u8.hash(&mut h),
            Some(v) => {
                1u8.hash(&mut h);
                v.fingerprint().hash(&mut h);
            }
        }
        h.finish()
    }
}

impl<T: Fingerprint> Fingerprint for Box<T> {
    fn fingerprint(&self) -> u64 {
        (**self).fingerprint()
    }
}

impl<A: Fingerprint, B: Fingerprint> Fingerprint for (A, B) {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.0.fingerprint().hash(&mut h);
        self.1.fingerprint().hash(&mut h);
        h.finish()
    }
}

impl<A: Fingerprint, B: Fingerprint, C: Fingerprint> Fingerprint for (A, B, C) {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.0.fingerprint().hash(&mut h);
        self.1.fingerprint().hash(&mut h);
        self.2.fingerprint().hash(&mut h);
        h.finish()
    }
}

impl Fingerprint for mgcore::types::LabelId {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.as_uint().hash(&mut h);
        h.finish()
    }
}

impl Fingerprint for mgcore::types::PropertyId {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.as_uint().hash(&mut h);
        h.finish()
    }
}

impl Fingerprint for mgcore::types::EdgeTypeId {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.as_uint().hash(&mut h);
        h.finish()
    }
}

macro_rules! fp_simple_enum {
    ($ty:ty; $($var:ident = $disc:literal),* $(,)?) => {
        impl Fingerprint for $ty {
            fn fingerprint(&self) -> u64 {
                let mut h = std::collections::hash_map::DefaultHasher::new();
                let disc: u8 = match self {
                    $( <$ty>::$var => $disc, )*
                };
                disc.hash(&mut h);
                h.finish()
            }
        }
    };
}

macro_rules! fp_newtype {
    ($ty:ty; $inner:ty) => {
        impl Fingerprint for $ty {
            fn fingerprint(&self) -> u64 {
                let mut h = std::collections::hash_map::DefaultHasher::new();
                <$inner>::from(*self).hash(&mut h);
                h.finish()
            }
        }
    };
}

// ─── AST types ───────────────────────────────────────────────────────────

/// Execution mode for a Cypher query.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum QueryMode {
    /// Normal execution (default).
    #[default]
    Standard,
    /// Return the query plan without executing.
    Explain,
    /// Execute and return statistics.
    Profile,
}

fp_simple_enum!(QueryMode; Standard = 0, Explain = 1, Profile = 2);

/// A complete Cypher query.
#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    pub clauses: Vec<Clause>,
    /// Optional UNION with another query.
    pub union: Option<UnionQuery>,
    /// EXPLAIN or PROFILE mode.
    pub mode: QueryMode,
    /// Optional periodic commit batch size (e.g. `USING PERIODIC COMMIT 1000`).
    pub periodic_commit: Option<usize>,
    /// Optional hop limit (e.g. `USING HOPS LIMIT 3`).
    pub hops_limit: Option<usize>,
    /// Optional index hints (e.g. `USING INDEX :Label(name)`).
    pub index_hints: Vec<IndexHint>,
}

impl Fingerprint for Query {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.clauses.fingerprint().hash(&mut h);
        self.union.fingerprint().hash(&mut h);
        self.mode.fingerprint().hash(&mut h);
        self.periodic_commit.hash(&mut h);
        self.hops_limit.hash(&mut h);
        self.index_hints.fingerprint().hash(&mut h);
        h.finish()
    }
}

/// An index hint: `USING INDEX :Label(property)`.
#[derive(Clone, Debug, PartialEq)]
pub struct IndexHint {
    pub label: LabelId,
    pub property: Option<PropertyId>,
}

impl Fingerprint for IndexHint {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        u32::from(self.label).hash(&mut h);
        self.property.map(|p| u32::from(p)).hash(&mut h);
        h.finish()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct UnionQuery {
    pub right: Box<Query>,
    pub all: bool,
}

impl Fingerprint for UnionQuery {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.right.fingerprint().hash(&mut h);
        self.all.hash(&mut h);
        h.finish()
    }
}

/// Top-level query clause.
#[derive(Clone, Debug, PartialEq)]
pub enum Clause {
    /// MATCH pattern WHERE expr
    Match {
        pattern: MatchPattern,
        where_clause: Option<Expression>,
    },
    /// OPTIONAL MATCH pattern WHERE expr
    OptionalMatch {
        pattern: MatchPattern,
        where_clause: Option<Expression>,
    },
    /// CREATE pattern
    Create {
        pattern: CreatePattern,
    },
    /// MERGE pattern
    Merge {
        pattern: MergePattern,
    },
    /// DELETE expressions
    Delete {
        expressions: Vec<Expression>,
        detach: bool,
    },
    /// SET assignments
    Set {
        items: Vec<SetItem>,
    },
    /// REMOVE items
    Remove {
        items: Vec<RemoveItem>,
    },
    /// RETURN expr AS alias, ... (or RETURN *)
    Return {
        items: Vec<ReturnItem>,
        distinct: bool,
        all: bool,
    },
    /// WITH expr AS alias, ... WHERE expr
    With {
        items: Vec<ReturnItem>,
        where_clause: Option<Expression>,
    },
    /// UNWIND expr AS alias
    Unwind {
        expression: Expression,
        alias: String,
    },
    /// ORDER BY expr ASC/DESC, ...
    OrderBy {
        items: Vec<OrderByItem>,
    },
    /// SKIP n
    Skip {
        count: Expression,
    },
    /// LIMIT n
    Limit {
        count: Expression,
    },
    /// CALL procedure(args) YIELD result
    Call {
        procedure_name: String,
        arguments: Vec<Expression>,
        yield_items: Vec<String>,
        yield_all: bool,
    },
    /// CALL { subquery } [IN TRANSACTIONS OF n ROWS]
    CallSubquery {
        query: Query,
        in_transactions: Option<usize>,
    },
    /// FOREACH (var IN list | clause ...)
    Foreach {
        variable: String,
        list: Expression,
        clauses: Vec<Clause>,
    },
    /// LOAD CSV FROM 'url' [WITH HEADERS] AS alias
    LoadCsv {
        url: String,
        with_headers: bool,
        alias: String,
    },
    /// LOAD JSONL FROM 'url' AS alias
    LoadJsonl {
        url: String,
        alias: String,
    },
    /// CREATE INDEX ON :Label(property)
    CreateIndex {
        label: LabelId,
        property: PropertyId,
    },
    /// DROP INDEX ON :Label(property)
    DropIndex {
        label: LabelId,
        property: PropertyId,
    },
    /// CREATE CONSTRAINT ON (n:Label) ASSERT n.property IS [UNIQUE|TYPED|NOT NULL]
    CreateConstraint {
        label: LabelId,
        property: PropertyId,
        constraint_type: ConstraintKind,
    },
    /// DROP CONSTRAINT ON (n:Label) ASSERT n.property IS [UNIQUE|TYPED|NOT NULL]
    DropConstraint {
        label: LabelId,
        property: PropertyId,
        constraint_type: ConstraintKind,
    },
    /// SHOW DATABASES | INDEXES | CONSTRAINTS
    Show {
        show_type: ShowType,
    },
    /// CREATE USER name IDENTIFIED BY 'password'
    CreateUser {
        username: String,
        password: String,
    },
    /// DROP USER name
    DropUser {
        username: String,
    },
    /// CREATE ROLE name
    CreateRole {
        role_name: String,
    },
    /// DROP ROLE name
    DropRole {
        role_name: String,
    },
    /// GRANT role TO user
    GrantRole {
        role_name: String,
        username: String,
    },
    /// REVOKE role FROM user
    RevokeRole {
        role_name: String,
        username: String,
    },
    /// SHOW USERS | ROLES
    ShowAuth {
        auth_type: ShowAuthType,
    },
    /// CREATE TRIGGER name ON VERTEX|EDGE CREATE|DELETE|UPDATE BEFORE|AFTER EXECUTE "stmt"
    CreateTrigger {
        name: String,
        target: TriggerTarget,
        event: TriggerEvent,
        timing: TriggerTiming,
        statement: String,
    },
    /// DROP TRIGGER name
    DropTrigger {
        name: String,
    },
    /// CREATE DATABASE name
    CreateDatabase {
        name: String,
    },
    /// DROP DATABASE name [FORCE]
    DropDatabase {
        name: String,
        force: bool,
    },
    /// SHOW DATABASE SETTING name
    ShowSetting {
        name: String,
    },
    /// SHOW DATABASE SETTINGS
    ShowSettings,
    /// SET SETTING name TO value
    SetSetting {
        name: String,
        value: Expression,
    },
    /// SHOW TRANSACTIONS
    ShowTransactions,
    /// TERMINATE TRANSACTIONS id
    TerminateTransaction {
        transaction_id: String,
    },
    /// GRANT PRIVILEGE ... TO (USER|ROLE)? name
    GrantPrivilege {
        privileges: Vec<Privilege>,
        target_name: String,
        is_user: bool,
    },
    /// REVOKE PRIVILEGE ... FROM (USER|ROLE)? name
    RevokePrivilege {
        privileges: Vec<Privilege>,
        target_name: String,
        is_user: bool,
    },
    /// DENY PRIVILEGE ... TO (USER|ROLE)? name
    DenyPrivilege {
        privileges: Vec<Privilege>,
        target_name: String,
        is_user: bool,
    },
    /// SHOW PRIVILEGES FOR (USER|ROLE)? name
    ShowPrivileges {
        target_name: String,
        is_user: bool,
    },
    /// ALTER USER name SET PASSWORD 'new_password'
    /// ALTER USER name RENAME TO new_name
    AlterUser {
        username: String,
        action: AlterUserAction,
    },
    /// BEGIN [TRANSACTION]
    BeginTransaction,
    /// COMMIT [TRANSACTION]
    CommitTransaction,
    /// ROLLBACK [TRANSACTION]
    RollbackTransaction,
    /// SET STORAGE MODE IN_MEMORY_ANALYTICAL | IN_MEMORY_TRANSACTIONAL | ON_DISK_TRANSACTIONAL
    SetStorageMode {
        mode: StorageMode,
    },
}

impl Fingerprint for Clause {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        match self {
            Clause::Match { pattern, where_clause } => {
                0u8.hash(&mut h);
                pattern.fingerprint().hash(&mut h);
                where_clause.fingerprint().hash(&mut h);
            }
            Clause::OptionalMatch { pattern, where_clause } => {
                1u8.hash(&mut h);
                pattern.fingerprint().hash(&mut h);
                where_clause.fingerprint().hash(&mut h);
            }
            Clause::Create { pattern } => {
                2u8.hash(&mut h);
                pattern.fingerprint().hash(&mut h);
            }
            Clause::Merge { pattern } => {
                3u8.hash(&mut h);
                pattern.fingerprint().hash(&mut h);
            }
            Clause::Delete { expressions, detach } => {
                4u8.hash(&mut h);
                expressions.fingerprint().hash(&mut h);
                detach.hash(&mut h);
            }
            Clause::Set { items } => {
                5u8.hash(&mut h);
                items.fingerprint().hash(&mut h);
            }
            Clause::Remove { items } => {
                6u8.hash(&mut h);
                items.fingerprint().hash(&mut h);
            }
            Clause::Return { items, distinct, all } => {
                7u8.hash(&mut h);
                items.fingerprint().hash(&mut h);
                distinct.hash(&mut h);
                all.hash(&mut h);
            }
            Clause::With { items, where_clause } => {
                8u8.hash(&mut h);
                items.fingerprint().hash(&mut h);
                where_clause.fingerprint().hash(&mut h);
            }
            Clause::Unwind { expression, alias } => {
                9u8.hash(&mut h);
                expression.fingerprint().hash(&mut h);
                alias.hash(&mut h);
            }
            Clause::OrderBy { items } => {
                10u8.hash(&mut h);
                items.fingerprint().hash(&mut h);
            }
            Clause::Skip { count } => {
                11u8.hash(&mut h);
                count.fingerprint().hash(&mut h);
            }
            Clause::Limit { count } => {
                12u8.hash(&mut h);
                count.fingerprint().hash(&mut h);
            }
            Clause::Call { procedure_name, arguments, yield_items, yield_all } => {
                13u8.hash(&mut h);
                procedure_name.hash(&mut h);
                arguments.fingerprint().hash(&mut h);
                yield_items.hash(&mut h);
                yield_all.hash(&mut h);
            }
            Clause::CallSubquery { query, in_transactions } => {
                14u8.hash(&mut h);
                query.fingerprint().hash(&mut h);
                in_transactions.hash(&mut h);
            }
            Clause::Foreach { variable, list, clauses } => {
                15u8.hash(&mut h);
                variable.hash(&mut h);
                list.fingerprint().hash(&mut h);
                clauses.fingerprint().hash(&mut h);
            }
            Clause::LoadCsv { url, with_headers, alias } => {
                16u8.hash(&mut h);
                url.hash(&mut h);
                with_headers.hash(&mut h);
                alias.hash(&mut h);
            }
            Clause::LoadJsonl { url, alias } => {
                17u8.hash(&mut h);
                url.hash(&mut h);
                alias.hash(&mut h);
            }
            Clause::CreateIndex { label, property } => {
                18u8.hash(&mut h);
                u32::from(*label).hash(&mut h);
                u32::from(*property).hash(&mut h);
            }
            Clause::DropIndex { label, property } => {
                19u8.hash(&mut h);
                u32::from(*label).hash(&mut h);
                u32::from(*property).hash(&mut h);
            }
            Clause::CreateConstraint { label, property, constraint_type } => {
                20u8.hash(&mut h);
                u32::from(*label).hash(&mut h);
                u32::from(*property).hash(&mut h);
                constraint_type.fingerprint().hash(&mut h);
            }
            Clause::DropConstraint { label, property, constraint_type } => {
                21u8.hash(&mut h);
                u32::from(*label).hash(&mut h);
                u32::from(*property).hash(&mut h);
                constraint_type.fingerprint().hash(&mut h);
            }
            Clause::Show { show_type } => {
                22u8.hash(&mut h);
                show_type.fingerprint().hash(&mut h);
            }
            Clause::CreateUser { username, password } => {
                23u8.hash(&mut h);
                username.hash(&mut h);
                password.hash(&mut h);
            }
            Clause::DropUser { username } => {
                24u8.hash(&mut h);
                username.hash(&mut h);
            }
            Clause::CreateRole { role_name } => {
                25u8.hash(&mut h);
                role_name.hash(&mut h);
            }
            Clause::DropRole { role_name } => {
                26u8.hash(&mut h);
                role_name.hash(&mut h);
            }
            Clause::GrantRole { role_name, username } => {
                27u8.hash(&mut h);
                role_name.hash(&mut h);
                username.hash(&mut h);
            }
            Clause::RevokeRole { role_name, username } => {
                28u8.hash(&mut h);
                role_name.hash(&mut h);
                username.hash(&mut h);
            }
            Clause::ShowAuth { auth_type } => {
                29u8.hash(&mut h);
                auth_type.fingerprint().hash(&mut h);
            }
            Clause::CreateTrigger { name, target, event, timing, statement } => {
                30u8.hash(&mut h);
                name.hash(&mut h);
                target.fingerprint().hash(&mut h);
                event.fingerprint().hash(&mut h);
                timing.fingerprint().hash(&mut h);
                statement.hash(&mut h);
            }
            Clause::DropTrigger { name } => {
                31u8.hash(&mut h);
                name.hash(&mut h);
            }
            Clause::CreateDatabase { name } => {
                32u8.hash(&mut h);
                name.hash(&mut h);
            }
            Clause::DropDatabase { name, force } => {
                33u8.hash(&mut h);
                name.hash(&mut h);
                force.hash(&mut h);
            }
            Clause::ShowSetting { name } => {
                34u8.hash(&mut h);
                name.hash(&mut h);
            }
            Clause::ShowSettings => {
                35u8.hash(&mut h);
            }
            Clause::SetSetting { name, value } => {
                36u8.hash(&mut h);
                name.hash(&mut h);
                value.fingerprint().hash(&mut h);
            }
            Clause::ShowTransactions => {
                37u8.hash(&mut h);
            }
            Clause::TerminateTransaction { transaction_id } => {
                38u8.hash(&mut h);
                transaction_id.hash(&mut h);
            }
            Clause::GrantPrivilege { privileges, target_name, is_user } => {
                39u8.hash(&mut h);
                privileges.fingerprint().hash(&mut h);
                target_name.hash(&mut h);
                is_user.hash(&mut h);
            }
            Clause::RevokePrivilege { privileges, target_name, is_user } => {
                40u8.hash(&mut h);
                privileges.fingerprint().hash(&mut h);
                target_name.hash(&mut h);
                is_user.hash(&mut h);
            }
            Clause::DenyPrivilege { privileges, target_name, is_user } => {
                41u8.hash(&mut h);
                privileges.fingerprint().hash(&mut h);
                target_name.hash(&mut h);
                is_user.hash(&mut h);
            }
            Clause::ShowPrivileges { target_name, is_user } => {
                42u8.hash(&mut h);
                target_name.hash(&mut h);
                is_user.hash(&mut h);
            }
            Clause::AlterUser { username, action } => {
                43u8.hash(&mut h);
                username.hash(&mut h);
                action.fingerprint().hash(&mut h);
            }
            Clause::BeginTransaction => 44u8.hash(&mut h),
            Clause::CommitTransaction => 45u8.hash(&mut h),
            Clause::RollbackTransaction => 46u8.hash(&mut h),
            Clause::SetStorageMode { mode } => {
                47u8.hash(&mut h);
                mode.fingerprint().hash(&mut h);
            }
        }
        h.finish()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum TriggerTarget {
    Vertex,
    Edge,
}

fp_simple_enum!(TriggerTarget; Vertex = 0, Edge = 1);

#[derive(Clone, Debug, PartialEq)]
pub enum TriggerEvent {
    Create,
    Delete,
    Update,
}

fp_simple_enum!(TriggerEvent; Create = 0, Delete = 1, Update = 2);

#[derive(Clone, Debug, PartialEq)]
pub enum TriggerTiming {
    Before,
    After,
}

fp_simple_enum!(TriggerTiming; Before = 0, After = 1);

#[derive(Clone, Debug, PartialEq)]
pub enum ShowType {
    Databases,
    Indexes,
    Constraints,
    Triggers,
    NodeLabels,
    EdgeTypes,
}

fp_simple_enum!(ShowType; Databases = 0, Indexes = 1, Constraints = 2, Triggers = 3, NodeLabels = 4, EdgeTypes = 5);

#[derive(Clone, Debug, PartialEq)]
pub enum ShowAuthType {
    Users,
    Roles,
}

fp_simple_enum!(ShowAuthType; Users = 0, Roles = 1);

/// Constraint kind for DDL.
#[derive(Clone, Debug, PartialEq)]
pub enum ConstraintKind {
    Unique,
    Exists,
    Type { expected: String },
}

impl Fingerprint for ConstraintKind {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        match self {
            ConstraintKind::Unique => 0u8.hash(&mut h),
            ConstraintKind::Exists => 1u8.hash(&mut h),
            ConstraintKind::Type { expected } => {
                2u8.hash(&mut h);
                expected.hash(&mut h);
            }
        }
        h.finish()
    }
}

/// System-level privilege for GRANT/REVOKE/DENY.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Privilege {
    Create,
    Delete,
    Match,
    Merge,
    Set,
    Remove,
    Index,
    Stats,
    Auth,
    Constraint,
    Dump,
    Replication,
    Durability,
    ReadFile,
    FreeMemory,
    Trigger,
    Config,
    Stream,
    ModuleRead,
    ModuleWrite,
    Websocket,
    StorageMode,
    TransactionManagement,
    MultiDatabaseEdit,
    MultiDatabaseUse,
    Coordinator,
    ImpersonateUser,
    ProfileRestriction,
    ParallelExecution,
    ServerSideParameters,
    ServerSideDescriptions,
}

fp_simple_enum!(Privilege;
    Create = 0, Delete = 1, Match = 2, Merge = 3,
    Set = 4, Remove = 5, Index = 6, Stats = 7,
    Auth = 8, Constraint = 9, Dump = 10, Replication = 11,
    Durability = 12, ReadFile = 13, FreeMemory = 14,
    Trigger = 15, Config = 16, Stream = 17,
    ModuleRead = 18, ModuleWrite = 19, Websocket = 20,
    StorageMode = 21, TransactionManagement = 22,
    MultiDatabaseEdit = 23, MultiDatabaseUse = 24,
    Coordinator = 25, ImpersonateUser = 26,
    ProfileRestriction = 27, ParallelExecution = 28,
    ServerSideParameters = 29, ServerSideDescriptions = 30
);

impl Privilege {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "CREATE" => Some(Self::Create),
            "DELETE" => Some(Self::Delete),
            "MATCH" => Some(Self::Match),
            "MERGE" => Some(Self::Merge),
            "SET" => Some(Self::Set),
            "REMOVE" => Some(Self::Remove),
            "INDEX" => Some(Self::Index),
            "STATS" => Some(Self::Stats),
            "AUTH" => Some(Self::Auth),
            "CONSTRAINT" => Some(Self::Constraint),
            "DUMP" => Some(Self::Dump),
            "REPLICATION" => Some(Self::Replication),
            "DURABILITY" => Some(Self::Durability),
            "READ_FILE" => Some(Self::ReadFile),
            "FREE_MEMORY" => Some(Self::FreeMemory),
            "TRIGGER" => Some(Self::Trigger),
            "CONFIG" => Some(Self::Config),
            "STREAM" => Some(Self::Stream),
            "MODULE_READ" => Some(Self::ModuleRead),
            "MODULE_WRITE" => Some(Self::ModuleWrite),
            "WEBSOCKET" => Some(Self::Websocket),
            "STORAGE_MODE" => Some(Self::StorageMode),
            "TRANSACTION_MANAGEMENT" => Some(Self::TransactionManagement),
            "MULTI_DATABASE_EDIT" => Some(Self::MultiDatabaseEdit),
            "MULTI_DATABASE_USE" => Some(Self::MultiDatabaseUse),
            "COORDINATOR" => Some(Self::Coordinator),
            "IMPERSONATE_USER" => Some(Self::ImpersonateUser),
            "PROFILE_RESTRICTION" => Some(Self::ProfileRestriction),
            "PARALLEL_EXECUTION" => Some(Self::ParallelExecution),
            "SERVER_SIDE_PARAMETERS" => Some(Self::ServerSideParameters),
            "SERVER_SIDE_DESCRIPTIONS" => Some(Self::ServerSideDescriptions),
            _ => None,
        }
    }

    pub fn all() -> Vec<Privilege> {
        vec![
            Self::Create, Self::Delete, Self::Match, Self::Merge,
            Self::Set, Self::Remove, Self::Index, Self::Stats,
            Self::Auth, Self::Constraint, Self::Dump, Self::Replication,
            Self::Durability, Self::ReadFile, Self::FreeMemory,
            Self::Trigger, Self::Config, Self::Stream,
            Self::ModuleRead, Self::ModuleWrite, Self::Websocket,
            Self::StorageMode, Self::TransactionManagement,
            Self::MultiDatabaseEdit, Self::MultiDatabaseUse,
            Self::Coordinator, Self::ImpersonateUser,
            Self::ProfileRestriction, Self::ParallelExecution,
            Self::ServerSideParameters, Self::ServerSideDescriptions,
        ]
    }
}

/// ALTER USER action.
#[derive(Clone, Debug, PartialEq)]
pub enum AlterUserAction {
    SetPassword { password: String },
    RenameTo { new_name: String },
}

impl Fingerprint for AlterUserAction {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        match self {
            AlterUserAction::SetPassword { password } => {
                0u8.hash(&mut h);
                password.hash(&mut h);
            }
            AlterUserAction::RenameTo { new_name } => {
                1u8.hash(&mut h);
                new_name.hash(&mut h);
            }
        }
        h.finish()
    }
}

/// Storage mode for SET STORAGE MODE.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageMode {
    InMemoryAnalytical,
    InMemoryTransactional,
    OnDiskTransactional,
}

fp_simple_enum!(StorageMode; InMemoryAnalytical = 0, InMemoryTransactional = 1, OnDiskTransactional = 2);

/// Pattern in a MATCH clause.
#[derive(Clone, Debug, PartialEq)]
pub struct MatchPattern {
    pub elements: Vec<PatternElement>,
}

impl Fingerprint for MatchPattern {
    fn fingerprint(&self) -> u64 {
        self.elements.fingerprint()
    }
}

/// Pattern element: [path_alias =] (node)-[edge]-(node)
#[derive(Clone, Debug, PartialEq)]
pub struct PatternElement {
    pub path_alias: Option<String>,
    pub node: NodePattern,
    pub edges: Vec<(EdgePattern, NodePattern)>,
}

impl Fingerprint for PatternElement {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.path_alias.hash(&mut h);
        self.node.fingerprint().hash(&mut h);
        self.edges.fingerprint().hash(&mut h);
        h.finish()
    }
}

/// Node pattern: (alias:Label {prop: val})
#[derive(Clone, Debug, PartialEq)]
pub struct NodePattern {
    pub alias: Option<String>,
    pub labels: Vec<LabelId>,
    pub properties: Vec<(PropertyId, Expression)>,
}

impl Fingerprint for NodePattern {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.alias.hash(&mut h);
        self.labels.len().hash(&mut h);
        for lbl in &self.labels {
            u32::from(*lbl).hash(&mut h);
        }
        self.properties.fingerprint().hash(&mut h);
        h.finish()
    }
}

/// Edge pattern: -[alias:TYPE {prop: val}*min..max]->
#[derive(Clone, Debug, PartialEq)]
pub struct EdgePattern {
    pub alias: Option<String>,
    pub edge_types: Vec<EdgeTypeId>,
    pub properties: Vec<(PropertyId, Expression)>,
    pub direction: Direction,
    /// Variable-length bounds. None means fixed single hop (default).
    /// Some((min, max)) where max=None means unbounded.
    pub var_length: Option<(usize, Option<usize>)>,
    /// Path traversal algorithm (BFS, WShortest, AllShortest, KShortest, or Default DFS).
    pub path_algorithm: PathAlgorithm,
    /// Limit expression for KShortest path expansion (e.g. `| 3`).
    pub kshortest_limit: Option<Expression>,
}

impl Fingerprint for EdgePattern {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.alias.hash(&mut h);
        self.edge_types.len().hash(&mut h);
        for et in &self.edge_types {
            u32::from(*et).hash(&mut h);
        }
        self.properties.fingerprint().hash(&mut h);
        self.direction.fingerprint().hash(&mut h);
        self.var_length.hash(&mut h);
        self.path_algorithm.fingerprint().hash(&mut h);
        self.kshortest_limit.fingerprint().hash(&mut h);
        h.finish()
    }
}

/// Edge direction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Left,
    Right,
    Either,
}

fp_simple_enum!(Direction; Left = 0, Right = 1, Either = 2);

/// Path traversal algorithm for variable-length edges.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PathAlgorithm {
    /// Standard DFS (default).
    Default,
    /// Breadth-first search.
    Bfs,
    /// Weighted shortest path.
    WShortest,
    /// All shortest paths.
    AllShortest,
    /// K shortest paths (Yen's algorithm).
    KShortest,
}

fp_simple_enum!(PathAlgorithm; Default = 0, Bfs = 1, WShortest = 2, AllShortest = 3, KShortest = 4);

/// Pattern in a CREATE clause.
#[derive(Clone, Debug, PartialEq)]
pub struct CreatePattern {
    pub elements: Vec<PatternElement>,
}

impl Fingerprint for CreatePattern {
    fn fingerprint(&self) -> u64 {
        self.elements.fingerprint()
    }
}

/// Pattern in a MERGE clause (same as MATCH but with ON CREATE/MATCH).
#[derive(Clone, Debug, PartialEq)]
pub struct MergePattern {
    pub pattern: MatchPattern,
    pub on_create: Vec<SetItem>,
    pub on_match: Vec<SetItem>,
}

impl Fingerprint for MergePattern {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.pattern.fingerprint().hash(&mut h);
        self.on_create.fingerprint().hash(&mut h);
        self.on_match.fingerprint().hash(&mut h);
        h.finish()
    }
}

/// SET clause item.
#[derive(Clone, Debug, PartialEq)]
pub enum SetItem {
    /// n.prop = expr
    Property {
        expression: Expression,
        key: PropertyId,
        value: Expression,
    },
    /// n = map
    Variable {
        alias: String,
        expression: Expression,
    },
    /// n += map
    VariableUpdate {
        alias: String,
        expression: Expression,
    },
    /// n:Label
    Label {
        alias: String,
        label: LabelId,
    },
}

impl Fingerprint for SetItem {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        match self {
            SetItem::Property { expression, key, value } => {
                0u8.hash(&mut h);
                expression.fingerprint().hash(&mut h);
                u32::from(*key).hash(&mut h);
                value.fingerprint().hash(&mut h);
            }
            SetItem::Variable { alias, expression } => {
                1u8.hash(&mut h);
                alias.hash(&mut h);
                expression.fingerprint().hash(&mut h);
            }
            SetItem::VariableUpdate { alias, expression } => {
                2u8.hash(&mut h);
                alias.hash(&mut h);
                expression.fingerprint().hash(&mut h);
            }
            SetItem::Label { alias, label } => {
                3u8.hash(&mut h);
                alias.hash(&mut h);
                u32::from(*label).hash(&mut h);
            }
        }
        h.finish()
    }
}

/// REMOVE clause item.
#[derive(Clone, Debug, PartialEq)]
pub enum RemoveItem {
    /// n.prop
    Property {
        expression: Expression,
        key: PropertyId,
    },
    /// n:Label
    Label {
        alias: String,
        label: LabelId,
    },
}

impl Fingerprint for RemoveItem {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        match self {
            RemoveItem::Property { expression, key } => {
                0u8.hash(&mut h);
                expression.fingerprint().hash(&mut h);
                u32::from(*key).hash(&mut h);
            }
            RemoveItem::Label { alias, label } => {
                1u8.hash(&mut h);
                alias.hash(&mut h);
                u32::from(*label).hash(&mut h);
            }
        }
        h.finish()
    }
}

/// RETURN/WITH clause item.
#[derive(Clone, Debug, PartialEq)]
pub struct ReturnItem {
    pub expression: Expression,
    pub alias: Option<String>,
}

impl Fingerprint for ReturnItem {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.expression.fingerprint().hash(&mut h);
        self.alias.hash(&mut h);
        h.finish()
    }
}

/// ORDER BY clause item.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderByItem {
    pub expression: Expression,
    pub ascending: bool,
}

impl Fingerprint for OrderByItem {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.expression.fingerprint().hash(&mut h);
        self.ascending.hash(&mut h);
        h.finish()
    }
}

// ─── Expressions ─────────────────────────────────────────────────────────

/// Expression types — mirrors C++ Expression hierarchy.
#[derive(Clone, Debug, PartialEq)]
pub enum Expression {
    /// Literal null
    Null,
    /// true/false
    Bool(bool),
    /// Integer literal
    Int(i64),
    /// Float literal
    Double(f64),
    /// String literal
    String(String),
    /// List literal [a, b, c]
    List(Vec<Expression>),
    /// Map literal {key: val, ...}
    Map(Vec<(String, Expression)>),
    /// Map projection {n.*, extra: 1} or {n.name, n.age}
    MapProjection {
        object: Box<Expression>,
        all: bool,
        extra: Vec<(String, Expression)>,
    },

    /// Variable reference (alias from MATCH/CREATE)
    Identifier(String),
    /// Query parameter ($name)
    Parameter(String),
    /// n.prop property access
    Property {
        object: Box<Expression>,
        key: PropertyId,
    },
    /// n.label label check
    Label {
        object: Box<Expression>,
        label: LabelId,
    },

    // Arithmetic
    Add(Box<Expression>, Box<Expression>),
    Sub(Box<Expression>, Box<Expression>),
    Mul(Box<Expression>, Box<Expression>),
    Div(Box<Expression>, Box<Expression>),
    Mod(Box<Expression>, Box<Expression>),
    Neg(Box<Expression>),

    // Comparison
    Eq(Box<Expression>, Box<Expression>),
    Neq(Box<Expression>, Box<Expression>),
    Lt(Box<Expression>, Box<Expression>),
    Gt(Box<Expression>, Box<Expression>),
    Lte(Box<Expression>, Box<Expression>),
    Gte(Box<Expression>, Box<Expression>),
    IsNull(Box<Expression>),
    IsNotNull(Box<Expression>),

    // Logical
    And(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),

    // Other
    In(Box<Expression>, Box<Expression>),
    StartsWith(Box<Expression>, Box<Expression>),
    EndsWith(Box<Expression>, Box<Expression>),
    Contains(Box<Expression>, Box<Expression>),
    RegexMatch(Box<Expression>, Box<Expression>),

    // Function call
    Function {
        name: String,
        arguments: Vec<Expression>,
        distinct: bool,
    },

    // Count star
    CountStar,

    // CASE expression
    Case {
        expression: Option<Box<Expression>>,
        whens: Vec<(Expression, Expression)>,
        else_branch: Option<Box<Expression>>,
    },

    // EXISTS { subquery }
    Exists(Box<Query>),

    // COUNT { subquery }
    CountSubquery(Box<Query>),

    // List predicates: ALL(x IN list WHERE expr), ANY(x IN list WHERE expr), etc.
    All {
        variable: String,
        list: Box<Expression>,
        predicate: Box<Expression>,
    },
    Any {
        variable: String,
        list: Box<Expression>,
        predicate: Box<Expression>,
    },
    None {
        variable: String,
        list: Box<Expression>,
        predicate: Box<Expression>,
    },
    Single {
        variable: String,
        list: Box<Expression>,
        predicate: Box<Expression>,
    },
    Filter {
        variable: String,
        list: Box<Expression>,
        predicate: Box<Expression>,
    },
    Extract {
        variable: String,
        list: Box<Expression>,
        expression: Box<Expression>,
    },
    Reduce {
        accumulator: String,
        initial: Box<Expression>,
        variable: String,
        list: Box<Expression>,
        expression: Box<Expression>,
    },

    /// Pattern comprehension: [(a)-[:R]->(b) WHERE b.x > 0 | b.name]
    PatternComprehension {
        pattern: MatchPattern,
        where_clause: Option<Box<Expression>>,
        expression: Box<Expression>,
    },

    /// List or string index: object[index]
    Index {
        object: Box<Expression>,
        index: Box<Expression>,
    },
    /// List slice: object[start..end]
    Slice {
        object: Box<Expression>,
        start: Box<Expression>,
        end: Box<Expression>,
    },
}

impl Fingerprint for Expression {
    fn fingerprint(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        match self {
            Expression::Null => 0u8.hash(&mut h),
            Expression::Bool(v) => { 1u8.hash(&mut h); v.hash(&mut h); }
            Expression::Int(v) => { 2u8.hash(&mut h); v.hash(&mut h); }
            Expression::Double(v) => { 3u8.hash(&mut h); v.to_bits().hash(&mut h); }
            Expression::String(v) => { 4u8.hash(&mut h); v.hash(&mut h); }
            Expression::List(v) => { 5u8.hash(&mut h); v.fingerprint().hash(&mut h); }
            Expression::Map(v) => { 6u8.hash(&mut h); v.fingerprint().hash(&mut h); }
            Expression::MapProjection { object, all, extra } => {
                7u8.hash(&mut h);
                object.fingerprint().hash(&mut h);
                all.hash(&mut h);
                extra.fingerprint().hash(&mut h);
            }
            Expression::Identifier(v) => { 8u8.hash(&mut h); v.hash(&mut h); }
            Expression::Parameter(v) => { 9u8.hash(&mut h); v.hash(&mut h); }
            Expression::Property { object, key } => {
                10u8.hash(&mut h);
                object.fingerprint().hash(&mut h);
                u32::from(*key).hash(&mut h);
            }
            Expression::Label { object, label } => {
                11u8.hash(&mut h);
                object.fingerprint().hash(&mut h);
                u32::from(*label).hash(&mut h);
            }
            Expression::Add(a, b) => { 12u8.hash(&mut h); a.fingerprint().hash(&mut h); b.fingerprint().hash(&mut h); }
            Expression::Sub(a, b) => { 13u8.hash(&mut h); a.fingerprint().hash(&mut h); b.fingerprint().hash(&mut h); }
            Expression::Mul(a, b) => { 14u8.hash(&mut h); a.fingerprint().hash(&mut h); b.fingerprint().hash(&mut h); }
            Expression::Div(a, b) => { 15u8.hash(&mut h); a.fingerprint().hash(&mut h); b.fingerprint().hash(&mut h); }
            Expression::Mod(a, b) => { 16u8.hash(&mut h); a.fingerprint().hash(&mut h); b.fingerprint().hash(&mut h); }
            Expression::Neg(a) => { 17u8.hash(&mut h); a.fingerprint().hash(&mut h); }
            Expression::Eq(a, b) => { 18u8.hash(&mut h); a.fingerprint().hash(&mut h); b.fingerprint().hash(&mut h); }
            Expression::Neq(a, b) => { 19u8.hash(&mut h); a.fingerprint().hash(&mut h); b.fingerprint().hash(&mut h); }
            Expression::Lt(a, b) => { 20u8.hash(&mut h); a.fingerprint().hash(&mut h); b.fingerprint().hash(&mut h); }
            Expression::Gt(a, b) => { 21u8.hash(&mut h); a.fingerprint().hash(&mut h); b.fingerprint().hash(&mut h); }
            Expression::Lte(a, b) => { 22u8.hash(&mut h); a.fingerprint().hash(&mut h); b.fingerprint().hash(&mut h); }
            Expression::Gte(a, b) => { 23u8.hash(&mut h); a.fingerprint().hash(&mut h); b.fingerprint().hash(&mut h); }
            Expression::IsNull(a) => { 24u8.hash(&mut h); a.fingerprint().hash(&mut h); }
            Expression::IsNotNull(a) => { 25u8.hash(&mut h); a.fingerprint().hash(&mut h); }
            Expression::And(a, b) => { 26u8.hash(&mut h); a.fingerprint().hash(&mut h); b.fingerprint().hash(&mut h); }
            Expression::Or(a, b) => { 27u8.hash(&mut h); a.fingerprint().hash(&mut h); b.fingerprint().hash(&mut h); }
            Expression::Not(a) => { 28u8.hash(&mut h); a.fingerprint().hash(&mut h); }
            Expression::In(a, b) => { 29u8.hash(&mut h); a.fingerprint().hash(&mut h); b.fingerprint().hash(&mut h); }
            Expression::StartsWith(a, b) => { 30u8.hash(&mut h); a.fingerprint().hash(&mut h); b.fingerprint().hash(&mut h); }
            Expression::EndsWith(a, b) => { 31u8.hash(&mut h); a.fingerprint().hash(&mut h); b.fingerprint().hash(&mut h); }
            Expression::Contains(a, b) => { 32u8.hash(&mut h); a.fingerprint().hash(&mut h); b.fingerprint().hash(&mut h); }
            Expression::RegexMatch(a, b) => { 33u8.hash(&mut h); a.fingerprint().hash(&mut h); b.fingerprint().hash(&mut h); }
            Expression::Function { name, arguments, distinct } => {
                34u8.hash(&mut h);
                name.hash(&mut h);
                arguments.fingerprint().hash(&mut h);
                distinct.hash(&mut h);
            }
            Expression::CountStar => 35u8.hash(&mut h),
            Expression::Case { expression, whens, else_branch } => {
                36u8.hash(&mut h);
                expression.fingerprint().hash(&mut h);
                whens.fingerprint().hash(&mut h);
                else_branch.fingerprint().hash(&mut h);
            }
            Expression::Exists(q) => { 37u8.hash(&mut h); q.fingerprint().hash(&mut h); }
            Expression::CountSubquery(q) => { 38u8.hash(&mut h); q.fingerprint().hash(&mut h); }
            Expression::All { variable, list, predicate } => {
                39u8.hash(&mut h);
                variable.hash(&mut h);
                list.fingerprint().hash(&mut h);
                predicate.fingerprint().hash(&mut h);
            }
            Expression::Any { variable, list, predicate } => {
                40u8.hash(&mut h);
                variable.hash(&mut h);
                list.fingerprint().hash(&mut h);
                predicate.fingerprint().hash(&mut h);
            }
            Expression::None { variable, list, predicate } => {
                41u8.hash(&mut h);
                variable.hash(&mut h);
                list.fingerprint().hash(&mut h);
                predicate.fingerprint().hash(&mut h);
            }
            Expression::Single { variable, list, predicate } => {
                42u8.hash(&mut h);
                variable.hash(&mut h);
                list.fingerprint().hash(&mut h);
                predicate.fingerprint().hash(&mut h);
            }
            Expression::Filter { variable, list, predicate } => {
                43u8.hash(&mut h);
                variable.hash(&mut h);
                list.fingerprint().hash(&mut h);
                predicate.fingerprint().hash(&mut h);
            }
            Expression::Extract { variable, list, expression } => {
                44u8.hash(&mut h);
                variable.hash(&mut h);
                list.fingerprint().hash(&mut h);
                expression.fingerprint().hash(&mut h);
            }
            Expression::Reduce { accumulator, initial, variable, list, expression } => {
                45u8.hash(&mut h);
                accumulator.hash(&mut h);
                initial.fingerprint().hash(&mut h);
                variable.hash(&mut h);
                list.fingerprint().hash(&mut h);
                expression.fingerprint().hash(&mut h);
            }
            Expression::PatternComprehension { pattern, where_clause, expression } => {
                46u8.hash(&mut h);
                pattern.fingerprint().hash(&mut h);
                where_clause.fingerprint().hash(&mut h);
                expression.fingerprint().hash(&mut h);
            }
            Expression::Index { object, index } => {
                47u8.hash(&mut h);
                object.fingerprint().hash(&mut h);
                index.fingerprint().hash(&mut h);
            }
            Expression::Slice { object, start, end } => {
                48u8.hash(&mut h);
                object.fingerprint().hash(&mut h);
                start.fingerprint().hash(&mut h);
                end.fingerprint().hash(&mut h);
            }
        }
        h.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fingerprint_stable() {
        let q1 = Query {
            clauses: vec![Clause::Match {
                pattern: MatchPattern { elements: vec![] },
                where_clause: None,
            }],
            union: None,
            mode: QueryMode::Standard,
            periodic_commit: None,
            hops_limit: None,
            index_hints: vec![],
        };
        let q2 = q1.clone();
        assert_eq!(q1.fingerprint(), q2.fingerprint());
    }

    #[test]
    fn test_fingerprint_differentiates() {
        let q1 = Query {
            clauses: vec![Clause::Match {
                pattern: MatchPattern { elements: vec![] },
                where_clause: None,
            }],
            union: None,
            mode: QueryMode::Standard,
            periodic_commit: None,
            hops_limit: None,
            index_hints: vec![],
        };
        let q2 = Query {
            clauses: vec![Clause::Create {
                pattern: CreatePattern { elements: vec![] },
            }],
            union: None,
            mode: QueryMode::Standard,
            periodic_commit: None,
            hops_limit: None,
            index_hints: vec![],
        };
        assert_ne!(q1.fingerprint(), q2.fingerprint());
    }

    #[test]
    fn test_fingerprint_expression_literals() {
        let e1 = Expression::Int(42);
        let e2 = Expression::Int(42);
        let e3 = Expression::Int(43);
        assert_eq!(e1.fingerprint(), e2.fingerprint());
        assert_ne!(e1.fingerprint(), e3.fingerprint());
    }

    #[test]
    fn test_fingerprint_f64_nan() {
        let e1 = Expression::Double(f64::NAN);
        let e2 = Expression::Double(f64::NAN);
        assert_eq!(e1.fingerprint(), e2.fingerprint());
    }
}
