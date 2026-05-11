//! Cypher recursive descent parser.

use crate::ast::*;
use crate::lexer::{Lexer, Token};
use mgcatalog::Catalog;
use mgcore::types::{EdgeTypeId, LabelId, PropertyId};

pub struct Parser<'a> {
    pos: usize,
    tokens: Vec<Token>,
    catalog: Option<&'a Catalog>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ParseError {
    pub message: String,
    pub pos: usize,
}

impl<'a> Parser<'a> {
    fn new(tokens: Vec<Token>, catalog: Option<&'a Catalog>) -> Self {
        Self {
            pos: 0,
            tokens,
            catalog,
        }
    }

    fn advance(&mut self) {
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn peek_n(&self, n: usize) -> Option<&Token> {
        self.tokens.get(self.pos + n)
    }

    fn match_keyword(&mut self, keyword: Token) -> bool {
        if let Some(tok) = self.peek() {
            if std::mem::discriminant(tok) == std::mem::discriminant(&keyword) {
                self.advance();
                return true;
            }
        }
        false
    }

    fn expect(&mut self, keyword: Token) -> Result<(), ParseError> {
        let expected = Self::token_name(&keyword);
        if self.match_keyword(keyword) {
            Ok(())
        } else {
            let got = self.peek().map(|t| Self::token_name(t)).unwrap_or("EOF");
            Err(self.error(format!("expected {}, got {}", expected, got)))
        }
    }

    fn error(&self, message: impl Into<String>) -> ParseError {
        ParseError {
            message: message.into(),
            pos: self.pos,
        }
    }

    /// Advance past a token that is a keyword but should be treated as an identifier
    /// (e.g. label names, property names, aliases).
    fn advance_name(&mut self) -> Option<String> {
        match self.peek() {
            Some(Token::Identifier(s)) => {
                let name = s.clone();
                self.advance();
                Some(name)
            }
            Some(tok) if Self::is_keyword(tok) => {
                let name = Self::keyword_str(tok).to_ascii_lowercase();
                self.advance();
                Some(name)
            }
            _ => None,
        }
    }

    fn is_keyword(tok: &Token) -> bool {
        !matches!(
            tok,
            Token::Identifier(_)
                | Token::StringLiteral(_)
                | Token::IntLiteral(_)
                | Token::FloatLiteral(_)
                | Token::LParen
                | Token::RParen
                | Token::LBracket
                | Token::RBracket
                | Token::LBrace
                | Token::RBrace
                | Token::Comma
                | Token::Colon
                | Token::Semicolon
                | Token::Dot
                | Token::DotDot
                | Token::Plus
                | Token::Minus
                | Token::Star
                | Token::Slash
                | Token::Percent
                | Token::Eq
                | Token::Neq
                | Token::Lt
                | Token::Gt
                | Token::Lte
                | Token::Gte
                | Token::ArrowRight
                | Token::ArrowLeft
                | Token::PlusEq
                | Token::Tilde
                | Token::Pipe
        )
    }

    fn keyword_str(tok: &Token) -> String {
        match tok {
            Token::Match => "MATCH".into(),
            Token::Optional => "OPTIONAL".into(),
            Token::Create => "CREATE".into(),
            Token::Merge => "MERGE".into(),
            Token::Delete => "DELETE".into(),
            Token::Detach => "DETACH".into(),
            Token::Set => "SET".into(),
            Token::Remove => "REMOVE".into(),
            Token::Return => "RETURN".into(),
            Token::With => "WITH".into(),
            Token::Unwind => "UNWIND".into(),
            Token::As => "AS".into(),
            Token::Where => "WHERE".into(),
            Token::Order => "ORDER".into(),
            Token::By => "BY".into(),
            Token::Asc => "ASC".into(),
            Token::Desc => "DESC".into(),
            Token::Skip => "SKIP".into(),
            Token::Limit => "LIMIT".into(),
            Token::Distinct => "DISTINCT".into(),
            Token::And => "AND".into(),
            Token::Or => "OR".into(),
            Token::Not => "NOT".into(),
            Token::In => "IN".into(),
            Token::Is => "IS".into(),
            Token::Null => "NULL".into(),
            Token::True => "TRUE".into(),
            Token::False => "FALSE".into(),
            Token::On => "ON".into(),
            Token::Call => "CALL".into(),
            Token::Yield => "YIELD".into(),
            Token::Union => "UNION".into(),
            Token::All => "ALL".into(),
            Token::Any => "ANY".into(),
            Token::None_ => "NONE".into(),
            Token::Single => "SINGLE".into(),
            Token::Filter => "FILTER".into(),
            Token::Extract => "EXTRACT".into(),
            Token::Case => "CASE".into(),
            Token::When => "WHEN".into(),
            Token::Then => "THEN".into(),
            Token::Else => "ELSE".into(),
            Token::End => "END".into(),
            Token::Exists => "EXISTS".into(),
            Token::Foreach => "FOREACH".into(),
            Token::StartsWith => "STARTS".into(),
            Token::EndsWith => "ENDS".into(),
            Token::Contains => "CONTAINS".into(),
            Token::Load => "LOAD".into(),
            Token::Csv => "CSV".into(),
            Token::Jsonl => "JSONL".into(),
            Token::From => "FROM".into(),
            Token::Headers => "HEADERS".into(),
            Token::Index => "INDEX".into(),
            Token::Drop => "DROP".into(),
            Token::Constraint => "CONSTRAINT".into(),
            Token::Assert => "ASSERT".into(),
            Token::Unique => "UNIQUE".into(),
            Token::Typed => "TYPED".into(),
            _ => format!("{:?}", tok),
        }
    }

    fn token_name(tok: &Token) -> &'static str {
        match tok {
            Token::Match => "MATCH",
            Token::Optional => "OPTIONAL",
            Token::Create => "CREATE",
            Token::Merge => "MERGE",
            Token::Delete => "DELETE",
            Token::Detach => "DETACH",
            Token::Set => "SET",
            Token::Remove => "REMOVE",
            Token::Return => "RETURN",
            Token::With => "WITH",
            Token::Unwind => "UNWIND",
            Token::As => "AS",
            Token::Where => "WHERE",
            Token::Order => "ORDER",
            Token::By => "BY",
            Token::Asc => "ASC",
            Token::Desc => "DESC",
            Token::Skip => "SKIP",
            Token::Limit => "LIMIT",
            Token::Distinct => "DISTINCT",
            Token::And => "AND",
            Token::Or => "OR",
            Token::Not => "NOT",
            Token::In => "IN",
            Token::Is => "IS",
            Token::Null => "NULL",
            Token::True => "TRUE",
            Token::False => "FALSE",
            Token::On => "ON",
            Token::Call => "CALL",
            Token::Yield => "YIELD",
            Token::Union => "UNION",
            Token::Show => "SHOW",
            Token::All => "ALL",
            Token::Any => "ANY",
            Token::None_ => "NONE",
            Token::Single => "SINGLE",
            Token::Filter => "FILTER",
            Token::Extract => "EXTRACT",
            Token::Reduce => "REDUCE",
            Token::Case => "CASE",
            Token::When => "WHEN",
            Token::Then => "THEN",
            Token::Else => "ELSE",
            Token::End => "END",
            Token::Exists => "EXISTS",
            Token::Foreach => "FOREACH",
            Token::StartsWith => "STARTS WITH",
            Token::EndsWith => "ENDS WITH",
            Token::Contains => "CONTAINS",
            Token::Load => "LOAD",
            Token::Csv => "CSV",
            Token::Jsonl => "JSONL",
            Token::From => "FROM",
            Token::Headers => "HEADERS",
            Token::Index => "INDEX",
            Token::Drop => "DROP",
            Token::Constraint => "CONSTRAINT",
            Token::Assert => "ASSERT",
            Token::Unique => "UNIQUE",
            Token::Typed => "TYPED",
            Token::LParen => "(",
            Token::RParen => ")",
            Token::LBracket => "[",
            Token::RBracket => "]",
            Token::LBrace => "{",
            Token::RBrace => "}",
            Token::Comma => ",",
            Token::Colon => ":",
            Token::Semicolon => ";",
            Token::Dot => ".",
            Token::DotDot => "..",
            Token::Plus => "+",
            Token::Minus => "-",
            Token::Star => "*",
            Token::Slash => "/",
            Token::Percent => "%",
            Token::Eq => "=",
            Token::Neq => "<>",
            Token::Lt => "<",
            Token::Gt => ">",
            Token::Lte => "<=",
            Token::Gte => ">=",
            Token::ArrowRight => "->",
            Token::ArrowLeft => "<-",
            Token::PlusEq => "+=",
            Token::Tilde => "~",
            Token::Pipe => "|",
            Token::Identifier(_) => "identifier",
            Token::StringLiteral(_) => "string",
            Token::IntLiteral(_) => "integer",
            Token::FloatLiteral(_) => "float",
            Token::Explain => "EXPLAIN",
            Token::Profile => "PROFILE",
            Token::User => "USER",
            Token::Role => "ROLE",
            Token::Grant => "GRANT",
            Token::Revoke => "REVOKE",
            Token::To => "TO",
            Token::Identified => "IDENTIFIED",
            Token::Password => "PASSWORD",
            Token::Trigger => "TRIGGER",
            Token::Before => "BEFORE",
            Token::After => "AFTER",
            Token::Execute => "EXECUTE",
            Token::Vertex => "VERTEX",
            Token::Edge => "EDGE",
            Token::Database => "DATABASE",
            Token::Force => "FORCE",
            Token::Bfs => "BFS",
            Token::WShortest => "WSHORTEST",
            Token::AllShortest => "ALLSHORTEST",
            Token::KShortest => "KSHORTEST",
            Token::ShortestPath => "SHORTESTPATH",
            Token::AllShortestPaths => "ALLSHORTESTPATHS",
            Token::Using => "USING",
            Token::Periodic => "PERIODIC",
            Token::Commit => "COMMIT",
            Token::Transactions => "TRANSACTIONS",
            Token::Of => "OF",
            Token::Rows => "ROWS",
            Token::Setting => "SETTING",
            Token::Settings => "SETTINGS",
            Token::Terminate => "TERMINATE",
            Token::Hops => "HOPS",
            Token::Begin => "BEGIN",
            Token::Rollback => "ROLLBACK",
            Token::Deny => "DENY",
            Token::Alter => "ALTER",
            Token::Rename => "RENAME",
            Token::Privilege => "PRIVILEGE",
            Token::Privileges => "PRIVILEGES",
            Token::Storage => "STORAGE",
            Token::Mode => "MODE",
            Token::Analytical => "ANALYTICAL",
            Token::Transactional => "TRANSACTIONAL",
            Token::OnDisk => "ON_DISK",
            Token::InMemory => "IN_MEMORY",
            Token::InMemoryAnalytical => "IN_MEMORY_ANALYTICAL",
            Token::InMemoryTransactional => "IN_MEMORY_TRANSACTIONAL",
            Token::OnDiskTransactional => "ON_DISK_TRANSACTIONAL",
            Token::For => "FOR",
            Token::Parameter(_) => "parameter",
        }
    }

    // ─── Name resolution ────────────────────────────────────────────────────

    fn resolve_label(&self, name: &str) -> LabelId {
        self.catalog
            .map(|c| c.label(name))
            .unwrap_or(LabelId::from_uint(0))
    }

    fn resolve_property(&self, name: &str) -> PropertyId {
        self.catalog
            .map(|c| c.property(name))
            .unwrap_or(PropertyId::from_uint(0))
    }

    fn resolve_edge_type(&self, name: &str) -> EdgeTypeId {
        self.catalog
            .map(|c| c.edge_type(name))
            .unwrap_or(EdgeTypeId::from_uint(0))
    }

    // ─── Top-level parsing ──────────────────────────────────────────────────

    fn parse_query(&mut self) -> Result<Query, ParseError> {
        let mode = if self.match_keyword(Token::Explain) {
            QueryMode::Explain
        } else if self.match_keyword(Token::Profile) {
            QueryMode::Profile
        } else {
            QueryMode::Standard
        };
        // Parse optional USING directives (can be multiple)
        let mut periodic_commit = None;
        let mut hops_limit = None;
        let mut index_hints = Vec::new();
        while self.match_keyword(Token::Using) {
            if self.match_keyword(Token::Periodic) {
                self.expect(Token::Commit)?;
                let batch_size = if let Some(Token::IntLiteral(n)) = self.peek() {
                    let n = *n as usize;
                    self.advance();
                    n.max(1)
                } else {
                    1000
                };
                periodic_commit = Some(batch_size);
            } else if self.match_keyword(Token::Hops) {
                self.expect(Token::Limit)?;
                let limit = if let Some(Token::IntLiteral(n)) = self.peek() {
                    let n = *n as usize;
                    self.advance();
                    n.max(1)
                } else {
                    return Err(self.error("expected integer after USING HOPS LIMIT"));
                };
                hops_limit = Some(limit);
            } else if self.match_keyword(Token::Index) {
                // USING INDEX :Label(property)
                self.expect(Token::Colon)?;
                let label_name = self
                    .advance_name()
                    .ok_or_else(|| self.error("expected label name after USING INDEX :"))?;
                let label = self.resolve_label(&label_name);
                let property = if self.match_keyword(Token::LParen) {
                    let prop_name = self
                        .advance_name()
                        .ok_or_else(|| self.error("expected property name in USING INDEX hint"))?;
                    self.expect(Token::RParen)?;
                    Some(self.resolve_property(&prop_name))
                } else {
                    None
                };
                index_hints.push(IndexHint { label, property });
            } else {
                return Err(
                    self.error("expected PERIODIC COMMIT, HOPS LIMIT, or INDEX after USING")
                );
            }
        }
        let mut clauses = Vec::new();
        while self.peek().is_some() {
            if self.peek() == Some(&Token::Union) {
                break;
            }
            clauses.push(self.parse_clause()?);
        }
        let union = if self.match_keyword(Token::Union) {
            let all = self.match_keyword(Token::All);
            let right = self.parse_query()?;
            Some(UnionQuery {
                right: Box::new(right),
                all,
            })
        } else {
            None
        };
        Ok(Query {
            clauses,
            union,
            mode,
            periodic_commit,
            hops_limit,
            index_hints,
        })
    }

    fn parse_clause(&mut self) -> Result<Clause, ParseError> {
        match self.peek() {
            Some(Token::Match) => self.parse_match(),
            Some(Token::Optional) => self.parse_optional_match(),
            Some(Token::Create) => {
                // Peek ahead to determine if this is CREATE (node), CREATE INDEX/CONSTRAINT,
                // CREATE USER/ROLE, CREATE TRIGGER, or CREATE DATABASE
                match self.peek_n(1) {
                    Some(Token::Index) | Some(Token::Constraint) => self.parse_create_ddl(),
                    Some(Token::User) => self.parse_create_user(),
                    Some(Token::Role) => self.parse_create_role(),
                    Some(Token::Trigger) => self.parse_create_trigger(),
                    Some(Token::Database) => self.parse_create_database(),
                    _ => self.parse_create(),
                }
            }
            Some(Token::Merge) => self.parse_merge(),
            Some(Token::Delete) | Some(Token::Detach) => self.parse_delete(),
            Some(Token::Set) => match self.peek_n(1) {
                Some(Token::Storage) => self.parse_set_storage_mode(),
                Some(Token::Setting) => self.parse_set_setting(),
                _ => self.parse_set(),
            },
            Some(Token::Remove) => self.parse_remove(),
            Some(Token::Return) => self.parse_return(),
            Some(Token::With) => self.parse_with(),
            Some(Token::Unwind) => self.parse_unwind(),
            Some(Token::Order) => self.parse_order_by(),
            Some(Token::Skip) => self.parse_skip(),
            Some(Token::Limit) => self.parse_limit(),
            Some(Token::Call) => self.parse_call(),
            Some(Token::Foreach) => self.parse_foreach(),
            Some(Token::Load) => {
                self.advance();
                match self.peek() {
                    Some(Token::Csv) => self.parse_load_csv_rest(),
                    Some(Token::Jsonl) => self.parse_load_jsonl_rest(),
                    _ => Err(self.error("expected CSV or JSONL after LOAD")),
                }
            }
            Some(Token::Drop) => match self.peek_n(1) {
                Some(Token::User) => self.parse_drop_user(),
                Some(Token::Role) => self.parse_drop_role(),
                Some(Token::Trigger) => self.parse_drop_trigger(),
                Some(Token::Database) => self.parse_drop_database(),
                _ => self.parse_drop(),
            },
            Some(Token::Show) => self.parse_show(),
            Some(Token::Grant) => self.parse_grant(),
            Some(Token::Revoke) => self.parse_revoke(),
            Some(Token::Deny) => self.parse_deny_privilege(),
            Some(Token::Alter) => self.parse_alter_user(),
            Some(Token::Begin) => self.parse_begin(),
            Some(Token::Commit) => self.parse_commit(),
            Some(Token::Rollback) => self.parse_rollback(),
            Some(Token::Terminate) => self.parse_terminate_transaction(),
            _ => Err(self.error("expected clause")),
        }
    }

    // ─── MATCH / OPTIONAL MATCH ─────────────────────────────────────────────

    fn parse_match(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Match)?;
        let pattern = self.parse_match_pattern()?;
        let where_clause = if self.match_keyword(Token::Where) {
            Some(self.parse_expression()?)
        } else {
            None
        };
        Ok(Clause::Match {
            pattern,
            where_clause,
        })
    }

    fn parse_optional_match(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Optional)?;
        self.expect(Token::Match)?;
        let pattern = self.parse_match_pattern()?;
        let where_clause = if self.match_keyword(Token::Where) {
            Some(self.parse_expression()?)
        } else {
            None
        };
        Ok(Clause::OptionalMatch {
            pattern,
            where_clause,
        })
    }

    // ─── CREATE ─────────────────────────────────────────────────────────────

    fn parse_create(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Create)?;
        let pattern = self.parse_create_pattern()?;
        Ok(Clause::Create { pattern })
    }

    // ─── MERGE ──────────────────────────────────────────────────────────────

    fn parse_merge(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Merge)?;
        let pattern = self.parse_match_pattern()?;
        let mut on_create = Vec::new();
        let mut on_match = Vec::new();
        while let Some(Token::On) = self.peek() {
            self.advance();
            if self.match_keyword(Token::Create) {
                self.expect(Token::Set)?;
                on_create.push(self.parse_set_item()?);
                while self.match_keyword(Token::Comma) {
                    on_create.push(self.parse_set_item()?);
                }
            } else if self.match_keyword(Token::Match) {
                self.expect(Token::Set)?;
                on_match.push(self.parse_set_item()?);
                while self.match_keyword(Token::Comma) {
                    on_match.push(self.parse_set_item()?);
                }
            } else {
                return Err(self.error("expected CREATE or MATCH after ON"));
            }
        }
        Ok(Clause::Merge {
            pattern: MergePattern {
                pattern,
                on_create,
                on_match,
            },
        })
    }

    // ─── DELETE ─────────────────────────────────────────────────────────────

    fn parse_delete(&mut self) -> Result<Clause, ParseError> {
        let detach = self.match_keyword(Token::Detach);
        self.expect(Token::Delete)?;
        let mut expressions = Vec::new();
        expressions.push(self.parse_expression()?);
        while self.match_keyword(Token::Comma) {
            expressions.push(self.parse_expression()?);
        }
        Ok(Clause::Delete {
            expressions,
            detach,
        })
    }

    // ─── SET ────────────────────────────────────────────────────────────────

    fn parse_set(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Set)?;
        let mut items = Vec::new();
        items.push(self.parse_set_item()?);
        while self.match_keyword(Token::Comma) {
            items.push(self.parse_set_item()?);
        }
        Ok(Clause::Set { items })
    }

    fn parse_set_item(&mut self) -> Result<SetItem, ParseError> {
        let left = self.parse_set_left()?;
        if self.match_keyword(Token::PlusEq) {
            let expr = self.parse_expression()?;
            if let Expression::Identifier(alias) = left {
                Ok(SetItem::VariableUpdate {
                    alias,
                    expression: expr,
                })
            } else {
                Err(self.error("+= only supported on variables"))
            }
        } else if self.match_keyword(Token::Eq) {
            let expr = self.parse_expression()?;
            match left {
                Expression::Property { object, key } => {
                    if let Expression::Identifier(alias) = *object {
                        Ok(SetItem::Property {
                            expression: Expression::Identifier(alias),
                            key,
                            value: expr,
                        })
                    } else {
                        Err(self.error("SET property must be on an identifier"))
                    }
                }
                Expression::Identifier(alias) => Ok(SetItem::Variable {
                    alias,
                    expression: expr,
                }),
                Expression::Label { object, label } => {
                    if let Expression::Identifier(alias) = *object {
                        Ok(SetItem::Label { alias, label })
                    } else {
                        Err(self.error("SET label must be on an identifier"))
                    }
                }
                _ => Err(self.error("invalid SET target")),
            }
        } else {
            // n:Label shorthand (no = or +=)
            match left {
                Expression::Label { object, label } => {
                    if let Expression::Identifier(alias) = *object {
                        Ok(SetItem::Label { alias, label })
                    } else {
                        Err(self.error("SET label must be on an identifier"))
                    }
                }
                _ => Err(self.error("expected = or += in SET")),
            }
        }
    }

    fn parse_set_left(&mut self) -> Result<Expression, ParseError> {
        let mut expr = self.parse_primary()?;
        loop {
            match self.peek() {
                Some(Token::Dot) => {
                    self.advance();
                    let name = self
                        .advance_name()
                        .ok_or_else(|| self.error("expected property name after ."))?;
                    let key = self.resolve_property(&name);
                    expr = Expression::Property {
                        object: Box::new(expr),
                        key,
                    };
                }
                Some(Token::Colon) => {
                    self.advance();
                    let name = self
                        .advance_name()
                        .ok_or_else(|| self.error("expected label name after :"))?;
                    let label = self.resolve_label(&name);
                    expr = Expression::Label {
                        object: Box::new(expr),
                        label,
                    };
                }
                _ => break,
            }
        }
        Ok(expr)
    }

    // ─── REMOVE ─────────────────────────────────────────────────────────────

    fn parse_remove(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Remove)?;
        let mut items = Vec::new();
        items.push(self.parse_remove_item()?);
        while self.match_keyword(Token::Comma) {
            items.push(self.parse_remove_item()?);
        }
        Ok(Clause::Remove { items })
    }

    fn parse_remove_item(&mut self) -> Result<RemoveItem, ParseError> {
        let mut expr = self.parse_primary()?;
        loop {
            match self.peek() {
                Some(Token::Dot) => {
                    self.advance();
                    let name = self
                        .advance_name()
                        .ok_or_else(|| self.error("expected property name after ."))?;
                    let key = self.resolve_property(&name);
                    expr = Expression::Property {
                        object: Box::new(expr),
                        key,
                    };
                }
                Some(Token::Colon) => {
                    self.advance();
                    let name = self
                        .advance_name()
                        .ok_or_else(|| self.error("expected label name after :"))?;
                    let label = self.resolve_label(&name);
                    // Check if there's another label after this one
                    if let Expression::Identifier(alias) = expr {
                        return Ok(RemoveItem::Label { alias, label });
                    } else {
                        return Err(self.error("REMOVE label must be on an identifier"));
                    }
                }
                _ => break,
            }
        }
        match expr {
            Expression::Property { object, key } => {
                if let Expression::Identifier(alias) = *object {
                    Ok(RemoveItem::Property {
                        expression: Expression::Identifier(alias),
                        key,
                    })
                } else {
                    Err(self.error("REMOVE property must be on an identifier"))
                }
            }
            _ => Err(self.error("invalid REMOVE target")),
        }
    }

    // ─── RETURN ─────────────────────────────────────────────────────────────

    fn parse_return(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Return)?;
        let distinct = self.match_keyword(Token::Distinct);
        let all = self.match_keyword(Token::Star);
        let mut items = Vec::new();
        if !all {
            items.push(self.parse_return_item()?);
            while self.match_keyword(Token::Comma) {
                items.push(self.parse_return_item()?);
            }
        }
        Ok(Clause::Return {
            items,
            distinct,
            all,
        })
    }

    fn parse_return_item(&mut self) -> Result<ReturnItem, ParseError> {
        let expression = self.parse_expression()?;
        let alias = if self.match_keyword(Token::As) {
            self.advance_name()
        } else {
            None
        };
        Ok(ReturnItem { expression, alias })
    }

    // ─── WITH ───────────────────────────────────────────────────────────────

    fn parse_with(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::With)?;
        let mut items = Vec::new();
        items.push(self.parse_return_item()?);
        while self.match_keyword(Token::Comma) {
            items.push(self.parse_return_item()?);
        }
        let where_clause = if self.match_keyword(Token::Where) {
            Some(self.parse_expression()?)
        } else {
            None
        };
        Ok(Clause::With {
            items,
            where_clause,
        })
    }

    // ─── UNWIND ─────────────────────────────────────────────────────────────

    fn parse_unwind(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Unwind)?;
        let expression = self.parse_expression()?;
        self.expect(Token::As)?;
        let alias = self
            .advance_name()
            .ok_or_else(|| self.error("expected alias after AS"))?;
        Ok(Clause::Unwind { expression, alias })
    }

    // ─── ORDER BY / SKIP / LIMIT ────────────────────────────────────────────

    fn parse_order_by(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Order)?;
        self.expect(Token::By)?;
        let mut items = Vec::new();
        items.push(self.parse_order_by_item()?);
        while self.match_keyword(Token::Comma) {
            items.push(self.parse_order_by_item()?);
        }
        Ok(Clause::OrderBy { items })
    }

    fn parse_order_by_item(&mut self) -> Result<OrderByItem, ParseError> {
        let expression = self.parse_expression()?;
        let ascending = if self.match_keyword(Token::Asc) {
            true
        } else {
            !self.match_keyword(Token::Desc)
        };
        Ok(OrderByItem {
            expression,
            ascending,
        })
    }

    fn parse_skip(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Skip)?;
        let count = self.parse_expression()?;
        Ok(Clause::Skip { count })
    }

    fn parse_limit(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Limit)?;
        let count = self.parse_expression()?;
        Ok(Clause::Limit { count })
    }

    // ─── CALL ───────────────────────────────────────────────────────────────

    fn parse_call(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Call)?;
        // CALL { subquery }  vs  CALL procedure_name(args)
        if self.match_keyword(Token::LBrace) {
            let mut clauses = Vec::new();
            while self.peek().is_some() && self.peek() != Some(&Token::RBrace) {
                clauses.push(self.parse_clause()?);
            }
            self.expect(Token::RBrace)?;
            let in_transactions = if self.match_keyword(Token::In) {
                self.expect(Token::Transactions)?;
                self.expect(Token::Of)?;
                let n = if let Some(Token::IntLiteral(v)) = self.peek() {
                    let v = *v as usize;
                    self.advance();
                    v.max(1)
                } else {
                    return Err(self.error("expected integer after IN TRANSACTIONS OF"));
                };
                if self.match_keyword(Token::Rows) {
                    // ok
                } else {
                    return Err(self.error("expected ROWS after IN TRANSACTIONS OF n"));
                }
                Some(n)
            } else {
                None
            };
            return Ok(Clause::CallSubquery {
                query: Query {
                    clauses,
                    union: None,
                    mode: QueryMode::Standard,
                    periodic_commit: None,
                    hops_limit: None,
                    index_hints: Vec::new(),
                },
                in_transactions,
            });
        }
        // Procedure name can be dotted: algo.triangle_count
        let mut procedure_name = self
            .advance_name()
            .ok_or_else(|| self.error("expected procedure name"))?;
        while self.match_keyword(Token::Dot) {
            let part = self
                .advance_name()
                .ok_or_else(|| self.error("expected procedure name part after ."))?;
            procedure_name.push('.');
            procedure_name.push_str(&part);
        }
        self.expect(Token::LParen)?;
        let mut arguments = Vec::new();
        if !matches!(self.peek(), Some(Token::RParen)) {
            arguments.push(self.parse_expression()?);
            while self.match_keyword(Token::Comma) {
                arguments.push(self.parse_expression()?);
            }
        }
        self.expect(Token::RParen)?;
        let mut yield_items = Vec::new();
        let mut yield_all = false;
        if self.match_keyword(Token::Yield) {
            if self.match_keyword(Token::Star) {
                yield_all = true;
            } else {
                let name = self
                    .advance_name()
                    .ok_or_else(|| self.error("expected yield item name"))?;
                yield_items.push(name);
                while self.match_keyword(Token::Comma) {
                    let name = self
                        .advance_name()
                        .ok_or_else(|| self.error("expected yield item name"))?;
                    yield_items.push(name);
                }
            }
        }
        Ok(Clause::Call {
            procedure_name,
            arguments,
            yield_items,
            yield_all,
        })
    }

    // ─── FOREACH ────────────────────────────────────────────────────────────

    fn parse_foreach(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Foreach)?;
        self.expect(Token::LParen)?;
        let variable = self
            .advance_name()
            .ok_or_else(|| self.error("expected variable name"))?;
        self.expect(Token::In)?;
        let list = self.parse_expression()?;
        self.expect(Token::Pipe)?;
        let mut clauses = Vec::new();
        // FOREACH body: one or more clauses until we hit something that looks like the end
        // In practice, this is usually CREATE or SET
        while let Some(tok) = self.peek() {
            if matches!(tok, Token::RParen) {
                break;
            }
            clauses.push(self.parse_clause()?);
        }
        self.expect(Token::RParen)?;
        Ok(Clause::Foreach {
            variable,
            list,
            clauses,
        })
    }

    // ─── LOAD CSV ───────────────────────────────────────────────────────────

    fn parse_load_csv_rest(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Csv)?;
        self.expect(Token::From)?;
        let url = match self.peek() {
            Some(Token::StringLiteral(s)) => {
                let url = s.clone();
                self.advance();
                url
            }
            _ => return Err(self.error("expected URL string after FROM")),
        };
        let with_headers = self.match_keyword(Token::With) && {
            self.expect(Token::Headers)?;
            true
        };
        self.expect(Token::As)?;
        let alias = self
            .advance_name()
            .ok_or_else(|| self.error("expected alias after AS"))?;
        Ok(Clause::LoadCsv {
            url,
            with_headers,
            alias,
        })
    }

    fn parse_load_jsonl_rest(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Jsonl)?;
        self.expect(Token::From)?;
        let url = match self.peek() {
            Some(Token::StringLiteral(s)) => {
                let url = s.clone();
                self.advance();
                url
            }
            _ => return Err(self.error("expected URL string after FROM")),
        };
        self.expect(Token::As)?;
        let alias = self
            .advance_name()
            .ok_or_else(|| self.error("expected alias after AS"))?;
        Ok(Clause::LoadJsonl { url, alias })
    }

    // ─── DDL: helpers ───────────────────────────────────────────────────────

    fn parse_constraint_kind(&mut self) -> Result<ConstraintKind, ParseError> {
        if self.match_keyword(Token::Unique) {
            Ok(ConstraintKind::Unique)
        } else if self.match_keyword(Token::Not) {
            self.advance(); // consume NULL
            Ok(ConstraintKind::Exists)
        } else if self.match_keyword(Token::Typed) {
            let expected = self
                .advance_name()
                .ok_or_else(|| self.error("expected type name after TYPED"))?;
            Ok(ConstraintKind::Type { expected })
        } else {
            Err(self.error("expected UNIQUE, NOT NULL, or TYPED"))
        }
    }

    // ─── DDL: CREATE INDEX / CONSTRAINT ─────────────────────────────────────

    fn parse_create_ddl(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Create)?;
        if self.match_keyword(Token::Index) {
            self.expect(Token::On)?;
            self.expect(Token::Colon)?;
            let label_name = self
                .advance_name()
                .ok_or_else(|| self.error("expected label name"))?;
            let label = self.resolve_label(&label_name);
            self.expect(Token::LParen)?;
            let prop_name = self
                .advance_name()
                .ok_or_else(|| self.error("expected property name"))?;
            let property = self.resolve_property(&prop_name);
            self.expect(Token::RParen)?;
            Ok(Clause::CreateIndex { label, property })
        } else if self.match_keyword(Token::Constraint) {
            self.expect(Token::On)?;
            self.expect(Token::LParen)?;
            let _alias = self.advance_name();
            self.expect(Token::Colon)?;
            let label_name = self
                .advance_name()
                .ok_or_else(|| self.error("expected label name"))?;
            let label = self.resolve_label(&label_name);
            self.expect(Token::RParen)?;
            self.expect(Token::Assert)?;
            let _var = self.advance_name();
            self.expect(Token::Dot)?;
            let prop_name = self
                .advance_name()
                .ok_or_else(|| self.error("expected property name"))?;
            let property = self.resolve_property(&prop_name);
            self.expect(Token::Is)?;
            let constraint_type = self.parse_constraint_kind()?;
            Ok(Clause::CreateConstraint {
                label,
                property,
                constraint_type,
            })
        } else {
            Err(self.error("expected INDEX or CONSTRAINT after CREATE"))
        }
    }

    // ─── DDL: DROP INDEX / CONSTRAINT ───────────────────────────────────────

    fn parse_drop(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Drop)?;
        if self.match_keyword(Token::Index) {
            self.expect(Token::On)?;
            self.expect(Token::Colon)?;
            let label_name = self
                .advance_name()
                .ok_or_else(|| self.error("expected label name"))?;
            let label = self.resolve_label(&label_name);
            self.expect(Token::LParen)?;
            let prop_name = self
                .advance_name()
                .ok_or_else(|| self.error("expected property name"))?;
            let property = self.resolve_property(&prop_name);
            self.expect(Token::RParen)?;
            Ok(Clause::DropIndex { label, property })
        } else if self.match_keyword(Token::Constraint) {
            self.expect(Token::On)?;
            self.expect(Token::LParen)?;
            let _alias = self.advance_name();
            self.expect(Token::Colon)?;
            let label_name = self
                .advance_name()
                .ok_or_else(|| self.error("expected label name"))?;
            let label = self.resolve_label(&label_name);
            self.expect(Token::RParen)?;
            self.expect(Token::Assert)?;
            let _var = self.advance_name();
            self.expect(Token::Dot)?;
            let prop_name = self
                .advance_name()
                .ok_or_else(|| self.error("expected property name"))?;
            let property = self.resolve_property(&prop_name);
            self.expect(Token::Is)?;
            let constraint_type = self.parse_constraint_kind()?;
            Ok(Clause::DropConstraint {
                label,
                property,
                constraint_type,
            })
        } else {
            Err(self.error("expected INDEX or CONSTRAINT after DROP"))
        }
    }

    fn parse_show(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Show)?;
        let name = self.advance_name()
            .ok_or_else(|| self.error("expected SHOW target (DATABASES, INDEXES, CONSTRAINTS, TRIGGERS, USERS, ROLES, TRANSACTIONS, DATABASE SETTING)"))?;
        let upper = name.to_ascii_uppercase();
        match upper.as_str() {
            "DATABASES" => Ok(Clause::Show {
                show_type: ShowType::Databases,
            }),
            "INDEXES" => Ok(Clause::Show {
                show_type: ShowType::Indexes,
            }),
            "CONSTRAINTS" => Ok(Clause::Show {
                show_type: ShowType::Constraints,
            }),
            "TRIGGERS" => Ok(Clause::Show {
                show_type: ShowType::Triggers,
            }),
            "NODE_LABELS" => {
                let info = self
                    .advance_name()
                    .ok_or_else(|| self.error("expected INFO after SHOW NODE_LABELS"))?;
                if !info.eq_ignore_ascii_case("INFO") {
                    return Err(self.error("expected INFO after SHOW NODE_LABELS"));
                }
                Ok(Clause::Show {
                    show_type: ShowType::NodeLabels,
                })
            }
            "EDGE_TYPES" => {
                let info = self
                    .advance_name()
                    .ok_or_else(|| self.error("expected INFO after SHOW EDGE_TYPES"))?;
                if !info.eq_ignore_ascii_case("INFO") {
                    return Err(self.error("expected INFO after SHOW EDGE_TYPES"));
                }
                Ok(Clause::Show {
                    show_type: ShowType::EdgeTypes,
                })
            }
            "USERS" => Ok(Clause::ShowAuth {
                auth_type: ShowAuthType::Users,
            }),
            "ROLES" => Ok(Clause::ShowAuth {
                auth_type: ShowAuthType::Roles,
            }),
            "TRANSACTIONS" => Ok(Clause::ShowTransactions),
            "PRIVILEGES" => {
                let _ = self.match_keyword(Token::For);
                let (target_name, is_user) = self.parse_grant_target()?;
                Ok(Clause::ShowPrivileges {
                    target_name,
                    is_user,
                })
            }
            "DATABASE" => match self.peek() {
                Some(Token::Settings) => {
                    self.advance();
                    Ok(Clause::ShowSettings)
                }
                Some(Token::Setting) => {
                    self.advance();
                    let setting_name = self.advance_name().ok_or_else(|| {
                        self.error("expected setting name after SHOW DATABASE SETTING")
                    })?;
                    Ok(Clause::ShowSetting { name: setting_name })
                }
                _ => Err(self.error("expected SETTING or SETTINGS after SHOW DATABASE")),
            },
            _ => Err(self.error(format!("unsupported SHOW target: {}", name))),
        }
    }

    // ─── Auth DDL parsing ───────────────────────────────────────────────────

    fn parse_create_user(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Create)?;
        self.expect(Token::User)?;
        let username = self
            .advance_name()
            .ok_or_else(|| self.error("expected username"))?;
        self.expect(Token::Identified)?;
        self.expect(Token::By)?;
        let password = if let Some(Token::StringLiteral(s)) = self.peek() {
            let s = s.clone();
            self.advance();
            s
        } else {
            return Err(self.error("expected password string"));
        };
        Ok(Clause::CreateUser { username, password })
    }

    fn parse_drop_user(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Drop)?;
        self.expect(Token::User)?;
        let username = self
            .advance_name()
            .ok_or_else(|| self.error("expected username"))?;
        Ok(Clause::DropUser { username })
    }

    fn parse_create_role(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Create)?;
        self.expect(Token::Role)?;
        let role_name = self
            .advance_name()
            .ok_or_else(|| self.error("expected role name"))?;
        Ok(Clause::CreateRole { role_name })
    }

    fn parse_drop_role(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Drop)?;
        self.expect(Token::Role)?;
        let role_name = self
            .advance_name()
            .ok_or_else(|| self.error("expected role name"))?;
        Ok(Clause::DropRole { role_name })
    }

    fn parse_grant(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Grant)?;
        // Check if next is ALL PRIVILEGES or a privilege name (not a role name followed by TO)
        // Disambiguation: GRANT role_name TO user  vs  GRANT PRIVILEGE TO role/user
        if self.match_keyword(Token::All) {
            // GRANT ALL PRIVILEGES TO target
            let _ = self.match_keyword(Token::Privileges);
            self.expect(Token::To)?;
            let (target_name, is_user) = self.parse_grant_target()?;
            Ok(Clause::GrantPrivilege {
                privileges: Privilege::all(),
                target_name,
                is_user,
            })
        } else if self.peek() == Some(&Token::Privilege) || self.peek() == Some(&Token::Privileges)
        {
            // GRANT PRIVILEGE ... TO target
            let _ = self.match_keyword(Token::Privilege) || self.match_keyword(Token::Privileges);
            let privileges = self.parse_privilege_list()?;
            self.expect(Token::To)?;
            let (target_name, is_user) = self.parse_grant_target()?;
            Ok(Clause::GrantPrivilege {
                privileges,
                target_name,
                is_user,
            })
        } else {
            // GRANT role_name TO user (legacy syntax)
            let role_name = self
                .advance_name()
                .ok_or_else(|| self.error("expected role name"))?;
            self.expect(Token::To)?;
            let username = self
                .advance_name()
                .ok_or_else(|| self.error("expected username"))?;
            Ok(Clause::GrantRole {
                role_name,
                username,
            })
        }
    }

    fn parse_revoke(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Revoke)?;
        if self.match_keyword(Token::All) {
            let _ = self.match_keyword(Token::Privileges);
            self.expect(Token::From)?;
            let (target_name, is_user) = self.parse_revoke_target()?;
            Ok(Clause::RevokePrivilege {
                privileges: Privilege::all(),
                target_name,
                is_user,
            })
        } else if self.peek() == Some(&Token::Privilege) || self.peek() == Some(&Token::Privileges)
        {
            let _ = self.match_keyword(Token::Privilege) || self.match_keyword(Token::Privileges);
            let privileges = self.parse_privilege_list()?;
            self.expect(Token::From)?;
            let (target_name, is_user) = self.parse_revoke_target()?;
            Ok(Clause::RevokePrivilege {
                privileges,
                target_name,
                is_user,
            })
        } else {
            let role_name = self
                .advance_name()
                .ok_or_else(|| self.error("expected role name"))?;
            self.expect(Token::From)?;
            let username = self
                .advance_name()
                .ok_or_else(|| self.error("expected username"))?;
            Ok(Clause::RevokeRole {
                role_name,
                username,
            })
        }
    }

    fn parse_deny_privilege(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Deny)?;
        let privileges = if self.match_keyword(Token::All) {
            let _ = self.match_keyword(Token::Privileges);
            Privilege::all()
        } else {
            let _ = self.match_keyword(Token::Privilege) || self.match_keyword(Token::Privileges);
            self.parse_privilege_list()?
        };
        self.expect(Token::To)?;
        let (target_name, is_user) = self.parse_grant_target()?;
        Ok(Clause::DenyPrivilege {
            privileges,
            target_name,
            is_user,
        })
    }

    fn parse_grant_target(&mut self) -> Result<(String, bool), ParseError> {
        let is_user = if self.match_keyword(Token::User) {
            true
        } else {
            let _ = self.match_keyword(Token::Role);
            false
        };
        let name = self
            .advance_name()
            .ok_or_else(|| self.error("expected name"))?;
        Ok((name, is_user))
    }

    fn parse_revoke_target(&mut self) -> Result<(String, bool), ParseError> {
        let is_user = if self.match_keyword(Token::User) {
            true
        } else {
            let _ = self.match_keyword(Token::Role);
            false
        };
        let name = self
            .advance_name()
            .ok_or_else(|| self.error("expected name"))?;
        Ok((name, is_user))
    }

    fn parse_privilege_list(&mut self) -> Result<Vec<Privilege>, ParseError> {
        let mut privileges = Vec::new();
        loop {
            let name = self
                .advance_name()
                .ok_or_else(|| self.error("expected privilege name"))?;
            let p = Privilege::parse_name(&name)
                .ok_or_else(|| self.error(format!("unknown privilege: {}", name)))?;
            privileges.push(p);
            if !self.match_keyword(Token::Comma) {
                break;
            }
        }
        Ok(privileges)
    }

    // ─── ALTER USER ────────────────────────────────────────────────────────

    fn parse_alter_user(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Alter)?;
        self.expect(Token::User)?;
        let username = self
            .advance_name()
            .ok_or_else(|| self.error("expected username"))?;
        let action = if self.match_keyword(Token::Set) {
            self.expect(Token::Password)?;
            let pw = match self.peek() {
                Some(Token::StringLiteral(s)) => {
                    let v = s.clone();
                    self.advance();
                    v
                }
                _ => return Err(self.error("expected password string")),
            };
            AlterUserAction::SetPassword { password: pw }
        } else if self.match_keyword(Token::Rename) {
            self.expect(Token::To)?;
            let new_name = self
                .advance_name()
                .ok_or_else(|| self.error("expected new username"))?;
            AlterUserAction::RenameTo { new_name }
        } else {
            return Err(self.error("expected SET PASSWORD or RENAME TO"));
        };
        Ok(Clause::AlterUser { username, action })
    }

    // ─── BEGIN / COMMIT / ROLLBACK ──────────────────────────────────────────

    fn parse_begin(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Begin)?;
        let _ = self.match_keyword(Token::Transactions);
        Ok(Clause::BeginTransaction)
    }

    fn parse_rollback(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Rollback)?;
        let _ = self.match_keyword(Token::Transactions);
        Ok(Clause::RollbackTransaction)
    }

    fn parse_commit(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Commit)?;
        let _ = self.match_keyword(Token::Transactions);
        Ok(Clause::CommitTransaction)
    }

    // ─── SET STORAGE MODE ──────────────────────────────────────────────────

    fn parse_set_storage_mode(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Set)?;
        self.expect(Token::Storage)?;
        self.expect(Token::Mode)?;
        let mode = if self.match_keyword(Token::InMemoryAnalytical) {
            StorageMode::InMemoryAnalytical
        } else if self.match_keyword(Token::InMemoryTransactional) {
            StorageMode::InMemoryTransactional
        } else if self.match_keyword(Token::OnDiskTransactional) {
            StorageMode::OnDiskTransactional
        } else if self.match_keyword(Token::InMemory) {
            if self.match_keyword(Token::Analytical) {
                StorageMode::InMemoryAnalytical
            } else {
                self.expect(Token::Transactional)?;
                StorageMode::InMemoryTransactional
            }
        } else if self.match_keyword(Token::OnDisk) {
            self.expect(Token::Transactional)?;
            StorageMode::OnDiskTransactional
        } else {
            return Err(self.error(
                "expected IN_MEMORY_ANALYTICAL, IN_MEMORY_TRANSACTIONAL, or ON_DISK_TRANSACTIONAL",
            ));
        };
        Ok(Clause::SetStorageMode { mode })
    }

    // ─── Settings DDL parsing ───────────────────────────────────────────────

    fn parse_set_setting(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Set)?;
        self.expect(Token::Setting)?;
        let name = self
            .advance_name()
            .ok_or_else(|| self.error("expected setting name"))?;
        // Allow optional TO keyword: SET SETTING name TO value
        if self.match_keyword(Token::To) {
            // consumed TO
        }
        let value = self.parse_expression()?;
        Ok(Clause::SetSetting { name, value })
    }

    fn parse_terminate_transaction(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Terminate)?;
        self.expect(Token::Transactions)?;
        let transaction_id = match self.peek() {
            Some(Token::StringLiteral(s)) => {
                let id = s.clone();
                self.advance();
                id
            }
            _ => self
                .advance_name()
                .ok_or_else(|| self.error("expected transaction id"))?,
        };
        Ok(Clause::TerminateTransaction { transaction_id })
    }

    // ─── Trigger DDL parsing ────────────────────────────────────────────────

    fn parse_create_trigger(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Create)?;
        self.expect(Token::Trigger)?;
        let name = self
            .advance_name()
            .ok_or_else(|| self.error("expected trigger name"))?;
        self.expect(Token::On)?;

        // Parse target: VERTEX or EDGE
        let target = match self.peek() {
            Some(Token::Vertex) => {
                self.advance();
                TriggerTarget::Vertex
            }
            Some(Token::Edge) => {
                self.advance();
                TriggerTarget::Edge
            }
            _ => return Err(self.error("expected VERTEX or EDGE after ON")),
        };

        // Parse event: CREATE, DELETE, UPDATE
        let event = match self.peek() {
            Some(Token::Create) => {
                self.advance();
                TriggerEvent::Create
            }
            Some(Token::Delete) => {
                self.advance();
                TriggerEvent::Delete
            }
            Some(Token::Set) => {
                self.advance();
                TriggerEvent::Update
            }
            _ => return Err(self.error("expected CREATE, DELETE, or SET (UPDATE) after target")),
        };

        // Parse timing: BEFORE or AFTER
        let timing = match self.peek() {
            Some(Token::Before) => {
                self.advance();
                TriggerTiming::Before
            }
            Some(Token::After) => {
                self.advance();
                TriggerTiming::After
            }
            _ => return Err(self.error("expected BEFORE or AFTER")),
        };

        self.expect(Token::Execute)?;

        let statement = if let Some(Token::StringLiteral(s)) = self.peek() {
            let s = s.clone();
            self.advance();
            s
        } else {
            return Err(self.error("expected trigger statement string"));
        };

        Ok(Clause::CreateTrigger {
            name,
            target,
            event,
            timing,
            statement,
        })
    }

    fn parse_drop_trigger(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Drop)?;
        self.expect(Token::Trigger)?;
        let name = self
            .advance_name()
            .ok_or_else(|| self.error("expected trigger name"))?;
        Ok(Clause::DropTrigger { name })
    }

    fn parse_create_database(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Create)?;
        self.expect(Token::Database)?;
        let name = self
            .advance_name()
            .ok_or_else(|| self.error("expected database name"))?;
        Ok(Clause::CreateDatabase { name })
    }

    fn parse_drop_database(&mut self) -> Result<Clause, ParseError> {
        self.expect(Token::Drop)?;
        self.expect(Token::Database)?;
        let name = self
            .advance_name()
            .ok_or_else(|| self.error("expected database name"))?;
        let force = self.match_keyword(Token::Force);
        Ok(Clause::DropDatabase { name, force })
    }

    // ─── Pattern parsing ────────────────────────────────────────────────────

    fn parse_match_pattern(&mut self) -> Result<MatchPattern, ParseError> {
        let mut elements = Vec::new();
        elements.push(self.parse_match_pattern_element()?);
        while self.match_keyword(Token::Comma) {
            elements.push(self.parse_match_pattern_element()?);
        }
        Ok(MatchPattern { elements })
    }

    fn parse_match_pattern_element(&mut self) -> Result<PatternElement, ParseError> {
        // Check for path alias: p = (n)-[...]->(...)
        let path_alias = if let Some(Token::Identifier(name)) = self.peek() {
            let name = name.clone();
            if let Some(Token::Eq) = self.peek_n(1) {
                self.advance();
                self.advance();
                Some(name)
            } else {
                None
            }
        } else {
            None
        };

        // Check for shortestPath((n)-[...]->(m)) or allShortestPaths((n)-[...]->(m))
        let shortest_algo = if self.match_keyword(Token::ShortestPath)
            || self.match_keyword(Token::AllShortestPaths)
        {
            self.expect(Token::LParen)?;
            Some(PathAlgorithm::AllShortest)
        } else {
            None
        };

        let node = self.parse_node_pattern()?;
        let mut edges = Vec::new();
        while self.peek() == Some(&Token::Minus) || self.peek() == Some(&Token::ArrowLeft) {
            let (mut edge, next_node) = self.parse_edge_and_node()?;
            if let Some(algo) = shortest_algo {
                edge.path_algorithm = algo;
            }
            edges.push((edge, next_node));
        }

        if shortest_algo.is_some() {
            self.expect(Token::RParen)?;
        }

        Ok(PatternElement {
            path_alias,
            node,
            edges,
        })
    }

    fn parse_create_pattern(&mut self) -> Result<CreatePattern, ParseError> {
        let mut elements = Vec::new();
        elements.push(self.parse_pattern_element()?);
        while self.match_keyword(Token::Comma) {
            elements.push(self.parse_pattern_element()?);
        }
        Ok(CreatePattern { elements })
    }

    fn parse_pattern_element(&mut self) -> Result<PatternElement, ParseError> {
        let node = self.parse_node_pattern()?;
        let mut edges = Vec::new();
        while self.peek() == Some(&Token::Minus) || self.peek() == Some(&Token::ArrowLeft) {
            let (edge, next_node) = self.parse_edge_and_node()?;
            edges.push((edge, next_node));
        }
        Ok(PatternElement {
            path_alias: None,
            node,
            edges,
        })
    }

    fn parse_node_pattern(&mut self) -> Result<NodePattern, ParseError> {
        self.expect(Token::LParen)?;
        let alias = self.advance_name();
        let mut labels = Vec::new();
        if self.match_keyword(Token::Colon) {
            let name = self
                .advance_name()
                .ok_or_else(|| self.error("expected label name"))?;
            labels.push(self.resolve_label(&name));
            while self.match_keyword(Token::Colon) {
                let name = self
                    .advance_name()
                    .ok_or_else(|| self.error("expected label name"))?;
                labels.push(self.resolve_label(&name));
            }
        }
        let properties = if self.peek() == Some(&Token::LBrace) {
            self.parse_property_map()?
        } else {
            Vec::new()
        };
        self.expect(Token::RParen)?;
        Ok(NodePattern {
            alias,
            labels,
            properties,
        })
    }

    fn parse_edge_and_node(&mut self) -> Result<(EdgePattern, NodePattern), ParseError> {
        if self.match_keyword(Token::ArrowLeft) {
            // Left direction: <-[...]- or <--
            let edge = if self.peek() == Some(&Token::LBracket) {
                let mut e = self.parse_edge_pattern()?;
                e.direction = Direction::Left;
                e
            } else {
                // Anonymous left edge: <--
                EdgePattern {
                    alias: None,
                    edge_types: Vec::new(),
                    properties: Vec::new(),
                    direction: Direction::Left,
                    var_length: None,
                    path_algorithm: PathAlgorithm::Default,
                    kshortest_limit: None,
                }
            };
            // Consume trailing - if present (for <--)
            self.match_keyword(Token::Minus);
            let node = self.parse_node_pattern()?;
            Ok((edge, node))
        } else if self.match_keyword(Token::Minus) {
            // Could be -[...]->, -[...]-, -->, --, or just -( next_node
            let mut edge = if self.peek() == Some(&Token::LBracket) {
                self.parse_edge_pattern()?
            } else {
                EdgePattern {
                    alias: None,
                    edge_types: Vec::new(),
                    properties: Vec::new(),
                    direction: Direction::Right,
                    var_length: None,
                    path_algorithm: PathAlgorithm::Default,
                    kshortest_limit: None,
                }
            };
            if self.match_keyword(Token::ArrowRight) {
                edge.direction = Direction::Right;
            } else {
                self.match_keyword(Token::Minus);
                edge.direction = Direction::Either;
            }
            let node = self.parse_node_pattern()?;
            Ok((edge, node))
        } else {
            Err(self.error("expected - or <-"))
        }
    }

    fn parse_edge_pattern(&mut self) -> Result<EdgePattern, ParseError> {
        self.expect(Token::LBracket)?;
        let alias = self.advance_name();
        let mut edge_types = Vec::new();
        if self.match_keyword(Token::Colon) {
            let name = self
                .advance_name()
                .ok_or_else(|| self.error("expected edge type name"))?;
            edge_types.push(self.resolve_edge_type(&name));
            while self.match_keyword(Token::Pipe) {
                let name = self
                    .advance_name()
                    .ok_or_else(|| self.error("expected edge type name"))?;
                edge_types.push(self.resolve_edge_type(&name));
            }
        }
        let properties = if self.peek() == Some(&Token::LBrace) {
            self.parse_property_map()?
        } else {
            Vec::new()
        };
        let (var_length, path_algorithm, kshortest_limit) = if self.match_keyword(Token::Star) {
            let algorithm = if self.match_keyword(Token::Bfs) {
                PathAlgorithm::Bfs
            } else if self.match_keyword(Token::WShortest) {
                PathAlgorithm::WShortest
            } else if self.match_keyword(Token::AllShortest) {
                PathAlgorithm::AllShortest
            } else if self.match_keyword(Token::KShortest) {
                PathAlgorithm::KShortest
            } else {
                PathAlgorithm::Default
            };
            let min = if let Some(Token::IntLiteral(n)) = self.peek() {
                let n = *n as usize;
                self.advance();
                n
            } else {
                1
            };
            let max = if self.match_keyword(Token::DotDot) {
                if let Some(Token::IntLiteral(n)) = self.peek() {
                    let n = *n as usize;
                    self.advance();
                    Some(n)
                } else {
                    None // unbounded: *1..
                }
            } else if self.peek() == Some(&Token::RBracket) || self.peek() == Some(&Token::Pipe) {
                // Just * with no bounds: unbounded, or about to parse kShortest limit
                None
            } else {
                Some(min)
            };
            let limit = if self.match_keyword(Token::Pipe) {
                Some(self.parse_expression()?)
            } else {
                None
            };
            (Some((min, max)), algorithm, limit)
        } else {
            (None, PathAlgorithm::Default, None)
        };
        // Property map may also appear after var_length (e.g. [*bfs..10 {id: 1}])
        let post_properties = if self.peek() == Some(&Token::LBrace) {
            self.parse_property_map()?
        } else {
            Vec::new()
        };
        self.expect(Token::RBracket)?;
        let mut properties = properties;
        properties.extend(post_properties);
        // Direction will be set by caller based on surrounding arrows
        Ok(EdgePattern {
            alias,
            edge_types,
            properties,
            direction: Direction::Right,
            var_length,
            path_algorithm,
            kshortest_limit,
        })
    }

    fn parse_property_map(&mut self) -> Result<Vec<(PropertyId, Expression)>, ParseError> {
        self.expect(Token::LBrace)?;
        let mut properties = Vec::new();
        if !matches!(self.peek(), Some(Token::RBrace)) {
            let name = self
                .advance_name()
                .ok_or_else(|| self.error("expected property name"))?;
            let key = self.resolve_property(&name);
            self.expect(Token::Colon)?;
            let value = self.parse_expression()?;
            properties.push((key, value));
            while self.match_keyword(Token::Comma) {
                let name = self
                    .advance_name()
                    .ok_or_else(|| self.error("expected property name"))?;
                let key = self.resolve_property(&name);
                self.expect(Token::Colon)?;
                let value = self.parse_expression()?;
                properties.push((key, value));
            }
        }
        self.expect(Token::RBrace)?;
        Ok(properties)
    }

    // ─── Expression parsing ─────────────────────────────────────────────────

    fn parse_expression(&mut self) -> Result<Expression, ParseError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_and()?;
        while self.match_keyword(Token::Or) {
            let right = self.parse_and()?;
            left = Expression::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_not()?;
        while self.match_keyword(Token::And) {
            let right = self.parse_not()?;
            left = Expression::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Expression, ParseError> {
        if self.match_keyword(Token::Not) {
            let expr = self.parse_not()?;
            Ok(Expression::Not(Box::new(expr)))
        } else {
            self.parse_comparison()
        }
    }

    fn parse_comparison(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_additive()?;
        loop {
            if self.match_keyword(Token::Eq) {
                let right = self.parse_additive()?;
                left = Expression::Eq(Box::new(left), Box::new(right));
            } else if self.match_keyword(Token::Neq) {
                let right = self.parse_additive()?;
                left = Expression::Neq(Box::new(left), Box::new(right));
            } else if self.match_keyword(Token::Lt) {
                let right = self.parse_additive()?;
                left = Expression::Lt(Box::new(left), Box::new(right));
            } else if self.match_keyword(Token::Gt) {
                let right = self.parse_additive()?;
                left = Expression::Gt(Box::new(left), Box::new(right));
            } else if self.match_keyword(Token::Lte) {
                let right = self.parse_additive()?;
                left = Expression::Lte(Box::new(left), Box::new(right));
            } else if self.match_keyword(Token::Gte) {
                let right = self.parse_additive()?;
                left = Expression::Gte(Box::new(left), Box::new(right));
            } else if self.match_keyword(Token::In) {
                let right = self.parse_additive()?;
                left = Expression::In(Box::new(left), Box::new(right));
            } else if self.match_keyword(Token::StartsWith) {
                self.expect(Token::With)?;
                let right = self.parse_additive()?;
                left = Expression::StartsWith(Box::new(left), Box::new(right));
            } else if self.match_keyword(Token::EndsWith) {
                self.expect(Token::With)?;
                let right = self.parse_additive()?;
                left = Expression::EndsWith(Box::new(left), Box::new(right));
            } else if self.match_keyword(Token::Contains) {
                let right = self.parse_additive()?;
                left = Expression::Contains(Box::new(left), Box::new(right));
            } else if self.match_keyword(Token::Is) {
                if self.match_keyword(Token::Null) {
                    left = Expression::IsNull(Box::new(left));
                } else if self.match_keyword(Token::Not) {
                    self.expect(Token::Null)?;
                    left = Expression::IsNotNull(Box::new(left));
                } else {
                    return Err(self.error("expected NULL or NOT NULL after IS"));
                }
            } else if self.match_keyword(Token::Tilde) {
                let right = self.parse_additive()?;
                left = Expression::RegexMatch(Box::new(left), Box::new(right));
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_additive(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_multiplicative()?;
        loop {
            if self.match_keyword(Token::Plus) {
                let right = self.parse_multiplicative()?;
                left = Expression::Add(Box::new(left), Box::new(right));
            } else if self.match_keyword(Token::Minus) {
                let right = self.parse_multiplicative()?;
                left = Expression::Sub(Box::new(left), Box::new(right));
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_unary()?;
        loop {
            if self.match_keyword(Token::Star) {
                let right = self.parse_unary()?;
                left = Expression::Mul(Box::new(left), Box::new(right));
            } else if self.match_keyword(Token::Slash) {
                let right = self.parse_unary()?;
                left = Expression::Div(Box::new(left), Box::new(right));
            } else if self.match_keyword(Token::Percent) {
                let right = self.parse_unary()?;
                left = Expression::Mod(Box::new(left), Box::new(right));
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expression, ParseError> {
        if self.match_keyword(Token::Minus) {
            let expr = self.parse_unary()?;
            Ok(Expression::Neg(Box::new(expr)))
        } else {
            self.parse_postfix()
        }
    }

    fn parse_postfix(&mut self) -> Result<Expression, ParseError> {
        let mut expr = self.parse_primary()?;
        loop {
            if self.match_keyword(Token::Dot) {
                // Check if next is a name (identifier or keyword) followed by (
                let is_func = match self.peek() {
                    Some(Token::Identifier(_)) => self.peek_n(1) == Some(&Token::LParen),
                    Some(tok) => Self::is_keyword(tok) && self.peek_n(1) == Some(&Token::LParen),
                    None => false,
                };
                if is_func {
                    let name = self.advance_name().unwrap();
                    self.advance(); // consume (
                    let mut arguments = Vec::new();
                    if !matches!(self.peek(), Some(Token::RParen)) {
                        arguments.push(self.parse_expression()?);
                        while self.match_keyword(Token::Comma) {
                            arguments.push(self.parse_expression()?);
                        }
                    }
                    self.expect(Token::RParen)?;
                    expr = Expression::Function {
                        name,
                        arguments,
                        distinct: false,
                    };
                    continue;
                }
                let name = self
                    .advance_name()
                    .ok_or_else(|| self.error("expected identifier after ."))?;
                let key = self.resolve_property(&name);
                expr = Expression::Property {
                    object: Box::new(expr),
                    key,
                };
            } else if self.match_keyword(Token::LBracket) {
                // Could be index [expr] or slice [start..end]
                let start = self.parse_expression()?;
                if self.match_keyword(Token::DotDot) {
                    let end = self.parse_expression()?;
                    self.expect(Token::RBracket)?;
                    expr = Expression::Slice {
                        object: Box::new(expr),
                        start: Box::new(start),
                        end: Box::new(end),
                    };
                } else {
                    self.expect(Token::RBracket)?;
                    expr = Expression::Index {
                        object: Box::new(expr),
                        index: Box::new(start),
                    };
                }
            } else if self.match_keyword(Token::Colon) {
                let name = self
                    .advance_name()
                    .ok_or_else(|| self.error("expected label name after :"))?;
                let label = self.resolve_label(&name);
                expr = Expression::Label {
                    object: Box::new(expr),
                    label,
                };
            } else {
                break;
            }
        }
        Ok(expr)
    }

    fn parse_primary(&mut self) -> Result<Expression, ParseError> {
        match self.peek() {
            Some(Token::All) | Some(Token::Any) | Some(Token::None_) | Some(Token::Single)
            | Some(Token::Filter) | Some(Token::Extract) | Some(Token::Reduce) => {
                self.parse_list_predicate_or_function()
            }
            Some(Token::Exists)
                if self.peek_n(1) == Some(&Token::LParen)
                    && self.peek_n(2) == Some(&Token::LParen) =>
            {
                // EXISTS((pattern)) — pattern exists function with double parens
                self.advance();
                self.expect(Token::LParen)?;
                let pattern = self.parse_match_pattern()?;
                self.expect(Token::RParen)?;
                let query = Query {
                    clauses: vec![Clause::Match {
                        pattern,
                        where_clause: None,
                    }],
                    union: None,
                    mode: QueryMode::Standard,
                    periodic_commit: None,
                    hops_limit: None,
                    index_hints: Vec::new(),
                };
                Ok(Expression::Exists(Box::new(query)))
            }
            Some(Token::Exists) if self.peek_n(1) == Some(&Token::LBrace) => {
                self.advance();
                self.expect(Token::LBrace)?;
                let query = self.parse_exists_subquery()?;
                self.expect(Token::RBrace)?;
                Ok(Expression::Exists(Box::new(query)))
            }
            Some(Token::Identifier(name))
                if name.eq_ignore_ascii_case("count") && self.peek_n(1) == Some(&Token::LBrace) =>
            {
                // COUNT { subquery }
                self.advance();
                self.expect(Token::LBrace)?;
                let query = self.parse_exists_subquery()?;
                self.expect(Token::RBrace)?;
                Ok(Expression::CountSubquery(Box::new(query)))
            }
            Some(Token::Identifier(name)) => {
                let name = name.clone();
                self.advance();
                if self.peek() == Some(&Token::LParen) {
                    // Function call
                    self.advance(); // consume (
                    let distinct = self.match_keyword(Token::Distinct);
                    let mut arguments = Vec::new();
                    if !matches!(self.peek(), Some(Token::RParen)) {
                        arguments.push(self.parse_expression()?);
                        while self.match_keyword(Token::Comma) {
                            arguments.push(self.parse_expression()?);
                        }
                    }
                    self.expect(Token::RParen)?;
                    Ok(Expression::Function {
                        name,
                        arguments,
                        distinct,
                    })
                } else {
                    Ok(Expression::Identifier(name))
                }
            }
            Some(Token::Parameter(name)) => {
                let name = name.clone();
                self.advance();
                Ok(Expression::Parameter(name))
            }
            Some(tok) if Self::is_keyword(tok) && self.peek_n(1) == Some(&Token::LParen) => {
                // Keywords used as function names: startswith, endswith, contains, etc.
                let name = self.advance_name().unwrap();
                self.advance(); // consume (
                let mut arguments = Vec::new();
                if !matches!(self.peek(), Some(Token::RParen)) {
                    arguments.push(self.parse_expression()?);
                    while self.match_keyword(Token::Comma) {
                        arguments.push(self.parse_expression()?);
                    }
                }
                self.expect(Token::RParen)?;
                Ok(Expression::Function {
                    name,
                    arguments,
                    distinct: false,
                })
            }
            Some(Token::IntLiteral(n)) => {
                let n = *n;
                self.advance();
                Ok(Expression::Int(n))
            }
            Some(Token::FloatLiteral(n)) => {
                let n = *n;
                self.advance();
                Ok(Expression::Double(n))
            }
            Some(Token::StringLiteral(s)) => {
                let s = s.clone();
                self.advance();
                Ok(Expression::String(s))
            }
            Some(Token::True) => {
                self.advance();
                Ok(Expression::Bool(true))
            }
            Some(Token::False) => {
                self.advance();
                Ok(Expression::Bool(false))
            }
            Some(Token::Null) => {
                self.advance();
                Ok(Expression::Null)
            }
            Some(Token::Star) => {
                self.advance();
                Ok(Expression::CountStar)
            }
            Some(Token::LParen) => {
                self.advance();
                let expr = self.parse_expression()?;
                self.expect(Token::RParen)?;
                Ok(expr)
            }
            Some(Token::LBracket) => {
                self.advance();
                // Could be list literal, list comprehension, or pattern comprehension
                if self.peek() == Some(&Token::LParen) {
                    // Pattern comprehension: [(pattern) | expr]
                    self.parse_pattern_comprehension()
                } else if let Some(Token::Identifier(_)) = self.peek() {
                    if self.peek_n(1) == Some(&Token::In) {
                        // List comprehension: [x IN list WHERE cond | expr]
                        self.parse_list_comprehension()
                    } else {
                        // List literal starting with identifier
                        let mut items = Vec::new();
                        items.push(self.parse_expression()?);
                        while self.match_keyword(Token::Comma) {
                            items.push(self.parse_expression()?);
                        }
                        self.expect(Token::RBracket)?;
                        Ok(Expression::List(items))
                    }
                } else {
                    let mut items = Vec::new();
                    if !matches!(self.peek(), Some(Token::RBracket)) {
                        items.push(self.parse_expression()?);
                        while self.match_keyword(Token::Comma) {
                            items.push(self.parse_expression()?);
                        }
                    }
                    self.expect(Token::RBracket)?;
                    Ok(Expression::List(items))
                }
            }
            Some(Token::LBrace) => {
                self.advance();
                // Map projection: {n.*, extra: 1} or {n.name, n.age}  vs  Map literal: {key: val}
                if let Some(Token::Identifier(_)) = self.peek() {
                    if self.peek_n(1) == Some(&Token::Dot) {
                        let mut all = false;
                        let mut extra = Vec::new();
                        let mut first_obj_name: Option<String> = None;
                        // Parse projection elements: n.*, n.name, or key: value
                        loop {
                            let obj_name = self.advance_name().unwrap();
                            if first_obj_name.is_none() {
                                first_obj_name = Some(obj_name.clone());
                            }
                            self.expect(Token::Dot)?;
                            if self.match_keyword(Token::Star) {
                                all = true;
                            } else {
                                let prop_name = self
                                    .advance_name()
                                    .ok_or_else(|| self.error("expected property name after ."))?;
                                extra.push((
                                    prop_name.clone(),
                                    Expression::Property {
                                        object: Box::new(Expression::Identifier(obj_name)),
                                        key: self.resolve_property(&prop_name),
                                    },
                                ));
                            }
                            if !self.match_keyword(Token::Comma) {
                                break;
                            }
                            // After comma, check if next is an identifier followed by dot
                            // (another projection element) or a regular key:value pair
                            if let Some(Token::Identifier(_)) = self.peek() {
                                if self.peek_n(1) != Some(&Token::Dot) {
                                    // Regular key:value pair after comma
                                    let key = self.advance_name().unwrap();
                                    let value = if self.match_keyword(Token::Colon) {
                                        self.parse_expression()?
                                    } else {
                                        Expression::Identifier(key.clone())
                                    };
                                    extra.push((key, value));
                                    if !self.match_keyword(Token::Comma) {
                                        break;
                                    }
                                }
                            } else {
                                // Not an identifier, must be a regular expression
                                let key = self
                                    .advance_name()
                                    .ok_or_else(|| self.error("expected projection key"))?;
                                let value = if self.match_keyword(Token::Colon) {
                                    self.parse_expression()?
                                } else {
                                    Expression::Identifier(key.clone())
                                };
                                extra.push((key, value));
                                if !self.match_keyword(Token::Comma) {
                                    break;
                                }
                            }
                        }
                        self.expect(Token::RBrace)?;
                        // Use first element's object as the projection object
                        let object =
                            if let Some((_, Expression::Property { object, .. })) = extra.first() {
                                object.clone()
                            } else if let Some(name) = first_obj_name {
                                Box::new(Expression::Identifier(name))
                            } else {
                                Box::new(Expression::Identifier("".to_string()))
                            };
                        return Ok(Expression::MapProjection { object, all, extra });
                    }
                }
                let mut pairs = Vec::new();
                if !matches!(self.peek(), Some(Token::RBrace)) {
                    let name = self
                        .advance_name()
                        .ok_or_else(|| self.error("expected map key"))?;
                    self.expect(Token::Colon)?;
                    let value = self.parse_expression()?;
                    pairs.push((name, value));
                    while self.match_keyword(Token::Comma) {
                        let name = self
                            .advance_name()
                            .ok_or_else(|| self.error("expected map key"))?;
                        self.expect(Token::Colon)?;
                        let value = self.parse_expression()?;
                        pairs.push((name, value));
                    }
                }
                self.expect(Token::RBrace)?;
                Ok(Expression::Map(pairs))
            }
            Some(Token::Case) => self.parse_case_expression(),
            _ => Err(self.error(format!("unexpected token: {:?}", self.peek()))),
        }
    }

    fn parse_list_predicate_or_function(&mut self) -> Result<Expression, ParseError> {
        let name = self.advance_name().unwrap();
        self.expect(Token::LParen)?;

        if name.eq_ignore_ascii_case("REDUCE") {
            let accumulator = self
                .advance_name()
                .ok_or_else(|| self.error("expected accumulator name"))?;
            self.expect(Token::Eq)?;
            let initial = Box::new(self.parse_expression()?);
            self.expect(Token::Comma)?;
            let variable = self
                .advance_name()
                .ok_or_else(|| self.error("expected variable name"))?;
            self.expect(Token::In)?;
            let list = Box::new(self.parse_expression()?);
            self.expect(Token::Pipe)?;
            let expression = Box::new(self.parse_expression()?);
            self.expect(Token::RParen)?;
            return Ok(Expression::Reduce {
                accumulator,
                initial,
                variable,
                list,
                expression,
            });
        }

        let variable = self
            .advance_name()
            .ok_or_else(|| self.error("expected variable name"))?;
        self.expect(Token::In)?;
        let list = Box::new(self.parse_expression()?);

        if name.eq_ignore_ascii_case("EXTRACT") {
            self.expect(Token::Pipe)?;
            let expression = Box::new(self.parse_expression()?);
            self.expect(Token::RParen)?;
            return Ok(Expression::Extract {
                variable,
                list,
                expression,
            });
        }

        self.expect(Token::Where)?;
        let predicate = Box::new(self.parse_expression()?);
        self.expect(Token::RParen)?;

        match name.to_ascii_uppercase().as_str() {
            "ALL" => Ok(Expression::All {
                variable,
                list,
                predicate,
            }),
            "ANY" => Ok(Expression::Any {
                variable,
                list,
                predicate,
            }),
            "NONE" => Ok(Expression::None {
                variable,
                list,
                predicate,
            }),
            "SINGLE" => Ok(Expression::Single {
                variable,
                list,
                predicate,
            }),
            "FILTER" => Ok(Expression::Filter {
                variable,
                list,
                predicate,
            }),
            _ => Err(self.error(format!("unknown list predicate: {}", name))),
        }
    }

    fn parse_pattern_comprehension(&mut self) -> Result<Expression, ParseError> {
        // We've already consumed '[' and peeked '('
        let pattern = self.parse_match_pattern()?;
        let where_clause = if self.match_keyword(Token::Where) {
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };
        self.expect(Token::Pipe)?;
        let expression = Box::new(self.parse_expression()?);
        self.expect(Token::RBracket)?;
        Ok(Expression::PatternComprehension {
            pattern,
            where_clause,
            expression,
        })
    }

    fn parse_list_comprehension(&mut self) -> Result<Expression, ParseError> {
        // We've already consumed '[' and confirmed peek() is Identifier followed by In
        let variable = self.advance_name().unwrap();
        self.expect(Token::In)?;
        let list = Box::new(self.parse_expression()?);
        let predicate = if self.match_keyword(Token::Where) {
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };
        let expression = if self.match_keyword(Token::Pipe) {
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };
        self.expect(Token::RBracket)?;
        match (predicate, expression) {
            (Some(pred), Some(expr)) => {
                let var = variable.clone();
                Ok(Expression::Extract {
                    variable: var.clone(),
                    list: Box::new(Expression::Filter {
                        variable: var,
                        list,
                        predicate: pred,
                    }),
                    expression: expr,
                })
            }
            (Some(pred), None) => Ok(Expression::Filter {
                variable,
                list,
                predicate: pred,
            }),
            (None, Some(expr)) => Ok(Expression::Extract {
                variable,
                list,
                expression: expr,
            }),
            (None, None) => Ok(Expression::List(vec![])),
        }
    }

    fn parse_exists_subquery(&mut self) -> Result<Query, ParseError> {
        // Parse a MATCH clause inside EXISTS { ... }
        // MATCH keyword is optional: EXISTS { (a)-[:R]->(b) } or EXISTS { MATCH (a)-[:R]->(b) }
        self.match_keyword(Token::Match);
        let pattern = self.parse_match_pattern()?;
        let where_clause = if self.match_keyword(Token::Where) {
            Some(self.parse_expression()?)
        } else {
            None
        };
        let mut clauses = vec![Clause::Match {
            pattern,
            where_clause,
        }];
        if self.match_keyword(Token::Return) {
            let distinct = self.match_keyword(Token::Distinct);
            let all = self.match_keyword(Token::Star);
            let mut items = Vec::new();
            if !all {
                items.push(self.parse_return_item()?);
                while self.match_keyword(Token::Comma) {
                    items.push(self.parse_return_item()?);
                }
            }
            clauses.push(Clause::Return {
                items,
                distinct,
                all,
            });
        }
        Ok(Query {
            clauses,
            union: None,
            mode: QueryMode::Standard,
            periodic_commit: None,
            hops_limit: None,
            index_hints: Vec::new(),
        })
    }

    fn parse_case_expression(&mut self) -> Result<Expression, ParseError> {
        self.expect(Token::Case)?;
        let expression = if !matches!(self.peek(), Some(Token::When)) {
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };
        let mut whens = Vec::new();
        while self.match_keyword(Token::When) {
            let when_expr = self.parse_expression()?;
            self.expect(Token::Then)?;
            let then_expr = self.parse_expression()?;
            whens.push((when_expr, then_expr));
        }
        let else_branch = if self.match_keyword(Token::Else) {
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };
        self.expect(Token::End)?;
        Ok(Expression::Case {
            expression,
            whens,
            else_branch,
        })
    }

    fn parse_argument_list(&mut self) -> Result<Vec<Expression>, ParseError> {
        let mut args = Vec::new();
        if !matches!(self.peek(), Some(Token::RParen)) {
            args.push(self.parse_expression()?);
            while self.match_keyword(Token::Comma) {
                args.push(self.parse_expression()?);
            }
        }
        Ok(args)
    }
}

// ─── Public API ───────────────────────────────────────────────────────────

pub fn parse_query(input: &str) -> Result<Query, ParseError> {
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize();
    let mut parser = Parser::new(tokens, None);
    parser.parse_query()
}

pub fn parse_query_with_catalog(
    input: &str,
    catalog: Option<&Catalog>,
) -> Result<Query, ParseError> {
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize();
    let mut parser = Parser::new(tokens, catalog);
    parser.parse_query()
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "parse error at token {}: {}", self.pos, self.message)
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_match_return() {
        let q = parse_query("MATCH (n) RETURN n").unwrap();
        assert_eq!(q.clauses.len(), 2);
    }

    #[test]
    fn test_parse_create() {
        let q = parse_query("CREATE (n:Person {name: \"Alice\"})").unwrap();
        assert_eq!(q.clauses.len(), 1);
    }

    #[test]
    fn test_parse_where() {
        let q = parse_query("MATCH (n) WHERE n.age > 30 RETURN n").unwrap();
        assert_eq!(q.clauses.len(), 2);
    }

    #[test]
    fn test_parse_relationship() {
        let q = parse_query("MATCH (a)-[r:KNOWS]->(b) RETURN r").unwrap();
        assert_eq!(q.clauses.len(), 2);
    }

    #[test]
    fn test_parse_left_directional_edge() {
        let q = parse_query("MATCH (a)<-[r:KNOWS]-(b) RETURN r").unwrap();
        assert_eq!(q.clauses.len(), 2);
    }

    #[test]
    fn test_parse_bidirectional_edge() {
        let q = parse_query("MATCH (a)-[r:KNOWS]-(b) RETURN r").unwrap();
        assert_eq!(q.clauses.len(), 2);
    }

    #[test]
    fn test_parse_return_star() {
        let q = parse_query("MATCH (n) RETURN *").unwrap();
        assert_eq!(q.clauses.len(), 2);
    }

    #[test]
    fn test_parse_with() {
        let q = parse_query("MATCH (n) WITH n AS x RETURN x").unwrap();
        assert_eq!(q.clauses.len(), 3);
    }

    #[test]
    fn test_parse_unwind() {
        let q = parse_query("UNWIND [1,2,3] AS x RETURN x").unwrap();
        assert_eq!(q.clauses.len(), 2);
    }

    #[test]
    fn test_parse_order_by() {
        let q = parse_query("MATCH (n) RETURN n ORDER BY n.name DESC SKIP 10 LIMIT 5").unwrap();
        assert_eq!(q.clauses.len(), 5);
    }

    #[test]
    fn test_parse_merge() {
        let q = parse_query("MERGE (n:Person {name: \"Alice\"})").unwrap();
        assert_eq!(q.clauses.len(), 1);
    }

    #[test]
    fn test_parse_merge_on_create() {
        let q = parse_query("MERGE (n:Person) ON CREATE SET n.created = 1").unwrap();
        assert_eq!(q.clauses.len(), 1);
    }

    #[test]
    fn test_parse_merge_on_match() {
        let q = parse_query("MERGE (n:Person) ON MATCH SET n.seen = 1").unwrap();
        assert_eq!(q.clauses.len(), 1);
    }

    #[test]
    fn test_parse_delete() {
        let q = parse_query("MATCH (n) DELETE n").unwrap();
        assert_eq!(q.clauses.len(), 2);
    }

    #[test]
    fn test_parse_detach_delete() {
        let q = parse_query("MATCH (n) DETACH DELETE n").unwrap();
        assert_eq!(q.clauses.len(), 2);
    }

    #[test]
    fn test_parse_set() {
        let q = parse_query("MATCH (n) SET n.name = \"Bob\"").unwrap();
        assert_eq!(q.clauses.len(), 2);
    }

    #[test]
    fn test_parse_remove() {
        let q = parse_query("MATCH (n) REMOVE n.name").unwrap();
        assert_eq!(q.clauses.len(), 2);
    }

    #[test]
    fn test_parse_remove_label() {
        let q = parse_query("MATCH (n) REMOVE n:Person").unwrap();
        assert_eq!(q.clauses.len(), 2);
    }

    #[test]
    fn test_parse_call() {
        let q = parse_query("CALL db.stats() YIELD stat, value RETURN value").unwrap();
        assert_eq!(q.clauses.len(), 2);
    }

    #[test]
    fn test_parse_call_dotted() {
        let q = parse_query("CALL algo.triangle_count()").unwrap();
        assert_eq!(q.clauses.len(), 1);
    }

    #[test]
    fn test_parse_foreach() {
        let q = parse_query("FOREACH (x IN [1,2,3] | CREATE (:Tag {id: x}))").unwrap();
        assert_eq!(q.clauses.len(), 1);
    }

    #[test]
    fn test_parse_load_csv() {
        let q = parse_query("LOAD CSV FROM \"file.csv\" WITH HEADERS AS row").unwrap();
        assert_eq!(q.clauses.len(), 1);
    }

    #[test]
    fn test_parse_load_jsonl() {
        let q = parse_query("LOAD JSONL FROM \"file.jsonl\" AS row").unwrap();
        assert_eq!(q.clauses.len(), 1);
    }

    #[test]
    fn test_parse_all_predicate() {
        let q = parse_query("RETURN all(x IN [1,2,3] WHERE x > 0) AS ok").unwrap();
        assert_eq!(q.clauses.len(), 1);
    }

    #[test]
    fn test_parse_any_predicate() {
        let q = parse_query("RETURN any(x IN [1,2,3] WHERE x > 2) AS ok").unwrap();
        assert_eq!(q.clauses.len(), 1);
    }

    #[test]
    fn test_parse_none_predicate() {
        let q = parse_query("RETURN none(x IN [1,2,3] WHERE x > 5) AS ok").unwrap();
        assert_eq!(q.clauses.len(), 1);
    }

    #[test]
    fn test_parse_single_predicate() {
        let q = parse_query("RETURN single(x IN [1,2,3] WHERE x > 2) AS ok").unwrap();
        assert_eq!(q.clauses.len(), 1);
    }

    #[test]
    fn test_parse_filter_function() {
        let q = parse_query("RETURN filter(x IN [1,2,3] WHERE x > 1)").unwrap();
        assert_eq!(q.clauses.len(), 1);
    }

    #[test]
    fn test_parse_extract_function() {
        let q = parse_query("RETURN extract(x IN [1,2,3] | x * 2)").unwrap();
        assert_eq!(q.clauses.len(), 1);
    }

    #[test]
    fn test_parse_exists() {
        let q = parse_query("MATCH (n) WHERE EXISTS { MATCH (n)-[:KNOWS]->(:Person) } RETURN n")
            .unwrap();
        assert_eq!(q.clauses.len(), 2);
    }

    #[test]
    fn test_parse_case_simple() {
        let q = parse_query("RETURN CASE n.age WHEN 1 THEN 'one' ELSE 'other' END").unwrap();
        assert_eq!(q.clauses.len(), 1);
    }

    #[test]
    fn test_parse_case_search() {
        let q = parse_query("RETURN CASE WHEN n.age > 18 THEN 'adult' ELSE 'minor' END").unwrap();
        assert_eq!(q.clauses.len(), 1);
    }

    #[test]
    fn test_parse_variable_length() {
        let q = parse_query("MATCH (a)-[:KNOWS*1..3]->(b) RETURN b").unwrap();
        assert_eq!(q.clauses.len(), 2);
    }

    #[test]
    fn test_parse_distinct() {
        let q = parse_query("MATCH (n) RETURN DISTINCT n").unwrap();
        assert_eq!(q.clauses.len(), 2);
    }

    #[test]
    fn test_parse_collect_distinct() {
        let q = parse_query("MATCH (n) RETURN collect(DISTINCT n.name) AS names").unwrap();
        assert_eq!(q.clauses.len(), 2);
    }

    #[test]
    fn test_parse_multiple_patterns() {
        let q = parse_query("MATCH (a)-[:KNOWS]->(b), (b)-[:WORKS_AT]->(c) RETURN a, c").unwrap();
        assert_eq!(q.clauses.len(), 2);
    }

    #[test]
    fn test_parse_path_alias() {
        let q = parse_query("MATCH p = (a)-[:KNOWS]->(b) RETURN p").unwrap();
        assert_eq!(q.clauses.len(), 2);
    }

    #[test]
    fn test_parse_pattern_comprehension() {
        let q = parse_query("MATCH (a) RETURN [(a)-[:KNOWS]->(f) | f.name] AS friends").unwrap();
        assert_eq!(q.clauses.len(), 2);
    }

    #[test]
    fn test_parse_create_index() {
        let q = parse_query("CREATE INDEX ON :Person(name)").unwrap();
        assert_eq!(q.clauses.len(), 1);
    }

    #[test]
    fn test_parse_drop_index() {
        let q = parse_query("DROP INDEX ON :Person(name)").unwrap();
        assert_eq!(q.clauses.len(), 1);
    }

    #[test]
    fn test_parse_create_constraint() {
        let q = parse_query("CREATE CONSTRAINT ON (n:Person) ASSERT n.email IS UNIQUE").unwrap();
        assert_eq!(q.clauses.len(), 1);
    }

    #[test]
    fn test_parse_dotted_function() {
        let q = parse_query("RETURN apoc.coll.union([1,2], [2,3])").unwrap();
        assert_eq!(q.clauses.len(), 1);
        match &q.clauses[0] {
            Clause::Return { items, .. } => match &items[0].expression {
                Expression::Function {
                    name, arguments, ..
                } => {
                    assert_eq!(name, "union");
                    assert_eq!(arguments.len(), 2);
                }
                _ => panic!("expected function call"),
            },
            _ => panic!("expected RETURN"),
        }
    }

    #[test]
    fn test_parse_keyword_as_label() {
        let q = parse_query("MATCH (n:Index) RETURN n").unwrap();
        match &q.clauses[0] {
            Clause::Match { pattern, .. } => {
                assert_eq!(pattern.elements[0].node.labels.len(), 1);
            }
            _ => panic!("expected MATCH"),
        }
    }

    #[test]
    fn test_parse_optional_match() {
        let q = parse_query("OPTIONAL MATCH (n:Person) RETURN n").unwrap();
        assert_eq!(q.clauses.len(), 2);
    }

    #[test]
    fn test_parse_count_star() {
        let q = parse_query("MATCH (n) RETURN count(*) AS cnt").unwrap();
        assert_eq!(q.clauses.len(), 2);
    }

    #[test]
    fn test_parse_union() {
        let q = parse_query(
            "MATCH (n:Person) RETURN n.name AS name UNION MATCH (n:Employee) RETURN n.name AS name",
        )
        .unwrap();
        assert!(q.union.is_some());
        let u = q.union.unwrap();
        assert!(!u.all);
        assert_eq!(u.right.clauses.len(), 2);
    }

    #[test]
    fn test_parse_union_all() {
        let q = parse_query("RETURN 1 AS x UNION ALL RETURN 2 AS x").unwrap();
        assert!(q.union.is_some());
        let u = q.union.unwrap();
        assert!(u.all);
    }

    #[test]
    fn test_parse_reduce() {
        let q = parse_query("RETURN reduce(sum = 0, x IN [1,2,3] | sum + x) AS total").unwrap();
        assert_eq!(q.clauses.len(), 1);
        match &q.clauses[0] {
            Clause::Return { items, .. } => match &items[0].expression {
                Expression::Reduce {
                    accumulator,
                    variable,
                    list,
                    expression,
                    ..
                } => {
                    assert_eq!(accumulator, "sum");
                    assert_eq!(variable, "x");
                    assert!(matches!(list.as_ref(), Expression::List(_)));
                    assert!(matches!(expression.as_ref(), Expression::Add(_, _)));
                }
                _ => panic!("expected reduce expression"),
            },
            _ => panic!("expected RETURN"),
        }
    }

    #[test]
    fn test_parse_show_databases() {
        let q = parse_query("SHOW DATABASES").unwrap();
        assert_eq!(q.clauses.len(), 1);
        match &q.clauses[0] {
            Clause::Show { show_type } => assert!(matches!(show_type, ShowType::Databases)),
            _ => panic!("expected SHOW DATABASES"),
        }
    }

    #[test]
    fn test_parse_show_indexes() {
        let q = parse_query("SHOW INDEXES").unwrap();
        match &q.clauses[0] {
            Clause::Show { show_type } => assert!(matches!(show_type, ShowType::Indexes)),
            _ => panic!("expected SHOW INDEXES"),
        }
    }

    #[test]
    fn test_parse_show_constraints() {
        let q = parse_query("SHOW CONSTRAINTS").unwrap();
        match &q.clauses[0] {
            Clause::Show { show_type } => assert!(matches!(show_type, ShowType::Constraints)),
            _ => panic!("expected SHOW CONSTRAINTS"),
        }
    }

    #[test]
    fn test_parse_grant_privilege_to_user() {
        let q = parse_query("GRANT PRIVILEGE CREATE, DELETE TO USER user1").unwrap();
        match &q.clauses[0] {
            Clause::GrantPrivilege {
                privileges,
                target_name,
                is_user,
            } => {
                assert_eq!(target_name, "user1");
                assert!(*is_user);
                assert!(privileges.contains(&Privilege::Create));
                assert!(privileges.contains(&Privilege::Delete));
            }
            _ => panic!("expected GRANT PRIVILEGE"),
        }
    }

    #[test]
    fn test_parse_grant_privilege_to_role() {
        let q = parse_query("GRANT PRIVILEGE MATCH, MERGE TO ROLE myrole").unwrap();
        match &q.clauses[0] {
            Clause::GrantPrivilege {
                privileges,
                target_name,
                is_user,
            } => {
                assert_eq!(target_name, "myrole");
                assert!(!is_user);
                assert!(privileges.contains(&Privilege::Match));
                assert!(privileges.contains(&Privilege::Merge));
            }
            _ => panic!("expected GRANT PRIVILEGE TO ROLE"),
        }
    }

    #[test]
    fn test_parse_revoke_privilege() {
        let q = parse_query("REVOKE PRIVILEGE INDEX FROM USER user1").unwrap();
        match &q.clauses[0] {
            Clause::RevokePrivilege {
                privileges,
                target_name,
                is_user,
            } => {
                assert_eq!(target_name, "user1");
                assert!(*is_user);
                assert!(privileges.contains(&Privilege::Index));
            }
            _ => panic!("expected REVOKE PRIVILEGE"),
        }
    }

    #[test]
    fn test_parse_deny_privilege() {
        let q = parse_query("DENY AUTH TO ROLE admin_role").unwrap();
        match &q.clauses[0] {
            Clause::DenyPrivilege {
                privileges,
                target_name,
                is_user,
            } => {
                assert_eq!(target_name, "admin_role");
                assert!(!is_user);
                assert!(privileges.contains(&Privilege::Auth));
            }
            _ => panic!("expected DENY PRIVILEGE"),
        }
    }

    #[test]
    fn test_parse_show_privileges() {
        let q = parse_query("SHOW PRIVILEGES FOR USER user1").unwrap();
        match &q.clauses[0] {
            Clause::ShowPrivileges {
                target_name,
                is_user,
            } => {
                assert_eq!(target_name, "user1");
                assert!(*is_user);
            }
            _ => panic!("expected SHOW PRIVILEGES"),
        }
    }

    #[test]
    fn test_parse_show_privileges_for_role() {
        let q = parse_query("SHOW PRIVILEGES FOR ROLE myrole").unwrap();
        match &q.clauses[0] {
            Clause::ShowPrivileges {
                target_name,
                is_user,
            } => {
                assert_eq!(target_name, "myrole");
                assert!(!is_user);
            }
            _ => panic!("expected SHOW PRIVILEGES FOR ROLE"),
        }
    }

    #[test]
    fn test_parse_alter_user_set_password() {
        let q = parse_query("ALTER USER admin SET PASSWORD \"newsecret\"").unwrap();
        match &q.clauses[0] {
            Clause::AlterUser { username, action } => {
                assert_eq!(username, "admin");
                match action {
                    AlterUserAction::SetPassword { password } => assert_eq!(password, "newsecret"),
                    _ => panic!("expected SET PASSWORD action"),
                }
            }
            _ => panic!("expected ALTER USER"),
        }
    }

    #[test]
    fn test_parse_alter_user_rename() {
        let q = parse_query("ALTER USER oldname RENAME TO newname").unwrap();
        match &q.clauses[0] {
            Clause::AlterUser { username, action } => {
                assert_eq!(username, "oldname");
                match action {
                    AlterUserAction::RenameTo { new_name } => assert_eq!(new_name, "newname"),
                    _ => panic!("expected RENAME TO action"),
                }
            }
            _ => panic!("expected ALTER USER RENAME"),
        }
    }

    #[test]
    fn test_parse_begin() {
        let q = parse_query("BEGIN").unwrap();
        match &q.clauses[0] {
            Clause::BeginTransaction => {}
            _ => panic!("expected BEGIN TRANSACTION"),
        }
    }

    #[test]
    fn test_parse_commit() {
        let q = parse_query("COMMIT").unwrap();
        match &q.clauses[0] {
            Clause::CommitTransaction => {}
            _ => panic!("expected COMMIT"),
        }
    }

    #[test]
    fn test_parse_rollback() {
        let q = parse_query("ROLLBACK").unwrap();
        match &q.clauses[0] {
            Clause::RollbackTransaction => {}
            _ => panic!("expected ROLLBACK"),
        }
    }

    #[test]
    fn test_parse_set_storage_mode_in_memory_analytical() {
        let q = parse_query("SET STORAGE MODE IN_MEMORY_ANALYTICAL").unwrap();
        match &q.clauses[0] {
            Clause::SetStorageMode { mode } => {
                assert!(matches!(mode, StorageMode::InMemoryAnalytical));
            }
            _ => panic!("expected SET STORAGE MODE"),
        }
    }

    #[test]
    fn test_parse_set_storage_mode_in_memory_transactional() {
        let q = parse_query("SET STORAGE MODE IN_MEMORY_TRANSACTIONAL").unwrap();
        match &q.clauses[0] {
            Clause::SetStorageMode { mode } => {
                assert!(matches!(mode, StorageMode::InMemoryTransactional));
            }
            _ => panic!("expected SET STORAGE MODE"),
        }
    }

    #[test]
    fn test_parse_set_storage_mode_on_disk_transactional() {
        let q = parse_query("SET STORAGE MODE ON_DISK_TRANSACTIONAL").unwrap();
        match &q.clauses[0] {
            Clause::SetStorageMode { mode } => {
                assert!(matches!(mode, StorageMode::OnDiskTransactional));
            }
            _ => panic!("expected SET STORAGE MODE"),
        }
    }

    #[test]
    fn test_parse_grant_all_privileges() {
        let q = parse_query("GRANT ALL PRIVILEGES TO USER user1").unwrap();
        match &q.clauses[0] {
            Clause::GrantPrivilege {
                privileges,
                target_name,
                is_user,
            } => {
                assert_eq!(target_name, "user1");
                assert!(*is_user);
                assert_eq!(privileges.len(), Privilege::all().len());
            }
            _ => panic!("expected GRANT ALL PRIVILEGES"),
        }
    }
}
