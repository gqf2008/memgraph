#![allow(dead_code)]
//! # mgparser — Cypher query parser
//!
//! Lexer and recursive descent parser for the Cypher query language.

pub mod ast;
pub mod lexer;
pub mod parser;

pub use ast::{Fingerprint, Query};
pub use parser::{parse_query, parse_query_with_catalog};
