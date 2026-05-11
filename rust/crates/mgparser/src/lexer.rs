//! Cypher lexer — tokenizes a query string into tokens.

use std::fmt;
use std::iter::Peekable;
use std::str::CharIndices;

/// Token produced by the lexer.
#[derive(Clone, PartialEq, Debug)]
pub enum Token {
    // Keywords
    Match,
    Optional,
    Create,
    Merge,
    Delete,
    Detach,
    Set,
    Remove,
    Return,
    With,
    Unwind,
    As,
    Where,
    Order,
    By,
    Asc,
    Desc,
    Skip,
    Limit,
    Distinct,
    And,
    Or,
    Not,
    In,
    Is,
    Null,
    True,
    False,
    On,
    Call,
    Yield,
    Union,
    Show,
    All,
    Any,
    None_,
    Single,
    Filter,
    Extract,
    Reduce,
    Case,
    When,
    Then,
    Else,
    End,
    Exists,
    Foreach,
    StartsWith,
    EndsWith,
    Contains,
    Load,
    Csv,
    Jsonl,
    From,
    Headers,
    Index,
    Drop,
    Constraint,
    Assert,
    Unique,
    Typed,
    Explain,
    Profile,
    User,
    Role,
    Grant,
    Revoke,
    To,
    Identified,
    Password,
    Trigger,
    Before,
    After,
    Execute,
    Vertex,
    Edge,
    Database,
    Force,
    Bfs,
    WShortest,
    AllShortest,
    KShortest,
    ShortestPath,
    AllShortestPaths,
    Using,
    Periodic,
    Commit,
    Transactions,
    Of,
    Rows,
    Setting,
    Settings,
    Terminate,
    Hops,
    Begin,
    Rollback,
    Deny,
    Alter,
    Rename,
    Privilege,
    Privileges,
    Storage,
    Mode,
    Analytical,
    Transactional,
    OnDisk,
    InMemory,
    InMemoryAnalytical,
    InMemoryTransactional,
    OnDiskTransactional,
    For,

    // Symbols
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Comma,
    Colon,
    Semicolon,
    Dot,
    DotDot,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    ArrowRight,  // ->
    ArrowLeft,   // <-
    PlusEq,      // +=
    Tilde,       // ~ for regex
    Pipe,        // | for FOREACH

    // Literals
    Identifier(String),
    StringLiteral(String),
    IntLiteral(i64),
    FloatLiteral(f64),
    Parameter(String), // $name
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::Match => write!(f, "MATCH"),
            Token::Optional => write!(f, "OPTIONAL"),
            Token::Create => write!(f, "CREATE"),
            Token::Return => write!(f, "RETURN"),
            Token::Identifier(s) => write!(f, "{}", s),
            Token::StringLiteral(s) => write!(f, "\"{}\"", s),
            Token::IntLiteral(n) => write!(f, "{}", n),
            Token::FloatLiteral(n) => write!(f, "{}", n),
            _ => write!(f, "{:?}", self),
        }
    }
}

/// Lexer state.
pub struct Lexer<'a> {
    chars: Peekable<CharIndices<'a>>,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self {
            chars: input.char_indices().peekable(),
        }
    }

    /// Tokenize the entire input.
    pub fn tokenize(&mut self) -> Vec<Token> {
        let mut tokens = Vec::new();
        while let Some(token) = self.next_token() {
            tokens.push(token);
        }
        tokens
    }

    fn next_token(&mut self) -> Option<Token> {
        self.skip_whitespace();
        let (_pos, ch) = self.chars.next()?;

        match ch {
            '(' => Some(Token::LParen),
            ')' => Some(Token::RParen),
            '[' => Some(Token::LBracket),
            ']' => Some(Token::RBracket),
            '{' => Some(Token::LBrace),
            '}' => Some(Token::RBrace),
            ',' => Some(Token::Comma),
            ':' => Some(Token::Colon),
            ';' => Some(Token::Semicolon),
            '.' => {
                if self.peek_char() == Some('.') {
                    self.chars.next();
                    Some(Token::DotDot)
                } else {
                    Some(Token::Dot)
                }
            }
            '%' => Some(Token::Percent),
            '~' => Some(Token::Tilde),
            '+' => {
                if self.peek_char() == Some('=') {
                    self.chars.next();
                    Some(Token::PlusEq)
                } else {
                    Some(Token::Plus)
                }
            }
            '-' => {
                if self.peek_char() == Some('>') {
                    self.chars.next();
                    Some(Token::ArrowRight)
                } else {
                    Some(Token::Minus)
                }
            }
            '*' => Some(Token::Star),
            '/' => {
                if self.peek_char() == Some('/') {
                    // Line comment — skip to end of line
                    self.skip_line();
                    self.next_token()
                } else {
                    Some(Token::Slash)
                }
            }
            '<' => {
                if self.peek_char() == Some('-') {
                    self.chars.next();
                    Some(Token::ArrowLeft)
                } else if self.peek_char() == Some('=') {
                    self.chars.next();
                    Some(Token::Lte)
                } else if self.peek_char() == Some('>') {
                    self.chars.next();
                    Some(Token::Neq)
                } else {
                    Some(Token::Lt)
                }
            }
            '>' => {
                if self.peek_char() == Some('=') {
                    self.chars.next();
                    Some(Token::Gte)
                } else {
                    Some(Token::Gt)
                }
            }
            '=' => {
                if self.peek_char() == Some('=') {
                    self.chars.next();
                    Some(Token::Eq)
                } else if self.peek_char() == Some('~') {
                    self.chars.next();
                    Some(Token::Tilde)
                } else {
                    // Single = is not valid Cypher, treat as error marker
                    Some(Token::Eq)
                }
            }
            '!' => {
                if self.peek_char() == Some('=') {
                    self.chars.next();
                    Some(Token::Neq)
                } else {
                    Some(Token::Not)
                }
            }
            '|' => Some(Token::Pipe),
            '$' => self.read_parameter(),
            '"' | '\'' => self.read_string(ch),
            '0'..='9' => self.read_number(ch),
            'a'..='z' | 'A'..='Z' | '_' => self.read_identifier(ch),
            _ => {
                // Skip unrecognized characters
                self.next_token()
            }
        }
    }

    fn skip_whitespace(&mut self) {
        while let Some(&(_, ch)) = self.chars.peek() {
            if ch.is_whitespace() {
                self.chars.next();
            } else {
                break;
            }
        }
    }

    fn skip_line(&mut self) {
        while let Some(&(_, ch)) = self.chars.peek() {
            self.chars.next();
            if ch == '\n' {
                break;
            }
        }
    }

    fn peek_char(&mut self) -> Option<char> {
        self.chars.peek().map(|&(_, ch)| ch)
    }

    fn read_string(&mut self, quote: char) -> Option<Token> {
        let mut s = String::new();
        while let Some(&(_, ch)) = self.chars.peek() {
            if ch == quote {
                self.chars.next();
                return Some(Token::StringLiteral(s));
            }
            if ch == '\\' {
                self.chars.next(); // skip backslash
                if let Some(&(_, escaped)) = self.chars.peek() {
                    self.chars.next();
                    match escaped {
                        'n' => s.push('\n'),
                        't' => s.push('\t'),
                        '\\' => s.push('\\'),
                        '"' => s.push('"'),
                        '\'' => s.push('\''),
                        _ => {
                            s.push('\\');
                            s.push(escaped);
                        }
                    }
                }
            } else {
                self.chars.next();
                s.push(ch);
            }
        }
        Some(Token::StringLiteral(s))
    }

    fn read_number(&mut self, first: char) -> Option<Token> {
        let mut s = String::new();
        s.push(first);
        let mut is_float = false;

        while let Some(&(_, ch)) = self.chars.peek() {
            if ch.is_ascii_digit() {
                self.chars.next();
                s.push(ch);
            } else if ch == '.' {
                // Only treat as float if next char is a digit.
                // This avoids eating '..' in constructions like '*1..3'.
                let mut ahead = self.chars.clone();
                ahead.next(); // consume the '.'
                if ahead.peek().map(|&(_, c)| c.is_ascii_digit()).unwrap_or(false) {
                    self.chars.next();
                    s.push(ch);
                    is_float = true;
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        if is_float {
            s.parse().ok().map(Token::FloatLiteral)
        } else {
            s.parse().ok().map(Token::IntLiteral)
        }
    }

    fn read_identifier(&mut self, first: char) -> Option<Token> {
        let mut s = String::new();
        s.push(first);

        while let Some(&(_, ch)) = self.chars.peek() {
            if ch.is_alphanumeric() || ch == '_' {
                self.chars.next();
                s.push(ch);
            } else {
                break;
            }
        }

        Some(Self::keyword_or_identifier(&s))
    }

    fn read_parameter(&mut self) -> Option<Token> {
        let mut s = String::new();
        while let Some(&(_, ch)) = self.chars.peek() {
            if ch.is_alphanumeric() || ch == '_' {
                self.chars.next();
                s.push(ch);
            } else {
                break;
            }
        }
        if s.is_empty() {
            // Bare $ with no name — skip it
            self.next_token()
        } else {
            Some(Token::Parameter(s))
        }
    }

    fn keyword_or_identifier(s: &str) -> Token {
        // ASCII-only case-insensitive keyword matching (no allocation)
        if s.eq_ignore_ascii_case("MATCH") { return Token::Match; }
        if s.eq_ignore_ascii_case("OPTIONAL") { return Token::Optional; }
        if s.eq_ignore_ascii_case("CREATE") { return Token::Create; }
        if s.eq_ignore_ascii_case("MERGE") { return Token::Merge; }
        if s.eq_ignore_ascii_case("DELETE") { return Token::Delete; }
        if s.eq_ignore_ascii_case("DETACH") { return Token::Detach; }
        if s.eq_ignore_ascii_case("SET") { return Token::Set; }
        if s.eq_ignore_ascii_case("REMOVE") { return Token::Remove; }
        if s.eq_ignore_ascii_case("RETURN") { return Token::Return; }
        if s.eq_ignore_ascii_case("WITH") { return Token::With; }
        if s.eq_ignore_ascii_case("UNWIND") { return Token::Unwind; }
        if s.eq_ignore_ascii_case("AS") { return Token::As; }
        if s.eq_ignore_ascii_case("WHERE") { return Token::Where; }
        if s.eq_ignore_ascii_case("ORDER") { return Token::Order; }
        if s.eq_ignore_ascii_case("BY") { return Token::By; }
        if s.eq_ignore_ascii_case("ASC") { return Token::Asc; }
        if s.eq_ignore_ascii_case("DESC") { return Token::Desc; }
        if s.eq_ignore_ascii_case("SKIP") { return Token::Skip; }
        if s.eq_ignore_ascii_case("LIMIT") { return Token::Limit; }
        if s.eq_ignore_ascii_case("DISTINCT") { return Token::Distinct; }
        if s.eq_ignore_ascii_case("AND") { return Token::And; }
        if s.eq_ignore_ascii_case("OR") { return Token::Or; }
        if s.eq_ignore_ascii_case("NOT") { return Token::Not; }
        if s.eq_ignore_ascii_case("IN") { return Token::In; }
        if s.eq_ignore_ascii_case("IS") { return Token::Is; }
        if s.eq_ignore_ascii_case("NULL") { return Token::Null; }
        if s.eq_ignore_ascii_case("TRUE") { return Token::True; }
        if s.eq_ignore_ascii_case("FALSE") { return Token::False; }
        if s.eq_ignore_ascii_case("ON") { return Token::On; }
        if s.eq_ignore_ascii_case("UNION") { return Token::Union; }
        if s.eq_ignore_ascii_case("ALL") { return Token::All; }
        if s.eq_ignore_ascii_case("ANY") { return Token::Any; }
        if s.eq_ignore_ascii_case("NONE") { return Token::None_; }
        if s.eq_ignore_ascii_case("SINGLE") { return Token::Single; }
        if s.eq_ignore_ascii_case("FILTER") { return Token::Filter; }
        if s.eq_ignore_ascii_case("EXTRACT") { return Token::Extract; }
        if s.eq_ignore_ascii_case("REDUCE") { return Token::Reduce; }
        if s.eq_ignore_ascii_case("CASE") { return Token::Case; }
        if s.eq_ignore_ascii_case("WHEN") { return Token::When; }
        if s.eq_ignore_ascii_case("THEN") { return Token::Then; }
        if s.eq_ignore_ascii_case("ELSE") { return Token::Else; }
        if s.eq_ignore_ascii_case("END") { return Token::End; }
        if s.eq_ignore_ascii_case("EXISTS") { return Token::Exists; }
        if s.eq_ignore_ascii_case("FOREACH") { return Token::Foreach; }
        if s.eq_ignore_ascii_case("CALL") { return Token::Call; }
        if s.eq_ignore_ascii_case("YIELD") { return Token::Yield; }
        if s.eq_ignore_ascii_case("SHOW") { return Token::Show; }
        if s.eq_ignore_ascii_case("CONTAINS") { return Token::Contains; }
        if s.eq_ignore_ascii_case("STARTS") { return Token::StartsWith; }
        if s.eq_ignore_ascii_case("ENDS") { return Token::EndsWith; }
        if s.eq_ignore_ascii_case("LOAD") { return Token::Load; }
        if s.eq_ignore_ascii_case("CSV") { return Token::Csv; }
        if s.eq_ignore_ascii_case("JSONL") { return Token::Jsonl; }
        if s.eq_ignore_ascii_case("FROM") { return Token::From; }
        if s.eq_ignore_ascii_case("HEADERS") { return Token::Headers; }
        if s.eq_ignore_ascii_case("INDEX") { return Token::Index; }
        if s.eq_ignore_ascii_case("DROP") { return Token::Drop; }
        if s.eq_ignore_ascii_case("CONSTRAINT") { return Token::Constraint; }
        if s.eq_ignore_ascii_case("ASSERT") { return Token::Assert; }
        if s.eq_ignore_ascii_case("UNIQUE") { return Token::Unique; }
        if s.eq_ignore_ascii_case("TYPED") { return Token::Typed; }
        if s.eq_ignore_ascii_case("EXPLAIN") { return Token::Explain; }
        if s.eq_ignore_ascii_case("PROFILE") { return Token::Profile; }
        if s.eq_ignore_ascii_case("USER") { return Token::User; }
        if s.eq_ignore_ascii_case("ROLE") { return Token::Role; }
        if s.eq_ignore_ascii_case("GRANT") { return Token::Grant; }
        if s.eq_ignore_ascii_case("REVOKE") { return Token::Revoke; }
        if s.eq_ignore_ascii_case("TO") { return Token::To; }
        if s.eq_ignore_ascii_case("IDENTIFIED") { return Token::Identified; }
        if s.eq_ignore_ascii_case("PASSWORD") { return Token::Password; }
        if s.eq_ignore_ascii_case("TRIGGER") { return Token::Trigger; }
        if s.eq_ignore_ascii_case("BEFORE") { return Token::Before; }
        if s.eq_ignore_ascii_case("AFTER") { return Token::After; }
        if s.eq_ignore_ascii_case("EXECUTE") { return Token::Execute; }
        if s.eq_ignore_ascii_case("VERTEX") { return Token::Vertex; }
        if s.eq_ignore_ascii_case("EDGE") { return Token::Edge; }
        if s.eq_ignore_ascii_case("DATABASE") { return Token::Database; }
        if s.eq_ignore_ascii_case("FORCE") { return Token::Force; }
        if s.eq_ignore_ascii_case("BFS") { return Token::Bfs; }
        if s.eq_ignore_ascii_case("WSHORTEST") { return Token::WShortest; }
        if s.eq_ignore_ascii_case("ALLSHORTEST") { return Token::AllShortest; }
        if s.eq_ignore_ascii_case("KSHORTEST") { return Token::KShortest; }
        if s.eq_ignore_ascii_case("SHORTESTPATH") { return Token::ShortestPath; }
        if s.eq_ignore_ascii_case("ALLSHORTESTPATHS") { return Token::AllShortestPaths; }
        if s.eq_ignore_ascii_case("USING") { return Token::Using; }
        if s.eq_ignore_ascii_case("PERIODIC") { return Token::Periodic; }
        if s.eq_ignore_ascii_case("COMMIT") { return Token::Commit; }
        if s.eq_ignore_ascii_case("TRANSACTIONS") { return Token::Transactions; }
        if s.eq_ignore_ascii_case("OF") { return Token::Of; }
        if s.eq_ignore_ascii_case("ROWS") { return Token::Rows; }
        if s.eq_ignore_ascii_case("SETTING") { return Token::Setting; }
        if s.eq_ignore_ascii_case("SETTINGS") { return Token::Settings; }
        if s.eq_ignore_ascii_case("TERMINATE") { return Token::Terminate; }
        if s.eq_ignore_ascii_case("HOPS") { return Token::Hops; }
        if s.eq_ignore_ascii_case("BEGIN") { return Token::Begin; }
        if s.eq_ignore_ascii_case("ROLLBACK") { return Token::Rollback; }
        if s.eq_ignore_ascii_case("DENY") { return Token::Deny; }
        if s.eq_ignore_ascii_case("ALTER") { return Token::Alter; }
        if s.eq_ignore_ascii_case("RENAME") { return Token::Rename; }
        if s.eq_ignore_ascii_case("PRIVILEGE") { return Token::Privilege; }
        if s.eq_ignore_ascii_case("PRIVILEGES") { return Token::Privileges; }
        if s.eq_ignore_ascii_case("STORAGE") { return Token::Storage; }
        if s.eq_ignore_ascii_case("MODE") { return Token::Mode; }
        if s.eq_ignore_ascii_case("ANALYTICAL") { return Token::Analytical; }
        if s.eq_ignore_ascii_case("TRANSACTIONAL") { return Token::Transactional; }
        if s.eq_ignore_ascii_case("ON_DISK") { return Token::OnDisk; }
        if s.eq_ignore_ascii_case("IN_MEMORY") { return Token::InMemory; }
        if s.eq_ignore_ascii_case("IN_MEMORY_ANALYTICAL") { return Token::InMemoryAnalytical; }
        if s.eq_ignore_ascii_case("IN_MEMORY_TRANSACTIONAL") { return Token::InMemoryTransactional; }
        if s.eq_ignore_ascii_case("ON_DISK_TRANSACTIONAL") { return Token::OnDiskTransactional; }
        if s.eq_ignore_ascii_case("FOR") { return Token::For; }
        Token::Identifier(s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_match_return() {
        let mut lexer = Lexer::new("MATCH (n) RETURN n");
        let tokens = lexer.tokenize();
        assert_eq!(tokens.len(), 6);
        assert_eq!(tokens[0], Token::Match);
        assert_eq!(tokens[1], Token::LParen);
        assert_eq!(tokens[2], Token::Identifier("n".into()));
        assert_eq!(tokens[3], Token::RParen);
        assert_eq!(tokens[4], Token::Return);
        assert_eq!(tokens[5], Token::Identifier("n".into()));
    }

    #[test]
    fn test_tokenize_create() {
        let mut lexer = Lexer::new("CREATE (n:Person {name: \"Alice\"})");
        let tokens = lexer.tokenize();
        assert_eq!(tokens[0], Token::Create);
        assert_eq!(tokens[1], Token::LParen);
        assert_eq!(tokens[2], Token::Identifier("n".into()));
        assert_eq!(tokens[3], Token::Colon);
        assert_eq!(tokens[4], Token::Identifier("Person".into()));
    }

    #[test]
    fn test_tokenize_where() {
        let mut lexer = Lexer::new("WHERE n.age > 30 AND n.name = \"Bob\"");
        let tokens = lexer.tokenize();
        assert_eq!(tokens[0], Token::Where);
    }

    #[test]
    fn test_tokenize_numbers() {
        let mut lexer = Lexer::new("42 3.14");
        let tokens = lexer.tokenize();
        assert_eq!(tokens[0], Token::IntLiteral(42));
        assert_eq!(tokens[1], Token::FloatLiteral(3.14));
    }

    #[test]
    fn test_tokenize_arrow() {
        let mut lexer = Lexer::new("(a)-[r]->(b)");
        let tokens = lexer.tokenize();
        assert!(tokens.contains(&Token::ArrowRight));
    }

    #[test]
    fn test_tokenize_dotted_function() {
        let mut lexer = Lexer::new("apoc.coll.union([1,2], [2,3])");
        let tokens = lexer.tokenize();
        assert_eq!(tokens[0], Token::Identifier("apoc".into()));
        assert_eq!(tokens[1], Token::Dot);
        assert_eq!(tokens[2], Token::Identifier("coll".into()));
        assert_eq!(tokens[3], Token::Dot);
        assert_eq!(tokens[4], Token::Union); // 'union' is a keyword
        assert_eq!(tokens[5], Token::LParen);
    }
}
