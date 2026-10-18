//! Dependency-free SQL tokenizer shared by the T-SQL and DuckDB translators.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Ws,
    Comment,
    Str,
    Num,
    Ident,
    QIdent,
    Var,
    Temp,
    Op,
    Punct,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tok {
    pub kind: Kind,
    pub text: String,
}

impl Tok {
    pub fn new(kind: Kind, text: impl Into<String>) -> Self {
        Self { kind, text: text.into() }
    }
    pub fn ident(s: &str) -> Self { Tok::new(Kind::Ident, s) }
    pub fn op(s: &str) -> Self { Tok::new(Kind::Op, s) }
    pub fn punct(s: &str) -> Self { Tok::new(Kind::Punct, s) }
    pub fn ws() -> Self { Tok::new(Kind::Ws, " ") }
    pub fn str_lit(s: &str) -> Self { Tok::new(Kind::Str, format!("'{}'", s.replace('\'', "''"))) }
    pub fn num(s: impl ToString) -> Self { Tok::new(Kind::Num, s.to_string()) }

    pub fn is_kw(&self, w: &str) -> bool {
        self.kind == Kind::Ident && self.text.eq_ignore_ascii_case(w)
    }
    pub fn is_any_kw(&self, ws: &[&str]) -> bool {
        self.kind == Kind::Ident && ws.iter().any(|w| self.text.eq_ignore_ascii_case(w))
    }
    pub fn is_punct(&self, p: &str) -> bool {
        self.kind == Kind::Punct && self.text == p
    }
    pub fn is_op(&self, p: &str) -> bool {
        self.kind == Kind::Op && self.text == p
    }
    pub fn is_trivia(&self) -> bool {
        matches!(self.kind, Kind::Ws | Kind::Comment)
    }
    /// Unquoted identifier / unescaped string body.
    pub fn value(&self) -> String {
        let t = &self.text;
        match self.kind {
            Kind::QIdent => {
                let inner = &t[1..t.len() - 1];
                match t.as_bytes()[0] {
                    b'[' => inner.replace("]]", "]"),
                    b'"' => inner.replace("\"\"", "\""),
                    _ => inner.to_string(),
                }
            }
            Kind::Str => {
                let body = if t.starts_with('N') || t.starts_with('n') { &t[2..t.len() - 1] } else { &t[1..t.len() - 1] };
                body.replace("''", "'")
            }
            _ => t.clone(),
        }
    }
}

const OPS3: &[&str] = &["<=>", "!<>"];
const OPS2: &[&str] = &["<>", "!=", "<=", ">=", "||", "::", "//", "**", "->", "+=", "-=", "*=", "/=", "%=", "!<", "!>", "->"];

/// `bracket_idents`: treat `[name]` as a quoted identifier (T-SQL). Otherwise `[`/`]` are punctuation (DuckDB lists).
pub fn tokenize(sql: &str, bracket_idents: bool) -> Vec<Tok> {
    let b = sql.as_bytes();
    let n = b.len();
    let mut out = Vec::new();
    let mut i = 0;
    let push = |out: &mut Vec<Tok>, k: Kind, s: &str| out.push(Tok::new(k, s));
    while i < n {
        let c = b[i];
        let start = i;
        if c.is_ascii_whitespace() {
            while i < n && b[i].is_ascii_whitespace() { i += 1; }
            push(&mut out, Kind::Ws, &sql[start..i]);
            continue;
        }
        if sql[i..].starts_with("--") {
            i = sql[i..].find('\n').map(|j| i + j).unwrap_or(n);
            push(&mut out, Kind::Comment, &sql[start..i]);
            continue;
        }
        if sql[i..].starts_with("/*") {
            let mut depth = 1;
            i += 2;
            while i < n && depth > 0 {
                if sql[i..].starts_with("/*") { depth += 1; i += 2; }
                else if sql[i..].starts_with("*/") { depth -= 1; i += 2; }
                else { i += 1; }
            }
            push(&mut out, Kind::Comment, &sql[start..i]);
            continue;
        }
        if c == b'\'' || ((c == b'N' || c == b'n') && i + 1 < n && b[i + 1] == b'\'') {
            i += if c == b'\'' { 1 } else { 2 };
            while i < n {
                if b[i] == b'\'' {
                    if i + 1 < n && b[i + 1] == b'\'' { i += 2; continue; }
                    i += 1;
                    break;
                }
                i += 1;
            }
            push(&mut out, Kind::Str, &sql[start..i]);
            continue;
        }
        if sql[i..].starts_with("$$") {
            i = sql[i + 2..].find("$$").map(|j| i + 2 + j + 2).unwrap_or(n);
            push(&mut out, Kind::Str, &sql[start..i]);
            continue;
        }
        if c == b'[' && bracket_idents {
            i += 1;
            while i < n {
                if b[i] == b']' {
                    if i + 1 < n && b[i + 1] == b']' { i += 2; continue; }
                    break;
                }
                i += 1;
            }
            i = (i + 1).min(n);
            push(&mut out, Kind::QIdent, &sql[start..i]);
            continue;
        }
        if c == b'"' {
            i += 1;
            while i < n {
                if b[i] == b'"' {
                    if i + 1 < n && b[i + 1] == b'"' { i += 2; continue; }
                    break;
                }
                i += 1;
            }
            i = (i + 1).min(n);
            push(&mut out, Kind::QIdent, &sql[start..i]);
            continue;
        }
        if c == b'`' {
            i = sql[i + 1..].find('`').map(|j| i + 1 + j + 1).unwrap_or(n);
            push(&mut out, Kind::QIdent, &sql[start..i]);
            continue;
        }
        if c.is_ascii_digit() || (c == b'.' && i + 1 < n && b[i + 1].is_ascii_digit()) {
            while i < n && (b[i].is_ascii_alphanumeric() || b[i] == b'.' || b[i] == b'_') {
                if (b[i] == b'e' || b[i] == b'E') && i + 1 < n && (b[i + 1] == b'+' || b[i + 1] == b'-') { i += 1; }
                i += 1;
            }
            push(&mut out, Kind::Num, &sql[start..i]);
            continue;
        }
        if c == b'@' {
            while i < n && (b[i].is_ascii_alphanumeric() || b"@_$#".contains(&b[i])) { i += 1; }
            push(&mut out, Kind::Var, &sql[start..i]);
            continue;
        }
        if c == b'#' {
            while i < n && (b[i].is_ascii_alphanumeric() || b"_#$".contains(&b[i])) { i += 1; }
            push(&mut out, Kind::Temp, &sql[start..i]);
            continue;
        }
        if c.is_ascii_alphabetic() || c == b'_' || c >= 0x80 {
            while i < n && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$' || b[i] >= 0x80) { i += 1; }
            push(&mut out, Kind::Ident, &sql[start..i]);
            continue;
        }
        if let Some(op) = OPS3.iter().find(|o| sql[i..].starts_with(**o)) {
            push(&mut out, Kind::Op, op); i += 3; continue;
        }
        if let Some(op) = OPS2.iter().find(|o| sql[i..].starts_with(**o)) {
            push(&mut out, Kind::Op, op); i += 2; continue;
        }
        if b"+-*/%<>=&|^~!".contains(&c) {
            push(&mut out, Kind::Op, &sql[i..i + 1]); i += 1; continue;
        }
        if b"(),;.[]".contains(&c) {
            push(&mut out, Kind::Punct, &sql[i..i + 1]); i += 1; continue;
        }
        // any other byte (keep utf-8 boundaries intact)
        let ch_len = sql[i..].chars().next().map(|ch| ch.len_utf8()).unwrap_or(1);
        push(&mut out, Kind::Op, &sql[i..i + ch_len]);
        i += ch_len;
    }
    out
}

pub fn join(toks: &[Tok]) -> String {
    toks.iter().map(|t| t.text.as_str()).collect()
}

/// Index of the ')' matching the '(' at `i`.
pub fn match_paren(toks: &[Tok], i: usize) -> usize {
    let mut depth = 0i32;
    for j in i..toks.len() {
        if toks[j].is_punct("(") { depth += 1; }
        else if toks[j].is_punct(")") {
            depth -= 1;
            if depth == 0 { return j; }
        }
    }
    toks.len().saturating_sub(1)
}

/// Split on a top-level separator token text, respecting parentheses.
pub fn split_top(toks: &[Tok], sep: &str) -> Vec<Vec<Tok>> {
    let mut parts: Vec<Vec<Tok>> = vec![vec![]];
    let mut depth = 0i32;
    for t in toks {
        if t.is_punct("(") { depth += 1; }
        else if t.is_punct(")") { depth -= 1; }
        if depth == 0 && (t.kind == Kind::Punct || t.kind == Kind::Op) && t.text == sep {
            parts.push(vec![]);
        } else {
            parts.last_mut().unwrap().push(t.clone());
        }
    }
    parts
}

/// Split into statements on top-level ';' and on T-SQL `GO` separators.
pub fn statements(toks: &[Tok]) -> Vec<Vec<Tok>> {
    let mut out = vec![];
    let mut cur: Vec<Tok> = vec![];
    let mut depth = 0i32;
    for t in toks {
        if t.is_punct("(") { depth += 1; }
        else if t.is_punct(")") { depth -= 1; }
        if depth == 0 && (t.is_punct(";") || t.is_kw("GO")) {
            if cur.iter().any(|x| !x.is_trivia()) { out.push(std::mem::take(&mut cur)); } else { cur.clear(); }
            continue;
        }
        cur.push(t.clone());
    }
    if cur.iter().any(|x| !x.is_trivia()) { out.push(cur); }
    out
}

/// Position of the next non-trivia token at or after `i`.
pub fn next_sig(toks: &[Tok], mut i: usize) -> Option<usize> {
    while i < toks.len() {
        if !toks[i].is_trivia() { return Some(i); }
        i += 1;
    }
    None
}

/// Position of the previous non-trivia token before `i`.
pub fn prev_sig(toks: &[Tok], i: usize) -> Option<usize> {
    let mut j = i;
    while j > 0 {
        j -= 1;
        if !toks[j].is_trivia() { return Some(j); }
    }
    None
}

pub fn strip_trivia(toks: &[Tok]) -> Vec<Tok> {
    toks.iter().filter(|t| !t.is_trivia()).cloned().collect()
}

pub fn trim(toks: &[Tok]) -> &[Tok] {
    let mut a = 0;
    let mut b = toks.len();
    while a < b && toks[a].is_trivia() { a += 1; }
    while b > a && toks[b - 1].is_trivia() { b -= 1; }
    &toks[a..b]
}
