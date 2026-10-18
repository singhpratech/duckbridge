//! Neutral catalog model shared by importer, exporter and emitters.
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct Column {
    pub name: String,
    /// DuckDB data type string (e.g. `DECIMAL(18,3)`, `VARCHAR`, `INTEGER[]`).
    pub duck_type: String,
    pub nullable: bool,
    /// Raw DuckDB default expression.
    pub default: Option<String>,
    pub comment: Option<String>,
    // ---- filled by the type mapper ----
    pub sql_type: String,
    /// Expression used when SELECTing this column out of DuckDB.
    pub select_expr: String,
    pub conv: Conv,
    pub max_len: Option<usize>,
    /// (seed, increment) when the default is `nextval('seq')`.
    pub identity: Option<(i64, i64)>,
    pub enum_values: Option<Vec<String>>,
    pub warnings: Vec<String>,
}

/// How a DuckDB value is coerced on the way out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Conv {
    Identity,
    Bool,
    Float,
    BigDecimal,
    Bytes,
    Uuid,
    Date,
    Time,
    DateTime,
    DateTimeTz,
    TsNsText,
    Text,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ConstraintKind {
    PrimaryKey,
    Unique,
    ForeignKey,
    Check,
}

#[derive(Debug, Clone, Serialize)]
pub struct Constraint {
    pub kind: ConstraintKind,
    pub name: Option<String>,
    pub columns: Vec<String>,
    pub ref_schema: Option<String>,
    pub ref_table: Option<String>,
    pub ref_columns: Vec<String>,
    pub expression: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Index {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct Table {
    pub schema: String,
    pub name: String,
    pub columns: Vec<Column>,
    pub constraints: Vec<Constraint>,
    pub indexes: Vec<Index>,
    pub row_count: i64,
    pub comment: Option<String>,
}

impl Table {
    pub fn pk(&self) -> Option<&Constraint> {
        self.constraints.iter().find(|c| c.kind == ConstraintKind::PrimaryKey)
    }
    pub fn key_columns(&self) -> std::collections::HashSet<String> {
        let mut s = std::collections::HashSet::new();
        for c in &self.constraints {
            if c.kind != ConstraintKind::Check {
                s.extend(c.columns.iter().cloned());
            }
        }
        for i in &self.indexes {
            s.extend(i.columns.iter().cloned());
        }
        s
    }
    pub fn has_identity(&self) -> bool {
        self.columns.iter().any(|c| c.identity.is_some())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct View {
    pub schema: String,
    pub name: String,
    pub sql: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Sequence {
    pub schema: String,
    pub name: String,
    pub start: i64,
    pub increment: i64,
    pub last_value: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Catalog {
    pub source: String,
    pub duckdb_version: String,
    pub schemas: Vec<String>,
    pub tables: Vec<Table>,
    pub views: Vec<View>,
    pub sequences: Vec<Sequence>,
    pub warnings: Vec<String>,
}

impl Catalog {
    pub fn total_rows(&self) -> i64 {
        self.tables.iter().map(|t| t.row_count).sum()
    }
    pub fn column_warnings(&self) -> Vec<String> {
        let mut out = self.warnings.clone();
        for t in &self.tables {
            for c in &t.columns {
                for w in &c.warnings {
                    out.push(format!("{}.{}.{}: {}", t.schema, t.name, c.name, w));
                }
            }
        }
        out
    }
}
