use std::fmt;

#[derive(Debug, Clone, PartialEq, Hash, Ord, PartialOrd, Eq)]
pub struct Ident(String);

impl Ident {
  pub fn new(text: String) -> Self {
    Ident(text)
  }
  pub fn text(&self) -> &str {
    &self.0
  }
}

impl From<String> for Ident {
  fn from(string: String) -> Ident {
    Ident::new(string)
  }
}

impl fmt::Display for Ident {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "\"{}\"", self.text())
  }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
  CreateTable(CreateTableStatement),
  Select(SelectStatement),
  Insert(InsertStatement),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStatement {
  pub table_name: Ident,
  pub column_defs: Vec<ColumnDef>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
  pub column_name: Ident,
  pub type_name: TypeName,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TypeName {
  pub name: Type,
  pub argument: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Type {
  Integer,
  Blob,
  Varchar,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
  pub columns: Vec<ResultColumn>,
  pub tables: Option<Vec<Ident>>,
  pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResultColumn {
  /// SELECT *
  Asterisk,
  /// SELECT table.*
  TableAsterisk(Ident),

  Expr {
    value: Expr,
    alias: Option<Ident>,
  },
}

/// Anywhere a column can appear, there can be:
/// 1. just the column name
/// 2. the column name + the table name
#[derive(Debug, Clone, PartialEq, Hash, Ord, PartialOrd, Eq)]
pub struct ColumnIdent {
  pub name: Ident,
  pub table: Option<Ident>,
}

impl ToString for ColumnIdent {
  fn to_string(&self) -> String {
    match self.table {
      None => format!("{}", self.name),
      Some(ref table) => format!("{}.{}", table, self.name),
    }
  }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
  LiteralValue(LiteralValue),
  ColumnIdent(ColumnIdent),
  RelOp(RelOp),
  Expr(Box<Expr>),
}

impl Expr {
  pub fn eagerly_evaluate(&self) -> Option<LiteralValue> {
    match self {
      Expr::LiteralValue(value) => Some(value.clone()),
      Expr::ColumnIdent(_) => None,
      Expr::RelOp(RelOp { lhs, rhs, kind }) => {
        let lhs = lhs.eagerly_evaluate()?;
        let rhs = rhs.eagerly_evaluate()?;
        let val = match kind {
          RelOpKind::Equals => lhs == rhs,
          RelOpKind::NotEquals => lhs != rhs,
        };
        Some(LiteralValue::BooleanLiteral(val))
      }
      Expr::Expr(sub_expr) => sub_expr.eagerly_evaluate(),
    }
  }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelOp {
  pub lhs: Box<Expr>,
  pub rhs: Box<Expr>,
  pub kind: RelOpKind,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RelOpKind {
  Equals,
  NotEquals,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
  NumericLiteral(i64),
  StringLiteral(String),
  BlobLiteral(Vec<u8>),
  BooleanLiteral(bool),
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
  pub table: Ident,
  pub columns: Vec<Ident>,
  /// VALUES (1, 'nlincoln'), (2, 'asdf')
  pub values: InsertStatementValues,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InsertStatementValues {
  SingleRow(Vec<Expr>),
  MultipleRows(Vec<Vec<Expr>>),
}
