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
}

#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
  NumericLiteral(i64),
  StringLiteral(String),
  BlobLiteral(Vec<u8>),
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
