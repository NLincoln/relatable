use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub struct Ident<'a>(&'a str);

impl<'a> Ident<'a> {
  pub fn new(text: &'a str) -> Self {
    Ident(text)
  }
  pub fn text(&self) -> &'a str {
    self.0
  }
}

impl<'a> fmt::Display for Ident<'a> {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "\"{}\"", self.text())
  }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement<'a> {
  CreateTable(CreateTableStatement<'a>),
  Select(SelectStatement<'a>),
  Insert(InsertStatement<'a>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStatement<'a> {
  pub table_name: Ident<'a>,
  pub column_defs: Vec<ColumnDef<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef<'a> {
  pub column_name: Ident<'a>,
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
pub struct SelectStatement<'a> {
  pub columns: Vec<ResultColumn<'a>>,
  pub table: Option<Ident<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResultColumn<'a> {
  /// SELECT *
  Asterisk,
  /// SELECT table.*
  TableAsterisk(Ident<'a>),

  Expr {
    value: Expr<'a>,
    alias: Option<Ident<'a>>,
  },
}

/// Anywhere a column can appear, there can be:
/// 1. just the column name
/// 2. the column name + the table name
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnIdent<'a> {
  pub column: Ident<'a>,
  pub table: Option<Ident<'a>>,
}

impl<'a> ToString for ColumnIdent<'a> {
  fn to_string(&self) -> String {
    match self.table {
      None => format!("{}", self.column),
      Some(ref table) => format!("{}.{}", table, self.column),
    }
  }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr<'a> {
  LiteralValue(LiteralValue<'a>),
  ColumnIdent(ColumnIdent<'a>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue<'a> {
  NumericLiteral(i64),
  StringLiteral(&'a str),
  BlobLiteral(&'a str),
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement<'a> {
  pub table: Ident<'a>,
  pub columns: Vec<Ident<'a>>,
  /// VALUES (1, 'nlincoln'), (2, 'asdf')
  pub values: InsertStatementValues<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InsertStatementValues<'a> {
  SingleRow(Vec<Expr<'a>>),
  MultipleRows(Vec<Vec<Expr<'a>>>),
}
