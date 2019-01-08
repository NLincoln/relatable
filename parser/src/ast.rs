#[derive(Debug, Clone, PartialEq)]
pub struct Ident<'a>(&'a str);

impl<'a> Ident<'a> {
  pub fn new(text: &'a str) -> Self {
    Ident(text)
  }
}

#[derive(Debug)]
pub enum Statement<'a> {
  CreateTable(CreateTableStatement<'a>),
  Select(SelectStatement<'a>),
  Insert(InsertStatement<'a>),
}

#[derive(Debug)]
pub struct CreateTableStatement<'a> {
  pub table_name: Ident<'a>,
  pub column_defs: Vec<ColumnDef<'a>>,
}

#[derive(Debug)]
pub struct ColumnDef<'a> {
  pub column_name: Ident<'a>,
  pub type_name: TypeName<'a>,
}

#[derive(Debug)]
pub struct TypeName<'a> {
  pub name: &'a str,
  pub argument: Option<i64>,
}

#[derive(Debug)]
pub struct SelectStatement<'a> {
  pub columns: Vec<ResultColumn<'a>>,
  pub tables: Vec<Ident<'a>>,
}

#[derive(Debug)]
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
#[derive(Debug)]
pub struct ColumnIdent<'a> {
  pub column: Ident<'a>,
  pub table: Option<Ident<'a>>,
}

#[derive(Debug)]
pub enum Expr<'a> {
  LiteralValue(LiteralValue<'a>),
  ColumnIdent(ColumnIdent<'a>),
}

#[derive(Debug)]
pub enum LiteralValue<'a> {
  NumericLiteral(i64),
  StringLiteral(&'a str),
  BlobLiteral(&'a str),
  Null,
}

#[derive(Debug)]
pub struct InsertStatement<'a> {
  pub table: Ident<'a>,
  pub columns: Vec<Ident<'a>>,
  /// VALUES (1, 'nlincoln'), (2, 'asdf')
  pub values: Vec<Vec<Expr<'a>>>,
}
