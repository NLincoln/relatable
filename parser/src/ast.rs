#[derive(Debug, Clone, PartialEq)]
pub struct Ident<'a>(&'a str);

impl<'a> Ident<'a> {
  pub fn new(text: &'a str) -> Self {
    Ident(text)
  }
}

pub enum Statement {
  CreateTable,
  Select,
  Insert,
}

pub struct CreateTable<'a> {
  table_name: Ident<'a>,
  column_defs: Vec<ColumnDef<'a>>,
}

pub struct ColumnDef<'a> {
  column_name: Ident<'a>,
}
