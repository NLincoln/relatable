//! Type checking for sql queries
//! Requires two things: the statement AST, and
//! something that can be used to get table schemas.

use crate::ast::*;
use schema::{Schema, SchemaError};

pub trait SchemaQuery {
  fn get_table(&mut self, name: &str) -> Result<Schema, SchemaError>;
}
#[derive(Debug)]
pub enum TypeError<'a> {
  TableNotFound(&'a str),
  TableAlreadyExists(&'a str),
  SchemaError(SchemaError),
}

impl<'a> From<SchemaError> for TypeError<'a> {
  fn from(err: SchemaError) -> TypeError<'a> {
    TypeError::SchemaError(err)
  }
}

pub fn typecheck_statement<'a>(
  ast: &Statement<'a>,
  db: &mut impl SchemaQuery,
) -> Result<(), TypeError<'a>> {
  match ast {
    Statement::CreateTable(create_table_statement) => {
      typecheck_create_table_statement(create_table_statement, db)
    }
    Statement::Select(select_stmt) => typecheck_select_statement(select_stmt, db),
    Statement::Insert(insert_stmt) => typecheck_insert_statement(insert_stmt, db),
  }
}

fn typecheck_create_table_statement<'a>(
  ast: &CreateTableStatement<'a>,
  db: &mut impl SchemaQuery,
) -> Result<(), TypeError<'a>> {
  let table_name = ast.table_name.text();
  match db.get_table(table_name) {
    Ok(_) => {
      // table already exists!
      Err(TypeError::TableAlreadyExists(table_name))
    }
    // table doesn't exist yet, we're good to create it
    Err(SchemaError::TableNotFound) => Ok(()),
    Err(err) => Err(err.into()),
  }
}

fn typecheck_select_statement<'a>(
  ast: &SelectStatement<'a>,
  db: &mut impl SchemaQuery,
) -> Result<(), TypeError<'a>> {
  unimplemented!()
}

fn typecheck_insert_statement<'a>(
  ast: &InsertStatement<'a>,
  db: &mut impl SchemaQuery,
) -> Result<(), TypeError<'a>> {
  let schema = db.get_table(ast.table.text())?;

  Ok(())
}
