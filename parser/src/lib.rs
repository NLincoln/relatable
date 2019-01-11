mod ast;
mod grammar;
mod lang;
mod tokenizer;
mod type_checking;

use self::lang::{Kind, Sql};

pub use self::grammar::parse;
pub use self::type_checking::{typecheck_statement, SchemaQuery};

#[derive(Debug)]
pub enum AstError<'a> {
  ParseError(crate::grammar::ParseError<'a>),
  TypeError(crate::type_checking::TypeError<'a>),
}

impl<'a> From<crate::grammar::ParseError<'a>> for AstError<'a> {
  fn from(err: crate::grammar::ParseError<'a>) -> AstError<'a> {
    AstError::ParseError(err)
  }
}

impl<'a> From<crate::type_checking::TypeError<'a>> for AstError<'a> {
  fn from(err: crate::type_checking::TypeError<'a>) -> AstError<'a> {
    AstError::TypeError(err)
  }
}

pub fn process_query<'a>(
  text: &'a str,
  db: &mut impl SchemaQuery,
) -> Result<Vec<crate::ast::Statement<'a>>, AstError<'a>> {
  let ast = parse(text)?;
  for statement in ast.iter() {
    typecheck_statement(statement, db)?;
  }
  Ok(ast)
}
