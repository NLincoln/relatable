mod ast;
mod grammar;
mod lang;
mod tokenizer;

pub use self::ast::*;
use self::lang::{Kind, Sql};

use self::grammar::parse;

#[derive(Debug)]
pub enum AstError {
  ParseError(crate::grammar::ParseError),
}

impl<'a> From<crate::grammar::ParseError> for AstError {
  fn from(err: crate::grammar::ParseError) -> AstError {
    AstError::ParseError(err)
  }
}

pub fn process_query(text: String) -> Result<Vec<crate::ast::Statement>, AstError> {
  let ast = parse(text)?;
  Ok(ast)
}
