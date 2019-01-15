mod ast;
mod grammar;
mod lang;
mod tokenizer;

pub use self::ast::*;
use self::lang::{Kind, Sql};

use self::grammar::parse;

#[derive(Debug)]
pub enum AstError<'a> {
  ParseError(crate::grammar::ParseError<'a>),
}

impl<'a> From<crate::grammar::ParseError<'a>> for AstError<'a> {
  fn from(err: crate::grammar::ParseError<'a>) -> AstError<'a> {
    AstError::ParseError(err)
  }
}

pub fn process_query<'a>(text: &'a str) -> Result<Vec<crate::ast::Statement<'a>>, AstError<'a>> {
  let ast = parse(text)?;
  Ok(ast)
}
