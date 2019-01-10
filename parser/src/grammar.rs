use crate::ast::*;
use crate::tokenizer::{self, Pos, Token};
use crate::{Kind, Sql};

use combine::error::Tracked;
use combine::stream::easy::{Error, Errors, Info};
use combine::{satisfy, ConsumedResult, Parser};
use std::marker::PhantomData;

pub fn parse<'a>(input: &'a str) -> ParseResult<'a, Statement<'a>> {
  statement().parse_stream(&mut TokenStream::new(Sql(()), input))
}

fn statement<'a>() -> impl Parser<Input = TokenStream<'a>, Output = Statement<'a>> {
  use combine::parser::choice::choice;
  choice((create_table_statement().map(Statement::CreateTable),))
}

fn create_table_statement<'a>(
) -> impl Parser<Input = TokenStream<'a>, Output = CreateTableStatement<'a>> {
  (token(Kind::Create), token(Kind::Table), ident()).map(|(_, _, table_name)| {
    CreateTableStatement {
      table_name,
      column_defs: vec![],
    }
  })
}

fn column_def<'a>() -> impl Parser<Input = TokenStream<'a>, Output = ColumnDef<'a>> {
  (ident(),).map(|(column_name)| ColumnDef { column_name })
}

fn type_name<'a>() -> impl Parser<Input = TokenStream<'a>, Output = TypeName> {
  use combine::parser::choice::optional;

  (
    r#type(),
    optional(
      (
        token(Kind::LeftParen),
        token(Kind::NumericLiteral),
        token(Kind::RightParen),
      )
        .map(|(_, num, _)| num),
    )
    .map(|(name, argument)| TypeName { name, argument }),
  )
}

fn r#type<'a>() -> impl Parser<Input = TokenStream<'a>, Output = Type> {
  use combine::parser::choice::choice;

  choice((
    token(Kind::Integer).map(|_| Type::Integer),
    token(Kind::Varchar).map(|_| Type::Varchar),
  ))
}

fn ident<'a>() -> impl Parser<Input = TokenStream<'a>, Output = Ident<'a>> {
  token(Kind::Ident).map(|val| Ident::new(val.value))
}

#[cfg(test)]
mod tests {
  use super::*;

  fn assert_ast<'a, T: PartialEq + std::fmt::Debug>(
    mut parser: impl Parser<Input = TokenStream<'a>, Output = T>,
    input: &'a str,
    expected: T,
  ) {
    assert_eq!(
      parser
        .parse_stream(&mut TokenStream::new(Sql(()), input))
        .unwrap()
        .0,
      expected
    );
  }

  #[test]
  fn test_ident() {
    assert_ast(ident(), "abcd", Ident::new("abcd"));
  }

  #[test]
  fn test_create_table_column_def() {}
}

#[derive(Debug, Clone)]
pub struct TokenMatch<'a> {
  kind: Kind,
  phantom: PhantomData<&'a ()>,
}

impl<'a> Parser for TokenMatch<'a> {
  type Input = TokenStream<'a>;
  type Output = Token<'a, Kind>;
  type PartialState = ();

  fn parse_lazy(&mut self, input: &mut Self::Input) -> ConsumedResult<Self::Output, Self::Input> {
    satisfy(|c: Token<'a, Kind>| c.kind == self.kind).parse_lazy(input)
  }

  fn add_error(&mut self, error: &mut Tracked<Errors<Token<'a, Kind>, Token<'a, Kind>, Pos>>) {
    error
      .error
      .add_error(Error::Expected(Info::Owned(format!("{:?}", self.kind))));
  }
}

///
/// Matches a single token coming off of the stream
///
fn token<'a>(kind: Kind) -> TokenMatch<'a> {
  TokenMatch {
    kind,
    phantom: PhantomData,
  }
}

pub type TokenStream<'a> = tokenizer::TokenStream<'a, Sql>;
pub type ParseResult<'a, T> = combine::ParseResult<T, TokenStream<'a>>;
