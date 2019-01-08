use crate::ast::*;
use crate::tokenizer::{self, Pos, Token};
use crate::{Kind, Sql};

use combine::error::Tracked;
use combine::stream::easy::{Error, Errors, Info};
use combine::{satisfy, ConsumedResult, Parser};
use std::marker::PhantomData;

pub fn parse<'a>(input: &'a str) -> ParseResult<'a, Statement<'a>> {
  combine::parser(statement).parse
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

fn ident<'a>() -> impl Parser<Input = TokenStream<'a>, Output = Ident<'a>> {
  token(Kind::Ident).map(|val| Ident::new(val.value))
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
