use crate::ast::*;
use crate::tokenizer::{self, Pos, Token};
use crate::{Kind, Sql};

use combine::stream::easy::{Error, Errors, Info};
use combine::StreamOnce;
use combine::{satisfy, ConsumedResult, Parser};
use std::marker::PhantomData;

use combine::error::{Consumed, Tracked};

pub type ParseError<'a> = Consumed<Tracked<<TokenStream<'a> as StreamOnce>::Error>>;

pub fn parse<'a>(input: &'a str) -> Result<Vec<Statement<'a>>, ParseError<'a>> {
  use combine::parser::repeat::many1;
  many1(statement())
    .parse_stream(&mut TokenStream::new(Sql(()), input))
    .map(|result| result.0)
}

fn statement<'a>() -> impl Parser<Input = TokenStream<'a>, Output = Statement<'a>> {
  use combine::parser::choice::choice;
  (
    choice((
      create_table_statement().map(Statement::CreateTable),
      select_statement().map(Statement::Select),
      insert_statement().map(Statement::Insert),
    )),
    token(Kind::SemiColon),
  )
    .map(|(statement, _)| statement)
}

fn create_table_statement<'a>(
) -> impl Parser<Input = TokenStream<'a>, Output = CreateTableStatement<'a>> {
  use combine::parser::repeat::sep_by1;
  (
    token(Kind::Create),
    token(Kind::Table),
    ident(),
    token(Kind::LeftParen),
    sep_by1(column_def(), token(Kind::Comma)),
    token(Kind::RightParen),
  )
    .map(
      |(_, _, table_name, _, column_defs, _)| CreateTableStatement {
        table_name,
        column_defs,
      },
    )
}

fn column_def<'a>() -> impl Parser<Input = TokenStream<'a>, Output = ColumnDef<'a>> {
  (ident(), type_name()).map(|(column_name, type_name)| ColumnDef {
    column_name,
    type_name,
  })
}

fn type_name<'a>() -> impl Parser<Input = TokenStream<'a>, Output = TypeName> {
  use combine::parser::choice::optional;

  (
    r#type(),
    optional(
      (
        token(Kind::LeftParen),
        numeric_literal(),
        token(Kind::RightParen),
      )
        .map(|(_, num, _)| num),
    ),
  )
    .map(|(name, argument)| TypeName { name, argument })
}

fn r#type<'a>() -> impl Parser<Input = TokenStream<'a>, Output = Type> {
  use combine::parser::choice::choice;

  choice((
    token(Kind::Integer).map(|_| Type::Integer),
    token(Kind::Varchar).map(|_| Type::Varchar),
  ))
}

fn select_statement<'a>() -> impl Parser<Input = TokenStream<'a>, Output = SelectStatement<'a>> {
  use combine::parser::{choice::optional, repeat::sep_by1};

  (
    token(Kind::Select),
    sep_by1(result_column(), token(Kind::Comma)),
    optional((token(Kind::From), ident()).map(|(_, tables)| tables)),
  )
    .map(|(_, columns, table)| SelectStatement { columns, table })
}

fn result_column<'a>() -> impl Parser<Input = TokenStream<'a>, Output = ResultColumn<'a>> {
  use combine::parser::{
    choice::{choice, optional},
    combinator::attempt,
  };

  choice((
    attempt(
      (ident(), token(Kind::Period), token(Kind::Asterisk))
        .map(|(ident, _, _)| ResultColumn::TableAsterisk(ident)),
    ),
    token(Kind::Asterisk).map(|_| ResultColumn::Asterisk),
    (
      expr(),
      optional((optional(token(Kind::As)), ident()).map(|(_, alias)| alias)),
    )
      .map(|(value, alias)| ResultColumn::Expr { value, alias }),
  ))
}

fn column_ident<'a>() -> impl Parser<Input = TokenStream<'a>, Output = ColumnIdent<'a>> {
  use combine::parser::{choice::choice, combinator::attempt};
  choice((
    attempt(
      (ident(), token(Kind::Period), ident()).map(|val| ColumnIdent {
        column: val.2,
        table: Some(val.0),
      }),
    ),
    ident().map(|val| ColumnIdent {
      column: val,
      table: None,
    }),
  ))
}

#[test]
fn test_column_ident() {
  assert_ast(
    column_ident(),
    "users",
    ColumnIdent {
      column: Ident::new("users"),
      table: None,
    },
  );
  assert_ast(
    column_ident(),
    "users.username",
    ColumnIdent {
      column: Ident::new("username"),
      table: Some(Ident::new("users")),
    },
  );
}

fn expr<'a>() -> impl Parser<Input = TokenStream<'a>, Output = Expr<'a>> {
  use combine::parser::choice::choice;
  choice((
    literal_value().map(Expr::LiteralValue),
    column_ident().map(Expr::ColumnIdent),
  ))
}

fn literal_value<'a>() -> impl Parser<Input = TokenStream<'a>, Output = LiteralValue<'a>> {
  use combine::parser::choice::choice;
  choice((
    numeric_literal().map(LiteralValue::NumericLiteral),
    string_literal().map(LiteralValue::StringLiteral),
    blob_literal().map(LiteralValue::BlobLiteral),
  ))
}

#[test]
fn test_literal_value() {
  assert_ast(literal_value(), "123", LiteralValue::NumericLiteral(123));
}

fn numeric_literal<'a>() -> impl Parser<Input = TokenStream<'a>, Output = i64> {
  token(Kind::NumericLiteral).map(|token| token.value.parse::<i64>().unwrap())
}

fn string_literal<'a>() -> impl Parser<Input = TokenStream<'a>, Output = &'a str> {
  token(Kind::StringLiteral).map(|token| {
    // need to strip off the leading and trailing '
    assert!(token.value.starts_with("'"));
    assert!(token.value.ends_with("'"));
    token.value.trim_matches('\'')
  })
}

#[test]
fn test_string_literal() {
  assert_ast(string_literal(), "'abc'", "abc");
}

fn insert_statement<'a>() -> impl Parser<Input = TokenStream<'a>, Output = InsertStatement<'a>> {
  use combine::parser::repeat::sep_by;

  (
    (token(Kind::Insert), token(Kind::Into)),
    ident(),
    token(Kind::LeftParen),
    sep_by(ident(), token(Kind::Comma)),
    token(Kind::RightParen),
    insert_statement_values(),
  )
    .map(|(_, table, _, columns, _, values)| InsertStatement {
      table,
      columns,
      values,
    })
}

fn insert_statement_values<'a>(
) -> impl Parser<Input = TokenStream<'a>, Output = InsertStatementValues<'a>> {
  use combine::parser::{
    choice::choice,
    combinator::attempt,
    repeat::{sep_by, sep_by1},
  };
  let single_row = || {
    (
      token(Kind::LeftParen),
      sep_by(expr(), token(Kind::Comma)),
      token(Kind::RightParen),
    )
      .map(|(_, exprs, _)| exprs)
  };

  choice((
    attempt((token(Kind::Value), single_row()))
      .map(|(_, row)| InsertStatementValues::SingleRow(row)),
    attempt((
      token(Kind::Values),
      sep_by1(single_row(), token(Kind::Comma)),
    ))
    .map(|(_, rows)| InsertStatementValues::MultipleRows(rows)),
  ))
}

#[test]
fn test_insert_statement_values() {
  assert_ast(
    insert_statement_values(),
    "VALUE (1, 'a')",
    InsertStatementValues::SingleRow(vec![
      Expr::LiteralValue(LiteralValue::NumericLiteral(1)),
      Expr::LiteralValue(LiteralValue::StringLiteral("a")),
    ]),
  );
  assert_ast(
    insert_statement_values(),
    "VALUES (1, 'a'), (2, 'b')",
    InsertStatementValues::MultipleRows(vec![
      vec![
        Expr::LiteralValue(LiteralValue::NumericLiteral(1)),
        Expr::LiteralValue(LiteralValue::StringLiteral("a")),
      ],
      vec![
        Expr::LiteralValue(LiteralValue::NumericLiteral(2)),
        Expr::LiteralValue(LiteralValue::StringLiteral("b")),
      ],
    ]),
  );
}

fn blob_literal<'a>() -> impl Parser<Input = TokenStream<'a>, Output = &'a str> {
  (token(Kind::X), string_literal()).map(|(_, string)| string)
}

#[test]
fn test_blob_literal() {
  assert_ast(blob_literal(), "x'abc'", "abc")
}

fn ident<'a>() -> impl Parser<Input = TokenStream<'a>, Output = Ident<'a>> {
  token(Kind::Ident).map(|val| Ident::new(val.value))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_ident() {
    assert_ast(ident(), "abcd", Ident::new("abcd"));
  }

  #[test]
  fn test_select_statement() {
    assert_ast(
      select_statement(),
      "select *, users.*, users.username as name, username from users",
      SelectStatement {
        columns: vec![
          ResultColumn::Asterisk,
          ResultColumn::TableAsterisk(Ident::new("users")),
          ResultColumn::Expr {
            value: Expr::ColumnIdent(ColumnIdent {
              column: Ident::new("username"),
              table: Some(Ident::new("users")),
            }),
            alias: Some(Ident::new("name")),
          },
          ResultColumn::Expr {
            value: Expr::ColumnIdent(ColumnIdent {
              column: Ident::new("username"),
              table: None,
            }),
            alias: None,
          },
        ],
        table: Some(Ident::new("users")),
      },
    )
  }

  #[test]
  fn test_create_table_column_def() {
    assert_ast(
      create_table_statement(),
      "create table users ( id integer, username varchar(20) )",
      CreateTableStatement {
        table_name: Ident::new("users"),
        column_defs: vec![
          ColumnDef {
            column_name: Ident::new("id"),
            type_name: TypeName {
              name: Type::Integer,
              argument: None,
            },
          },
          ColumnDef {
            column_name: Ident::new("username"),
            type_name: TypeName {
              name: Type::Varchar,
              argument: Some(20),
            },
          },
        ],
      },
    )
  }
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

#[cfg(test)]
fn assert_ast<'a, T: PartialEq + std::fmt::Debug>(
  mut parser: impl Parser<Input = TokenStream<'a>, Output = T>,
  input: &'a str,
  expected: T,
) {
  let result = parser
    .parse_stream(&mut TokenStream::new(Sql(()), input))
    .map_err(|err| {
      panic!("{:#?}", err);
    })
    .unwrap();
  assert_eq!(result.0, expected);
}
