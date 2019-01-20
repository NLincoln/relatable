use crate::ast::*;
use crate::tokenizer::{self, Pos, Token};
use crate::{Kind, Sql};

use combine::stream::easy::{Error, Errors, Info};
use combine::StreamOnce;
use combine::{satisfy, ConsumedResult, Parser};

use combine::error::{Consumed, Tracked};

pub type ParseError = Consumed<Tracked<<TokenStream as StreamOnce>::Error>>;

pub fn parse(input: String) -> Result<Vec<Statement>, ParseError> {
  use combine::parser::repeat::many1;
  many1(statement())
    .parse_stream(&mut TokenStream::new(Sql(()), input))
    .map(|result| result.0)
}

fn statement() -> impl Parser<Input = TokenStream, Output = Statement> {
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

fn create_table_statement() -> impl Parser<Input = TokenStream, Output = CreateTableStatement> {
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

fn column_def() -> impl Parser<Input = TokenStream, Output = ColumnDef> {
  (ident(), type_name()).map(|(column_name, type_name)| ColumnDef {
    column_name,
    type_name,
  })
}

fn type_name() -> impl Parser<Input = TokenStream, Output = TypeName> {
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

fn r#type() -> impl Parser<Input = TokenStream, Output = Type> {
  use combine::parser::choice::choice;

  choice((
    token(Kind::Integer).map(|_| Type::Integer),
    token(Kind::Varchar).map(|_| Type::Varchar),
  ))
}

fn select_statement() -> impl Parser<Input = TokenStream, Output = SelectStatement> {
  use combine::parser::{choice::optional, repeat::sep_by1};

  (
    token(Kind::Select),
    sep_by1(result_column(), token(Kind::Comma)),
    optional((token(Kind::From), table_list()).map(|(_, tables)| tables)),
  )
    .map(|(_, columns, tables)| SelectStatement { columns, tables })
}

fn table_list() -> impl Parser<Input = TokenStream, Output = Vec<Ident>> {
  use combine::parser::repeat::sep_by1;
  sep_by1(ident(), token(Kind::Comma))
}

fn result_column() -> impl Parser<Input = TokenStream, Output = ResultColumn> {
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

fn column_ident() -> impl Parser<Input = TokenStream, Output = ColumnIdent> {
  use combine::parser::{choice::choice, combinator::attempt};
  choice((
    attempt(
      (ident(), token(Kind::Period), ident()).map(|val| ColumnIdent {
        name: val.2,
        table: Some(val.0),
      }),
    ),
    ident().map(|val| ColumnIdent {
      name: val,
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
      name: Ident::new("users".into()),
      table: None,
    },
  );
  assert_ast(
    column_ident(),
    "users.username",
    ColumnIdent {
      name: Ident::new("username".into()),
      table: Some(Ident::new("users".into())),
    },
  );
}

fn expr() -> impl Parser<Input = TokenStream, Output = Expr> {
  use combine::parser::choice::choice;
  choice((
    literal_value().map(Expr::LiteralValue),
    column_ident().map(Expr::ColumnIdent),
  ))
}

fn literal_value() -> impl Parser<Input = TokenStream, Output = LiteralValue> {
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

fn numeric_literal() -> impl Parser<Input = TokenStream, Output = i64> {
  token(Kind::NumericLiteral).map(|token| token.value.parse::<i64>().unwrap())
}

fn string_literal() -> impl Parser<Input = TokenStream, Output = String> {
  token(Kind::StringLiteral).map(|token| {
    // need to strip off the leading and trailing '
    assert!(token.value.starts_with("'"));
    assert!(token.value.ends_with("'"));
    // TODO :: REMOVE CLONE
    token.value.trim_matches('\'').to_string()
  })
}

#[test]
fn test_string_literal() {
  assert_ast(string_literal(), "'abc'", "abc".to_string());
}

fn insert_statement() -> impl Parser<Input = TokenStream, Output = InsertStatement> {
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

fn insert_statement_values() -> impl Parser<Input = TokenStream, Output = InsertStatementValues> {
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
      Expr::LiteralValue(LiteralValue::StringLiteral("a".into())),
    ]),
  );
  assert_ast(
    insert_statement_values(),
    "VALUES (1, 'a'), (2, 'b')",
    InsertStatementValues::MultipleRows(vec![
      vec![
        Expr::LiteralValue(LiteralValue::NumericLiteral(1)),
        Expr::LiteralValue(LiteralValue::StringLiteral("a".into())),
      ],
      vec![
        Expr::LiteralValue(LiteralValue::NumericLiteral(2)),
        Expr::LiteralValue(LiteralValue::StringLiteral("b".into())),
      ],
    ]),
  );
}

fn blob_literal() -> impl Parser<Input = TokenStream, Output = Vec<u8>> {
  (token(Kind::X), string_literal()).map(|(_, string)| hex::decode(string).unwrap())
}

#[test]
fn test_blob_literal() {
  assert_ast(blob_literal(), "x'abc'", "abc".to_string().into_bytes())
}

fn ident() -> impl Parser<Input = TokenStream, Output = Ident> {
  token(Kind::Ident).map(|val| Ident::new(val.value))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_ident() {
    assert_ast(ident(), "abcd", Ident::new("abcd".into()));
  }

  #[test]
  fn test_select_statement() {
    assert_ast(
      select_statement(),
      "select *, users.*, users.username as name, username from users",
      SelectStatement {
        columns: vec![
          ResultColumn::Asterisk,
          ResultColumn::TableAsterisk(Ident::new("users".into())),
          ResultColumn::Expr {
            value: Expr::ColumnIdent(ColumnIdent {
              name: Ident::new("username".into()),
              table: Some(Ident::new("users".into())),
            }),
            alias: Some(Ident::new("name".into())),
          },
          ResultColumn::Expr {
            value: Expr::ColumnIdent(ColumnIdent {
              name: Ident::new("username".into()),
              table: None,
            }),
            alias: None,
          },
        ],
        tables: Some(vec![Ident::new("users".into())]),
      },
    )
  }

  #[test]
  fn test_create_table_column_def() {
    assert_ast(
      create_table_statement(),
      "create table users ( id integer, username varchar(20) )",
      CreateTableStatement {
        table_name: Ident::new("users".into()),
        column_defs: vec![
          ColumnDef {
            column_name: Ident::new("id".into()),
            type_name: TypeName {
              name: Type::Integer,
              argument: None,
            },
          },
          ColumnDef {
            column_name: Ident::new("username".into()),
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
pub struct TokenMatch {
  kind: Kind,
}

impl Parser for TokenMatch {
  type Input = TokenStream;
  type Output = Token<Kind>;
  type PartialState = ();

  fn parse_lazy(&mut self, input: &mut Self::Input) -> ConsumedResult<Self::Output, Self::Input> {
    satisfy(|c: Token<Kind>| c.kind == self.kind).parse_lazy(input)
  }

  fn add_error(&mut self, error: &mut Tracked<Errors<Token<Kind>, Token<Kind>, Pos>>) {
    error
      .error
      .add_error(Error::Expected(Info::Owned(format!("{:?}", self.kind))));
  }
}

///
/// Matches a single token coming off of the stream
///
fn token(kind: Kind) -> TokenMatch {
  TokenMatch { kind }
}

pub type TokenStream = tokenizer::TokenStream<Sql>;
pub type ParseResult<T> = combine::ParseResult<T, TokenStream>;

#[cfg(test)]
fn assert_ast<T: PartialEq + std::fmt::Debug>(
  mut parser: impl Parser<Input = TokenStream, Output = T>,
  input: &str,
  expected: T,
) {
  let result = parser
    .parse_stream(&mut TokenStream::new(Sql(()), input.to_string()))
    .map_err(|err| {
      panic!("{:#?}", err);
    })
    .unwrap();
  assert_eq!(result.0, expected);
}
