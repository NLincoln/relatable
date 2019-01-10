use crate::tokenizer::{Keyword, Language, Punctuation, RegexToken};

/*
 * we want to start out being able to parse the following SQL:
 * ```
 * CREATE TABLE users (
 *   id INTEGER(8),
 *   username VARCHAR(20)
 * );
 * INSERT INTO users VALUE (1, "nlincoln");
 * SELECT * FROM users;
 * ```
 *
 * More will be added as needed. Grammar largely comes from sqlite (see grammar.g4)
 */
pub struct Sql(pub(crate) ());

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Kind {
  Create,
  Table,
  Integer,

  Varchar,
  Insert,
  Into,
  Value,
  Select,
  From,

  Ident,

  StringLiteral,
  NumericLiteral,

  Comma,
  LeftParen,
  RightParen,
  SemiColon,
  Asterisk,
}
impl Language for Sql {
  type Kind = Kind;
  fn keywords() -> Vec<Keyword<Kind>> {
    vec![
      ("create", Kind::Create),
      ("table", Kind::Table),
      ("integer", Kind::Integer),
      ("varchar", Kind::Varchar),
      ("insert", Kind::Insert),
      ("into", Kind::Into),
      ("value", Kind::Value),
      ("select", Kind::Select),
      ("from", Kind::From),
    ]
    .into_iter()
    .map(|(text, kind)| Keyword::create(text, kind).set_case_sensitive(false))
    .collect()
  }
  fn punctuation() -> Vec<Punctuation<Kind>> {
    vec![
      (",", Kind::Comma),
      ("(", Kind::LeftParen),
      (")", Kind::RightParen),
      (";", Kind::SemiColon),
      ("*", Kind::Asterisk),
    ]
    .into_iter()
    .map(|(text, kind)| Punctuation::create(text, kind))
    .collect()
  }
  fn regexes() -> Vec<RegexToken<Kind>> {
    vec![
      RegexToken::create("[a-zA-Z_][a-zA-Z_0-9]*", Kind::Ident),
      RegexToken::create(r"'\w*?'", Kind::StringLiteral),
      RegexToken::create(r"[0-9]+", Kind::NumericLiteral),
    ]
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use combine::easy::Error;
  use combine::{Positioned, StreamOnce};
  use crate::tokenizer::TokenStream;

  fn tok_str(s: &str) -> Vec<&str> {
    let mut r = Vec::new();
    let mut s = TokenStream::new(Sql(()), s);
    loop {
      match s.uncons() {
        Ok(x) => r.push(x.value),
        Err(ref e) if e == &Error::end_of_input() => break,
        Err(e) => panic!("Parse error at {}: {}", s.position(), e),
      }
    }
    return r;
  }
  fn tok_typ(s: &str) -> Vec<Kind> {
    let mut r = Vec::new();
    let mut s = TokenStream::new(Sql(()), s);
    loop {
      match s.uncons() {
        Ok(x) => r.push(x.kind),
        Err(ref e) if e == &Error::end_of_input() => break,
        Err(e) => panic!("Parse error at {}: {}", s.position(), e),
      }
    }
    return r;
  }

  fn assert_tokens(text: &str, types: &[Kind], tokens: &[&str]) {
    assert_eq!(tok_typ(text), types);
    assert_eq!(tok_str(text), tokens);
  }
  #[test]
  fn test_string_literals() {
    assert_tokens(
      "123 'a1' 456",
      &[
        Kind::NumericLiteral,
        Kind::StringLiteral,
        Kind::NumericLiteral,
      ],
      &["123", "'a1'", "456"],
    );
  }
}
