use combine::easy::{Error, Errors};
use combine::error::StreamError;
use combine::stream::Resetable;
use combine::{Positioned, StreamOnce};
use std::fmt;
use std::fmt::Debug;

/// Original position of element in source code
#[derive(PartialOrd, Ord, PartialEq, Eq, Clone, Copy, Default)]
pub struct Pos {
  /// One-based line number
  pub line: usize,
  /// One-based column number
  pub column: usize,
}

impl fmt::Debug for Pos {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "Pos({}:{})", self.line, self.column)
  }
}

impl fmt::Display for Pos {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{}:{}", self.line, self.column)
  }
}

/// A token in the grammar.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Token<K> {
  pub kind: K,
  pub value: String,
}

pub trait Language {
  type Kind: PartialEq + Debug + Clone + Copy;
  fn keywords() -> Vec<Keyword<Self::Kind>> {
    vec![]
  }
  fn punctuation() -> Vec<Punctuation<Self::Kind>> {
    vec![]
  }
  fn regexes() -> Vec<RegexToken<Self::Kind>> {
    vec![]
  }

  #[allow(unused_variables)]
  fn peek_token(&self, text: &str) -> Option<(Self::Kind, usize)> {
    None
  }
  #[allow(unused_variables)]
  fn skip_comments(&self, text: &str) -> Option<usize> {
    None
  }
}

/// The stream of tokens through the grammar
/// This is usually what you want to be passing around when building the grammar.
#[derive(Debug)]
pub struct TokenStream<L: Language> {
  keywords: Vec<Keyword<L::Kind>>,
  punctuation: Vec<Punctuation<L::Kind>>,
  regexes: Vec<RegexToken<L::Kind>>,
  language: L,
  buf: String,
  position: Pos,
  off: usize,
  next_state: Option<(usize, Token<L::Kind>, usize, Pos)>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Checkpoint {
  position: Pos,
  off: usize,
}

impl<L: Language> StreamOnce for TokenStream<L> {
  type Item = Token<L::Kind>;
  type Range = Token<L::Kind>;
  type Position = Pos;
  type Error = Errors<Token<L::Kind>, Token<L::Kind>, Pos>;

  fn uncons(&mut self) -> Result<Self::Item, Error<Token<L::Kind>, Token<L::Kind>>> {
    if let Some((at, tok, off, pos)) = &self.next_state {
      if *at == self.off {
        self.off = *off;
        self.position = pos.clone();
        return Ok(tok.clone());
      }
    }
    let old_pos = self.off;
    let (kind, len) = self.peek_token()?;
    // TODO :: this is probably super slow. Need a String type that can handle removing the front of it.
    // or go back to being zero-copy :eyes:. But that makes the lifetimes wicked complicated
    let value = &self.buf[self.off - len..self.off];
    let value = value.to_string();

    self.skip_whitespace();
    let token = Token { kind, value };
    self.next_state = Some((old_pos, token.clone(), self.off, self.position));
    Ok(token)
  }
}

impl<L: Language> Positioned for TokenStream<L> {
  fn position(&self) -> Self::Position {
    self.position
  }
}

impl<L: Language> Resetable for TokenStream<L> {
  type Checkpoint = Checkpoint;
  fn checkpoint(&self) -> Self::Checkpoint {
    Checkpoint {
      position: self.position,
      off: self.off,
    }
  }
  fn reset(&mut self, checkpoint: Checkpoint) {
    self.position = checkpoint.position;
    self.off = checkpoint.off;
  }
}

#[derive(Debug, Clone)]
pub struct Keyword<T> {
  pub is_case_sensitive: bool,
  pub text: &'static str,
  pub token: T,
}

impl<T> Keyword<T> {
  pub fn create(text: &'static str, token: T) -> Keyword<T> {
    Keyword {
      text,
      token,
      is_case_sensitive: false,
    }
  }
  pub fn set_case_sensitive(mut self, is_sensitive: bool) -> Keyword<T> {
    self.is_case_sensitive = is_sensitive;
    return self;
  }
}

#[derive(Debug, Clone)]
pub struct Punctuation<T> {
  pub text: &'static str,
  pub token: T,
}

impl<T> Punctuation<T> {
  pub fn create(text: &'static str, token: T) -> Self {
    Self { text, token }
  }
}

#[derive(Debug, Clone)]
pub struct RegexToken<T> {
  pub regex: regex::Regex,
  pub token: T,
}

impl<T> RegexToken<T> {
  pub fn create(regex: &'static str, token: T) -> Self {
    Self {
      regex: regex::Regex::new(&format!("^{}", regex)).unwrap(),
      token,
    }
  }
}

impl<L: Language> TokenStream<L> {
  #[allow(dead_code)]
  pub fn new(lang: L, s: String) -> TokenStream<L> {
    let mut me = TokenStream {
      keywords: L::keywords(),
      punctuation: L::punctuation(),
      regexes: L::regexes(),
      language: lang,
      buf: s,
      position: Pos { line: 1, column: 1 },
      off: 0,
      next_state: None,
    };
    me.skip_whitespace();
    me
  }

  /// Helper function that updates the current position / offsets
  /// forward one line.
  /// Usually you don't need this, unless you're processing a language
  /// with multiline strings.
  pub fn next_line(&mut self) {
    self.off += 1;
    self.position.line += 1;
    self.position.column = 1;
  }

  pub fn next_char(&self) -> Option<char> {
    let mut iter = self.get_str().chars();
    return iter.next();
  }

  /// Helper function for swallowing a single character.
  ///
  /// Returns the character that was swallowing
  pub fn swallow_token(&mut self) -> Option<char> {
    return self.next_char().map(|val| match val {
      '\n' => {
        self.next_line();
        val
      }
      _ => {
        self.off += 1;
        self.position.column += 1;
        val
      }
    });
  }

  fn swallow_n_tokens(&mut self, num: usize) -> usize {
    for i in 0..num {
      if self.swallow_token().is_none() {
        return i;
      }
    }
    return num;
  }

  /// Get the current string of the TokenStream
  pub fn get_str(&self) -> &str {
    &self.buf[self.off..]
  }

  fn peek_punctuation(&self) -> Option<Punctuation<L::Kind>> {
    let iter = self.get_str();
    for punc in self.punctuation.iter() {
      if iter.starts_with(punc.text) {
        return Some(punc.clone());
      }
    }
    None
  }

  fn peek_keyword(&self) -> Option<Keyword<L::Kind>> {
    let iter = self.get_str();
    for key in self.keywords.iter() {
      if !key.is_case_sensitive {
        // Pull off key.length tokens from the iter
        if let Some(next_tokens) = iter.get(0..key.text.len()) {
          if next_tokens.to_lowercase() == key.text {
            return Some(key.clone());
          }
        }
      } else if iter.starts_with(key.text) {
        return Some(key.clone());
      }
    }
    None
  }

  fn peek_regexes<'b>(&self, text: &'b str) -> Option<(RegexToken<L::Kind>, regex::Match<'b>)> {
    for regextoken in self.regexes.iter() {
      if let Some(reg_match) = regextoken.regex.find(text) {
        assert!(reg_match.start() == 0);
        return Some((regextoken.clone(), reg_match));
      }
    }
    None
  }

  fn peek_token(&mut self) -> Result<(L::Kind, usize), Error<Token<L::Kind>, Token<L::Kind>>> {
    let mut iter = self.buf[self.off..].char_indices();
    /*
     * Eagerly handle EOF.
     */
    let cur_char = match iter.next() {
      Some((_, x)) => x,
      None => return Err(Error::end_of_input()),
    };

    if let Some(punc) = self.peek_punctuation() {
      let length = punc.text.len();
      self.swallow_n_tokens(length);
      return Ok((punc.token, length));
    }

    if let Some(key) = self.peek_keyword() {
      let length = key.text.len();
      self.swallow_n_tokens(length);
      return Ok((key.token, length));
    }

    if let Some((regex_token, regex_match)) = self.peek_regexes(self.get_str()) {
      let length = regex_match.as_str().len();
      self.swallow_n_tokens(length);
      return Ok((regex_token.token, length));
    }

    if let Some((kind, offset)) = self.language.peek_token(self.get_str()) {
      self.swallow_n_tokens(offset);
      return Ok((kind, offset));
    }

    Err(Error::unexpected_message(format_args!(
      "unexpected character {:?}",
      cur_char
    )))
  }

  fn skip_whitespace(&mut self) {
    // This code is really messy, need to clean it up a bit.
    let mut iter = self.buf[self.off..].chars();
    loop {
      if let Some(comment_chars) = self.language.skip_comments(iter.as_str()) {
        self.swallow_n_tokens(comment_chars);
        iter = self.buf[self.off..].chars();
        continue;
      }

      let cur_char = match iter.next() {
        Some(pair) => pair,
        None => {
          self.off += self.buf.len() - self.off;
          break;
        }
      };

      match cur_char {
        '\u{feff}' | '\r' => {
          self.off += 1;
          continue;
        }
        '\t' => {
          self.position.column += 2;
          self.off += 1;
        }
        '\n' => {
          self.position.column = 1;
          self.position.line += 1;
          self.off += 1;
        }
        ' ' => {
          self.position.column += 1;
          self.off += 1;
        }
        _ => {
          break;
        }
      }
    }
  }

  #[allow(dead_code)]
  fn update_position(&mut self, len: usize) {
    let val = &self.buf[self.off..][..len];
    self.off += len;
    let lines = val.as_bytes().iter().filter(|&&x| x == b'\n').count();
    self.position.line += lines;
    if lines > 0 {
      let line_offset = val.rfind('\n').unwrap() + 1;
      let num = val[line_offset..].chars().count();
      self.position.column = num + 1;
    } else {
      let num = val.chars().count();
      self.position.column += num;
    }
  }
}

impl<K: PartialEq + Debug> fmt::Display for Token<K> {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{}[{:?}]", self.value, self.kind)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /*
   * In order to properly test the tokenizer, we create a dummy language.
   * It's fairly simple
   * ```
   * var a := (123 b)
   * ```
   */
  #[derive(Debug, PartialEq, Clone, Copy)]
  enum Kind {
    Assign,     // :=
    Colon,      // :
    Equal,      // =
    LeftParen,  // (
    RightParen, // )
    Var,        // var
    Program,    // program (case insensitive)
    Ident,      // a sequence of lowercase letters
    IntValue,   // a sequence of numbers
  }

  #[derive(Debug)]
  struct Simple {}
  impl Language for Simple {
    type Kind = Kind;
    fn keywords() -> Vec<Keyword<Kind>> {
      vec![
        Keyword::create("var", Kind::Var),
        Keyword::create("program", Kind::Program).set_case_sensitive(false),
      ]
    }
    fn punctuation() -> Vec<Punctuation<Kind>> {
      vec![
        Punctuation::create(":=", Kind::Assign),
        Punctuation::create(":", Kind::Colon),
        Punctuation::create("=", Kind::Equal),
        Punctuation::create("(", Kind::LeftParen),
        Punctuation::create(")", Kind::RightParen),
      ]
    }
    fn regexes() -> Vec<RegexToken<Kind>> {
      vec![
        RegexToken::create("[a-z]+", Kind::Ident),
        RegexToken::create("[0-9]+", Kind::IntValue),
      ]
    }

    fn skip_comments(&self, text: &str) -> Option<usize> {
      if !text.starts_with("/*") {
        return None;
      }
      let mut iter = text.char_indices();
      let len = loop {
        let (idx, _) = match iter.next() {
          Some(pair) => pair,
          None => break text.len(),
        };
        if iter.as_str().starts_with("*/") {
          break idx + 3;
        }
      };
      return Some(len);
    }
  }
  fn tok_str(s: &str) -> Vec<String> {
    let mut r = Vec::new();
    let mut s = TokenStream::new(Simple {}, s.to_string());
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
    let mut s = TokenStream::new(Simple {}, s.to_string());
    loop {
      match s.uncons() {
        Ok(x) => r.push(x.kind),
        Err(ref e) if e == &Error::end_of_input() => break,
        Err(e) => panic!("Parse error at {}: {}", s.position(), e),
      }
    }
    return r;
  }
  use self::Kind::*;

  fn assert_tokens(text: &str, types: &[Kind], tokens: &[&str]) {
    assert_eq!(tok_typ(text), types);
    assert_eq!(tok_str(text), tokens);
  }

  #[test]
  fn test_combo() {
    assert_tokens(
      "abc 123 def",
      &[Ident, IntValue, Ident],
      &["abc", "123", "def"],
    );
  }

  #[test]
  fn test_idents() {
    assert_tokens(
      "abc a b zz",
      &[Ident, Ident, Ident, Ident],
      &["abc", "a", "b", "zz"],
    );
  }

  #[test]
  fn test_numbers() {
    assert_tokens(
      "123 321 444",
      &[IntValue, IntValue, IntValue],
      &["123", "321", "444"],
    );
  }

  #[test]
  fn test_symbols() {
    assert_tokens(":= : =", &[Assign, Colon, Equal], &[":=", ":", "="])
  }

  #[test]
  fn test_keywords() {
    assert_tokens(
      "prOgrAM program var PROGRAM",
      &[Program, Program, Var, Program],
      &["prOgrAM", "program", "var", "PROGRAM"],
    )
  }

  #[test]
  fn test_comments() {
    assert_tokens(
      "/**
    comment
    **/a/* other comment*/bc /**//**/ cd",
      &[Ident, Ident, Ident],
      &["a", "bc", "cd"],
    )
  }
}
