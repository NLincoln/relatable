use crate::{FieldKind, Schema};
use std::io;
use std::str::Utf8Error;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum RowCell<'a> {
  Number(i64),
  Str(&'a str),
  Blob(&'a [u8]),
}

#[derive(Debug)]
pub enum RowCellError {
  Utf8Error(Utf8Error),
  Io(io::Error),
}

impl From<Utf8Error> for RowCellError {
  fn from(err: Utf8Error) -> RowCellError {
    RowCellError::Utf8Error(err)
  }
}

impl From<io::Error> for RowCellError {
  fn from(err: io::Error) -> RowCellError {
    RowCellError::Io(err)
  }
}

impl<'a> RowCell<'a> {
  pub fn new(data: &'a [u8], schema: &Schema, field_index: usize) -> Result<Self, RowCellError> {
    use byteorder::{BigEndian, ReadBytesExt};

    let offset = schema.offset_of(field_index);
    let slice = &data[offset..];
    let field = &schema.fields()[field_index];
    match field.kind() {
      FieldKind::Number(n) => {
        let mut cursor = io::Cursor::new(data);
        Ok(RowCell::Number(
          cursor.read_int::<BigEndian>(*n as usize).unwrap(),
        ))
      }
      FieldKind::Blob(n) => Ok(RowCell::Blob(&slice[0..*n as usize])),
      FieldKind::Str(n) => {
        use std::str;
        let mut cursor = io::Cursor::new(slice);
        let len = cursor.read_u64::<BigEndian>()?;
        // Remove the length of the string...
        let slice = &slice[8..];
        // More or less assert that we won't read past the end of this string.
        // could probably remove this in a debug build
        let slice = &slice[0..*n as usize];
        let slice = &slice[0..len as usize];
        Ok(RowCell::Str(&str::from_utf8(slice)?))
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::Field;
  use byteorder::{BigEndian, WriteBytesExt};
  #[test]
  fn parsing_a_row() -> Result<(), RowCellError> {
    let schema = Schema::from_fields(
      "table".into(),
      vec![
        Field::new(FieldKind::Number(2), "id".into()).unwrap(),
        Field::new(FieldKind::Str(10), "username".into()).unwrap(),
      ],
    );
    let mut buf = std::io::Cursor::new(vec![]);
    buf.write_u16::<BigEndian>(1).unwrap(); // id
    buf.write_u64::<BigEndian>("nlincoln".len() as u64).unwrap();
    std::io::Write::write_all(&mut buf, "nlincoln\0\0".as_bytes()).unwrap();

    let buf = buf.into_inner();

    let id_rowcell = RowCell::new(&buf, &schema, 0).unwrap();
    assert_eq!(id_rowcell, RowCell::Number(1));

    let username_rowcell = RowCell::new(&buf, &schema, 1).unwrap();
    assert_eq!(username_rowcell, RowCell::Str("nlincoln"));

    Ok(())
  }
}
