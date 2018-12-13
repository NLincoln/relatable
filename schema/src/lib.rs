use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

/// A Field represents a column in the database
/// This enum doesn't actually have the data associated
/// with it, instead you combine this with a set of bytes
/// and use that to extract the data
#[derive(Debug, PartialEq, Clone)]
pub enum Field {
  /// An integer
  Number,
  /// A blob of bytes, with the specified size
  Blob(u64),
}

impl Field {
  const NUMBER_TAG: u8 = 1;
  const BLOB_TAG: u8 = 2;

  fn persist(&self) -> Vec<u8> {
    match self {
      Field::Number => vec![Self::NUMBER_TAG],
      Field::Blob(n) => {
        let mut buf = vec![Self::BLOB_TAG];
        // Can unwrap because writing to a vec won't fail
        buf.write_u64::<BigEndian>(*n).unwrap();
        buf
      }
    }
  }
  /// The tuple is (num_bytes_we_read, Field)
  fn from_persisted(buf: &[u8]) -> Result<(usize, Field), TableFromBytesError> {
    match buf[0] {
      Self::NUMBER_TAG => Ok((1, Field::Number)),
      Self::BLOB_TAG => {
        if buf.len() < 9 {
          return Err(TableFromBytesError::NotEnoughBytes);
        }
        use std::io::Cursor;

        let mut buf = Cursor::new(&buf[1..9]);
        let size = buf.read_u64::<BigEndian>().unwrap();
        Ok((9, Field::Blob(size)))
      }
      unknown => return Err(TableFromBytesError::UnknownFieldType(unknown)),
    }
  }
}

/// A table schema.
#[derive(Debug, PartialEq, Clone)]
pub struct Table {
  fields: Vec<Field>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum TableFromBytesError {
  /// We encountered a field with a tag we didn't recognize
  UnknownFieldType(u8),
  /// Not enough information was provided
  NotEnoughBytes,
}

impl Table {
  pub fn fields(&self) -> &[Field] {
    &self.fields
  }
  /// Serialize this schema to a series of bytes that could be
  /// written to disk, or communicated over the network, or whatever.
  pub fn persist(&self) -> Vec<u8> {
    let mut buf = vec![];
    for field in self.fields() {
      buf.append(&mut field.persist());
    }
    buf
  }

  pub fn from_persisted(bytes: &[u8]) -> Result<Self, TableFromBytesError> {
    let mut fields = vec![];
    let mut curr_pos = 0;
    while curr_pos < bytes.len() {
      let (offset, field) = Field::from_persisted(&bytes[curr_pos..])?;
      fields.push(field);
      curr_pos += offset;
    }
    Ok(Self { fields })
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  #[test]
  fn persist_field_number() {
    assert_eq!(Field::Number.persist(), vec![1 as u8]);
  }

  #[test]
  fn persist_field_block() {
    assert_eq!(Field::Blob(5u64).persist(), vec![2, 0, 0, 0, 0, 0, 0, 0, 5]);
  }

  #[test]
  fn from_persisted_field() {
    assert_eq!(Field::from_persisted(&[1]).unwrap(), (1, Field::Number));
    assert_eq!(
      Field::from_persisted(&[2, 0, 0, 0, 0, 0, 0, 0, 5]).unwrap(),
      (9, Field::Blob(5))
    );
  }

  #[test]
  fn persist_schema_with_number() {
    let table = Table {
      fields: vec![Field::Number],
    };
    let persisted = table.persist();
    let revived_table = Table::from_persisted(&persisted);
    assert_eq!(Ok(table), revived_table);
  }
}
