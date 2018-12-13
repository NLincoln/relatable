use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

/// A Field represents a column in the database
/// This enum doesn't actually have the data associated
/// with it, instead you combine this with a set of bytes
/// and use that to extract the data
#[derive(Debug, PartialEq, Clone)]
pub struct Field {
  kind: FieldKind,
  name: String,
}

impl Field {
  pub fn new(kind: FieldKind, name: String) -> Field {
    Field { kind, name }
  }

  fn persist(&self) -> Vec<u8> {
    /*
     * Format is:
     * name_len(u16) name kind
     */
    let mut name_buf = self.name.as_bytes().to_vec();
    let mut kind_buf = self.kind.persist();
    let mut buf = vec![];
    // writing into a vec never fails
    buf.write_u16::<BigEndian>(name_buf.len() as u16).unwrap();
    buf.append(&mut name_buf);
    buf.append(&mut kind_buf);
    buf
  }
  fn from_persisted(buf: &[u8]) -> Result<(usize, Field), SchemaFromBytesError> {
    let mut buf = std::io::Cursor::new(buf);
    let name_len = buf.read_u16::<BigEndian>();

    unimplemented!()
  }
}

#[derive(Debug, PartialEq, Clone)]
pub enum FieldKind {
  /// An integer
  Number,
  /// A blob of bytes, with the specified size
  Blob(u64),
}

impl FieldKind {
  const NUMBER_TAG: u8 = 1;
  const BLOB_TAG: u8 = 2;

  fn persist(&self) -> Vec<u8> {
    match self {
      FieldKind::Number => vec![Self::NUMBER_TAG],
      FieldKind::Blob(n) => {
        let mut buf = vec![Self::BLOB_TAG];
        // Can unwrap because writing to a vec won't fail
        buf.write_u64::<BigEndian>(*n).unwrap();
        buf
      }
    }
  }
  /// The tuple is (num_bytes_we_read, Field)
  fn from_persisted(buf: &[u8]) -> Result<(usize, FieldKind), SchemaFromBytesError> {
    match buf[0] {
      Self::NUMBER_TAG => Ok((1, FieldKind::Number)),
      Self::BLOB_TAG => {
        if buf.len() < 9 {
          return Err(SchemaFromBytesError::NotEnoughBytes);
        }
        use std::io::Cursor;

        let mut buf = Cursor::new(&buf[1..9]);
        let size = buf.read_u64::<BigEndian>().unwrap();
        Ok((9, FieldKind::Blob(size)))
      }
      unknown => return Err(SchemaFromBytesError::UnknownFieldType(unknown)),
    }
  }
}

/// A Schema schema.
#[derive(Debug, PartialEq, Clone)]
pub struct Schema {
  fields: Vec<Field>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum SchemaFromBytesError {
  /// We encountered a field with a tag we didn't recognize
  UnknownFieldType(u8),
  /// Not enough information was provided
  NotEnoughBytes,
}

impl Schema {
  pub fn from_fields(fields: Vec<Field>) -> Self {
    Self { fields }
  }
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

  pub fn from_persisted(bytes: &[u8]) -> Result<Self, SchemaFromBytesError> {
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
    assert_eq!(FieldKind::Number.persist(), vec![1 as u8]);
  }

  #[test]
  fn persist_field_block() {
    assert_eq!(
      FieldKind::Blob(5u64).persist(),
      vec![2, 0, 0, 0, 0, 0, 0, 0, 5]
    );
  }

  #[test]
  fn from_persisted_field() {
    assert_eq!(
      FieldKind::from_persisted(&[1]).unwrap(),
      (1, FieldKind::Number)
    );
    assert_eq!(
      FieldKind::from_persisted(&[2, 0, 0, 0, 0, 0, 0, 0, 5]).unwrap(),
      (9, FieldKind::Blob(5))
    );
  }

  #[test]
  fn persist_schema_with_number() {
    let schema = Schema {
      fields: vec![Field::new(FieldKind::Number, "id".into())],
    };
    let persisted = schema.persist();
    let revived_schema = Schema::from_persisted(&persisted);
    assert_eq!(Ok(schema), revived_schema);
  }
}
