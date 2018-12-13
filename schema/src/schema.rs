use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{self, Read, Write};

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

  fn persist(&self, disk: &mut impl Write) -> io::Result<usize> {
    /*
     * Format is:
     * name_len(u16) name kind
     */
    let name_buf = self.name.as_bytes();
    // writing into a vec never fails
    disk.write_u16::<BigEndian>(name_buf.len() as u16)?;

    disk.write_all(name_buf)?;
    let kind_len = self.kind.persist(disk)?;

    Ok(2 + name_buf.len() + kind_len)
  }
  /// Returns (num bytes read, Field)
  fn from_persisted(disk: &mut impl Read) -> Result<(usize, Field), SchemaFromBytesError> {
    let name_len = disk.read_u16::<BigEndian>()?;

    let mut name = vec![0; name_len as usize];
    disk.read_exact(&mut name)?;
    let (kind_read, kind) = FieldKind::from_persisted(disk)?;
    Ok((
      2 + name_len as usize + kind_read,
      Field {
        name: String::from_utf8(name)?,
        kind,
      },
    ))
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

  fn persist(&self, disk: &mut impl Write) -> io::Result<usize> {
    match self {
      FieldKind::Number => {
        disk.write_all(&[Self::NUMBER_TAG])?;
        Ok(1)
      }
      FieldKind::Blob(n) => {
        disk.write(&[Self::BLOB_TAG])?;
        disk.write_u64::<BigEndian>(*n)?;
        Ok(9)
      }
    }
  }
  /// The tuple is (num_bytes_we_read, Field)
  fn from_persisted(disk: &mut impl Read) -> Result<(usize, FieldKind), SchemaFromBytesError> {
    let tag = disk.read_u8()?;

    match tag {
      Self::NUMBER_TAG => Ok((1, FieldKind::Number)),
      Self::BLOB_TAG => {
        let size = disk.read_u64::<BigEndian>()?;
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

#[derive(Debug)]
pub enum SchemaFromBytesError {
  /// We encountered a field with a tag we didn't recognize
  UnknownFieldType(u8),
  /// An i/o error occurred
  Io(io::Error),
  /// An error occurred converting to utf8
  FromUtf8Error(std::string::FromUtf8Error),
}

impl From<io::Error> for SchemaFromBytesError {
  fn from(err: io::Error) -> SchemaFromBytesError {
    SchemaFromBytesError::Io(err)
  }
}

impl From<std::string::FromUtf8Error> for SchemaFromBytesError {
  fn from(err: std::string::FromUtf8Error) -> SchemaFromBytesError {
    SchemaFromBytesError::FromUtf8Error(err)
  }
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
  pub fn persist(&self, disk: &mut impl Write) -> io::Result<usize> {
    disk.write_u16::<BigEndian>(self.fields().len() as u16)?;
    let mut count = 2;

    for field in self.fields() {
      count += field.persist(disk)?;
    }
    Ok(count)
  }

  pub fn from_persisted(disk: &mut impl Read) -> Result<Self, SchemaFromBytesError> {
    let mut fields = vec![];
    let num_fields = disk.read_u16::<BigEndian>()?;

    for _ in 0..num_fields {
      let (_, field) = Field::from_persisted(disk)?;
      fields.push(field);
    }
    Ok(Self { fields })
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  fn persist_kind(kind: FieldKind) -> Vec<u8> {
    let mut buf = io::Cursor::new(vec![]);
    kind.persist(&mut buf).unwrap();
    buf.into_inner()
  }
  #[test]
  fn persist_field_number() {
    assert_eq!(persist_kind(FieldKind::Number), vec![1 as u8]);
  }

  #[test]
  fn persist_field_block() {
    assert_eq!(
      persist_kind(FieldKind::Blob(5u64)),
      vec![2, 0, 0, 0, 0, 0, 0, 0, 5]
    );
  }

  #[test]
  fn from_persisted_field() {
    use std::io::Cursor;
    assert_eq!(
      FieldKind::from_persisted(&mut Cursor::new(&[1])).unwrap(),
      (1, FieldKind::Number)
    );
    assert_eq!(
      FieldKind::from_persisted(&mut Cursor::new(&[2, 0, 0, 0, 0, 0, 0, 0, 5])).unwrap(),
      (9, FieldKind::Blob(5))
    );
  }

  #[test]
  fn persist_schema_with_number() {
    let schema = Schema {
      fields: vec![Field::new(FieldKind::Number, "id".into())],
    };
    let mut disk = io::Cursor::new(vec![]);
    schema.persist(&mut disk).unwrap();
    disk.set_position(0);
    let revived_schema = Schema::from_persisted(&mut disk).unwrap();
    assert_eq!(schema, revived_schema);
  }
}
