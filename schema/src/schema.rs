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
  /// Creates a new field with the given kind and name
  pub fn new(kind: FieldKind, name: String) -> Result<Field, SchemaError> {
    if let FieldKind::Number(n) = kind {
      if n.count_ones() != 1 || n > 8 {
        return Err(SchemaError::InvalidFieldConfig);
      }
    }

    Ok(Field { kind, name })
  }

  fn persist(&self, disk: &mut impl Write) -> Result<usize, SchemaError> {
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
  fn from_persisted(disk: &mut impl Read) -> Result<(usize, Field), SchemaError> {
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

/// The kind of a field.
#[derive(Debug, PartialEq, Clone)]
pub enum FieldKind {
  /// An integer with n bytes of storage.
  ///
  /// n must be a power of two, and has a maximum of 8 (64-bit)
  Number(u8),
  /// A blob of bytes, with the specified size
  Blob(u64),

  /// A string with the specified number of bytes allocated to it.
  ///
  /// Keep in mind that most non-ascii characters will take up 2-4 bytes.
  Str(u64),
}

impl FieldKind {
  const NUMBER_TAG: u8 = 1;
  const BLOB_TAG: u8 = 2;
  const STR_TAG: u8 = 3;

  fn persist(&self, disk: &mut impl Write) -> Result<usize, SchemaError> {
    match self {
      FieldKind::Number(n) => {
        disk.write_all(&[Self::NUMBER_TAG])?;
        disk.write_u8(*n)?;
        Ok(2)
      }
      FieldKind::Blob(n) => {
        disk.write(&[Self::BLOB_TAG])?;
        disk.write_u64::<BigEndian>(*n)?;
        Ok(9)
      }
      FieldKind::Str(n) => {
        disk.write(&[Self::STR_TAG])?;
        disk.write_u64::<BigEndian>(*n)?;
        Ok(9)
      }
    }
  }

  /// The tuple is (num_bytes_we_read, Field)
  fn from_persisted(disk: &mut impl Read) -> Result<(usize, FieldKind), SchemaError> {
    let tag = disk.read_u8()?;

    match tag {
      Self::NUMBER_TAG => {
        let size = disk.read_u8()?;
        Ok((2, FieldKind::Number(size)))
      }
      Self::BLOB_TAG => {
        let size = disk.read_u64::<BigEndian>()?;
        Ok((9, FieldKind::Blob(size)))
      }
      Self::STR_TAG => {
        let size = disk.read_u64::<BigEndian>()?;
        Ok((9, FieldKind::Str(size)))
      }
      unknown => return Err(SchemaError::UnknownFieldType(unknown)),
    }
  }
}

/// The schema for a given table.
#[derive(Debug, PartialEq, Clone)]
pub struct Schema {
  fields: Vec<Field>,
  name: String,
}

/// A generic error for all errors that
/// can happen when trying to read the schema back
/// from the disk
#[derive(Debug)]
pub enum SchemaError {
  /// We encountered a field with a tag we didn't recognize
  UnknownFieldType(u8),
  /// An i/o error occurred
  Io(io::Error),
  /// An error occurred converting to utf8
  Utf8Error(std::string::FromUtf8Error),
  /// A column was created that had an invalid data type
  InvalidFieldConfig,
}

impl From<io::Error> for SchemaError {
  fn from(err: io::Error) -> SchemaError {
    SchemaError::Io(err)
  }
}

impl From<std::string::FromUtf8Error> for SchemaError {
  fn from(err: std::string::FromUtf8Error) -> SchemaError {
    SchemaError::Utf8Error(err)
  }
}

impl Schema {
  /// Creates a new schema from a set of fields
  pub fn from_fields(name: String, fields: Vec<Field>) -> Self {
    Self { fields, name }
  }

  pub fn name(&self) -> &str {
    &self.name
  }

  /// Gets the fields of this schema
  pub fn fields(&self) -> &[Field] {
    &self.fields
  }

  pub fn write_tables(tables: &[Schema], disk: &mut impl Write) -> Result<(), SchemaError> {
    disk.write_u16::<BigEndian>(tables.len() as u16)?;
    for table in tables {
      table.persist(disk)?;
    }
    Ok(())
  }

  pub fn read_tables(disk: &mut impl Read) -> Result<Vec<Self>, SchemaError> {
    let num_tables = disk.read_u16::<BigEndian>()?;
    let mut buf = Vec::with_capacity(num_tables as usize);

    for _ in 0..num_tables {
      buf.push(Schema::from_persisted(disk)?);
    }
    Ok(buf)
  }

  /// Serialize this schema to a series of bytes that could be
  /// written to disk, or communicated over the network, or whatever.
  pub fn persist(&self, disk: &mut impl Write) -> Result<usize, SchemaError> {
    let name = self.name.as_bytes();
    disk.write_u16::<BigEndian>(name.len() as u16)?;
    disk.write_all(name)?;
    disk.write_u16::<BigEndian>(self.fields().len() as u16)?;
    let mut count = 2;

    for field in self.fields() {
      count += field.persist(disk)?;
    }
    Ok(count)
  }

  /// Reads the schema information from the disk
  ///
  /// Note that the schema that is being read here _must_ have
  /// been written by `persist`
  pub fn from_persisted(disk: &mut impl Read) -> Result<Self, SchemaError> {
    let name_len = disk.read_u16::<BigEndian>()?;
    let mut buf = vec![0; name_len as usize];
    disk.read_exact(&mut buf)?;
    let name = String::from_utf8(buf)?;
    let mut fields = vec![];
    let num_fields = disk.read_u16::<BigEndian>()?;

    for _ in 0..num_fields {
      let (_, field) = Field::from_persisted(disk)?;
      fields.push(field);
    }
    Ok(Self { fields, name })
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
    assert_eq!(persist_kind(FieldKind::Number(2)), vec![1u8, 2]);
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
      FieldKind::from_persisted(&mut Cursor::new(&[1, 4])).unwrap(),
      (2, FieldKind::Number(4))
    );
    assert_eq!(
      FieldKind::from_persisted(&mut Cursor::new(&[2, 0, 0, 0, 0, 0, 0, 0, 5])).unwrap(),
      (9, FieldKind::Blob(5))
    );
  }

  #[test]
  fn number_type_constraints() {
    let field = Field::new(FieldKind::Number(7), "id".into()).unwrap_err();
    match field {
      SchemaError::InvalidFieldConfig => {}
      _ => panic!("Failing Test"),
    };
    let field = Field::new(FieldKind::Number(16), "id".into()).unwrap_err();
    match field {
      SchemaError::InvalidFieldConfig => {}
      _ => panic!("Failing Test"),
    };
  }

  #[test]
  fn persist_schema_with_number() {
    let schema = Schema {
      name: "foo".into(),
      fields: vec![Field::new(FieldKind::Number(8), "id".into()).unwrap()],
    };
    let mut disk = io::Cursor::new(vec![]);
    schema.persist(&mut disk).unwrap();
    disk.set_position(0);
    let revived_schema = Schema::from_persisted(&mut disk).unwrap();
    assert_eq!(schema, revived_schema);
  }
}
