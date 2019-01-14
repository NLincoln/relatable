use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crate::SchemaError;
use std::io::{Read, Write};

pub trait Field {
  fn kind(&self) -> &FieldKind;
}

/// A Field represents a column in a table
/// This enum doesn't actually have the data associated
/// with it, instead you combine this with a set of bytes
/// and use that to extract the data
#[derive(Debug, PartialEq, Clone)]
pub struct SchemaField {
  kind: FieldKind,
  name: String,
}

impl Field for SchemaField {
  fn kind(&self) -> &FieldKind {
    &self.kind
  }
}

impl SchemaField {
  /// Creates a new field with the given kind and name
  pub fn new(kind: FieldKind, name: String) -> Result<SchemaField, FieldError> {
    if let FieldKind::Number(n) = kind {
      if n.count_ones() != 1 || n > 8 {
        return Err(FieldError::InvalidNumberType(n));
      }
    }

    Ok(SchemaField { kind, name })
  }

  pub(crate) fn persist(&self, disk: &mut impl Write) -> Result<(), SchemaError> {
    let name_buf = self.name.as_bytes();
    disk.write_u16::<BigEndian>(name_buf.len() as u16)?;

    disk.write_all(name_buf)?;
    self.kind.persist(disk)?;

    Ok(())
  }
  /// Returns (num bytes read, Field)
  pub(crate) fn from_persisted(disk: &mut impl Read) -> Result<SchemaField, SchemaError> {
    let name_len = disk.read_u16::<BigEndian>()?;
    log::debug!("Reading field");
    log::debug!("-> name_len is {}", name_len);
    let mut name = vec![0; name_len as usize];
    disk.read_exact(&mut name)?;
    let name = String::from_utf8(name)?;
    log::debug!("-> Name is {}", name);
    let kind = FieldKind::from_persisted(disk)?;
    log::debug!("-> FieldKind is {:?}", kind);
    Ok(SchemaField { name, kind })
  }

  pub fn kind(&self) -> &FieldKind {
    &self.kind
  }
  pub fn name(&self) -> &str {
    &self.name
  }

  pub fn from_column_def<'a, 'b>(
    column_def: &'b parser::ColumnDef<'a>,
  ) -> Result<Self, FieldError> {
    use parser::Type;
    let name = column_def.column_name.text().to_string();
    let type_name = &column_def.type_name;
    match type_name.name {
      Type::Integer => {
        let size = type_name.argument.unwrap_or(8);
        Ok(SchemaField::new(FieldKind::Number(size as u8), name)?)
      }
      Type::Blob => {
        let size = type_name.argument.unwrap_or(100);
        Ok(SchemaField::new(FieldKind::Blob(size as u64), name)?)
      }
      Type::Varchar => {
        let size = type_name.argument.unwrap_or(128);
        Ok(SchemaField::new(FieldKind::Str(size as u64), name)?)
      }
    }
  }
}

#[derive(Debug, PartialEq)]
pub enum FieldError {
  /// Invalid numeric type, returns the number requested
  InvalidNumberType(u8),
}

/// The kind of a field.
#[derive(Debug, PartialEq, Clone)]
pub enum FieldKind {
  /// An integer with n bytes of storage.
  ///
  /// n must be a power of two, and has a maximum of 8 (64-bit)
  Number(u8),
  /// A blob of bytes, with the specified size.
  /// Note that when you access this field, you will always
  /// get a blob of bytes of exactly the given size. Any needed
  /// bookkeeping to know how many bytes have been written is left
  /// to the user
  Blob(u64),

  /// A string with the specified number of bytes allocated to it.
  ///
  /// Keep in mind that most non-ascii characters will take up 2-4 bytes.
  ///
  /// The main way that a `Str` differs from a `Blob` is that a `Str` takes up
  /// an extra 8 bytes to hold the length of the string.
  Str(u64),
}

impl FieldKind {
  const NUMBER_TAG: u8 = 1;
  const BLOB_TAG: u8 = 2;
  const STR_TAG: u8 = 3;

  pub(crate) fn size(&self) -> usize {
    match self {
      FieldKind::Number(n) => *n as usize,
      FieldKind::Blob(n) => *n as usize,
      FieldKind::Str(n) => *n as usize + 8, // 8 extra for the size
    }
  }

  pub(crate) fn persist(&self, disk: &mut impl Write) -> Result<(), SchemaError> {
    match self {
      FieldKind::Number(n) => {
        disk.write_u8(Self::NUMBER_TAG)?;
        disk.write_u8(*n)?;
      }
      FieldKind::Blob(n) => {
        disk.write_u8(Self::BLOB_TAG)?;
        disk.write_u64::<BigEndian>(*n)?;
      }
      FieldKind::Str(n) => {
        disk.write_u8(Self::STR_TAG)?;
        disk.write_u64::<BigEndian>(*n)?;
      }
    };
    Ok(())
  }

  /// The tuple is (num_bytes_we_read, Field)
  pub(crate) fn from_persisted(disk: &mut impl Read) -> Result<FieldKind, SchemaError> {
    let tag = disk.read_u8()?;
    log::debug!("-> FieldKind Tag is {}", tag);

    match tag {
      Self::NUMBER_TAG => {
        let size = disk.read_u8()?;
        Ok(FieldKind::Number(size))
      }
      Self::BLOB_TAG => {
        let size = disk.read_u64::<BigEndian>()?;
        Ok(FieldKind::Blob(size))
      }
      Self::STR_TAG => {
        let size = disk.read_u64::<BigEndian>()?;
        Ok(FieldKind::Str(size))
      }
      unknown => return Err(SchemaError::UnknownFieldType(unknown)),
    }
  }
}
