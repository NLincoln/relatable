use crate::SchemaError;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

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
  pub fn new(kind: FieldKind, name: String) -> Result<Field, FieldError> {
    if let FieldKind::Number(n) = kind {
      if n.count_ones() != 1 || n > 8 {
        return Err(FieldError::InvalidNumberType(n));
      }
    }

    Ok(Field { kind, name })
  }

  pub(crate) fn persist(&self, disk: &mut impl Write) -> Result<(), SchemaError> {
    let name_buf = self.name.as_bytes();
    disk.write_u16::<BigEndian>(name_buf.len() as u16)?;

    disk.write_all(name_buf)?;
    self.kind.persist(disk)?;

    Ok(())
  }
  /// Returns (num bytes read, Field)
  pub(crate) fn from_persisted(disk: &mut impl Read) -> Result<Field, SchemaError> {
    let name_len = disk.read_u16::<BigEndian>()?;
    log::debug!("Reading field");
    log::debug!("-> name_len is {}", name_len);
    let mut name = vec![0; name_len as usize];
    disk.read_exact(&mut name)?;
    let name = String::from_utf8(name)?;
    log::debug!("-> Name is {}", name);
    let kind = FieldKind::from_persisted(disk)?;
    log::debug!("-> FieldKind is {:?}", kind);
    Ok(Field { name, kind })
  }

  pub fn kind(&self) -> &FieldKind {
    &self.kind
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
