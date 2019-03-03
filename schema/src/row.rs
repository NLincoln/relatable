use crate::field::Field;
use crate::{FieldKind, Schema};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{self, Read, Seek, Write};
use std::str::Utf8Error;

#[derive(Debug, Clone)]
struct RowMeta {
  is_last_row: bool,
}

impl RowMeta {
  fn size() -> usize {
    2 // 2 bytes for is_last_row (alignment)
  }
  fn persist(&self, disk: &mut impl Write) -> io::Result<()> {
    let is_last_row = if self.is_last_row { 1 } else { 0 };
    log::debug!("Persisting RowMeta: is_last_row: {}", is_last_row);
    disk.write_u16::<BigEndian>(is_last_row)?;
    Ok(())
  }
  fn from_persisted(disk: &mut impl Read) -> Result<Self, RowCellError> {
    let is_last_row = match disk.read_u16::<BigEndian>()? {
      0 => false,
      1 => true,
      _ => return Err(RowCellError::InvalidRowMeta),
    };
    Ok(Self { is_last_row })
  }
}

#[derive(Debug, Clone)]
pub struct Row {
  data: Vec<u8>,
  meta: RowMeta,
}

impl Row {
  pub fn sizeof_row_on_disk(schema: &Schema) -> usize {
    schema.sizeof_row() + RowMeta::size()
  }

  pub fn is_last_row(&self) -> bool {
    self.meta.is_last_row
  }
  pub fn data(&self) -> &[u8] {
    &self.data
  }
  pub fn into_data(self) -> Vec<u8> {
    self.data
  }

  pub fn from_data(data: Vec<u8>) -> Self {
    Row {
      data,
      meta: RowMeta { is_last_row: false },
    }
  }

  pub fn from_schema(disk: &mut impl Read, schema: &Schema) -> Result<Self, RowCellError> {
    let meta = RowMeta::from_persisted(disk)?;

    let num_bytes = schema.sizeof_row();
    let mut data = vec![0; num_bytes];
    disk.read_exact(&mut data)?;
    Ok(Self { data, meta })
  }

  pub fn from_cells(cells: Vec<OwnedRowCell>) -> io::Result<Row> {
    Row::from_cells_impl(cells, RowMeta { is_last_row: false })
  }

  fn from_cells_impl(cells: Vec<OwnedRowCell>, meta: RowMeta) -> io::Result<Row> {
    let mut data = io::Cursor::new(vec![]);

    for cell in cells.iter() {
      cell.persist(&mut data)?;
    }

    let data = data.into_inner();
    Ok(Row { data, meta })
  }
  fn insert_sentinal_row(schema: &Schema, disk: &mut impl Write) -> Result<(), RowCellError> {
    let meta = RowMeta { is_last_row: true };
    meta.persist(disk)?;
    // pre-allocate space for the next row
    disk.write_all(&vec![0; schema.sizeof_row()])?;
    Ok(())
  }

  pub fn as_cells<'a>(&'a self, fields: &[impl Field]) -> Result<Vec<RowCell<'a>>, RowCellError> {
    let mut buf = Vec::with_capacity(fields.len());
    let mut offset = 0;
    for field in fields.iter() {
      buf.push(RowCell::new(&self.data, field, offset)?);
      offset += field.kind().size();
    }
    Ok(buf)
  }
  pub fn into_cells(self, fields: &[impl Field]) -> Result<Vec<OwnedRowCell>, RowCellError> {
    let mut buf = Vec::with_capacity(fields.len());
    let mut offset = 0;
    for field in fields.iter() {
      buf.push(OwnedRowCell::from(RowCell::new(&self.data, field, offset)?));
      offset += field.kind().size();
    }
    Ok(buf)
  }

  fn persist(&self, disk: &mut impl Write) -> Result<(), RowCellError> {
    self.meta.persist(disk)?;
    disk.write_all(&self.data)?;
    Ok(())
  }

  /// Unsafe because this may only be called ONCE per table, at the very beginning when it's created
  pub unsafe fn init_table(schema: &Schema, disk: &mut impl Write) -> Result<(), RowCellError> {
    log::debug!(
      "Writing initial sentinal row (Size-Of-Row {})",
      schema.sizeof_row() + RowMeta::size()
    );
    Row::insert_sentinal_row(schema, disk)?;
    Ok(())
  }

  /// Unsafe because you must have called `init_table` before calling this function
  /// Once `Table` is a concept this will go away, but for now the primary abstraction
  /// is rows and we need this
  pub unsafe fn insert_row(
    row: Vec<OwnedRowCell>,
    disk: &mut (impl Write + Seek + Read),
    schema: &Schema,
  ) -> Result<(), RowCellError> {
    // Need to do two steps:
    // 1. Un-mark the previous row as the last row
    // 2. Write the current row into the old space left by the previous sentinal
    // 3. Write a new sentinal row
    log::debug!("insert_row");
    let size_of_row = schema.sizeof_row() + RowMeta::size();
    log::debug!("-> size_of_row {}", size_of_row);

    disk.seek(io::SeekFrom::End(-(size_of_row as i64)))?;
    {
      let row = Row::from_cells_impl(row, RowMeta { is_last_row: false })?;
      log::debug!("-> Writing new row over the old sentinal");
      row.persist(disk)?;
    }

    // write a new sentinal row
    log::debug!("-> Writing new sentinal");
    Row::insert_sentinal_row(schema, disk)?;
    Ok(())
  }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum OwnedRowCell {
  Number { value: i64, size: u8 },
  Str { value: String, max_size: u64 },
  Blob(Vec<u8>),
}

impl<'a> From<RowCell<'a>> for OwnedRowCell {
  fn from(cell: RowCell<'a>) -> OwnedRowCell {
    match cell {
      RowCell::Blob(data) => OwnedRowCell::Blob(data.to_vec()),
      RowCell::Number { value, size } => OwnedRowCell::Number {
        value: value,
        size: size,
      },
      RowCell::Str { value, max_size } => OwnedRowCell::Str {
        value: value.to_string(),
        max_size: max_size,
      },
    }
  }
}

impl OwnedRowCell {
  pub fn coerce_to(mut self, field: &impl Field) -> Option<OwnedRowCell> {
    use std::cmp::{Ord, Ordering};
    match &mut self {
      OwnedRowCell::Blob(data) => {
        let needed_len = match field.kind() {
          FieldKind::Blob(len) => len,
          _ => return None,
        };

        let data_len = data.len() as u64;
        match data_len.cmp(needed_len) {
          Ordering::Equal => Some(self),
          Ordering::Greater => {
            data.truncate(*needed_len as usize);
            Some(self)
          }
          Ordering::Less => {
            let padding_bytes = *needed_len - data_len;
            let mut padding = vec![0; padding_bytes as usize];
            data.append(&mut padding);
            Some(self)
          }
        }
      }
      OwnedRowCell::Number { size, .. } => match field.kind() {
        // it's all i64's under the hood...
        FieldKind::Number(schema_size) => {
          *size = *schema_size;
          Some(self)
        }
        _ => None,
      },
      OwnedRowCell::Str { max_size, .. } => {
        // padding / truncating happens when we write the string
        // which is convenient, because we only need to make the
        // size args match
        let max_len = match field.kind() {
          FieldKind::Str(len) => len,
          _ => return None,
        };
        *max_size = *max_len;
        Some(self)
      }
    }
  }

  pub fn as_rowcell<'a>(&'a self) -> RowCell<'a> {
    match self {
      OwnedRowCell::Number { value, size } => RowCell::Number {
        value: *value,
        size: *size,
      },
      OwnedRowCell::Str { value, max_size } => RowCell::Str {
        value: value.as_ref(),
        max_size: *max_size,
      },
      OwnedRowCell::Blob(data) => RowCell::Blob(data.as_ref()),
    }
  }
  pub fn persist(&self, disk: &mut impl Write) -> io::Result<()> {
    match self {
      OwnedRowCell::Number { value, size } => {
        disk.write_int::<BigEndian>(*value, *size as usize)?
      }
      OwnedRowCell::Blob(data) => disk.write_all(data)?,
      OwnedRowCell::Str { value, max_size } => {
        disk.write_u64::<BigEndian>(value.len() as u64)?;
        disk.write_all(value.as_bytes())?;

        let remaining_buf_size = *max_size as usize - value.len();
        disk.write_all(&vec![0; remaining_buf_size])?;

        assert_eq!(
          *max_size as usize,
          value.as_bytes().len() + remaining_buf_size
        );
      }
    };
    Ok(())
  }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum RowCell<'a> {
  Number { value: i64, size: u8 },
  Str { value: &'a str, max_size: u64 },
  Blob(&'a [u8]),
}

#[derive(Debug)]
pub enum RowCellError {
  Utf8Error(Utf8Error),
  Io(io::Error),
  InvalidRowMeta,
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
  pub fn new(data: &'a [u8], field: &impl Field, offset: usize) -> Result<Self, RowCellError> {
    use byteorder::{BigEndian, ReadBytesExt};

    let slice = &data[offset..];
    match field.kind() {
      FieldKind::Number(n) => {
        let n = *n;
        let mut cursor = io::Cursor::new(slice);
        Ok(RowCell::Number {
          value: cursor.read_int::<BigEndian>(n as usize).unwrap(),
          size: n,
        })
      }
      FieldKind::Blob(n) => Ok(RowCell::Blob(&slice[0..*n as usize])),
      FieldKind::Str(n) => {
        let n = *n;
        use std::str;
        let mut cursor = io::Cursor::new(slice);
        let len = cursor.read_u64::<BigEndian>()?;
        // Remove the length of the string...
        let slice = &slice[8..];
        // More or less assert that we won't read past the end of this string.
        // could probably remove this in a debug build
        let slice = &slice[0..n as usize];
        let slice = &slice[0..len as usize];
        Ok(RowCell::Str {
          value: &str::from_utf8(slice)?,
          max_size: n,
        })
      }
    }
  }
}

use std::fmt::{self, Display};

impl<'a> Display for RowCell<'a> {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self {
      RowCell::Blob(data) => write!(f, "{}", hex::encode(data)),
      RowCell::Str { value, .. } => write!(f, "{}", value),
      RowCell::Number { value, .. } => write!(f, "{}", value),
    }
  }
}
