use crate::{FieldKind, Schema};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::fmt::{self, Display};
use std::io::{self, Read, Seek, Write};
use std::str::Utf8Error;
pub struct RowIterator<'a, 'b, D: Read> {
  disk: &'a mut D,
  schema: &'b Schema,
}

impl<'a, 'b, D: Read> Iterator for RowIterator<'a, 'b, D> {
  type Item = Result<Row, RowCellError>;
  fn next(&mut self) -> Option<Self::Item> {
    /*
     * How in the world do we know we're done reading?
     * I think atm I'm inclined to make a magic byte at the beginning
     * of the row saying whether or not it's the "last" row. Meaning
     * I need a RowMeta.
     *
     * Unfortunately due to the stateless nature Iterator
     * I have to read the _previous_ row in to know if it's the last.
     * Ugh I think I'll just change it so there's a sentinal row at the end
     * instead. That way there's always at least one row in the table.
     *
     * This is ugly but it's more of a hack until I can get btree tables working.
     * The hacks are also contained to this file (including stuff like RowMeta)
     */
    let row = match Row::from_schema(self.disk, self.schema) {
      Ok(row) => row,
      err @ Err(_) => return Some(err),
    };
    if row.meta.is_last_row {
      log::debug!("Encountered the last row");
      None
    } else {
      log::debug!("Encountered existing row");
      Some(Ok(row))
    }
  }
}

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
    let is_last_row = match disk.read_u8()? {
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
  pub fn row_iterator<'a, 'b, D: Read + Seek>(
    disk: &'a mut D,
    schema: &'b Schema,
  ) -> io::Result<RowIterator<'a, 'b, D>> {
    disk.seek(io::SeekFrom::Start(0))?;
    Ok(RowIterator { disk, schema })
  }

  pub fn from_schema(disk: &mut impl Read, schema: &Schema) -> Result<Self, RowCellError> {
    let meta = RowMeta::from_persisted(disk)?;

    let num_bytes = schema.sizeof_row();
    let mut data = vec![0; num_bytes];
    disk.read_exact(&mut data)?;
    Ok(Self { data, meta })
  }

  fn from_cells(schema: &Schema, cells: Vec<OwnedRowCell>, meta: RowMeta) -> io::Result<Row> {
    let mut data = io::Cursor::new(vec![]);

    for (i, cell) in cells.iter().enumerate() {
      cell.persist(schema, i, &mut data)?;
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

  pub fn as_cells<'a>(&'a self, schema: &Schema) -> Result<Vec<RowCell<'a>>, RowCellError> {
    let mut buf = Vec::with_capacity(schema.fields().len());
    for field_index in 0..schema.fields().len() {
      buf.push(RowCell::new(&self.data, schema, field_index)?);
    }
    Ok(buf)
  }
  pub fn into_cells(self, schema: &Schema) -> Result<Vec<OwnedRowCell>, RowCellError> {
    let mut buf = Vec::with_capacity(schema.fields().len());
    for field_index in 0..schema.fields().len() {
      buf.push(OwnedRowCell::from(RowCell::new(
        &self.data,
        schema,
        field_index,
      )?))
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
      let row = Row::from_cells(schema, row, RowMeta { is_last_row: false })?;
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
      RowCell::Blob(data) => OwnedRowCell::Blob(data.into()),
      RowCell::Number { value, size } => OwnedRowCell::Number {
        value: value,
        size: size,
      },
      RowCell::Str { value, max_size } => OwnedRowCell::Str {
        value: value.into(),
        max_size: max_size,
      },
    }
  }
}

impl OwnedRowCell {
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
  pub fn persist(
    &self,
    schema: &Schema,
    field_index: usize,
    disk: &mut impl Write,
  ) -> io::Result<()> {
    match self {
      OwnedRowCell::Number { value, size } => {
        disk.write_int::<BigEndian>(*value, *size as usize)?
      }
      OwnedRowCell::Blob(data) => disk.write_all(data)?,
      OwnedRowCell::Str { value, max_size } => {
        disk.write_u64::<BigEndian>(value.len() as u64)?;
        disk.write_all(value.as_bytes())?;
        disk.write_all(&vec![0; *max_size as usize - value.len()])?;
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
  pub fn new(data: &'a [u8], schema: &Schema, field_index: usize) -> Result<Self, RowCellError> {
    use byteorder::{BigEndian, ReadBytesExt};

    let offset = schema.offset_of(field_index);
    let slice = &data[offset..];
    let field = &schema.fields()[field_index];
    match field.kind() {
      FieldKind::Number(n) => {
        let n = *n;
        let mut cursor = io::Cursor::new(data);
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
