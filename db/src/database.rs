use crate::{Block, BlockDisk};
use log::debug;
use schema::{OnDiskSchema, Schema};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{self, Read, Seek, Write};

/// Convenience trait for read + write + seek
pub trait Disk: io::Read + io::Write + io::Seek {}
impl<T: io::Read + io::Write + io::Seek> Disk for T {}

#[derive(Debug)]
pub struct Database<T: Disk> {
  disk: T,
  meta: DatabaseMeta,
}

/// Basically a structure that holds all the information in the root block
#[derive(Debug)]
struct DatabaseMeta {
  /// The version of this database. Should be 1
  version: u8,

  /// The _exponent_ for the block size. So an 8 here would mean
  /// our block size is `2 ^ 8` = 256 bytes
  /// Default is 11 (2048 bytes)
  block_size_exp: u8,
  /// The number of blocks we have allocated
  num_allocated_blocks: u64,
  /// The offset of the schema block. Usually this will be
  /// the first block after the root block but it's configurable
  schema_block_offset: u64,
}

impl DatabaseMeta {
  fn new(version: u8, block_size_exp: u8) -> DatabaseMeta {
    // Weird dance here. We initialize the schema_block_offset to block_size,
    // despite the fact that we usually haven't made it yet. Bit spooky, bit dangerous
    // TODO :: Can we clean that up?
    DatabaseMeta {
      version,
      block_size_exp,
      num_allocated_blocks: 2, // 1 for the root block, 1 for the schema block
      schema_block_offset: 2u64.pow(block_size_exp as u32),
    }
  }

  fn block_size(&self) -> u64 {
    2u64.pow(self.block_size_exp as u32)
  }

  fn persist<D: Write + Seek>(&self, disk: &mut D) -> io::Result<()> {
    disk.seek(io::SeekFrom::Start(0))?;
    disk.write_u8(self.version)?;
    disk.write_u8(self.block_size_exp)?;
    disk.write_u64::<BigEndian>(self.num_allocated_blocks)?;
    disk.write_u64::<BigEndian>(self.schema_block_offset)?;
    Ok(())
  }

  fn from_disk<D: Read + Seek>(disk: &mut D) -> io::Result<DatabaseMeta> {
    disk.seek(io::SeekFrom::Start(0))?;
    let version = disk.read_u8()?;
    let block_size_exp = disk.read_u8()?;
    let num_allocated_blocks = disk.read_u64::<BigEndian>()?;
    let schema_block_offset = disk.read_u64::<BigEndian>()?;
    Ok(DatabaseMeta {
      version,
      block_size_exp,
      num_allocated_blocks,
      schema_block_offset,
    })
  }
}

#[derive(Debug)]
pub enum DatabaseError {
  TableNotFound { table_name: String },
  RowCellError(schema::RowCellError),
  Schema(schema::SchemaError),
  Io(io::Error),
  FieldError(schema::FieldError),
}

impl From<io::Error> for DatabaseError {
  fn from(err: io::Error) -> Self {
    DatabaseError::Io(err)
  }
}

impl From<schema::SchemaError> for DatabaseError {
  fn from(err: schema::SchemaError) -> Self {
    DatabaseError::Schema(err)
  }
}

impl From<schema::RowCellError> for DatabaseError {
  fn from(err: schema::RowCellError) -> Self {
    DatabaseError::RowCellError(err)
  }
}

impl From<schema::FieldError> for DatabaseError {
  fn from(err: schema::FieldError) -> Self {
    DatabaseError::FieldError(err)
  }
}

impl<T: Disk> Database<T> {
  pub fn get_table(&mut self, table_name: &str) -> Result<OnDiskSchema, DatabaseError> {
    self
      .schema()?
      .into_iter()
      .find(|owned_table| owned_table.schema().name() == table_name)
      .ok_or_else(|| DatabaseError::TableNotFound {
        table_name: table_name.into(),
      })
  }
  pub fn add_row(
    &mut self,
    table: &str,
    row: Vec<schema::OwnedRowCell>,
  ) -> Result<(), DatabaseError> {
    debug!("Adding row to table");
    let schema = self.get_table(table)?;

    let mut data_blockdisk = BlockDisk::new(self, schema.data_block_offset())?;
    unsafe { schema::Row::insert_row(row, &mut data_blockdisk, schema.schema())? };

    Ok(())
  }

  pub fn read_table(&mut self, table_name: &str) -> Result<Vec<schema::Row>, DatabaseError> {
    let table = self.get_table(table_name)?;
    let mut blockdisk = BlockDisk::new(self, table.data_block_offset())?;
    let mut rows = vec![];
    for row in schema::Row::row_iterator(&mut blockdisk, table.schema())? {
      log::debug!("Row was read");
      let row = row?;
      rows.push(row);
    }
    Ok(rows)
  }

  pub fn create_table(&mut self, schema: Schema) -> Result<(), DatabaseError> {
    // Alright so the first thing we need to do is go find the
    // schema table and add this entry to it.
    debug!("Creating Table");
    debug!(
      "=> We currently have {} blocks allocated",
      self.meta.num_allocated_blocks
    );
    let schema_block_offset = self.meta.schema_block_offset;

    self.disk.seek(io::SeekFrom::Start(schema_block_offset))?;
    let data_block = self.allocate_block()?;
    let data_block_offset = data_block.meta().offset();
    {
      let mut data_blockdisk = BlockDisk::from_block(self, data_block)?;
      debug!("Initializing data block, offset {}", data_block_offset);
      unsafe { schema::Row::init_table(&schema, &mut data_blockdisk)? };
    }

    let mut blockdisk = BlockDisk::new(self, schema_block_offset)?;
    let mut existing_schema = OnDiskSchema::read_tables(&mut blockdisk)?;
    blockdisk.seek(io::SeekFrom::Start(0))?;
    existing_schema.push(OnDiskSchema::new(data_block_offset, schema));

    OnDiskSchema::write_tables(&existing_schema, &mut blockdisk)?;

    Ok(())
  }

  pub fn schema(&mut self) -> Result<Vec<OnDiskSchema>, schema::SchemaError> {
    let schema_block_offset = self.meta.schema_block_offset;
    let mut reader = crate::BlockDisk::new(self, schema_block_offset)?;
    OnDiskSchema::read_tables(&mut reader)
  }

  /// Initializes a new database on the provided disk
  /// There should be no information on the provided disk
  pub fn new(mut disk: T) -> io::Result<Self> {
    // version 1, block size of 2048
    let block_size_exp = 6 as u8;
    let version = 1;
    let block_size = 2u64.pow(block_size_exp as u32);
    // create a new root block
    let root_block = Block::new(0, block_size);
    root_block.persist(&mut disk)?;

    let schema_block = Block::new(block_size, block_size);
    schema_block.persist(&mut disk)?;
    let meta = DatabaseMeta::new(version, block_size_exp);
    meta.persist(&mut disk)?;
    Ok(Database { disk, meta })
  }

  pub fn from_disk(mut disk: T) -> io::Result<Self> {
    let meta = DatabaseMeta::from_disk(&mut disk)?;

    Ok(Database { disk, meta })
  }
}

use crate::blockdisk::BlockAllocator;

impl<T: Disk> BlockAllocator for Database<T> {
  fn allocate_block(&mut self) -> io::Result<Block> {
    let next_block_offset = self.meta.num_allocated_blocks * self.meta.block_size();
    log::debug!("Allocating block at offset {}", next_block_offset);
    self.disk.seek(io::SeekFrom::Start(next_block_offset))?;
    let block = Block::new(next_block_offset, self.meta.block_size());
    self.meta.num_allocated_blocks += 1;
    self.meta.persist(&mut self.disk)?;
    block.persist(&mut self.disk)?;
    Ok(block)
  }
  fn read_block(&mut self, offset: u64) -> io::Result<Block> {
    log::debug!("Reading block at offset {}", offset);
    Block::from_disk(offset, self.meta.block_size(), &mut self.disk)
  }
  fn write_block(&mut self, block: &Block) -> io::Result<()> {
    log::debug!("Writing block at offset {}", block.meta().offset());
    block.persist(&mut self.disk).map(|_| ())
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use schema::SchemaError;

  #[test]
  fn test_adding_rows() -> Result<(), DatabaseError> {
    use schema::{Field, FieldKind};

    // Disk should have two blocks: one for the dbmeta and an empty schema block
    let mut database = Database::new(io::Cursor::new(vec![]))?;
    let schema = Schema::from_fields(
      "users".into(),
      vec![
        Field::new(FieldKind::Number(8), "id".into()).map_err(|err| SchemaError::from(err))?,
        Field::new(FieldKind::Str(20), "username".into()).map_err(|err| SchemaError::from(err))?,
      ],
    );
    use schema::OwnedRowCell;

    // Disk should have 3 blocks: dbmeta, schema block with one table, and a data block with one empty row
    database.create_table(schema.clone())?;
    let rows = vec![
      OwnedRowCell::Number { value: 1, size: 8 },
      OwnedRowCell::Str {
        value: "nlincoln".into(),
        max_size: 20,
      },
    ];
    // 3rd block should have 2 rows: the just inserted row, and the new sentinal
    database.add_row("users", rows.clone())?;

    let all_rows = database.read_table("users")?[0]
      .clone()
      .into_cells(&schema)?;

    assert_eq!(all_rows, rows);

    Ok(())
  }
  #[test]
  fn test_adding_a_bunch_of_tables() -> Result<(), DatabaseError> {
    use schema::{Field, FieldKind};
    let mut database = Database::new(io::Cursor::new(vec![]))?;
    let schema = Schema::from_fields(
      "the_name".into(),
      vec![
        Field::new(FieldKind::Blob(10), "id".into())?,
        Field::new(FieldKind::Blob(10), "id2".into())?,
        Field::new(FieldKind::Blob(10), "id3".into())?,
        Field::new(FieldKind::Blob(10), "id4".into())?,
        Field::new(FieldKind::Blob(10), "id5".into())?,
      ],
    );
    let mut expected_tables = vec![];

    for _i in 0..100 {
      // at each iteration, add the table again. Then re-read the tables.
      // they should match
      database.create_table(schema.clone())?;
      expected_tables.push(schema.clone());
      // value of data_block_offset is an impl detail, so we compare
      // the underlying schemas instead
      let schemas = database
        .schema()?
        .into_iter()
        .map(|ondiskschema: OnDiskSchema| ondiskschema.schema().clone())
        .collect::<Vec<_>>();
      assert_eq!(schemas, expected_tables);
    }
    Ok(())
  }

}
