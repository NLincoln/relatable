use crate::{Block, Disk, Schema};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{self, Read, Seek, Write};

pub struct Table {
  name: String,
  columns: Schema,
}

impl Table {
  pub fn name(&self) -> &str {
    &self.name
  }
  pub fn schema(&self) -> &Schema {
    &self.columns
  }
  /*
   * Format here goes:
   * tablename_size(u16) tablename num_columns(u16) columns+
   */
  fn persist(&self, disk: &mut impl Write) -> io::Result<usize> {
    unimplemented!()
  }
  fn from_persisted(disk: &mut impl Read) -> io::Result<Self> {
    unimplemented!()
  }
}

pub struct Database<T: Disk> {
  disk: T,
  meta: DatabaseMeta,
}

/// Basically a structure that holds all the information in the root block
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

  fn persist(&self, disk: &mut (impl Write + Seek)) -> io::Result<()> {
    disk.seek(io::SeekFrom::Start(0))?;
    disk.write_u8(self.version)?;
    disk.write_u8(self.block_size_exp)?;
    disk.write_u64::<BigEndian>(self.num_allocated_blocks)?;
    disk.write_u64::<BigEndian>(self.schema_block_offset)?;
    Ok(())
  }

  fn from_disk(disk: &mut (impl Read + Seek)) -> io::Result<DatabaseMeta> {
    disk.seek(io::SeekFrom::Start(0));
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

impl<T: Disk> Database<T> {
  pub fn create_table(&mut self, schema: Schema) -> io::Result<()> {
    // Alright so the first thing we need to do is go find the
    // schema table and add this entry to it.

    unimplemented!()
  }
  /// Initializes a new database on the provided disk
  /// There should be no information on the provided disk
  pub fn new(mut disk: T) -> io::Result<Self> {
    use crate::BlockKind;
    // create a new root block
    let root_block = Block::new(BlockKind::Root, 0, crate::BLOCK_SIZE);
    root_block.persist(&mut disk)?;

    let schema_block = Block::new(BlockKind::Schema, crate::BLOCK_SIZE, crate::BLOCK_SIZE);
    schema_block.persist(&mut disk)?;
    // version 1, block size of 2048
    let meta = DatabaseMeta::new(1, 11);
    meta.persist(&mut disk)?;
    Ok(Database { disk, meta })
  }

  pub fn from_disk(mut disk: T) -> io::Result<Self> {
    let meta = DatabaseMeta::from_disk(&mut disk)?;

    Ok(Database { disk, meta })
  }
}
