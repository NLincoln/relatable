use crate::{Block, BlockDisk};
use log::debug;
use schema::{Schema, SchemaFromBytesError};

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

impl<T: Disk> Database<T> {
  pub fn create_table(&mut self, schema: Schema) -> Result<(), schema::SchemaFromBytesError> {
    // Alright so the first thing we need to do is go find the
    // schema table and add this entry to it.
    debug!("Creating Table");
    debug!(
      "=> We currently have {} blocks allocated",
      self.meta.num_allocated_blocks
    );
    let schema_block_offset = self.meta.schema_block_offset;

    self.disk.seek(io::SeekFrom::Start(schema_block_offset))?;
    let block = Block::from_disk(schema_block_offset, self.meta.block_size(), &mut self.disk)?;

    let mut existing_schema = {
      let mut reader = BlockDisk::new(self, block)?;
      Schema::read_tables(&mut reader)?
    };
    existing_schema.push(schema);
    self.disk.seek(io::SeekFrom::Start(schema_block_offset))?;

    let block = Block::from_disk(schema_block_offset, self.meta.block_size(), &mut self.disk)?;

    let mut writer = BlockDisk::new(self, block)?;
    Schema::write_tables(&existing_schema, &mut writer)?;

    Ok(())
  }
  pub fn schema(&mut self) -> Result<Vec<Schema>, schema::SchemaFromBytesError> {
    let schema_block_offset = self.meta.schema_block_offset;
    self.disk.seek(io::SeekFrom::Start(schema_block_offset))?;
    let block =
      crate::Block::from_disk(schema_block_offset, self.meta.block_size(), &mut self.disk)?;
    let mut reader = crate::BlockDisk::new(self, block)?;
    Schema::read_tables(&mut reader)
  }

  /// Initializes a new database on the provided disk
  /// There should be no information on the provided disk
  pub fn new(mut disk: T) -> io::Result<Self> {
    // version 1, block size of 2048
    let block_size_exp = 11 as u8;
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

impl<T: Disk> io::Write for Database<T> {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    self.disk.write(buf)
  }
  fn flush(&mut self) -> io::Result<()> {
    self.disk.flush()
  }
}

impl<T: Disk> io::Read for Database<T> {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    self.disk.read(buf)
  }
}

impl<T: Disk> io::Seek for Database<T> {
  fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
    self.disk.seek(pos)
  }
}

impl<T: Disk> BlockAllocator for Database<T> {
  fn allocate_block(&mut self) -> io::Result<Block> {
    let next_block_offset = self.meta.num_allocated_blocks * self.meta.block_size();
    self.disk.seek(io::SeekFrom::Start(next_block_offset))?;
    let block = Block::new(next_block_offset, self.meta.block_size());
    self.meta.num_allocated_blocks += 1;
    self.meta.persist(&mut self.disk)?;
    block.persist(&mut self.disk)?;
    Ok(block)
  }
  fn read_block(&mut self, offset: u64) -> io::Result<Block> {
    Block::from_disk(offset, self.meta.block_size(), &mut self.disk)
  }
}

#[test]
fn test_adding_a_bunch_of_tables() -> Result<(), SchemaFromBytesError> {
  env_logger::init();

  use schema::{Field, FieldKind};
  let mut database = Database::new(io::Cursor::new(vec![]))?;
  let schema = Schema::from_fields(
    "the_name".into(),
    vec![
      Field::new(FieldKind::Blob(10), "id".into()),
      Field::new(FieldKind::Blob(10), "id2".into()),
      Field::new(FieldKind::Blob(10), "id3".into()),
      Field::new(FieldKind::Blob(10), "id4".into()),
      Field::new(FieldKind::Blob(10), "id5".into()),
    ],
  );
  let mut expected_tables = vec![];

  for _ in 0..100 {
    // at each iteration, add the table again. Then re-read the tables.
    // they should match
    database.create_table(schema.clone())?;
    expected_tables.push(schema.clone());
    assert_eq!(database.schema()?, expected_tables);
  }
  Ok(())
}
