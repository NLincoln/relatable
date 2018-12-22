use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{self, Read, Seek, Write};
#[derive(Debug, PartialEq, Clone)]
#[repr(u8)]
pub enum BlockKind {
  /// The root block. Contains information about the database itself, such as
  /// the location of the root block
  Root = 1,
  /// A Schema Block. These contain the serialized schema for the db.
  /// There may be multiple schema blocks.
  Schema = 2,

  /// A record block contains the actual data. It's important to note that
  /// if a block is a `Record` block, then it will only contain data for a single
  /// table. It's impossible to say what that table is, however (that data isn't encoded into a block)
  Record = 3,
}

/// Meta-information about a block
/// It is possible to create one of these
/// without actually reading in the entire block,
/// which is useful for situations when you want to know
/// _what_ is in a block without actually reading the entire thing
/// in
#[derive(Debug)]
pub struct BlockMeta {
  kind: BlockKind,
  /// The offset in the file this block appears at. Isn't actually written to disk
  offset: u64,

  /// The offset of the next block.
  ///
  /// Reasons why this wouldn't exist:
  /// - This type of block never has additional blocks (e.g. the Root block)
  /// - This is the last block in the linked list
  /// If this doesn't exist, it is all zeros.
  next_block: Option<u64>,

  /// The total number of bytes that have been written to this block
  size: u64,
}

impl BlockMeta {
  pub fn kind(&self) -> BlockKind {
    self.kind.clone()
  }
  pub fn offset(&self) -> u64 {
    self.offset
  }
  pub fn next_block(&self) -> Option<u64> {
    self.next_block
  }
  fn size_on_disk() -> usize {
    // 1 byte for tag, 8 bytes for next block, 8 bytes for size
    // Just gonna go ahead and say that this is always the case,
    // to avoid headaches
    17
  }
  /// This will only write the block header.
  /// So i.e. only kind and next_block
  fn persist(&self, disk: &mut impl Write) -> io::Result<()> {
    disk.write_u8(self.kind.clone() as u8)?;
    disk.write_u64::<BigEndian>(self.next_block.unwrap_or(0))?;
    disk.write_u64::<BigEndian>(self.size)?;

    Ok(())
  }

  pub fn new(offset: u64, disk: &mut impl Read) -> io::Result<Self> {
    // blocks start off with the block meta, then the rest of the data.
    let tag = disk.read_u8()?;
    let kind = match tag {
      1 => BlockKind::Root,
      2 => BlockKind::Schema,
      3 => BlockKind::Record,
      unknown => panic!("Unknown block type {}", unknown),
    };
    let next_block = disk.read_u64::<BigEndian>()?;
    let next_block = if next_block == 0 {
      None
    } else {
      Some(next_block)
    };
    let size = disk.read_u64::<BigEndian>()?;
    Ok(BlockMeta {
      kind,
      next_block,
      size,
      offset,
    })
  }
}

/// A block is a piece of data in the file.
/// Each block is equal in size, but they all hold distinct pieces of
/// information. There's a good bit of internal fragmentation.
#[derive(Debug)]
pub struct Block {
  /// The properly allocated data in the block.
  data: Vec<u8>,
  /// Meta-information about the block
  meta: BlockMeta,
}

impl Block {
  fn space_available(&self) -> usize {
    self.data.len() - self.meta.size as usize
  }
  pub fn set_next_block(&mut self, next: Option<u64>) {
    self.meta.next_block = next;
  }
  pub fn set_block_kind(&mut self, kind: BlockKind) {
    if kind == BlockKind::Root {
      self.meta.next_block = None;
    }
    self.meta.kind = kind;
  }
  pub fn meta(&self) -> &BlockMeta {
    &self.meta
  }

  pub fn data(&self) -> &[u8] {
    &self.data
  }

  pub fn persist(&self, disk: &mut (impl Write + Seek)) -> io::Result<usize> {
    use std::io::SeekFrom;
    disk.seek(SeekFrom::Start(self.meta.offset))?;
    self.meta.persist(disk)?;
    disk.write_all(&self.data)?;

    Ok(self.data().len() + BlockMeta::size_on_disk())
  }

  /// Creates a new block from the given disk.
  /// Will read the entire block from the disk (i.e. blocksize bytes)
  pub fn from_disk(offset: u64, blocksize: u64, disk: &mut impl Read) -> io::Result<Self> {
    let meta = BlockMeta::new(offset, disk)?;
    let bytes_to_read = blocksize as usize - BlockMeta::size_on_disk();
    let mut buf = vec![0; bytes_to_read];
    disk.read_exact(&mut buf)?;
    Ok(Block { data: buf, meta })
  }

  pub(crate) fn disk<'a>(&'a mut self, start_offset: u64) -> BlockDiskView<'a> {
    BlockDiskView {
      block: self,
      current_offset: start_offset,
    }
  }

  pub fn new(kind: BlockKind, offset: u64, blocksize: u64) -> Self {
    let meta = BlockMeta {
      kind,
      offset,
      next_block: None,
      size: 0,
    };
    Self {
      meta,
      data: vec![0; blocksize as usize - BlockMeta::size_on_disk()],
    }
  }
}

#[derive(Debug)]
pub struct BlockDiskView<'a> {
  current_offset: u64,
  block: &'a mut Block,
}

impl<'a> BlockDiskView<'a> {
  fn is_at_end_of_block(&self) -> bool {
    self.current_offset as usize >= self.block.data.len()
  }
  fn end_of_block(&self) -> io::Result<()> {
    if self.is_at_end_of_block() {
      return Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        format!(
          "Reached end of block. Current offset is {}",
          self.current_offset
        ),
      ));
    }
    Ok(())
  }
}
impl<'a> io::Read for BlockDiskView<'a> {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    self.end_of_block()?;

    for i in 0..buf.len() {
      let offset = self.current_offset as usize;
      if self.is_at_end_of_block() {
        return Ok(i);
      }
      buf[i] = self.block.data[offset];
      self.current_offset += 1;
    }
    Ok(buf.len())
  }
}

impl<'a> io::Write for BlockDiskView<'a> {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    self.end_of_block()?;

    for i in 0..buf.len() {
      let offset = self.current_offset as usize;
      if self.is_at_end_of_block() {
        return Ok(i);
      }
      self.block.data[offset] = buf[i];
      self.block.meta.size += 1;
      self.current_offset += 1;
    }
    Ok(buf.len())
  }
  fn flush(&mut self) -> io::Result<()> {
    Ok(())
  }
}

impl<'a> io::Seek for BlockDiskView<'a> {
  fn seek(&mut self, seek: io::SeekFrom) -> io::Result<u64> {
    use std::io::SeekFrom;
    match seek {
      SeekFrom::Start(offset) => self.current_offset = offset,
      SeekFrom::Current(offset) => {
        let mut current = self.current_offset as i64;
        current += offset;
        if current < 0 {
          return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Tried to seek to a negative",
          ));
        }
        self.current_offset = current as u64;
      }
      SeekFrom::End(offset) => {
        let end_offset = self.block.data.len() - 1;
        let end_offset = end_offset as i64;
        let current = end_offset - offset;
        if current < 0 {
          return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Tried to seek to a negative",
          ));
        }
        self.current_offset = current as u64;
      }
    };
    Ok(self.current_offset)
  }
}

#[test]
fn test_block_disk_view_err() {
  let block_size = 128;
  let mut block = Block::new(BlockKind::Record, 0, block_size);
  let data_size = block.data.len() as u64;

  let mut view = block.disk(0);
  let mut data = Vec::<u8>::new();

  for i in 0..data_size {
    data.push(i as u8);
  }
  view.write_all(&data).unwrap();

  view
    .seek(io::SeekFrom::Start(
      // going to try to read 10 bytes, going right up to the end
      data_size - 5,
    ))
    .unwrap();
  let mut data = vec![0; 5];
  view.read_exact(&mut data).unwrap();
  assert_eq!(data, vec![106, 107, 108, 109, 110]);
  // now for the interesting part: the next read should fail:
  let mut data = vec![0; 5];
  assert!(view.read(&mut data).is_err());
}

#[test]
fn test_block_disk_view() {
  let mut block = Block::new(BlockKind::Record, 0, 256);
  let mut view = block.disk(0);
  let mut data = vec![];

  for i in 0..128 {
    data.push(i);
  }
  view.write_all(&data).unwrap();

  view.seek(io::SeekFrom::Start(12)).unwrap();
  let mut data = vec![0; 10];
  view.read_exact(&mut data).unwrap();
  assert_eq!(data, vec![12, 13, 14, 15, 16, 17, 18, 19, 20, 21]);
}
