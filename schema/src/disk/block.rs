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
    let mut bytes_read = 1;
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
pub struct Block {
  /// The properly allocated data in the block.
  data: Vec<u8>,
  /// Meta-information about the block
  meta: BlockMeta,
}

impl Block {
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

  pub fn writer<'a>(&'a mut self) -> BlockWriter<'a> {
    BlockWriter { block: self }
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

pub struct BlockWriter<'a> {
  block: &'a mut Block,
}

impl<'a> io::Write for BlockWriter<'a> {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    let how_much_space_is_left_in_this_buffer =
      self.block.data.len() - self.block.meta.size as usize;
    let how_many_bytes_will_we_write =
      std::cmp::min(buf.len(), how_much_space_is_left_in_this_buffer);

    let is_there_space_left = how_many_bytes_will_we_write == 0;
    if !is_there_space_left {
      return Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "Attempted to write too many bytes. Allocate a new buffer and try again",
      ));
    }

    for i in 0..how_many_bytes_will_we_write {
      let idx = self.block.meta.size + i as u64;
      self.block.data[idx as usize] = buf[i];
    }
    self.block.meta.size += how_many_bytes_will_we_write as u64;
    Ok(how_many_bytes_will_we_write)
  }

  fn flush(&mut self) -> io::Result<()> {
    Ok(())
  }
}
