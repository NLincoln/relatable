use crate::{disk::BlockAllocator, Block};
use std::io::{self, Seek};
const BLOCK_SIZE: u64 = 32;

#[derive(Debug)]
pub struct InMemoryDatabase {
  blocks_allocated: u64,
  disk: io::Cursor<Vec<u8>>,
}

impl InMemoryDatabase {
  pub fn new(disk: io::Cursor<Vec<u8>>) -> InMemoryDatabase {
    InMemoryDatabase {
      blocks_allocated: 0,
      disk,
    }
  }
}

impl io::Write for InMemoryDatabase {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    self.disk.write(buf)
  }
  fn flush(&mut self) -> io::Result<()> {
    self.disk.flush()
  }
}

impl io::Read for InMemoryDatabase {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    self.disk.read(buf)
  }
}

impl io::Seek for InMemoryDatabase {
  fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
    self.disk.seek(pos)
  }
}

impl BlockAllocator for InMemoryDatabase {
  fn read_block(&mut self, offset: u64) -> io::Result<Block> {
    self.disk.seek(io::SeekFrom::Start(offset))?;

    Block::from_disk(offset, BLOCK_SIZE, &mut self.disk)
  }
  fn allocate_block(&mut self) -> io::Result<Block> {
    let next_block_offset = BLOCK_SIZE * self.blocks_allocated;
    self.disk.seek(io::SeekFrom::Start(next_block_offset))?;
    let block = Block::new(next_block_offset, BLOCK_SIZE);
    block.persist(&mut self.disk)?;
    self.blocks_allocated += 1;
    Ok(block)
  }
}
