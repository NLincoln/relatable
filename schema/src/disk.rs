use std::io::{self, Read, Seek, Write};

/// Convenience trait for Read + Write + Seek
pub trait Disk: Read + Write + Seek {}
impl<T: Read + Write + Seek> Disk for T {}

pub mod block;

use self::block::Block;

pub trait BlockAllocator: Disk {
  fn allocate_block(&mut self) -> io::Result<Block>;
  fn read_block(&mut self, offset: u64) -> io::Result<Block>;
}

pub struct BlockDisk<'a, D: BlockAllocator> {
  blocks: Vec<block::Block>,
  current_offset: u64,
  disk: &'a mut D,
}

impl<'a, D: BlockAllocator> BlockDisk<'a, D> {
  pub fn new(disk: &'a mut D, start_block: block::Block) -> io::Result<Self> {
    Ok(BlockDisk {
      blocks: vec![start_block],
      current_offset: 0,
      disk,
    })
  }

  fn increase_read_size_by_block(&mut self) -> io::Result<()> {
    let block_idx = self.current_block_idx() as usize;
    let current_block = &mut self.blocks[block_idx];
    let next_block = current_block.meta().next_block();
    match next_block {
      Some(offset) => {
        let block = self.disk.read_block(offset)?;
        self.blocks.push(block);
        Ok(())
      }
      None => {
        let next_block = self.disk.allocate_block()?;
        current_block.set_next_block(Some(next_block.meta().offset()));
        current_block.persist(&mut self.disk)?;
        self.blocks.push(next_block);
        Ok(())
      }
    }
  }

  fn block_size(&self) -> u64 {
    self.blocks[0].data().len() as u64
  }
  fn current_block_idx(&self) -> u64 {
    self.current_offset / self.block_size()
  }
  fn current_size_allocated(&self) -> u64 {
    self.block_size() * self.blocks.len() as u64
  }
}

impl<'a, D: BlockAllocator> io::Read for BlockDisk<'a, D> {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    // Start at the current offset and read n bytes from the buffer.
    // if we hit the point at which we're at the end of a block,
    // look and see if we're at the end of a block
    let block_size = self.block_size();

    let current_block = {
      let idx = self.current_block_idx() as usize;
      &mut self.blocks[idx]
    };

    let mut disk = current_block.disk(self.current_offset % block_size);

    unimplemented!()
  }
}

impl<'a, D: BlockAllocator> io::Write for BlockDisk<'a, D> {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    unimplemented!()
  }
  fn flush(&mut self) -> io::Result<()> {
    Ok(())
  }
}

impl<'a, D: BlockAllocator> io::Seek for BlockDisk<'a, D> {
  fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
    use std::io::SeekFrom;

    let next_offset = match pos {
      SeekFrom::Start(offset) => offset,
      SeekFrom::End(_) => return Err(io::Error::new(io::ErrorKind::InvalidInput, "Attempted to seek from the end of a BlockDisk. BlockDisks are assumed to be infinite in size")),
      SeekFrom::Current(offset) => {
        let current = self.current_offset as i64;
        let next = current + offset;
        if next < 0 {
          return Err(io::Error::new(io::ErrorKind::InvalidInput, "Attempted to seek to a negative offset"));
        }
        next as u64
      }
    };
    while self.current_size_allocated() < next_offset {
      self.increase_read_size_by_block()?;
    }

    self.current_offset = next_offset;

    Ok(next_offset)
  }
}

#[cfg(test)]
mod tests {
  use super::block::Block;
  use super::BlockAllocator;
  use super::*;
  use std::io::{Read, Seek, Write};
  const BLOCK_SIZE: u64 = 32;

  struct InMemoryDatabase {
    blocks_allocated: u64,
    disk: io::Cursor<Vec<u8>>,
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
      let block = Block::new(
        super::block::BlockKind::Record,
        next_block_offset,
        BLOCK_SIZE,
      );
      block.persist(&mut self.disk)?;
      Ok(block)
    }
  }

  #[test]
  fn test_blockdisk_io() -> io::Result<()> {
    let mut db = InMemoryDatabase {
      blocks_allocated: 0,
      disk: std::io::Cursor::new(vec![0; 2048]),
    };

    let block = BlockAllocator::allocate_block(&mut db)?;
    let mut blockdisk = BlockDisk::new(&mut db, block)?;

    let mut data_to_write = vec![];
    for i in 0..=255 {
      data_to_write.push(i);
    }
    // write a BUNCH of data
    data_to_write.append(&mut data_to_write.clone());
    data_to_write.append(&mut data_to_write.clone());
    data_to_write.append(&mut data_to_write.clone());
    blockdisk.write_all(&data_to_write)?;

    blockdisk.seek(io::SeekFrom::Start(260))?;
    let mut result = vec![0; 5];
    blockdisk.read_exact(&mut result)?;
    assert_eq!(result, vec![4, 5, 6, 7, 8]);

    Ok(())
  }

}
