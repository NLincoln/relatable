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

// pub mod block_io {
//   use super::block::Block;
//   use std::io;

//   pub trait BlockAllocator {
//     fn allocate_block(&mut self) -> io::Result<Block>;
//     fn read_block(&mut self, offset: u64) -> io::Result<Block>;
//   }

//   pub struct BlockDiskReader<'a> {
//     disk: &'a mut BlockAllocator,
//     start_block: Block,
//     current_block_offset: usize,
//   }

//   impl<'a> BlockDiskReader<'a> {
//     pub fn new(disk: &'a mut BlockAllocator, start_block: Block) -> Self {
//       BlockDiskReader {
//         disk,
//         start_block,
//         current_block_offset: 0,
//       }
//     }
//   }

//   impl<'a> io::Read for BlockDiskReader<'a> {
//     fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
//     }
//   }

//   pub struct BlockDiskWriter<'a> {
//     disk: &'a mut BlockAllocator,
//     block_list: Vec<Block>,
//   }

//   impl<'a> BlockDiskWriter<'a> {
//     pub fn new(disk: &'a mut impl BlockAllocator, start_block: Block) -> Self {
//       BlockDiskWriter {
//         disk,
//         block_list: vec![start_block],
//       }
//     }
//   }

//   impl<'a> io::Write for BlockDiskWriter<'a> {
//     fn write(&mut self, mut buf: &[u8]) -> io::Result<usize> {
//       let buf_len = buf.len();
//       /*
//        * So here's the plan of attack: Write to the block that we have. When we get back UnexpectedEof from it,
//        * we allocate a new block, set the old blocks next_block to this new block, then replace our start_block with this block
//        * and continue writing
//        */
//       while !buf.is_empty() {
//         let current_block = self.block_list.last_mut().unwrap();

//         let mut writer = current_block.writer();
//         match writer.write(buf) {
//           Ok(bytes_written) => {
//             buf = &buf[bytes_written..];
//           }
//           Err(ref err) if err.kind() == io::ErrorKind::UnexpectedEof => {
//             let mut new_block = self.disk.allocate_block()?;
//             new_block.set_block_kind(current_block.meta().kind());
//             current_block.set_next_block(Some(new_block.meta().offset()));
//             // Ok so we don't reassign new_block here: that's find because we'll go to the start of the
//             // loop above and it'll get assigned there
//             self.block_list.push(new_block);
//           }
//           other_error @ Err(_) => return other_error,
//         }
//       }
//       Ok(buf_len)
//     }
//     fn flush(&mut self) -> io::Result<()> {
//       Ok(())
//     }
//   }
// }
