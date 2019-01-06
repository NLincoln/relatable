use crate::Block;
use std::io;

pub trait BlockAllocator {
  fn allocate_block(&mut self) -> io::Result<Block>;
  fn read_block(&mut self, offset: u64) -> io::Result<Block>;
  fn write_block(&mut self, block: &Block) -> io::Result<()>;
}

#[derive(Debug)]
pub struct BlockDisk<'a, D: BlockAllocator> {
  blocks: Vec<Block>,
  current_offset: u64,
  disk: &'a mut D,
}

impl<'a, D: BlockAllocator> BlockDisk<'a, D> {
  pub fn new(disk: &'a mut D, start_block_offset: u64) -> io::Result<Self> {
    let start_block = disk.read_block(start_block_offset)?;
    Ok(BlockDisk {
      blocks: vec![start_block],
      current_offset: 0,
      disk,
    })
  }

  pub fn from_block(disk: &'a mut D, start_block: Block) -> io::Result<Self> {
    Ok(BlockDisk {
      blocks: vec![start_block],
      current_offset: 0,
      disk,
    })
  }

  /// Returns whether a new block was allocated
  fn increase_read_size_by_block(&mut self, force: bool) -> io::Result<bool> {
    let current_block = self.blocks.last_mut().unwrap(); // unwrap is fine because we always start with a block
    let next_block = current_block.meta().next_block();
    match next_block {
      Some(offset) => {
        let block = self.disk.read_block(offset)?;
        self.blocks.push(block);
        log::debug!(
          "read existing block. new size {}",
          self.current_size_allocated()
        );
        Ok(true)
      }
      None => {
        if force {
          self.allocate_new_block_at_end()?;
          log::debug!("read new block. new size {}", self.current_size_allocated());
          Ok(true)
        } else {
          Ok(false)
        }
      }
    }
  }

  fn allocate_new_block_at_end(&mut self) -> io::Result<()> {
    let current_block = self.blocks.last_mut().unwrap();
    let next_block = current_block.meta().next_block();
    match next_block {
      Some(_) => return Ok(()),
      None => {
        let next_block = self.disk.allocate_block()?;
        current_block.set_next_block(Some(next_block.meta().offset()));
        self.disk.write_block(current_block)?;
        self.blocks.push(next_block);
        Ok(())
      }
    }
  }

  /// Make sure that we have at least n blocks allocated
  fn ensure_num_blocks(&mut self, num: usize, force: bool) -> io::Result<()> {
    while self.blocks.len() < num {
      self.increase_read_size_by_block(force)?;
    }
    Ok(())
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
  fn current_disk_size(&self) -> u64 {
    let mut total = 0;
    for block in &self.blocks {
      total += block.meta().size();
    }
    total
  }
}

impl<'a, D: BlockAllocator> io::Read for BlockDisk<'a, D> {
  fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
    // Start at the current offset and read n bytes from the buffer.
    // if we hit the point at which we're at the end of a block,
    // look and see if we're at the end of a block
    let block_size = self.block_size();
    let start_offset = self.current_offset;

    while !buf.is_empty() {
      let current_block = {
        let idx = self.current_block_idx() as usize;
        self.ensure_num_blocks(idx + 1, false)?;
        match self.blocks.get_mut(idx) {
          Some(block) => block,
          None => return Ok((self.current_offset - start_offset) as usize),
        }
      };

      let mut disk = current_block.disk(self.current_offset % block_size);
      match disk.read(buf) {
        Ok(bytes_written) => {
          self.current_offset += bytes_written as u64;
          buf = &mut buf[bytes_written..];
        }
        Err(ref err) if err.kind() == io::ErrorKind::UnexpectedEof => unreachable!(),
        err @ Err(_) => {
          let bytes_written = self.current_offset - start_offset;
          if bytes_written == 0 {
            return err;
          }
          self.disk.write_block(current_block)?;

          return Ok(bytes_written as usize);
        }
      };
    }

    Ok((self.current_offset - start_offset) as usize)
  }
}

impl<'a, D: BlockAllocator> io::Write for BlockDisk<'a, D> {
  fn write(&mut self, mut buf: &[u8]) -> io::Result<usize> {
    let block_size = self.block_size();
    let start_offset = self.current_offset;

    while !buf.is_empty() {
      let current_block = {
        let idx = self.current_block_idx() as usize;
        self.ensure_num_blocks(idx + 1, true)?;
        &mut self.blocks[idx]
      };
      let mut disk = current_block.disk(self.current_offset % block_size);

      match disk.write(buf) {
        Ok(bytes_written) => {
          self.current_offset += bytes_written as u64;
          buf = &buf[bytes_written..];
          self.disk.write_block(current_block)?;
        }
        Err(ref err) if err.kind() == io::ErrorKind::UnexpectedEof => {
          // We will always be able to write an entire blocks worth of bytes. Unless the buf is empty.
          // if the buf is empty we return tho
          unreachable!();
        }
        err @ Err(_) => {
          let bytes_written_total = self.current_offset - start_offset;

          if bytes_written_total == 0 {
            return err;
          }
          self.disk.write_block(current_block)?;

          return Ok(bytes_written_total as usize);
        }
      };
    }

    Ok((self.current_offset - start_offset) as usize)
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
      SeekFrom::End(offset) => {
        // allocate new blocks until we run out
        while self.increase_read_size_by_block(false)? {}
        assert!(offset <= 0); // We don't handle + properly
        let next_pos = self.current_disk_size() as i64 + offset; // surely nobody will pass in a positive number here...
        log::debug!("SeekFrom::End({}) -> {}", offset, next_pos);

        assert!(next_pos >= 0);
        next_pos as u64
      }
      SeekFrom::Current(offset) => {
        let current = self.current_offset as i64;
        let next = current + offset;
        if next < 0 {
          return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Attempted to seek to a negative offset",
          ));
        }
        next as u64
      }
    };
    while self.current_size_allocated() < next_offset {
      self.increase_read_size_by_block(true)?;
    }

    self.current_offset = next_offset;

    Ok(next_offset)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::inmemorydb::InMemoryDatabase;
  use std::io::{self, Read, Seek, Write};
  #[test]
  fn test_blockdisk_io() -> io::Result<()> {
    let mut db = InMemoryDatabase::new(io::Cursor::new(vec![0; 128]));

    let block = BlockAllocator::allocate_block(&mut db)?;
    let mut blockdisk = BlockDisk::from_block(&mut db, block)?;

    let mut data_to_write = vec![];
    for i in 0..=255 {
      data_to_write.push(i);
    }
    // write a BUNCH of data
    data_to_write.append(&mut data_to_write.clone());
    blockdisk.write_all(&data_to_write)?;

    blockdisk.seek(io::SeekFrom::Start(260))?;
    let mut result = vec![0; 5];
    blockdisk.read_exact(&mut result)?;
    assert_eq!(result, vec![4, 5, 6, 7, 8]);

    Ok(())
  }
  #[test]
  fn test_a_bunch_of_small_writes() -> io::Result<()> {
    use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
    let mut db = InMemoryDatabase::new(io::Cursor::new(vec![]));
    let block = db.allocate_block()?;
    assert!(db.blocks_allocated == 1);
    let mut blockdisk = BlockDisk::from_block(&mut db, block)?;

    // to get the offsets all wonky
    blockdisk.write_u8(1)?;
    blockdisk.write_u64::<BigEndian>(10)?;
    blockdisk.write_u64::<BigEndian>(11)?;
    blockdisk.write_u64::<BigEndian>(12)?;
    blockdisk.write_u64::<BigEndian>(13)?;

    blockdisk.seek(io::SeekFrom::Start(0))?;

    assert_eq!(1, blockdisk.read_u8()?);
    assert_eq!(10, blockdisk.read_u64::<BigEndian>()?);
    assert_eq!(11, blockdisk.read_u64::<BigEndian>()?);
    assert_eq!(12, blockdisk.read_u64::<BigEndian>()?);
    assert_eq!(13, blockdisk.read_u64::<BigEndian>()?);

    // make sure we ACTUALLY allocated a block here
    assert!(db.blocks_allocated > 1);
    Ok(())
  }
  #[test]
  fn test_funky_block_ordering() -> io::Result<()> {
    use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
    let mut db = InMemoryDatabase::new(io::Cursor::new(vec![]));
    let start_block_a = db.allocate_block()?;
    // Allocate a next block so that when block a overflows we have to skip a block
    db.allocate_block()?;

    let mut blockdisk = BlockDisk::from_block(&mut db, start_block_a)?;
    blockdisk.write_u16::<BigEndian>(1)?;
    blockdisk.write_u64::<BigEndian>(10)?;
    blockdisk.write_u64::<BigEndian>(11)?;
    blockdisk.write_u64::<BigEndian>(12)?;

    blockdisk.seek(io::SeekFrom::Start(0))?;

    assert_eq!(1, blockdisk.read_u16::<BigEndian>()?);
    assert_eq!(10, blockdisk.read_u64::<BigEndian>()?);
    assert_eq!(11, blockdisk.read_u64::<BigEndian>()?);
    assert_eq!(12, blockdisk.read_u64::<BigEndian>()?);
    Ok(())
  }

}
