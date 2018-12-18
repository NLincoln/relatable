use std::io::{Read, Seek, Write};

/// Convenience trait for Read + Write + Seek
pub trait Disk: Read + Write + Seek {}
impl<T: Read + Write + Seek> Disk for T {}

pub mod block;

mod block_io {
  use super::{block::Block, Disk};
  use std::io;

  pub trait BlockAllocator: Disk {
    fn allocate_block(&mut self) -> io::Result<Block>;
  }

  pub struct BlockWriter<'a, 'b> {
    disk: &'a mut BlockAllocator,
    start_block: &'b mut Block,
    current_offset: u64,
  }

  impl<'a, 'b> BlockWriter<'a, 'b> {
    pub fn new(disk: &'a mut impl BlockAllocator, start_block: &'b mut Block) -> Self {
      Self {
        disk,
        start_block,
        current_offset: 0,
      }
    }
  }
  use std::io::SeekFrom;
  impl<'a, 'b> io::Seek for BlockWriter<'a, 'b> {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
      let new_pos = match pos {
        SeekFrom::Current(num) => self.current_offset + num as u64,
        SeekFrom::Start(num) => num,
        SeekFrom::End(num) => return Err(io::Error::new(io::ErrorKind::InvalidData, "Attempted to seek from the end of a BlockWriter. BlockWriters operate on a conceptually infinite amount of memory, so seeking from the end is impossible"))
      };
      // TODO :: Possibly need to allocate new blocks here.
      self.current_offset = new_pos;
      Ok(new_pos)
    }
  }

  impl<'a, 'b> io::Write for BlockWriter<'a, 'b> {
    fn write(&mut self, mut buf: &[u8]) -> io::Result<usize> {
      use std::cell::Cell;
      // So the first step is to see if we have enough space in our
      // current buf left to actually write all of this
      let mut current_block = Cell::new(self.start_block);

      loop {
        let target_buf = current_block.get_mut().data_mut();
        let num_bytes_that_can_fit_in_this_block = target_buf.len() - self.current_offset as usize;
        let num_bytes_to_copy = std::cmp::min(num_bytes_that_can_fit_in_this_block, buf.len());
        for i in 0..num_bytes_to_copy {
          let offset = self.current_offset + i as u64;
          target_buf[offset as usize] = buf[i];
        }

        self.current_offset += num_bytes_to_copy as u64;
        buf = &buf[(self.current_offset as usize)..];
        if buf.is_empty() {
          break;
        }
        current_block.set(&mut self.disk.allocate_block()?);
      }

      unimplemented!()
    }
    fn flush(&mut self) -> io::Result<()> {
      unimplemented!()
    }
  }
}
