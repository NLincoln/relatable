use std::io::{Read, Seek, Write};

/// Convenience trait for Read + Write + Seek
pub trait Disk: Read + Write + Seek {}
impl<T: Read + Write + Seek> Disk for T {}

pub mod block;

pub mod block_io {
  use super::block::Block;
  use std::io::{self, Read, Write};

  pub trait BlockAllocator: Read + Write {
    fn allocate_block(&mut self) -> io::Result<Block>;
  }

  pub struct BlockDiskWriter<'a> {
    disk: &'a mut BlockAllocator,
    current_block: Block,
  }

  impl<'a> BlockDiskWriter<'a> {
    pub fn new(disk: &'a mut impl BlockAllocator, start_block: Block) -> Self {
      BlockDiskWriter {
        disk,
        current_block: start_block,
      }
    }
  }

  impl<'a> io::Write for BlockDiskWriter<'a> {
    fn write(&mut self, mut buf: &[u8]) -> io::Result<usize> {
      let buf_len = buf.len();
      /*
       * So here's the plan of attack: Write to the block that we have. When we get back UnexpectedEof from it,
       * we allocate a new block, set the old blocks next_block to this new block, then replace our start_block with this block
       * and continue writing
       */
      while !buf.is_empty() {
        let mut writer = self.current_block.writer();
        match writer.write(buf) {
          Ok(bytes_written) => {
            buf = &buf[bytes_written..];
          }
          Err(ref err) if err.kind() == io::ErrorKind::UnexpectedEof => {
            let mut new_block = self.disk.allocate_block()?;
            new_block.set_block_kind(self.current_block.meta().kind());
            self
              .current_block
              .set_next_block(Some(new_block.meta().offset()));
            self.current_block = new_block;
          }
          other_error @ Err(_) => return other_error,
        }
      }
      Ok(buf_len)
    }
    fn flush(&mut self) -> io::Result<()> {
      Ok(())
    }
  }
}
