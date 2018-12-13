use crate::Schema;
use std::io::{self, Read, Seek, Write};

pub struct Table {
  name: String,
  columns: Schema,
}

pub struct Database<T: Read + Write + Seek> {
  tables: Vec<Table>,
  data: T,
}

impl<T: Read + Write + Seek> Database<T> {
  pub fn from_io(io: T) -> io::Result<Self> {
    /*
     * So we position our cursor at the start of the buffer.
     * The question is how this starts. What do we read off first?
     *
     * I'm thinking that the file encoding should go something like the following:
     *
     * start: data_offset(u64) schema+
     * schema: tablename_size(u16) + tablename(&str) + column_size(u32) + columns
     *
     * My main worry with this solution is that I'll spend a lot of time seeking around
     * in the file. Maybe this isn't a big deal?
     */
    unimplemented!()
  }
}
