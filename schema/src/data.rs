use crate::{Disk, Schema};
use std::io::{self, Read, Seek, Write};

pub struct Table {
  name: String,
  columns: Schema,
}

impl Table {
  pub fn name(&self) -> &str {
    &self.name
  }
  pub fn schema(&self) -> &Schema {
    &self.columns
  }
  /*
   * Format here goes:
   * tablename_size(u16) tablename num_columns(u16) columns+
   */
  fn persist(&self, disk: &mut impl Write) -> io::Result<usize> {
    unimplemented!()
  }
  fn from_persisted(disk: &mut impl Read) -> io::Result<Self> {
    unimplemented!()
  }
}

pub struct Database<T: Disk> {
  tables: Vec<Table>,
  data: T,
}

impl<T: Disk> Database<T> {
  /// Initializes a new database on the provided disk
  /// There should be no information on the provided disk
  pub fn new(disk: T) -> io::Result<Self> {
    unimplemented!()
  }
  pub fn from_disk(disk: T) -> io::Result<Self> {
    unimplemented!()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::io::Cursor;
  #[test]
  fn create_db() {
    let mut disk = Cursor::new(vec![]);
    let db = Database::new(&mut disk).unwrap();
  }
}
