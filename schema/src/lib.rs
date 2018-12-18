//! Schema definition and data storage
mod data;
mod disk;
mod schema;
const BLOCK_SIZE: u64 = 2048;
use self::disk::{
  block::{Block, BlockKind},
  Disk,
};

pub use self::data::{Database, Table};
pub use self::schema::{Field, FieldKind, Schema, SchemaFromBytesError};
