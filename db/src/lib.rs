//! Schema definition and data storage

mod block;
mod blockdisk;
mod database;
#[cfg(test)]
mod inmemorydb;

use self::block::Block;
use self::blockdisk::BlockDisk;

pub use self::database::{Database, DatabaseError, DatabaseQueryError};
