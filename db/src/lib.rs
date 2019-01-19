//! Schema definition and data storage

mod block;
mod blockdisk;
mod database;
#[cfg(test)]
mod inmemorydb;
mod table;

use self::block::Block;
use self::blockdisk::BlockDisk;

pub use self::database::{Database, DatabaseError, DatabaseQueryError};
