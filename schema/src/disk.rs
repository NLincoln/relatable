use std::io::{Read, Seek, Write};

/// Convenience trait for Read + Write + Seek
pub(crate) trait Disk: Read + Write + Seek {}
impl<T: Read + Write + Seek> Disk for T {}
