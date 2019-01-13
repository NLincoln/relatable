use crate::{Field, Row, RowCellError};

// aight we're about to get fancy with this.

pub trait Table: Iterator<Item = Result<Row, RowCellError>> {
  fn schema(&self) -> &[Field];
}
