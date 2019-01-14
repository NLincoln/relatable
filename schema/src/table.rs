use crate::field::Field;
use crate::{FieldKind, OwnedRowCell, Row, RowCellError};

#[derive(Debug)]
pub enum TableError {
  RowCell(RowCellError),
  Other(String)
}

impl From<RowCellError> for TableError {
  fn from(err: RowCellError) -> TableError {
    TableError::RowCell(err)
  }
}

#[derive(Debug, PartialEq, Clone)]
pub enum TableFieldLiteral {
  Number(i64),
  Str(String),
  Blob(Vec<u8>),
}

#[derive(Debug, PartialEq, Clone)]
pub struct TableField {
  name: Option<String>,
  kind: FieldKind,
  literal_value: Option<TableFieldLiteral>,
}

impl Field for TableField {
  fn kind(&self) -> &FieldKind {
    &self.kind
  }
}

impl<'a> From<&'a crate::SchemaField> for TableField {
  fn from(field: &'a crate::SchemaField) -> TableField {
    TableField {
      name: Some(field.name().to_string()),
      kind: field.kind().clone(),
      literal_value: None,
    }
  }
}

pub trait Table: Iterator<Item = Result<Row, RowCellError>> {
  fn schema(&self) -> Vec<TableField>;
  fn map_schema(self, schema: Vec<TableField>) -> MapSchema<Self>
  where
    Self: Sized,
  {
    MapSchema {
      prev_schema: self.schema().to_vec(),
      schema,
      iter: self,
    }
  }

  fn into_iter_cells<T>(self) -> IntoIterCells<Self>
  where
    Self: Sized,
  {
    IntoIterCells {
      schema: self.schema().to_vec(),
      iter: self,
    }
  }
}

pub struct IntoIterCells<I> {
  iter: I,
  schema: Vec<TableField>,
}

impl<I> Iterator for IntoIterCells<I>
where
  I: Table,
{
  type Item = Result<Vec<OwnedRowCell>, RowCellError>;
  fn next(&mut self) -> Option<Self::Item> {
    let next = self.iter.next()?;
    Some(next.and_then(|row| row.into_cells(&self.schema)))
  }
}

pub struct MapSchema<I> {
  prev_schema: Vec<TableField>,
  schema: Vec<TableField>,
  iter: I,
}

impl<I> Iterator for MapSchema<I>
where
  I: Table,
{
  type Item = Result<Row, RowCellError>;
  fn next(&mut self) -> Option<Result<Row, RowCellError>> {
    let next = self.iter.next()?;
    let next = next.and_then(|row| {
      // pre-compute a mapping of the name of the column in the previous
      // schema to it's field
      // TODO :: do this once
      let prev_schema_lookup = {
        use std::collections::BTreeMap;
        let mut table: BTreeMap<&str, (&TableField, usize)> = BTreeMap::default();
        let mut offset = 0;
        for column in self.prev_schema.iter() {
          if let Some(name) = &column.name {
            table.insert(name, (column, offset));
          }
          offset += column.kind().size();
        }
        table
      };

      let mut next_row: Vec<OwnedRowCell> = Vec::with_capacity(self.schema.len());
      for column in self.schema.iter() {
        match &column.literal_value {
          Some(literal) => {
            let row_val = match literal {
              TableFieldLiteral::Blob(data) => OwnedRowCell::Blob(data.clone()),
              TableFieldLiteral::Str(data) => OwnedRowCell::Str {
                value: data.clone(),
                max_size: data.len() as u64,
              },
              TableFieldLiteral::Number(value) => OwnedRowCell::Number {
                value: *value,
                size: 8,
              },
            };
            // TODO :: This unwrap should be safe, but we need to
            // make the types better
            let row_val = row_val.coerce_to(column).unwrap();
            next_row.push(row_val);
            continue;
          }
          None => {}
        };
        match prev_schema_lookup.get(column.name) {
          Some()
        }
      }
      Ok(Row::from_cells(next_row)?)
    });
    Some(next)
  }
}

impl<I: Table> Table for MapSchema<I> {
  fn schema(&self) -> Vec<TableField> {
    self.schema.to_vec()
  }
}
