use crate::field::Field;
use crate::{FieldKind, OwnedRowCell, Row, RowCell, RowCellError};
use std::collections::BTreeMap;

#[derive(Debug)]
pub enum TableError {
  RowCell(RowCellError),
  Other(String),
  Io(std::io::Error),
}

impl From<RowCellError> for TableError {
  fn from(err: RowCellError) -> TableError {
    TableError::RowCell(err)
  }
}

impl From<std::io::Error> for TableError {
  fn from(err: std::io::Error) -> TableError {
    TableError::Io(err)
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

impl TableField {
  pub fn new(
    name: Option<String>,
    kind: FieldKind,
    literal_value: Option<TableFieldLiteral>,
  ) -> TableField {
    TableField {
      name,
      kind,
      literal_value,
    }
  }
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

pub trait Table: Iterator<Item = Result<Row, TableError>> {
  fn schema(&self) -> Vec<TableField>;
  fn map_schema(self, schema: Vec<TableField>) -> MapSchema<Self>
  where
    Self: Sized,
  {
    MapSchema::new(self.schema().to_vec(), schema, self)
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
  type Item = Result<Vec<OwnedRowCell>, TableError>;
  fn next(&mut self) -> Option<Self::Item> {
    let next = self.iter.next()?;
    Some(next.and_then(|row| {
      row
        .into_cells(&self.schema)
        .map_err(|err| TableError::from(err))
    }))
  }
}

pub struct MapSchema<I> {
  prev_schema_lookup: BTreeMap<String, (TableField, usize)>,
  schema: Vec<TableField>,
  iter: I,
}

impl<I: Table> MapSchema<I> {
  fn new(prev_schema: Vec<TableField>, schema: Vec<TableField>, iter: I) -> Self {
    let prev_schema_lookup = {
      let mut table: BTreeMap<String, (TableField, usize)> = BTreeMap::default();
      let mut offset = 0;
      for column in prev_schema.into_iter() {
        let size = column.kind().size();
        if let Some(name) = &column.name {
          table.insert(name.clone(), (column, offset));
        }
        offset += size;
      }
      table
    };
    MapSchema {
      prev_schema_lookup,
      schema,
      iter,
    }
  }
}

impl<I> Iterator for MapSchema<I>
where
  I: Table,
{
  type Item = Result<Row, TableError>;
  fn next(&mut self) -> Option<Result<Row, TableError>> {
    let next = self.iter.next()?;
    let next = next.and_then(|row| {
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
        let column_name = match &column.name {
          Some(name) => name,
          None => return Err(TableError::Other(format!("Invalid schema"))),
        };
        match self.prev_schema_lookup.get(column_name.as_str()) {
          Some((prev_column, offset)) => {
            let data = RowCell::new(row.data(), prev_column, *offset)?;
            next_row.push(data.into());
          }
          None => return Err(TableError::Other(format!("Invalid schema"))),
        };
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
