use schema::{
  Field, FieldKind, OnDiskSchema, OwnedRowCell, Row, RowCell, RowCellError, SchemaField,
};

use parser::ColumnIdent;
use std::collections::BTreeMap;

pub trait RowReader {
  fn read_nth_row(&mut self, schema: &OnDiskSchema, index: u64) -> Result<Option<Row>, TableError>;
}

pub trait Table {
  fn schema(&self) -> Vec<TableField>;
  fn next_row(&mut self, disk: &mut dyn RowReader) -> Result<Option<Row>, TableError>;
  fn map_schema(
    self,
    next_schema: Vec<TableField>,
    alias_mapping: BTreeMap<ColumnIdent, &str>,
  ) -> MapSchema<Self>
  where
    Self: Sized,
  {
    MapSchema::new(self.schema(), next_schema, alias_mapping, self)
  }
  fn into_iter_cells<'a>(self, disk: &'a mut dyn RowReader) -> IntoIterCells<'a, Self>
  where
    Self: Sized,
  {
    IntoIterCells {
      schema: self.schema().to_vec(),
      d: disk,
      iter: self,
    }
  }
}

pub struct SchemaReader {
  schema: OnDiskSchema,
  current_row: u64,
}

impl SchemaReader {
  pub fn new(schema: OnDiskSchema) -> SchemaReader {
    SchemaReader {
      schema,
      current_row: 0,
    }
  }
}

impl Table for SchemaReader {
  fn schema(&self) -> Vec<TableField> {
    let table_name = self.schema.schema().name();
    self
      .schema
      .schema()
      .fields()
      .iter()
      .map(|schema_field| {
        TableField::new(
          Some(ColumnIdent {
            name: schema_field.name().to_string().into(),
            table: Some(table_name.to_string().into()),
          }),
          schema_field.kind().clone(),
          None,
        )
      })
      .collect()
  }
  fn next_row(&mut self, disk: &mut dyn RowReader) -> Result<Option<Row>, TableError> {
    let row = disk.read_nth_row(&self.schema, self.current_row)?;
    match row {
      Some(row) => {
        self.current_row += 1;
        Ok(Some(row))
      }
      None => Ok(None),
    }
  }
}

pub struct MultiTableIterator<A: Table, B: Table> {
  tables: (A, B),
}

impl<A: Table, B: Table> Table for MultiTableIterator<A, B> {
  fn schema(&self) -> Vec<TableField> {
    let mut buf = vec![];
    buf.append(&mut self.tables.0.schema());
    buf.append(&mut self.tables.1.schema());
    buf
  }
  fn next_row(&mut self, disk: &mut dyn RowReader) -> Result<Option<Row>, TableError> {
    let row_a = self.tables.0.next_row(disk)?;
    let row_b = self.tables.1.next_row(disk)?;
    let row_a = match row_a {
      Some(row) => row,
      None => return Ok(None),
    };
    let row_b = match row_b {
      Some(row) => row,
      None => return Ok(None),
    };

    let mut row_a = row_a.into_data();
    let mut row_b = row_b.into_data();
    let mut buf = vec![];
    buf.append(&mut row_a);
    buf.append(&mut row_b);

    Ok(Some(Row::from_data(buf)))
  }
}

pub struct IntoIterCells<'a, I> {
  iter: I,
  d: &'a mut dyn RowReader,
  schema: Vec<TableField>,
}

impl<'a, I: Table> Iterator for IntoIterCells<'a, I> {
  type Item = Result<Vec<OwnedRowCell>, TableError>;
  fn next(&mut self) -> Option<Self::Item> {
    let next = self.iter.next_row(self.d);
    match next {
      Ok(None) => None,
      Ok(Some(row)) => Some(row.into_cells(&self.schema).map_err(TableError::RowCell)),
      Err(err) => Some(Err(err)),
    }
  }
}

pub struct MapSchema<I> {
  prev_schema_lookup: BTreeMap<ColumnIdent, (TableField, usize)>,
  schema: Vec<TableField>,
  iter: I,
}

impl<I: Table> MapSchema<I> {
  fn new(
    prev_schema: Vec<TableField>,
    schema: Vec<TableField>,
    alias_mapping: BTreeMap<ColumnIdent, &str>,
    iter: I,
  ) -> Self {
    let prev_schema_lookup = {
      let mut table: BTreeMap<ColumnIdent, (TableField, usize)> = BTreeMap::default();
      let mut offset = 0;
      for field in prev_schema.into_iter() {
        let size = field.kind().size();
        // ok so alias_mapping goes original_column -> alias_name
        // prev_schema is the actual, physical table itself.
        if let Some(column) = &field.column {
          let real_name = alias_mapping
            .get(&column)
            .map(|string| ColumnIdent {
              name: string.to_string().into(),
              table: None,
            })
            .unwrap_or_else(|| column.clone());
          table.insert(real_name, (field, offset));
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

impl<I> Table for MapSchema<I>
where
  I: Table,
{
  fn next_row(&mut self, disk: &mut dyn RowReader) -> Result<Option<Row>, TableError> {
    let row = self.iter.next_row(disk)?;
    let row = match row {
      Some(row) => row,
      None => return Ok(None),
    };
    let mut next_row: Vec<OwnedRowCell> = Vec::with_capacity(self.schema.len());
    for field in self.schema.iter() {
      match &field.literal_value {
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
          let row_val = row_val.coerce_to(field).unwrap();
          next_row.push(row_val);
          continue;
        }
        None => {}
      };
      let column = match &field.column {
        Some(name) => name,
        None => return Err(TableError::Other(format!("Invalid schema"))),
      };
      match self.prev_schema_lookup.get(&column) {
        Some((prev_column, offset)) => {
          let data = RowCell::new(row.data(), prev_column, *offset)?;
          next_row.push(data.into());
        }
        None => return Err(TableError::Other(format!("Invalid schema"))),
      };
    }
    Ok(Some(Row::from_cells(next_row)?))
  }
  fn schema(&self) -> Vec<TableField> {
    self.schema.to_vec()
  }
}

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
  column: Option<ColumnIdent>,
  kind: FieldKind,
  literal_value: Option<TableFieldLiteral>,
}

impl TableField {
  pub fn name(&self) -> Option<&ColumnIdent> {
    self.column.as_ref()
  }
  pub fn new(
    column: Option<ColumnIdent>,
    kind: FieldKind,
    literal_value: Option<TableFieldLiteral>,
  ) -> TableField {
    TableField {
      column,
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
