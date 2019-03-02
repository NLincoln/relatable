use crate::table::TableField;
use crate::table::{Table, TableError};
use crate::{Block, BlockDisk};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use log::debug;
use parser::ColumnIdent;
use parser::{Expr, ResultColumn};
use schema::{OnDiskSchema, Row, Schema};
use std::collections::BTreeMap;
use std::io::{self, Read, Seek, Write};

/// Convenience trait for read + write + seek
pub trait Disk: io::Read + io::Write + io::Seek {}
impl<T: io::Read + io::Write + io::Seek> Disk for T {}

#[derive(Debug)]
pub struct Database<T: Disk> {
  disk: T,
  meta: DatabaseMeta,
}

/// Basically a structure that holds all the information in the root block
#[derive(Debug)]
struct DatabaseMeta {
  /// The version of this database. Should be 1
  version: u8,

  /// The _exponent_ for the block size. So an 8 here would mean
  /// our block size is `2 ^ 8` = 256 bytes
  /// Default is 11 (2048 bytes)
  block_size_exp: u8,
  /// The number of blocks we have allocated
  num_allocated_blocks: u64,
  /// The offset of the schema block. Usually this will be
  /// the first block after the root block but it's configurable
  schema_block_offset: u64,
}

impl DatabaseMeta {
  fn new(version: u8, block_size_exp: u8) -> DatabaseMeta {
    // Weird dance here. We initialize the schema_block_offset to block_size,
    // despite the fact that we usually haven't made it yet. Bit spooky, bit dangerous
    // TODO :: Can we clean that up?
    DatabaseMeta {
      version,
      block_size_exp,
      num_allocated_blocks: 2, // 1 for the root block, 1 for the schema block
      schema_block_offset: 2u64.pow(block_size_exp as u32),
    }
  }

  fn block_size(&self) -> u64 {
    2u64.pow(self.block_size_exp as u32)
  }

  fn persist<D: Write + Seek>(&self, disk: &mut D) -> io::Result<()> {
    disk.seek(io::SeekFrom::Start(0))?;
    disk.write_u8(self.version)?;
    disk.write_u8(self.block_size_exp)?;
    disk.write_u64::<BigEndian>(self.num_allocated_blocks)?;
    disk.write_u64::<BigEndian>(self.schema_block_offset)?;
    Ok(())
  }

  fn from_disk<D: Read + Seek>(disk: &mut D) -> io::Result<DatabaseMeta> {
    disk.seek(io::SeekFrom::Start(0))?;
    let version = disk.read_u8()?;
    let block_size_exp = disk.read_u8()?;
    let num_allocated_blocks = disk.read_u64::<BigEndian>()?;
    let schema_block_offset = disk.read_u64::<BigEndian>()?;
    Ok(DatabaseMeta {
      version,
      block_size_exp,
      num_allocated_blocks,
      schema_block_offset,
    })
  }
}

#[derive(Debug)]
pub enum DatabaseError {
  TableNotFound { table_name: String },
  RowCellError(schema::RowCellError),
  Schema(schema::SchemaError),
  Io(io::Error),
  FieldError(schema::FieldError),
  TableError(TableError),
  AstError(parser::AstError),
  // basically a catch all because I'm lazy
  // todo -> make proper enumeriations for all
  // these cases
  Other(String),
}

impl From<io::Error> for DatabaseError {
  fn from(err: io::Error) -> Self {
    DatabaseError::Io(err)
  }
}

impl From<schema::SchemaError> for DatabaseError {
  fn from(err: schema::SchemaError) -> Self {
    DatabaseError::Schema(err)
  }
}

impl From<schema::RowCellError> for DatabaseError {
  fn from(err: schema::RowCellError) -> Self {
    DatabaseError::RowCellError(err)
  }
}

impl From<schema::FieldError> for DatabaseError {
  fn from(err: schema::FieldError) -> Self {
    DatabaseError::FieldError(err)
  }
}

impl From<TableError> for DatabaseError {
  fn from(err: TableError) -> Self {
    DatabaseError::TableError(err)
  }
}

#[derive(Debug)]
pub enum DatabaseQueryError {
  InternalError(DatabaseError),
  AstError(parser::AstError),
}

impl From<parser::AstError> for DatabaseError {
  fn from(err: parser::AstError) -> Self {
    DatabaseError::AstError(err)
  }
}

impl<T: Disk> Database<T> {
  pub fn execute_query<'disk, F>(
    &'disk mut self,
    query: String,
    mut f: F,
  ) -> Result<(), DatabaseError>
  where
    F: FnMut(Option<Vec<schema::OwnedRowCell>>) -> (),
  {
    let ast = parser::process_query(query)?;
    for statement in ast.into_iter() {
      match self.process_statement(statement)? {
        Some(mut result_iter) => {
          let schema = result_iter.schema();
          let mut get_next_row = || {
            let res = result_iter.current_row(self);
            result_iter.next_row(self)?;
            res
          };
          while let Some(row) = get_next_row().map_err(DatabaseError::TableError)? {
            (f)(Some(row.into_cells(&schema).map_err(DatabaseError::from)?))
          }
        }
        None => (f)(None),
      }
    }
    Ok(())
  }
  pub fn process_statement(
    &mut self,
    ast: parser::Statement,
  ) -> Result<Option<Box<dyn Table>>, DatabaseError> {
    use parser::Statement;
    match ast {
      Statement::CreateTable(create_table_statement) => {
        // does this table already exist?
        if let Ok(_) = self.get_table(create_table_statement.table_name.text()) {
          return Err(DatabaseError::Other(format!(
            "Could not create table {}: table with the same name already exists",
            create_table_statement.table_name.text()
          )));
        }

        let schema_fields = create_table_statement
          .column_defs
          .iter()
          .map(schema::SchemaField::from_column_def)
          .collect::<Result<Vec<_>, schema::FieldError>>()?;

        let schema = schema::Schema::from_fields(
          create_table_statement.table_name.text().to_string(),
          schema_fields,
        );
        self.create_table(schema)?;
        return Ok(None);
      }
      Statement::Insert(insert_statement) => {
        use parser::InsertStatementValues as Values;
        let disk_schema = self.get_table(insert_statement.table.text())?;
        let schema = &disk_schema.schema();

        if schema.fields().len() != insert_statement.columns.len() {
          return Err(DatabaseError::Other(format!("Could not insert into {}: Number of columns specified does not match number of columns in table.", insert_statement.table.text())));
        }
        let mut mapping: BTreeMap<usize, usize> = Default::default();
        'col: for (col_idx, column) in insert_statement.columns.iter().enumerate() {
          for (field_idx, field) in schema.fields().iter().enumerate() {
            if column.text() == field.name() {
              mapping.insert(field_idx, col_idx);
              continue 'col;
            }
          }
          return Err(DatabaseError::Other(format!(
            "Could not insert into {}: Column {} was not found in table",
            insert_statement.table.text(),
            column.text()
          )));
        }

        match &insert_statement.values {
          Values::SingleRow(row) => {
            self.insert_ast_row(schema, &row, &mapping)?;
            Ok(None)
          }
          Values::MultipleRows(rows) => {
            for row in rows.iter() {
              self.insert_ast_row(schema, &row, &mapping)?;
            }
            Ok(None)
          }
        }
      }
      Statement::Select(select_statement) => self.read_select_statement(select_statement).map(Some),
    }
  }

  fn transform_literal_value_to_field(
    literal_value: &parser::LiteralValue,
    alias: Option<&parser::Ident>,
  ) -> crate::table::TableField {
    use crate::table::TableField;
    use crate::table::TableFieldLiteral;
    use parser::LiteralValue;
    use schema::FieldKind;

    let alias = alias.map(|alias| parser::ColumnIdent {
      name: alias.clone(),
      table: None,
    });
    match literal_value {
      LiteralValue::NumericLiteral(num) => TableField::new(
        alias,
        FieldKind::Number(8),
        Some(TableFieldLiteral::Number(*num)),
      ),
      LiteralValue::StringLiteral(string) => TableField::new(
        alias,
        FieldKind::Str(string.len() as u64),
        Some(TableFieldLiteral::Str(string.to_string())),
      ),
      LiteralValue::BlobLiteral(blob) => TableField::new(
        alias,
        FieldKind::Blob(blob.len() as u64),
        // TODO :: handle this error but ugh I want to see this work!
        Some(TableFieldLiteral::Blob(hex::decode(blob).unwrap())),
      ),
    }
  }
  fn create_schema_mapping_for_select_statement<'alias>(
    &mut self,
    columns: &'alias [parser::ResultColumn],
    table: &OnDiskSchema,
    next_schema: &mut Vec<TableField>,
    alias_mapping: &mut BTreeMap<ColumnIdent, &'alias str>,
  ) -> Result<(), DatabaseError> {
    for column in columns.iter() {
      match column {
        ResultColumn::Asterisk => {
          for field in table.schema().fields().iter() {
            next_schema.push(TableField::new(
              Some(ColumnIdent {
                name: field.name().to_string().into(),
                table: Some(table.schema().name().to_string().into()),
              }),
              field.kind().clone(),
              None,
            ));
          }
        }
        ResultColumn::TableAsterisk(_table) => unimplemented!(),
        ResultColumn::Expr { value, alias } => match value {
          Expr::ColumnIdent(column_ident) => {
            use parser::Ident;
            let schema_column =
              table
                .schema()
                .field(column_ident.name.text())
                .ok_or(DatabaseError::Other(format!(
                  "Error: Could not find column {} in table",
                  column_ident.name
                )))?;

            let table_column_ident = ColumnIdent {
              name: column_ident.name.clone(),
              table: Some(
                column_ident
                  .table
                  .clone()
                  .unwrap_or(Ident::new(table.schema().name().to_string())),
              ),
            };
            if let Some(alias) = alias {
              alias_mapping.insert(table_column_ident.clone(), alias.text());
            }

            next_schema.push(TableField::new(
              Some(
                alias
                  .clone()
                  .map(|alias| ColumnIdent {
                    name: alias,
                    table: None,
                  })
                  .unwrap_or(table_column_ident.clone()),
              ),
              schema_column.kind().clone(),
              None,
            ));
          }
          Expr::LiteralValue(literal_value) => {
            next_schema.push(Self::transform_literal_value_to_field(
              literal_value,
              alias.as_ref(),
            ));
          }
        },
      };
    }
    Ok(())
  }

  fn read_select_statement(
    &mut self,
    select_statement: parser::SelectStatement,
  ) -> Result<Box<dyn Table>, DatabaseError> {
    match select_statement.tables {
      Some(tables) => {
        use crate::table::{MultiTableIterator, SchemaReader};
        let mut next_schema = vec![];
        let mut alias_mapping = BTreeMap::new();
        let mut table_readers = vec![];
        for table in tables.into_iter() {
          let table = self.get_table(table.text())?;
          self.create_schema_mapping_for_select_statement(
            &select_statement.columns,
            &table,
            &mut next_schema,
            &mut alias_mapping,
          )?;
          table_readers.push(SchemaReader::new(table));
        }
        let first_table = table_readers.remove(0);

        let iter: Box<dyn Table> = table_readers
          .into_iter()
          .fold(Box::new(first_table), |a, b| {
            Box::new(MultiTableIterator::new(a, Box::new(b)))
          });

        Ok(iter)
      }
      None => unimplemented!(),
    }
  }

  fn insert_ast_row(
    &mut self,
    schema: &schema::Schema,
    ast: &[parser::Expr],
    mapping: &BTreeMap<usize, usize>,
  ) -> Result<(), DatabaseError> {
    // We don't have defaults for columns (yet). Assert that the columns are the same length
    // at least.
    let mut row = vec![];
    for i in 0..schema.fields().len() {
      match schema::OwnedRowCell::from_ast_expr(&ast[mapping[&i]]) {
        Some(cell) => {
          row.push(cell);
        }
        None => {
          return Err(DatabaseError::Other(format!(
            "Could not insert into {}: Invalid Cell",
            schema.name()
          )));
        }
      }
    }

    self.add_row(schema.name(), row)?;
    Ok(())
  }
  pub fn get_table(&mut self, table_name: &str) -> Result<OnDiskSchema, DatabaseError> {
    self
      .schema()?
      .into_iter()
      .find(|owned_table| owned_table.schema().name() == table_name)
      .ok_or_else(|| DatabaseError::TableNotFound {
        table_name: table_name.to_string(),
      })
  }
  fn add_row(&mut self, table: &str, row: Vec<schema::OwnedRowCell>) -> Result<(), DatabaseError> {
    debug!("Adding row to table");
    let schema = self.get_table(table)?;
    // elements in the row must be coercible to the tables schema
    // otherwise Bad Things will happen
    if schema.schema().fields().len() != row.len() {
      return Err(DatabaseError::Other(format!("Could not insert into {}: The number of columns in the new row does not match the number of columns in the table", table)));
    }
    let mut valid_row = vec![];
    for (cell, field) in row.into_iter().zip(schema.schema().fields().iter()) {
      match cell.coerce_to(field) {
        Some(field) => valid_row.push(field),
        None => {
          return Err(DatabaseError::Other(format!(
            "Could not insert into {}: The data provided for column {} is invalid",
            table,
            field.name()
          )));
        }
      }
    }

    let mut data_blockdisk = BlockDisk::new(self, schema.data_block_offset())?;
    unsafe { schema::Row::insert_row(valid_row, &mut data_blockdisk, schema.schema())? };

    Ok(())
  }

  #[allow(dead_code)]
  fn read_table<'a>(
    &'a mut self,
    table_name: &str,
  ) -> Result<Vec<Vec<schema::OwnedRowCell>>, DatabaseError> {
    let table = self.get_table(table_name)?;
    let iter = crate::table::SchemaReader::new(table);
    let iter = iter.into_iter_cells(self);

    Ok(iter.collect::<Result<Vec<_>, TableError>>()?)
  }

  fn create_table(&mut self, schema: Schema) -> Result<(), DatabaseError> {
    // Alright so the first thing we need to do is go find the
    // schema table and add this entry to it.
    debug!("Creating Table");
    debug!(
      "=> We currently have {} blocks allocated",
      self.meta.num_allocated_blocks
    );
    let schema_block_offset = self.meta.schema_block_offset;

    self.disk.seek(io::SeekFrom::Start(schema_block_offset))?;
    let data_block = self.allocate_block()?;
    let data_block_offset = data_block.meta().offset();
    {
      let mut data_blockdisk = BlockDisk::from_block(self, data_block)?;
      debug!("Initializing data block, offset {}", data_block_offset);
      unsafe { schema::Row::init_table(&schema, &mut data_blockdisk)? };
    }

    let mut blockdisk = BlockDisk::new(self, schema_block_offset)?;
    let mut existing_schema = OnDiskSchema::read_tables(&mut blockdisk)?;
    blockdisk.seek(io::SeekFrom::Start(0))?;
    existing_schema.push(OnDiskSchema::new(data_block_offset, schema));

    OnDiskSchema::write_tables(&existing_schema, &mut blockdisk)?;

    Ok(())
  }

  pub fn schema(&mut self) -> Result<Vec<OnDiskSchema>, schema::SchemaError> {
    let schema_block_offset = self.meta.schema_block_offset;
    let mut reader = crate::BlockDisk::new(self, schema_block_offset)?;
    OnDiskSchema::read_tables(&mut reader)
  }

  /// Initializes a new database on the provided disk
  /// There should be no information on the provided disk
  pub fn new(mut disk: T) -> io::Result<Self> {
    // version 1, block size of 2048
    let block_size_exp = 6 as u8;
    let version = 1;
    let block_size = 2u64.pow(block_size_exp as u32);
    // create a new root block
    let root_block = Block::new(0, block_size);
    root_block.persist(&mut disk)?;

    let schema_block = Block::new(block_size, block_size);
    schema_block.persist(&mut disk)?;
    let meta = DatabaseMeta::new(version, block_size_exp);
    meta.persist(&mut disk)?;
    Ok(Database { disk, meta })
  }

  pub fn from_disk(mut disk: T) -> io::Result<Self> {
    let meta = DatabaseMeta::from_disk(&mut disk)?;

    Ok(Database { disk, meta })
  }
}

use crate::blockdisk::BlockAllocator;

impl<T: Disk> BlockAllocator for Database<T> {
  fn allocate_block(&mut self) -> io::Result<Block> {
    let next_block_offset = self.meta.num_allocated_blocks * self.meta.block_size();
    log::debug!("Allocating block at offset {}", next_block_offset);
    self.disk.seek(io::SeekFrom::Start(next_block_offset))?;
    let block = Block::new(next_block_offset, self.meta.block_size());
    self.meta.num_allocated_blocks += 1;
    self.meta.persist(&mut self.disk)?;
    block.persist(&mut self.disk)?;
    Ok(block)
  }
  fn read_block(&mut self, offset: u64) -> io::Result<Block> {
    log::debug!("Reading block at offset {}", offset);
    Block::from_disk(offset, self.meta.block_size(), &mut self.disk)
  }
  fn write_block(&mut self, block: &Block) -> io::Result<()> {
    log::debug!("Writing block at offset {}", block.meta().offset());
    block.persist(&mut self.disk).map(|_| ())
  }
}
use crate::table::RowReader;

impl<T: Disk> RowReader for Database<T> {
  fn read_nth_row(&mut self, schema: &OnDiskSchema, index: u64) -> Result<Option<Row>, TableError> {
    // TODO :: cache this because it's gonna be SLOOWWWWWW
    log::debug!("Reading row {} for table {}", index, schema.schema().name());
    let mut blockdisk = BlockDisk::new(self, schema.data_block_offset())?;

    blockdisk.seek(io::SeekFrom::Start(
      Row::sizeof_row_on_disk(schema.schema()) as u64 * index,
    ))?;

    let row = Row::from_schema(&mut blockdisk, schema.schema())?;
    if row.is_last_row() {
      log::debug!("row is last row!");
      Ok(None)
    } else {
      log::debug!("more rows to go!");
      Ok(Some(row))
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use schema::SchemaError;

  #[test]
  fn test_adding_rows() -> Result<(), DatabaseError> {
    use schema::{FieldKind, SchemaField};

    // Disk should have two blocks: one for the dbmeta and an empty schema block
    let mut database = Database::new(io::Cursor::new(vec![]))?;
    let schema = Schema::from_fields(
      "users".into(),
      vec![
        SchemaField::new(FieldKind::Number(8), "id".into())
          .map_err(|err| SchemaError::from(err))?,
        SchemaField::new(FieldKind::Str(20), "username".into())
          .map_err(|err| SchemaError::from(err))?,
      ],
    );
    use schema::OwnedRowCell;

    // Disk should have 3 blocks: dbmeta, schema block with one table, and a data block with one empty row
    database.create_table(schema.clone())?;
    let rows = vec![
      OwnedRowCell::Number { value: 1, size: 8 },
      OwnedRowCell::Str {
        value: "nlincoln".into(),
        max_size: 20,
      },
    ];
    let mut expected_rows = vec![];
    for _i in 0..100 {
      database.add_row("users", rows.clone())?;
      expected_rows.push(rows.clone());

      let all_rows = database
        .read_table("users")?
        .into_iter()
        .collect::<Vec<_>>();

      assert_eq!(all_rows, expected_rows);
    }

    Ok(())
  }
  #[test]
  fn test_adding_a_bunch_of_tables() -> Result<(), DatabaseError> {
    use schema::{FieldKind, SchemaField};
    let mut database = Database::new(io::Cursor::new(vec![]))?;
    let schema = Schema::from_fields(
      "the_name".into(),
      vec![
        SchemaField::new(FieldKind::Blob(10), "id".into())?,
        SchemaField::new(FieldKind::Blob(10), "id2".into())?,
        SchemaField::new(FieldKind::Blob(10), "id3".into())?,
        SchemaField::new(FieldKind::Blob(10), "id4".into())?,
        SchemaField::new(FieldKind::Blob(10), "id5".into())?,
      ],
    );
    let mut expected_tables = vec![];

    for _i in 0..100 {
      // at each iteration, add the table again. Then re-read the tables.
      // they should match
      database.create_table(schema.clone())?;
      expected_tables.push(schema.clone());
      // value of data_block_offset is an impl detail, so we compare
      // the underlying schemas instead
      let schemas = database
        .schema()?
        .into_iter()
        .map(|ondiskschema: OnDiskSchema| ondiskschema.schema().clone())
        .collect::<Vec<_>>();
      assert_eq!(schemas, expected_tables);
    }
    Ok(())
  }

}
