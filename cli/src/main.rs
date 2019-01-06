use db::Database;
use schema::{Field, FieldKind, Schema};
use std::{env, fs};

fn main() -> Result<(), schema::SchemaError> {
  env_logger::init();
  let args: Vec<_> = env::args().collect();
  if args.len() < 3 {
    panic!("Args are read|write <filename>");
  }
  let op = &args[1];
  let filename = &args[2];

  if op == "read" {
    let mut file = fs::OpenOptions::new().read(true).open(filename)?;
    let mut database = Database::from_disk(&mut file)?;
    let schema = database.schema().unwrap();
    println!("Current Schema");

    println!("{:?}", schema);
  } else if op == "add" {
    let mut file = fs::OpenOptions::new()
      .read(true)
      .write(true)
      .truncate(false)
      .open(filename)?;

    let mut database = Database::from_disk(&mut file)?;
    database
      .create_table(Schema::from_fields(
        "the_name".into(),
        vec![
          Field::new(FieldKind::Blob(10), "id".into())?,
          Field::new(FieldKind::Blob(11), "id1".into())?,
          Field::new(FieldKind::Blob(12), "id12".into())?,
          Field::new(FieldKind::Blob(13), "id123".into())?,
          Field::new(FieldKind::Blob(14), "id1234".into())?,
        ],
      ))
      .expect("Error creating table");
    println!("Successfully added table");
  } else if op == "create" {
    let mut file = fs::OpenOptions::new()
      .read(true)
      .write(true)
      .truncate(true)
      .create_new(true)
      .open(filename)?;
    let database = Database::new(&mut file)?;
    println!("Successfully created database");
    println!("{:?}", database);
  } else if op == "dbmeta" {
    let mut file = fs::OpenOptions::new().read(true).open(filename)?;
    let database = Database::from_disk(&mut file)?;
    println!("{:?}", database);
  } else if op == "init-table" {
    let mut file = fs::OpenOptions::new()
      .read(true)
      .write(true)
      .truncate(false)
      .open(filename)?;

    let mut database = Database::from_disk(&mut file)?;
    database
      .create_table(Schema::from_fields(
        "users".into(),
        vec![
          Field::new(FieldKind::Number(8), "id".into())?,
          Field::new(FieldKind::Str(20), "username".into())?,
        ],
      ))
      .unwrap();
  } else if op == "add-row" {
    let mut file = fs::OpenOptions::new()
      .read(true)
      .write(true)
      .truncate(false)
      .open(filename)?;

    let mut database = Database::from_disk(&mut file)?;
    database
      .add_row(
        "users",
        vec![
          schema::OwnedRowCell::Number { value: 1, size: 8 },
          schema::OwnedRowCell::Str {
            value: "nlincoln".into(),
            max_size: 20,
          },
        ],
      )
      .unwrap();
  } else if op == "read-table" {
    let mut file = fs::OpenOptions::new()
      .read(true)
      .write(true)
      .truncate(false)
      .open(filename)?;

    let mut database = Database::from_disk(&mut file)?;
    let rows = database.read_table("users").unwrap();
    let schema = database.get_table("users").unwrap();
    if rows.is_empty() {
      println!("No rows!");
    }
    for row in rows {
      println!("{:?}", row.as_cells(schema.schema()));
    }
  }

  Ok(())
}
