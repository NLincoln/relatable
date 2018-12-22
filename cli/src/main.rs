use std::{env, fs, io};

use schema::{Field, FieldKind, Schema};

fn main() -> Result<(), io::Error> {
  let args: Vec<_> = env::args().collect();
  if args.len() < 3 {
    panic!("Args are read|write <filename>");
  }
  let op = &args[1];
  let filename = &args[2];
  if op == "read" {
    let mut file = fs::OpenOptions::new().read(true).open(filename)?;
    let mut database = schema::Database::from_disk(&mut file)?;
    let schema = database.schema().unwrap();
    println!("Current Schema");

    println!("{:?}", schema);
  } else if op == "add" {
    let mut file = fs::OpenOptions::new()
      .read(true)
      .write(true)
      .truncate(false)
      .open(filename)?;

    let mut database = schema::Database::from_disk(&mut file)?;
    database
      .create_table(Schema::from_fields(
        "the_name".into(),
        vec![
          Field::new(FieldKind::Blob(80), "id".into()),
          Field::new(FieldKind::Number, "num".into()),
          Field::new(FieldKind::Blob(500), "store".into()),
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
    let database = schema::Database::new(&mut file)?;
    println!("Successfully created database");
    println!("{:?}", database);
  }

  Ok(())
}
