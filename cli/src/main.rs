use std::{
  env, fs,
  io::{self, Read, Write},
};

use schema::{Field, FieldKind, Schema};

fn main() -> Result<(), io::Error> {
  let args: Vec<_> = env::args().collect();
  if args.len() < 3 {
    panic!("Args are read|write <filename>");
  }
  let op = &args[1];
  let filename = &args[2];
  if op == "read" {
    let mut file = fs::File::open(filename)?;

    let schema = Schema::from_persisted(&mut file);

    println!("{:#?}", schema);
  } else {
    let mut file = fs::OpenOptions::new()
      .truncate(true)
      .write(true)
      .create(true)
      .open(filename)?;
    Schema::from_fields(vec![
      Field::new(FieldKind::Blob(80), "id".into()),
      Field::new(FieldKind::Number, "num".into()),
      Field::new(FieldKind::Blob(500), "store".into()),
    ])
    .persist(&mut file)?;
  }

  Ok(())
}
