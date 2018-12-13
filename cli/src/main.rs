use std::{
  env, fs,
  io::{self, Read, Write},
};

use schema::{Field, Schema};

fn main() -> Result<(), io::Error> {
  let args: Vec<_> = env::args().collect();
  if args.len() < 3 {
    panic!("Args are read|write <filename>");
  }
  let op = &args[1];
  let filename = &args[2];
  if op == "read" {
    let mut file = fs::File::open(filename)?;
    let mut buf = vec![];
    file.read_to_end(&mut buf)?;

    let schema = Schema::from_persisted(&buf);

    println!("{:#?}", schema);
  } else {
    let mut file = fs::OpenOptions::new()
      .truncate(true)
      .write(true)
      .create(true)
      .open(filename)?;
    file.write_all(
      &Schema::from_fields(vec![Field::Blob(80), Field::Number, Field::Blob(500)]).persist(),
    )?;
  }

  Ok(())
}
