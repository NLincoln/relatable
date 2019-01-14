use db::Database;
use std::{
  env, fs,
  io::{self, Write},
};

fn main() -> Result<(), schema::SchemaError> {
  env_logger::init();
  let args: Vec<_> = env::args().collect();
  if args.len() < 3 {
    panic!("Args are read|write <filename>");
  }
  let op = &args[1];
  let filename = &args[2];

  if op == "create" || op == "init" {
    let mut file = fs::OpenOptions::new()
      .read(true)
      .write(true)
      .truncate(true)
      .create_new(true)
      .open(filename)?;
    let database = Database::new(&mut file)?;
    println!("Successfully created database");
    println!("{:?}", database);
    return Ok(());
  }
  let mut file = fs::OpenOptions::new()
    .read(true)
    .write(true)
    .truncate(false)
    .open(filename)?;
  let mut database = Database::from_disk(&mut file)?;

  if op == "run-file" {
    if args.len() < 4 {
      panic!("Need the name of the sql file to read as the last arg");
    }

    let query = fs::read_to_string(&args[3])?;
    let results = database.execute_query(&query).unwrap();
    for result in results {
      println!("{:?}", result);
    }
  } else if op == "repl" {
    loop {
      print!("> ");
      io::stdout().flush()?;
      let query = {
        let mut buf = String::new();
        io::stdin().read_line(&mut buf)?;
        buf
      };
      if query == "exit" {
        break;
      }
      match database.execute_query(&query) {
        Ok(result) => println!("{:?}", result),
        Err(err) => println!("{:?}", err),
      };
    }
  } else if op == "schema" {
    let schema = database.schema().unwrap();
    println!("Current Schema");

    println!("{:?}", schema);
  } else if op == "dbmeta" {
    println!("{:?}", database);
  }

  Ok(())
}
