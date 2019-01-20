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
    let file = fs::OpenOptions::new()
      .read(true)
      .write(true)
      .truncate(true)
      .create_new(true)
      .open(filename)?;
    let database = Database::new(file)?;
    println!("Successfully created database");
    println!("{:?}", database);
    return Ok(());
  }
  let file = fs::OpenOptions::new()
    .read(true)
    .write(true)
    .truncate(false)
    .open(filename)?;
  let mut database = Database::from_disk(file)?;

  if op == "run-file" {
    if args.len() < 4 {
      panic!("Need the name of the sql file to read as the last arg");
    }

    let query = fs::read_to_string(&args[3])?;
    let query = parser::process_query(query).expect("Invalid SQL");

    for statement in query.into_iter() {
      let mut table = prettytable::Table::new();
      match database.process_statement(statement).unwrap() {
        Some(mut result_iter) => {
          let schema = result_iter.schema();

          {
            let schema = result_iter.schema();
            let mut cells = vec![];
            for field in schema.iter() {
              match field.name() {
                Some(name) => cells.push(prettytable::Cell::new(&name.to_string())),
                None => cells.push(prettytable::Cell::new("<unnamed>")),
              };
            }
            table.add_row(prettytable::Row::new(cells));
          };
          while let Some(row) = result_iter.next_row(&mut database).unwrap() {
            let row = row.into_cells(&schema).unwrap();
            table.add_row(prettytable::Row::new(
              row
                .into_iter()
                .map(|cell| prettytable::Cell::new(&format!("{}", cell.as_rowcell())))
                .collect(),
            ));
          }
          table.printstd();
        }
        None => {}
      }
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

      match database.execute_query(query, |row| {
        println!("{:?}", row);
      }) {
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
