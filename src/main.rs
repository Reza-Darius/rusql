use std::error::Error;

use rusql::{Database, Parser};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().collect();

    if args.len() > 2 {
        return Err("too many arguments provided!".into());
    }

    let path = format!("{}.rdb", &args[1]);
    let db = Database::open(&path);

    println!("welcome to RUSQL!");
    println!("database opened: {path}");

    loop {
        let mut buf = String::new();
        std::io::stdin().read_line(&mut buf)?;

        match Parser::parse(&buf) {
            Ok(stmt) => match db.execute(stmt) {
                Ok(res) => println!("{}", res[0]),
                Err(e) => println!("Error! {e}"),
            },
            Err(e) => println!("Error! {e}"),
        }
    }
}
