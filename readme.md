# RuSQL

A complete database written entirely from scratch in Rust!

## Highlights
- Persistent B+Tree based key-value storage
- Copy-on-write architecture
- Memory-mapped file I/O 
- Free-list garbage collection
- Crash resilience through rollbacks
- Supports tables and secondary indices
- Concurrent transactions through MVCC
- Hand rolled LRU cache for shared reads
- Hand rolled lexer and parser for accepting SQL queries
- Custom serialization and deserializtion


## Hello world!

```Rust
use rusql::{Database, Query};

struct User {
    name: String,
    age: u8,
}

fn main() -> Result<(), Box<dyn Error>> {
    let db = Database::open("my_database");

    let query = Query::new(
        r#"
            CREATE TABLE my_table (
                id = INT,
                name = STR,
                age = INT,
            );
        "#,
    );

    db.execute(query)?;

    let users = vec![
        User {
            name: "Alice".to_string(),
            age: 20,
        },
        User {
            name: "Bob".to_string(),
            age: 25,
        },
        User {
            name: "Charlie".to_string(),
            age: 30,
        },
    ];

    // insert some data!
    for (idx, user) in users.iter().enumerate() {
        let query = Query::new("INSERT INTO my_table (id, name, age) VALUES ?, ?, ?;")
            .bind(idx as i64)
            .bind(user.name.as_str())
            .bind(user.age);

        db.execute(query)?;
    }

    // we can execute queries directly
    let response = db.execute("SELECT * FROM my_table;".into())?;
    let rows = response.get_rows().unwrap();

    assert_eq!(rows[0][0], 0);
    assert_eq!(rows[0][1], "Alice");
    assert_eq!(rows[0][2], 20);

    assert_eq!(rows[1][0], 1);
    assert_eq!(rows[1][1], "Bob");
    assert_eq!(rows[1][2], 25);

    assert_eq!(rows[2][0], 2);
    assert_eq!(rows[2][1], "Charlie");
    assert_eq!(rows[2][2], 30);

    Ok(())
}
```
