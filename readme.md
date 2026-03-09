# RuSQL

## A complete, from scratch database 

Key Highlights:
- Persistent B+Tree based key-value storage
- Copy-on-write architecture
- Memory-mapped file I/O 
- Free-list garbage collection
- Truncation logic
- Crash resilience through rollbacks
- Supports tables and secondary indices
- Concurrent transactions through version control (multi reader and writer)
- Hand rolled LRU cache for shared reads
- Hand rolled lexer and parser for accepting SQL queries
- Custom serialization and deserializtion

Supported Operating Systems:
- x86_64 Linux

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

## Queries
RUSQL follows a syntax cloesly resembling SQLite and should be familiar to most user.

Expressions allow for arithmetic (even with strings!), so `((2 * (10 + 1)) * 2)` in legitimate syntax.

Bracketed keywords denote optional clauses

---
#### Select

`SELECT column, ... FROM table (WHERE) column op expression (LIMIT) expression;`

use the special `*` wildcard operator to select every column!

#### Insert

`INSERT INTO table (column, ...) VALUES expression, ...;`

string values are denoted with double quotes like `"Alice"`

RUSQL rejects mismatching data types


#### Update

`UPDATE table SET column = expression, ... (WHERE) column operator expression, ... (LIMIT) expression;`

note: omitting both WHERE and LIMIT clauses sets every column to the given value!

RUSQL will reject attempts to overwrite primary key columns in ways that would leave them not unique.

#### Delete

`DELETE FROM table (WHERE) column operator expression, ... (LIMIT) expression;`

note: omitting both WHERE and LIMIT clauses deletes every entry from the table!

#### Create

`CREATE TABLE table (column = DATATYPE, ...);`

supported data types:
```
INT = 64 bit signed integer
STR = UTF8 encoded string
```

`CREATE INDEX index_name ON table FOR column;`

create a secondary index for faster lookups


#### Drop

`DROP TABLE table;`

deletes the entire table

`DROP INDEX index_name FROM table;`

deletes the index and all associated rows
### examples

```
SELECT col1, col2 FROM mytable WHERE col1 = ((2 * (10 + 1)) * 2), col2 = "hello" LIMIT -5 + 7;
INSERT INTO mytable (col1, col2) VALUES (2*2), "Hello";
UPDATE mytable SET col1 = "hello", col2 = 10 WHERE col2 > 10 LIMIT 5;
DELETE FROM mytable WHERE col1 = 1, col2 > 10, col3 <= "hello" LIMIT 10 - 2 ORDER col2;

CREATE TABLE mytable (col1 = INT, col2 = STR, col3 = STR);
CREATE INDEX myidx ON mytable FOR col1;

DROP TABLE mytable;
DROP INDEX myidx FROM mytable;
```

### currently not supported:
- joins
- multiple primary keys/secondary indices
