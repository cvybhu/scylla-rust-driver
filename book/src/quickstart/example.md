# Connecting and running a simple query

Now everything is ready to use the driver.
Here is a small example:
```rust
# extern crate scylla;
# extern crate tokio;
use scylla::{IntoTypedRows, Session, SessionBuilder};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Create a new Session which connects to node at 127.0.0.1:9042
    let session: Session = SessionBuilder::new()
        .known_node("127.0.0.1:9042")
        .build()
        .await?;

    // Create an example keyspace and table
    session
        .query(
            "CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = \
            {'class' : 'SimpleStrategy', 'replication_factor' : 1}",
            &[],
        )
        .await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.extab (a int primary key)",
            &[],
        )
        .await?;

    // Insert a value into the table
    let to_insert: i32 = 12345;
    session
        .query("INSERT INTO ks.extab (a) VALUES(?)", (to_insert,))
        .await?;

    // Query rows from the table and print them
    if let Some(rows) = session.query("SELECT a FROM ks.extab", &[]).await? {
        // Parse each row as a tuple containing single i32
        for row in rows.into_typed::<(i32,)>() {
            let read_row: (i32,) = row?;
            println!("Read a value from row: {}", read_row.0);
        }
    }

    Ok(())
}
```
