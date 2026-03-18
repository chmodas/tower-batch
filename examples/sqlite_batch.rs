//! Batch-insert rows into an in-memory SQLite database using the rarray virtual table.
//!
//! Run with: `cargo run --example sqlite_batch`

use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use rusqlite::vtab::array::load_module;
use rusqlite::Connection;
use tower::Service;
use tower_batch::{Batch, BatchControl, BoxError};

struct InsertRow {
    name: String,
    value: i64,
}

struct SqliteBatchService {
    conn: Arc<Mutex<Connection>>,
    pending: Vec<InsertRow>,
}

impl Service<BatchControl<InsertRow>> for SqliteBatchService {
    type Response = ();
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: BatchControl<InsertRow>) -> Self::Future {
        match req {
            BatchControl::Item(row) => {
                self.pending.push(row);
                Box::pin(std::future::ready(Ok(())))
            }
            BatchControl::Flush => {
                let rows = mem::take(&mut self.pending);
                let conn = Arc::clone(&self.conn);

                Box::pin(async move {
                    tokio::task::spawn_blocking(move || {
                        let conn = conn
                            .lock()
                            .map_err(|e| -> BoxError { e.to_string().into() })?;
                        let tx = conn.unchecked_transaction()?;

                        let names: Vec<rusqlite::types::Value> = rows
                            .iter()
                            .map(|r| rusqlite::types::Value::Text(r.name.clone()))
                            .collect();
                        let values: Vec<rusqlite::types::Value> = rows
                            .iter()
                            .map(|r| rusqlite::types::Value::Integer(r.value))
                            .collect();

                        // Rc is required here: rusqlite's rarray only implements ToSql for Rc<Vec<Value>>.
                        // This is safe because the Rc never leaves the spawn_blocking closure.
                        let names = Rc::new(names);
                        let values = Rc::new(values);

                        conn.execute(
                            "INSERT INTO data(name, value) \
                             SELECT n.value, v.value \
                             FROM rarray(?1) AS n \
                             JOIN rarray(?2) AS v ON n.rowid = v.rowid",
                            rusqlite::params![names, values],
                        )?;

                        tx.commit()?;
                        Ok::<(), BoxError>(())
                    })
                    .await?
                })
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE data (name TEXT NOT NULL, value INTEGER NOT NULL)")?;
    load_module(&conn)?;

    let conn = Arc::new(Mutex::new(conn));

    let service = SqliteBatchService {
        conn: Arc::clone(&conn),
        pending: Vec::new(),
    };

    let batch = Batch::new(service, 50, Duration::from_millis(100));

    let mut handles = Vec::new();
    for task_id in 0..4 {
        let mut batch = batch.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..50 {
                tower::ServiceExt::ready(&mut batch).await.unwrap();
                batch
                    .call(InsertRow {
                        name: format!("task{task_id}_row{i}"),
                        value: (task_id * 50 + i) as i64,
                    })
                    .await
                    .unwrap();
            }
        }));
    }

    for handle in handles {
        handle.await?;
    }

    // Drop the last Batch handle so the worker knows no more requests are coming,
    // then give it time to flush. In production you may want a more robust shutdown
    // mechanism.
    drop(batch);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let count: i64 = conn
        .lock()
        .map_err(|e| -> BoxError { e.to_string().into() })?
        .query_row("SELECT COUNT(*) FROM data", [], |row| row.get(0))?;

    println!("Inserted {count} rows (expected 200)");
    assert_eq!(count, 200);

    Ok(())
}
