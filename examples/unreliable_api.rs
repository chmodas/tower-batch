//! Batch-write events to a simulated unreliable remote API with retry and timeout.
//!
//! Demonstrates composing `tower-batch` with other Tower layers:
//!
//! ```text
//! Client → Batch → Retry → Timeout → UnreliableApi
//! ```
//!
//! - **Batch** collects individual events and flushes them in groups.
//! - **Retry** retries transient flush failures with exponential backoff.
//! - **Timeout** caps how long a single flush attempt can take.
//! - **`UnreliableApi`** simulates a remote endpoint that fails ~30% of the time.
//!
//! The retry policy only retries `Flush` errors – item buffering is an in-memory
//! operation that never fails. Because the Batch worker drives the inner service
//! sequentially (Item, Item, …, Flush), no new items arrive between a failed
//! Flush and its retry, so the shared pending buffer stays consistent.
//!
//! Run with: `cargo run --example unreliable_api`

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use tower::retry::Policy;
use tower::{Service, ServiceBuilder, ServiceExt};
use tower_batch::{BatchControl, BatchLayer, BoxError};

// ── Domain types ────────────────────────────────────────────────────────

/// A single event to be batch-written to the remote API.
#[allow(dead_code)]
#[derive(Clone)]
struct Event {
    payload: String,
}

// ── Retry policy ────────────────────────────────────────────────────────

/// Retry policy that retries flush failures with exponential backoff.
///
/// Only `Flush` calls are retried – `Item` calls are in-memory and infallible.
#[derive(Clone, Debug)]
struct FlushRetryPolicy {
    max_retries: usize,
    attempt: usize,
}

impl FlushRetryPolicy {
    fn new(max_retries: usize) -> Self {
        Self {
            max_retries,
            attempt: 0,
        }
    }
}

impl Policy<BatchControl<Event>, (), BoxError> for FlushRetryPolicy {
    type Future = Pin<Box<dyn Future<Output = ()> + Send>>;

    fn retry(
        &mut self,
        req: &mut BatchControl<Event>,
        result: &mut Result<(), BoxError>,
    ) -> Option<Self::Future> {
        // Only retry Flush failures.
        if !matches!(req, BatchControl::Flush) {
            return None;
        }

        if let Err(e) = result {
            if self.attempt < self.max_retries {
                self.attempt += 1;
                let backoff = Duration::from_millis(10 * (1 << self.attempt));
                tracing::warn!(
                    attempt = self.attempt,
                    max = self.max_retries,
                    backoff_ms = backoff.as_millis(),
                    error = %e,
                    "flush failed, retrying"
                );
                return Some(Box::pin(tokio::time::sleep(backoff)));
            }
            tracing::error!(
                attempts = self.attempt,
                error = %e,
                "flush failed, retries exhausted"
            );
        }

        // Reset attempt counter on success or when giving up, so the next
        // flush starts fresh.
        self.attempt = 0;
        None
    }

    fn clone_request(&mut self, req: &BatchControl<Event>) -> Option<BatchControl<Event>> {
        match req {
            BatchControl::Item(event) => Some(BatchControl::Item(event.clone())),
            BatchControl::Flush => Some(BatchControl::Flush),
        }
    }
}

// ── Simulated unreliable API service ────────────────────────────────────

/// Simulates a remote bulk-write endpoint that occasionally fails.
///
/// Pending items are stored behind an `Arc<Mutex<_>>` so that they survive
/// a failed flush – the Retry layer replays the Flush call, and the items
/// are still there.
#[derive(Clone)]
struct UnreliableApi {
    pending: Arc<Mutex<Vec<Event>>>,
    flush_count: Arc<AtomicUsize>,
    written: Arc<AtomicUsize>,
}

impl UnreliableApi {
    fn new() -> Self {
        Self {
            pending: Arc::new(Mutex::new(Vec::new())),
            flush_count: Arc::new(AtomicUsize::new(0)),
            written: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Service<BatchControl<Event>> for UnreliableApi {
    type Response = ();
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: BatchControl<Event>) -> Self::Future {
        match req {
            BatchControl::Item(event) => {
                self.pending.lock().unwrap().push(event);
                Box::pin(std::future::ready(Ok(())))
            }
            BatchControl::Flush => {
                let pending = self.pending.clone();
                let flush_count = self.flush_count.clone();
                let written = self.written.clone();

                Box::pin(async move {
                    // Simulate network latency.
                    tokio::time::sleep(Duration::from_millis(1)).await;

                    let n = flush_count.fetch_add(1, Ordering::SeqCst);

                    // Fail every 3rd flush to simulate transient errors.
                    if n % 3 == 1 {
                        let count = pending.lock().unwrap().len();
                        return Err(format!(
                            "simulated transient error on flush attempt #{n} ({count} events)"
                        )
                        .into());
                    }

                    // Success – drain the buffer.
                    let events = std::mem::take(&mut *pending.lock().unwrap());
                    let count = events.len();
                    written.fetch_add(count, Ordering::SeqCst);
                    tracing::info!(flush = n, events = count, "bulk write succeeded");
                    Ok(())
                })
            }
        }
    }
}

// ── Main ────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let api = UnreliableApi::new();
    let written = api.written.clone();

    // Build the full stack using ServiceBuilder:
    //   Client → Batch → Retry → Timeout → UnreliableApi
    //
    // ServiceBuilder applies layers outside-in, so BatchLayer is listed
    // first (outermost, what the client sees) and .service(api) is last.
    let batch = ServiceBuilder::new()
        .layer(BatchLayer::new(10, Duration::from_millis(50)))
        .retry(FlushRetryPolicy::new(3))
        .timeout(Duration::from_secs(5))
        .service(api);

    // Spawn 4 tasks, each sending 50 events.
    let mut handles = Vec::new();
    for task_id in 0u64..4 {
        let mut svc = batch.clone();
        handles.push(tokio::spawn(async move {
            for i in 0u64..50 {
                svc.ready().await.unwrap();
                svc.call(Event {
                    payload: format!("task={task_id} seq={i}"),
                })
                .await
                .unwrap();
            }
            tracing::info!(task_id, "task finished sending");
        }));
    }

    for handle in handles {
        handle.await?;
    }

    // Drop the last Batch handle so the worker knows no more requests are
    // coming, then give it time to flush.
    drop(batch);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let total = written.load(Ordering::SeqCst);
    tracing::info!(total, "all events written");
    assert_eq!(total, 200, "expected 200 events written");

    Ok(())
}
