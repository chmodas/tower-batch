#![allow(clippy::type_complexity)]

//! A Tower middleware that buffers requests and flushes them in batches.
//!
//! Writing data in bulk is a common technique for improving the efficiency of
//! certain tasks – databases, message brokers, object stores, etc. `tower-batch`
//! collects individual requests and flushes them as a group when the buffer
//! reaches a maximum size **or** a maximum duration elapses.
//!
//! # Inner service contract
//!
//! Your inner service must implement `Service<BatchControl<R>>` where `R` is
//! your request type. The middleware sends two kinds of calls:
//!
//! - [`BatchControl::Item(request)`](BatchControl::Item) – buffer this request.
//! - [`BatchControl::Flush`] – process the buffered items and return the result.
//!
//! # How the worker operates
//!
//! [`Batch::new`] (or [`BatchLayer`]) spawns a background worker that owns the
//! inner service. The worker cycles through three states:
//!
//! 1. **Collecting** – the worker pulls requests from the channel and forwards
//!    each one to the inner service as a [`BatchControl::Item`]. A timer starts
//!    when the first item of a new batch arrives.
//! 2. **Flushing** – triggered when the batch reaches `max_size` items or the
//!    `max_time` duration elapses. The worker calls the inner service with
//!    [`BatchControl::Flush`]. Once the flush completes, all callers in the
//!    batch receive the outcome and the worker returns to collecting.
//! 3. **Finished** – the worker shuts down, either because all [`Batch`] handles
//!    were dropped (no more requests possible) or because the inner service
//!    returned an error.
//!
//! # Backpressure
//!
//! [`Batch`] handles are cheap to clone – each clone shares the same worker.
//! Backpressure is enforced via a semaphore with `max_size` permits: once
//! `max_size` callers have received `Ready` from [`poll_ready`](tower::Service::poll_ready)
//! without yet calling [`call`](tower::Service::call), subsequent `poll_ready`
//! calls will return `Pending` until capacity is freed.
//!
//! # Errors
//!
//! Callers receive one of two error types through the [`BoxError`] returned by
//! [`ResponseFuture`](future::ResponseFuture):
//!
//! - [`error::ServiceError`] – the inner service returned an error, either
//!   during an item call or during a flush. The worker terminates and all
//!   pending callers in the current batch receive this error. The original
//!   error is accessible via [`source()`](std::error::Error::source).
//!
//! - [`error::Closed`] – the worker shut down before the caller's request
//!   could be completed. This happens when all [`Batch`] handles are dropped
//!   while items are still collecting (the batch was never flushed), or when
//!   the worker is dropped for any other reason. Callers should treat this as
//!   meaning their request was **NOT** processed.

/// Export tower's alias for a type-erased error type.
pub use tower::BoxError;

pub use self::layer::BatchLayer;
pub use self::service::Batch;

pub mod error;
pub mod future;
mod layer;
mod message;
mod service;
mod worker;

/// Signaling mechanism for services that allow processing in batches.
#[derive(Debug, Eq, PartialEq)]
pub enum BatchControl<R> {
    /// Collect a new batch item.
    Item(R),

    /// The current batch should be processed.
    Flush,
}

impl<R> From<R> for BatchControl<R> {
    fn from(req: R) -> BatchControl<R> {
        BatchControl::Item(req)
    }
}
