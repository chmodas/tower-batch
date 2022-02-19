#![allow(clippy::type_complexity)]

//! A Tower middleware that provides a buffered mpsc for processing requests in batches.
//!
//! Writing data in bulk is a common technique for improving the efficiency of certain tasks.
//! `batch-tower` is a middleware that allows you to buffer requests for batch processing until
//! the buffer reaches a maximum size OR a maximum duration elapses.
//!
//! Clients enqueue requests by sending on the channel from any of the handles ([`Batch`]), and the
//! single service running elsewhere (usually spawned) receives and collects the requests wrapped
//! in [`Item(R)`](BatchControl::Item). Once the [`Batch`]) buffer is full or the maximum duration
//! elapses, the service is instructed to write the data with a [`Flush`](BatchControl::Flush)
//! request. Upon completion of the flush operation, the client's will receive a response with the
//! outcome.

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
