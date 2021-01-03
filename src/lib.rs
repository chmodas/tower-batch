#![allow(clippy::type_complexity)]

//! Tower middleware for batch request processing

/// Export tower's alias for a type-erased error type.
pub use tower::BoxError;

pub use self::service::Batch;

pub mod error;
pub mod future;
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
