use std::{fmt, marker::PhantomData, time::Duration};

use tower::{layer::Layer, Service};

use super::{service::Batch, BatchControl};

/// A [`Layer`] that wraps an inner service with [`Batch`].
///
/// The background worker is spawned on the default Tokio executor, so
/// this layer can only be used on the Tokio runtime.
///
/// See the [module documentation](crate) for the full lifecycle and error
/// semantics.
pub struct BatchLayer<Request> {
    size: usize,
    time: Duration,
    _p: PhantomData<fn(Request)>,
}

impl<Request> BatchLayer<Request> {
    /// Creates a new [`BatchLayer`].
    ///
    /// * `size` – the maximum number of items per batch.
    /// * `time` – the maximum duration before a batch is flushed.
    #[must_use]
    pub fn new(size: usize, time: Duration) -> Self {
        Self {
            size,
            time,
            _p: PhantomData,
        }
    }
}

impl<S, Request> Layer<S> for BatchLayer<Request>
where
    S: Service<BatchControl<Request>> + Send + 'static,
    S::Future: Send,
    S::Error: Into<crate::BoxError> + Send + Sync,
    Request: Send + 'static,
{
    type Service = Batch<S, Request>;

    fn layer(&self, service: S) -> Self::Service {
        Batch::new(service, self.size, self.time)
    }
}

impl<Request> fmt::Debug for BatchLayer<Request> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("BatchLayer")
            .field("size", &self.size)
            .field("time", &self.time)
            .finish()
    }
}
