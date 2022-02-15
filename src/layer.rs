use std::{fmt, marker::PhantomData, time::Duration};

use tower::{layer::Layer, Service};

use super::{service::Batch, BatchControl};

/// Adds a layer performing batch processing of requests.
///
/// The default Tokio executor is used to run the given service,
/// which means that this layer can only be used on the Tokio runtime.
///
/// See the module documentation for more details.
pub struct BatchLayer<Request> {
    size: usize,
    time: Duration,
    _p: PhantomData<fn(Request)>,
}

impl<Request> BatchLayer<Request> {
    /// Creates a new [`BatchLayer`].
    ///
    /// The wrapper is responsible for telling the inner service when to flush a batch of requests.
    /// Two parameters control this policy:
    ///
    /// * `size` gives the maximum number of items per batch.
    /// * `time` gives the maximum duration before a batch is flushed.
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
        f.debug_struct("BufferLayer")
            .field("size", &self.size)
            .field("time", &self.time)
            .finish()
    }
}
