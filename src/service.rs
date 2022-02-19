use std::{
    fmt::Debug,
    sync::Arc,
    task::{Context, Poll},
};

use futures_core::ready;
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::PollSemaphore;
use tower::Service;

use super::{
    future::ResponseFuture,
    message::Message,
    worker::{Handle, Worker},
    BatchControl,
};

/// Allows batch processing of requests.
///
/// See the module documentation for more details.
#[derive(Debug)]
pub struct Batch<T, Request>
where
    T: Service<BatchControl<Request>>,
{
    // Note: this actually _is_ bounded, but rather than using Tokio's bounded
    // channel, we use Tokio's semaphore separately to implement the bound.
    tx: mpsc::UnboundedSender<Message<Request, T::Future>>,

    // When the buffer's channel is full, we want to exert backpressure in
    // `poll_ready`, so that callers such as load balancers could choose to call
    // another service rather than waiting for buffer capacity.
    //
    // Unfortunately, this can't be done easily using Tokio's bounded MPSC
    // channel, because it doesn't expose a polling-based interface, only an
    // `async fn ready`, which borrows the sender. Therefore, we implement our
    // own bounded MPSC on top of the unbounded channel, using a semaphore to
    // limit how many items are in the channel.
    semaphore: PollSemaphore,

    // The current semaphore permit, if one has been acquired.
    //
    // This is acquired in `poll_ready` and taken in `call`.
    permit: Option<OwnedSemaphorePermit>,
    handle: Handle,
}

impl<T, Request> Batch<T, Request>
where
    T: Service<BatchControl<Request>>,
    T::Error: Into<crate::BoxError>,
{
    /// Creates a new `Batch` wrapping `service`.
    ///
    /// The wrapper is responsible for telling the inner service when to flush a
    /// batch of requests.
    ///
    /// The default Tokio executor is used to run the given service, which means
    /// that this method must be called while on the Tokio runtime.
    pub fn new(service: T, size: usize, time: std::time::Duration) -> Self
    where
        T: Send + 'static,
        T::Future: Send,
        T::Error: Send + Sync,
        Request: Send + 'static,
    {
        let (service, worker) = Self::pair(service, size, time);
        tokio::spawn(worker);
        service
    }

    /// Creates a new `Batch` wrapping `service`, but returns the background worker.
    ///
    /// This is useful if you do not want to spawn directly onto the `tokio`
    /// runtime but instead want to use your own executor. This will return the
    /// `Batch` and the background `Worker` that you can then spawn.
    pub fn pair(service: T, size: usize, time: std::time::Duration) -> (Self, Worker<T, Request>)
    where
        T: Send + 'static,
        T::Future: Send,
        T::Error: Send + Sync,
        Request: Send + 'static,
    {
        // The semaphore bound limits the maximum number of concurrent requests
        // (specifically, requests which got a `Ready` from `poll_ready`, but haven't
        // used their semaphore reservation in a `call` yet).
        // We choose a bound that allows callers to check readiness for every item in
        // a batch, then actually submit those items.
        let (tx, rx) = mpsc::unbounded_channel();
        let bound = size;
        let semaphore = Arc::new(Semaphore::new(bound));

        let (handle, worker) = Worker::new(rx, service, size, time, &semaphore);

        let batch = Self {
            tx,
            semaphore: PollSemaphore::new(semaphore),
            permit: None,
            handle,
        };
        (batch, worker)
    }

    fn get_worker_error(&self) -> crate::BoxError {
        self.handle.get_error_on_closed()
    }
}

impl<T, Request> Service<Request> for Batch<T, Request>
where
    T: Service<BatchControl<Request>>,
    T::Error: Into<crate::BoxError>,
{
    // Our response is effectively the response of the service used by the Worker
    type Response = T::Response;
    type Error = crate::BoxError;
    type Future = ResponseFuture<T::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        tracing::debug!("checking if service is ready");

        // First, check if the worker is still alive.
        if self.tx.is_closed() {
            // If the inner service has errored, then we error here.
            return Poll::Ready(Err(self.get_worker_error()));
        }

        // Then, check if we've already acquired a permit.
        if self.permit.is_some() {
            // We've already reserved capacity to send a request. We're ready!
            return Poll::Ready(Ok(()));
        }

        // Finally, if we haven't already acquired a permit, poll the semaphore to acquire one. If
        // we acquire a permit, then there's enough buffer capacity to send a new request.
        // Otherwise, we need to wait for capacity.
        //
        // The current task must be scheduled for wakeup every time we return `Poll::Pending`. If
        // it returns Pending, the semaphore also schedules the task for wakeup when the next permit
        // is available.
        let permit =
            ready!(self.semaphore.poll_acquire(cx)).ok_or_else(|| self.get_worker_error())?;
        self.permit = Some(permit);

        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        tracing::debug!("sending request to batch worker");

        let _permit = self
            .permit
            .take()
            .expect("batch full; poll_ready must be called first");

        // Get the current Span so that we can explicitly propagate it to the worker
        // if we didn't do this, events on the worker related to this span wouldn't be counted
        // towards that span since the worker would have no way of entering it.
        let span = tracing::Span::current();

        // If we've made it here, then a semaphore permit has already been acquired, so we can
        // freely allocate a oneshot.
        let (tx, rx) = oneshot::channel();

        // The worker is in control of completing the request now.
        match self.tx.send(Message {
            request,
            span,
            tx,
            _permit,
        }) {
            Err(_) => ResponseFuture::failed(self.get_worker_error()),
            Ok(_) => ResponseFuture::new(rx),
        }
    }
}

impl<T, Request> Clone for Batch<T, Request>
where
    T: Service<BatchControl<Request>>,
{
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            semaphore: self.semaphore.clone(),
            handle: self.handle.clone(),

            // The new clone hasn't acquired a permit yet. It will when it's next polled ready.
            permit: None,
        }
    }
}
