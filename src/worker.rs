use std::{
    future::Future,
    ops::Add,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use futures_core::ready;
use tokio::{
    sync::{mpsc, Semaphore},
    time::{sleep, Sleep},
};
use tower::Service;

use super::{
    BatchControl,
    error::{Closed, ServiceError},
    message::Message,
};

/// Get the error out
#[derive(Debug)]
pub(crate) struct Handle {
    inner: Arc<Mutex<Option<ServiceError>>>,
}

pin_project_lite::pin_project! {
    /// Task that handles processing the buffer. This type should not be used
    /// directly, instead `Batch` requires an `Executor` that can accept this task.
    ///
    /// The struct is `pub` in the private module and the type is *not* re-exported
    /// as part of the public API. This is the "sealed" pattern to include "private"
    /// types in public traits that are not meant for consumers of the library to
    /// implement (only call).
    #[derive(Debug)]
    pub struct Worker<T, Request>
    where
        T: Service<BatchControl<Request>>,
        T::Error: Into<crate::BoxError>,
    {
        rx: mpsc::UnboundedReceiver<Message<Request, T::Future>>,
        service: T,
        max_size: usize,
        max_time: Duration,
        handle: Handle,
        current_message: Option<Message<Request, T::Future>>,
        close: Option<Weak<Semaphore>>,
        failed: Option<ServiceError>,
        finish: bool,
        batch_until: Option<Instant>,
        batch_size: usize,
        sleep: Option<Pin<Box<Sleep>>>
    }

    impl<T, Request> PinnedDrop for Worker<T, Request>
    where
        T: Service<BatchControl<Request>>,
        T::Error: Into<crate::BoxError>
    {
        fn drop(mut this: Pin<&mut Self>) {
            this.as_mut().close_semaphore();
        }
    }
}

// ===== impl Worker =====

impl<T, Request> Worker<T, Request>
where
    T: Service<BatchControl<Request>>,
    T::Error: Into<crate::BoxError>,
{
    /// Closes the buffer's semaphore if it is still open, waking any pending tasks.
    fn close_semaphore(&mut self) {
        if let Some(close) = self.close.take().as_ref().and_then(Weak::<Semaphore>::upgrade) {
            tracing::debug!("buffer closing; waking pending tasks");
            close.close();
        } else {
            tracing::trace!("buffer already closed");
        }
    }
}

impl<T, Request> Worker<T, Request>
where
    T: Service<BatchControl<Request>>,
    T::Error: Into<crate::BoxError>,
{
    pub(crate) fn new(
        rx: mpsc::UnboundedReceiver<Message<Request, T::Future>>,
        service: T,
        max_size: usize,
        max_time: Duration,
        semaphore: &Arc<Semaphore>,
    ) -> (Handle, Worker<T, Request>) {
        tracing::trace!("creating Batch worker");

        let handle = Handle {
            inner: Arc::new(Mutex::new(None)),
        };

        // The service and worker have a parent - child relationship, so we must
        // downgrade the Arc to Weak, to ensure a cycle between Arc pointers will
        // never be deallocated.
        let semaphore = Arc::downgrade(semaphore);
        let worker = Self {
            rx,
            service,
            max_size,
            max_time,

            handle: handle.clone(),
            close: Some(semaphore),
            failed: None,
            finish: false,
            current_message: None,

            batch_until: None,
            batch_size: 0,
            sleep: None,
        };

        (handle, worker)
    }

    fn failed(&mut self, error: crate::BoxError) {
        tracing::debug!({ %error }, "service failed");

        // The underlying service failed when we called `poll_ready` on it with the given `error`. We
        // need to communicate this to all the `Buffer` handles. To do so, we wrap up the error in
        // an `Arc`, send that `Arc<E>` to all pending requests, and store it so that subsequent
        // requests will also fail with the same error.

        // Note that we need to handle the case where some handle is concurrently trying to send us
        // a request. We need to make sure that *either* the send of the request fails *or* it
        // receives an error on the `oneshot` it constructed. Specifically, we want to avoid the
        // case where we send errors to all outstanding requests, and *then* the caller sends its
        // request. We do this by *first* exposing the error, *then* closing the channel used to
        // send more requests (so the client will see the error when the send fails), and *then*
        // sending the error to all outstanding requests.
        let error = ServiceError::new(error);

        let mut inner = self.handle.inner.lock().unwrap();

        if inner.is_some() {
            // Future::poll was called after we've already errored out!
            return;
        }

        *inner = Some(error.clone());
        drop(inner);

        self.rx.close();

        // Wake any tasks waiting on channel capacity.
        self.close_semaphore();

        // By closing the mpsc::Receiver, we know that that the run() loop will
        // drain all pending requests. We just need to make sure that any
        // requests that we receive before we've exhausted the receiver receive
        // the error:
        self.failed = Some(error);
    }

    /// Return the next queued Message that hasn't been canceled.
    ///
    /// If a `Message` is returned, the `bool` is true if this is the first time we received this
    /// message, and false otherwise (i.e., we tried to forward it to the backing service before).
    fn poll_next_msg(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<(Message<Request, T::Future>, bool)>> {
        tracing::trace!("worker polling for next message");

        if self.finish {
            // We've already received None and are shutting down
            return Poll::Ready(None);
        }

        // Pick any delayed request first
        if let Some(msg) = self.current_message.take() {
            // If the oneshot sender is closed, then the receiver is dropped,
            // and nobody cares about the response. If this is the case, we
            // should continue to the next request.
            if !msg.tx.is_closed() {
                tracing::trace!("resuming buffered request");
                return Poll::Ready(Some((msg, false)));
            }

            tracing::trace!("dropping cancelled buffered request");
        }

        // Get the next request
        while let Some(msg) = ready!(Pin::new(&mut self.rx).poll_recv(cx)) {
            if !msg.tx.is_closed() {
                tracing::trace!("processing new request");
                return Poll::Ready(Some((msg, true)));
            }
            // Otherwise, request is canceled, so pop the next one.
            tracing::trace!("dropping cancelled request");
        }

        Poll::Ready(None)
    }

    fn flush(&mut self, cx: &mut Context<'_>, reason: &str) -> Poll<()> {
        tracing::trace!(reason, message = "waiting for service readiness");
        match self.service.poll_ready(cx) {
            Poll::Ready(Ok(())) => {
                tracing::debug!(service.ready = true, reason, message = "flushing batch");

                let response = self.service.call(BatchControl::Flush);
                tokio::pin!(response);
                if let Err(e) = ready!(response.poll(cx)) {
                    self.failed(e.into());
                }

                tracing::trace!("preparing for next batch");
                self.batch_size = 0;
                self.batch_until = None;
                self.sleep = None;
            }
            Poll::Pending => {
                tracing::trace!(service.ready = false, reason, message = "delay flush");
                return Poll::Pending;
            }
            Poll::Ready(Err(e)) => {
                self.failed(e.into());
            }
        }

        Poll::Ready(())
    }
}

impl<T, Request> Future for Worker<T, Request>
where
    T: Service<BatchControl<Request>>,
    T::Error: Into<crate::BoxError>,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        tracing::trace!("polling Batch worker");

        if self.finish {
            return Poll::Ready(());
        }

        // The timer is started when the first entry of a new batch is
        // submitted, so that the batch latency of all entries is at most
        // self.max_time. However, we don't keep the timer running unless
        // there is a pending request to prevent wakeups on idle services.
        if let Some(ts) = self.batch_until {
            let now = Instant::now();
            if ts <= now {
                tracing::trace!("time expired, flushing");
                let _ = ready!(self.flush(cx, "time"));
            }
        }

        // Attempt to flush the batch again when a previous flush was delayed,
        // because the service was not available,
        if self.batch_size == self.max_size {
            let _ = ready!(self.flush(cx, "size"));
        }

        loop {
            match ready!(self.poll_next_msg(cx)) {
                Some((msg, first)) => {
                    let _guard = msg.span.enter();
                    if let Some(ref failed) = self.failed {
                        tracing::trace!("notifying caller about worker failure");
                        let _ = msg.tx.send(Err(failed.clone()));
                        continue;
                    }

                    tracing::trace!(
                        batch_size = &self.batch_size,
                        batch_time = ?self.batch_until.map(|time| time.duration_since(Instant::now())),
                        resumed = !first,
                        message = "worker received request"
                    );

                    // The first message in a new batch.
                    if self.batch_size == 0 {
                        // Set the batch timer
                        tracing::trace!("buffer empty; starting 'max time' wake-up timer");
                        self.sleep = Some(Box::pin(sleep(self.max_time)));
                        self.batch_until = Some(Instant::now().add(self.max_time));
                    }

                    // Wait for the service to be ready
                    tracing::trace!(resumed = !first, message = "waiting for service readiness");
                    match self.service.poll_ready(cx) {
                        Poll::Ready(Ok(())) => {
                            tracing::debug!(service.ready = true, message = "adding item");

                            let response = self.service.call(msg.request.into());
                            self.batch_size += 1;

                            // Send the response future back to the sender.
                            //
                            // An error means the request had been canceled in-between
                            // our calls, the response future will just be dropped.
                            tracing::trace!("returning response future");
                            let _ = msg.tx.send(Ok(response));

                            // TODO: Should we discard the failed futures?
                            if self.batch_size == self.max_size {
                                let _ = ready!(self.flush(cx, "size"));
                            }

                            if let Some(ref mut sleep) = self.sleep {
                                if Pin::new(sleep).poll(cx).is_ready() {
                                    tracing::trace!("max time reached; waking the worker");
                                }
                            }
                        }
                        Poll::Pending => {
                            tracing::trace!(service.ready = false, message = "delay item addition");
                            // Put out current message back in its slot.
                            drop(_guard);
                            self.current_message = Some(msg);
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(e)) => {
                            drop(_guard);
                            self.failed(e.into());
                            let _ = msg.tx.send(Err(self
                                .failed
                                .as_ref()
                                .expect("Worker::failed did not set self.failed?")
                                .clone()));
                        }
                    }
                }
                // Shutting down, no more requests _ever_.
                None => {
                    tracing::trace!("shutting down, no more requests _ever_");
                    self.finish = true;
                    return Poll::Ready(());
                }
            }
        }
    }
}

// ===== impl Handle =====

impl Handle {
    pub(crate) fn get_error_on_closed(&self) -> crate::BoxError {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .map(|svc_err| svc_err.clone().into())
            .unwrap_or_else(|| Closed::new().into())
    }
}

impl Clone for Handle {
    fn clone(&self) -> Self {
        Handle {
            inner: self.inner.clone(),
        }
    }
}
