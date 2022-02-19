use std::{
    future::Future,
    mem,
    ops::Add,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use futures_core::ready;
use tokio::{
    sync::{mpsc, Semaphore},
    time::{sleep_until, Sleep},
};
use tower::Service;
use tracing::{debug, trace};

use super::{
    error::{Closed, ServiceError},
    message::{Message, Tx},
    BatchControl,
};

/// Get the error out
#[derive(Debug)]
pub(crate) struct Handle {
    inner: Arc<Mutex<Option<ServiceError>>>,
}

/// Wrap `Service` channel for easier use through projections.
#[derive(Debug)]
struct Bridge<Fut, Request> {
    rx: mpsc::UnboundedReceiver<Message<Request, Fut>>,
    handle: Handle,
    current_message: Option<Message<Request, Fut>>,
    close: Option<Weak<Semaphore>>,
    failed: Option<ServiceError>,
}

#[derive(Debug)]
struct Lot<Fut> {
    max_size: usize,
    max_time: Duration,
    responses: Vec<(Tx<Fut>, Result<Fut, ServiceError>)>,
    time_elapses: Option<Pin<Box<Sleep>>>,
    time_elapsed: bool,
}

pin_project_lite::pin_project! {
    #[project = StateProj]
    #[derive(Debug)]
    enum State<Fut> {
        Collecting,
        Flushing {
            reason: Option<String>,
            #[pin]
            flush_fut: Option<Fut>,
        },
        Finished
    }
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
        service: T,
        bridge: Bridge<T::Future, Request>,
        lot: Lot<T::Future>,
        #[pin]
        state: State<T::Future>,
    }
}

// ===== impl Worker =====

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
        trace!("creating Batch worker");

        let handle = Handle {
            inner: Arc::new(Mutex::new(None)),
        };

        // The service and worker have a parent - child relationship, so we must
        // downgrade the Arc to Weak, to ensure a cycle between Arc pointers will
        // never be deallocated.
        let semaphore = Arc::downgrade(semaphore);
        let worker = Self {
            service,
            bridge: Bridge {
                rx,
                current_message: None,
                handle: handle.clone(),
                close: Some(semaphore),
                failed: None,
            },
            lot: Lot::new(max_size, max_time),
            state: State::Collecting,
        };

        (handle, worker)
    }
}

impl<T, Request> Future for Worker<T, Request>
where
    T: Service<BatchControl<Request>>,
    T::Error: Into<crate::BoxError>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        trace!("polling worker");

        let mut this = self.project();

        // Flush if the max wait time is reached.
        if let Poll::Ready(Some(())) = this.lot.poll_max_time(cx) {
            this.state.set(State::flushing("time".to_owned(), None))
        }

        loop {
            match this.state.as_mut().project() {
                StateProj::Collecting => {
                    match ready!(this.bridge.poll_next_msg(cx)) {
                        Some((msg, first)) => {
                            let _guard = msg.span.enter();

                            trace!(resumed = !first, message = "worker received request");

                            // Wait for the service to be ready
                            trace!(message = "waiting for service readiness");
                            match this.service.poll_ready(cx) {
                                Poll::Ready(Ok(())) => {
                                    debug!(service.ready = true, message = "adding item");

                                    let response = this.service.call(msg.request.into());
                                    this.lot.add((msg.tx, Ok(response)));

                                    // Flush if the batch is full.
                                    if this.lot.is_full() {
                                        this.state.set(State::flushing("size".to_owned(), None));
                                    }

                                    // Or flush if the max time has elapsed.
                                    if this.lot.poll_max_time(cx).is_ready() {
                                        this.state.set(State::flushing("time".to_owned(), None));
                                    }
                                }
                                Poll::Pending => {
                                    drop(_guard);
                                    debug!(service.ready = false, message = "delay item addition");
                                    this.bridge.return_msg(msg);
                                    return Poll::Pending;
                                }
                                Poll::Ready(Err(e)) => {
                                    drop(_guard);
                                    this.bridge.failed("item addition", e.into());
                                    if let Some(ref e) = this.bridge.failed {
                                        // Ensure the current caller is notified too.
                                        this.lot.add((msg.tx, Err(e.clone())));
                                        this.lot.notify(Some(e.clone()));
                                    }
                                }
                            }
                        }
                        None => {
                            trace!("shutting down, no more requests _ever_");
                            this.state.set(State::Finished);
                            return Poll::Ready(());
                        }
                    }
                }
                StateProj::Flushing { reason, flush_fut } => match flush_fut.as_pin_mut() {
                    None => {
                        trace!(
                            reason = reason.as_mut().unwrap().as_str(),
                            message = "waiting for service readiness"
                        );
                        match this.service.poll_ready(cx) {
                            Poll::Ready(Ok(())) => {
                                debug!(
                                    service.ready = true,
                                    reason = reason.as_mut().unwrap().as_str(),
                                    message = "flushing batch"
                                );
                                let response = this.service.call(BatchControl::Flush);
                                let reason = reason.take().expect("missing reason");
                                this.state.set(State::flushing(reason, Some(response)));
                            }
                            Poll::Pending => {
                                debug!(
                                    service.ready = false,
                                    reason = reason.as_mut().unwrap().as_str(),
                                    message = "delay flush"
                                );
                                return Poll::Pending;
                            }
                            Poll::Ready(Err(e)) => {
                                this.bridge.failed("flush", e.into());
                                if let Some(ref e) = this.bridge.failed {
                                    this.lot.notify(Some(e.clone()));
                                }
                            }
                        }
                    }
                    Some(future) => {
                        match ready!(future.poll(cx)) {
                            Ok(_) => {
                                debug!(reason = reason.as_mut().unwrap().as_str(), "batch flushed");
                                this.lot.notify(None);
                                this.state.set(State::Collecting)
                            },
                            Err(e) => {
                                this.bridge.failed("flush", e.into());
                                if let Some(ref e) = this.bridge.failed {
                                    this.lot.notify(Some(e.clone()));
                                }
                                this.state.set(State::Finished);
                                return Poll::Ready(());
                            }
                        }
                    }
                },
                StateProj::Finished => {
                    // We've already received None and are shutting down
                    return Poll::Ready(());
                }
            }
        }
    }
}

// ===== impl State =====

impl<Fut> State<Fut> {
    fn flushing(reason: String, f: Option<Fut>) -> Self {
        Self::Flushing {
            reason: Some(reason),
            flush_fut: f,
        }
    }
}

// ===== impl Bridge =====

impl<Fut, Request> Drop for Bridge<Fut, Request> {
    fn drop(&mut self) {
        self.close_semaphore()
    }
}

impl<Fut, Request> Bridge<Fut, Request> {
    /// Closes the buffer's semaphore if it is still open, waking any pending tasks.
    fn close_semaphore(&mut self) {
        if let Some(close) = self
            .close
            .take()
            .as_ref()
            .and_then(Weak::<Semaphore>::upgrade)
        {
            debug!("buffer closing; waking pending tasks");
            close.close();
        } else {
            trace!("buffer already closed");
        }
    }

    fn failed(&mut self, action: &str, error: crate::BoxError) {
        debug!(action,  %error , "service failed");

        // The underlying service failed when we called `poll_ready` on it with the given `error`.
        // We need to communicate this to all the `Buffer` handles. To do so, we wrap up the error
        // in an `Arc`, send that `Arc<E>` to all pending requests, and store it so that subsequent
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

        // By closing the mpsc::Receiver, we know that that the run() loop will drain all pending
        // requests. We just need to make sure that any requests that we receive before we've
        // exhausted the receiver receive the error:
        self.failed = Some(error);
    }

    /// Return the next queued Message that hasn't been canceled.
    ///
    /// If a `Message` is returned, the `bool` is true if this is the first time we received this
    /// message, and false otherwise (i.e., we tried to forward it to the backing service before).
    fn poll_next_msg(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<(Message<Request, Fut>, bool)>> {
        trace!("worker polling for next message");

        // Pick any delayed request first
        if let Some(msg) = self.current_message.take() {
            // If the oneshot sender is closed, then the receiver is dropped, and nobody cares about
            // the response. If this is the case, we should continue to the next request.
            if !msg.tx.is_closed() {
                trace!("resuming buffered request");
                return Poll::Ready(Some((msg, false)));
            }

            trace!("dropping cancelled buffered request");
        }

        // Get the next request
        while let Some(msg) = ready!(Pin::new(&mut self.rx).poll_recv(cx)) {
            if !msg.tx.is_closed() {
                trace!("processing new request");
                return Poll::Ready(Some((msg, true)));
            }

            // Otherwise, request is canceled, so pop the next one.
            trace!("dropping cancelled request");
        }

        Poll::Ready(None)
    }

    fn return_msg(&mut self, msg: Message<Request, Fut>) {
        self.current_message = Some(msg)
    }
}

// ===== impl Lot =====

impl<Fut> Lot<Fut> {
    fn new(max_size: usize, max_time: Duration) -> Self {
        Self {
            max_size,
            max_time,
            responses: Vec::with_capacity(max_size),
            time_elapses: None,
            time_elapsed: false,
        }
    }

    fn poll_max_time(&mut self, cx: &mut Context<'_>) -> Poll<Option<()>> {
        // When the Worker is polled and the time has elapsed, we return `Some` to let the Worker
        // know it's time to enter the Flushing state. Subsequent polls (e.g. by the Flush future)
        // will return None to prevent the Worker from getting stuck in an endless loop of entering
        // the Flushing state.
        if self.time_elapsed {
            return Poll::Ready(None);
        }

        if let Some(ref mut sleep) = self.time_elapses {
            if Pin::new(sleep).poll(cx).is_ready() {
                self.time_elapsed = true;
                return Poll::Ready(Some(()));
            }
        }

        Poll::Pending
    }

    fn is_full(&self) -> bool {
        self.responses.len() == self.max_size
    }

    fn add(&mut self, item: (Tx<Fut>, Result<Fut, ServiceError>)) {
        if self.responses.is_empty() {
            self.time_elapses = Some(Box::pin(sleep_until(
                Instant::now().add(self.max_time).into(),
            )));
        }
        self.responses.push(item);
    }

    fn notify(&mut self, err: Option<ServiceError>) {
        for (tx, response) in mem::replace(&mut self.responses, Vec::with_capacity(self.max_size)) {
            if let Some(ref response) = err {
                let _ = tx.send(Err(response.clone()));
            } else {
                let _ = tx.send(response);
            }
        }
        self.time_elapses = None;
        self.time_elapsed = false;
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
