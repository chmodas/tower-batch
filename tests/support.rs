#![allow(dead_code, clippy::missing_panics_doc)]

//! The code below is borrowed from Tower's test suite.

use std::{
    fmt,
    future::{self, Future},
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::Duration,
};
use tower::Service;
use tower_batch::{BatchControl, BoxError};

#[must_use]
pub fn trace_init() -> tracing::subscriber::DefaultGuard {
    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::TRACE)
        .with_thread_names(true)
        .finish();
    tracing::subscriber::set_default(subscriber)
}

#[derive(Clone, Debug)]
pub struct AssertSpanSvc {
    span: tracing::Span,
}

pub struct AssertSpanError(String);

impl fmt::Debug for AssertSpanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl fmt::Display for AssertSpanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for AssertSpanError {}

impl AssertSpanSvc {
    #[must_use]
    pub fn new(span: tracing::Span) -> Self {
        Self { span }
    }

    /// Verifies the Service propagates the current Span to the Worker.
    ///
    // Get the current Span so that we can explicitly propagate it to the worker if we didn't do
    // this, events on the worker related to this span wouldn't be counted towards that span since
    // the worker would have no way of entering it.
    fn check(&self, func: &str) -> Result<(), AssertSpanError> {
        let current_span = tracing::Span::current();
        tracing::debug!(?current_span, ?self.span, %func);
        if current_span == self.span {
            return Ok(());
        }

        Err(AssertSpanError(format!(
            "{func} called outside expected span\n expected: {span:?}\n  current: {current_span:?}",
            span = self.span,
        )))
    }
}

impl Service<BatchControl<()>> for AssertSpanSvc {
    type Response = ();
    type Error = AssertSpanError;
    type Future = future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: BatchControl<()>) -> Self::Future {
        if req == BatchControl::Flush {
            return future::ready(Ok(()));
        }
        future::ready(self.check("call"))
    }
}

// ===== Aggregator =====

#[derive(Clone)]
pub struct Aggregator<T> {
    items: Arc<Mutex<Vec<Vec<T>>>>,
    current: Arc<AtomicUsize>,
}

impl<T> Default for Aggregator<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Aggregator<T> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            items: Arc::new(Mutex::new(Vec::new())),
            current: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[must_use]
    pub fn batch_has_size(&self, index: usize, size: usize) -> bool {
        if index == self.current.load(Ordering::Acquire) {
            return false;
        }
        let items = &self.items.lock().unwrap();
        items.get(index).is_some_and(|v| v.len() == size)
    }

    #[must_use]
    pub fn batch_items(&self, index: usize) -> Option<Vec<T>>
    where
        T: Clone,
    {
        if index == self.current.load(Ordering::Acquire) {
            return None;
        }
        let items = self.items.lock().unwrap();
        items.get(index).cloned()
    }

    #[must_use]
    pub fn all_items_flat(&self) -> Vec<T>
    where
        T: Clone,
    {
        let items = self.items.lock().unwrap();
        let current = self.current.load(Ordering::Acquire);
        items.iter().take(current).flatten().cloned().collect()
    }

    #[must_use]
    pub fn completed_batches(&self) -> Vec<Vec<T>>
    where
        T: Clone,
    {
        let items = self.items.lock().unwrap();
        let current = self.current.load(Ordering::Acquire);
        items.iter().take(current).cloned().collect()
    }
}

impl<T> Service<BatchControl<T>> for Aggregator<T>
where
    T: fmt::Debug,
{
    type Response = ();
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + Sync + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: BatchControl<T>) -> Self::Future {
        match req {
            BatchControl::Item(item) => {
                let mut items = self.items.lock().unwrap();
                match items.get_mut(self.current.load(Ordering::Acquire)) {
                    None => {
                        items.push(vec![item]);
                    }
                    Some(v) => {
                        v.push(item);
                    }
                }
            }
            BatchControl::Flush => {
                self.current.fetch_add(1, Ordering::SeqCst);
                return Box::pin(async {
                    tracing::info!("sleeping ...");
                    async {
                        // Simulate some activity to catch any flushing issues
                        tokio::time::sleep(Duration::from_nanos(5)).await;
                    }
                    .await;
                    tracing::info!("awaking ...");
                    Ok(())
                });
            }
        }

        Box::pin(futures::future::ready(Ok(())))
    }
}
