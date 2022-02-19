#![allow(dead_code)]

//! The code below is borrowed from Tower's test suite.

use std::{
    fmt, future,
    task::{Context, Poll},
};
use tower::Service;
use tower_batch::BatchControl;

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
            "{} called outside expected span\n expected: {:?}\n  current: {:?}",
            func, self.span, current_span
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
