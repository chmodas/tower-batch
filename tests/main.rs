use std::{
    fmt::Debug,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering}, Mutex,
    },
    task::{Context, Poll},
    time::Duration,
};

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio_test::{assert_pending, assert_ready, assert_ready_err, assert_ready_ok, task};
use tower::{Service, ServiceExt};
use tower_test::{assert_request_eq, mock::{self, Mock}};

use tower_batch::{Batch, BatchControl, BoxError, error};

mod support;

#[derive(Clone)]
struct Aggregator<T> {
    items: Arc<Mutex<Vec<Vec<T>>>>,
    current: Arc<AtomicUsize>,
}

impl<T> Aggregator<T> {
    pub fn new() -> Self {
        Self {
            items: Arc::new(Mutex::new(Vec::new())),
            current: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn batch_has_size(&self, index: usize, size: usize) -> bool {
        if index == self.current.load(Ordering::Acquire) {
            return false;
        }
        let items = &self.items.lock().unwrap();
        items.get(index).map(|v| v.len() == size).unwrap_or(false)
    }
}

impl<T> Service<BatchControl<T>> for Aggregator<T>
    where
        T: Debug,
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
                self.current
                    .fetch_add(self.current.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
            }
        }

        Box::pin(futures::future::ready(Ok(())))
    }
}

#[tokio::test]
async fn batch_flushes_on_max_size() -> Result<(), BoxError> {
    let _guard = support::trace_init();

    // Use a very long max_latency and a short timeout to check that
    // flushing is happening based on hitting max_items.
    let aggregator: Aggregator<u32> = Aggregator::new();
    let mut batch = Batch::new(aggregator.clone(), 10, Duration::from_secs(1));

    let mut results = FuturesUnordered::new();

    for i in 0..10 {
        let span = tracing::trace_span!("msg", i);

        batch.ready().await?;
        results.push(span.in_scope(|| batch.call(i)));
    }

    while let Some(Ok(_)) = results.next().await {}

    assert!(aggregator.batch_has_size(0, 10));

    Ok(())
}

#[tokio::test]
async fn batch_flushes_on_elapsed_time() -> Result<(), BoxError> {
    let _guard = support::trace_init();

    // Use a very high max_items and a short timeout to check that
    // flushing is happening based on hitting max_latency.
    let aggregator: Aggregator<u32> = Aggregator::new();
    let mut batch = Batch::new(aggregator.clone(), 100, Duration::from_millis(200));

    let mut results = FuturesUnordered::new();

    for i in 0..10 {
        let span = tracing::trace_span!("msg", i);

        batch.ready().await?;
        results.push(span.in_scope(|| batch.call(i)));
    }

    while let Some(Ok(_)) = results.next().await {}

    // Give it enough time to finish
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert!(aggregator.batch_has_size(0, 10));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clears_canceled_requests() {
    let _guard = support::trace_init();

    let (mut service, mut handle) = mock::spawn_with(|s: Mock<BatchControl<&str>, &str>| {
        let (svc, worker) = Batch::pair(s, 10, Duration::from_secs(1));

        tokio::spawn(async move {
            let _guard = support::trace_init();

            let mut fut = tokio_test::task::spawn(worker);
            while fut.poll().is_pending() {}
        });

        svc
    });

    handle.allow(1);

    assert_ready_ok!(service.poll_ready());
    let mut res1 = task::spawn(service.call("hello"));

    let send_response1 = assert_request_eq!(handle, BatchControl::from("hello"));

    // don't respond yet, new requests will get buffered
    assert_ready_ok!(service.poll_ready());
    let res2 = task::spawn(service.call("hello2"));

    assert_pending!(handle.poll_request());

    assert_ready_ok!(service.poll_ready());
    let mut res3 = task::spawn(service.call("hello3"));

    drop(res2);

    send_response1.send_response("world");

    // Let worker work
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(assert_ready_ok!(res1.poll()), "world");

    // res2 was dropped, so it should have been canceled in the buffer
    handle.allow(1);

    assert_request_eq!(handle, BatchControl::from("hello3")).send_response("world3");

    // Let worker work
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(assert_ready_ok!(res3.poll()), "world3");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn when_inner_is_not_ready() {
    let _guard = support::trace_init();

    let (mut service, mut handle) = mock::spawn_with(|s: Mock<BatchControl<&str>, &str>| {
        let (svc, worker) = Batch::pair(s, 10, Duration::from_secs(1));

        tokio::spawn(async move {
            let _guard = support::trace_init();

            let mut fut = tokio_test::task::spawn(worker);
            while fut.poll().is_pending() {}
        });

        svc
    });

    // Make the service NotReady
    handle.allow(0);

    assert_ready_ok!(service.poll_ready());
    let mut res1 = task::spawn(service.call("hello"));

    // Let worker work
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_pending!(res1.poll());
    assert_pending!(handle.poll_request());

    handle.allow(1);

    assert_request_eq!(handle, BatchControl::from("hello")).send_response("world");

    // Let worker work
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(assert_ready_ok!(res1.poll()), "world");
}

#[tokio::test(flavor = "current_thread")]
async fn when_inner_fails() {
    use std::error::Error as StdError;

    let _guard = support::trace_init();

    let (mut service, mut handle) = mock::spawn_with(|s: Mock<BatchControl<&str>, &str>| {
        let (svc, worker) = Batch::pair(s, 10, Duration::from_secs(1));

        tokio::spawn(async move {
            let mut fut = tokio_test::task::spawn(worker);
            while fut.poll().is_pending() {}
        });

        svc
    });

    // Make the service NotReady
    handle.allow(0);
    handle.send_error("foobar");

    assert_ready_ok!(service.poll_ready());
    let mut res1 = task::spawn(service.call("hello"));

    // Let worker work
    tokio::time::sleep(Duration::from_millis(100)).await;

    let e = assert_ready_err!(res1.poll());
    if let Some(e) = e.downcast_ref::<error::ServiceError>() {
        let e = e.source().unwrap();

        assert_eq!(e.to_string(), "foobar");
    } else {
        panic!("unexpected error type: {:?}", e);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn poll_ready_when_worker_is_dropped_early() {
    let _guard = support::trace_init();

    let (service, _handle) = mock::pair::<BatchControl<()>, ()>();
    let (service, worker) = Batch::pair(service, 1, Duration::from_secs(1));

    let mut service = mock::Spawn::new(service);

    drop(worker);

    let err = assert_ready_err!(service.poll_ready());

    assert!(err.is::<error::Closed>(), "should be a Closed: {:?}", err);
}

#[tokio::test(flavor = "current_thread")]
async fn response_future_when_worker_is_dropped_early() {
    let _guard = support::trace_init();

    let (service, mut handle) = mock::pair::<BatchControl<&'static str>, &'static str>();
    let (service, worker) = Batch::pair(service, 1, Duration::from_secs(1));
    let mut service = mock::Spawn::new(service);

    // keep the request in the worker
    handle.allow(0);
    assert_ready_ok!(service.poll_ready());
    let mut response = task::spawn(service.call("hello"));

    drop(worker);

    // Let worker work
    tokio::time::sleep(Duration::from_millis(100)).await;

    let err = assert_ready_err!(response.poll());
    assert!(err.is::<error::Closed>(), "should be a Closed: {:?}", err);
}

#[ignore = "response4 stuck because the flush() future never completes"]
#[tokio::test(flavor = "current_thread")]
async fn waits_for_channel_capacity() -> Result<(), BoxError> {
    let _guard = support::trace_init();

    let (service, mut handle) = mock::pair::<BatchControl<&'static str>, &'static str>();
    let (service, worker) = Batch::pair(service, 3, Duration::from_secs(1));

    let mut service = mock::Spawn::new(service);
    let mut worker = task::spawn(worker);

    // keep requests in the worker
    handle.allow(0);
    assert_ready_ok!(service.poll_ready());
    let mut response1 = task::spawn(service.call("hello"));
    assert_pending!(worker.poll());

    assert_ready_ok!(service.poll_ready());
    let mut response2 = task::spawn(service.call("hello"));
    assert_pending!(worker.poll());

    assert_ready_ok!(service.poll_ready());
    let mut response3 = task::spawn(service.call("hello"));
    assert_pending!(worker.poll());

    assert_pending!(service.poll_ready());

    handle.allow(1);
    assert_pending!(worker.poll());

    handle
        .next_request()
        .await
        .unwrap()
        .1
        .send_response("world");
    assert_pending!(worker.poll());
    assert_ready_ok!(response1.poll());

    assert_ready_ok!(service.poll_ready());
    let mut response4 = task::spawn(service.call("hello"));
    assert_pending!(worker.poll());

    handle.allow(4);
    assert_pending!(worker.poll());

    handle
        .next_request()
        .await
        .unwrap()
        .1
        .send_response("world");
    assert_pending!(worker.poll());
    assert_ready_ok!(response2.poll());

    handle
        .next_request()
        .await
        .unwrap()
        .1
        .send_response("world");
    assert_pending!(worker.poll());
    assert_ready_ok!(response3.poll());

    // The batch size is 3, therefore we expect a Flush at this point
    // FIXME: What is preventing the future from completing?
    assert_request_eq!(handle, BatchControl::Flush).send_response("world");

    handle
        .next_request()
        .await
        .unwrap()
        .1
        .send_response("world");
    assert_pending!(worker.poll());
    assert_ready_ok!(response4.poll());

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn wakes_pending_waiters_on_close() -> Result<(), BoxError> {
    let _guard = support::trace_init();

    let (service, mut handle) = mock::pair::<BatchControl<&str>, ()>();
    let (mut service, worker) = Batch::pair(service, 1, Duration::from_secs(1));
    let mut worker = task::spawn(worker);

    // Keep the request in the worker
    handle.allow(0);
    let service1 = service.ready().await?;
    assert_pending!(worker.poll());
    let mut response = task::spawn(service1.call("hello"));

    let mut service1 = service.clone();
    let mut ready1 = task::spawn(service1.ready());
    assert_pending!(worker.poll());
    assert_pending!(ready1.poll(), "no capacity");

    let mut service1 = service.clone();
    let mut ready2 = task::spawn(service1.ready());
    assert_pending!(worker.poll());
    assert_pending!(ready2.poll(), "no capacity");

    // kill the worker task
    drop(worker);

    let err = assert_ready_err!(response.poll());
    assert!(
        err.is::<error::Closed>(),
        "response should fail with a Closed, got: {:?}",
        err
    );

    assert!(
        ready1.is_woken(),
        "dropping worker should wake ready task 1"
    );
    let err = assert_ready_err!(ready1.poll());
    assert!(
        err.is::<error::Closed>(),
        "ready 1 should fail with a Closed, got: {:?}",
        err
    );

    assert!(
        ready2.is_woken(),
        "dropping worker should wake ready task 2"
    );
    let err = assert_ready_err!(ready1.poll());
    assert!(
        err.is::<error::Closed>(),
        "ready 2 should fail with a Closed, got: {:?}",
        err
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn wakes_pending_waiters_on_failure() -> Result<(), BoxError> {
    let _guard = support::trace_init();

    let (service, mut handle) = mock::pair::<BatchControl<&str>, ()>();
    let (mut service, worker) = Batch::pair(service, 1, Duration::from_secs(1));
    let mut worker = task::spawn(worker);

    // Keep the request in the worker
    handle.allow(0);
    let service1 = service.ready().await?;
    assert_pending!(worker.poll());
    let mut response = task::spawn(service1.call("hello"));

    let mut service1 = service.clone();
    let mut ready1 = task::spawn(service1.ready());
    assert_pending!(worker.poll());
    assert_pending!(ready1.poll(), "no capacity");

    let mut service1 = service.clone();
    let mut ready2 = task::spawn(service1.ready());
    assert_pending!(worker.poll());
    assert_pending!(ready2.poll(), "no capacity");

    // fail the inner service
    handle.send_error("foobar");
    // worker task terminates
    assert_ready!(worker.poll());

    let err = assert_ready_err!(response.poll());
    assert!(
        err.is::<error::ServiceError>(),
        "response should fail with a ServiceError, got: {:?}",
        err
    );

    assert!(
        ready1.is_woken(),
        "dropping worker should wake ready task 1"
    );
    let err = assert_ready_err!(ready1.poll());
    assert!(
        err.is::<error::ServiceError>(),
        "ready 1 should fail with a ServiceError, got: {:?}",
        err
    );

    assert!(
        ready2.is_woken(),
        "dropping worker should wake ready task 2"
    );
    let err = assert_ready_err!(ready1.poll());
    assert!(
        err.is::<error::ServiceError>(),
        "ready 2 should fail with a ServiceError, got: {:?}",
        err
    );

    Ok(())
}

#[tokio::test]
async fn propagates_trace_spans() -> Result<(), BoxError> {
    use tower::util::ServiceExt;
    use tracing::Instrument;

    let _guard = support::trace_init();

    let span = tracing::info_span!("my_span");

    let service = support::AssertSpanSvc::new(span.clone());
    let (service, worker) = Batch::pair(service, 5, Duration::from_secs(1));
    let worker = tokio::spawn(worker);

    let result = tokio::spawn(service.oneshot(()).instrument(span));

    result.await??;
    worker.await?;

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn doesnt_leak_permits() {
    let _guard = support::trace_init();

    let (service, mut handle) = mock::pair::<BatchControl<()>, ()>();

    let (mut service1, worker) = Batch::pair(service, 2, Duration::from_secs(1));
    let mut worker = task::spawn(worker);
    let mut service2 = service1.clone();
    let mut service3 = service1.clone();

    // Attempt to poll the first clone of the buffer to readiness multiple
    // times. These should all succeed, because the readiness is never
    // *consumed* --- no request is sent.
    assert_ready_ok!(task::spawn(service1.ready()).poll());
    assert_ready_ok!(task::spawn(service1.ready()).poll());
    assert_ready_ok!(task::spawn(service1.ready()).poll());

    // It should also be possible to drive the second clone of the service to
    // readiness --- it should only acquire one permit, as well.
    assert_ready_ok!(task::spawn(service2.ready()).poll());
    assert_ready_ok!(task::spawn(service2.ready()).poll());
    assert_ready_ok!(task::spawn(service2.ready()).poll());

    // The third clone *doesn't* poll ready, because the first two clones have
    // each acquired one permit.
    let mut ready3 = task::spawn(service3.ready());
    assert_pending!(ready3.poll());

    // Consume the first service's readiness.
    let mut response = task::spawn(service1.call(()));
    handle.allow(1);
    assert_pending!(worker.poll());

    handle.next_request().await.unwrap().1.send_response(());
    assert_pending!(worker.poll());
    assert_ready_ok!(response.poll());

    // Now, the third service should acquire a permit...
    assert!(ready3.is_woken());
    assert_ready_ok!(ready3.poll());
}

// TODO: the 'batch_flushes_on_elapsed_time' and the 'batch_flushes_on_elapsed_time' test the same
//  functionality with a different approach; the former is an integration test and the latter a unit
//  one. Any good reason to keep both around?
#[tokio::test(flavor = "current_thread")]
async fn batch_flushes_on_elapsed_time_unit() -> Result<(), BoxError> {
    let _guard = support::trace_init();

    let (service, mut handle) = mock::pair::<_, ()>();

    let (mut service, worker) = Batch::pair(service, 10, Duration::from_millis(100));
    let mut worker = task::spawn(worker);

    // Keep the request in the worker
    handle.allow(0);
    service.ready().await?;
    assert_pending!(worker.poll());

    let mut response = task::spawn(service.call("hello"));
    assert_pending!(response.poll());
    assert_pending!(handle.poll_request());

    handle.allow(1);

    assert_pending!(worker.poll());
    assert_request_eq!(handle, BatchControl::Item("hello")).send_response(());
    assert_ready_ok!(response.poll());

    tokio::time::sleep(Duration::from_millis(98)).await;
    assert!(!worker.is_woken());

    // Give the batch plenty of time to flush
    tokio::time::sleep(Duration::from_millis(2)).await;

    // Poll the worker - enough time has passed, but the service is not ready.
    assert!(worker.is_woken());
    assert_pending!(worker.poll());
    assert_pending!(handle.poll_request());

    // Make the service ready
    handle.allow(1);

    assert!(worker.is_woken());
    assert_pending!(worker.poll());
    assert_request_eq!(handle, BatchControl::Flush).send_response(());

    Ok(())
}
