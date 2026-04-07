use std::{
    fmt::Debug,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::Duration,
};

use futures::{stream::FuturesUnordered, StreamExt};
use tokio::task::JoinHandle;
use tokio_test::{assert_pending, assert_ready, assert_ready_err, assert_ready_ok, task};
use tower::{layer::Layer, Service, ServiceExt};
use tower_test::{
    assert_request_eq,
    mock::{self, Mock},
};

use tower_batch::{error, Batch, BatchControl, BatchLayer, BoxError};

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

    fn batch_items(&self, index: usize) -> Option<Vec<T>>
    where
        T: Clone,
    {
        if index == self.current.load(Ordering::Acquire) {
            return None;
        }
        let items = self.items.lock().unwrap();
        items.get(index).cloned()
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

#[tokio::test]
async fn batch_flushes_multiple_times() -> Result<(), BoxError> {
    let _guard = support::trace_init();

    let aggregator: Aggregator<u32> = Aggregator::new();
    let mut batch = Batch::new(aggregator.clone(), 10, Duration::from_secs(1));

    let mut results = FuturesUnordered::new();

    for i in 0..20 {
        let span = tracing::trace_span!("msg", i);
        batch.ready().await?;
        results.push(span.in_scope(|| batch.call(i)));
    }

    while let Some(Ok(_)) = results.next().await {}

    assert!(aggregator.batch_has_size(0, 10));
    assert!(aggregator.batch_has_size(1, 10));

    Ok(())
}

#[tokio::test]
async fn batch_items_are_ordered() -> Result<(), BoxError> {
    let _guard = support::trace_init();

    let aggregator: Aggregator<u32> = Aggregator::new();
    let mut batch = Batch::new(aggregator.clone(), 10, Duration::from_secs(1));

    let mut results = FuturesUnordered::new();

    for i in 0..10 {
        batch.ready().await?;
        results.push(batch.call(i));
    }

    while let Some(Ok(_)) = results.next().await {}

    let items = aggregator.batch_items(0).expect("batch 0 should exist");
    assert_eq!(items, (0..10).collect::<Vec<u32>>());

    Ok(())
}

#[tokio::test]
async fn concurrent_clones_send_requests() -> Result<(), BoxError> {
    let _guard = support::trace_init();

    let aggregator: Aggregator<u32> = Aggregator::new();
    let batch = Batch::new(aggregator.clone(), 10, Duration::from_secs(1));

    let mut handles = FuturesUnordered::new();

    for clone_id in 0..3u32 {
        let mut svc = batch.clone();
        handles.push(tokio::spawn(async move {
            let mut results = Vec::new();
            for i in 0..3 {
                svc.ready().await.unwrap();
                results.push(svc.call(clone_id * 10 + i).await);
            }
            results
        }));
    }

    let mut total = 0usize;
    while let Some(result) = handles.next().await {
        let results = result.unwrap();
        for r in results {
            r.unwrap();
            total += 1;
        }
    }

    assert_eq!(total, 9);

    // Drop the Batch handle so the worker can shut down and flush remaining items.
    drop(batch);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify all 9 items were actually delivered to the aggregator.
    let items = aggregator.items.lock().unwrap();
    let delivered: usize = items.iter().map(|batch| batch.len()).sum();
    assert_eq!(delivered, 9, "all 9 items should reach the aggregator");

    Ok(())
}

#[tokio::test]
async fn time_based_flush_triggers_multiple_batches() -> Result<(), BoxError> {
    let _guard = support::trace_init();

    // Large max_size so flushes are triggered only by the short max_time.
    let aggregator: Aggregator<u32> = Aggregator::new();
    let mut batch = Batch::new(aggregator.clone(), 100, Duration::from_millis(100));

    // First group: send all items, then await responses (flush fires on time).
    let mut results = FuturesUnordered::new();
    for i in 0..3 {
        batch.ready().await?;
        results.push(batch.call(i));
    }
    while let Some(Ok(_)) = results.next().await {}
    // Give the flush time to complete.
    tokio::time::sleep(Duration::from_millis(250)).await;

    // Second group
    let mut results = FuturesUnordered::new();
    for i in 10..13 {
        batch.ready().await?;
        results.push(batch.call(i));
    }
    while let Some(Ok(_)) = results.next().await {}
    tokio::time::sleep(Duration::from_millis(250)).await;

    assert!(
        aggregator.batch_has_size(0, 3),
        "first time-based batch should have 3 items"
    );
    assert!(
        aggregator.batch_has_size(1, 3),
        "second time-based batch should have 3 items"
    );

    let b0 = aggregator.batch_items(0).unwrap();
    assert_eq!(b0, vec![0, 1, 2]);
    let b1 = aggregator.batch_items(1).unwrap();
    assert_eq!(b1, vec![10, 11, 12]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_clones_with_backpressure() {
    let _guard = support::trace_init();

    let (mut service, mut handle) = mock::spawn_with(|s: Mock<BatchControl<&str>, &str>| {
        // batch size 2, so after 2 items the semaphore is exhausted
        let (svc, worker) = Batch::pair(s, 2, Duration::from_secs(1));

        tokio::spawn(async move {
            let _guard = support::trace_init();
            let mut fut = tokio_test::task::spawn(worker);
            while fut.poll().is_pending() {}
        });

        svc
    });

    let mut service2 = service.clone();

    // Inner service starts not ready – creates back-pressure.
    handle.allow(0);

    // Clone 1 sends a request; it will be buffered but the inner service won't accept it yet.
    assert_ready_ok!(service.poll_ready());
    let mut res1 = task::spawn(service.call("from_clone1"));

    // Clone 2 also sends a request.
    assert_ready_ok!(service2.poll_ready());
    let mut res2 = task::spawn(service2.call("from_clone2"));

    // Let the worker attempt to process (it can't – inner service not ready).
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_pending!(res1.poll());
    assert_pending!(res2.poll());

    // Now allow the inner service to accept requests + flush.
    handle.allow(4);

    assert_request_eq!(handle, BatchControl::from("from_clone1")).send_response("resp1");
    assert_request_eq!(handle, BatchControl::from("from_clone2")).send_response("resp2");
    assert_request_eq!(handle, BatchControl::Flush).send_response("flushed");

    // Let the worker deliver responses.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(assert_ready_ok!(res1.poll()), "resp1");
    assert_eq!(assert_ready_ok!(res2.poll()), "resp2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clears_canceled_requests() {
    let _guard = support::trace_init();

    let (mut service, mut handle) = mock::spawn_with(|s: Mock<BatchControl<&str>, &str>| {
        let (svc, worker) = Batch::pair(s, 2, Duration::from_secs(1));

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

    tokio::time::sleep(Duration::from_millis(10)).await;

    assert_ready_ok!(service.poll_ready());
    let mut res3 = task::spawn(service.call("hello3"));

    drop(res2);

    send_response1.send_response("world");

    // Let worker work
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_pending!(res1.poll());
    assert_pending!(res3.poll());

    // res2 was dropped, so it should have been canceled in the buffer
    handle.allow(1);
    assert_request_eq!(handle, BatchControl::from("hello3")).send_response("world3");

    // Let worker work
    handle.allow(2);
    assert_request_eq!(handle, BatchControl::Flush).send_response("flush");

    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(assert_ready_ok!(res1.poll()), "world");
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

    handle.allow(3);

    assert_request_eq!(handle, BatchControl::from("hello")).send_response("world");
    assert_request_eq!(handle, BatchControl::Flush).send_response("flushed");

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
    assert_pending!(response1.poll());

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
    assert_pending!(response2.poll());

    handle
        .next_request()
        .await
        .unwrap()
        .1
        .send_response("world");
    assert_pending!(worker.poll());
    assert_pending!(response3.poll());

    // Flushing the batch will complete the pending futures
    assert_request_eq!(handle, BatchControl::Flush).send_response("world");
    assert_pending!(worker.poll());

    assert_ready_ok!(response1.poll());
    assert_ready_ok!(response2.poll());
    assert_ready_ok!(response3.poll());

    // Only the queued one is still pending
    handle
        .next_request()
        .await
        .unwrap()
        .1
        .send_response("world");
    assert_pending!(worker.poll());
    assert_pending!(response4.poll());

    Ok(())
}

#[tokio::test]
async fn request_futures_fail_if_flush_fails() {
    let _guard = support::trace_init();

    let (service, mut handle) = mock::pair::<BatchControl<&str>, ()>();
    let (service, worker) = Batch::pair(service, 2, Duration::from_secs(1));

    let mut service = mock::Spawn::new(service);
    let mut worker = task::spawn(worker);

    handle.allow(4);

    assert_ready_ok!(service.poll_ready());
    let mut res1 = task::spawn(service.call("hello1"));

    assert_ready_ok!(service.poll_ready());
    let mut res2 = task::spawn(service.call("hello2"));

    // Let the worker work.
    assert_pending!(worker.poll());

    assert_request_eq!(handle, BatchControl::from("hello1")).send_response(());
    assert_pending!(res1.poll());

    assert_request_eq!(handle, BatchControl::from("hello2")).send_response(());
    assert_pending!(res2.poll());

    // If the flush fails, so will the worker and any pending futures
    handle.allow(2);
    assert_request_eq!(handle, BatchControl::Flush).send_error("flush failed");

    assert_ready!(worker.poll());
    assert_ready_err!(res1.poll());
    assert_ready_err!(res2.poll());
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
    let err = assert_ready_err!(ready2.poll());
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
    let err = assert_ready_err!(ready2.poll());
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
    let (mut service, worker) = Batch::pair(service, 5, Duration::from_millis(250));
    let worker = tokio::spawn(worker);

    let result: JoinHandle<Result<(), tower_batch::BoxError>> = tokio::spawn(async move {
        service.ready().await?;
        service.call(()).await?;
        Ok(())
    });

    let _ = result.instrument(span).await?;
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

    // Consume the first two service's readiness.
    let mut response1 = task::spawn(service1.call(()));
    let mut response2 = task::spawn(service2.call(()));
    handle.allow(3);
    assert_pending!(worker.poll());

    handle.next_request().await.unwrap().1.send_response(());
    assert_pending!(worker.poll());

    handle.next_request().await.unwrap().1.send_response(());
    assert_pending!(worker.poll());

    assert_request_eq!(handle, BatchControl::Flush).send_response(());
    assert_pending!(worker.poll());

    assert_ready_ok!(response1.poll());
    assert_ready_ok!(response2.poll());

    // Now, the third service should acquire a permit...
    assert!(ready3.is_woken());
    assert_ready_ok!(ready3.poll());
}

// === New coverage tests ===

#[tokio::test]
async fn batch_layer_wraps_service() {
    let _guard = support::trace_init();

    let aggregator: Aggregator<u32> = Aggregator::new();
    let layer = BatchLayer::<u32>::new(10, Duration::from_secs(1));

    // Cover Debug impl
    let debug_str = format!("{:?}", layer);
    assert!(
        debug_str.contains("BatchLayer"),
        "Debug should contain 'BatchLayer', got: {}",
        debug_str
    );

    // Cover Layer::layer() which delegates to Batch::new()
    let mut service = layer.layer(aggregator.clone());
    service.ready().await.unwrap();
    service.call(42).await.unwrap();

    // Give time for the flush
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert!(
        aggregator.batch_has_size(0, 1),
        "layer-created service should deliver the item"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn error_display_and_debug_formatting() {
    use std::error::Error as StdError;

    let _guard = support::trace_init();

    // --- Closed: drop worker before poll_ready ---
    {
        let (service, _handle) = mock::pair::<BatchControl<()>, ()>();
        let (service, worker) = Batch::pair(service, 1, Duration::from_secs(1));
        let mut service = mock::Spawn::new(service);

        drop(worker);

        let err = assert_ready_err!(service.poll_ready());
        let closed = err
            .downcast_ref::<error::Closed>()
            .expect("should be Closed");
        let debug_str = format!("{:?}", closed);
        assert!(debug_str.contains("Closed"), "Debug: {}", debug_str);
        let display_str = format!("{}", closed);
        assert!(
            display_str.contains("batch's worker closed unexpectedly"),
            "Display: {}",
            display_str
        );
    }

    // --- ServiceError: inner service fails ---
    {
        let (service, mut handle) = mock::pair::<BatchControl<&str>, &str>();
        let (service, worker) = Batch::pair(service, 10, Duration::from_secs(1));
        let mut service = mock::Spawn::new(service);
        let mut worker = task::spawn(worker);

        handle.allow(0);
        handle.send_error("boom");

        assert_ready_ok!(service.poll_ready());
        let mut response = task::spawn(service.call("hello"));

        // Let worker process and fail
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_ready!(worker.poll());

        let err = assert_ready_err!(response.poll());
        let svc_err = err
            .downcast_ref::<error::ServiceError>()
            .expect("should be ServiceError");
        let display_str = format!("{}", svc_err);
        assert!(
            display_str.contains("batch service failed:"),
            "Display: {}",
            display_str
        );
        // Also check source()
        let source = svc_err.source().unwrap();
        assert_eq!(source.to_string(), "boom");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn call_after_worker_death() {
    let _guard = support::trace_init();

    let (service, _handle) = mock::pair::<BatchControl<&str>, &str>();
    let (service, worker) = Batch::pair(service, 1, Duration::from_secs(1));
    let mut service = mock::Spawn::new(service);

    // Acquire permit while worker is still alive
    assert_ready_ok!(service.poll_ready());

    // Kill the worker – channel receiver is dropped
    drop(worker);

    // call() tries tx.send() which fails → ResponseFuture::failed()
    let mut response = task::spawn(service.call("hello"));

    // Exercises ResponseState::Failed arm
    let err = assert_ready_err!(response.poll());
    assert!(
        err.is::<error::Closed>(),
        "should be Closed, got: {:?}",
        err
    );
}

#[tokio::test(flavor = "current_thread")]
async fn flush_phase_poll_ready_failure() {
    let _guard = support::trace_init();

    let (service, mut handle) = mock::pair::<BatchControl<&str>, &str>();
    let (service, worker) = Batch::pair(service, 2, Duration::from_secs(1));
    let mut service = mock::Spawn::new(service);
    let mut worker = task::spawn(worker);

    // Allow inner service to accept 2 items (the batch items)
    handle.allow(2);

    assert_ready_ok!(service.poll_ready());
    let mut res1 = task::spawn(service.call("a"));

    assert_ready_ok!(service.poll_ready());
    let mut res2 = task::spawn(service.call("b"));

    // Worker processes both items, batch is full, enters Flushing { flush_fut: None },
    // tries poll_ready for Flush – gets Pending (0 allows left)
    assert_pending!(worker.poll());

    // Respond to item requests
    assert_request_eq!(handle, BatchControl::from("a")).send_response("ra");
    assert_request_eq!(handle, BatchControl::from("b")).send_response("rb");

    // Queue an error for the next poll_ready (during flush phase)
    handle.send_error("flush ready failed");

    // Worker polls again: in Flushing state, poll_ready returns Err → lines 241-247.
    // Worker transitions to Finished and terminates.
    assert_ready!(worker.poll());

    // Both response futures should fail with ServiceError (lot.notify was called)
    let err1 = assert_ready_err!(res1.poll());
    assert!(
        err1.is::<error::ServiceError>(),
        "res1 should be ServiceError, got: {:?}",
        err1
    );
    let err2 = assert_ready_err!(res2.poll());
    assert!(
        err2.is::<error::ServiceError>(),
        "res2 should be ServiceError, got: {:?}",
        err2
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_request_in_channel() {
    let _guard = support::trace_init();

    let (service, mut handle) = mock::pair::<BatchControl<&str>, &str>();
    // Need max_size >= 2 for 2 semaphore permits; use short timer to trigger flush.
    let (service, worker) = Batch::pair(service, 2, Duration::from_millis(1));
    let mut service = mock::Spawn::new(service);
    let mut worker = task::spawn(worker);

    // Do NOT poll worker yet – messages stay in the channel

    // Send request "a" and immediately drop its response future (cancels it)
    assert_ready_ok!(service.poll_ready());
    let res_a = task::spawn(service.call("a"));
    drop(res_a);

    // Send request "b" and keep its response future
    assert_ready_ok!(service.poll_ready());
    let mut res_b = task::spawn(service.call("b"));

    // Let inner service accept requests + flush
    handle.allow(3);

    // First poll: worker enters poll_next_msg while-let loop.
    // Receives "a" – tx.is_closed() == true – skips (line 377).
    // Receives "b" – processes normally. Lot timer starts. Batch not full.
    // Tries to get next message – channel empty → Pending.
    assert_pending!(worker.poll());

    // Only "b" should reach the mock handle (not "a")
    assert_request_eq!(handle, BatchControl::from("b")).send_response("rb");

    // Wait for the short max_time to elapse
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Second poll: poll_max_time fires → enters Flushing state
    assert_pending!(worker.poll());

    assert_request_eq!(handle, BatchControl::Flush).send_response("flushed");
    assert_pending!(worker.poll());

    assert_eq!(assert_ready_ok!(res_b.poll()), "rb");
}

/// Regression test: poll_max_time must not overwrite an in-progress flush.
///
/// With batch size 1 and a 1ms timer, the size-based flush fires immediately.
/// The 1ms timer expires well before the 50ms flush completes. Before the fix,
/// the timer would reset the Flushing state, dropping the flush future.
#[tokio::test]
async fn timer_does_not_overwrite_in_progress_flush() {
    let _guard = support::trace_init();

    let flush_count = Arc::new(AtomicUsize::new(0));

    struct SlowFlushService {
        flush_count: Arc<AtomicUsize>,
    }

    impl Service<BatchControl<String>> for SlowFlushService {
        type Response = ();
        type Error = BoxError;
        type Future = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: BatchControl<String>) -> Self::Future {
            match req {
                BatchControl::Item(_) => Box::pin(futures::future::ready(Ok(()))),
                BatchControl::Flush => {
                    let count = self.flush_count.clone();
                    Box::pin(async move {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        count.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                }
            }
        }
    }

    let service = SlowFlushService {
        flush_count: flush_count.clone(),
    };

    // Batch size 1: flush triggers immediately on first item.
    // max_time 1ms: timer fires well before the 50ms flush completes.
    let mut batch = Batch::new(service, 1, Duration::from_millis(1));

    batch.ready().await.unwrap();
    let response = batch.call("hello".to_string());

    response.await.unwrap();

    assert_eq!(
        flush_count.load(Ordering::SeqCst),
        1,
        "flush future should have run to completion, not been dropped by the timer"
    );
}

/// Regression: when poll_ready fails with multiple messages queued in the
/// channel, the worker must terminate immediately instead of calling
/// poll_ready again on the broken service (which could hang or behave
/// inconsistently).
#[tokio::test(flavor = "current_thread")]
async fn worker_terminates_on_poll_ready_error_with_queued_messages() {
    let _guard = support::trace_init();

    let (service, mut handle) = mock::pair::<BatchControl<&str>, &str>();
    let (service, worker) = Batch::pair(service, 3, Duration::from_secs(1));
    let mut service = mock::Spawn::new(service);
    let mut worker = task::spawn(worker);

    // Inner service not ready -- messages queue in the channel.
    handle.allow(0);

    assert_ready_ok!(service.poll_ready());
    let mut res1 = task::spawn(service.call("msg1"));

    assert_ready_ok!(service.poll_ready());
    let mut res2 = task::spawn(service.call("msg2"));

    // Worker tries to process msg1 but service is not ready -- returns Pending.
    assert_pending!(worker.poll());

    // Now make the next poll_ready fail.
    handle.send_error("boom");

    // Worker should hit the error on msg1 and terminate immediately,
    // without attempting poll_ready for msg2.
    assert_ready!(worker.poll());

    // The first response gets the ServiceError (notified via lot).
    let err1 = assert_ready_err!(res1.poll());
    assert!(
        err1.is::<error::ServiceError>(),
        "res1 should be ServiceError, got: {:?}",
        err1
    );

    // Drop the worker so the channel receiver and remaining messages are
    // freed. This drops msg2's oneshot sender, unblocking res2.
    drop(worker);

    // The second response gets Closed (its oneshot sender was dropped
    // when the worker was dropped without processing it).
    let err2 = assert_ready_err!(res2.poll());
    assert!(
        err2.is::<error::Closed>(),
        "res2 should be Closed, got: {:?}",
        err2
    );
}
