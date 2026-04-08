use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};

use futures::{stream::FuturesUnordered, StreamExt};
use proptest::prelude::*;
use tower::{Service, ServiceExt};
use tower_batch::{Batch, BatchControl, BoxError};

mod support;
use support::Aggregator;

const MAX_TIME: Duration = Duration::from_millis(100);

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn rt_multi_thread() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

// ===== FailingAggregator =====

#[derive(Clone, Debug)]
struct FailingAggregator {
    flush_count: Arc<AtomicUsize>,
    fail_at_flush: usize,
}

impl FailingAggregator {
    fn new(fail_at_flush: usize) -> Self {
        Self {
            flush_count: Arc::new(AtomicUsize::new(0)),
            fail_at_flush,
        }
    }
}

impl Service<BatchControl<u32>> for FailingAggregator {
    type Response = ();
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + Sync>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: BatchControl<u32>) -> Self::Future {
        match req {
            BatchControl::Item(_) => Box::pin(futures::future::ready(Ok(()))),
            BatchControl::Flush => {
                let n = self.flush_count.fetch_add(1, Ordering::SeqCst);
                if n == self.fail_at_flush {
                    Box::pin(futures::future::ready(Err("injected flush error".into())))
                } else {
                    Box::pin(futures::future::ready(Ok(())))
                }
            }
        }
    }
}

// ===== Property tests =====

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn batch_flush_boundary(
        batch_size in 1usize..=100,
        request_count in 1usize..=500,
    ) {
        let rt = rt();
        let batches = rt.block_on(async {
            let aggregator: Aggregator<u32> = Aggregator::new();
            let mut batch = Batch::new(aggregator.clone(), batch_size, MAX_TIME);

            let mut futs = FuturesUnordered::new();
            for i in 0..request_count {
                batch.ready().await.unwrap();
                #[allow(clippy::cast_possible_truncation)]
                futs.push(batch.call(i as u32));
            }
            while let Some(result) = futs.next().await {
                result.unwrap();
            }

            aggregator.completed_batches()
        });

        let expected_flushes = request_count.div_ceil(batch_size);
        prop_assert_eq!(batches.len(), expected_flushes);
        for (i, b) in batches.iter().enumerate() {
            if i < batches.len() - 1 {
                prop_assert_eq!(b.len(), batch_size);
            } else {
                let rem = request_count % batch_size;
                let expected_last = if rem == 0 { batch_size } else { rem };
                prop_assert_eq!(b.len(), expected_last);
            }
        }
    }

    #[test]
    fn request_ordering(
        batch_size in 1usize..=50,
        request_count in 1usize..=200,
    ) {
        let rt = rt();
        let flat_items = rt.block_on(async {
            let aggregator: Aggregator<u32> = Aggregator::new();
            let mut batch = Batch::new(aggregator.clone(), batch_size, MAX_TIME);

            let mut futs = FuturesUnordered::new();
            #[allow(clippy::cast_possible_truncation)]
            for i in 0..request_count as u32 {
                batch.ready().await.unwrap();
                futs.push(batch.call(i));
            }
            while let Some(r) = futs.next().await {
                r.unwrap();
            }

            aggregator.all_items_flat()
        });

        #[allow(clippy::cast_possible_truncation)]
        let expected: Vec<u32> = (0..request_count as u32).collect();
        prop_assert_eq!(flat_items, expected);
    }

    #[test]
    fn permit_accounting_no_leak(
        batch_size in 2usize..=10,
        ops in prop::collection::vec(any::<bool>(), 1..=30),
    ) {
        let rt = rt();
        rt.block_on(async {
            let aggregator: Aggregator<u32> = Aggregator::new();
            let mut batch = Batch::new(aggregator.clone(), batch_size, MAX_TIME);
            let mut call_futs = FuturesUnordered::new();

            for (i, do_call) in ops.iter().enumerate() {
                if *do_call {
                    batch.ready().await.unwrap();
                    #[allow(clippy::cast_possible_truncation)]
                    call_futs.push(batch.call(i as u32));
                } else {
                    let mut clone = batch.clone();
                    clone.ready().await.unwrap();
                    drop(clone);
                }
            }

            // Drain all call futures
            while let Some(r) = call_futs.next().await {
                r.unwrap();
            }

            // Verify all permits are available by acquiring batch_size of them via clones.
            // If any leaked, this will deadlock and the timeout will catch it.
            let mut clones: Vec<_> = (0..batch_size).map(|_| batch.clone()).collect();
            for c in &mut clones {
                tokio::time::timeout(Duration::from_secs(5), c.ready())
                    .await
                    .expect("timed out waiting for permit — likely a permit leak")
                    .unwrap();
            }
            drop(clones);
        });
    }

    #[test]
    fn cancellation_patterns(
        (batch_size, cancel_mask) in
            (1usize..=20, 1usize..=50).prop_flat_map(|(bs, rc)| {
                (
                    Just(bs),
                    prop::collection::vec(any::<bool>(), rc..=rc),
                )
            })
    ) {
        let rt = rt();
        let (results, batches) = rt.block_on(async {
            let aggregator: Aggregator<u32> = Aggregator::new();
            let mut batch = Batch::new(aggregator.clone(), batch_size, MAX_TIME);

            let mut kept_futs = FuturesUnordered::new();
            for (i, cancelled) in cancel_mask.iter().enumerate() {
                batch.ready().await.unwrap();
                #[allow(clippy::cast_possible_truncation)]
                let fut = batch.call(i as u32);
                if *cancelled {
                    drop(fut);
                } else {
                    kept_futs.push(fut);
                }
            }

            let mut results = Vec::new();
            while let Some(r) = kept_futs.next().await {
                results.push(r.is_ok());
            }

            drop(batch);
            tokio::time::sleep(Duration::from_millis(150)).await;
            (results, aggregator.completed_batches())
        });

        // All non-cancelled requests got Ok responses
        for (i, ok) in results.iter().enumerate() {
            prop_assert!(*ok, "non-cancelled request {} should have succeeded", i);
        }
        // No batch exceeds batch_size
        for b in &batches {
            prop_assert!(b.len() <= batch_size);
        }
    }

    #[test]
    fn error_propagation(
        (batch_size, request_count, fail_at_flush) in
            (1usize..=20, 2usize..=50).prop_flat_map(|(bs, rc)| {
                let num_flushes = rc.div_ceil(bs);
                (Just(bs), Just(rc), 0..num_flushes)
            })
    ) {
        let rt = rt();
        let results = rt.block_on(async {
            let svc = FailingAggregator::new(fail_at_flush);
            let mut batch = Batch::new(svc, batch_size, MAX_TIME);

            let mut futs = FuturesUnordered::new();
            #[allow(clippy::cast_possible_truncation)]
            for i in 0..request_count as u32 {
                match batch.ready().await {
                    Ok(_) => futs.push(batch.call(i)),
                    Err(_) => break,
                }
            }

            let mut results = Vec::new();
            while let Some(r) = futs.next().await {
                results.push(r.is_ok());
            }
            results
        });

        let has_errors = results.iter().any(|ok| !ok);
        prop_assert!(has_errors, "should have at least one error from the injected flush failure");
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn concurrent_clone_stress(
        num_clones in 2usize..=10,
        requests_per_clone in 1usize..=30,
        batch_size in 1usize..=20,
    ) {
        let rt = rt_multi_thread();
        let (total_delivered, total_succeeded) = rt.block_on(async {
            let aggregator: Aggregator<u32> = Aggregator::new();
            let batch = Batch::new(aggregator.clone(), batch_size, MAX_TIME);

            let mut handles = Vec::new();
            for clone_id in 0..num_clones {
                let mut svc = batch.clone();
                handles.push(tokio::spawn(async move {
                    let mut count = 0usize;
                    for i in 0..requests_per_clone {
                        #[allow(clippy::cast_possible_truncation)]
                        if svc.ready().await.is_ok()
                            && svc.call((clone_id * 1000 + i) as u32).await.is_ok()
                        {
                            count += 1;
                        }
                    }
                    count
                }));
            }

            let mut total_succeeded = 0usize;
            for h in handles {
                total_succeeded += h.await.unwrap();
            }

            // Drop the original handle and wait for partial-batch flush
            drop(batch);
            tokio::time::sleep(Duration::from_millis(150)).await;

            let total_delivered: usize =
                aggregator.completed_batches().iter().map(Vec::len).sum();
            (total_delivered, total_succeeded)
        });

        prop_assert_eq!(total_delivered, total_succeeded);
    }

    #[test]
    fn service_error_display_contains_message(msg in "[a-zA-Z0-9 ]{1,50}") {
        let rt = rt();
        let err_display = rt.block_on(async {
            use tower_test::mock;

            let (service, mut handle) = mock::pair::<BatchControl<()>, ()>();
            let mut batch = Batch::new(service, 1, Duration::from_secs(1));

            // Allow the first request through so the worker picks it up
            handle.allow(1);

            batch.ready().await.unwrap();
            let resp_fut = batch.call(());

            // The mock receives BatchControl::Item(()), respond to it
            let (request, send_response) = handle.next_request().await.unwrap();
            assert_eq!(request, BatchControl::Item(()));
            send_response.send_response(());

            // Now the worker will try to flush; the mock receives BatchControl::Flush
            handle.allow(1);
            let (request, send_response) = handle.next_request().await.unwrap();
            assert_eq!(request, BatchControl::Flush);
            send_response.send_error(msg.clone());

            let result = resp_fut.await;
            result.unwrap_err().to_string()
        });

        prop_assert!(
            err_display.contains("batch service failed:"),
            "display was: {}", err_display
        );
        prop_assert!(
            err_display.contains(&msg),
            "display should contain inner message '{}', was: {}", msg, err_display
        );
    }
}
