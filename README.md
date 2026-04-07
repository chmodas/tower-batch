# tower-batch

[![CI Build](https://github.com/chmodas/tower-batch/actions/workflows/ci.yml/badge.svg)](https://github.com/chmodas/tower-batch/actions/workflows/ci.yml)
[![codecov](https://codecov.io/github/chmodas/tower-batch/graph/badge.svg?token=GLA3M3GGQC)](https://codecov.io/github/chmodas/tower-batch)

A [Tower] middleware that buffers requests and flushes them in batches. Use it when the downstream system is more efficient with bulk writes – databases, message brokers, object stores, etc. The middleware collects individual requests as `BatchControl::Item(R)` and, once the buffer reaches a maximum size **or** a maximum duration elapses, signals the inner service with `BatchControl::Flush` so it can process the accumulated batch.

## Quick start

Add the dependency to your `Cargo.toml`:

```toml
[dependencies]
tower-batch = "0.4.0"
```

Create a batch service and start sending requests:

```rust
use std::time::Duration;
use tower_batch::Batch;

// `my_service` implements `Service<BatchControl<MyRequest>>`
let batch = Batch::new(my_service, 100, Duration::from_millis(250));
```

If you prefer the [Tower layer] pattern:

```rust
use tower_batch::BatchLayer;

let layer = BatchLayer::new(100, Duration::from_millis(250));
```

## How it works

Your inner service must implement `Service<BatchControl<R>>` where `R` is the request type. The middleware sends two kinds of calls:

- **`BatchControl::Item(request)`** – buffer this request. Typically, you just push it onto a `Vec` and return `Ok(())`.
- **`BatchControl::Flush`** – process everything you have buffered, then return the result.

`Batch::new` spawns a background worker that owns the inner service. It forwards each incoming request as an `Item`, and triggers a `Flush` when the batch is full or the timer fires. `Batch` handles are cheap to clone – each clone shares the same worker, so you can hand them to multiple tasks.

## Examples

See the [`examples/`](examples/) directory:

- **[`sqlite_batch`](examples/sqlite_batch.rs)** – batch-insert rows into an in-memory SQLite database using the rarray virtual table.
- **[`unreliable_api`](examples/unreliable_api.rs)** – batch-write events to a simulated unreliable remote API, composing `Batch` with Tower's `Retry` and `Timeout` layers.

Run an example with:

```sh
cargo run --example sqlite_batch
cargo run --example unreliable_api
```

## License

This project is licensed under the [MIT](LICENSE) license.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in `tower-batch` by you, shall be licensed as MIT, without any additional terms or conditions.

[Tower]: https://docs.rs/tower
[Tower layer]: https://docs.rs/tower/latest/tower/trait.Layer.html
