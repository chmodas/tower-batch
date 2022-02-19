# tower-batch

Writing data in bulk is a common technique for improving the efficiency of certain
tasks. `batch-tower` is a [Tower] middleware that allows you to buffer requests for batch processing
until the buffer reaches a maximum size OR a maximum duration elapses.

[Tower]: https://docs.rs/tower