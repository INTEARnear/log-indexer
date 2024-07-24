# Log Indexer

This indexer streams all logs and [NEP-297](https://nomicon.io/Standards/EventsFormat) events to `log_text` and `log_nep297` Redis streams.

To run it, set `REDIS_URL` environment variable and `cargo run --release`
