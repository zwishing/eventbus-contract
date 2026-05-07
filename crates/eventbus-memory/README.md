# eventbus-memory

[![Crates.io](https://img.shields.io/crates/v/eventbus-memory.svg)](https://crates.io/crates/eventbus-memory)
[![Docs.rs](https://docs.rs/eventbus-memory/badge.svg)](https://docs.rs/eventbus-memory)

In-process `StreamBackend` for the [eventbus-contract](https://crates.io/crates/eventbus-contract) workspace. Intended for tests, examples, and local development — **not a production backend**.

State lives behind a single `tokio::sync::Mutex`, so every backend operation is globally serialized. This makes the backend a deterministic reference for correctness tests but is not representative of production throughput.

## Use via the facade (recommended)

```toml
[dependencies]
eventbus-contract = { version = "0.2", features = ["memory"] }   # default-on
```

```rust
use eventbus_contract::memory::MemoryStreamBackend;
```

## Direct dependency

```toml
[dependencies]
eventbus-core = "0.2"
eventbus-memory = "0.2"     # default-on `test-utils` feature exposes MemoryStreamBackend
```

```rust
use eventbus_memory::MemoryStreamBackend;
```

Disable in production code with `default-features = false` so the backend cannot be linked accidentally.

For benchmark-quality throughput use [eventbus-redis](https://crates.io/crates/eventbus-redis).

## License

MIT
