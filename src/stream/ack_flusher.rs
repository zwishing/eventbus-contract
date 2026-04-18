use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::EventBusError;

use super::backend::StreamBackend;

const DEFAULT_ACK_BATCH_SIZE: usize = 64;
const DEFAULT_ACK_FLUSH_INTERVAL: Duration = Duration::from_millis(2);

pub(super) struct AckRequest {
    pub id: String,
    pub done: oneshot::Sender<Result<(), EventBusError>>,
}

/// Spawns a background task that collects individual ack requests and flushes
/// them to the backend in batches via [`StreamBackend::ack_many`].
///
/// Batching triggers on whichever comes first:
/// - `batch_size` requests accumulated, or
/// - `flush_interval` elapsed since the first un-flushed request.
///
/// Returns the sender half (cloneable, held by each `StreamDelivery`)
/// and the task handle (awaited by the subscription on shutdown).
///
/// **Shutdown**: when all senders drop, the flusher drains remaining requests,
/// flushes a final batch, and exits.
pub(super) fn spawn<B: StreamBackend>(
    backend: Arc<B>,
    stream: String,
    group: String,
    batch_size: usize,
    flush_interval: Duration,
) -> (mpsc::Sender<AckRequest>, JoinHandle<()>) {
    let batch_size = if batch_size == 0 {
        DEFAULT_ACK_BATCH_SIZE
    } else {
        batch_size
    };
    let flush_interval = if flush_interval.is_zero() {
        DEFAULT_ACK_FLUSH_INTERVAL
    } else {
        flush_interval
    };

    let channel_cap = batch_size.saturating_mul(4).max(64);
    let (tx, mut rx) = mpsc::channel::<AckRequest>(channel_cap);

    let handle = tokio::spawn(async move {
        let mut buf: Vec<AckRequest> = Vec::with_capacity(batch_size);

        loop {
            let first = match rx.recv().await {
                Some(req) => req,
                None => break,
            };
            buf.push(first);

            // Greedy synchronous drain — grab everything already queued.
            while buf.len() < batch_size {
                match rx.try_recv() {
                    Ok(req) => buf.push(req),
                    Err(_) => break,
                }
            }

            // If batch full, flush immediately; otherwise wait for more.
            if buf.len() < batch_size {
                let deadline = tokio::time::sleep(flush_interval);
                tokio::pin!(deadline);
                loop {
                    tokio::select! {
                        biased;
                        maybe = rx.recv() => match maybe {
                            Some(req) => {
                                buf.push(req);
                                if buf.len() >= batch_size {
                                    break;
                                }
                            }
                            None => break,
                        },
                        _ = &mut deadline => break,
                    }
                }
            }

            flush_batch(&backend, &stream, &group, &mut buf).await;
        }

        // Final drain after all senders dropped.
        while let Ok(req) = rx.try_recv() {
            buf.push(req);
        }
        if !buf.is_empty() {
            flush_batch(&backend, &stream, &group, &mut buf).await;
        }
    });

    (tx, handle)
}

async fn flush_batch<B: StreamBackend>(
    backend: &Arc<B>,
    stream: &str,
    group: &str,
    buf: &mut Vec<AckRequest>,
) {
    if buf.is_empty() {
        return;
    }

    let ids: Vec<String> = buf.iter().map(|r| r.id.clone()).collect();
    let result = backend.ack_many(stream, group, &ids).await;

    match result {
        Ok(()) => {
            for req in buf.drain(..) {
                let _ = req.done.send(Ok(()));
            }
        }
        Err(err) => {
            let msg = err.to_string();
            for req in buf.drain(..) {
                let _ = req.done.send(Err(EventBusError::Connection(format!(
                    "batched xack on {stream}: {msg}"
                ))));
            }
        }
    }
}
