use std::{sync::Arc, time::Duration};

use chrono::Utc;
use rand::Rng;
use tokio::{
    sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore},
    task::{JoinHandle, JoinSet},
};

use crate::{
    AckMode, Delivery, EventBusError, Handler, Message, PublishOptions, Publisher, Subscriber,
    SubscriptionConfig,
};

use super::{
    ack_flusher::{self, AckRequest},
    backend::{ClaimedMessage, SharedBackend, StreamBackend},
    delivery::StreamDelivery,
    subscription::StreamSubscription,
};

const DEFAULT_PUBLISH_BATCH_PARALLELISM: usize = 32;
const MAX_BACKOFF_CEILING: Duration = Duration::from_secs(5);

type DeliveryTaskResult = Result<(), EventBusError>;

/// Shared runtime state passed to the consumer loop.
///
/// Unified limiter semantics: every message in flight holds exactly one
/// [`OwnedSemaphorePermit`] from `limiter`, for the full handler + ack
/// round-trip. The permit drops with the `Delivery`, so every termination
/// path (success, panic, cancel, orphan) returns the slot automatically.
struct RuntimeState<H> {
    handler: Arc<H>,
    config: Arc<SubscriptionConfig>,
    limiter: Arc<Semaphore>,
    ack_tx: mpsc::Sender<AckRequest>,
}

impl<H> Clone for RuntimeState<H> {
    fn clone(&self) -> Self {
        Self {
            handler: Arc::clone(&self.handler),
            config: Arc::clone(&self.config),
            limiter: Arc::clone(&self.limiter),
            ack_tx: self.ack_tx.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StreamBusOptions {
    pub block_timeout: Duration,
    pub claim_idle_timeout: Duration,
    pub claim_scan_batch_size: usize,
    pub group_start_id: String,
    /// Maximum concurrent backend `publish` calls inside a single
    /// `publish_batch`. Saturating the Redis connection pool gives diminishing
    /// returns; 32 is a sensible default for `MultiplexedConnection`.
    pub publish_batch_parallelism: usize,
    /// Maximum number of ack IDs batched into a single `XACK` command.
    pub ack_batch_size: usize,
    /// Maximum time to wait after the first un-flushed ack before forcing a
    /// flush. Smaller values reduce ack latency; larger values amortize more
    /// round-trips.
    pub ack_flush_interval: Duration,
    /// How often the independent reclaim task checks for idle messages.
    /// Decoupled from `block_timeout` so reclaim latency is predictable
    /// regardless of read polling cadence.
    pub reclaim_interval: Duration,
}

impl Default for StreamBusOptions {
    fn default() -> Self {
        Self {
            block_timeout: Duration::from_secs(2),
            claim_idle_timeout: Duration::from_secs(60),
            claim_scan_batch_size: 64,
            group_start_id: "$".to_string(),
            publish_batch_parallelism: DEFAULT_PUBLISH_BATCH_PARALLELISM,
            ack_batch_size: 64,
            ack_flush_interval: Duration::from_millis(2),
            reclaim_interval: Duration::from_millis(500),
        }
    }
}

impl StreamBusOptions {
    /// Constructs options with all defaults. Equivalent to [`Default::default`],
    /// provided so callers can chain `with_*` methods without importing `Default`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the `XREADGROUP BLOCK` timeout.
    #[must_use]
    pub fn with_block_timeout(mut self, v: Duration) -> Self {
        self.block_timeout = v;
        self
    }

    /// Sets how long a pending entry must sit before `XAUTOCLAIM` reclaims it.
    #[must_use]
    pub fn with_claim_idle_timeout(mut self, v: Duration) -> Self {
        self.claim_idle_timeout = v;
        self
    }

    /// Sets the `XAUTOCLAIM COUNT` per scan.
    #[must_use]
    pub fn with_claim_scan_batch_size(mut self, v: usize) -> Self {
        self.claim_scan_batch_size = v;
        self
    }

    /// Sets the consumer-group start id (`$` = new only, `0` = from history).
    #[must_use]
    pub fn with_group_start_id(mut self, v: impl Into<String>) -> Self {
        self.group_start_id = v.into();
        self
    }

    /// Sets the cap on concurrent backend `publish` calls per `publish_batch`.
    #[must_use]
    pub fn with_publish_batch_parallelism(mut self, v: usize) -> Self {
        self.publish_batch_parallelism = v;
        self
    }

    /// Sets the maximum number of ack ids per `XACK` command.
    #[must_use]
    pub fn with_ack_batch_size(mut self, v: usize) -> Self {
        self.ack_batch_size = v;
        self
    }

    /// Sets the maximum delay before forcing a partial ack batch flush.
    #[must_use]
    pub fn with_ack_flush_interval(mut self, v: Duration) -> Self {
        self.ack_flush_interval = v;
        self
    }

    /// Sets how often the reclaim task scans for idle pending entries.
    #[must_use]
    pub fn with_reclaim_interval(mut self, v: Duration) -> Self {
        self.reclaim_interval = v;
        self
    }

    fn normalize(mut self) -> Result<Self, EventBusError> {
        if self.block_timeout.is_zero() {
            self.block_timeout = Duration::from_secs(2);
        }

        if self.claim_idle_timeout.is_zero() {
            self.claim_idle_timeout = Duration::from_secs(60);
        }

        if self.claim_scan_batch_size == 0 {
            self.claim_scan_batch_size = 64;
        }

        if self.group_start_id.trim().is_empty() {
            self.group_start_id = "$".to_string();
        }

        if self.publish_batch_parallelism == 0 {
            self.publish_batch_parallelism = DEFAULT_PUBLISH_BATCH_PARALLELISM;
        }

        if self.ack_batch_size == 0 {
            self.ack_batch_size = 64;
        }

        if self.ack_flush_interval.is_zero() {
            self.ack_flush_interval = Duration::from_millis(2);
        }

        if self.reclaim_interval.is_zero() {
            self.reclaim_interval = Duration::from_millis(500);
        }

        Ok(self)
    }
}

pub struct StreamBus<B: StreamBackend> {
    backend: SharedBackend<B>,
    options: StreamBusOptions,
}

impl<B: StreamBackend> Clone for StreamBus<B> {
    fn clone(&self) -> Self {
        Self {
            backend: Arc::clone(&self.backend),
            options: self.options.clone(),
        }
    }
}

impl<B: StreamBackend> StreamBus<B> {
    pub fn new(
        backend: SharedBackend<B>,
        options: StreamBusOptions,
    ) -> Result<Self, EventBusError> {
        Ok(Self {
            backend,
            options: options.normalize()?,
        })
    }

    pub async fn publish(&self, msg: Message, opts: PublishOptions) -> Result<(), EventBusError> {
        <Self as Publisher>::publish(self, msg, opts).await
    }

    pub async fn publish_batch<I>(&self, msgs: I, opts: PublishOptions) -> Result<(), EventBusError>
    where
        I: IntoIterator<Item = Message> + Send,
    {
        <Self as Publisher>::publish_batch(self, msgs, opts).await
    }

    pub async fn subscribe<H>(
        &self,
        cfg: SubscriptionConfig,
        handler: H,
    ) -> Result<StreamSubscription, EventBusError>
    where
        H: Handler + Send + Sync + 'static,
    {
        <Self as Subscriber>::subscribe(self, cfg, handler).await
    }

    async fn publish_inner(
        &self,
        message: Message,
        options: &PublishOptions,
    ) -> Result<(), EventBusError> {
        options.validate()?;

        if let Some(delay) = options.delay {
            tokio::time::sleep(delay).await;
        }

        let message = Self::prepare_message(message, options)?;

        let topic = message.topic.clone();
        self.backend.publish(&topic, message).await?;
        Ok(())
    }

    fn prepare_message(
        mut message: Message,
        options: &PublishOptions,
    ) -> Result<Message, EventBusError> {
        if message.topic.trim().is_empty() {
            return Err(EventBusError::Validation(
                "message topic is required".into(),
            ));
        }

        for (key, value) in &options.metadata {
            message.headers.insert(key.clone(), value.clone());
        }

        if let Some(idempotency_key) = options.idempotency_key.as_deref() {
            message.set_idempotency_key(idempotency_key);
        }

        Ok(message)
    }

    async fn consume_loop<H>(
        self,
        mut close_rx: watch::Receiver<bool>,
        runtime: RuntimeState<H>,
        mut reclaim_rx: mpsc::Receiver<Vec<ClaimedMessage>>,
        flusher_handle: JoinHandle<()>,
        reclaim_handle: JoinHandle<()>,
    ) -> Result<(), EventBusError>
    where
        H: Handler + Send + Sync + 'static,
    {
        let mut tasks = JoinSet::new();
        let mut first_delivery_error: Option<EventBusError> = None;
        let mut backoff = BackoffState::new(runtime.config.retry_backoff);

        loop {
            if *close_rx.borrow() {
                break;
            }

            drain_completed_tasks(&mut tasks, &mut first_delivery_error)?;

            let available = runtime.limiter.available_permits();
            if available == 0 {
                if !wait_for_task_or_close(
                    &mut tasks,
                    &mut close_rx,
                    backoff.peek(),
                    &mut first_delivery_error,
                )
                .await
                {
                    break;
                }
                continue;
            }

            let read_limit = runtime
                .config
                .backpressure
                .as_ref()
                .map_or(available, |policy| policy.max_batch_size.max(1))
                .min(available);

            let read_future = self.backend.read_new(
                &runtime.config.topic,
                &runtime.config.consumer_group,
                &runtime.config.consumer_name,
                read_limit,
                self.options.block_timeout,
            );
            tokio::pin!(read_future);

            let mut any_work = false;

            // Select between: close signal, reclaimed messages, new messages.
            // Reclaim results arrive from the independent reclaim task and get
            // spawned immediately — even while read_new is blocked.
            tokio::select! {
                biased;
                changed = close_rx.changed() => {
                    if changed.is_ok() && *close_rx.borrow() {
                        break;
                    }
                    continue;
                }
                Some(reclaimed) = reclaim_rx.recv() => {
                    if !reclaimed.is_empty() {
                        any_work = true;
                        self.spawn_messages(&mut tasks, reclaimed, &runtime).await?;
                    }
                }
                result = &mut read_future => {
                    match result {
                        Ok(messages) if !messages.is_empty() => {
                            any_work = true;
                            self.spawn_messages(&mut tasks, messages, &runtime).await?;
                        }
                        Ok(_) => {}
                        Err(_) => {
                            let sleep_dur = backoff.next();
                            if !sleep_or_close(&mut close_rx, sleep_dur).await {
                                break;
                            }
                            continue;
                        }
                    }
                }
            }

            if any_work {
                backoff.reset();
            }
        }

        // Graceful drain: wait for in-flight tasks to finish.
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    first_delivery_error.get_or_insert(err);
                }
                Err(err) => {
                    first_delivery_error.get_or_insert_with(|| {
                        EventBusError::Internal(format!("delivery task panicked: {err}"))
                    });
                }
            }
        }

        // Drop all senders so the flusher drains its remaining buffer and exits.
        drop(runtime);
        drop(reclaim_rx);
        let _ = reclaim_handle.await;
        let _ = flusher_handle.await;

        if let Some(err) = first_delivery_error {
            return Err(err);
        }

        Ok(())
    }

    async fn spawn_messages<H>(
        &self,
        tasks: &mut JoinSet<DeliveryTaskResult>,
        messages: Vec<ClaimedMessage>,
        runtime: &RuntimeState<H>,
    ) -> Result<(), EventBusError>
    where
        H: Handler + Send + Sync + 'static,
    {
        for claimed in messages {
            let permit = runtime
                .limiter
                .clone()
                .acquire_owned()
                .await
                .map_err(|err| {
                    EventBusError::Internal(format!("in-flight limiter closed: {err}"))
                })?;
            let bus = self.clone();
            let config = Arc::clone(&runtime.config);
            let handler = Arc::clone(&runtime.handler);
            let ack_tx = runtime.ack_tx.clone();
            tasks.spawn(async move {
                bus.process_single_message(config, handler, claimed, permit, ack_tx)
                    .await
            });
        }
        Ok(())
    }

    async fn process_single_message<H>(
        &self,
        config: Arc<SubscriptionConfig>,
        handler: Arc<H>,
        claimed: ClaimedMessage,
        permit: OwnedSemaphorePermit,
        ack_tx: mpsc::Sender<AckRequest>,
    ) -> Result<(), EventBusError>
    where
        H: Handler + Send + Sync + 'static,
    {
        let mut state = claimed.state;
        state.max_attempt = config.max_retry.max(1) as u32;

        let ack_mode = config.ack_mode;
        let delivery = StreamDelivery::new(
            Arc::clone(&self.backend),
            ack_tx,
            claimed.id,
            claimed.message,
            state,
            config,
            permit,
        );

        if ack_mode == AckMode::AutoOnReceive {
            delivery.ack().await?;
        }

        match handler.handle(&delivery).await {
            Ok(()) => {
                if ack_mode == AckMode::AutoOnHandlerSuccess && !delivery.is_finalized() {
                    delivery.ack().await?;
                }
            }
            Err(err) => {
                if ack_mode == AckMode::AutoOnHandlerSuccess && !delivery.is_finalized() {
                    delivery.retry(&err).await?;
                }
            }
        }

        Ok(())
    }
}

impl<B: StreamBackend> Publisher for StreamBus<B> {
    async fn publish(&self, msg: Message, opts: PublishOptions) -> Result<(), EventBusError> {
        self.publish_inner(msg, &opts).await
    }

    async fn publish_batch<I>(&self, msgs: I, opts: PublishOptions) -> Result<(), EventBusError>
    where
        I: IntoIterator<Item = Message> + Send,
    {
        opts.validate()?;

        if let Some(delay) = opts.delay {
            tokio::time::sleep(delay).await;
        }

        let prepared_messages = msgs
            .into_iter()
            .map(|message| Self::prepare_message(message, &opts))
            .collect::<Result<Vec<_>, _>>()?;

        let parallelism = prepared_messages
            .len()
            .clamp(1, self.options.publish_batch_parallelism);
        let mut pending = prepared_messages.into_iter();
        let mut tasks = JoinSet::new();
        let mut first_error = None;

        for _ in 0..parallelism {
            let Some(message) = pending.next() else {
                break;
            };
            let backend = Arc::clone(&self.backend);
            tasks.spawn(async move {
                let topic = message.topic.clone();
                backend.publish(&topic, message).await.map(|_| ())
            });
        }

        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
                Err(err) => {
                    if first_error.is_none() {
                        first_error = Some(EventBusError::Internal(format!(
                            "publish batch task failed: {err}"
                        )));
                    }
                }
            }

            if first_error.is_none() {
                if let Some(message) = pending.next() {
                    let backend = Arc::clone(&self.backend);
                    tasks.spawn(async move {
                        let topic = message.topic.clone();
                        backend.publish(&topic, message).await.map(|_| ())
                    });
                }
            }
        }

        if let Some(err) = first_error {
            return Err(err);
        }

        Ok(())
    }
}

impl<B: StreamBackend> Subscriber for StreamBus<B> {
    type Sub = StreamSubscription;

    async fn subscribe<H>(
        &self,
        mut cfg: SubscriptionConfig,
        handler: H,
    ) -> Result<Self::Sub, EventBusError>
    where
        H: Handler + Send + Sync + 'static,
    {
        if cfg.topic.trim().is_empty() {
            return Err(EventBusError::Validation(
                "subscription topic is required".into(),
            ));
        }
        if cfg.consumer_group.trim().is_empty() {
            return Err(EventBusError::Validation(
                "consumer group is required".into(),
            ));
        }
        if cfg.consumer_name.trim().is_empty() {
            cfg.consumer_name = format!(
                "consumer-{}",
                Utc::now().timestamp_nanos_opt().unwrap_or_default()
            );
        }

        cfg.normalize_and_validate()?;

        if cfg.balance_mode == Some(crate::ConsumerBalanceMode::FanOut) {
            return Err(EventBusError::Validation(
                "FanOut balance mode is not yet supported by StreamBus".into(),
            ));
        }

        self.backend
            .create_group(
                &cfg.topic,
                &cfg.consumer_group,
                &self.options.group_start_id,
            )
            .await?;

        let (close_tx, close_rx) = watch::channel(false);
        let limit = cfg.max_pending_acks.max(cfg.max_in_flight.max(1));
        let consumer_name = cfg.consumer_name.clone();

        let stream = cfg.topic.clone();
        let group = cfg.consumer_group.clone();
        let (ack_tx, flusher_handle) = ack_flusher::spawn(
            Arc::clone(&self.backend),
            stream,
            group,
            self.options.ack_batch_size,
            self.options.ack_flush_interval,
        );

        let limiter = Arc::new(Semaphore::new(limit));

        // Independent reclaim task — sends batches back to the consume loop.
        let (reclaim_tx, reclaim_rx) = mpsc::channel::<Vec<ClaimedMessage>>(4);
        let reclaim_handle = tokio::spawn({
            let bus = self.clone();
            let close_rx = close_rx.clone();
            let limiter = Arc::clone(&limiter);
            let config_topic = cfg.topic.clone();
            let config_group = cfg.consumer_group.clone();
            let config_consumer = cfg.consumer_name.clone();
            let claim_idle_timeout = self.options.claim_idle_timeout;
            let claim_scan_batch_size = self.options.claim_scan_batch_size;
            let reclaim_interval = self.options.reclaim_interval;
            async move {
                reclaim_loop(
                    bus.backend,
                    close_rx,
                    limiter,
                    reclaim_tx,
                    config_topic,
                    config_group,
                    config_consumer,
                    claim_idle_timeout,
                    claim_scan_batch_size,
                    reclaim_interval,
                )
                .await;
            }
        });

        let runtime = RuntimeState {
            handler: Arc::new(handler),
            config: Arc::new(cfg),
            limiter,
            ack_tx,
        };

        let task = tokio::spawn({
            let bus = self.clone();
            let runtime = runtime.clone();
            async move {
                bus.consume_loop(
                    close_rx,
                    runtime,
                    reclaim_rx,
                    flusher_handle,
                    reclaim_handle,
                )
                .await
            }
        });

        drop(runtime);

        Ok(StreamSubscription::new(consumer_name, close_tx, task))
    }
}

// ---------------------------------------------------------------------------
// Reclaim loop (independent task)
// ---------------------------------------------------------------------------

async fn reclaim_loop<B: StreamBackend>(
    backend: SharedBackend<B>,
    mut close_rx: watch::Receiver<bool>,
    limiter: Arc<Semaphore>,
    reclaim_tx: mpsc::Sender<Vec<ClaimedMessage>>,
    topic: String,
    group: String,
    consumer: String,
    claim_idle_timeout: Duration,
    claim_scan_batch_size: usize,
    reclaim_interval: Duration,
) {
    let mut backoff = BackoffState::new(Duration::from_millis(100));

    loop {
        if !sleep_or_close(&mut close_rx, reclaim_interval).await {
            break;
        }

        let available = limiter.available_permits();
        if available == 0 {
            continue;
        }

        let count = available.min(claim_scan_batch_size);
        match backend
            .reclaim_idle(&topic, &group, &consumer, claim_idle_timeout, count)
            .await
        {
            Ok(messages) => {
                if !messages.is_empty() && reclaim_tx.send(messages).await.is_err() {
                    break;
                }
                backoff.reset();
            }
            Err(_) => {
                let dur = backoff.next();
                if !sleep_or_close(&mut close_rx, dur).await {
                    break;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Drain synchronously-ready completed tasks without an executor roundtrip.
fn drain_completed_tasks(
    tasks: &mut JoinSet<DeliveryTaskResult>,
    first_delivery_error: &mut Option<EventBusError>,
) -> Result<(), EventBusError> {
    while let Some(result) = tasks.try_join_next() {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                first_delivery_error.get_or_insert(err);
            }
            Err(err) => {
                return Err(EventBusError::Internal(format!(
                    "delivery task panicked: {err}"
                )));
            }
        }
    }
    Ok(())
}

/// Exponential backoff with full jitter, capped at [`MAX_BACKOFF_CEILING`].
struct BackoffState {
    base: Duration,
    current: Duration,
}

impl BackoffState {
    fn new(base: Duration) -> Self {
        let base = if base.is_zero() {
            Duration::from_millis(100)
        } else {
            base
        };
        Self {
            base,
            current: base,
        }
    }

    fn peek(&self) -> Duration {
        self.base
    }

    fn next(&mut self) -> Duration {
        let dur = self.current;
        let next_raw = dur.saturating_mul(2).min(MAX_BACKOFF_CEILING);
        self.current = next_raw;

        let jitter_nanos = rand::thread_rng().gen_range(0..=dur.as_nanos() as u64);
        Duration::from_nanos(jitter_nanos)
            .saturating_add(dur / 2)
            .min(MAX_BACKOFF_CEILING)
    }

    fn reset(&mut self) {
        self.current = self.base;
    }
}

async fn sleep_or_close(close_rx: &mut watch::Receiver<bool>, duration: Duration) -> bool {
    tokio::select! {
        changed = close_rx.changed() => {
            if changed.is_err() {
                false
            } else {
                !*close_rx.borrow()
            }
        }
        _ = tokio::time::sleep(duration) => true,
    }
}

async fn wait_for_task_or_close(
    tasks: &mut JoinSet<DeliveryTaskResult>,
    close_rx: &mut watch::Receiver<bool>,
    duration: Duration,
    first_delivery_error: &mut Option<EventBusError>,
) -> bool {
    if tasks.is_empty() {
        return sleep_or_close(close_rx, duration).await;
    }

    tokio::select! {
        changed = close_rx.changed() => {
            if changed.is_err() {
                false
            } else {
                !*close_rx.borrow()
            }
        }
        result = tasks.join_next() => match result {
            Some(Ok(Ok(()))) | None => true,
            Some(Ok(Err(err))) => {
                first_delivery_error.get_or_insert(err);
                true
            }
            Some(Err(err)) => {
                first_delivery_error.get_or_insert_with(|| {
                    EventBusError::Internal(format!("delivery task failed: {err}"))
                });
                true
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{BackoffState, StreamBusOptions, MAX_BACKOFF_CEILING};

    #[test]
    fn zero_duration_options_normalize_to_defaults() {
        let normalized = StreamBusOptions {
            block_timeout: Duration::ZERO,
            claim_idle_timeout: Duration::ZERO,
            claim_scan_batch_size: 0,
            group_start_id: String::new(),
            publish_batch_parallelism: 0,
            ack_batch_size: 0,
            ack_flush_interval: Duration::ZERO,
            reclaim_interval: Duration::ZERO,
        }
        .normalize()
        .expect("normalize options");

        assert_eq!(normalized.block_timeout, Duration::from_secs(2));
        assert_eq!(normalized.claim_idle_timeout, Duration::from_secs(60));
        assert_eq!(normalized.claim_scan_batch_size, 64);
        assert_eq!(normalized.group_start_id, "$".to_string());
        assert_eq!(normalized.publish_batch_parallelism, 32);
        assert_eq!(normalized.ack_batch_size, 64);
        assert_eq!(normalized.ack_flush_interval, Duration::from_millis(2));
        assert_eq!(normalized.reclaim_interval, Duration::from_millis(500));
    }

    #[test]
    fn backoff_grows_exponentially_and_caps() {
        let mut backoff = BackoffState::new(Duration::from_millis(100));
        for _ in 0..20 {
            let dur = backoff.next();
            assert!(dur <= MAX_BACKOFF_CEILING);
        }
        backoff.reset();
        let first = backoff.next();
        assert!(first <= MAX_BACKOFF_CEILING);
    }
}
