use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::Utc;
use tokio::{
    sync::{watch, Mutex, OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
};

use crate::{
    AckMode, Delivery, EventBusError, Handler, Message, PublishOptions, Publisher, Subscriber,
    SubscriptionConfig,
};

use super::{
    backend::{ClaimedMessage, SharedBackend, StreamBackend},
    delivery::RedisStreamDelivery,
    subscription::RedisStreamSubscription,
};

const PUBLISH_BATCH_PARALLELISM: usize = 32;

type DeliveryTaskResult = Result<(), EventBusError>;

#[derive(Debug)]
pub(super) struct PendingAckTracker {
    permits: Arc<Semaphore>,
    permits_by_message: Mutex<HashMap<String, OwnedSemaphorePermit>>,
}

impl PendingAckTracker {
    fn new(limit: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(limit)),
            permits_by_message: Mutex::new(HashMap::new()),
        }
    }

    fn available_permits(&self) -> usize {
        self.permits.available_permits()
    }

    async fn track_new(&self, message_id: String) -> Result<(), EventBusError> {
        let permit =
            self.permits.clone().acquire_owned().await.map_err(|err| {
                EventBusError::Internal(format!("pending ack tracker closed: {err}"))
            })?;
        self.permits_by_message
            .lock()
            .await
            .insert(message_id, permit);
        Ok(())
    }

    pub(super) async fn release(&self, message_id: &str) {
        self.permits_by_message.lock().await.remove(message_id);
    }
}

struct RuntimeState<H> {
    handler: Arc<H>,
    in_flight_limiter: Arc<Semaphore>,
    pending_ack_tracker: Arc<PendingAckTracker>,
}

#[derive(Debug, Clone)]
pub struct RedisStreamBusOptions {
    pub block_timeout: Duration,
    pub claim_idle_timeout: Duration,
    pub claim_scan_batch_size: usize,
    pub group_start_id: String,
}

impl Default for RedisStreamBusOptions {
    fn default() -> Self {
        Self {
            block_timeout: Duration::from_secs(2),
            claim_idle_timeout: Duration::from_secs(60),
            claim_scan_batch_size: 64,
            group_start_id: "$".to_string(),
        }
    }
}

impl RedisStreamBusOptions {
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

        Ok(self)
    }
}

pub struct RedisStreamBus<B: StreamBackend> {
    backend: SharedBackend<B>,
    options: RedisStreamBusOptions,
}

impl<B: StreamBackend> Clone for RedisStreamBus<B> {
    fn clone(&self) -> Self {
        Self {
            backend: Arc::clone(&self.backend),
            options: self.options.clone(),
        }
    }
}

impl<B: StreamBackend> RedisStreamBus<B> {
    pub fn new(
        backend: SharedBackend<B>,
        options: RedisStreamBusOptions,
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
    ) -> Result<RedisStreamSubscription, EventBusError>
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
        config: SubscriptionConfig,
        mut close_rx: watch::Receiver<bool>,
        runtime: RuntimeState<H>,
    ) -> Result<(), EventBusError>
    where
        H: Handler + Send + Sync + 'static,
    {
        let mut tasks = JoinSet::new();
        // Delivery infrastructure errors (e.g. ack failure) do not kill the consumer
        // loop — the message remains in the PEL and will be re-claimed.  We collect
        // the first such error here and surface it when the subscription closes.
        let mut first_delivery_error: Option<EventBusError> = None;

        loop {
            if *close_rx.borrow() {
                break;
            }

            self.drain_completed_tasks(&mut tasks, &mut first_delivery_error).await?;

            let available_in_flight = runtime.in_flight_limiter.available_permits();
            if available_in_flight == 0 {
                if !wait_for_task_or_close(&mut tasks, &mut close_rx, config.retry_backoff).await? {
                    break;
                }
                continue;
            }

            let reclaim_limit = available_in_flight.min(self.options.claim_scan_batch_size);
            if reclaim_limit > 0 {
                match self
                    .backend
                    .reclaim_idle(
                        &config.topic,
                        &config.consumer_group,
                        &config.consumer_name,
                        self.options.claim_idle_timeout,
                        reclaim_limit,
                    )
                    .await
                {
                    Ok(messages) => {
                        self.spawn_claimed_messages(&mut tasks, &config, messages, true, &runtime)
                            .await?;
                    }
                    Err(_) => {
                        if !sleep_or_close(&mut close_rx, config.retry_backoff).await {
                            break;
                        }
                        continue;
                    }
                }
            }

            let read_limit = config
                .backpressure
                .as_ref()
                .map_or(1, |policy| policy.max_batch_size)
                .min(runtime.in_flight_limiter.available_permits())
                .min(runtime.pending_ack_tracker.available_permits());

            if read_limit == 0 {
                if !wait_for_task_or_close(&mut tasks, &mut close_rx, config.retry_backoff).await? {
                    break;
                }
                continue;
            }

            let read_future = self.backend.read_new(
                &config.topic,
                &config.consumer_group,
                &config.consumer_name,
                read_limit,
                self.options.block_timeout,
            );
            tokio::pin!(read_future);
            let new_messages = match tokio::select! {
                biased;
                changed = close_rx.changed() => {
                    if changed.is_ok() && *close_rx.borrow() {
                        break;
                    }
                    continue;
                }
                result = &mut read_future => result,
            } {
                Ok(messages) => messages,
                Err(_) => {
                    if !sleep_or_close(&mut close_rx, config.retry_backoff).await {
                        break;
                    }
                    continue;
                }
            };

            if let Err(err) = self
                .spawn_claimed_messages(&mut tasks, &config, new_messages, true, &runtime)
                .await
            {
                if !sleep_or_close(&mut close_rx, config.retry_backoff).await {
                    break;
                }
                return Err(err);
            }
        }

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

        if let Some(err) = first_delivery_error {
            return Err(err);
        }

        Ok(())
    }

    async fn drain_completed_tasks(
        &self,
        tasks: &mut JoinSet<DeliveryTaskResult>,
        first_delivery_error: &mut Option<EventBusError>,
    ) -> Result<(), EventBusError> {
        loop {
            match tokio::time::timeout(Duration::ZERO, tasks.join_next()).await {
                Ok(Some(Ok(Ok(())))) => {}
                // Delivery infrastructure errors (ack/retry failure) don't kill the loop.
                // Collect them so they're surfaced when the subscription closes.
                Ok(Some(Ok(Err(err)))) => {
                    first_delivery_error.get_or_insert(err);
                }
                // Task panics are programming errors — propagate them immediately.
                Ok(Some(Err(err))) => {
                    return Err(EventBusError::Internal(format!(
                        "delivery task panicked: {err}"
                    )))
                }
                Ok(None) | Err(_) => return Ok(()),
            }
        }
    }

    async fn spawn_claimed_messages<H>(
        &self,
        tasks: &mut JoinSet<DeliveryTaskResult>,
        config: &SubscriptionConfig,
        messages: Vec<ClaimedMessage>,
        reserve_pending_ack: bool,
        runtime: &RuntimeState<H>,
    ) -> Result<(), EventBusError>
    where
        H: Handler + Send + Sync + 'static,
    {
        for claimed in messages {
            if reserve_pending_ack {
                runtime
                    .pending_ack_tracker
                    .track_new(claimed.id.clone())
                    .await?;
            }

            let in_flight_permit = match runtime.in_flight_limiter.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(err) => {
                    // Release the pending ack slot we just reserved to avoid leaking it.
                    if reserve_pending_ack {
                        runtime.pending_ack_tracker.release(&claimed.id).await;
                    }
                    return Err(EventBusError::Internal(format!(
                        "in-flight limiter closed: {err}"
                    )));
                }
            };
            let bus = self.clone();
            let handler = Arc::clone(&runtime.handler);
            let config = config.clone();
            let pending_ack_tracker = Arc::clone(&runtime.pending_ack_tracker);

            tasks.spawn(async move {
                let _in_flight_permit = in_flight_permit;
                bus.process_single_message(&config, &handler, claimed, pending_ack_tracker)
                    .await
            });
        }

        Ok(())
    }

    async fn process_single_message<H>(
        &self,
        config: &SubscriptionConfig,
        handler: &Arc<H>,
        claimed: ClaimedMessage,
        pending_ack_tracker: Arc<PendingAckTracker>,
    ) -> Result<(), EventBusError>
    where
        H: Handler + Send + Sync + 'static,
    {
        let mut state = claimed.state;
        state.max_attempt = config.max_retry.max(1) as u32;

        let delivery = RedisStreamDelivery::new(
            Arc::clone(&self.backend),
            claimed.id,
            claimed.message,
            state,
            config.clone(),
            pending_ack_tracker,
        );

        if config.ack_mode == AckMode::AutoOnReceive {
            delivery.ack().await?;
        }

        match handler.handle(&delivery).await {
            Ok(()) => {
                if config.ack_mode == AckMode::AutoOnHandlerSuccess && !delivery.is_finalized() {
                    delivery.ack().await?;
                }
            }
            Err(err) => {
                if config.ack_mode == AckMode::AutoOnHandlerSuccess && !delivery.is_finalized() {
                    delivery.retry(&err).await?;
                }
            }
        }

        Ok(())
    }
}

impl<B: StreamBackend> Publisher for RedisStreamBus<B> {
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

        let parallelism = prepared_messages.len().clamp(1, PUBLISH_BATCH_PARALLELISM);
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

impl<B: StreamBackend> Subscriber for RedisStreamBus<B> {
    type Sub = RedisStreamSubscription;

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
                "FanOut balance mode is not yet supported by RedisStreamBus".into(),
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
        let in_flight_limit = cfg.concurrency.max(1).min(cfg.max_in_flight.max(1));
        let runtime = RuntimeState {
            handler: Arc::new(handler),
            in_flight_limiter: Arc::new(Semaphore::new(in_flight_limit)),
            pending_ack_tracker: Arc::new(PendingAckTracker::new(cfg.max_pending_acks.max(1))),
        };

        let task = tokio::spawn({
            let bus = self.clone();
            let cfg = cfg.clone();
            let handler = Arc::clone(&runtime.handler);
            let in_flight_limiter = Arc::clone(&runtime.in_flight_limiter);
            let pending_ack_tracker = Arc::clone(&runtime.pending_ack_tracker);
            async move {
                bus.consume_loop(
                    cfg,
                    close_rx,
                    RuntimeState {
                        handler,
                        in_flight_limiter,
                        pending_ack_tracker,
                    },
                )
                .await
            }
        });

        Ok(RedisStreamSubscription::new(
            cfg.consumer_name.clone(),
            close_tx,
            task,
        ))
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
) -> Result<bool, EventBusError> {
    if tasks.is_empty() {
        return Ok(sleep_or_close(close_rx, duration).await);
    }

    tokio::select! {
        changed = close_rx.changed() => {
            if changed.is_err() {
                Ok(false)
            } else {
                Ok(!*close_rx.borrow())
            }
        }
        result = tasks.join_next() => match result {
            Some(Ok(Ok(()))) | None => Ok(true),
            Some(Ok(Err(err))) => Err(err),
            Some(Err(err)) => Err(EventBusError::Internal(format!(
                "delivery task failed: {err}"
            ))),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::RedisStreamBusOptions;

    #[test]
    fn zero_duration_options_normalize_to_go_defaults() {
        let normalized = RedisStreamBusOptions {
            block_timeout: Duration::ZERO,
            claim_idle_timeout: Duration::ZERO,
            claim_scan_batch_size: 0,
            group_start_id: String::new(),
        }
        .normalize()
        .expect("normalize options");

        assert_eq!(normalized.block_timeout, Duration::from_secs(2));
        assert_eq!(normalized.claim_idle_timeout, Duration::from_secs(60));
        assert_eq!(normalized.claim_scan_batch_size, 64);
        assert_eq!(normalized.group_start_id, "$".to_string());
    }
}
