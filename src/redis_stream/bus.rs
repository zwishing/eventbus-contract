use std::{sync::Arc, thread::JoinHandle, time::Duration};

use chrono::Utc;

use tokio::sync::watch;

use crate::{
    AckMode, Delivery, EventBusError, Handler, Message, PublishOptions, Publisher, Subscriber,
    SubscriptionConfig,
};

use super::{
    backend::{ClaimedMessage, SharedBackend, StreamBackend},
    delivery::RedisStreamDelivery,
    subscription::RedisStreamSubscription,
};

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
        handler: Arc<H>,
        mut close_rx: watch::Receiver<bool>,
        consumer_name: String,
    ) where
        H: Handler + Send + Sync + 'static,
    {
        loop {
            if *close_rx.borrow() {
                break;
            }

            match self
                .backend
                .reclaim_idle(
                    &config.topic,
                    &config.consumer_group,
                    &consumer_name,
                    self.options.claim_idle_timeout,
                    self.options.claim_scan_batch_size,
                )
                .await
            {
                Ok(messages) => {
                    if self
                        .process_claimed_messages(&config, &handler, messages, true)
                        .await
                        .is_err()
                    {
                        if !sleep_or_close(&mut close_rx, config.retry_backoff).await {
                            break;
                        }
                        continue;
                    }
                }
                Err(_) => {
                    if !sleep_or_close(&mut close_rx, config.retry_backoff).await {
                        break;
                    }
                    continue;
                }
            }

            let batch_size = config
                .backpressure
                .as_ref()
                .map_or(config.max_in_flight.max(1), |policy| {
                    policy.max_batch_size.max(1)
                });

            let new_messages = match self
                .backend
                .read_new(
                    &config.topic,
                    &config.consumer_group,
                    &consumer_name,
                    batch_size,
                )
                .await
            {
                Ok(messages) => messages,
                Err(_) => {
                    if !sleep_or_close(&mut close_rx, config.retry_backoff).await {
                        break;
                    }
                    continue;
                }
            };

            if new_messages.is_empty() {
                tokio::select! {
                    _ = close_rx.changed() => {
                        if *close_rx.borrow() {
                            break;
                        }
                    }
                    _ = self.backend.wait_for_messages(self.options.block_timeout) => {}
                }
                continue;
            }

            let _ = self
                .process_claimed_messages(&config, &handler, new_messages, false)
                .await;
        }
    }

    async fn process_claimed_messages<H>(
        &self,
        config: &SubscriptionConfig,
        handler: &Arc<H>,
        messages: Vec<ClaimedMessage>,
        stop_on_error: bool,
    ) -> Result<(), EventBusError>
    where
        H: Handler + Send + Sync + 'static,
    {
        for claimed in messages {
            if let Err(err) = self.process_single_message(config, handler, claimed).await {
                if stop_on_error {
                    return Err(err);
                }
            }
        }

        Ok(())
    }

    async fn process_single_message<H>(
        &self,
        config: &SubscriptionConfig,
        handler: &Arc<H>,
        claimed: ClaimedMessage,
    ) -> Result<(), EventBusError>
    where
        H: Handler + Send + Sync + 'static,
    {
        let mut state = claimed.state;
        state.max_attempt = config.max_retry.max(1) as u32;

        let delivery = RedisStreamDelivery::new(
            Arc::clone(&self.backend),
            config.topic.clone(),
            config.consumer_group.clone(),
            claimed.id,
            claimed.message,
            state,
            config.clone(),
        );

        if config.ack_mode == Some(AckMode::AutoOnReceive) {
            delivery.ack().await?;
        }

        match handler.handle(&delivery).await {
            Ok(()) => {
                if config.ack_mode == Some(AckMode::AutoOnHandlerSuccess)
                    && !delivery.is_finalized()
                {
                    delivery.ack().await?;
                }
            }
            Err(err) => {
                if config.ack_mode == Some(AckMode::AutoOnHandlerSuccess)
                    && !delivery.is_finalized()
                {
                    delivery.retry(&err).await?;
                } else {
                    return Err(err);
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

        for message in prepared_messages {
            let topic = message.topic.clone();
            self.backend.publish(&topic, message).await?;
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

        let worker_count = cfg.concurrency.max(1);
        let handler = Arc::new(handler);
        let (close_tx, close_rx) = watch::channel(false);

        let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(worker_count);
        for worker_index in 0..worker_count {
            let bus = self.clone();
            let handler = Arc::clone(&handler);
            let consumer_name = if worker_count == 1 {
                cfg.consumer_name.clone()
            } else {
                format!("{}-{worker_index}", cfg.consumer_name)
            };
            let close_rx = close_rx.clone();
            let cfg_clone = cfg.clone();
            handles.push(std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build redis stream subscription runtime");
                runtime.block_on(async move {
                    bus.consume_loop(cfg_clone, handler, close_rx, consumer_name)
                        .await;
                });
            }));
        }

        Ok(RedisStreamSubscription::new(
            cfg.consumer_name.clone(),
            close_tx,
            handles,
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
