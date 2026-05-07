//! Helper that lets the consume loop finalize a delivery if the handler
//! returned without doing so itself. Used by `AckMode::AutoOnHandlerSuccess`.
//!
//! The handler receives a `Box<AutoFinalizeProxy>` typed as
//! `Box<dyn DeliveryHandle>`. Internally the proxy holds the *real* boxed
//! delivery in a `Mutex<Option<...>>` shared with the consume loop.
//!
//! - If the handler calls `ack()`/`nack()`/`retry()` on the proxy, the proxy
//!   takes the inner box and forwards the call. Subsequent attempts are
//!   compile-prevented (the proxy box was consumed).
//! - If the handler returns without finalizing, the consume loop calls
//!   `tracker.take_remaining()` to retrieve the inner box and finalize itself.
//!
//! The shared `Arc<Mutex<Option<...>>>` is the only piece of "state shared
//! between handler and loop" — kept tiny to make the invariant easy to audit.

use std::sync::{Arc, Mutex};

use crate::{
    BoxFuture, BoxedError, Delivery, DeliveryControl, DeliveryHandle, DeliveryInspector,
    DeliveryState, EventBusError, Message,
};

type SharedHandle = Arc<Mutex<Option<Box<dyn DeliveryHandle>>>>;

/// Handle held by the consume loop. After the handler returns, the loop
/// pulls any unfinalized inner box back via [`take_remaining`].
pub(super) struct AutoFinalizeTracker {
    inner: SharedHandle,
}

impl AutoFinalizeTracker {
    pub(super) async fn new(
        boxed: Box<dyn DeliveryHandle>,
    ) -> Result<(Self, AutoFinalizeProxy), EventBusError> {
        // Snapshot message + state so the proxy's `message()` / `state()`
        // can serve reads without touching the inner box (which may have
        // been moved out for finalization).
        let msg_snapshot = Arc::new(boxed.message().clone());
        let state_snapshot = boxed.state().await?;
        let inner: SharedHandle = Arc::new(Mutex::new(Some(boxed)));
        let tracker = Self {
            inner: Arc::clone(&inner),
        };
        let proxy = AutoFinalizeProxy {
            inner,
            msg_snapshot,
            state_snapshot,
        };
        Ok((tracker, proxy))
    }

    /// Pull out the inner box if the handler did not finalize. Returns
    /// `None` if the handler already consumed it.
    pub(super) fn take_remaining(&self) -> Option<Box<dyn DeliveryHandle>> {
        self.inner.lock().expect("auto-finalize lock").take()
    }
}

/// Proxy `DeliveryHandle` given to the user handler in `AutoOnHandlerSuccess`
/// mode. Forwarding `ack`/`nack`/`retry` consumes the proxy's box and the
/// inner real box.
pub(super) struct AutoFinalizeProxy {
    inner: SharedHandle,
    msg_snapshot: Arc<Message>,
    state_snapshot: DeliveryState,
}

impl AutoFinalizeProxy {
    fn take(&self) -> Option<Box<dyn DeliveryHandle>> {
        self.inner.lock().expect("auto-finalize lock").take()
    }
}

impl Delivery for AutoFinalizeProxy {
    fn message(&self) -> &Message {
        &self.msg_snapshot
    }
}

impl DeliveryInspector for AutoFinalizeProxy {
    fn state(&self) -> BoxFuture<'_, Result<DeliveryState, EventBusError>> {
        let snap = self.state_snapshot.clone();
        Box::pin(async move { Ok(snap) })
    }
}

impl DeliveryControl for AutoFinalizeProxy {
    fn ack(self: Box<Self>) -> BoxFuture<'static, Result<(), EventBusError>> {
        Box::pin(async move {
            match self.take() {
                Some(b) => b.ack().await,
                None => Ok(()),
            }
        })
    }

    fn nack(
        self: Box<Self>,
        reason: BoxedError,
    ) -> BoxFuture<'static, Result<(), EventBusError>> {
        Box::pin(async move {
            match self.take() {
                Some(b) => b.nack(reason).await,
                None => Ok(()),
            }
        })
    }

    fn retry(
        self: Box<Self>,
        reason: BoxedError,
    ) -> BoxFuture<'static, Result<(), EventBusError>> {
        Box::pin(async move {
            match self.take() {
                Some(b) => b.retry(reason).await,
                None => Ok(()),
            }
        })
    }
}
