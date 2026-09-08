//! These generic bodies are compile-time regressions: a concrete implementation
//! alone cannot prove that the public trait promises Send futures to callers.
use eventbus_outbox::{
    AppendRequest, DeadLetterMessageRecord, DeadLetterStore, Dispatcher, IdempotencyClaim,
    IdempotencyClaimStore, IdempotencyStore, Listener, Notifier, OutboxStore, StateTransitionStore,
    TransitionInput,
};

fn require_send(_: impl Send) {}

#[allow(dead_code)]
fn worker_contract<D: Dispatcher, N: Notifier, L: Listener>(d: &D, n: &N, l: &L) {
    require_send(d.start());
    require_send(d.stop());
    require_send(d.dispatch_once());
    require_send(n.notify("channel", "payload"));
    require_send(l.listen("channel"));
    require_send(l.recv());
    require_send(l.close());
}

#[allow(dead_code)]
fn store_contract<S: OutboxStore>(s: &S, request: AppendRequest) {
    require_send(s.append(request));
    require_send(s.append_batch(Vec::new()));
    require_send(s.lock_pending("worker", 1, chrono::Utc::now()));
    require_send(s.mark_sent(&[], chrono::Utc::now()));
    require_send(s.mark_failed("id", None, "failure"));
    require_send(s.mark_dead("id", "failure"));
    require_send(s.release_stale_locks(std::time::Duration::from_secs(60), chrono::Utc::now()));
}

#[allow(dead_code)]
fn transition_contract<S: StateTransitionStore, D: DeadLetterStore>(
    s: &S,
    d: &D,
    input: TransitionInput,
    message: DeadLetterMessageRecord,
) {
    require_send(s.transition(input));
    require_send(d.append_dead_letter(message));
}

#[allow(dead_code)]
fn idempotency_contract<S: IdempotencyStore, C: IdempotencyClaimStore>(
    s: &S,
    c: &C,
    claim: IdempotencyClaim,
) {
    require_send(s.is_processed("group", "id"));
    require_send(s.mark_processed("group", "id"));
    require_send(c.claim(claim));
    require_send(c.complete("group", "id"));
    require_send(c.release("group", "id"));
}
