use std::sync::Arc;

use anyhow::Result;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct UserInteractionRequest {
    pub tool: String,
    pub input: Value,
}

pub type UserInteractionHandler =
    Arc<dyn Fn(&UserInteractionRequest) -> Result<Value> + Send + Sync>;

/// Lifecycle observer for a blocking user interaction.
///
/// The observer is intentionally generic: terminal services can pause work-only
/// behavior while a permission, question, or approval dialog is waiting for the
/// user without coupling the interaction layer to a particular renderer.
#[derive(Clone)]
pub struct InteractionWaitObserver {
    begin: Arc<dyn Fn() + Send + Sync>,
    end: Arc<dyn Fn() + Send + Sync>,
}

impl InteractionWaitObserver {
    pub fn new(
        begin: impl Fn() + Send + Sync + 'static,
        end: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        Self {
            begin: Arc::new(begin),
            end: Arc::new(end),
        }
    }

    pub fn enter(&self) -> InteractionWaitGuard {
        (self.begin)();
        InteractionWaitGuard {
            observer: Some(self.clone()),
        }
    }
}

/// Ensures the matching interaction-end callback runs on every return path.
#[must_use]
pub struct InteractionWaitGuard {
    observer: Option<InteractionWaitObserver>,
}

impl Drop for InteractionWaitGuard {
    fn drop(&mut self) {
        if let Some(observer) = self.observer.take() {
            (observer.end)();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[test]
    fn wait_observer_balances_nested_guards_and_early_returns() {
        let waiting = Arc::new(AtomicUsize::new(0));
        let started = Arc::clone(&waiting);
        let ended = Arc::clone(&waiting);
        let observer = InteractionWaitObserver::new(
            move || {
                started.fetch_add(1, Ordering::SeqCst);
            },
            move || {
                ended.fetch_sub(1, Ordering::SeqCst);
            },
        );

        let outer = observer.enter();
        assert_eq!(waiting.load(Ordering::SeqCst), 1);
        {
            let _inner = observer.enter();
            assert_eq!(waiting.load(Ordering::SeqCst), 2);
        }
        assert_eq!(waiting.load(Ordering::SeqCst), 1);
        drop(outer);
        assert_eq!(waiting.load(Ordering::SeqCst), 0);
    }
}
