use std::{sync::Arc, time::Duration};

/// Gates are a concurrency helper, primarily used for implementing safe shutdown.
///
/// Users of a resource call `enter()` to acquire a GateGuard, and the owner of
/// the resource calls `close()` when they want to ensure that all holders of guards
/// have released them, and that no future guards will be issued.
pub struct Gate {
    /// Each caller of enter() takes one unit from the semaphore. In close(), we
    /// take all the units to ensure all GateGuards are destroyed.
    sem: Arc<tokio::sync::Semaphore>,

    /// For observability only: a name that will be used to log warnings if a particular
    /// gate is holding up shutdown
    name: String,
}

/// RAII guard for a [`Gate`]: as long as this exists, calls to [`Gate::close`] will
/// not complete.
pub struct GateGuard(tokio::sync::OwnedSemaphorePermit);

/// Observability helper: every `warn_period`, emit a log warning that we're still waiting on this gate
async fn warn_if_stuck<Fut: std::future::Future>(
    fut: Fut,
    name: &str,
    warn_period: std::time::Duration,
) -> <Fut as std::future::Future>::Output {
    let started = std::time::Instant::now();

    let mut fut = std::pin::pin!(fut);

    loop {
        match tokio::time::timeout(warn_period, &mut fut).await {
            Ok(ret) => return ret,
            Err(_) => {
                tracing::warn!(
                    gate = name,
                    elapsed_ms = started.elapsed().as_millis(),
                    "still waiting, taking longer than expected..."
                );
            }
        }
    }
}

pub enum GateError {
    GateClosed,
}

impl Gate {
    pub fn new(name: String) -> Self {
        Self {
            sem: Arc::new(tokio::sync::Semaphore::new(
                tokio::sync::Semaphore::MAX_PERMITS,
            )),
            name,
        }
    }

    /// Acquire a guard that will prevent close() calls from completing. If close()
    /// was already called, this will return an error which should be interpreted
    /// as "shutting down".
    ///
    /// This function would typically be used from e.g. request handlers. While holding
    /// the guard returned from this function, it is important to respect a CancellationToken
    /// to avoid blocking close() indefinitely: typically types that contain a Gate will
    /// also contain a CancellationToken.
    pub fn enter(&self) -> Result<GateGuard, GateError> {
        self.sem
            .clone()
            .try_acquire_owned()
            .map(GateGuard)
            .map_err(|_| GateError::GateClosed)
    }

    /// Types with a shutdown() method and a gate should call this method at the
    /// end of shutdown, to ensure that all GateGuard holders are done.
    ///
    /// This will wait for all guards to be destroyed.  For this to complete promptly, it is
    /// important that the holders of such guards are respecting a CancellationToken which has
    /// been cancelled before entering this function.
    pub async fn close(&self) {
        warn_if_stuck(self.do_close(), &self.name, Duration::from_millis(1000)).await
    }

    async fn do_close(&self) {
        tracing::debug!(gate = self.name, "Closing Gate...");
        match self
            .sem
            .acquire_many(tokio::sync::Semaphore::MAX_PERMITS as u32)
            .await
        {
            Ok(_units) => {
                // While holding all units, close the semaphore.  All subsequent calls to enter() will fail.
                self.sem.close();
            }
            Err(_) => {
                // Semaphore closed: we are the only function that can do this, so it indicates a double-call.
                // This is legal.  Timeline::shutdown for example is not protected from being called more than
                // once.
            }
        }
        tracing::debug!(gate = self.name, "Closed Gate.")
    }
}
