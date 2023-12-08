/// use with fail::cfg("$name", "return(2000)")
///
/// The effect is similar to a "sleep(2000)" action, i.e. we sleep for the
/// specified time (in milliseconds). The main difference is that we use async
/// tokio sleep function. Another difference is that we print lines to the log,
/// which can be useful in tests to check that the failpoint was hit.
#[macro_export]
macro_rules! __failpoint_sleep_millis_async {
    ($name:literal) => {{
        // If the failpoint is used with a "return" action, set should_sleep to the
        // returned value (as string). Otherwise it's set to None.
        let should_sleep = (|| {
            ::fail::fail_point!($name, |x| x);
            ::std::option::Option::None
        })();

        // Sleep if the action was a returned value
        if let ::std::option::Option::Some(duration_str) = should_sleep {
            $crate::failpoint_support::failpoint_sleep_helper($name, duration_str).await
        }
    }};
}
pub use __failpoint_sleep_millis_async as sleep_millis_async;

// Helper function used by the macro. (A function has nicer scoping so we
// don't need to decorate everything with "::")
#[doc(hidden)]
pub(crate) async fn failpoint_sleep_helper(name: &'static str, duration_str: String) {
    let millis = duration_str.parse::<u64>().unwrap();
    let d = std::time::Duration::from_millis(millis);

    tracing::info!("failpoint {:?}: sleeping for {:?}", name, d);
    tokio::time::sleep(d).await;
    tracing::info!("failpoint {:?}: sleep done", name);
}

/// Declare a failpoint that can use the `pause` failpoint action.
/// We don't want to block the executor thread, hence, spawn_blocking + await.
#[macro_export]
macro_rules! __pausable_failpoint {
    ($name:literal) => {
        if cfg!(feature = "testing") {
            let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
            let mut jh = tokio::task::spawn_blocking({
                let current = tracing::Span::current();
                let barrier = barrier.clone();
                move || {
                    let _entered = current.entered();
                    // pausing will park the thread within `fail`, but we want to synchronize with
                    // failpoints from test code which we currently do by waiting for a log line.
                    //
                    // instead of producing the log line conditionally, produce it only after
                    // 100ms of joining at the barrier.
                    tokio::runtime::Handle::current().block_on(barrier.wait());
                    fail::fail_point!($name);
                }
            });

            barrier.wait().await;

            let res = tokio::time::timeout(std::time::Duration::from_millis(100), &mut jh).await;

            if res.is_ok() {
                // `fail` action "pause" was not hit
            } else {
                tracing::info!("at failpoint {}", $name);
                jh.await.expect("spawn_blocking");
            }
        }
    };
}

pub use __pausable_failpoint as pausable_failpoint;

pub fn init() -> fail::FailScenario<'static> {
    // The failpoints lib provides support for parsing the `FAILPOINTS` env var.
    // We want non-default behavior for `exit`, though, so, we handle it separately.
    //
    // Format for FAILPOINTS is "name=actions" separated by ";".
    let actions = std::env::var("FAILPOINTS");
    if actions.is_ok() {
        std::env::remove_var("FAILPOINTS");
    } else {
        // let the library handle non-utf8, or nothing for not present
    }

    let scenario = fail::FailScenario::setup();

    if let Ok(val) = actions {
        val.split(';')
            .enumerate()
            .map(|(i, s)| s.split_once('=').ok_or((i, s)))
            .for_each(|res| {
                let (name, actions) = match res {
                    Ok(t) => t,
                    Err((i, s)) => {
                        panic!(
                            "startup failpoints: missing action on the {}th failpoint; try `{s}=return`",
                            i + 1,
                        );
                    }
                };
                if let Err(e) = apply_failpoint(name, actions) {
                    panic!("startup failpoints: failed to apply failpoint {name}={actions}: {e}");
                }
            });
    }

    scenario
}

pub(crate) fn apply_failpoint(name: &str, actions: &str) -> Result<(), String> {
    if actions == "exit" {
        fail::cfg_callback(name, exit_failpoint)
    } else {
        fail::cfg(name, actions)
    }
}

#[inline(never)]
fn exit_failpoint() {
    tracing::info!("Exit requested by failpoint");
    std::process::exit(1);
}
