use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;

/// Initialize logging to stderr, and OpenTelemetry tracing and exporter.
///
/// Logging is configured using either `default_log_level` or
/// `RUST_LOG` environment variable as default log level.
///
/// OpenTelemetry is configured with OTLP/HTTP exporter. It picks up
/// configuration from environment variables. For example, to change the destination,
/// set `OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318`. See
/// `tracing-utils` package description.
///
pub async fn init_tracing_and_logging(default_log_level: &str) -> anyhow::Result<()> {
    // Initialize Logging
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_log_level));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_target(false)
        .with_writer(std::io::stderr);

    // Initialize OpenTelemetry
    let otlp_layer =
        tracing_utils::init_tracing("compute_ctl", tracing_utils::ExportConfig::default()).await;

    // Put it all together
    tracing_subscriber::registry()
        .with(env_filter)
        .with(otlp_layer)
        .with(fmt_layer)
        .init();
    tracing::info!("logging and tracing started");

    utils::logging::replace_panic_hook_with_tracing_panic_hook().forget();

    Ok(())
}

/// Replace all newline characters with a special character to make it
/// easier to grep for log messages.
pub fn inlinify(s: &str) -> String {
    s.replace('\n', "\u{200B}")
}
