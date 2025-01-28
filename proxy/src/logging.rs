use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};
use opentelemetry::trace::TraceContextExt;
use serde::ser::{SerializeMap, Serializer};
use serde::Serialize;
use tracing::subscriber::Interest;
use tracing::{callsite, Event, Level, Metadata, Span, Subscriber};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::filter::{EnvFilter, LevelFilter};
use tracing_subscriber::fmt::format::{Format, Full};
use tracing_subscriber::fmt::time::SystemTime;
use tracing_subscriber::fmt::{FormatEvent, FormatFields};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;

/// Initialize logging and OpenTelemetry tracing and exporter.
///
/// Logging can be configured using `RUST_LOG` environment variable.
///
/// OpenTelemetry is configured with OTLP/HTTP exporter. It picks up
/// configuration from environment variables. For example, to change the
/// destination, set `OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318`.
/// See <https://opentelemetry.io/docs/reference/specification/sdk-environment-variables>
pub async fn init() -> anyhow::Result<LoggingGuard> {
    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::DEBUG.into())
        .from_env_lossy()
        .add_directive(
            "aws_config=info"
                .parse()
                .expect("this should be a valid filter directive"),
        )
        .add_directive(
            "azure_core::policies::transport=off"
                .parse()
                .expect("this should be a valid filter directive"),
        );

    let otlp_layer = tracing_utils::init_tracing("proxy").await;

    let log_layer = JsonLoggingLayer {
        clock: RealClock,
        skipped_field_index: Arc::default(),
        writer: StderrWriter {
            stderr: std::io::stderr(),
        },
    };

    tracing_subscriber::registry()
        .with(env_filter)
        .with(otlp_layer)
        .with(log_layer)
        .try_init()?;

    Ok(LoggingGuard)
}

/// Initialize logging for local_proxy with log prefix and no opentelemetry.
///
/// Logging can be configured using `RUST_LOG` environment variable.
pub fn init_local_proxy() -> anyhow::Result<LoggingGuard> {
    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .event_format(LocalProxyFormatter(Format::default().with_target(false)));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .try_init()?;

    Ok(LoggingGuard)
}

pub struct LocalProxyFormatter(Format<Full, SystemTime>);

impl<S, N> FormatEvent<S, N> for LocalProxyFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: tracing_subscriber::fmt::format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        writer.write_str("[local_proxy] ")?;
        self.0.format_event(ctx, writer, event)
    }
}

pub struct LoggingGuard;

impl Drop for LoggingGuard {
    fn drop(&mut self) {
        // Shutdown trace pipeline gracefully, so that it has a chance to send any
        // pending traces before we exit.
        tracing::info!("shutting down the tracing machinery");
        tracing_utils::shutdown_tracing();
    }
}

pub trait MakeWriter {
    fn make(&self) -> impl io::Write;
}

struct StderrWriter {
    stderr: io::Stderr,
}

impl MakeWriter for StderrWriter {
    fn make(&self) -> impl io::Write {
        self.stderr.lock()
    }
}

// TODO: move into separate module or even separate crate.
pub trait Clock {
    fn now(&self) -> DateTime<Utc>;
}

struct RealClock;

impl Clock for RealClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

pub struct JsonLoggingLayer<C: Clock, W: MakeWriter> {
    clock: C,
    skipped_field_index: Arc<RwLock<HashMap<callsite::Identifier, Vec<usize>>>>,
    // TODO: use or ask for buffered writer
    writer: W,
}

impl<S, C: Clock + 'static, W: MakeWriter + 'static> Layer<S> for JsonLoggingLayer<C, W>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        // TODO: consider special tracing subscriber to grab timestamp very
        //       early, before OTel machinery, and add as event extension.
        let now = self.clock.now();

        let res: io::Result<()> = EVENT_FORMATTER.with_borrow_mut(move |formatter| {
            formatter.reset();
            formatter.format(
                now,
                event,
                &ctx,
                &self.skipped_field_index.read().expect("poisoned"),
            )?;
            use std::io::Write;
            self.writer.make().write_all(&formatter.logline_buffer)
        });

        res.unwrap(); // XXX
    }

    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        if !metadata.is_event() {
            return Interest::never();
        }

        // TODO: add "log." fields to list.

        let mut skipped_field_indices = self.skipped_field_index.write().expect("poisoned");

        let mut seen_fields = HashMap::<&'static str, usize>::new();
        for field in metadata.fields().iter() {
            use std::collections::hash_map::Entry;
            match seen_fields.entry(field.name()) {
                Entry::Vacant(entry) => {
                    // field not seen yet
                    entry.insert(field.index());
                }
                Entry::Occupied(mut entry) => {
                    // replace currently stored index
                    let old_index = entry.insert(field.index());
                    // ... and append it to list of skippable indices
                    match skipped_field_indices.entry(metadata.callsite()) {
                        Entry::Vacant(entry) => {
                            entry.insert(vec![old_index]);
                        }
                        Entry::Occupied(mut entry) => {
                            entry.get_mut().push(old_index);
                        }
                    }
                }
            }
        }

        Interest::always()
    }
}

thread_local! {
    static EVENT_FORMATTER: RefCell<EventFormatter> = RefCell::new(EventFormatter::new());
}

// TODO: capacity management
struct EventFormatter {
    logline_buffer: Vec<u8>,
    field_tracker: HashSet<u32>,
}

impl EventFormatter {
    fn new() -> Self {
        EventFormatter {
            logline_buffer: Vec::new(),
            field_tracker: HashSet::new(),
        }
    }

    fn reset(&mut self) {
        self.logline_buffer.clear();
        self.field_tracker.clear();
    }

    fn format<S>(
        &mut self,
        now: DateTime<Utc>,
        event: &Event<'_>,
        ctx: &Context<'_, S>,
        skipped_field_indices: &HashMap<callsite::Identifier, Vec<usize>>,
    ) -> io::Result<()>
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let timestamp = now.to_rfc3339_opts(chrono::SecondsFormat::Micros, true);

        // TODO: tracing-log?
        let meta = event.metadata();

        let skipped_field_indices = skipped_field_indices
            .get(&meta.callsite())
            .map(Vec::as_slice)
            .unwrap_or_default();

        let mut serialize = || {
            let mut serializer = serde_json::Serializer::new(&mut self.logline_buffer);

            let mut serializer = serializer.serialize_map(None)?;

            // Timestamp comes first, so raw lines can be sorted by timestamp.
            serializer.serialize_entry("t", &timestamp)?;

            // Level next. "lvl" is the shortest name recognized by Loki's auto-detection.
            serializer.serialize_entry("lvl", &SerializeLevel(meta.level()))?;

            // Message next.
            serializer.serialize_key("m")?;
            let mut message_extractor = MessageExtractor::new(serializer, skipped_field_indices);
            event.record(&mut message_extractor);
            let mut serializer = message_extractor.into_serializer()?;

            let mut fields_present = FieldsPresent(false);
            event.record(&mut fields_present);
            if fields_present.0 {
                serializer.serialize_entry(
                    "fields",
                    &SerializableEventFields(event, skipped_field_indices),
                )?;
            }

            serializer.serialize_entry("proc", &ProcessInfo)?;

            #[cfg(tokio_unstable)]
            if let Some(task_id) = tokio::task::try_id() {
                serializer.serialize_entry("task", &TaskInfo { task_id })?;
            }

            // TODO: or just file:line?
            serializer.serialize_entry("loc", &LocationInfo(meta))?;

            {
                let otel_context = Span::current().context();
                let otel_spanref = otel_context.span();
                let span_context = otel_spanref.span_context();
                if span_context.is_valid() {
                    serializer.serialize_entry(
                        "trace_id",
                        &format_args!("{:032x}", span_context.trace_id()),
                    )?;
                }
            }

            let _ = ctx;
            // TODO: spans + span fields

            serializer.end()
        };

        serialize().map_err(io::Error::other)?;
        self.logline_buffer.push(b'\n');
        Ok(())
    }
}

struct SerializeLevel<'a>(&'a Level);

impl<'a> Serialize for SerializeLevel<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // This order was chosen to match log level probabilities.
        if self.0 == &Level::INFO {
            serializer.serialize_str("INFO")
        } else if self.0 == &Level::WARN {
            serializer.serialize_str("WARN")
        } else if self.0 == &Level::ERROR {
            serializer.serialize_str("ERROR")
        } else if self.0 == &Level::DEBUG {
            serializer.serialize_str("DEBUG")
        } else if self.0 == &Level::TRACE {
            serializer.serialize_str("TRACE")
        } else {
            unreachable!()
        }
    }
}

/// Name of the field used by tracing crate to store the event message.
const MESSAGE_FIELD: &str = "message";

pub struct MessageExtractor<'a, S: serde::ser::SerializeMap> {
    serializer: S,
    skipped_field_indices: &'a [usize],
    state: Option<Result<(), S::Error>>,
}

impl<'a, S: serde::ser::SerializeMap> MessageExtractor<'a, S> {
    fn new(serializer: S, skipped_field_indices: &'a [usize]) -> Self {
        Self {
            serializer,
            skipped_field_indices,
            state: None,
        }
    }

    fn into_serializer(mut self) -> Result<S, S::Error> {
        match self.state {
            Some(Ok(())) => {}
            Some(Err(err)) => return Err(err),
            None => self.serializer.serialize_value("")?,
        }
        Ok(self.serializer)
    }

    fn accept_field(&self, field: &tracing::field::Field) -> bool {
        self.state.is_none()
            && field.name() == MESSAGE_FIELD
            && !self.skipped_field_indices.contains(&field.index())
    }
}

impl<'a, S: serde::ser::SerializeMap> tracing::field::Visit for MessageExtractor<'a, S> {
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&value));
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&value));
        }
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&value));
        }
    }

    fn record_i128(&mut self, field: &tracing::field::Field, value: i128) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&value));
        }
    }

    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&value));
        }
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&value));
        }
    }

    fn record_bytes(&mut self, field: &tracing::field::Field, value: &[u8]) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&format_args!("{value:x?}")));
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&value));
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&format_args!("{value:?}")));
        }
    }

    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&format_args!("{value}")));
        }
    }
}

struct FieldsPresent(pub bool);

impl tracing::field::Visit for FieldsPresent {
    fn record_debug(&mut self, field: &tracing::field::Field, _: &dyn std::fmt::Debug) {
        if field.name() != MESSAGE_FIELD {
            self.0 |= true;
        }
    }
}

struct SerializableEventFields<'a, 'event>(&'a tracing::Event<'event>, &'a [usize]);

impl<'a, 'event> serde::ser::Serialize for SerializableEventFields<'a, 'event> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;
        let serializer = serializer.serialize_map(None)?;
        let mut message_skipper = MessageSkipper::new(serializer, self.1);
        self.0.record(&mut message_skipper);
        let serializer = message_skipper.into_serializer()?;
        serializer.end()
    }
}

pub struct MessageSkipper<'a, S: serde::ser::SerializeMap> {
    serializer: S,
    skipped_field_indices: &'a [usize],
    state: Result<(), S::Error>,
}

impl<'a, S: serde::ser::SerializeMap> MessageSkipper<'a, S> {
    fn new(serializer: S, skipped_field_indices: &'a [usize]) -> Self {
        Self {
            serializer,
            skipped_field_indices,
            state: Ok(()),
        }
    }

    fn accept_field(&self, field: &tracing::field::Field) -> bool {
        self.state.is_ok()
            && field.name() != MESSAGE_FIELD
            && !self.skipped_field_indices.contains(&field.index())
    }

    fn into_serializer(self) -> Result<S, S::Error> {
        self.state?;
        Ok(self.serializer)
    }
}

impl<'a, S: serde::ser::SerializeMap> tracing::field::Visit for MessageSkipper<'a, S> {
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        if self.accept_field(field) {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        if self.accept_field(field) {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if self.accept_field(field) {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    fn record_i128(&mut self, field: &tracing::field::Field, value: i128) {
        if self.accept_field(field) {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        if self.accept_field(field) {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        if self.accept_field(field) {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    fn record_bytes(&mut self, field: &tracing::field::Field, value: &[u8]) {
        if self.accept_field(field) {
            self.state = self
                .serializer
                .serialize_entry(field.name(), &format_args!("{value:x?}"));
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if self.accept_field(field) {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if self.accept_field(field) {
            self.state = self
                .serializer
                .serialize_entry(field.name(), &format_args!("{value:?}"));
        }
    }

    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        if self.accept_field(field) {
            self.state = self.serializer.serialize_value(&format_args!("{value}"));
        }
    }
}

struct ProcessInfo;

impl serde::ser::Serialize for ProcessInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        use serde::ser::SerializeMap;

        let mut serializer = serializer.serialize_map(None)?;
        serializer.serialize_entry("thread_id", &gettid::gettid())?;

        // TODO: tls cache? name could change
        if let Some(thread_name) = std::thread::current().name() {
            if !thread_name.is_empty() {
                serializer.serialize_entry("thread_name", thread_name)?;
            }
        }

        serializer.end()
    }
}

#[cfg(tokio_unstable)]
struct TaskInfo {
    task_id: tokio::task::Id,
}

#[cfg(tokio_unstable)]
impl serde::ser::Serialize for TaskInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        use serde::ser::SerializeMap;

        let mut serializer = serializer.serialize_map(None)?;
        serializer.serialize_entry("id", &format_args!("{}", self.task_id))?;

        serializer.end()
    }
}

struct LocationInfo<'a>(&'a tracing::metadata::Metadata<'a>);

impl<'a> serde::ser::Serialize for LocationInfo<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        use serde::ser::SerializeMap;

        let mut serializer = serializer.serialize_map(None)?;
        serializer.serialize_entry("target", self.0.target())?;
        if let Some(module) = self.0.module_path() {
            if module != self.0.target() {
                serializer.serialize_entry("module", module)?;
            }
        }

        if let Some(file) = self.0.file() {
            if let Some(line) = self.0.line() {
                serializer.serialize_entry("src", &format_args!("{file}:{line}"))?;
            } else {
                serializer.serialize_entry("src", file)?;
            }
        }
        serializer.end()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, MutexGuard};

    use assert_json_diff::assert_json_eq;
    use tracing::info_span;

    use super::*;

    struct TestClock {
        current_time: Mutex<DateTime<Utc>>,
    }

    impl TestClock {
        fn set(self: &Arc<TestClock>, current_time: DateTime<Utc>) {
            *self.current_time.lock().expect("poisoned") = current_time;
        }
    }

    impl Clock for Arc<TestClock> {
        fn now(&self) -> DateTime<Utc> {
            *self.current_time.lock().expect("poisoned")
        }
    }

    struct VecWriter<'a> {
        buffer: MutexGuard<'a, Vec<u8>>,
    }

    impl MakeWriter for Arc<Mutex<Vec<u8>>> {
        fn make(&self) -> impl io::Write {
            VecWriter {
                buffer: self.lock().expect("poisoned"),
            }
        }
    }

    impl<'a> io::Write for VecWriter<'a> {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.buffer.write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_field_collection() {
        let clock = Arc::new(TestClock {
            current_time: Mutex::new(Utc::now()),
        });
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let log_layer = JsonLoggingLayer {
            clock: clock.clone(),
            skipped_field_index: Arc::default(),
            writer: buffer.clone(),
        };
        let registry = tracing_subscriber::Registry::default().with(log_layer);

        tracing::subscriber::with_default(registry, || {
            let _span = info_span!("test_unset_fields").entered();
            tracing::info!(
                a = 1,
                a = 2,
                a = 3,
                message = "explicit message field",
                "implicit message field"
            );
        });

        let buffer = Arc::try_unwrap(buffer).unwrap().into_inner().unwrap();
        let actual: serde_json::Value = serde_json::from_slice(&buffer).unwrap();
        let expected: serde_json::Value = serde_json::json!(
            {
                "t": clock.now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                "lvl": "INFO",
                "m": "explicit message field",
                "fields": {
                    "a": 3
                },
                "loc": {
                    "src": actual.as_object().unwrap().get("loc").unwrap().as_object().unwrap().get("src").unwrap().as_str().unwrap(),
                    "target": "proxy::logging::tests"
                },
                "proc": {
                    "thread_id": actual.as_object().unwrap().get("proc").unwrap().as_object().unwrap().get("thread_id").unwrap().as_number().unwrap(),
                    "thread_name": "logging::tests::test_field_collection"
                },
            }
        );

        assert_json_eq!(actual, expected);
    }
}
