use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::{env, io};

use chrono::{DateTime, Utc};
use opentelemetry::trace::TraceContextExt;
use scopeguard::defer;
use serde::ser::{SerializeMap, Serializer};
use tracing::span;
use tracing::subscriber::Interest;
use tracing::{callsite, Event, Metadata, Span, Subscriber};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::filter::{EnvFilter, LevelFilter};
use tracing_subscriber::fmt::format::{Format, Full};
use tracing_subscriber::fmt::time::SystemTime;
use tracing_subscriber::fmt::{FormatEvent, FormatFields};
use tracing_subscriber::layer::{Context, Identity, Layer};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::{LookupSpan, SpanRef};

/// Initialize logging and OpenTelemetry tracing and exporter.
///
/// Logging can be configured using `RUST_LOG` environment variable.
///
/// OpenTelemetry is configured with OTLP/HTTP exporter. It picks up
/// configuration from environment variables. For example, to change the
/// destination, set `OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318`.
/// See <https://opentelemetry.io/docs/reference/specification/sdk-environment-variables>
pub async fn init() -> anyhow::Result<LoggingGuard> {
    let logfmt = LogFormat::from_env()?;

    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
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

    let json_log_layer = if logfmt == LogFormat::Json {
        Some(JsonLoggingLayer {
            clock: RealClock,
            skipped_field_indices: papaya::HashMap::default(),
            writer: StderrWriter {
                stderr: std::io::stderr(),
            },
        })
    } else {
        None
    };

    let text_log_layer = if logfmt == LogFormat::Text {
        Some(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(std::io::stderr)
                .with_target(false),
        )
    } else {
        None
    };

    let export_layer = Identity::new()
        .and_then(otlp_layer)
        .and_then(json_log_layer)
        .and_then(text_log_layer)
        .with_filter(env_filter);

    #[cfg(not(tokio_unstable))]
    let tokio_console_layer = Identity::new();
    #[cfg(tokio_unstable)]
    let tokio_console_layer = console_subscriber::ConsoleLayer::builder()
        .with_default_env()
        .enable_grpc_web(true)
        .spawn();

    tracing_subscriber::registry()
        .with(export_layer)
        .with(tokio_console_layer)
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

// TODO: make JSON the default
#[derive(Copy, Clone, PartialEq, Eq, Default, Debug)]
enum LogFormat {
    #[default]
    Text = 1,
    Json,
}

impl LogFormat {
    fn from_env() -> anyhow::Result<Self> {
        let logfmt = env::var("LOGFMT");
        Ok(match logfmt.as_deref() {
            Err(_) => LogFormat::default(),
            Ok("text") => LogFormat::Text,
            Ok("json") => LogFormat::Json,
            Ok(logfmt) => anyhow::bail!("unknown log format: {logfmt}"),
        })
    }
}

trait MakeWriter {
    fn make_writer(&self) -> impl io::Write;
}

struct StderrWriter {
    stderr: io::Stderr,
}

impl MakeWriter for StderrWriter {
    #[inline]
    fn make_writer(&self) -> impl io::Write {
        self.stderr.lock()
    }
}

// TODO: move into separate module or even separate crate.
trait Clock {
    fn now(&self) -> DateTime<Utc>;
}

struct RealClock;

impl Clock for RealClock {
    #[inline]
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Name of the field used by tracing crate to store the event message.
const MESSAGE_FIELD: &str = "message";

thread_local! {
    /// Protects against deadlocks and double panics during log writing.
    /// The current panic handler will use tracing to log panic information.
    static REENTRANCY_GUARD: Cell<bool> = const { Cell::new(false) };
    /// Thread-local instance with per-thread buffer for log writing.
    static EVENT_FORMATTER: RefCell<EventFormatter> = RefCell::new(EventFormatter::new());
    /// Cached OS thread ID.
    static THREAD_ID: u64 = gettid::gettid();
}

/// Implements tracing layer to handle events specific to logging.
struct JsonLoggingLayer<C: Clock, W: MakeWriter> {
    clock: C,
    skipped_field_indices: papaya::HashMap<callsite::Identifier, SkippedFieldIndices>,
    writer: W,
}

impl<S, C: Clock + 'static, W: MakeWriter + 'static> Layer<S> for JsonLoggingLayer<C, W>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        use std::io::Write;

        // TODO: consider special tracing subscriber to grab timestamp very
        //       early, before OTel machinery, and add as event extension.
        let now = self.clock.now();

        let res: io::Result<()> = REENTRANCY_GUARD.with(move |entered| {
            if entered.get() {
                let mut formatter = EventFormatter::new();
                formatter.format(now, event, &ctx, &self.skipped_field_indices)?;
                self.writer.make_writer().write_all(formatter.buffer())
            } else {
                entered.set(true);
                defer!(entered.set(false););

                EVENT_FORMATTER.with_borrow_mut(move |formatter| {
                    formatter.reset();
                    formatter.format(now, event, &ctx, &self.skipped_field_indices)?;
                    self.writer.make_writer().write_all(formatter.buffer())
                })
            }
        });

        // In case logging fails we generate a simpler JSON object.
        if let Err(err) = res {
            if let Ok(mut line) = serde_json::to_vec(&serde_json::json!( {
                "timestamp": now.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                "level": "ERROR",
                "message": format_args!("cannot log event: {err:?}"),
                "fields": {
                    "event": format_args!("{event:?}"),
                },
            })) {
                line.push(b'\n');
                self.writer.make_writer().write_all(&line).ok();
            }
        }
    }

    /// Registers a SpanFields instance as span extension.
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("span must exist");
        let fields = SpanFields::default();
        fields.record_fields(attrs);
        // This could deadlock when there's a panic somewhere in the tracing
        // event handling and a read or write guard is still held. This includes
        // the OTel subscriber.
        span.extensions_mut().insert(fields);
    }

    fn on_record(&self, id: &span::Id, values: &span::Record<'_>, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("span must exist");
        let ext = span.extensions();
        if let Some(data) = ext.get::<SpanFields>() {
            data.record_fields(values);
        }
    }

    /// Called (lazily) whenever a new log call is executed. We quickly check
    /// for duplicate field names and record duplicates as skippable. Last one
    /// wins.
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        if !metadata.is_event() {
            // Must not be never because we wouldn't get trace and span data.
            return Interest::always();
        }

        let mut field_indices = SkippedFieldIndices::default();
        let mut seen_fields = HashMap::<&'static str, usize>::new();
        for field in metadata.fields() {
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
                    field_indices.push(old_index);
                }
            }
        }

        if !field_indices.is_empty() {
            self.skipped_field_indices
                .pin()
                .insert(metadata.callsite(), field_indices);
        }

        Interest::always()
    }
}

/// Stores span field values recorded during the spans lifetime.
#[derive(Default)]
struct SpanFields {
    // TODO: Switch to custom enum with lasso::Spur for Strings?
    fields: papaya::HashMap<&'static str, serde_json::Value>,
}

impl SpanFields {
    #[inline]
    fn record_fields<R: tracing_subscriber::field::RecordFields>(&self, fields: R) {
        fields.record(&mut SpanFieldsRecorder {
            fields: self.fields.pin(),
        });
    }
}

/// Implements a tracing field visitor to convert and store values.
struct SpanFieldsRecorder<'m, S, G> {
    fields: papaya::HashMapRef<'m, &'static str, serde_json::Value, S, G>,
}

impl<S: BuildHasher, G: papaya::Guard> tracing::field::Visit for SpanFieldsRecorder<'_, S, G> {
    #[inline]
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.fields
            .insert(field.name(), serde_json::Value::from(value));
    }

    #[inline]
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.fields
            .insert(field.name(), serde_json::Value::from(value));
    }

    #[inline]
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.fields
            .insert(field.name(), serde_json::Value::from(value));
    }

    #[inline]
    fn record_i128(&mut self, field: &tracing::field::Field, value: i128) {
        if let Ok(value) = i64::try_from(value) {
            self.fields
                .insert(field.name(), serde_json::Value::from(value));
        } else {
            self.fields
                .insert(field.name(), serde_json::Value::from(format!("{value}")));
        }
    }

    #[inline]
    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        if let Ok(value) = u64::try_from(value) {
            self.fields
                .insert(field.name(), serde_json::Value::from(value));
        } else {
            self.fields
                .insert(field.name(), serde_json::Value::from(format!("{value}")));
        }
    }

    #[inline]
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.fields
            .insert(field.name(), serde_json::Value::from(value));
    }

    #[inline]
    fn record_bytes(&mut self, field: &tracing::field::Field, value: &[u8]) {
        self.fields
            .insert(field.name(), serde_json::Value::from(value));
    }

    #[inline]
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.fields
            .insert(field.name(), serde_json::Value::from(value));
    }

    #[inline]
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.fields
            .insert(field.name(), serde_json::Value::from(format!("{value:?}")));
    }

    #[inline]
    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        self.fields
            .insert(field.name(), serde_json::Value::from(format!("{value}")));
    }
}

/// List of field indices skipped during logging. Can list duplicate fields or
/// metafields not meant to be logged.
#[derive(Clone, Default)]
struct SkippedFieldIndices {
    bits: u64,
}

impl SkippedFieldIndices {
    #[inline]
    fn is_empty(&self) -> bool {
        self.bits == 0
    }

    #[inline]
    fn push(&mut self, index: usize) {
        self.bits |= 1u64
            .checked_shl(index as u32)
            .expect("field index too large");
    }

    #[inline]
    fn contains(&self, index: usize) -> bool {
        self.bits
            & 1u64
                .checked_shl(index as u32)
                .expect("field index too large")
            != 0
    }
}

/// Formats a tracing event and writes JSON to its internal buffer including a newline.
// TODO: buffer capacity management, truncate if too large
struct EventFormatter {
    logline_buffer: Vec<u8>,
}

impl EventFormatter {
    #[inline]
    fn new() -> Self {
        EventFormatter {
            logline_buffer: Vec::new(),
        }
    }

    #[inline]
    fn buffer(&self) -> &[u8] {
        &self.logline_buffer
    }

    #[inline]
    fn reset(&mut self) {
        self.logline_buffer.clear();
    }

    fn format<S>(
        &mut self,
        now: DateTime<Utc>,
        event: &Event<'_>,
        ctx: &Context<'_, S>,
        skipped_field_indices: &papaya::HashMap<callsite::Identifier, SkippedFieldIndices>,
    ) -> io::Result<()>
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let timestamp = now.to_rfc3339_opts(chrono::SecondsFormat::Micros, true);

        use tracing_log::NormalizeEvent;
        let normalized_meta = event.normalized_metadata();
        let meta = normalized_meta.as_ref().unwrap_or_else(|| event.metadata());

        let skipped_field_indices = skipped_field_indices.pin();
        let skipped_field_indices = skipped_field_indices.get(&meta.callsite());

        let mut serialize = || {
            let mut serializer = serde_json::Serializer::new(&mut self.logline_buffer);

            let mut serializer = serializer.serialize_map(None)?;

            // Timestamp comes first, so raw lines can be sorted by timestamp.
            serializer.serialize_entry("timestamp", &timestamp)?;

            // Level next.
            serializer.serialize_entry("level", &meta.level().as_str())?;

            // Message next.
            serializer.serialize_key("message")?;
            let mut message_extractor =
                MessageFieldExtractor::new(serializer, skipped_field_indices);
            event.record(&mut message_extractor);
            let mut serializer = message_extractor.into_serializer()?;

            let mut fields_present = FieldsPresent(false, skipped_field_indices);
            event.record(&mut fields_present);
            if fields_present.0 {
                serializer.serialize_entry(
                    "fields",
                    &SerializableEventFields(event, skipped_field_indices),
                )?;
            }

            let pid = std::process::id();
            if pid != 1 {
                serializer.serialize_entry("process_id", &pid)?;
            }

            THREAD_ID.with(|tid| serializer.serialize_entry("thread_id", tid))?;

            // TODO: tls cache? name could change
            if let Some(thread_name) = std::thread::current().name() {
                if !thread_name.is_empty() && thread_name != "tokio-runtime-worker" {
                    serializer.serialize_entry("thread_name", thread_name)?;
                }
            }

            if let Some(task_id) = tokio::task::try_id() {
                serializer.serialize_entry("task_id", &format_args!("{task_id}"))?;
            }

            serializer.serialize_entry("target", meta.target())?;

            if let Some(module) = meta.module_path() {
                if module != meta.target() {
                    serializer.serialize_entry("module", module)?;
                }
            }

            if let Some(file) = meta.file() {
                if let Some(line) = meta.line() {
                    serializer.serialize_entry("src", &format_args!("{file}:{line}"))?;
                } else {
                    serializer.serialize_entry("src", file)?;
                }
            }

            {
                let otel_context = Span::current().context();
                let otel_spanref = otel_context.span();
                let span_context = otel_spanref.span_context();
                if span_context.is_valid() {
                    serializer.serialize_entry(
                        "trace_id",
                        &format_args!("{}", span_context.trace_id()),
                    )?;
                }
            }

            serializer.serialize_entry("spans", &SerializableSpanStack(ctx))?;

            serializer.end()
        };

        serialize().map_err(io::Error::other)?;
        self.logline_buffer.push(b'\n');
        Ok(())
    }
}

/// Extracts the message field that's mixed will other fields.
struct MessageFieldExtractor<'a, S: serde::ser::SerializeMap> {
    serializer: S,
    skipped_field_indices: Option<&'a SkippedFieldIndices>,
    state: Option<Result<(), S::Error>>,
}

impl<'a, S: serde::ser::SerializeMap> MessageFieldExtractor<'a, S> {
    #[inline]
    fn new(serializer: S, skipped_field_indices: Option<&'a SkippedFieldIndices>) -> Self {
        Self {
            serializer,
            skipped_field_indices,
            state: None,
        }
    }

    #[inline]
    fn into_serializer(mut self) -> Result<S, S::Error> {
        match self.state {
            Some(Ok(())) => {}
            Some(Err(err)) => return Err(err),
            None => self.serializer.serialize_value("")?,
        }
        Ok(self.serializer)
    }

    #[inline]
    fn accept_field(&self, field: &tracing::field::Field) -> bool {
        self.state.is_none()
            && field.name() == MESSAGE_FIELD
            && !self
                .skipped_field_indices
                .is_some_and(|i| i.contains(field.index()))
    }
}

impl<S: serde::ser::SerializeMap> tracing::field::Visit for MessageFieldExtractor<'_, S> {
    #[inline]
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&value));
        }
    }

    #[inline]
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&value));
        }
    }

    #[inline]
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&value));
        }
    }

    #[inline]
    fn record_i128(&mut self, field: &tracing::field::Field, value: i128) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&value));
        }
    }

    #[inline]
    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&value));
        }
    }

    #[inline]
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&value));
        }
    }

    #[inline]
    fn record_bytes(&mut self, field: &tracing::field::Field, value: &[u8]) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&format_args!("{value:x?}")));
        }
    }

    #[inline]
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&value));
        }
    }

    #[inline]
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&format_args!("{value:?}")));
        }
    }

    #[inline]
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

/// Checks if there's any fields and field values present. If not, the JSON subobject
/// can be skipped.
// This is entirely optional and only cosmetic, though maybe helps a
// bit during log parsing in dashboards when there's no field with empty object.
struct FieldsPresent<'a>(pub bool, Option<&'a SkippedFieldIndices>);

// Even though some methods have an overhead (error, bytes) it is assumed the
// compiler won't include this since we ignore the value entirely.
impl tracing::field::Visit for FieldsPresent<'_> {
    #[inline]
    fn record_debug(&mut self, field: &tracing::field::Field, _: &dyn std::fmt::Debug) {
        if !self.1.is_some_and(|i| i.contains(field.index()))
            && field.name() != MESSAGE_FIELD
            && !field.name().starts_with("log.")
        {
            self.0 |= true;
        }
    }
}

/// Serializes the fields directly supplied with a log event.
struct SerializableEventFields<'a, 'event>(
    &'a tracing::Event<'event>,
    Option<&'a SkippedFieldIndices>,
);

impl serde::ser::Serialize for SerializableEventFields<'_, '_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;
        let serializer = serializer.serialize_map(None)?;
        let mut message_skipper = MessageFieldSkipper::new(serializer, self.1);
        self.0.record(&mut message_skipper);
        let serializer = message_skipper.into_serializer()?;
        serializer.end()
    }
}

/// A tracing field visitor that skips the message field.
struct MessageFieldSkipper<'a, S: serde::ser::SerializeMap> {
    serializer: S,
    skipped_field_indices: Option<&'a SkippedFieldIndices>,
    state: Result<(), S::Error>,
}

impl<'a, S: serde::ser::SerializeMap> MessageFieldSkipper<'a, S> {
    #[inline]
    fn new(serializer: S, skipped_field_indices: Option<&'a SkippedFieldIndices>) -> Self {
        Self {
            serializer,
            skipped_field_indices,
            state: Ok(()),
        }
    }

    #[inline]
    fn accept_field(&self, field: &tracing::field::Field) -> bool {
        self.state.is_ok()
            && field.name() != MESSAGE_FIELD
            && !field.name().starts_with("log.")
            && !self
                .skipped_field_indices
                .is_some_and(|i| i.contains(field.index()))
    }

    #[inline]
    fn into_serializer(self) -> Result<S, S::Error> {
        self.state?;
        Ok(self.serializer)
    }
}

impl<S: serde::ser::SerializeMap> tracing::field::Visit for MessageFieldSkipper<'_, S> {
    #[inline]
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        if self.accept_field(field) {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    #[inline]
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        if self.accept_field(field) {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    #[inline]
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if self.accept_field(field) {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    #[inline]
    fn record_i128(&mut self, field: &tracing::field::Field, value: i128) {
        if self.accept_field(field) {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    #[inline]
    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        if self.accept_field(field) {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    #[inline]
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        if self.accept_field(field) {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    #[inline]
    fn record_bytes(&mut self, field: &tracing::field::Field, value: &[u8]) {
        if self.accept_field(field) {
            self.state = self
                .serializer
                .serialize_entry(field.name(), &format_args!("{value:x?}"));
        }
    }

    #[inline]
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if self.accept_field(field) {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    #[inline]
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if self.accept_field(field) {
            self.state = self
                .serializer
                .serialize_entry(field.name(), &format_args!("{value:?}"));
        }
    }

    #[inline]
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

/// Serializes the span stack from root to leaf (parent of event) enumerated
/// inside an object where the keys are just the number padded with zeroes
/// to retain sorting order.
// The object is necessary because Loki cannot flatten arrays.
struct SerializableSpanStack<'a, 'b, Span>(&'b Context<'a, Span>)
where
    Span: Subscriber + for<'lookup> LookupSpan<'lookup>;

impl<Span> serde::ser::Serialize for SerializableSpanStack<'_, '_, Span>
where
    Span: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn serialize<Ser>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error>
    where
        Ser: serde::ser::Serializer,
    {
        let mut serializer = serializer.serialize_map(None)?;

        if let Some(leaf_span) = self.0.lookup_current() {
            for (i, span) in leaf_span.scope().from_root().enumerate() {
                serializer.serialize_entry(&format_args!("{i:02}"), &SerializableSpan(&span))?;
            }
        }

        serializer.end()
    }
}

/// Serializes a single span. Include the span ID, name and its fields as
/// recorded up to this point.
struct SerializableSpan<'a, 'b, Span>(&'b SpanRef<'a, Span>)
where
    Span: for<'lookup> LookupSpan<'lookup>;

impl<Span> serde::ser::Serialize for SerializableSpan<'_, '_, Span>
where
    Span: for<'lookup> LookupSpan<'lookup>,
{
    fn serialize<Ser>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error>
    where
        Ser: serde::ser::Serializer,
    {
        let mut serializer = serializer.serialize_map(None)?;
        // TODO: the span ID is probably only useful for debugging tracing.
        serializer.serialize_entry("span_id", &format_args!("{:016x}", self.0.id().into_u64()))?;
        serializer.serialize_entry("span_name", self.0.metadata().name())?;

        let ext = self.0.extensions();
        if let Some(data) = ext.get::<SpanFields>() {
            for (key, value) in &data.fields.pin() {
                serializer.serialize_entry(key, value)?;
            }
        }

        serializer.end()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::{Arc, Mutex, MutexGuard};

    use assert_json_diff::assert_json_eq;
    use tracing::info_span;

    use super::*;

    struct TestClock {
        current_time: Mutex<DateTime<Utc>>,
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
        fn make_writer(&self) -> impl io::Write {
            VecWriter {
                buffer: self.lock().expect("poisoned"),
            }
        }
    }

    impl io::Write for VecWriter<'_> {
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
            skipped_field_indices: papaya::HashMap::default(),
            writer: buffer.clone(),
        };

        let registry = tracing_subscriber::Registry::default().with(log_layer);

        tracing::subscriber::with_default(registry, || {
            info_span!("span1", x = 40, x = 41, x = 42).in_scope(|| {
                info_span!("span2").in_scope(|| {
                    tracing::error!(
                        a = 1,
                        a = 2,
                        a = 3,
                        message = "explicit message field",
                        "implicit message field"
                    );
                });
            });
        });

        let buffer = Arc::try_unwrap(buffer)
            .expect("no other reference")
            .into_inner()
            .expect("poisoned");
        let actual: serde_json::Value = serde_json::from_slice(&buffer).expect("valid JSON");
        let expected: serde_json::Value = serde_json::json!(
            {
                "timestamp": clock.now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                "level": "ERROR",
                "message": "explicit message field",
                "fields": {
                    "a": 3,
                },
                "spans": {
                    "00":{
                        "span_id": "0000000000000001",
                        "span_name": "span1",
                        "x": 42,
                    },
                    "01": {
                        "span_id": "0000000000000002",
                        "span_name": "span2",
                    }
                },
                "src": actual.as_object().unwrap().get("src").unwrap().as_str().unwrap(),
                "target": "proxy::logging::tests",
                "process_id": actual.as_object().unwrap().get("process_id").unwrap().as_number().unwrap(),
                "thread_id": actual.as_object().unwrap().get("thread_id").unwrap().as_number().unwrap(),
                "thread_name": "logging::tests::test_field_collection",
            }
        );

        assert_json_eq!(actual, expected);
    }
}
