// src/logging/db_layer.rs
//
// Tracing subscriber layer that captures events and sends them
// to a background flusher for batch insertion into structured_logs.
//
// Non-blocking: uses an async mpsc channel. If the channel is full,
// events are dropped (console logging still works as primary output).
//
// Context extraction: reads span fields (run_id, company_id, client_id,
// job_id) from the current span chain to attach to log entries.

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;
use uuid::Uuid;

/// A single log entry to be inserted into structured_logs.
#[derive(Debug, Clone, Serialize)]
pub struct LogEntry {
    pub timestamp: DateTime<Utc>,
    pub level: String,
    pub service: String,
    pub module: Option<String>,
    pub run_id: Option<Uuid>,
    pub company_id: Option<Uuid>,
    pub client_id: Option<Uuid>,
    pub job_id: Option<Uuid>,
    pub message: String,
    pub data_json: Option<JsonValue>,
}

/// Tracing layer that sends LogEntry to a channel for batch DB insertion.
pub struct DbLogLayer {
    sender: mpsc::Sender<LogEntry>,
    service: String,
    min_level: tracing::Level,
}

impl DbLogLayer {
    /// Create a new DbLogLayer.
    ///
    /// - `sender`: channel to the LogFlusher background task
    /// - `service`: service name (e.g. "backend", "worker")
    /// - `min_level`: minimum level to persist (e.g. Level::INFO to skip DEBUG/TRACE)
    pub fn new(
        sender: mpsc::Sender<LogEntry>,
        service: impl Into<String>,
        min_level: tracing::Level,
    ) -> Self {
        Self {
            sender,
            service: service.into(),
            min_level,
        }
    }
}

impl<S> Layer<S> for DbLogLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        // Skip events below minimum level
        let meta = event.metadata();
        if *meta.level() > self.min_level {
            return;
        }

        // Extract event fields (message + any extra fields)
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);

        // Walk the span chain to find context UUIDs
        let mut run_id: Option<Uuid> = None;
        let mut company_id: Option<Uuid> = None;
        let mut client_id: Option<Uuid> = None;
        let mut job_id: Option<Uuid> = None;

        if let Some(scope) = ctx.event_scope(event) {
            for span in scope {
                let extensions = span.extensions();
                if let Some(fields) = extensions.get::<SpanFields>() {
                    if run_id.is_none() {
                        run_id = fields.run_id;
                    }
                    if company_id.is_none() {
                        company_id = fields.company_id;
                    }
                    if client_id.is_none() {
                        client_id = fields.client_id;
                    }
                    if job_id.is_none() {
                        job_id = fields.job_id;
                    }
                }
            }
        }

        // Also check event-level fields for IDs
        if run_id.is_none() {
            run_id = visitor.uuid_field("run_id");
        }
        if company_id.is_none() {
            company_id = visitor.uuid_field("company_id");
        }
        if client_id.is_none() {
            client_id = visitor.uuid_field("client_id");
        }
        if job_id.is_none() {
            job_id = visitor.uuid_field("job_id");
        }

        // Build data_json from extra fields (excluding message and known IDs)
        let data_json = if visitor.extra_fields.is_empty() {
            None
        } else {
            Some(serde_json::to_value(&visitor.extra_fields).unwrap_or(JsonValue::Null))
        };

        let entry = LogEntry {
            timestamp: Utc::now(),
            level: meta.level().to_string().to_lowercase(),
            service: self.service.clone(),
            module: meta.module_path().map(String::from),
            run_id,
            company_id,
            client_id,
            job_id,
            message: visitor.message,
            data_json,
        };

        // Non-blocking send — drop if channel is full
        let _ = self.sender.try_send(entry);
    }

    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: Context<'_, S>,
    ) {
        let mut fields = SpanFields::default();
        attrs.record(&mut fields);

        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(fields);
        }
    }
}

// ============================================================================
// Span field storage
// ============================================================================

/// Stored in span extensions to carry context UUIDs.
#[derive(Debug, Default)]
struct SpanFields {
    run_id: Option<Uuid>,
    company_id: Option<Uuid>,
    client_id: Option<Uuid>,
    job_id: Option<Uuid>,
}

impl Visit for SpanFields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let s = format!("{:?}", value);
        match field.name() {
            "run_id" => self.run_id = Uuid::parse_str(s.trim_matches('"')).ok(),
            "company_id" => self.company_id = Uuid::parse_str(s.trim_matches('"')).ok(),
            "client_id" => self.client_id = Uuid::parse_str(s.trim_matches('"')).ok(),
            "job_id" => self.job_id = Uuid::parse_str(s.trim_matches('"')).ok(),
            _ => {}
        }
    }
}

// ============================================================================
// Event field visitor
// ============================================================================

#[derive(Default)]
struct FieldVisitor {
    message: String,
    extra_fields: HashMap<String, JsonValue>,
}

impl FieldVisitor {
    fn uuid_field(&self, name: &str) -> Option<Uuid> {
        self.extra_fields
            .get(name)
            .and_then(|v| v.as_str())
            .and_then(|s| Uuid::parse_str(s).ok())
    }
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let name = field.name();
        let val_str = format!("{:?}", value);

        if name == "message" {
            self.message = val_str.trim_matches('"').to_string();
        } else if !matches!(name, "run_id" | "company_id" | "client_id" | "job_id") {
            self.extra_fields
                .insert(name.to_string(), JsonValue::String(val_str));
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        let name = field.name();
        if name == "message" {
            self.message = value.to_string();
        } else if !matches!(name, "run_id" | "company_id" | "client_id" | "job_id") {
            self.extra_fields
                .insert(name.to_string(), JsonValue::String(value.to_string()));
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.extra_fields
            .insert(field.name().to_string(), JsonValue::from(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.extra_fields
            .insert(field.name().to_string(), JsonValue::from(value));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.extra_fields
            .insert(field.name().to_string(), JsonValue::from(value));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.extra_fields
            .insert(field.name().to_string(), JsonValue::from(value));
    }
}
