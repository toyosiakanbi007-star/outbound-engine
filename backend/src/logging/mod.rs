// src/logging/mod.rs
//
// Structured logging subsystem.
//
// Provides a tracing subscriber layer that captures log events
// and batch-inserts them into the structured_logs Postgres table.

pub mod db_layer;
pub mod flusher;

pub use db_layer::DbLogLayer;
pub use flusher::LogFlusher;
