// src/logging/flusher.rs
//
// Background task that collects LogEntry items from a channel
// and batch-inserts them into the structured_logs table.
//
// Flush triggers:
// - Buffer reaches 100 entries
// - 5 seconds elapsed since last flush
//
// Graceful degradation: if DB insert fails, entries are dropped
// (console logging is the primary output; DB logs are secondary).

use sqlx::PgPool;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{interval, Instant};
use tracing::{debug, error, warn};

use super::db_layer::LogEntry;

const FLUSH_INTERVAL_SECS: u64 = 5;
const FLUSH_BATCH_SIZE: usize = 100;
const MAX_BUFFER_SIZE: usize = 1000; // Drop oldest if buffer exceeds this

/// Background flusher that receives LogEntry items and batch-inserts them.
pub struct LogFlusher {
    receiver: mpsc::Receiver<LogEntry>,
    pool: PgPool,
    buffer: Vec<LogEntry>,
    last_flush: Instant,
}

impl LogFlusher {
    /// Create a new LogFlusher.
    ///
    /// Returns the flusher (to be spawned) and a sender (to give to DbLogLayer).
    pub fn new(pool: PgPool, channel_size: usize) -> (Self, mpsc::Sender<LogEntry>) {
        let (sender, receiver) = mpsc::channel(channel_size);
        let flusher = Self {
            receiver,
            pool,
            buffer: Vec::with_capacity(FLUSH_BATCH_SIZE),
            last_flush: Instant::now(),
        };
        (flusher, sender)
    }

    /// Run the flusher loop. Call this inside tokio::spawn.
    pub async fn run(mut self) {
        let mut tick = interval(Duration::from_secs(FLUSH_INTERVAL_SECS));

        loop {
            tokio::select! {
                // Receive entries from the channel
                entry = self.receiver.recv() => {
                    match entry {
                        Some(entry) => {
                            self.buffer.push(entry);

                            // Drop oldest entries if buffer gets too large
                            if self.buffer.len() > MAX_BUFFER_SIZE {
                                let excess = self.buffer.len() - MAX_BUFFER_SIZE;
                                self.buffer.drain(..excess);
                                warn!("Log buffer overflow: dropped {} entries", excess);
                            }

                            // Flush if batch size reached
                            if self.buffer.len() >= FLUSH_BATCH_SIZE {
                                self.flush().await;
                            }
                        }
                        None => {
                            // Channel closed — flush remaining and exit
                            if !self.buffer.is_empty() {
                                self.flush().await;
                            }
                            debug!("Log flusher channel closed, exiting");
                            return;
                        }
                    }
                }

                // Periodic flush
                _ = tick.tick() => {
                    if !self.buffer.is_empty()
                        && self.last_flush.elapsed() >= Duration::from_secs(FLUSH_INTERVAL_SECS)
                    {
                        self.flush().await;
                    }
                }
            }
        }
    }

    /// Batch-insert buffered entries into structured_logs.
    async fn flush(&mut self) {
        if self.buffer.is_empty() {
            return;
        }

        let entries: Vec<LogEntry> = self.buffer.drain(..).collect();
        let count = entries.len();

        // Build batch INSERT using unnest for efficiency
        let result = self.insert_batch(&entries).await;

        match result {
            Ok(inserted) => {
                debug!("Flushed {} log entries to DB ({} inserted)", count, inserted);
            }
            Err(e) => {
                // Log to console (NOT to the DB layer to avoid infinite loop)
                eprintln!(
                    "[log-flusher] Failed to insert {} log entries: {:?}",
                    count, e
                );
            }
        }

        self.last_flush = Instant::now();
    }

    /// Insert a batch of entries using a single multi-row INSERT.
    async fn insert_batch(&self, entries: &[LogEntry]) -> Result<u64, sqlx::Error> {
        // For simplicity and reliability, use individual inserts in a transaction.
        // For higher throughput, switch to COPY or unnest arrays.
        let mut tx = self.pool.begin().await?;

        let mut inserted: u64 = 0;

        for entry in entries {
            let result = sqlx::query(
                r#"INSERT INTO structured_logs
                       (timestamp, level, service, module,
                        run_id, company_id, client_id, job_id,
                        message, data_json)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"#,
            )
            .bind(entry.timestamp)
            .bind(&entry.level)
            .bind(&entry.service)
            .bind(entry.module.as_deref())
            .bind(entry.run_id)
            .bind(entry.company_id)
            .bind(entry.client_id)
            .bind(entry.job_id)
            .bind(&entry.message)
            .bind(&entry.data_json)
            .execute(&mut *tx)
            .await;

            match result {
                Ok(_) => inserted += 1,
                Err(e) => {
                    // Skip individual failures (e.g. constraint violations)
                    eprintln!("[log-flusher] Skipping entry: {:?}", e);
                }
            }
        }

        tx.commit().await?;
        Ok(inserted)
    }
}

/// Convenience function to set up the logging pipeline.
///
/// Returns a DbLogLayer to add to the tracing subscriber,
/// and a JoinHandle for the flusher background task.
///
/// Usage in main.rs:
/// ```rust
/// let (log_layer, flusher_handle) = logging::setup_db_logging(pool.clone(), "worker");
///
/// tracing_subscriber::registry()
///     .with(fmt_layer)
///     .with(log_layer)
///     .init();
/// ```
pub fn setup_db_logging(
    pool: PgPool,
    service: &str,
) -> (
    super::DbLogLayer,
    tokio::task::JoinHandle<()>,
) {
    let channel_size = 2000;
    let (flusher, sender) = LogFlusher::new(pool, channel_size);

    let layer = super::DbLogLayer::new(
        sender,
        service.to_string(),
        tracing::Level::INFO, // Only persist INFO and above
    );

    let handle = tokio::spawn(flusher.run());

    (layer, handle)
}
