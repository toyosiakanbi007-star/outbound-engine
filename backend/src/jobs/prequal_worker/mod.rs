// src/jobs/prequal_worker/mod.rs
//
// Prequal Worker subsystem.
//
// Automates the flow: fetched companies → batched prequal via Azure Function.
//
// TWO JOB TYPES:
//   PREQUAL_DISPATCH  — planner: select eligible → claim → enqueue batches
//   PREQUAL_BATCH     — executor: for each company → call Azure Function → update status
//
// THREE TRIGGER MODES:
//   1. Autopilot:  company_fetcher enqueues PREQUAL_DISPATCH after successful run
//   2. Scheduled:  external cron enqueues PREQUAL_DISPATCH periodically
//   3. Manual:     API/admin enqueues PREQUAL_DISPATCH or PREQUAL_BATCH directly
//
// CONFIG:
//   Per-client settings in client_configs.prequal_config JSONB column.
//   Controls batch_size, max_batches, autopilot, retry limits.

pub mod batch;
pub mod db;
pub mod dispatch;
pub mod models;

// Re-export commonly used items for worker.rs
pub use batch::handle_prequal_batch;
pub use dispatch::handle_prequal_dispatch;
pub use models::{PrequalBatchPayload, PrequalConfig, PrequalDispatchPayload};
