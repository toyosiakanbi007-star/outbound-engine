// src/jobs/prequal_worker/models.rs
//
// Data models for the Prequal Worker subsystem.
//
// TWO JOB TYPES:
//   PREQUAL_DISPATCH — planner: selects eligible companies, enqueues batches
//   PREQUAL_BATCH    — executor: calls Azure Function for each company in batch
//
// CONFIG:
//   PrequalConfig is read from client_configs.prequal_config JSONB column.
//   It controls batch size, autopilot, retry limits, etc.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ============================================================================
// Job Payloads
// ============================================================================

/// Payload for PREQUAL_DISPATCH job.
///
/// The dispatcher selects eligible company_candidates, claims them,
/// splits into batches, and enqueues PREQUAL_BATCH jobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrequalDispatchPayload {
    pub client_id: Uuid,

    /// How many companies per batch (overrides config if set).
    #[serde(default)]
    pub batch_size: Option<i32>,

    /// Max number of batches to create in this dispatch.
    #[serde(default)]
    pub max_batches: Option<i32>,

    /// If true, reprocess companies that already have prequal results.
    #[serde(default)]
    pub force: bool,

    /// Optional filters to narrow candidate selection.
    #[serde(default)]
    pub filters: DispatchFilters,

    /// How this dispatch was triggered.
    #[serde(default = "default_dispatch_source")]
    pub source: String,
}

fn default_dispatch_source() -> String {
    "manual".to_string()
}

/// Optional filters for candidate selection during dispatch.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DispatchFilters {
    /// Only select candidates created after this timestamp.
    pub only_new_since: Option<DateTime<Utc>>,

    /// Only select candidates from a specific industry.
    pub industry_name: Option<String>,

    /// Only select candidates from a specific fetch run.
    pub run_id: Option<Uuid>,
}

/// Payload for PREQUAL_BATCH job.
///
/// The batch worker iterates over company_ids, calling the Azure Function
/// with mode="prequal" for each. Failures are per-company, not per-batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrequalBatchPayload {
    pub client_id: Uuid,

    /// List of company_candidate IDs to process.
    pub candidate_ids: Vec<Uuid>,

    /// Unique batch ID for tracking/logging.
    pub batch_id: Uuid,

    /// If true, reprocess even if prequal was already done.
    #[serde(default)]
    pub force: bool,

    /// How this batch was triggered.
    #[serde(default = "default_batch_source")]
    pub source: String,
}

fn default_batch_source() -> String {
    "dispatch".to_string()
}

// ============================================================================
// Config (from client_configs.prequal_config)
// ============================================================================

/// Per-client prequal worker configuration.
///
/// Stored in `client_configs.prequal_config` JSONB column.
/// All fields have sensible defaults so an empty `{}` works.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrequalConfig {
    /// Whether company_fetcher autopilot should trigger prequal dispatch.
    #[serde(default = "default_autopilot_enabled")]
    pub autopilot_enabled: bool,

    /// Number of companies per PREQUAL_BATCH job.
    #[serde(default = "default_batch_size")]
    pub batch_size: i32,

    /// Max batches a single PREQUAL_DISPATCH can create.
    #[serde(default = "default_max_batches")]
    pub max_batches_per_dispatch: i32,

    /// Minutes between scheduled dispatches (used by external cron).
    #[serde(default = "default_dispatch_interval")]
    pub dispatch_interval_minutes: i32,

    /// Optional cap per industry per dispatch to prevent domination.
    #[serde(default)]
    pub max_per_industry_per_dispatch: Option<i32>,

    /// Max retry attempts per company before marking as permanently failed.
    #[serde(default = "default_max_attempts")]
    pub max_attempts_per_company: i32,

    /// Timeout for individual Azure Function prequal calls.
    #[serde(default = "default_azure_timeout")]
    pub azure_function_timeout_secs: u64,
}

fn default_autopilot_enabled() -> bool { true }
fn default_batch_size() -> i32 { 5 }
fn default_max_batches() -> i32 { 10 }
fn default_dispatch_interval() -> i32 { 15 }
fn default_max_attempts() -> i32 { 4 }
fn default_azure_timeout() -> u64 { 120 }

impl Default for PrequalConfig {
    fn default() -> Self {
        Self {
            autopilot_enabled: default_autopilot_enabled(),
            batch_size: default_batch_size(),
            max_batches_per_dispatch: default_max_batches(),
            dispatch_interval_minutes: default_dispatch_interval(),
            max_per_industry_per_dispatch: None,
            max_attempts_per_company: default_max_attempts(),
            azure_function_timeout_secs: default_azure_timeout(),
        }
    }
}

impl PrequalConfig {
    /// Effective batch size: payload override > config > default.
    pub fn effective_batch_size(&self, payload_override: Option<i32>) -> i32 {
        let raw = payload_override.unwrap_or(self.batch_size);
        raw.clamp(1, 50) // hard cap
    }

    /// Effective max batches: payload override > config > default.
    pub fn effective_max_batches(&self, payload_override: Option<i32>) -> i32 {
        let raw = payload_override.unwrap_or(self.max_batches_per_dispatch);
        raw.clamp(1, 100) // hard cap
    }

    /// Max total companies per dispatch.
    pub fn max_candidates_per_dispatch(&self, payload: &PrequalDispatchPayload) -> i32 {
        let batch = self.effective_batch_size(payload.batch_size);
        let batches = self.effective_max_batches(payload.max_batches);
        (batch * batches).min(5000) // hard cap
    }
}

// ============================================================================
// Candidate Info (loaded for batch processing)
// ============================================================================

/// Info about a candidate + its company, loaded for batch processing.
#[derive(Debug, Clone)]
pub struct CandidateCompanyInfo {
    pub candidate_id: Uuid,
    pub company_id: Uuid,
    pub company_name: String,
    pub domain: Option<String>,
    pub industry: Option<String>,
    pub status: String,
    pub prequal_attempts: i32,
}

/// Result of processing a single company in a batch.
#[derive(Debug)]
pub struct CompanyPrequalResult {
    pub candidate_id: Uuid,
    pub company_id: Uuid,
    pub success: bool,
    pub qualifies: Option<bool>,
    pub score: Option<f64>,
    pub error: Option<String>,
}

// ============================================================================
// Batch Outcome (for logging/telemetry)
// ============================================================================

/// Summary of a batch execution.
#[derive(Debug, Default, Serialize)]
pub struct BatchOutcome {
    pub batch_id: Uuid,
    pub total: i32,
    pub processed: i32,
    pub skipped: i32,
    pub succeeded: i32,
    pub failed: i32,
    pub qualified: i32,
    pub disqualified: i32,
    pub elapsed_seconds: u64,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prequal_config_defaults() {
        let config = PrequalConfig::default();
        assert!(config.autopilot_enabled);
        assert_eq!(config.batch_size, 5);
        assert_eq!(config.max_batches_per_dispatch, 10);
        assert_eq!(config.max_attempts_per_company, 4);
    }

    #[test]
    fn test_prequal_config_from_empty_json() {
        let config: PrequalConfig = serde_json::from_str("{}").unwrap();
        assert!(config.autopilot_enabled);
        assert_eq!(config.batch_size, 5);
    }

    #[test]
    fn test_prequal_config_from_partial_json() {
        let config: PrequalConfig = serde_json::from_str(r#"{"batch_size": 10, "autopilot_enabled": false}"#).unwrap();
        assert!(!config.autopilot_enabled);
        assert_eq!(config.batch_size, 10);
        assert_eq!(config.max_batches_per_dispatch, 10); // default
    }

    #[test]
    fn test_effective_batch_size_clamped() {
        let config = PrequalConfig { batch_size: 3, ..Default::default() };
        // No override → uses config value
        assert_eq!(config.effective_batch_size(None), 3);
        // Override respects hard cap
        assert_eq!(config.effective_batch_size(Some(100)), 50);
        // Override respects floor
        assert_eq!(config.effective_batch_size(Some(0)), 1);
    }

    #[test]
    fn test_max_candidates_per_dispatch() {
        let config = PrequalConfig { batch_size: 50, max_batches_per_dispatch: 200, ..Default::default() };
        let payload = PrequalDispatchPayload {
            client_id: Uuid::nil(),
            batch_size: None,
            max_batches: None,
            force: false,
            filters: DispatchFilters::default(),
            source: "test".into(),
        };
        // 50 * 100 (clamped) = 5000, but hard cap is 5000
        assert_eq!(config.max_candidates_per_dispatch(&payload), 5000);
    }

    #[test]
    fn test_dispatch_payload_deserialize() {
        let json = r#"{
            "client_id": "11111111-1111-1111-1111-111111111111",
            "source": "autopilot"
        }"#;
        let payload: PrequalDispatchPayload = serde_json::from_str(json).unwrap();
        assert_eq!(payload.source, "autopilot");
        assert!(!payload.force);
        assert!(payload.batch_size.is_none());
    }

    #[test]
    fn test_batch_payload_deserialize() {
        let json = r#"{
            "client_id": "11111111-1111-1111-1111-111111111111",
            "candidate_ids": ["22222222-2222-2222-2222-222222222222"],
            "batch_id": "33333333-3333-3333-3333-333333333333",
            "source": "dispatch"
        }"#;
        let payload: PrequalBatchPayload = serde_json::from_str(json).unwrap();
        assert_eq!(payload.candidate_ids.len(), 1);
        assert!(!payload.force);
    }
}
