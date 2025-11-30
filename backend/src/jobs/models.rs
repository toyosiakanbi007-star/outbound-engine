use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

/// High-level job type in your system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobType {
    DiscoverProspects,
    EnrichLeads,
    AiPersonalize,
    SendEmails,
    ClientAcquisitionOutreach,
}

impl JobType {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobType::DiscoverProspects => "discover_prospects",
            JobType::EnrichLeads => "enrich_leads",
            JobType::AiPersonalize => "ai_personalize",
            JobType::SendEmails => "send_emails",
            JobType::ClientAcquisitionOutreach => "client_acquisition_outreach",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Running,
    Done,
    Failed,
}

impl JobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobStatus::Pending => "pending",
            JobStatus::Running => "running",
            JobStatus::Done => "done",
            JobStatus::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Job {
    pub id: Uuid,
    pub client_id: Uuid,
    pub campaign_id: Option<Uuid>,
    pub contact_id: Option<Uuid>,

    pub job_type: String,
    pub status: String,

    pub payload: serde_json::Value,
    pub assigned_worker: Option<String>,

    pub run_at: DateTime<Utc>,
    pub attempts: i32,
    pub last_error: Option<String>,

    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}
