use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use std::str::FromStr;
use uuid::Uuid;

/// High-level job type in your system.
///
/// Stored in the database as a string in `jobs.job_type`,
/// typically in snake_case (e.g. "send_emails").
///
/// When you need to work with the enum, use:
/// `JobType::from_str(&job.job_type)` or `job.job_type_enum()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobType {
    DiscoverProspects,
    EnrichLeads,
    AiPersonalize,
    SendEmails,
    ClientAcquisitionOutreach,
    FetchNews,
}

impl JobType {
    /// Canonical DB / JSON representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            JobType::DiscoverProspects => "discover_prospects",
            JobType::EnrichLeads => "enrich_leads",
            JobType::AiPersonalize => "ai_personalize",
            JobType::SendEmails => "send_emails",
            JobType::ClientAcquisitionOutreach => "client_acquisition_outreach",
            JobType::FetchNews => "fetch_news",
        }
    }
}

impl FromStr for JobType {
    type Err = String;

    /// Parse from DB string.
    ///
    /// Accepts both snake_case and legacy UPPER_CASE variants
    /// so you don't break older data if any exists.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            // snake_case (preferred)
            "discover_prospects" => Ok(JobType::DiscoverProspects),
            "enrich_leads" => Ok(JobType::EnrichLeads),
            "ai_personalize" => Ok(JobType::AiPersonalize),
            "send_emails" => Ok(JobType::SendEmails),
            "client_acquisition_outreach" => Ok(JobType::ClientAcquisitionOutreach),
            "fetch_news" => Ok(JobType::FetchNews),

            // optional: legacy UPPER_CASE support
            "DISCOVER_PROSPECTS" => Ok(JobType::DiscoverProspects),
            "ENRICH_LEADS" => Ok(JobType::EnrichLeads),
            "AI_PERSONALIZE" => Ok(JobType::AiPersonalize),
            "SEND_EMAILS" => Ok(JobType::SendEmails),
            "CLIENT_ACQUISITION_OUTREACH" => Ok(JobType::ClientAcquisitionOutreach),
            "FETCH_NEWS" => Ok(JobType::FetchNews),

            other => Err(format!("Unknown job_type: {}", other)),
        }
    }
}

impl From<JobType> for String {
    fn from(jt: JobType) -> Self {
        jt.as_str().to_string()
    }
}

/// Status of a job in the internal queue.
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

impl FromStr for JobStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(JobStatus::Pending),
            "running" => Ok(JobStatus::Running),
            "done" => Ok(JobStatus::Done),
            "failed" => Ok(JobStatus::Failed),

            "PENDING" => Ok(JobStatus::Pending),
            "RUNNING" => Ok(JobStatus::Running),
            "DONE" => Ok(JobStatus::Done),
            "FAILED" => Ok(JobStatus::Failed),

            other => Err(format!("Unknown job status: {}", other)),
        }
    }
}

impl From<JobStatus> for String {
    fn from(js: JobStatus) -> Self {
        js.as_str().to_string()
    }
}

/// Jobs in the internal queue.
///
/// `job_type` and `status` are stored as strings in the DB for flexibility,
/// but you can convert them to enums using the helper methods below.
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

impl Job {
    /// Parse `job_type` string into `JobType` enum.
    pub fn job_type_enum(&self) -> Result<JobType, String> {
        JobType::from_str(&self.job_type)
    }

    /// Parse `status` string into `JobStatus` enum.
    pub fn status_enum(&self) -> Result<JobStatus, String> {
        JobStatus::from_str(&self.status)
    }
}
