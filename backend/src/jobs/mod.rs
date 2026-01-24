// src/jobs/mod.rs

pub mod models;
pub mod service;

// V3: Phase B modules
pub mod employee_metrics;
pub mod funding_analysis;
pub mod phase_b_controller;
pub mod prequal_listener;

// Re-export commonly used items for convenience
pub use models::{Job, JobStatus, JobType};

// V3: Re-export Phase B handlers and payloads
pub use employee_metrics::{
    handle_analyze_employee_metrics,
    AnalyzeEmployeeMetricsPayload,
    EmployeeMetricsAnalysis,
};

pub use funding_analysis::{
    handle_analyze_funding_events,
    AnalyzeFundingEventsPayload,
    FundingAnalysis,
};

pub use phase_b_controller::{
    handle_phase_b_enrich,
    handle_prequal_notification,
    PhaseBPayload,
    PrequalReadyNotification,
};

pub use prequal_listener::run_prequal_listener;
