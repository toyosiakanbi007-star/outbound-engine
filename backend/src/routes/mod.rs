// src/routes/mod.rs

pub mod health;
pub mod clients;
pub mod discovery;
pub mod companies;
pub mod prequal;
pub mod pipeline;
pub mod queue;
pub mod logs;
pub mod onboarding;
pub mod manage;

// Legacy endpoints (keep for backward compat)
pub mod debug;
pub mod news;
