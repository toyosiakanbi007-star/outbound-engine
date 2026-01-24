// src/jobs/funding_analysis.rs
//
// Funding Events Analysis for V3 Phase B.
//
// PURPOSE:
// - Analyze Apollo funding_events to extract investment signals
// - Compute total raised, runway interpretation
// - Detect funding velocity and stage progression
// - Generate natural language interpretation for LLM aggregator
//
// APOLLO STRUCTURE (funding_events):
// [
//   {
//     "id": "6574c1ff9b797d0001fdab1b",
//     "date": "2023-08-01T00:00:00.000+00:00",
//     "news_url": "https://...",
//     "type": "Series D",
//     "investors": "Bain Capital Ventures, Sequoia Capital, Tribe Capital",
//     "amount": "100M",    // STRING like "100M", "7M", "2.2M"
//     "currency": "$"      // STRING
//   }
// ]
//
// NOTE: `amount` is a STRING (e.g., "100M", "7M"), not a number!

use chrono::{DateTime, Datelike, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::PgPool;
use tracing::{info, warn};
use uuid::Uuid;

// ----------------------------
// Data Models (Apollo Structure)
// ----------------------------

/// Single funding event from Apollo
#[derive(Debug, Clone, Deserialize)]
pub struct ApolloFundingEvent {
    pub id: Option<String>,
    
    /// Round type: "Series A", "Series B", "Seed", "Other", etc.
    #[serde(rename = "type")]
    pub funding_type: Option<String>,
    
    /// Amount as STRING: "100M", "7M", "2.2M", etc.
    pub amount: Option<String>,
    
    /// Currency symbol: "$", "€", etc.
    pub currency: Option<String>,
    
    /// Date as ISO string: "2023-08-01T00:00:00.000+00:00"
    pub date: Option<String>,
    
    /// News URL for the funding announcement
    pub news_url: Option<String>,
    
    /// Investors as comma-separated string
    pub investors: Option<String>,
}

impl ApolloFundingEvent {
    /// Parse amount string to numeric value in dollars
    /// "100M" -> 100_000_000
    /// "7M" -> 7_000_000
    /// "2.2M" -> 2_200_000
    /// "500K" -> 500_000
    /// "1.5B" -> 1_500_000_000
    pub fn amount_numeric(&self) -> Option<i64> {
        let amount_str = self.amount.as_ref()?;
        parse_amount_string(amount_str)
    }
    
    /// Parse date string to NaiveDate
    pub fn date_parsed(&self) -> Option<NaiveDate> {
        let date_str = self.date.as_ref()?;
        // Format: "2023-08-01T00:00:00.000+00:00" or "2023-08-01"
        if let Some(date_part) = date_str.split('T').next() {
            NaiveDate::parse_from_str(date_part, "%Y-%m-%d").ok()
        } else {
            None
        }
    }
    
    /// Parse investors string to list
    pub fn investors_list(&self) -> Vec<String> {
        self.investors.as_ref()
            .map(|s| s.split(',').map(|i| i.trim().to_string()).filter(|i| !i.is_empty()).collect())
            .unwrap_or_default()
    }
}

/// Parse amount string like "100M", "7M", "2.2M", "500K", "1.5B"
fn parse_amount_string(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    
    // Remove currency symbols and whitespace
    let cleaned: String = s.chars()
        .filter(|c| c.is_ascii_digit() || *c == '.' || *c == 'M' || *c == 'K' || *c == 'B' || *c == 'm' || *c == 'k' || *c == 'b')
        .collect();
    
    if cleaned.is_empty() {
        return None;
    }
    
    let last_char = cleaned.chars().last()?;
    let multiplier: f64 = match last_char.to_ascii_uppercase() {
        'B' => 1_000_000_000.0,
        'M' => 1_000_000.0,
        'K' => 1_000.0,
        _ => 1.0,
    };
    
    let number_part: String = if last_char.is_ascii_alphabetic() {
        cleaned[..cleaned.len()-1].to_string()
    } else {
        cleaned
    };
    
    let value: f64 = number_part.parse().ok()?;
    Some((value * multiplier) as i64)
}

// ----------------------------
// Analysis Output Models
// ----------------------------

/// Analyzed funding data (output)
#[derive(Debug, Clone, Serialize, Default)]
pub struct FundingAnalysis {
    // Totals
    pub total_raised: i64,
    pub total_raised_formatted: String,
    pub funding_rounds_count: i32,
    
    // Last round
    pub last_round: Option<LastRoundInfo>,
    
    // Stage and velocity
    pub funding_velocity: String,  // "accelerating", "steady", "slowing", "stalled"
    pub stage: String,  // "pre_seed", "seed", "early_growth", "growth", "late_stage"
    
    // Interpretation for LLM
    pub interpretation: String,
    pub signals: Vec<String>,
    
    // Investors
    pub notable_investors: Vec<String>,
    pub investor_count: i32,
    
    // Timeline
    pub first_funding_date: Option<String>,
    pub years_since_first_funding: Option<f64>,
    
    pub analysis_timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LastRoundInfo {
    #[serde(rename = "type")]
    pub round_type: String,
    pub amount: i64,
    pub amount_formatted: String,
    pub date: String,
    pub months_ago: i32,
    pub investors: Vec<String>,
    pub news_url: Option<String>,
}

// ----------------------------
// Analysis Logic
// ----------------------------

/// Analyze funding events and return structured analysis.
pub fn analyze_funding_events(events: &[ApolloFundingEvent]) -> FundingAnalysis {
    let mut analysis = FundingAnalysis {
        analysis_timestamp: Utc::now(),
        funding_velocity: "unknown".to_string(),
        stage: "unknown".to_string(),
        ..Default::default()
    };
    
    if events.is_empty() {
        analysis.interpretation = "No funding history available.".to_string();
        return analysis;
    }
    
    // Filter events with dates and sort (oldest first)
    let mut sorted_events: Vec<_> = events.iter()
        .filter(|e| e.date.is_some())
        .collect();
    sorted_events.sort_by(|a, b| {
        a.date.as_ref().unwrap().cmp(b.date.as_ref().unwrap())
    });
    
    if sorted_events.is_empty() {
        analysis.interpretation = "Funding events found but no dates available.".to_string();
        return analysis;
    }
    
    // Calculate totals
    analysis.funding_rounds_count = sorted_events.len() as i32;
    analysis.total_raised = sorted_events.iter()
        .filter_map(|e| e.amount_numeric())
        .sum();
    analysis.total_raised_formatted = format_amount(analysis.total_raised);
    
    // First funding date
    if let Some(first) = sorted_events.first() {
        if let Some(date) = first.date_parsed() {
            analysis.first_funding_date = Some(date.to_string());
            let now = Utc::now().naive_utc().date();
            let days = (now - date).num_days();
            analysis.years_since_first_funding = Some(days as f64 / 365.0);
        }
    }
    
    // Collect all investors
    let mut all_investors: Vec<String> = sorted_events.iter()
        .flat_map(|e| e.investors_list())
        .collect();
    all_investors.sort();
    all_investors.dedup();
    analysis.investor_count = all_investors.len() as i32;
    analysis.notable_investors = all_investors.into_iter().take(10).collect();
    
    // Analyze last round
    if let Some(last) = sorted_events.last() {
        let months_ago = last.date_parsed()
            .map(|d| calculate_months_ago(&d))
            .unwrap_or(-1);
        
        analysis.last_round = Some(LastRoundInfo {
            round_type: last.funding_type.clone().unwrap_or_else(|| "Unknown".to_string()),
            amount: last.amount_numeric().unwrap_or(0),
            amount_formatted: last.amount.clone().map(|a| format!("{}{}", last.currency.as_deref().unwrap_or("$"), a)).unwrap_or_else(|| "Undisclosed".to_string()),
            date: last.date.clone().unwrap_or_default(),
            months_ago,
            investors: last.investors_list(),
            news_url: last.news_url.clone(),
        });
    }
    
    // Determine stage
    analysis.stage = determine_stage(&sorted_events, analysis.total_raised);
    
    // Determine velocity
    analysis.funding_velocity = determine_velocity(&sorted_events);
    
    // Generate signals
    analysis.signals = generate_signals(&analysis, &sorted_events);
    
    // Generate interpretation
    analysis.interpretation = generate_interpretation(&analysis);
    
    analysis
}

/// Format amount as human-readable string
fn format_amount(amount: i64) -> String {
    if amount >= 1_000_000_000 {
        format!("${:.1}B", amount as f64 / 1_000_000_000.0)
    } else if amount >= 1_000_000 {
        format!("${:.1}M", amount as f64 / 1_000_000.0)
    } else if amount >= 1_000 {
        format!("${:.0}K", amount as f64 / 1_000.0)
    } else if amount > 0 {
        format!("${}", amount)
    } else {
        "Undisclosed".to_string()
    }
}

/// Calculate months since a date
fn calculate_months_ago(date: &NaiveDate) -> i32 {
    let now = Utc::now().naive_utc().date();
    let months = (now.year() - date.year()) * 12 + (now.month() as i32 - date.month() as i32);
    months.max(0)
}

/// Determine company stage from funding history
fn determine_stage(events: &[&ApolloFundingEvent], total_raised: i64) -> String {
    // Look at the most recent round type
    if let Some(last) = events.last() {
        if let Some(round_type) = &last.funding_type {
            let rt_lower = round_type.to_lowercase();
            
            if rt_lower.contains("series d") || rt_lower.contains("series e") || 
               rt_lower.contains("series f") || rt_lower.contains("ipo") ||
               rt_lower.contains("series g") {
                return "late_stage".to_string();
            }
            if rt_lower.contains("series c") {
                return "growth".to_string();
            }
            if rt_lower.contains("series b") {
                return "early_growth".to_string();
            }
            if rt_lower.contains("series a") {
                return "early_growth".to_string();
            }
            if rt_lower.contains("seed") {
                return "seed".to_string();
            }
            if rt_lower.contains("pre-seed") || rt_lower.contains("angel") {
                return "pre_seed".to_string();
            }
        }
    }
    
    // Fallback: estimate from total raised
    if total_raised >= 100_000_000 {
        "late_stage".to_string()
    } else if total_raised >= 30_000_000 {
        "growth".to_string()
    } else if total_raised >= 5_000_000 {
        "early_growth".to_string()
    } else if total_raised >= 500_000 {
        "seed".to_string()
    } else {
        "pre_seed".to_string()
    }
}

/// Determine funding velocity
fn determine_velocity(events: &[&ApolloFundingEvent]) -> String {
    if events.len() < 2 {
        return "unknown".to_string();
    }
    
    let last = events.last().unwrap();
    let prev = events[events.len() - 2];
    
    let last_months = last.date_parsed()
        .map(|d| calculate_months_ago(&d))
        .unwrap_or(-1);
    let prev_months = prev.date_parsed()
        .map(|d| calculate_months_ago(&d))
        .unwrap_or(-1);
    
    if last_months < 0 || prev_months < 0 {
        return "unknown".to_string();
    }
    
    // Check if last round was recent
    if last_months > 36 {
        return "stalled".to_string();
    }
    
    let gap = prev_months - last_months;
    
    if gap < 12 {
        "accelerating".to_string()
    } else if gap < 24 {
        "steady".to_string()
    } else {
        "slowing".to_string()
    }
}

/// Generate structured signals
fn generate_signals(analysis: &FundingAnalysis, _events: &[&ApolloFundingEvent]) -> Vec<String> {
    let mut signals = Vec::new();
    
    if let Some(ref last) = analysis.last_round {
        // Recency signals
        if last.months_ago <= 6 {
            signals.push("very_recent_funding".to_string());
        } else if last.months_ago <= 12 {
            signals.push("recent_funding".to_string());
        } else if last.months_ago <= 24 {
            signals.push("funding_within_2_years".to_string());
        }
        
        // Amount signals
        if last.amount >= 100_000_000 {
            signals.push("mega_round".to_string());
        } else if last.amount >= 50_000_000 {
            signals.push("large_round".to_string());
        }
        
        // Stage signals
        let rt_lower = last.round_type.to_lowercase();
        if rt_lower.contains("series a") {
            signals.push("series_a_company".to_string());
            signals.push("expansion_likely".to_string());
        } else if rt_lower.contains("series b") {
            signals.push("series_b_company".to_string());
            signals.push("scaling_phase".to_string());
        } else if rt_lower.contains("series c") {
            signals.push("series_c_company".to_string());
            signals.push("growth_stage".to_string());
        } else if rt_lower.contains("series d") || rt_lower.contains("series e") {
            signals.push("late_stage_company".to_string());
        }
    }
    
    // Velocity signals
    match analysis.funding_velocity.as_str() {
        "accelerating" => signals.push("accelerating_funding".to_string()),
        "stalled" => signals.push("funding_stalled".to_string()),
        _ => {}
    }
    
    // Total raised signals
    if analysis.total_raised >= 100_000_000 {
        signals.push("well_funded".to_string());
    }
    
    // Stage-based implications
    match analysis.stage.as_str() {
        "early_growth" | "growth" => {
            signals.push("tooling_upgrades_likely".to_string());
            signals.push("process_improvements_expected".to_string());
        }
        "late_stage" => {
            signals.push("enterprise_ready".to_string());
            signals.push("mature_operations".to_string());
        }
        _ => {}
    }
    
    signals
}

/// Generate natural language interpretation
fn generate_interpretation(analysis: &FundingAnalysis) -> String {
    let mut parts = Vec::new();
    
    // Last round info
    if let Some(ref last) = analysis.last_round {
        if last.amount > 0 {
            parts.push(format!(
                "Raised {} {} ({} months ago).",
                last.amount_formatted,
                last.round_type,
                last.months_ago
            ));
        } else {
            parts.push(format!(
                "Last funding: {} ({} months ago).",
                last.round_type,
                last.months_ago
            ));
        }
    }
    
    // Total raised
    if analysis.total_raised > 0 && analysis.funding_rounds_count > 1 {
        parts.push(format!(
            "Total raised: {} across {} rounds.",
            analysis.total_raised_formatted,
            analysis.funding_rounds_count
        ));
    }
    
    // Implications based on recency
    if let Some(ref last) = analysis.last_round {
        if last.months_ago <= 12 {
            parts.push("Cash runway likely improved.".to_string());
            
            let rt_lower = last.round_type.to_lowercase();
            if rt_lower.contains("series a") {
                parts.push("Expect product-market fit push and initial scaling.".to_string());
            } else if rt_lower.contains("series b") {
                parts.push("Expect aggressive growth and team expansion.".to_string());
            } else if rt_lower.contains("series c") || rt_lower.contains("series d") {
                parts.push("Scaling operations, potential IPO/acquisition preparation.".to_string());
            }
        } else if last.months_ago > 36 {
            parts.push("No recent funding - may be bootstrapped, profitable, or seeking new round.".to_string());
        }
    }
    
    // Notable investors
    if analysis.notable_investors.len() > 2 {
        let top_3: Vec<_> = analysis.notable_investors.iter().take(3).cloned().collect();
        parts.push(format!("Backed by: {}.", top_3.join(", ")));
    }
    
    parts.join(" ")
}

// ----------------------------
// Database Operations
// ----------------------------

/// Load funding events from company_apollo_enrichment table.
pub async fn load_funding_events(
    pool: &PgPool,
    company_id: Uuid,
) -> Result<Vec<ApolloFundingEvent>, sqlx::Error> {
    let row: Option<(JsonValue,)> = sqlx::query_as(
        r#"
        SELECT funding_events
        FROM company_apollo_enrichment
        WHERE company_id = $1
        "#,
    )
    .bind(company_id)
    .fetch_optional(pool)
    .await?;
    
    match row {
        Some((json_val,)) => {
            match serde_json::from_value::<Vec<ApolloFundingEvent>>(json_val) {
                Ok(events) => Ok(events),
                Err(e) => {
                    warn!("Failed to parse funding_events for company {}: {}", company_id, e);
                    Ok(Vec::new())
                }
            }
        }
        None => Ok(Vec::new()),
    }
}

/// Save funding analysis to company_apollo_enrichment table.
pub async fn save_funding_analysis(
    pool: &PgPool,
    company_id: Uuid,
    analysis: &FundingAnalysis,
) -> Result<(), sqlx::Error> {
    let analysis_json = serde_json::to_value(analysis)
        .map_err(|e| sqlx::Error::Protocol(format!("JSON serialization failed: {}", e)))?;
    
    // Parse last funding date for summary field
    let last_funding_date: Option<NaiveDate> = analysis.last_round.as_ref()
        .and_then(|r| {
            r.date.split('T').next()
                .and_then(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())
        });
    
    let last_funding_type: Option<&str> = analysis.last_round.as_ref()
        .map(|r| r.round_type.as_str());
    
    sqlx::query(
        r#"
        UPDATE company_apollo_enrichment
        SET funding_analysis = $2,
            total_funding_raised = $3,
            last_funding_date = $4,
            last_funding_type = $5,
            analyzed_at = NOW()
        WHERE company_id = $1
        "#,
    )
    .bind(company_id)
    .bind(analysis_json)
    .bind(analysis.total_raised)
    .bind(last_funding_date)
    .bind(last_funding_type)
    .execute(pool)
    .await?;
    
    info!("Saved funding analysis for company {}", company_id);
    
    Ok(())
}

// ----------------------------
// Job Handler
// ----------------------------

/// Payload for ANALYZE_FUNDING_EVENTS job.
#[derive(Debug, Clone, Deserialize)]
pub struct AnalyzeFundingEventsPayload {
    pub company_id: Uuid,
    pub client_id: Uuid,
}

/// Handle ANALYZE_FUNDING_EVENTS job.
pub async fn handle_analyze_funding_events(
    pool: &PgPool,
    payload: &AnalyzeFundingEventsPayload,
    worker_id: &str,
) -> Result<FundingAnalysis, sqlx::Error> {
    info!(
        "Worker {}: analyzing funding events for company {}",
        worker_id, payload.company_id
    );
    
    // Load funding events
    let events = load_funding_events(pool, payload.company_id).await?;
    
    if events.is_empty() {
        info!(
            "Worker {}: no funding events found for company {}",
            worker_id, payload.company_id
        );
        return Ok(FundingAnalysis::default());
    }
    
    // Analyze
    let analysis = analyze_funding_events(&events);
    
    // Save analysis
    save_funding_analysis(pool, payload.company_id, &analysis).await?;
    
    info!(
        "Worker {}: funding analysis complete for company {} (total={}, rounds={}, stage={})",
        worker_id,
        payload.company_id,
        analysis.total_raised_formatted,
        analysis.funding_rounds_count,
        analysis.stage
    );
    
    Ok(analysis)
}

// ----------------------------
// Tests
// ----------------------------

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_amount_string() {
        assert_eq!(parse_amount_string("100M"), Some(100_000_000));
        assert_eq!(parse_amount_string("7M"), Some(7_000_000));
        assert_eq!(parse_amount_string("2.2M"), Some(2_200_000));
        assert_eq!(parse_amount_string("500K"), Some(500_000));
        assert_eq!(parse_amount_string("1.5B"), Some(1_500_000_000));
        assert_eq!(parse_amount_string("$50M"), Some(50_000_000));
        assert_eq!(parse_amount_string(""), None);
    }
    
    #[test]
    fn test_analyze_funding() {
        let events = vec![
            ApolloFundingEvent {
                id: Some("1".to_string()),
                funding_type: Some("Seed".to_string()),
                amount: Some("2M".to_string()),
                currency: Some("$".to_string()),
                date: Some("2022-01-15T00:00:00.000+00:00".to_string()),
                news_url: None,
                investors: Some("Angel Investor".to_string()),
            },
            ApolloFundingEvent {
                id: Some("2".to_string()),
                funding_type: Some("Series A".to_string()),
                amount: Some("15M".to_string()),
                currency: Some("$".to_string()),
                date: Some("2023-06-15T00:00:00.000+00:00".to_string()),
                news_url: Some("https://techcrunch.com/...".to_string()),
                investors: Some("Sequoia Capital, a16z".to_string()),
            },
        ];
        
        let analysis = analyze_funding_events(&events);
        
        assert_eq!(analysis.total_raised, 17_000_000);
        assert_eq!(analysis.funding_rounds_count, 2);
        assert_eq!(analysis.stage, "early_growth");
        assert!(analysis.last_round.is_some());
        assert!(analysis.interpretation.contains("Series A"));
    }
    
    #[test]
    fn test_investors_parsing() {
        let event = ApolloFundingEvent {
            id: None,
            funding_type: None,
            amount: None,
            currency: None,
            date: None,
            news_url: None,
            investors: Some("Bain Capital Ventures, Sequoia Capital, Tribe Capital".to_string()),
        };
        
        let investors = event.investors_list();
        assert_eq!(investors.len(), 3);
        assert_eq!(investors[0], "Bain Capital Ventures");
    }
}
