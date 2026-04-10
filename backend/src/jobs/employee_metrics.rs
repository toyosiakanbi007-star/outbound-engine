// src/jobs/employee_metrics.rs
//
// Employee Metrics Analysis for V3 Phase B.
//
// PURPOSE:
// - Analyze Apollo employee_metrics data to extract hiring/growth signals
// - Compute headcount growth rates (30/90/180 days)
// - Detect department spikes dynamically (any department Apollo returns)
// - Identify contraction signals (layoffs/decline)
// - Flag anomalies (sudden hiring surges)
//
// APOLLO STRUCTURE (employee_metrics):
// [
//   {
//     "start_date": "2024-01-01",
//     "departments": [
//       { "functions": null, "new": 10, "retained": 500, "churned": 5 },  // Total
//       { "functions": "engineering", "new": 3, "retained": 200, "churned": 1 },
//       { "functions": "sales", "new": 5, "retained": 100, "churned": 2 },
//       ...any other department Apollo returns
//     ]
//   },
//   ...more months
// ]

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::PgPool;
use std::collections::HashMap;
use tracing::{error, info, warn};
use uuid::Uuid;

// ----------------------------
// Data Models (Apollo Structure)
// ----------------------------

/// Monthly employee metrics snapshot from Apollo
#[derive(Debug, Clone, Deserialize)]
pub struct MonthlySnapshot {
    pub start_date: String,  // "2024-01-01" format
    
    #[serde(default)]
    pub departments: Vec<DepartmentMetrics>,
}

/// Department metrics within a monthly snapshot
#[derive(Debug, Clone, Deserialize)]
pub struct DepartmentMetrics {
    /// Department name, or null for company-wide total
    pub functions: Option<String>,
    
    /// New hires this month
    #[serde(default)]
    pub new: i32,
    
    /// Retained employees
    #[serde(default)]
    pub retained: i32,
    
    /// Employees who left (churned)
    #[serde(default)]
    pub churned: i32,
}

impl DepartmentMetrics {
    /// Total headcount = retained + new (churned already left)
    pub fn total(&self) -> i32 {
        self.retained + self.new
    }
    
    /// Net change = new - churned
    pub fn net_change(&self) -> i32 {
        self.new - self.churned
    }
}

// ----------------------------
// Analysis Output Models
// ----------------------------

/// Analyzed employee metrics (output)
#[derive(Debug, Clone, Serialize, Default)]
pub struct EmployeeMetricsAnalysis {
    // Overall headcount
    pub current_headcount: Option<i32>,
    pub headcount_trend: String,  // "growing", "stable", "declining"
    
    // Growth rates
    pub headcount_growth_rate_30d: Option<f64>,
    pub headcount_growth_rate_90d: Option<f64>,
    pub headcount_growth_rate_180d: Option<f64>,
    
    // Net hires (new - churned) over periods
    pub net_hires_30d: Option<i32>,
    pub net_hires_90d: Option<i32>,
    pub net_hires_180d: Option<i32>,
    
    // Department analysis (dynamic - any department)
    pub department_spikes: Vec<DepartmentSpike>,
    pub department_contractions: Vec<DepartmentContraction>,
    pub department_summary: HashMap<String, DepartmentSummary>,
    
    // Signals
    pub contraction_signals: bool,
    pub anomaly_flags: Vec<String>,
    pub hiring_velocity: String,  // "aggressive", "moderate", "slow", "contracting"
    
    // Churn analysis
    pub total_churned_90d: i32,
    pub churn_rate_90d: Option<f64>,
    
    pub analysis_timestamp: DateTime<Utc>,
    pub months_of_data: i32,
}

#[derive(Debug, Clone, Serialize)]
pub struct DepartmentSpike {
    pub department: String,
    pub growth_pct: f64,
    pub period: String,  // "30d", "90d", "180d"
    pub absolute_change: i32,
    pub new_hires: i32,
    pub current_headcount: i32,
}

#[derive(Debug, Clone, Serialize)]
pub struct DepartmentContraction {
    pub department: String,
    pub decline_pct: f64,
    pub period: String,
    pub churned: i32,
    pub current_headcount: i32,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct DepartmentSummary {
    pub current_headcount: i32,
    pub new_hires_90d: i32,
    pub churned_90d: i32,
    pub net_change_90d: i32,
    pub growth_rate_90d: Option<f64>,
}

// ----------------------------
// Analysis Logic
// ----------------------------

/// Analyze employee metrics and return structured analysis.
pub fn analyze_employee_metrics(snapshots: &[MonthlySnapshot]) -> EmployeeMetricsAnalysis {
    let mut analysis = EmployeeMetricsAnalysis {
        analysis_timestamp: Utc::now(),
        headcount_trend: "stable".to_string(),
        hiring_velocity: "moderate".to_string(),
        ..Default::default()
    };
    
    if snapshots.is_empty() {
        return analysis;
    }
    
    // Sort by date (oldest first)
    let mut sorted: Vec<_> = snapshots.iter().collect();
    sorted.sort_by(|a, b| a.start_date.cmp(&b.start_date));
    
    analysis.months_of_data = sorted.len() as i32;
    
    // Get current (most recent) snapshot
    let latest = sorted.last().unwrap();
    
    // Extract company-wide total from latest
    if let Some(total) = get_company_total(latest) {
        analysis.current_headcount = Some(total.total());
    }
    
    // Calculate growth rates
    calculate_growth_rates(&mut analysis, &sorted);
    
    // Calculate net hires
    calculate_net_hires(&mut analysis, &sorted);
    
    // Analyze all departments dynamically
    analyze_departments(&mut analysis, &sorted);
    
    // Determine trend
    determine_trend(&mut analysis);
    
    // Detect anomalies
    detect_anomalies(&mut analysis, &sorted);
    
    // Calculate churn
    calculate_churn(&mut analysis, &sorted);
    
    analysis
}

/// Get company-wide total (functions: null) from a snapshot
fn get_company_total(snapshot: &MonthlySnapshot) -> Option<&DepartmentMetrics> {
    snapshot.departments.iter().find(|d| d.functions.is_none())
}

/// Get department by name from a snapshot
fn get_department<'a>(snapshot: &'a MonthlySnapshot, name: &str) -> Option<&'a DepartmentMetrics> {
    snapshot.departments.iter().find(|d| {
        d.functions.as_ref().map(|f| f.as_str()) == Some(name)
    })
}

/// Calculate headcount growth rates over different periods
fn calculate_growth_rates(analysis: &mut EmployeeMetricsAnalysis, sorted: &[&MonthlySnapshot]) {
    let n = sorted.len();
    
    if let Some(latest_total) = get_company_total(sorted[n - 1]) {
        let latest_count = latest_total.total();
        
        // 30-day (1 month back)
        if n >= 2 {
            if let Some(prev_total) = get_company_total(sorted[n - 2]) {
                let prev_count = prev_total.total();
                if prev_count > 0 {
                    analysis.headcount_growth_rate_30d = 
                        Some((latest_count - prev_count) as f64 / prev_count as f64);
                }
            }
        }
        
        // 90-day (3 months back)
        if n >= 4 {
            if let Some(prev_total) = get_company_total(sorted[n - 4]) {
                let prev_count = prev_total.total();
                if prev_count > 0 {
                    analysis.headcount_growth_rate_90d = 
                        Some((latest_count - prev_count) as f64 / prev_count as f64);
                }
            }
        }
        
        // 180-day (6 months back)
        if n >= 7 {
            if let Some(prev_total) = get_company_total(sorted[n - 7]) {
                let prev_count = prev_total.total();
                if prev_count > 0 {
                    analysis.headcount_growth_rate_180d = 
                        Some((latest_count - prev_count) as f64 / prev_count as f64);
                }
            }
        }
    }
}

/// Calculate net hires (new - churned) over periods
fn calculate_net_hires(analysis: &mut EmployeeMetricsAnalysis, sorted: &[&MonthlySnapshot]) {
    let n = sorted.len();
    
    // Sum net hires over last N months
    let sum_net_hires = |months: usize| -> i32 {
        sorted.iter()
            .rev()
            .take(months.min(n))
            .filter_map(|s| get_company_total(s))
            .map(|t| t.net_change())
            .sum()
    };
    
    if n >= 1 {
        analysis.net_hires_30d = Some(sum_net_hires(1));
    }
    if n >= 3 {
        analysis.net_hires_90d = Some(sum_net_hires(3));
    }
    if n >= 6 {
        analysis.net_hires_180d = Some(sum_net_hires(6));
    }
}

/// Analyze all departments dynamically
fn analyze_departments(analysis: &mut EmployeeMetricsAnalysis, sorted: &[&MonthlySnapshot]) {
    let n = sorted.len();
    if n < 1 {
        return;
    }
    
    let latest = sorted[n - 1];
    
    // Collect all department names from latest snapshot
    let dept_names: Vec<String> = latest.departments
        .iter()
        .filter_map(|d| d.functions.clone())
        .collect();
    
    // For each department, compute summary and detect spikes/contractions
    for dept_name in &dept_names {
        // Current state
        let current = match get_department(latest, dept_name) {
            Some(d) => d,
            None => continue,
        };
        
        let current_headcount = current.total();
        
        // Build summary
        let mut summary = DepartmentSummary {
            current_headcount,
            ..Default::default()
        };
        
        // Sum metrics over last 3 months (90 days)
        for snapshot in sorted.iter().rev().take(3.min(n)) {
            if let Some(dept) = get_department(snapshot, dept_name) {
                summary.new_hires_90d += dept.new;
                summary.churned_90d += dept.churned;
            }
        }
        summary.net_change_90d = summary.new_hires_90d - summary.churned_90d;
        
        // Calculate growth rate over 90 days
        if n >= 4 {
            if let Some(prev) = get_department(sorted[n - 4], dept_name) {
                let prev_count = prev.total();
                if prev_count > 0 {
                    summary.growth_rate_90d = 
                        Some((current_headcount - prev_count) as f64 / prev_count as f64);
                    
                    let growth_pct = summary.growth_rate_90d.unwrap() * 100.0;
                    
                    // Detect spike (>15% growth)
                    if growth_pct > 15.0 {
                        analysis.department_spikes.push(DepartmentSpike {
                            department: dept_name.clone(),
                            growth_pct,
                            period: "90d".to_string(),
                            absolute_change: current_headcount - prev_count,
                            new_hires: summary.new_hires_90d,
                            current_headcount,
                        });
                    }
                    
                    // Detect contraction (>10% decline)
                    if growth_pct < -10.0 {
                        analysis.department_contractions.push(DepartmentContraction {
                            department: dept_name.clone(),
                            decline_pct: growth_pct.abs(),
                            period: "90d".to_string(),
                            churned: summary.churned_90d,
                            current_headcount,
                        });
                    }
                }
            }
        }
        
        analysis.department_summary.insert(dept_name.clone(), summary);
    }
    
    // Sort spikes by growth percentage (highest first)
    analysis.department_spikes.sort_by(|a, b| {
        b.growth_pct.partial_cmp(&a.growth_pct).unwrap_or(std::cmp::Ordering::Equal)
    });
    
    // Sort contractions by decline percentage (highest first)
    analysis.department_contractions.sort_by(|a, b| {
        b.decline_pct.partial_cmp(&a.decline_pct).unwrap_or(std::cmp::Ordering::Equal)
    });
}

/// Determine overall headcount trend
fn determine_trend(analysis: &mut EmployeeMetricsAnalysis) {
    if let Some(g90) = analysis.headcount_growth_rate_90d {
        if g90 > 0.15 {
            analysis.headcount_trend = "rapidly_growing".to_string();
            analysis.hiring_velocity = "aggressive".to_string();
        } else if g90 > 0.05 {
            analysis.headcount_trend = "growing".to_string();
            analysis.hiring_velocity = "moderate".to_string();
        } else if g90 < -0.1 {
            analysis.headcount_trend = "declining".to_string();
            analysis.contraction_signals = true;
            analysis.hiring_velocity = "contracting".to_string();
        } else if g90 < -0.02 {
            analysis.headcount_trend = "slightly_declining".to_string();
            analysis.hiring_velocity = "slow".to_string();
        } else {
            analysis.headcount_trend = "stable".to_string();
            analysis.hiring_velocity = "moderate".to_string();
        }
    }
}

/// Detect anomalies in hiring patterns
fn detect_anomalies(analysis: &mut EmployeeMetricsAnalysis, sorted: &[&MonthlySnapshot]) {
    // Rapid overall growth (>30% in 90 days)
    if let Some(g90) = analysis.headcount_growth_rate_90d {
        if g90 > 0.3 {
            analysis.anomaly_flags.push("rapid_headcount_surge".to_string());
        }
        if g90 < -0.2 {
            analysis.anomaly_flags.push("significant_contraction".to_string());
        }
    }
    
    // Department-specific anomalies
    for spike in &analysis.department_spikes {
        if spike.growth_pct > 50.0 {
            analysis.anomaly_flags.push(format!("explosive_{}_growth", spike.department));
        } else if spike.growth_pct > 30.0 {
            analysis.anomaly_flags.push(format!("rapid_{}_growth", spike.department));
        }
    }
    
    for contraction in &analysis.department_contractions {
        if contraction.decline_pct > 30.0 {
            analysis.anomaly_flags.push(format!("major_{}_contraction", contraction.department));
        }
    }
    
    // High month-over-month volatility
    if sorted.len() >= 3 {
        let recent_totals: Vec<i32> = sorted.iter()
            .rev()
            .take(3)
            .filter_map(|s| get_company_total(s).map(|t| t.total()))
            .collect();
        
        if recent_totals.len() == 3 {
            let avg = recent_totals.iter().sum::<i32>() / 3;
            if avg > 0 {
                let m1_change = (recent_totals[0] - recent_totals[1]).abs();
                let m2_change = (recent_totals[1] - recent_totals[2]).abs();
                let volatility = (m1_change + m2_change) as f64 / (2.0 * avg as f64);
                
                if volatility > 0.1 {
                    analysis.anomaly_flags.push("high_headcount_volatility".to_string());
                }
            }
        }
    }
}

/// Calculate churn metrics
fn calculate_churn(analysis: &mut EmployeeMetricsAnalysis, sorted: &[&MonthlySnapshot]) {
    let n = sorted.len();
    if n < 3 {
        return;
    }
    
    // Sum churned over last 3 months
    let total_churned: i32 = sorted.iter()
        .rev()
        .take(3)
        .filter_map(|s| get_company_total(s))
        .map(|t| t.churned)
        .sum();
    
    analysis.total_churned_90d = total_churned;
    
    // Calculate churn rate (churned / average headcount)
    if let Some(current) = analysis.current_headcount {
        if current > 0 {
            analysis.churn_rate_90d = Some(total_churned as f64 / current as f64);
        }
    }
}

// ----------------------------
// Database Operations
// ----------------------------

/// Load employee metrics from company_apollo_enrichment table.
pub async fn load_employee_metrics(
    pool: &PgPool,
    company_id: Uuid,
) -> Result<Vec<MonthlySnapshot>, sqlx::Error> {
    let row: Option<(JsonValue,)> = sqlx::query_as(
        r#"
        SELECT employee_metrics
        FROM company_apollo_enrichment
        WHERE company_id = $1
        "#,
    )
    .bind(company_id)
    .fetch_optional(pool)
    .await?;
    
    match row {
        Some((json_val,)) => {
            match serde_json::from_value::<Vec<MonthlySnapshot>>(json_val) {
                Ok(snapshots) => Ok(snapshots),
                Err(e) => {
                    warn!("Failed to parse employee_metrics for company {}: {}", company_id, e);
                    Ok(Vec::new())
                }
            }
        }
        None => Ok(Vec::new()),
    }
}

/// Save employee metrics analysis to company_apollo_enrichment table.
pub async fn save_employee_metrics_analysis(
    pool: &PgPool,
    company_id: Uuid,
    analysis: &EmployeeMetricsAnalysis,
) -> Result<(), sqlx::Error> {
    let analysis_json = serde_json::to_value(analysis)
        .map_err(|e| sqlx::Error::Protocol(format!("JSON serialization failed: {}", e)))?;
    
    sqlx::query(
        r#"
        UPDATE company_apollo_enrichment
        SET employee_metrics_analysis = $2,
            analyzed_at = NOW()
        WHERE company_id = $1
        "#,
    )
    .bind(company_id)
    .bind(analysis_json)
    .execute(pool)
    .await?;
    
    info!("Saved employee metrics analysis for company {}", company_id);
    
    Ok(())
}

// ----------------------------
// Job Handler
// ----------------------------

/// Payload for ANALYZE_EMPLOYEE_METRICS job.
#[derive(Debug, Clone, Deserialize)]
pub struct AnalyzeEmployeeMetricsPayload {
    pub company_id: Uuid,
    pub client_id: Uuid,
}

/// Handle ANALYZE_EMPLOYEE_METRICS job.
pub async fn handle_analyze_employee_metrics(
    pool: &PgPool,
    payload: &AnalyzeEmployeeMetricsPayload,
    worker_id: &str,
) -> Result<EmployeeMetricsAnalysis, sqlx::Error> {
    info!(
        "Worker {}: analyzing employee metrics for company {}",
        worker_id, payload.company_id
    );
    
    // Load raw metrics
    let snapshots = load_employee_metrics(pool, payload.company_id).await?;
    
    if snapshots.is_empty() {
        warn!(
            "Worker {}: no employee metrics found for company {}",
            worker_id, payload.company_id
        );
        return Ok(EmployeeMetricsAnalysis::default());
    }
    
    // Analyze
    let analysis = analyze_employee_metrics(&snapshots);
    
    // Save analysis
    save_employee_metrics_analysis(pool, payload.company_id, &analysis).await?;
    
    info!(
        "Worker {}: employee metrics analysis complete for company {} \
        (headcount={:?}, growth_90d={:?}, spikes={}, anomalies={})",
        worker_id,
        payload.company_id,
        analysis.current_headcount,
        analysis.headcount_growth_rate_90d,
        analysis.department_spikes.len(),
        analysis.anomaly_flags.len()
    );
    
    Ok(analysis)
}

// ----------------------------
// Tests
// ----------------------------

#[cfg(test)]
mod tests {
    use super::*;
    
    fn make_dept(name: Option<&str>, new: i32, retained: i32, churned: i32) -> DepartmentMetrics {
        DepartmentMetrics {
            functions: name.map(|s| s.to_string()),
            new,
            retained,
            churned,
        }
    }
    
    #[test]
    fn test_analyze_growth() {
        let snapshots = vec![
            MonthlySnapshot {
                start_date: "2024-01-01".to_string(),
                departments: vec![
                    make_dept(None, 5, 95, 2),
                    make_dept(Some("engineering"), 3, 45, 1),
                    make_dept(Some("sales"), 2, 50, 1),
                ],
            },
            MonthlySnapshot {
                start_date: "2024-02-01".to_string(),
                departments: vec![
                    make_dept(None, 8, 98, 3),
                    make_dept(Some("engineering"), 5, 47, 1),
                    make_dept(Some("sales"), 3, 51, 2),
                ],
            },
            MonthlySnapshot {
                start_date: "2024-03-01".to_string(),
                departments: vec![
                    make_dept(None, 10, 103, 2),
                    make_dept(Some("engineering"), 6, 51, 0),
                    make_dept(Some("sales"), 4, 52, 2),
                ],
            },
            MonthlySnapshot {
                start_date: "2024-04-01".to_string(),
                departments: vec![
                    make_dept(None, 12, 111, 3),
                    make_dept(Some("engineering"), 8, 57, 1),
                    make_dept(Some("sales"), 4, 54, 2),
                ],
            },
        ];
        
        let analysis = analyze_employee_metrics(&snapshots);
        
        assert_eq!(analysis.current_headcount, Some(123)); // 111 + 12
        assert!(analysis.headcount_growth_rate_90d.is_some());
        assert!(analysis.department_summary.contains_key("engineering"));
        assert!(analysis.department_summary.contains_key("sales"));
    }
    
    #[test]
    fn test_detect_contraction() {
        let snapshots = vec![
            MonthlySnapshot {
                start_date: "2024-01-01".to_string(),
                departments: vec![make_dept(None, 2, 100, 5)],
            },
            MonthlySnapshot {
                start_date: "2024-02-01".to_string(),
                departments: vec![make_dept(None, 1, 95, 6)],
            },
            MonthlySnapshot {
                start_date: "2024-03-01".to_string(),
                departments: vec![make_dept(None, 1, 88, 7)],
            },
            MonthlySnapshot {
                start_date: "2024-04-01".to_string(),
                departments: vec![make_dept(None, 0, 80, 8)],
            },
        ];
        
        let analysis = analyze_employee_metrics(&snapshots);
        
        assert!(analysis.contraction_signals);
        assert_eq!(analysis.headcount_trend, "declining");
    }
    
    #[test]
    fn test_department_spike() {
        let snapshots = vec![
            MonthlySnapshot {
                start_date: "2024-01-01".to_string(),
                departments: vec![
                    make_dept(None, 5, 100, 2),
                    make_dept(Some("engineering"), 2, 20, 1),
                ],
            },
            MonthlySnapshot {
                start_date: "2024-02-01".to_string(),
                departments: vec![
                    make_dept(None, 8, 103, 2),
                    make_dept(Some("engineering"), 5, 21, 0),
                ],
            },
            MonthlySnapshot {
                start_date: "2024-03-01".to_string(),
                departments: vec![
                    make_dept(None, 10, 109, 2),
                    make_dept(Some("engineering"), 6, 26, 0),
                ],
            },
            MonthlySnapshot {
                start_date: "2024-04-01".to_string(),
                departments: vec![
                    make_dept(None, 12, 117, 2),
                    make_dept(Some("engineering"), 8, 32, 0),  // 40 total, was 21 = 90% growth
                ],
            },
        ];
        
        let analysis = analyze_employee_metrics(&snapshots);
        
        assert!(!analysis.department_spikes.is_empty());
        assert_eq!(analysis.department_spikes[0].department, "engineering");
    }
}
