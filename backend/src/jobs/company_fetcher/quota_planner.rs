// src/jobs/company_fetcher/quota_planner.rs
//
// Quota Planner for the Company Fetcher.
//
// RESPONSIBILITIES:
// - Compute per-industry quotas for a fetch run
// - Two-phase allocation:
//   1. Cold start (no history): even split across ICP industries
//   2. Adaptive (after history): exploration floor + yield-weighted allocation
// - Respect min/max per-industry bounds from job payload
// - Output a QuotaPlan that the orchestrator iterates over
//
// ALGORITHM (Adaptive):
//   Given batch_target=2000, exploration_pct=0.20, 5 industries:
//
//   exploration_budget = 2000 * 0.20 = 400
//   yield_budget       = 2000 * 0.80 = 1600
//
//   Exploration: 400 / 5 = 80 per industry (even)
//
//   Yield allocation: weighted by composite_score per industry
//     composite_score = 0.6 * tier_ab_rate + 0.3 * prequal_pass_rate + 0.1 * (1 - dupe_rate)
//     weight_i = composite_score_i / sum(composite_scores)
//     yield_share_i = yield_budget * weight_i
//
//   Final: target_i = clamp(exploration_share + yield_share, min, max)
//   If total > batch_target after clamping: scale down proportionally
//   If total < batch_target after clamping: distribute remainder to uncapped industries
//
// USAGE:
//   let plan = compute_quota_plan(
//       &icp_profile,
//       &yield_metrics,  // empty vec on cold start
//       &payload,
//   );

use std::collections::HashMap;
use tracing::{debug, info};

use super::models::{
    DiscoverCompaniesPayload, IcpFetchProfile, IndustryQuota, IndustryYieldMetric, QuotaPlan,
};

// ============================================================================
// Public API
// ============================================================================

/// Compute the per-industry quota plan for a fetch run.
///
/// # Arguments
/// * `icp` — Parsed ICP profile (must have non-empty `industries`).
/// * `yield_metrics` — Historical yield metrics per industry (empty = cold start).
/// * `payload` — Job payload with budget and constraint knobs.
///
/// # Returns
/// A `QuotaPlan` mapping industry name → `IndustryQuota`.
pub fn compute_quota_plan(
    icp: &IcpFetchProfile,
    yield_metrics: &[IndustryYieldMetric],
    payload: &DiscoverCompaniesPayload,
) -> QuotaPlan {
    let industries = icp.industry_names();
    assert!(!industries.is_empty(), "ICP must have at least one industry");

    let n = industries.len();
    let batch_target = payload.batch_target;
    let min_per = payload.min_per_industry;
    let max_per = payload.max_per_industry;

    // Build yield lookup: industry → metrics
    let yield_map: HashMap<&str, &IndustryYieldMetric> = yield_metrics
        .iter()
        .map(|m| (m.industry.as_str(), m))
        .collect();

    // Determine if we have enough history for adaptive allocation.
    // "Enough" = at least 2 industries have run_count > 0 and some prequal data.
    let industries_with_history = industries
        .iter()
        .filter(|ind| {
            yield_map
                .get(ind.as_str())
                .map(|m| m.run_count > 0 && m.total_upserted > 0)
                .unwrap_or(false)
        })
        .count();

    let use_adaptive = industries_with_history >= 2;

    let plan = if use_adaptive {
        info!(
            "Using adaptive quota allocation ({}/{} industries have history)",
            industries_with_history, n
        );
        compute_adaptive(&industries, &yield_map, batch_target, payload.exploration_pct, min_per, max_per)
    } else {
        info!(
            "Using cold-start even quota allocation ({} industries, batch_target={})",
            n, batch_target
        );
        compute_cold_start(&industries, batch_target, min_per, max_per)
    };

    log_plan(&plan, batch_target);
    plan
}

// ============================================================================
// Cold Start: Even Split
// ============================================================================

/// Even distribution across all industries, clamped to [min, max].
fn compute_cold_start(
    industries: &[String],
    batch_target: i32,
    min_per: i32,
    max_per: i32,
) -> QuotaPlan {
    let n = industries.len() as i32;
    let even_share = batch_target / n;

    let mut plan = QuotaPlan::new();
    let mut total_allocated = 0;

    for industry in industries {
        let target = even_share.max(min_per).min(max_per);
        plan.insert(
            industry.clone(),
            IndustryQuota {
                target,
                exploration_share: target,
                yield_share: 0,
            },
        );
        total_allocated += target;
    }

    // Distribute any remainder from rounding to industries that haven't hit max
    distribute_remainder(&mut plan, batch_target, total_allocated, max_per);

    plan
}

// ============================================================================
// Adaptive: Exploration Floor + Yield-Weighted
// ============================================================================

/// Two-tier allocation:
/// 1. Exploration budget split evenly (guaranteed floor)
/// 2. Yield budget allocated proportional to composite_score
fn compute_adaptive(
    industries: &[String],
    yield_map: &HashMap<&str, &IndustryYieldMetric>,
    batch_target: i32,
    exploration_pct: f64,
    min_per: i32,
    max_per: i32,
) -> QuotaPlan {
    let n = industries.len() as i32;

    // Split budget into exploration and yield pools
    let exploration_budget = ((batch_target as f64) * exploration_pct).round() as i32;
    let yield_budget = batch_target - exploration_budget;

    // Exploration: even across all industries
    let exploration_per = (exploration_budget / n).max(1);

    // Compute composite scores for yield allocation
    let scores: Vec<(String, f64)> = industries
        .iter()
        .map(|ind| {
            let score = yield_map
                .get(ind.as_str())
                .map(|m| composite_score(m))
                .unwrap_or(0.0);
            (ind.clone(), score)
        })
        .collect();

    let total_score: f64 = scores.iter().map(|(_, s)| s).sum();

    // Allocate yield budget proportionally
    let mut plan = QuotaPlan::new();
    let mut total_allocated = 0;

    for (industry, score) in &scores {
        let yield_share = if total_score > 0.0 {
            ((yield_budget as f64) * (score / total_score)).round() as i32
        } else {
            // All scores zero (e.g. all new industries) → even split of yield too
            yield_budget / n
        };

        let raw_target = exploration_per + yield_share;
        let target = raw_target.max(min_per).min(max_per);

        debug!(
            "Industry '{}': score={:.3}, exploration={}, yield_share={}, raw={}, clamped={}",
            industry, score, exploration_per, yield_share, raw_target, target
        );

        plan.insert(
            industry.clone(),
            IndustryQuota {
                target,
                exploration_share: exploration_per,
                yield_share,
            },
        );
        total_allocated += target;
    }

    // Redistribute remainder or trim excess
    distribute_remainder(&mut plan, batch_target, total_allocated, max_per);

    plan
}

/// Composite score for an industry based on historical yield.
///
/// Weights:
///   0.6 × tier_ab_rate      (how many become Tier A/B)
///   0.3 × prequal_pass_rate (how many pass prequal at all)
///   0.1 × freshness_rate    (1 - duplicate_rate: how many are new vs already seen)
///
/// Returns a score in [0.0, 1.0].
fn composite_score(m: &IndustryYieldMetric) -> f64 {
    let tier_ab = m.tier_ab_rate.clamp(0.0, 1.0);
    let prequal = m.prequal_pass_rate.clamp(0.0, 1.0);

    let dupe_rate = if m.total_fetched > 0 {
        m.total_duplicates as f64 / m.total_fetched as f64
    } else {
        0.0
    };
    let freshness = (1.0 - dupe_rate).clamp(0.0, 1.0);

    // Penalize depleted industries slightly (they have less headroom)
    let depletion_penalty = if m.depletion_count > 2 { 0.85 } else { 1.0 };

    let raw = 0.6 * tier_ab + 0.3 * prequal + 0.1 * freshness;
    (raw * depletion_penalty).clamp(0.0, 1.0)
}

// ============================================================================
// Remainder Distribution
// ============================================================================

/// After initial allocation + clamping, total may not equal batch_target.
/// - If under: distribute surplus to industries that haven't hit max.
/// - If over: trim from the highest-allocated industries.
fn distribute_remainder(
    plan: &mut QuotaPlan,
    batch_target: i32,
    current_total: i32,
    max_per: i32,
) {
    let diff = batch_target - current_total;

    if diff == 0 {
        return;
    }

    if diff > 0 {
        // Under-allocated: give more to uncapped industries
        let mut eligible: Vec<String> = plan
            .iter()
            .filter(|(_, q)| q.target < max_per)
            .map(|(k, _)| k.clone())
            .collect();

        if eligible.is_empty() {
            return; // All at max, nothing to do
        }

        // Sort by current target ascending (give more to the smallest first)
        eligible.sort_by_key(|k| plan[k].target);

        let mut remaining = diff;
        for industry in eligible.iter().cycle() {
            if remaining <= 0 {
                break;
            }
            let quota = plan.get_mut(industry).unwrap();
            if quota.target < max_per {
                quota.target += 1;
                quota.yield_share += 1;
                remaining -= 1;
            }
            // Safety: if we've cycled through all and none can take more, break
            if plan.values().all(|q| q.target >= max_per) {
                break;
            }
        }
    } else {
        // Over-allocated: trim from highest-allocated industries
        let mut by_target: Vec<String> = plan.keys().cloned().collect();
        by_target.sort_by(|a, b| plan[b].target.cmp(&plan[a].target));

        let mut excess = -diff;
        for industry in by_target.iter().cycle() {
            if excess <= 0 {
                break;
            }
            let quota = plan.get_mut(industry).unwrap();
            let min = 1; // Never go below 1
            if quota.target > min {
                quota.target -= 1;
                if quota.yield_share > 0 {
                    quota.yield_share -= 1;
                } else {
                    quota.exploration_share -= 1;
                }
                excess -= 1;
            }
        }
    }
}

// ============================================================================
// Logging
// ============================================================================

fn log_plan(plan: &QuotaPlan, batch_target: i32) {
    let total: i32 = plan.values().map(|q| q.target).sum();

    let mut sorted: Vec<_> = plan.iter().collect();
    sorted.sort_by(|a, b| b.1.target.cmp(&a.1.target));

    info!(
        "Quota plan: {} industries, batch_target={}, total_allocated={}",
        plan.len(),
        batch_target,
        total
    );
    for (industry, quota) in &sorted {
        debug!(
            "  {} → target={} (explore={}, yield={})",
            industry, quota.target, quota.exploration_share, quota.yield_share
        );
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn make_payload(batch_target: i32, exploration_pct: f64) -> DiscoverCompaniesPayload {
        DiscoverCompaniesPayload {
            client_id: Uuid::new_v4(),
            batch_target,
            page_size: 100,
            exploration_pct,
            min_per_industry: 50,
            max_per_industry: 600,
            max_pages_per_query: 50,
            max_runtime_seconds: 3600,
            enqueue_prequal_jobs: true,
            resume_run_id: None,
        }
    }

    fn make_icp(industries: Vec<&str>) -> IcpFetchProfile {
        use super::super::models::IndustrySpec;
        IcpFetchProfile {
            industries: industries.into_iter().map(IndustrySpec::from_category_string).collect(),
            company_sizes: vec!["51-200".into()],
            locations: vec!["United States".into()],
            excluded_locations: vec![],
            keywords: vec![],
            revenue_min: None,
            revenue_max: None,
            v2_locations: vec![],
            v3_sizes: vec![],
        }
    }

    fn make_yield_metric(
        industry: &str,
        run_count: i32,
        total_upserted: i32,
        prequal_pass_rate: f64,
        tier_ab_rate: f64,
    ) -> IndustryYieldMetric {
        IndustryYieldMetric {
            id: Uuid::new_v4(),
            client_id: Uuid::new_v4(),
            industry: industry.to_string(),
            total_fetched: (total_upserted as f64 * 1.2) as i32,
            total_upserted,
            total_duplicates: (total_upserted as f64 * 0.2) as i32,
            prequal_passed: (total_upserted as f64 * prequal_pass_rate) as i32,
            prequal_failed: (total_upserted as f64 * (1.0 - prequal_pass_rate)) as i32,
            tier_a_count: (total_upserted as f64 * tier_ab_rate * 0.4) as i32,
            tier_b_count: (total_upserted as f64 * tier_ab_rate * 0.6) as i32,
            tier_c_count: 0,
            tier_d_count: 0,
            prequal_pass_rate,
            tier_ab_rate,
            avg_aggregate_score: 0.65,
            last_depleted_at: None,
            depletion_count: 0,
            run_count,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    // ---- Cold Start Tests ----

    #[test]
    fn test_cold_start_even_split() {
        let icp = make_icp(vec!["fintech", "healthcare", "cybersecurity", "edtech"]);
        let payload = make_payload(2000, 0.20);

        let plan = compute_quota_plan(&icp, &[], &payload);

        assert_eq!(plan.len(), 4);
        let total: i32 = plan.values().map(|q| q.target).sum();
        assert_eq!(total, 2000);

        // Each should be 500 (2000 / 4)
        for q in plan.values() {
            assert_eq!(q.target, 500);
            assert_eq!(q.yield_share, 0); // cold start → no yield allocation
        }
    }

    #[test]
    fn test_cold_start_respects_min() {
        // 1 industry, batch_target=10 but min=50
        let icp = make_icp(vec!["fintech"]);
        let payload = DiscoverCompaniesPayload {
            min_per_industry: 50,
            ..make_payload(10, 0.20)
        };

        let plan = compute_quota_plan(&icp, &[], &payload);
        // Should be clamped up to min=50
        assert_eq!(plan["fintech"].target, 50);
    }

    #[test]
    fn test_cold_start_respects_max() {
        // 2 industries, batch=2000, max=400 → each capped at 400
        let icp = make_icp(vec!["fintech", "healthcare"]);
        let payload = DiscoverCompaniesPayload {
            max_per_industry: 400,
            ..make_payload(2000, 0.20)
        };

        let plan = compute_quota_plan(&icp, &[], &payload);
        for q in plan.values() {
            assert!(q.target <= 400);
        }
    }

    // ---- Adaptive Tests ----

    #[test]
    fn test_adaptive_favors_high_yield() {
        let icp = make_icp(vec!["fintech", "healthcare", "cybersecurity"]);
        let payload = make_payload(1500, 0.20);

        let metrics = vec![
            make_yield_metric("fintech", 3, 400, 0.40, 0.60),      // great yield
            make_yield_metric("healthcare", 3, 400, 0.20, 0.20),   // mediocre
            make_yield_metric("cybersecurity", 3, 400, 0.10, 0.05), // poor
        ];

        let plan = compute_quota_plan(&icp, &metrics, &payload);

        assert_eq!(plan.len(), 3);
        let total: i32 = plan.values().map(|q| q.target).sum();
        assert_eq!(total, 1500);

        // Fintech should get the largest allocation
        assert!(
            plan["fintech"].target > plan["healthcare"].target,
            "fintech ({}) should beat healthcare ({})",
            plan["fintech"].target,
            plan["healthcare"].target
        );
        assert!(
            plan["healthcare"].target > plan["cybersecurity"].target
                || plan["healthcare"].target == plan["cybersecurity"].target,
            "healthcare ({}) should be >= cybersecurity ({})",
            plan["healthcare"].target,
            plan["cybersecurity"].target
        );
    }

    #[test]
    fn test_adaptive_still_gives_exploration_floor() {
        // Even a zero-yield industry gets the exploration share
        let icp = make_icp(vec!["fintech", "deadzone"]);
        let payload = make_payload(1000, 0.30); // 30% exploration

        let metrics = vec![
            make_yield_metric("fintech", 3, 500, 0.50, 0.40),
            make_yield_metric("deadzone", 3, 100, 0.0, 0.0), // zero yield
        ];

        let plan = compute_quota_plan(&icp, &metrics, &payload);

        // deadzone should still get at least exploration_budget/2 = 150
        // (possibly clamped to min_per_industry=50)
        assert!(
            plan["deadzone"].target >= 50,
            "deadzone should get at least min_per_industry, got {}",
            plan["deadzone"].target
        );
        assert!(
            plan["deadzone"].exploration_share > 0,
            "deadzone should have exploration share"
        );
    }

    #[test]
    fn test_falls_back_to_cold_start_without_enough_history() {
        // Only 1 industry has history → not enough for adaptive
        let icp = make_icp(vec!["fintech", "healthcare", "cybersecurity"]);
        let payload = make_payload(900, 0.20);

        let metrics = vec![
            make_yield_metric("fintech", 3, 400, 0.40, 0.60),
            // healthcare and cybersecurity have no history
        ];

        let plan = compute_quota_plan(&icp, &metrics, &payload);

        // Cold start → all exploration_share, zero yield_share
        for q in plan.values() {
            assert_eq!(q.yield_share, 0);
        }
        let total: i32 = plan.values().map(|q| q.target).sum();
        assert_eq!(total, 900);
    }

    // ---- Composite Score Tests ----

    #[test]
    fn test_composite_score_perfect() {
        let m = make_yield_metric("x", 5, 1000, 1.0, 1.0);
        let score = composite_score(&m);
        // 0.6*1.0 + 0.3*1.0 + 0.1*(1-0.2/1.2) ≈ 0.98
        assert!(score > 0.9);
    }

    #[test]
    fn test_composite_score_zero() {
        let m = make_yield_metric("x", 5, 1000, 0.0, 0.0);
        let score = composite_score(&m);
        // Only freshness component: 0.1 * (1-dupe_rate)
        assert!(score < 0.15);
        assert!(score > 0.0);
    }

    #[test]
    fn test_composite_score_depletion_penalty() {
        let mut m = make_yield_metric("x", 5, 500, 0.50, 0.40);
        let score_normal = composite_score(&m);

        m.depletion_count = 5; // Heavily depleted
        let score_depleted = composite_score(&m);

        assert!(
            score_depleted < score_normal,
            "depleted ({}) should be less than normal ({})",
            score_depleted,
            score_normal
        );
    }

    // ---- Remainder Distribution Tests ----

    #[test]
    fn test_distribute_remainder_adds() {
        let mut plan = QuotaPlan::new();
        plan.insert("a".into(), IndustryQuota { target: 100, exploration_share: 100, yield_share: 0 });
        plan.insert("b".into(), IndustryQuota { target: 100, exploration_share: 100, yield_share: 0 });

        distribute_remainder(&mut plan, 250, 200, 600);

        let total: i32 = plan.values().map(|q| q.target).sum();
        assert_eq!(total, 250);
    }

    #[test]
    fn test_distribute_remainder_trims() {
        let mut plan = QuotaPlan::new();
        plan.insert("a".into(), IndustryQuota { target: 400, exploration_share: 100, yield_share: 300 });
        plan.insert("b".into(), IndustryQuota { target: 400, exploration_share: 100, yield_share: 300 });

        distribute_remainder(&mut plan, 600, 800, 600);

        let total: i32 = plan.values().map(|q| q.target).sum();
        assert_eq!(total, 600);
    }
}
