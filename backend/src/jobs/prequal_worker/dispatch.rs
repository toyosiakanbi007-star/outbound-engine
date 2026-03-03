// src/jobs/prequal_worker/dispatch.rs
//
// PREQUAL_DISPATCH Job Handler.
//
// PURPOSE:
//   Planner job that selects eligible company_candidates, claims them
//   (status → 'prequal_queued'), splits into batches, and enqueues
//   PREQUAL_BATCH jobs for the worker pool.
//
// FLOW:
//   1. Load prequal config for client
//   2. Compute effective batch_size and max_batches
//   3. SELECT eligible candidates (FOR UPDATE SKIP LOCKED)
//   4. Claim all selected rows (status='prequal_queued')
//   5. Split into chunks of batch_size
//   6. Enqueue one PREQUAL_BATCH per chunk
//   7. Return summary
//
// CONCURRENCY:
//   The FOR UPDATE SKIP LOCKED pattern ensures two concurrent dispatchers
//   for the same client won't grab the same candidates.

use sqlx::PgPool;
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::db;
use super::models::PrequalDispatchPayload;

/// Result of a dispatch operation.
#[derive(Debug)]
pub struct DispatchResult {
    pub candidates_selected: i32,
    pub candidates_claimed: i32,
    pub batches_created: i32,
    pub batch_job_ids: Vec<Uuid>,
}

/// Handle a PREQUAL_DISPATCH job.
///
/// Selects eligible candidates, claims them, and enqueues batch jobs.
pub async fn handle_prequal_dispatch(
    pool: &PgPool,
    payload: &PrequalDispatchPayload,
    worker_id: &str,
) -> Result<DispatchResult, Box<dyn std::error::Error + Send + Sync>> {
    let client_id = payload.client_id;

    info!(
        "Worker {}: starting PREQUAL_DISPATCH for client {} (source={}, force={})",
        worker_id, client_id, payload.source, payload.force
    );

    // 1. Load config
    let config = db::load_prequal_config(pool, client_id).await?;
    let batch_size = config.effective_batch_size(payload.batch_size);
    let max_batches = config.effective_max_batches(payload.max_batches);
    let max_candidates = config.max_candidates_per_dispatch(payload);

    info!(
        "Worker {}: dispatch config — batch_size={}, max_batches={}, max_candidates={}, max_attempts={}",
        worker_id, batch_size, max_batches, max_candidates, config.max_attempts_per_company
    );

    // 2. Select eligible candidates
    let eligible = db::select_eligible_candidates(
        pool,
        client_id,
        max_candidates,
        config.max_attempts_per_company,
        payload.force,
        &payload.filters,
    )
    .await?;

    let selected_count = eligible.len() as i32;

    if selected_count == 0 {
        info!(
            "Worker {}: no eligible candidates for client {} — dispatch complete (nothing to do)",
            worker_id, client_id
        );
        return Ok(DispatchResult {
            candidates_selected: 0,
            candidates_claimed: 0,
            batches_created: 0,
            batch_job_ids: vec![],
        });
    }

    info!(
        "Worker {}: found {} eligible candidates for client {}",
        worker_id, selected_count, client_id
    );

    // 3. Claim all selected candidates (status → 'prequal_queued')
    let candidate_ids: Vec<Uuid> = eligible.iter().map(|c| c.id).collect();
    let claimed = db::claim_candidates(pool, &candidate_ids).await?;

    debug!(
        "Worker {}: claimed {}/{} candidates",
        worker_id, claimed, selected_count
    );

    // 4. Split into batches
    let chunks: Vec<Vec<Uuid>> = candidate_ids
        .chunks(batch_size as usize)
        .take(max_batches as usize)
        .map(|chunk| chunk.to_vec())
        .collect();

    let batch_count = chunks.len() as i32;

    info!(
        "Worker {}: creating {} batches (batch_size={}, {} candidates total)",
        worker_id, batch_count, batch_size, candidate_ids.len()
    );

    // 5. Enqueue PREQUAL_BATCH jobs
    let mut batch_job_ids = Vec::with_capacity(chunks.len());

    for (i, chunk) in chunks.into_iter().enumerate() {
        let batch_id = Uuid::new_v4();

        match db::enqueue_prequal_batch_job(
            pool,
            client_id,
            &chunk,
            batch_id,
            &payload.source,
        )
        .await
        {
            Ok(job_id) => {
                debug!(
                    "Worker {}: batch {}/{} → job {} ({} candidates, batch_id={})",
                    worker_id,
                    i + 1,
                    batch_count,
                    job_id,
                    chunk.len(),
                    batch_id
                );
                batch_job_ids.push(job_id);
            }
            Err(e) => {
                warn!(
                    "Worker {}: failed to enqueue batch {}/{} for client {}: {}",
                    worker_id,
                    i + 1,
                    batch_count,
                    client_id,
                    e
                );
                // Continue with remaining batches — partial dispatch is better than none
            }
        }
    }

    let result = DispatchResult {
        candidates_selected: selected_count,
        candidates_claimed: claimed as i32,
        batches_created: batch_job_ids.len() as i32,
        batch_job_ids,
    };

    info!(
        "Worker {}: PREQUAL_DISPATCH complete for client {} — selected={}, claimed={}, batches={}",
        worker_id, client_id, result.candidates_selected, result.candidates_claimed, result.batches_created
    );

    Ok(result)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    // Integration tests would require a test DB.
    // Unit tests for chunking logic:

    #[test]
    fn test_chunking() {
        let ids: Vec<u32> = (0..13).collect();
        let batch_size = 5;
        let max_batches = 10;

        let chunks: Vec<Vec<u32>> = ids
            .chunks(batch_size)
            .take(max_batches)
            .map(|c| c.to_vec())
            .collect();

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), 5);
        assert_eq!(chunks[1].len(), 5);
        assert_eq!(chunks[2].len(), 3);
    }

    #[test]
    fn test_chunking_respects_max_batches() {
        let ids: Vec<u32> = (0..100).collect();
        let batch_size = 5;
        let max_batches = 3;

        let chunks: Vec<Vec<u32>> = ids
            .chunks(batch_size)
            .take(max_batches)
            .map(|c| c.to_vec())
            .collect();

        // Only 3 batches even though there are 20 possible
        assert_eq!(chunks.len(), 3);
        // Total candidates processed = 15 (not 100)
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total, 15);
    }
}
