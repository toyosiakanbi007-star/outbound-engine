// src/jobs/prequal_listener.rs
//
// Postgres LISTEN/NOTIFY subscriber for prequal_ready notifications.
//
// PURPOSE:
// - Listen for prequal_ready notifications from Azure Function
// - Update company_candidates.status (belt-and-suspenders alongside DB trigger)
// - Check if company qualifies for Phase B
// - Queue PHASE_B_ENRICH_APOLLO job if qualified
//
// WHY BELT-AND-SUSPENDERS:
//   The DB trigger (trg_sync_candidate_status on v3_analysis_runs) is the primary
//   mechanism for updating company_candidates.status. This listener provides a
//   secondary path using the NOTIFY payload. If the trigger already ran, the
//   update here is a no-op (WHERE status IN ('prequal_queued','new') won't match).
//
// USAGE:
// - Run as part of worker: MODE=worker+prequal_listener
// - Or as separate process: MODE=prequal_listener
//
// ENV VARS:
// - DATABASE_URL: Postgres connection string
// - PREQUAL_LISTEN_CHANNEL: Channel name (default: "prequal_ready")

use sqlx::{postgres::PgListener, PgPool};
use tracing::{error, info, warn};

use crate::jobs::phase_b_controller::{
    handle_prequal_notification, PrequalReadyNotification,
};
use crate::jobs::prequal_worker::db as prequal_db;

/// Run the prequal listener loop.
///
/// This function runs indefinitely, listening for prequal_ready notifications,
/// updating candidate status, and queueing Phase B jobs for qualified companies.
pub async fn run_prequal_listener(pool: PgPool) -> anyhow::Result<()> {
    let channel = std::env::var("PREQUAL_LISTEN_CHANNEL")
        .unwrap_or_else(|_| "prequal_ready".to_string());
    
    let worker_id = std::env::var("WORKER_ID")
        .unwrap_or_else(|_| "prequal-listener".to_string());
    
    info!(
        "Starting prequal listener on channel '{}' (worker_id={})",
        channel, worker_id
    );
    
    let mut listener = PgListener::connect_with(&pool).await?;
    listener.listen(&channel).await?;
    
    info!("Subscribed to Postgres LISTEN {}", channel);
    
    loop {
        match listener.recv().await {
            Ok(notification) => {
                let payload_str = notification.payload();
                info!("Received prequal notification: {}", payload_str);
                
                match serde_json::from_str::<PrequalReadyNotification>(payload_str) {
                    Ok(prequal) => {
                        // -----------------------------------------------
                        // Step 1: Update company_candidates.status
                        // (belt-and-suspenders: DB trigger is the primary path)
                        // -----------------------------------------------
                        match prequal_db::update_candidate_status_by_company(
                            &pool,
                            prequal.company_id,
                            prequal.client_id,
                            prequal.qualifies,
                            Some(prequal.score),
                        )
                        .await
                        {
                            Ok(affected) => {
                                if affected > 0 {
                                    info!(
                                        "Updated candidate status via NOTIFY for company {} (qualifies={}, score={:.2})",
                                        prequal.company_id, prequal.qualifies, prequal.score
                                    );
                                } else {
                                    // DB trigger likely already handled this — expected and fine
                                    info!(
                                        "Candidate already updated (trigger handled it) for company {} (qualifies={})",
                                        prequal.company_id, prequal.qualifies
                                    );
                                }
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to update candidate status for company {}: {:?} (DB trigger may handle it)",
                                    prequal.company_id, e
                                );
                            }
                        }

                        // -----------------------------------------------
                        // Step 2: Queue Phase B if qualified
                        // -----------------------------------------------
                        match handle_prequal_notification(&pool, &prequal, &worker_id).await {
                            Ok(queued) => {
                                if queued {
                                    info!(
                                        "Queued Phase B job for company {} (score={:.2})",
                                        prequal.company_id, prequal.score
                                    );
                                } else {
                                    info!(
                                        "Company {} did not qualify for Phase B (score={:.2})",
                                        prequal.company_id, prequal.score
                                    );
                                }
                            }
                            Err(e) => {
                                error!(
                                    "Failed to handle prequal notification for company {}: {:?}",
                                    prequal.company_id, e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to parse prequal notification: {:?} (payload: {})", e, payload_str);
                    }
                }
            }
            Err(e) => {
                error!("Error receiving prequal notification: {:?}", e);
                // Sleep briefly before retrying to avoid tight loop on persistent errors
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    }
}
