// src/jobs/prequal_listener.rs
//
// Postgres LISTEN/NOTIFY subscriber for prequal_ready notifications.
//
// PURPOSE:
// - Listen for prequal_ready notifications from Azure Function
// - Check if company qualifies for Phase B
// - Queue PHASE_B_ENRICH_APOLLO job if qualified
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

/// Run the prequal listener loop.
///
/// This function runs indefinitely, listening for prequal_ready notifications
/// and queueing Phase B jobs for qualified companies.
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
