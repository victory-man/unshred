mod drift_handler;
mod storage;
mod types;

use std::{sync::Arc, time::Duration};

use crate::{
    drift_handler::DriftHandler,
    storage::{Storage, StorageConfig},
    types::DriftEvent,
};
use anyhow::Result;
use prometheus::{register_int_gauge_vec_with_registry, IntGaugeVec, Registry};
use tokio::{signal, time::interval};
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use unshred::UnshredProcessor;
use warp::Filter;

#[tokio::main]
async fn main() -> Result<()> {
    // Logging
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer().json())
        .init();

    info!("Starting Drift monitor...");

    // Storage
    let storage_config = StorageConfig {
        clickhouse_url: std::env::var("CLICKHOUSE_URL").unwrap(),
        clickhouse_db: std::env::var("CLICKHOUSE_DB").unwrap(),
        clickhouse_user: std::env::var("CLICKHOUSE_USER").unwrap(),
        clickhouse_password: std::env::var("CLICKHOUSE_PASSWORD").unwrap(),
    };

    let storage = Arc::new(Storage::new(&storage_config)?);
    let (event_tx, event_rx) = tokio::sync::mpsc::channel::<DriftEvent>(10000);

    // Start metrics server
    let registry = Arc::new(Registry::new());
    let metrics_port: u16 = std::env::var("METRICS_PORT").unwrap().parse().unwrap();
    let metrics_registry = registry.clone();
    let metrics_server_handle =
        tokio::spawn(async move { serve_metrics(metrics_registry, metrics_port).await });

    let system_cpu_usage = register_int_gauge_vec_with_registry!(
        "system_cpu_usage_percent",
        "System CPU usage percentage",
        &["cpu"],
        registry.clone()
    )?;

    let system_memory_usage = register_int_gauge_vec_with_registry!(
        "system_memory_bytes",
        "System memory measurements in bytes",
        &["type"],
        registry.clone()
    )?;

    let cpu_metric = system_cpu_usage.clone();
    let memory_metric = system_memory_usage.clone();
    let system_metrics_handle = tokio::spawn(async move {
        let mut system = sysinfo::System::new();
        let mut interval = interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            if let Err(e) = update_system_metrics(&mut system, &cpu_metric, &memory_metric) {
                tracing::error!("Failed to update system metrics: {}", e);
            }
        }
    });

    // Drift handler
    let drift_handler = DriftHandler::new(
        std::env::var("PROCESSOR_DRIFT_PROGRAM_ID").unwrap(),
        event_tx.clone(),
        &registry,
    )?;

    // Start Clickhouse event writer
    let storage_writer = storage.clone();
    let event_writer_handle =
        tokio::spawn(async move { storage_writer.clickhouse.run_event_writer(event_rx).await });

    // Start ClickHouse cleanup task
    let cleanup_handle = tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(300));
        loop {
            interval.tick().await;
            if let Err(e) = storage
                .clickhouse
                .cleanup_slots_by_size(
                    std::env::var("PROCESSOR_MAX_TABLE_SIZE_GB")
                        .unwrap()
                        .parse()
                        .unwrap(),
                )
                .await
            {
                error!("ClickHouse cleanup failed: {}", e);
            }
        }
    });

    // Start unshred processor
    let processor = UnshredProcessor::builder()
        .handler(drift_handler)
        .bind_address(std::env::var("RECEIVER_BIND_ADDRESS").unwrap())
        .metrics_registry(registry)
        .build()?;

    let processor_handle = tokio::spawn(async move { processor.run().await });

    // Wait for shutdown
    shutdown_signal().await;
    processor_handle.abort();
    system_metrics_handle.abort();
    cleanup_handle.abort();
    metrics_server_handle.abort();
    drop(event_tx);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), event_writer_handle).await;

    Ok(())
}

async fn serve_metrics(registry: Arc<Registry>, port: u16) {
    let metrics_route = warp::path("metrics").and(warp::get()).map(move || {
        let encoder = prometheus::TextEncoder::new();
        let metric_families = registry.gather();
        encoder.encode_to_string(&metric_families).unwrap()
    });

    info!("Starting metrics server on port {}", port);
    warp::serve(metrics_route).run(([0, 0, 0, 0], port)).await;
}

pub fn update_system_metrics(
    system: &mut sysinfo::System,
    cpu_metric: &IntGaugeVec,
    memory_metric: &IntGaugeVec,
) -> Result<()> {
    system.refresh_memory();
    system.refresh_cpu_usage();

    // Memory
    memory_metric
        .with_label_values(&["used"])
        .set(system.used_memory() as i64);

    memory_metric
        .with_label_values(&["available"])
        .set(system.available_memory() as i64);

    // CPU
    let cpus = system.cpus();
    for (i, cpu) in cpus.iter().enumerate() {
        let usage = cpu.cpu_usage();

        cpu_metric
            .with_label_values(&[&format!("{}", i)])
            .set((usage) as i64);
    }

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to set up Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to set up signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
        },
        _ = terminate => {
        },
    }
}
