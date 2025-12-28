use anyhow::Result;
#[cfg(feature = "metrics")]
use prometheus::{
    register_histogram_vec_with_registry, register_int_counter_vec_with_registry,
    register_int_gauge_vec_with_registry, HistogramVec, IntCounterVec, IntGaugeVec, Registry,
};
use std::sync::Arc;

#[cfg(feature = "metrics")]
pub struct Metrics {
    pub channel_capacity_utilization: IntGaugeVec,

    pub receiver_shreds_received: IntCounterVec,
    pub receiver_socket_buffer_utilization: IntGaugeVec,

    pub processor_shreds_accumulated: IntCounterVec,
    pub processor_fec_sets_completed: IntCounterVec,
    pub processor_transactions_processed: IntCounterVec,

    pub processing_latency: HistogramVec,

    pub active_slots: IntGaugeVec,

    pub slot_accumulators_count: IntGaugeVec,
    pub processed_slots_count: IntGaugeVec,

    pub errors: IntCounterVec,
}

#[cfg(feature = "metrics")]
static METRICS: std::sync::OnceLock<Option<Arc<Metrics>>> = std::sync::OnceLock::new();

#[cfg(feature = "metrics")]
impl Metrics {
    pub fn new(registry: Arc<Registry>) -> Result<Self> {
        let metrics = Metrics {
            channel_capacity_utilization: register_int_gauge_vec_with_registry!(
                "channel_capacity_utilization_percentage",
                "Channel utilization percentage",
                &["type"],
                registry.clone()
            )?,

            receiver_shreds_received: register_int_counter_vec_with_registry!(
                "receiver_shreds_received_total",
                "Total shreds received",
                &["type"],
                registry.clone()
            )?,
            receiver_socket_buffer_utilization: register_int_gauge_vec_with_registry!(
                "receiver_socket_buffer_utilization_percentage",
                "Socket buffer utilization percentage",
                &["type"],
                registry.clone()
            )?,

            processor_shreds_accumulated: register_int_counter_vec_with_registry!(
                "processor_shreds_accumulated_total",
                "Total shreds accumulated by type and slot",
                &["type"],
                registry.clone()
            )?,
            processor_transactions_processed: register_int_counter_vec_with_registry!(
                "processor_transactions_processed_total",
                "Total transactions processed by type",
                &["type"],
                registry.clone()
            )?,
            processor_fec_sets_completed: register_int_counter_vec_with_registry!(
                "processor_fec_sets_completed_total",
                "Total FEC sets completed by completion method",
                &["method"], // natural, recovery
                registry.clone()
            )?,

            processing_latency: register_histogram_vec_with_registry!(
                "processing_latency_seconds",
                "Time from shred received to transaction processed",
                &["stage"],
                vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
                registry.clone()
            )?,

            active_slots: register_int_gauge_vec_with_registry!(
                "active_slots",
                "Number of active slots being processed",
                &["component"],
                registry.clone()
            )?,

            slot_accumulators_count: register_int_gauge_vec_with_registry!(
                "slot_accumulators_count",
                "Number of slot accumulators in memory",
                &["component"],
                registry.clone()
            )?,

            processed_slots_count: register_int_gauge_vec_with_registry!(
                "processed_slots_count",
                "Number of processed slots in memory",
                &["component"],
                registry.clone()
            )?,

            errors: register_int_counter_vec_with_registry!(
                "errors_total",
                "Total errors",
                &["stage", "type"],
                registry.clone()
            )?,
        };

        Ok(metrics)
    }

    pub fn init_with_registry(registry: Arc<Registry>) -> Result<()> {
        let metrics = Arc::new(Self::new(registry)?);
        METRICS
            .set(Some(metrics))
            .map_err(|_| anyhow::anyhow!("Metrics already initialized"))?;

        Ok(())
    }

    pub fn init_disabled() -> Result<()> {
        METRICS
            .set(None)
            .map_err(|_| anyhow::anyhow!("Metrics already initialized"))?;

        Ok(())
    }

    pub fn try_get() -> Option<&'static Arc<Metrics>> {
        METRICS.get().and_then(|opt| opt.as_ref())
    }
}

/// No-op metrics implementation for when metrics feature is disabled
#[cfg(not(feature = "metrics"))]
pub struct Metrics;

#[cfg(not(feature = "metrics"))]
impl Metrics {
    pub fn init_with_registry(_: Arc<Registry>) -> Result<()> {
        Ok(())
    }

    pub fn get() -> &'static Metrics {
        static METRICS: Metrics = Metrics;
        &METRICS
    }
}

#[cfg(not(feature = "metrics"))]
pub struct IntCounterVec;
#[cfg(not(feature = "metrics"))]
pub struct IntGaugeVec;
#[cfg(not(feature = "metrics"))]
pub struct HistogramVec;

#[cfg(not(feature = "metrics"))]
impl IntCounterVec {
    pub fn with_label_values(&self, _: &[&str]) -> &Self {
        self
    }
    pub fn inc(&self) {}
}

#[cfg(not(feature = "metrics"))]
impl IntGaugeVec {
    pub fn with_label_values(&self, _: &[&str]) -> &Self {
        self
    }
    pub fn set(&self, _: i64) {}
}

#[cfg(not(feature = "metrics"))]
impl HistogramVec {
    pub fn with_label_values(&self, _: &[&str]) -> &Self {
        self
    }
    pub fn observe(&self, _: f64) {}
}
