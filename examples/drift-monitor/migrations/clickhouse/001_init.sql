CREATE DATABASE IF NOT EXISTS drift;
USE drift;

CREATE TABLE IF NOT EXISTS drift_events (
    slot UInt64,
    signature String,
    instruction_data String,
    accounts Array(String),
    liquidation_type Nullable(String),
    received_at_micros Nullable(UInt64),
    processed_at_micros UInt64,
    _inserted_at DateTime64(3) DEFAULT now64(3),
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(_inserted_at)
ORDER BY (_inserted_at, signature)
SETTINGS index_granularity = 8192;

-- Index for liquidation analysis
CREATE INDEX idx_liquidations ON drift_events
    (liquidation_type, _inserted_at)
    TYPE minmax GRANULARITY 4;

-- Real-time liquidation metrics
CREATE MATERIALIZED VIEW liquidation_metrics_realtime
ENGINE = SummingMergeTree()
ORDER BY (window_start)
AS SELECT
    toDateTime64(toStartOfMinute(_inserted_at), 3) as window_start,
    countIf(liquidation_type IS NOT NULL) as total_liquidations,
    countIf(liquidation_type = 'liquidate_spot') as liquidate_spots,
    countIf(liquidation_type = 'liquidate_perp') as liquidate_perps,
    countIf(liquidation_type = 'liquidate_spot_with_swap_begin') as liquidate_spot_with_swap_begins,
    countIf(liquidation_type = 'liquidate_spot_with_swap_end') as liquidate_spot_with_swap_ends,
    countIf(liquidation_type = 'liquidate_borrow_for_perp_pnl') as liquidate_borrow_for_perp_pnls,
    countIf(liquidation_type = 'liquidate_perp_with_fill') as liquidate_perp_with_fills,
    countIf(liquidation_type = 'liquidate_perp_pnl_for_deposit') as liquidate_perp_pnl_for_deposits,
    countIf(liquidation_type = 'resolve_spot_bankruptcy') as resolve_spot_bankruptcys,
    countIf(liquidation_type = 'resolve_perp_bankruptcy') as resolve_perp_bankruptcys,
    countIf(liquidation_type = 'set_user_status_to_being_liquidated') as set_user_status_to_being_liquidateds
FROM drift_events
WHERE _inserted_at >= now() - INTERVAL 2 HOUR
GROUP BY window_start;

-- Raw recent liquidation events
CREATE VIEW recent_liquidations AS
SELECT
    _inserted_at,
    liquidation_type,
    signature,
    slot,
    CASE
        WHEN received_at_micros IS NOT NULL
        THEN (processed_at_micros - received_at_micros) / 1000
        ELSE NULL
    END as processing_latency_ms,
    length(accounts) as account_count,
    accounts[1] as primary_account,
    accounts[2] as liquidator_account
FROM drift_events
WHERE liquidation_type IS NOT NULL
  AND _inserted_at >= now() - INTERVAL 1 HOUR
ORDER BY _inserted_at DESC;
