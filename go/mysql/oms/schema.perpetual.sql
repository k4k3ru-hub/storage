CREATE TABLE oms_order_execution_perpetual_events (
    id BIGINT UNSIGNED NOT NULL,
    order_id BIGINT UNSIGNED NOT NULL,
    execution_record_id BIGINT UNSIGNED NOT NULL,
    sequence BIGINT UNSIGNED NOT NULL,
    event_type VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    submission_event_id BIGINT UNSIGNED NULL,
    reference_event_id BIGINT UNSIGNED NULL,
    record_key VARBINARY(128) NOT NULL,
    execution_system VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    execution_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    requested_quantity VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL,

    venue VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    network VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    trading_account VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    market_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    client_order_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NULL,
    venue_order_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NULL,
    venue_status VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    venue_status_time_ms BIGINT UNSIGNED NULL,
    observed_at DATETIME(6) NOT NULL,

    fill_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NULL,
    -- Populated on original filled events only; corrections reference that event.
    fill_identity_hash BINARY(32) NULL,
    fill_time_ms BIGINT UNSIGNED NULL,
    quantity VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL,
    counter_quantity VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL,
    quantity_asset_id TEXT CHARACTER SET ascii COLLATE ascii_bin NULL,
    counter_asset_id TEXT CHARACTER SET ascii COLLATE ascii_bin NULL,
    quantity_decimals SMALLINT UNSIGNED NULL,
    counter_decimals SMALLINT UNSIGNED NULL,
    order_quantity VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL,
    order_counter_quantity VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL,
    price VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL,
    start_position VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL,
    direction VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    liquidity VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NULL,
    closed_pnl VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL,
    closed_pnl_asset_id TEXT CHARACTER SET ascii COLLATE ascii_bin NULL,
    closed_pnl_decimals SMALLINT UNSIGNED NULL,
    fees_complete BOOLEAN NULL,
    source_version BIGINT UNSIGNED NULL,
    protocol_version SMALLINT UNSIGNED NOT NULL,
    protocol_data JSON NOT NULL COMMENT 'Versioned allowlisted facts; no signature or prepared token',
    occurred_at DATETIME(6) NOT NULL,
    recorded_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    PRIMARY KEY (id),
    UNIQUE KEY uq_oms_perp_event_parent (id, order_id, execution_record_id),
    UNIQUE KEY uq_oms_perp_event_sequence (order_id, sequence),
    UNIQUE KEY uq_oms_perp_event_record (order_id, record_key),
    UNIQUE KEY uq_oms_perp_event_fill (execution_record_id, fill_identity_hash),
    UNIQUE KEY uq_oms_perp_event_submission
        (execution_system, (CASE WHEN event_type = 'submission_accepted' THEN execution_id ELSE NULL END)),
    KEY idx_oms_perp_event_execution (execution_record_id, sequence),
    KEY idx_oms_perp_event_external (execution_system, execution_id, sequence),
    CONSTRAINT fk_oms_perp_event_execution FOREIGN KEY (execution_record_id, order_id)
        REFERENCES oms_order_executions(id, order_id) ON DELETE RESTRICT,
    CONSTRAINT fk_oms_perp_event_submission FOREIGN KEY (submission_event_id, order_id, execution_record_id)
        REFERENCES oms_order_execution_perpetual_events(id, order_id, execution_record_id) ON DELETE RESTRICT,
    CONSTRAINT fk_oms_perp_event_reference FOREIGN KEY (reference_event_id, order_id, execution_record_id)
        REFERENCES oms_order_execution_perpetual_events(id, order_id, execution_record_id) ON DELETE RESTRICT,
    CONSTRAINT ck_oms_perp_event_sequence CHECK (sequence > 0),
    CONSTRAINT ck_oms_perp_event_type CHECK (event_type IN (
        'submission_accepted', 'submitted', 'submission_rejected',
        'execution_succeeded', 'execution_failed', 'execution_reversed',
        'filled', 'fill_reversed', 'fill_corrected', 'fees_recorded', 'fees_adjusted',
        'evidence_recorded', 'order_canceled', 'order_expired', 'order_rejected', 'order_failed'
    )),
    CONSTRAINT ck_oms_perp_event_required CHECK (
        OCTET_LENGTH(record_key) > 0 AND CHAR_LENGTH(execution_system) > 0
        AND CHAR_LENGTH(execution_id) > 0 AND CHAR_LENGTH(venue) > 0
        AND CHAR_LENGTH(network) > 0 AND CHAR_LENGTH(trading_account) > 0
        AND CHAR_LENGTH(market_id) > 0 AND protocol_version > 0
        AND JSON_TYPE(protocol_data) = 'OBJECT'
    ),
    CONSTRAINT ck_oms_perp_event_submission CHECK (
        (event_type = 'submission_accepted' AND submission_event_id IS NULL
            AND requested_quantity IS NOT NULL)
        OR (event_type <> 'submission_accepted' AND submission_event_id IS NOT NULL
            AND requested_quantity IS NULL AND submission_event_id <> id)
    ),
    CONSTRAINT ck_oms_perp_event_reference CHECK (
        (reference_event_id IS NULL OR reference_event_id <> id)
        AND (event_type NOT IN ('execution_reversed', 'fill_reversed', 'fill_corrected',
             'fees_recorded', 'fees_adjusted', 'evidence_recorded') OR reference_event_id IS NOT NULL)
    ),
    CONSTRAINT ck_oms_perp_event_fill_key CHECK (
        (event_type = 'filled' AND fill_identity_hash IS NOT NULL)
        OR (event_type <> 'filled' AND fill_identity_hash IS NULL)
    ),
    CONSTRAINT ck_oms_perp_event_fill CHECK (
        (event_type IN ('filled', 'fill_corrected') AND fill_id IS NOT NULL AND fill_time_ms IS NOT NULL
            AND quantity IS NOT NULL AND counter_quantity IS NOT NULL
            AND quantity_asset_id IS NOT NULL AND counter_asset_id IS NOT NULL
            AND quantity_decimals IS NOT NULL AND counter_decimals IS NOT NULL
            AND order_quantity IS NOT NULL AND order_counter_quantity IS NOT NULL AND price IS NOT NULL)
        OR (event_type NOT IN ('filled', 'fill_corrected') AND fill_id IS NULL AND fill_time_ms IS NULL
            AND quantity IS NULL AND counter_quantity IS NULL
            AND quantity_asset_id IS NULL AND counter_asset_id IS NULL
            AND quantity_decimals IS NULL AND counter_decimals IS NULL
            AND order_quantity IS NULL AND order_counter_quantity IS NULL AND price IS NULL
            AND start_position IS NULL AND direction IS NULL AND liquidity IS NULL AND closed_pnl IS NULL)
    ),
    CONSTRAINT ck_oms_perp_event_decimals CHECK (
        (quantity_decimals IS NULL OR quantity_decimals <= 255)
        AND (counter_decimals IS NULL OR counter_decimals <= 255)
    ),
    CONSTRAINT ck_oms_perp_event_pnl CHECK (
        (closed_pnl IS NULL AND closed_pnl_asset_id IS NULL AND closed_pnl_decimals IS NULL)
        OR (closed_pnl IS NOT NULL AND CHAR_LENGTH(closed_pnl) > 0
            AND closed_pnl_asset_id IS NOT NULL AND CHAR_LENGTH(closed_pnl_asset_id) > 0
            AND closed_pnl_decimals IS NOT NULL AND closed_pnl_decimals <= 255)
    ),
    CONSTRAINT ck_oms_perp_event_liquidity CHECK (liquidity IS NULL OR liquidity IN ('maker', 'taker', 'unknown')),
    CONSTRAINT ck_oms_perp_event_fees CHECK (fees_complete IS NULL OR fees_complete IN (0, 1)),
    CONSTRAINT ck_oms_perp_event_identifiers CHECK (
        (client_order_id IS NULL OR CHAR_LENGTH(client_order_id) > 0)
        AND (venue_order_id IS NULL OR CHAR_LENGTH(venue_order_id) > 0)
        AND (fill_id IS NULL OR CHAR_LENGTH(fill_id) > 0)
    )
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

