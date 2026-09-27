-- OMS order/execution snapshots and append-only onchain events, 2026-09-26. Breaking fresh-DB schema.
-- Quote, Prepare and approval are not persisted. Submit starts the history.
-- MySQL 8.4 / InnoDB; timestamps are UTC; decimal values are canonical strings.
-- The subsequent Store/operation changes must precede deployment to this schema.

CREATE TABLE oms_orders (
    id BIGINT UNSIGNED NOT NULL COMMENT 'Order ID',
    account_id BIGINT UNSIGNED NOT NULL COMMENT 'K4K3RU account ID',
    parent_order_id BIGINT UNSIGNED NULL COMMENT 'Parent order ID',
    account_ref VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Execution account or wallet reference',
    asset_class VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Asset class',
    domain VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Order domain',
    venue VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL COMMENT 'Requested venue; null permits routing to multiple venues',
    symbol VARCHAR(128) NOT NULL COMMENT 'Instrument display and search symbol',
    side VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Order side',
    order_type VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Order type',
    status VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'pending' COMMENT 'Current order snapshot status',
    quantity VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL COMMENT 'Order quantity; null until determined',
    filled_quantity VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '0' COMMENT 'Cumulative filled quantity in the order quantity unit',
    filled_counter_quantity VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL DEFAULT NULL COMMENT 'Cumulative counter quantity in the order counter asset; null when unavailable',
    limit_price VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL,
    take_profit_type VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NULL,
    take_profit_value VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL,
    stop_loss_type VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NULL,
    stop_loss_value VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL,
    specification_version SMALLINT UNSIGNED NOT NULL COMMENT 'Order specification format version',
    specification JSON NOT NULL COMMENT 'Original order conditions and asset units; no credentials or prepared quote',
    last_event_sequence BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Last history sequence applied to this snapshot',
    idempotency_key VARBINARY(128) NOT NULL COMMENT 'Account-scoped order creation key',
    expires_at DATETIME(6) NULL COMMENT 'Order expiry time',
    completed_at DATETIME(6) NULL COMMENT 'Time of transition to a terminal state',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    UNIQUE KEY uq_oms_order_id_account (id, account_id),
    UNIQUE KEY uq_oms_orders_account_idempotency (account_id, idempotency_key),
    KEY idx_oms_orders_parent (parent_order_id),
    KEY idx_oms_orders_account_status_created (account_id, status, created_at, id),
    KEY idx_oms_orders_account_venue_symbol (account_id, venue, symbol, id),
    KEY idx_oms_orders_status_expires (status, expires_at, id),
    CONSTRAINT fk_oms_order_parent FOREIGN KEY (parent_order_id, account_id) REFERENCES oms_orders(id, account_id) ON DELETE RESTRICT,
    CONSTRAINT ck_oms_order_status CHECK (status IN ('pending', 'partially_filled', 'filled', 'canceled', 'expired', 'failed', 'rejected')),
    CONSTRAINT ck_oms_order_specification CHECK (specification_version > 0 AND JSON_TYPE(specification) = 'OBJECT'),
    CONSTRAINT ck_oms_orders_take_profit CHECK (
        (take_profit_type IS NULL AND take_profit_value IS NULL)
        OR (take_profit_type IS NOT NULL AND take_profit_value IS NOT NULL AND take_profit_type IN ('price', 'return_bps'))
    ),
    CONSTRAINT ck_oms_orders_stop_loss CHECK (
        (stop_loss_type IS NULL AND stop_loss_value IS NULL)
        OR (stop_loss_type IS NOT NULL AND stop_loss_value IS NOT NULL AND stop_loss_type IN ('price', 'return_bps'))
    )
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

CREATE TABLE oms_order_executions (
    id BIGINT UNSIGNED NOT NULL COMMENT 'Stable execution snapshot ID',
    order_id BIGINT UNSIGNED NOT NULL,
    execution_system VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    execution_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Public execution identifier',
    event_family VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Adapter owning the event history, currently onchain',
    venue VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    status VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'pending',
    quantity VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL COMMENT 'Allocated quantity in order units; null when unspecified',
    filled_quantity VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '0' COMMENT 'Sum of effective fill contributions in order units, not intermediate leg quantities',
    filled_counter_quantity VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL DEFAULT NULL COMMENT 'Cumulative counter quantity in the order counter asset; null when unavailable',
    fees_complete BOOLEAN NOT NULL DEFAULT FALSE,
    last_event_sequence BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Last order-local event sequence applied to this execution',
    completed_at DATETIME(6) NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    UNIQUE KEY uq_oms_execution_id_order (id, order_id),
    UNIQUE KEY uq_oms_execution_external (execution_system, execution_id),
    KEY idx_oms_execution_order (order_id, id),
    KEY idx_oms_execution_pending (event_family, status, id),
    CONSTRAINT fk_oms_execution_order FOREIGN KEY (order_id) REFERENCES oms_orders(id) ON DELETE RESTRICT,
    CONSTRAINT ck_oms_execution_status CHECK (status IN ('pending', 'partially_filled', 'filled', 'succeeded', 'failed', 'rejected')),
    CONSTRAINT ck_oms_execution_required CHECK (CHAR_LENGTH(execution_system) > 0 AND CHAR_LENGTH(execution_id) > 0 AND CHAR_LENGTH(event_family) > 0 AND CHAR_LENGTH(venue) > 0 AND CHAR_LENGTH(filled_quantity) > 0),
    CONSTRAINT ck_oms_execution_fees CHECK (fees_complete IN (0, 1))
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

CREATE TABLE oms_order_execution_onchain_events (
    id BIGINT UNSIGNED NOT NULL COMMENT 'Execution record ID',
    order_id BIGINT UNSIGNED NOT NULL COMMENT 'Order ID',
    execution_record_id BIGINT UNSIGNED NOT NULL COMMENT 'Owning execution snapshot',
    requested_quantity VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL COMMENT 'Accepted allocation in order units; only on submission_accepted',
    sequence BIGINT UNSIGNED NOT NULL COMMENT 'Order-local append sequence allocated under the order lock',
    event_type VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Immutable fact type, not mutable progress status',
    submission_event_id BIGINT UNSIGNED NULL COMMENT 'Submission acceptance record; null only on acceptance',
    reference_event_id BIGINT UNSIGNED NULL COMMENT 'Prior record corrected, reversed or otherwise referenced',
    record_key VARBINARY(128) NOT NULL COMMENT 'Stable record key within the order',
    execution_system VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Execution system identifier',
    execution_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL COMMENT 'Public submission ID shared by acceptance, result and fills',
    venue VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL COMMENT 'Actual execution venue, required on submission acceptance',
    quantity VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL COMMENT 'Individual filled quantity in quantity_asset_id units',
    counter_quantity VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL COMMENT 'Individual filled counter asset quantity',
    quantity_asset_id TEXT CHARACTER SET ascii COLLATE ascii_bin NULL COMMENT 'Canonical scoped asset identifier, not display symbol',
    counter_asset_id TEXT CHARACTER SET ascii COLLATE ascii_bin NULL COMMENT 'Canonical scoped counter asset identifier',
    quantity_decimals SMALLINT UNSIGNED NULL,
    counter_decimals SMALLINT UNSIGNED NULL,
    order_quantity VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL COMMENT 'Fill contribution in order quantity units; zero for intermediate legs',
    order_counter_quantity VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL DEFAULT NULL COMMENT 'Fill contribution in order counter asset units; null when unavailable',
    price VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL COMMENT 'Optional fill price in counter asset per quantity asset',
    fees_complete BOOLEAN NULL COMMENT 'Fee investigation completed; null means this fact makes no assertion',
    source_version BIGINT UNSIGNED NULL COMMENT 'Source-provided record revision when available',
    occurred_at DATETIME(6) NOT NULL COMMENT 'Occurrence time of this record',
    recorded_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    chain_family VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    chain VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    network VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    tx_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    ledger_unit VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NULL,
    ledger_sequence BIGINT UNSIGNED NULL,
    ledger_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NULL,
    tx_position BIGINT UNSIGNED NULL,
    event_position VARCHAR(512) CHARACTER SET ascii COLLATE ascii_bin NULL COMMENT 'Versioned source position, not a universal numeric index',
    emitter_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NULL,
    pool_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NULL,
    signer_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NULL,
    recipient_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NULL,
    payload_digest VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NULL,
    payload_encoding VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NULL,
    tx_payload MEDIUMBLOB NULL COMMENT 'Accepted swap recovery material; never expose in history responses',
    finality_level VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NULL,
    protocol_version SMALLINT UNSIGNED NOT NULL,
    protocol_data JSON NULL COMMENT 'Versioned typed protocol evidence; no credentials or private keys',
    PRIMARY KEY (id),
    UNIQUE KEY uq_oms_event_id_order (id, order_id),
    UNIQUE KEY uq_oms_event_parent (id, order_id, execution_record_id),
    UNIQUE KEY uq_oms_event_sequence (order_id, sequence),
    UNIQUE KEY uq_oms_event_record (order_id, record_key),
    UNIQUE KEY uq_oms_event_submission (execution_system, (CASE WHEN event_type = 'submission_accepted' THEN execution_id ELSE NULL END)),
    KEY idx_oms_event_external (execution_system, execution_id, sequence),
    KEY idx_oms_event_submission_type (submission_event_id, event_type, id),
    KEY idx_oms_event_type_recorded (event_type, recorded_at, id),
    KEY idx_oms_event_execution (execution_record_id, sequence),
    CONSTRAINT fk_oms_event_execution FOREIGN KEY (execution_record_id, order_id) REFERENCES oms_order_executions(id, order_id) ON DELETE RESTRICT,
    UNIQUE KEY uq_oms_onchain_submission (chain, network, (CASE WHEN event_type = 'submission_accepted' THEN tx_id ELSE NULL END)),
    KEY idx_oms_onchain_tx (chain, network, tx_id),
    KEY idx_oms_onchain_ledger (chain, network, ledger_unit, ledger_sequence),
    CONSTRAINT fk_oms_event_order FOREIGN KEY (order_id) REFERENCES oms_orders(id) ON DELETE RESTRICT,
    CONSTRAINT fk_oms_event_submission FOREIGN KEY (submission_event_id, order_id, execution_record_id) REFERENCES oms_order_execution_onchain_events(id, order_id, execution_record_id) ON DELETE RESTRICT,
    CONSTRAINT fk_oms_event_reference FOREIGN KEY (reference_event_id, order_id, execution_record_id) REFERENCES oms_order_execution_onchain_events(id, order_id, execution_record_id) ON DELETE RESTRICT,
    CONSTRAINT ck_oms_event_sequence CHECK (sequence > 0),
    CONSTRAINT ck_oms_event_type CHECK (event_type IN (
        'submission_accepted', 'submitted', 'submission_rejected',
        'execution_succeeded', 'execution_failed', 'execution_reversed',
        'filled', 'fill_reversed', 'fill_corrected',
        'fees_recorded', 'fees_adjusted', 'evidence_recorded',
        'order_canceled', 'order_expired', 'order_rejected', 'order_failed'
    )),
    CONSTRAINT ck_oms_event_required CHECK (OCTET_LENGTH(record_key) > 0 AND CHAR_LENGTH(execution_system) > 0),
    CONSTRAINT ck_oms_event_submission_shape CHECK (
        (event_type = 'submission_accepted' AND submission_event_id IS NULL
            AND execution_id IS NOT NULL AND CHAR_LENGTH(execution_id) > 0
            AND venue IS NOT NULL AND CHAR_LENGTH(venue) > 0)
        OR (event_type <> 'submission_accepted'
            AND submission_event_id IS NOT NULL AND execution_id IS NOT NULL AND CHAR_LENGTH(execution_id) > 0)
    ),
    CONSTRAINT ck_oms_event_reference CHECK (
        (submission_event_id IS NULL OR submission_event_id <> id)
        AND (reference_event_id IS NULL OR reference_event_id <> id)
        AND (event_type NOT IN ('execution_reversed', 'fill_reversed', 'fill_corrected', 'fees_recorded', 'fees_adjusted', 'evidence_recorded') OR reference_event_id IS NOT NULL)
    ),
    CONSTRAINT ck_oms_event_fill CHECK (
        (event_type IN ('filled', 'fill_corrected')
            AND quantity IS NOT NULL AND counter_quantity IS NOT NULL AND order_quantity IS NOT NULL
            AND quantity_asset_id IS NOT NULL AND counter_asset_id IS NOT NULL
            AND quantity_decimals IS NOT NULL AND counter_decimals IS NOT NULL
            AND CHAR_LENGTH(quantity) > 0 AND CHAR_LENGTH(counter_quantity) > 0 AND CHAR_LENGTH(order_quantity) > 0
            AND CHAR_LENGTH(quantity_asset_id) > 0 AND CHAR_LENGTH(counter_asset_id) > 0
            AND quantity_decimals <= 255 AND counter_decimals <= 255)
        OR (event_type NOT IN ('filled', 'fill_corrected')
            AND quantity IS NULL AND counter_quantity IS NULL AND order_quantity IS NULL AND order_counter_quantity IS NULL
            AND quantity_asset_id IS NULL AND counter_asset_id IS NULL
            AND quantity_decimals IS NULL AND counter_decimals IS NULL AND price IS NULL)
    ),
    CONSTRAINT ck_oms_event_fees_complete CHECK (fees_complete IS NULL OR fees_complete IN (0, 1)),
    CONSTRAINT ck_oms_event_request CHECK (requested_quantity IS NULL OR (event_type = 'submission_accepted' AND CHAR_LENGTH(requested_quantity) > 0)),
    CONSTRAINT ck_oms_onchain_required CHECK (
        chain_family IN ('evm', 'solana', 'sui') AND CHAR_LENGTH(chain) > 0
        AND CHAR_LENGTH(network) > 0 AND CHAR_LENGTH(tx_id) > 0 AND protocol_version > 0
        AND (protocol_data IS NULL OR JSON_TYPE(protocol_data) = 'OBJECT')
    ),
    CONSTRAINT ck_oms_onchain_ledger CHECK (
        (ledger_unit IS NULL AND ledger_sequence IS NULL AND ledger_id IS NULL AND tx_position IS NULL AND finality_level IS NULL)
        OR (ledger_unit IS NOT NULL AND ledger_sequence IS NOT NULL
            AND ((chain_family = 'evm' AND ledger_unit = 'block')
                OR (chain_family = 'solana' AND ledger_unit = 'slot')
                OR (chain_family = 'sui' AND ledger_unit = 'checkpoint'))
            AND (finality_level IS NULL OR finality_level IN ('observed', 'confirmed', 'finalized')))
    ),
    CONSTRAINT ck_oms_onchain_acceptance CHECK (
        (event_type = 'submission_accepted' AND ledger_unit IS NULL AND event_position IS NULL
            AND signer_id IS NOT NULL AND CHAR_LENGTH(signer_id) > 0
            AND payload_digest IS NOT NULL AND CHAR_LENGTH(payload_digest) > 0
            AND payload_encoding IS NOT NULL AND CHAR_LENGTH(payload_encoding) > 0
            AND tx_payload IS NOT NULL AND OCTET_LENGTH(tx_payload) > 0)
        OR (event_type <> 'submission_accepted' AND signer_id IS NULL AND recipient_id IS NULL
            AND payload_digest IS NULL AND payload_encoding IS NULL AND tx_payload IS NULL)
    ),
    CONSTRAINT ck_oms_onchain_result CHECK (
        event_type NOT IN ('execution_succeeded', 'execution_failed', 'filled', 'fill_corrected')
        OR (ledger_unit IS NOT NULL AND finality_level IS NOT NULL)
    ),
    CONSTRAINT ck_oms_onchain_event CHECK (
        (event_type IN ('filled', 'fill_corrected') AND event_position IS NOT NULL AND CHAR_LENGTH(event_position) > 0)
        OR (event_type NOT IN ('filled', 'fill_corrected') AND event_position IS NULL)
    )
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

CREATE TABLE oms_order_execution_fees (
    id BIGINT UNSIGNED NOT NULL,
    order_id BIGINT UNSIGNED NOT NULL,
    execution_record_id BIGINT UNSIGNED NOT NULL COMMENT 'Owning execution snapshot',
    event_id BIGINT UNSIGNED NOT NULL COMMENT 'Source event in the execution event_family table, validated by Store',
    adjustment_of_fee_id BIGINT UNSIGNED NULL COMMENT 'Original fee adjusted by this signed delta',
    record_key VARBINARY(128) NOT NULL,
    fee_type VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    accounting_treatment VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    asset_namespace VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    asset_chain VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    asset_network VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    asset_id TEXT CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    asset_decimals SMALLINT UNSIGNED NOT NULL,
    amount VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Canonical asset-unit decimal; positive cost, negative rebate or refund',
    source_reference VARCHAR(512) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    source_version BIGINT UNSIGNED NULL,
    occurred_at DATETIME(6) NOT NULL,
    recorded_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    UNIQUE KEY uq_oms_fee_id_order (id, order_id, execution_record_id),
    UNIQUE KEY uq_oms_fee_record (order_id, record_key),
    KEY idx_oms_fee_event (event_id, execution_record_id),
    KEY idx_oms_fee_execution (execution_record_id, order_id),
    CONSTRAINT fk_oms_fee_execution FOREIGN KEY (execution_record_id, order_id)
        REFERENCES oms_order_executions(id, order_id) ON DELETE RESTRICT,
    CONSTRAINT fk_oms_fee_adjustment FOREIGN KEY (adjustment_of_fee_id, order_id, execution_record_id)
        REFERENCES oms_order_execution_fees(id, order_id, execution_record_id) ON DELETE RESTRICT,
    CONSTRAINT ck_oms_fee_adjustment CHECK (adjustment_of_fee_id IS NULL OR adjustment_of_fee_id <> id),
    CONSTRAINT ck_oms_fee_treatment CHECK (accounting_treatment IN ('additional', 'included_in_input', 'included_in_output', 'unknown')),
    CONSTRAINT ck_oms_fee_asset CHECK (
        (asset_namespace = 'onchain' AND asset_chain IS NOT NULL AND asset_network IS NOT NULL
            AND CHAR_LENGTH(asset_chain) > 0 AND CHAR_LENGTH(asset_network) > 0)
        OR (asset_namespace IN ('venue', 'currency') AND asset_chain IS NULL AND asset_network IS NULL)
    ),
    CONSTRAINT ck_oms_fee_required CHECK (
        OCTET_LENGTH(record_key) > 0 AND CHAR_LENGTH(fee_type) > 0 AND CHAR_LENGTH(asset_id) > 0
        AND CHAR_LENGTH(amount) > 0 AND CHAR_LENGTH(source_reference) > 0 AND asset_decimals <= 255
    )
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;
