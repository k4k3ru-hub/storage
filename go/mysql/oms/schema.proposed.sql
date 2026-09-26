-- OMS snapshot and append-only history, 2026-09-26. Breaking fresh-DB schema.
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
    limit_price VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL,
    take_profit_type VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NULL,
    take_profit_value VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL,
    stop_loss_type VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NULL,
    stop_loss_value VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL,
    specification_version SMALLINT UNSIGNED NOT NULL COMMENT 'Order specification format version',
    specification JSON NOT NULL COMMENT 'Original order conditions and asset units; no credentials or prepared quote',
    last_execution_sequence BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Last history sequence applied to this snapshot',
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
    id BIGINT UNSIGNED NOT NULL COMMENT 'Execution record ID',
    order_id BIGINT UNSIGNED NOT NULL COMMENT 'Order ID',
    sequence BIGINT UNSIGNED NOT NULL COMMENT 'Order-local append sequence allocated under the order lock',
    exec_type VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Immutable fact type, not mutable progress status',
    submission_record_id BIGINT UNSIGNED NULL COMMENT 'Submission acceptance record; null on the acceptance itself or order-wide facts',
    reference_record_id BIGINT UNSIGNED NULL COMMENT 'Prior record corrected, reversed or otherwise referenced',
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
    price VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL COMMENT 'Optional fill price in counter asset per quantity asset',
    fees_complete BOOLEAN NULL COMMENT 'Fee investigation completed; null means this fact makes no assertion',
    source_version BIGINT UNSIGNED NULL COMMENT 'Source-provided record revision when available',
    occurred_at DATETIME(6) NOT NULL COMMENT 'Occurrence time of this record',
    recorded_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    UNIQUE KEY uq_oms_exec_id_order (id, order_id),
    UNIQUE KEY uq_oms_exec_id_order_type (id, order_id, exec_type),
    UNIQUE KEY uq_oms_exec_sequence (order_id, sequence),
    UNIQUE KEY uq_oms_exec_record (order_id, record_key),
    UNIQUE KEY uq_oms_exec_submission (execution_system, (CASE WHEN exec_type = 'submission_accepted' THEN execution_id ELSE NULL END)),
    KEY idx_oms_exec_external (execution_system, execution_id, sequence),
    KEY idx_oms_exec_submission_type (submission_record_id, exec_type, id),
    KEY idx_oms_exec_type_recorded (exec_type, recorded_at, id),
    CONSTRAINT fk_oms_exec_order FOREIGN KEY (order_id) REFERENCES oms_orders(id) ON DELETE RESTRICT,
    CONSTRAINT fk_oms_exec_submission FOREIGN KEY (submission_record_id, order_id) REFERENCES oms_order_executions(id, order_id) ON DELETE RESTRICT,
    CONSTRAINT fk_oms_exec_reference FOREIGN KEY (reference_record_id, order_id) REFERENCES oms_order_executions(id, order_id) ON DELETE RESTRICT,
    CONSTRAINT ck_oms_exec_sequence CHECK (sequence > 0),
    CONSTRAINT ck_oms_exec_type CHECK (exec_type IN (
        'submission_accepted', 'submitted', 'submission_rejected',
        'execution_succeeded', 'execution_failed', 'execution_reversed',
        'filled', 'fill_reversed', 'fill_corrected',
        'fees_recorded', 'fees_adjusted', 'evidence_recorded',
        'order_canceled', 'order_expired', 'order_rejected', 'order_failed'
    )),
    CONSTRAINT ck_oms_exec_required CHECK (OCTET_LENGTH(record_key) > 0 AND CHAR_LENGTH(execution_system) > 0),
    CONSTRAINT ck_oms_exec_submission_shape CHECK (
        (exec_type = 'submission_accepted' AND submission_record_id IS NULL
            AND execution_id IS NOT NULL AND CHAR_LENGTH(execution_id) > 0
            AND venue IS NOT NULL AND CHAR_LENGTH(venue) > 0)
        OR (exec_type IN ('order_canceled', 'order_expired', 'order_rejected', 'order_failed')
            AND submission_record_id IS NULL AND execution_id IS NULL)
        OR (exec_type NOT IN ('submission_accepted', 'order_canceled', 'order_expired', 'order_rejected', 'order_failed')
            AND submission_record_id IS NOT NULL AND execution_id IS NOT NULL AND CHAR_LENGTH(execution_id) > 0)
    ),
    CONSTRAINT ck_oms_exec_reference CHECK (
        (submission_record_id IS NULL OR submission_record_id <> id)
        AND (reference_record_id IS NULL OR reference_record_id <> id)
        AND (exec_type NOT IN ('execution_reversed', 'fill_reversed', 'fill_corrected', 'fees_recorded', 'fees_adjusted', 'evidence_recorded') OR reference_record_id IS NOT NULL)
    ),
    CONSTRAINT ck_oms_exec_fill CHECK (
        (exec_type IN ('filled', 'fill_corrected')
            AND quantity IS NOT NULL AND counter_quantity IS NOT NULL AND order_quantity IS NOT NULL
            AND quantity_asset_id IS NOT NULL AND counter_asset_id IS NOT NULL
            AND quantity_decimals IS NOT NULL AND counter_decimals IS NOT NULL
            AND CHAR_LENGTH(quantity) > 0 AND CHAR_LENGTH(counter_quantity) > 0 AND CHAR_LENGTH(order_quantity) > 0
            AND CHAR_LENGTH(quantity_asset_id) > 0 AND CHAR_LENGTH(counter_asset_id) > 0
            AND quantity_decimals <= 255 AND counter_decimals <= 255)
        OR (exec_type NOT IN ('filled', 'fill_corrected')
            AND quantity IS NULL AND counter_quantity IS NULL AND order_quantity IS NULL
            AND quantity_asset_id IS NULL AND counter_asset_id IS NULL
            AND quantity_decimals IS NULL AND counter_decimals IS NULL AND price IS NULL)
    ),
    CONSTRAINT ck_oms_exec_fees_complete CHECK (fees_complete IS NULL OR fees_complete IN (0, 1))
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

CREATE TABLE oms_order_execution_onchain_details (
    execution_record_id BIGINT UNSIGNED NOT NULL,
    order_id BIGINT UNSIGNED NOT NULL,
    exec_type VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Parent fact discriminator, enforced by composite FK',
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
    PRIMARY KEY (execution_record_id),
    UNIQUE KEY uq_oms_onchain_submission (chain, network, (CASE WHEN exec_type = 'submission_accepted' THEN tx_id ELSE NULL END)),
    KEY idx_oms_onchain_tx (chain, network, tx_id),
    KEY idx_oms_onchain_ledger (chain, network, ledger_unit, ledger_sequence),
    CONSTRAINT fk_oms_onchain_execution FOREIGN KEY (execution_record_id, order_id, exec_type)
        REFERENCES oms_order_executions(id, order_id, exec_type) ON DELETE RESTRICT,
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
        (exec_type = 'submission_accepted' AND ledger_unit IS NULL AND event_position IS NULL
            AND signer_id IS NOT NULL AND CHAR_LENGTH(signer_id) > 0
            AND payload_digest IS NOT NULL AND CHAR_LENGTH(payload_digest) > 0
            AND payload_encoding IS NOT NULL AND CHAR_LENGTH(payload_encoding) > 0
            AND tx_payload IS NOT NULL AND OCTET_LENGTH(tx_payload) > 0)
        OR (exec_type <> 'submission_accepted' AND signer_id IS NULL AND recipient_id IS NULL
            AND payload_digest IS NULL AND payload_encoding IS NULL AND tx_payload IS NULL)
    ),
    CONSTRAINT ck_oms_onchain_result CHECK (
        exec_type NOT IN ('execution_succeeded', 'execution_failed', 'filled', 'fill_corrected')
        OR (ledger_unit IS NOT NULL AND finality_level IS NOT NULL)
    ),
    CONSTRAINT ck_oms_onchain_event CHECK (
        (exec_type IN ('filled', 'fill_corrected') AND event_position IS NOT NULL AND CHAR_LENGTH(event_position) > 0)
        OR (exec_type NOT IN ('filled', 'fill_corrected') AND event_position IS NULL)
    )
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

CREATE TABLE oms_order_execution_fees (
    id BIGINT UNSIGNED NOT NULL,
    order_id BIGINT UNSIGNED NOT NULL,
    execution_record_id BIGINT UNSIGNED NOT NULL COMMENT 'Result, fill or later fee fact owning this component',
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
    UNIQUE KEY uq_oms_fee_id_order (id, order_id),
    UNIQUE KEY uq_oms_fee_record (order_id, record_key),
    KEY idx_oms_fee_execution (execution_record_id, order_id),
    CONSTRAINT fk_oms_fee_execution FOREIGN KEY (execution_record_id, order_id)
        REFERENCES oms_order_executions(id, order_id) ON DELETE RESTRICT,
    CONSTRAINT fk_oms_fee_adjustment FOREIGN KEY (adjustment_of_fee_id, order_id)
        REFERENCES oms_order_execution_fees(id, order_id) ON DELETE RESTRICT,
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
