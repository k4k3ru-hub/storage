-- Reviewed OMS schema, 2026-09-20. Not an installed production migration.
-- MySQL 8.4 / InnoDB; timestamps are UTC; decimal values are canonical strings.

CREATE TABLE oms_orders (
    id BIGINT UNSIGNED NOT NULL COMMENT 'Order ID',
    account_id BIGINT UNSIGNED NOT NULL COMMENT 'K4K3RU account ID',
    parent_order_id BIGINT UNSIGNED NULL COMMENT 'Parent order ID',
    account_ref VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Execution account or wallet reference',
    asset_class VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Asset class',
    domain VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Order domain',
    venue VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Execution venue',
    symbol VARCHAR(128) NOT NULL COMMENT 'Instrument display and search symbol',
    side VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Order side',
    order_type VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Order type',
    status VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Order status: pending, processing, partially_filled, filled, canceled, expired, failed, rejected',
    quantity VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL COMMENT 'Order quantity; null until determined',
    filled_quantity VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '0' COMMENT 'Cumulative filled quantity in the order quantity unit',
    limit_price VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL,
    take_profit_type VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NULL,
    take_profit_value VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL,
    stop_loss_type VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NULL,
    stop_loss_value VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL,
    idempotency_key VARBINARY(128) NOT NULL COMMENT 'Account-scoped order creation key',
    expires_at DATETIME(6) NULL COMMENT 'Order expiry time',
    completed_at DATETIME(6) NULL COMMENT 'Time of transition to a terminal state',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    UNIQUE KEY uq_oms_orders_account_idempotency (account_id, idempotency_key),
    KEY idx_oms_orders_parent (parent_order_id),
    KEY idx_oms_orders_account_status_created (account_id, status, created_at, id),
    KEY idx_oms_orders_account_venue_symbol (account_id, venue, symbol, id),
    KEY idx_oms_orders_status_expires (status, expires_at, id),
    CONSTRAINT fk_oms_orders_parent FOREIGN KEY (parent_order_id) REFERENCES oms_orders(id) ON DELETE RESTRICT,
    CONSTRAINT ck_oms_orders_take_profit CHECK (
        (take_profit_type IS NULL AND take_profit_value IS NULL)
        OR (take_profit_type IS NOT NULL AND take_profit_value IS NOT NULL AND take_profit_type IN ('price', 'return_bps'))
    ),
    CONSTRAINT ck_oms_orders_stop_loss CHECK (
        (stop_loss_type IS NULL AND stop_loss_value IS NULL)
        OR (stop_loss_type IS NOT NULL AND stop_loss_value IS NOT NULL AND stop_loss_type IN ('price', 'return_bps'))
    )
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

CREATE TABLE oms_order_onchain_amm_pool_swaps (
    order_id BIGINT UNSIGNED NOT NULL COMMENT 'Parent order ID',
    chain_family VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Chain family',
    chain VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Chain',
    network VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Network',
    pool_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'AMM pool identifier',
    token_in_id TEXT CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Input token identifier',
    token_out_id TEXT CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Output token identifier',
    token_in_decimals SMALLINT UNSIGNED NOT NULL COMMENT 'Input token decimals',
    token_out_decimals SMALLINT UNSIGNED NOT NULL COMMENT 'Output token decimals',
    swap_kind VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Swap kind: exact-input or exact-output',
    signer VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Signer public address',
    recipient VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Output asset recipient address',
    maximum_slippage_bps SMALLINT UNSIGNED NOT NULL COMMENT 'Maximum slippage in basis points',
    execution_ttl_ms BIGINT UNSIGNED NOT NULL COMMENT 'Prepared execution lifetime in milliseconds',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (order_id),
    CONSTRAINT fk_oms_order_onchain_amm_pool_swaps_order FOREIGN KEY (order_id) REFERENCES oms_orders(id) ON DELETE RESTRICT,
    CONSTRAINT ck_oms_order_onchain_amm_pool_swaps_kind CHECK (swap_kind IN ('exact-input', 'exact-output')),
    CONSTRAINT ck_oms_order_onchain_amm_pool_swaps_slippage CHECK (maximum_slippage_bps <= 10000),
    CONSTRAINT ck_oms_order_onchain_amm_pool_swaps_ttl CHECK (execution_ttl_ms > 0)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

CREATE TABLE oms_order_executions (
    id BIGINT UNSIGNED NOT NULL COMMENT 'Execution record ID',
    order_id BIGINT UNSIGNED NOT NULL COMMENT 'Order ID',
    attempt_number BIGINT UNSIGNED NOT NULL COMMENT 'Execution attempt number within the order',
    exec_type VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Record type: prepared, submitted, filled',
    purpose VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Execution purpose: approval or trade',
    status VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Record status: pending, succeeded, failed, reversed, rejected',
    record_key VARBINARY(128) NOT NULL COMMENT 'Stable record key within the order',
    execution_system VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Execution system identifier',
    execution_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL COMMENT 'External execution identifier',
    quantity VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL COMMENT 'Individual filled quantity in the order quantity unit',
    counter_quantity VARCHAR(384) CHARACTER SET ascii COLLATE ascii_bin NULL COMMENT 'Individual filled counter asset quantity',
    source_version BIGINT UNSIGNED NULL COMMENT 'Source-provided record revision when available',
    occurred_at DATETIME(6) NOT NULL COMMENT 'Occurrence time of this record',
    expires_at DATETIME(6) NULL COMMENT 'Prepared execution expiry time',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    UNIQUE KEY uq_oms_order_executions_record (order_id, record_key),
    KEY idx_oms_order_executions_attempt (order_id, attempt_number, id),
    KEY idx_oms_order_executions_external (execution_system, execution_id),
    KEY idx_oms_order_executions_status (exec_type, status, updated_at, id),
    CONSTRAINT fk_oms_order_executions_order FOREIGN KEY (order_id) REFERENCES oms_orders(id) ON DELETE RESTRICT,
    CONSTRAINT ck_oms_order_executions_attempt CHECK (attempt_number > 0),
    CONSTRAINT ck_oms_order_executions_filled_quantity CHECK (
        (exec_type = 'filled' AND purpose = 'trade' AND quantity IS NOT NULL AND counter_quantity IS NOT NULL)
        OR (exec_type <> 'filled' AND quantity IS NULL AND counter_quantity IS NULL)
    ),
    CONSTRAINT ck_oms_order_executions_expiry CHECK (expires_at IS NULL OR exec_type = 'prepared')
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;
