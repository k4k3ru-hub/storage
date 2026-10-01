CREATE TABLE IF NOT EXISTS onchain_amm_pool_new_pair_snapshots (
    id BINARY(32) NOT NULL,
    chain_family VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    chain VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    network VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    venue VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    pool_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    token0_id TEXT CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    token1_id TEXT CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    pool_created_at DATETIME(6) NOT NULL,
    swap_observed_at DATETIME(6) NULL,
    position_kind VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NULL,
    swap_observed_position_number BIGINT UNSIGNED NULL,
    swap_observed_position_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NULL,
    confirmed_at DATETIME(6) NULL,
    liquidity_usd DECIMAL(38,18) NULL,
    liquidity_evaluated_at DATETIME(6) NULL,
    state JSON NOT NULL,
    revision BIGINT UNSIGNED NOT NULL,
    is_canonical BOOLEAN NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_amm_pool_identity (chain_family, chain, network, venue, pool_id),
    KEY idx_amm_pool_created (is_canonical, pool_created_at, id),
    KEY idx_amm_pool_chain_created (chain, network, is_canonical, pool_created_at, id)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

CREATE TABLE IF NOT EXISTS onchain_amm_pool_new_pair_events (
    id BINARY(32) NOT NULL,
    pool_id BINARY(32) NOT NULL,
    source_id BINARY(32) NOT NULL,
    position_number BIGINT UNSIGNED NOT NULL,
    position_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    transaction_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    event_index VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    event_type VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    occurred_at DATETIME(6) NOT NULL,
    observed_at DATETIME(6) NOT NULL,
    payload JSON NOT NULL,
    is_canonical BOOLEAN NOT NULL,
    PRIMARY KEY (id),
    KEY idx_amm_pool_event_history (pool_id, position_number, id),
    KEY idx_amm_pool_event_source (source_id, position_number),
    KEY idx_amm_pool_event_retention (observed_at),
    FOREIGN KEY (pool_id) REFERENCES onchain_amm_pool_new_pair_snapshots(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

CREATE TABLE IF NOT EXISTS onchain_amm_pool_new_pair_sync_cursors (
    id BINARY(32) NOT NULL,
    chain_family VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    chain VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    network VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    venue VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    source_key VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    position JSON NOT NULL,
    revision BIGINT UNSIGNED NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_amm_pool_source (chain_family, chain, network, venue, source_key)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

CREATE TABLE IF NOT EXISTS onchain_amm_pool_new_pair_activity_minutes (
    pool_id BINARY(32) NOT NULL,
    minute_started_at DATETIME(6) NOT NULL,
    totals JSON NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (pool_id, minute_started_at),
    KEY idx_amm_pool_activity_minute_retention (minute_started_at),
    FOREIGN KEY (pool_id)
        REFERENCES onchain_amm_pool_new_pair_snapshots(id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

CREATE TABLE IF NOT EXISTS onchain_amm_pool_new_pair_sender_snapshots (
    pool_id BINARY(32) NOT NULL,
    creation_event_id BINARY(32) NOT NULL,
    generation BIGINT UNSIGNED NOT NULL,
    initialized_at DATETIME(6) NOT NULL,
    invalid_from DATETIME(6) NULL,
    invalid_to DATETIME(6) NULL,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (pool_id),
    UNIQUE KEY uk_sender_pool_generation (pool_id, generation),
    FOREIGN KEY (pool_id)
        REFERENCES onchain_amm_pool_new_pair_snapshots(id)
        ON DELETE CASCADE,
    CHECK (generation > 0),
    CHECK (
        (invalid_from IS NULL AND invalid_to IS NULL)
        OR (invalid_from IS NOT NULL AND invalid_to IS NOT NULL AND invalid_from < invalid_to)
    )
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

CREATE TABLE IF NOT EXISTS onchain_amm_pool_new_pair_sender_transactions (
    id BINARY(32) NOT NULL,
    chain_family VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    chain VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    network VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    transaction_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    sender_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NULL,
    status VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    attempts TINYINT UNSIGNED NOT NULL DEFAULT 0,
    next_attempt_at DATETIME(6) NULL,
    deadline_at DATETIME(6) NOT NULL,
    expires_at DATETIME(6) NOT NULL,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_sender_transaction (chain_family, chain, network, transaction_id),
    KEY idx_sender_transaction_due (status, next_attempt_at),
    KEY idx_sender_transaction_expiry (expires_at),
    CHECK (attempts <= 4),
    CHECK (deadline_at > created_at AND expires_at >= deadline_at),
    CHECK (
        (status = 'pending' AND sender_id IS NULL AND next_attempt_at IS NOT NULL)
        OR (status = 'resolved' AND sender_id IS NOT NULL AND next_attempt_at IS NULL)
        OR (status = 'abandoned' AND sender_id IS NULL AND next_attempt_at IS NULL)
    )
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

CREATE TABLE IF NOT EXISTS onchain_amm_pool_new_pair_sender_events (
    id BINARY(32) NOT NULL,
    pool_id BINARY(32) NOT NULL,
    generation BIGINT UNSIGNED NOT NULL,
    transaction_ref BINARY(32) NOT NULL,
    position_number BIGINT UNSIGNED NOT NULL,
    position_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    event_index VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    minute_started_at DATETIME(6) NULL,
    direction TINYINT UNSIGNED NOT NULL,
    is_canonical BOOLEAN NOT NULL,
    observed_at DATETIME(6) NOT NULL,
    expires_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (id),
    KEY idx_sender_event_window (pool_id, generation, minute_started_at),
    KEY idx_sender_event_transaction (transaction_ref),
    KEY idx_sender_event_expiry (expires_at),
    FOREIGN KEY (pool_id, generation)
        REFERENCES onchain_amm_pool_new_pair_sender_snapshots(pool_id, generation)
        ON DELETE CASCADE,
    FOREIGN KEY (transaction_ref)
        REFERENCES onchain_amm_pool_new_pair_sender_transactions(id)
        ON DELETE RESTRICT,
    CHECK (generation > 0),
    CHECK (direction IN (0, 1, 2)),
    CHECK (is_canonical IN (0, 1))
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

CREATE TABLE IF NOT EXISTS onchain_amm_pool_new_pair_lp_checkpoints (
    pool_id BINARY(32) NOT NULL,
    source_id BINARY(32) NOT NULL,
    creation_event_id BINARY(32) NOT NULL,
    format_version SMALLINT UNSIGNED NOT NULL,
    position_kind VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    position_number BIGINT UNSIGNED NOT NULL,
    position_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    event_index VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NULL,
    payload JSON NOT NULL,
    payload_bytes INT UNSIGNED GENERATED ALWAYS AS (
        OCTET_LENGTH(CAST(payload AS CHAR CHARACTER SET utf8mb4))
    ) STORED,
    revision BIGINT UNSIGNED NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (pool_id),
    KEY idx_lp_checkpoint_source (source_id, pool_id, payload_bytes),
    FOREIGN KEY (pool_id)
        REFERENCES onchain_amm_pool_new_pair_snapshots(id)
        ON DELETE CASCADE,
    FOREIGN KEY (source_id)
        REFERENCES onchain_amm_pool_new_pair_sync_cursors(id)
        ON DELETE RESTRICT,
    CHECK (format_version > 0),
    CHECK (CHAR_LENGTH(position_kind) > 0),
    CHECK (CHAR_LENGTH(position_id) > 0),
    CHECK (event_index IS NULL OR CHAR_LENGTH(event_index) > 0),
    CHECK (JSON_TYPE(payload) = 'OBJECT'),
    CHECK (payload_bytes BETWEEN 2 AND 524288),
    CHECK (revision > 0)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;
