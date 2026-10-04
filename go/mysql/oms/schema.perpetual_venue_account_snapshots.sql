CREATE TABLE oms_perpetual_venue_account_snapshots (
    account_id BIGINT UNSIGNED NOT NULL,
    venue VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    network VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    trading_account VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    snapshot_version SMALLINT UNSIGNED NOT NULL DEFAULT 1,
    revision BIGINT UNSIGNED NOT NULL DEFAULT 1,
    observed_through_ms BIGINT UNSIGNED NOT NULL
        COMMENT 'Inclusive completed scan boundary; not proof of history completeness',
    snapshot JSON NOT NULL
        COMMENT 'Versioned exact monitoring restart state and necessary overlap evidence',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),

    PRIMARY KEY (account_id, venue, network, trading_account),
    CONSTRAINT ck_oms_perp_venue_account_snapshot_identity CHECK (
        account_id > 0 AND CHAR_LENGTH(venue) > 0
        AND CHAR_LENGTH(network) > 0 AND CHAR_LENGTH(trading_account) > 0
    ),
    CONSTRAINT ck_oms_perp_venue_account_snapshot_state CHECK (
        snapshot_version > 0 AND revision > 0 AND observed_through_ms > 0
        AND JSON_TYPE(snapshot) = 'OBJECT'
    )
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;
