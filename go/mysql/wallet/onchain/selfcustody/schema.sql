CREATE TABLE IF NOT EXISTS wallet_onchain_selfcustody_addresses (
    id BIGINT UNSIGNED NOT NULL,
    chain_family VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    address VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    created_at DATETIME(6) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_address_identity (chain_family, address),
    CHECK (id > 0),
    CHECK (CHAR_LENGTH(chain_family) > 0),
    CHECK (CHAR_LENGTH(address) > 0)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

CREATE TABLE IF NOT EXISTS wallet_onchain_selfcustody_links (
    id BIGINT UNSIGNED NOT NULL,
    address_id BIGINT UNSIGNED NOT NULL,
    subject VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    display_name VARCHAR(128) NOT NULL DEFAULT '',
    linked_at DATETIME(6) NOT NULL,
    unlinked_at DATETIME(6) NULL,
    active_address_id BIGINT UNSIGNED GENERATED ALWAYS AS
        (CASE WHEN unlinked_at IS NULL THEN address_id ELSE NULL END) STORED,
    PRIMARY KEY (id),
    UNIQUE KEY uq_active_address (active_address_id),
    KEY idx_subject_links (subject, unlinked_at, id),
    KEY idx_address_history (address_id, linked_at, id),
    FOREIGN KEY (address_id) REFERENCES wallet_onchain_selfcustody_addresses(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (id > 0),
    CHECK (CHAR_LENGTH(subject) > 0),
    CHECK (unlinked_at IS NULL OR unlinked_at >= linked_at)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

CREATE TABLE IF NOT EXISTS wallet_onchain_selfcustody_link_challenges (
    id BIGINT UNSIGNED NOT NULL,
    subject VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    binding_hash BINARY(32) NOT NULL COMMENT 'Application-defined authentication-context hash, never a raw session token',
    chain_family VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    address VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    nonce BINARY(32) NOT NULL COMMENT 'Cryptographically random challenge nonce',
    message BLOB NOT NULL COMMENT 'Exact application-defined ownership proof message bytes',
    created_at DATETIME(6) NOT NULL,
    expires_at DATETIME(6) NOT NULL,
    consumed_at DATETIME(6) NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_challenge_nonce (nonce),
    KEY idx_subject_challenges (subject, created_at, id),
    KEY idx_challenge_expiry (expires_at),
    CHECK (id > 0),
    CHECK (CHAR_LENGTH(subject) > 0),
    CHECK (CHAR_LENGTH(chain_family) > 0),
    CHECK (CHAR_LENGTH(address) > 0),
    CHECK (OCTET_LENGTH(message) > 0),
    CHECK (expires_at > created_at),
    CHECK (consumed_at IS NULL OR (consumed_at >= created_at AND consumed_at < expires_at))
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;
