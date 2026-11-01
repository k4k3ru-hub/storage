-- Additive news schedule schema; application-owned migration execution.
CREATE TABLE news_source_schedules (
    source_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    auto_fetch_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    interval_seconds INT UNSIGNED NOT NULL,
    settings_version BIGINT UNSIGNED NOT NULL,
    retry_not_before_at DATETIME(6) NULL,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (source_id),
    KEY ix_news_schedule_enabled (auto_fetch_enabled, source_id),
    FOREIGN KEY (source_id) REFERENCES news_sources(source_id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (auto_fetch_enabled IN (FALSE, TRUE)),
    CHECK (interval_seconds BETWEEN 60 AND 604800),
    CHECK (settings_version > 0),
    CHECK (updated_at >= created_at)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;
