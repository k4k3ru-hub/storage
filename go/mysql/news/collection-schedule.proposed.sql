-- REVIEW DRAFT: 2026-11-01. Not embedded by Store.Schema() or an application migration.
-- Additive to schema.sql; MySQL 8.4 / InnoDB; UTC DATETIME(6).
-- The application owns migration execution, approved sources, scheduling and HTTP.
-- Apply the application's prefix to both news_source_schedules and news_sources.
-- See COLLECTION_SCHEDULE.md for due calculation, locking and failure semantics.

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
