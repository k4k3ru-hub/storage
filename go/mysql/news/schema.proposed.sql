-- DESIGN REVIEW ONLY. Not an installed migration or an executable Store schema.
-- Target: MySQL 8.4 / InnoDB; UTC DATETIME(6); application-owned table prefix.
-- No publisher/category seeds, network rules, migration execution or deletion jobs.
-- See README.md for identity, hashing, transaction and snapshot invariants.

CREATE TABLE news_publishers (
    publisher_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    display_name VARCHAR(256) NOT NULL,
    homepage_url VARCHAR(2048) NOT NULL,
    created_at DATETIME(6) NOT NULL,
    PRIMARY KEY (publisher_id),
    CHECK (CHAR_LENGTH(publisher_id) > 0),
    CHECK (CHAR_LENGTH(display_name) > 0),
    CHECK (CHAR_LENGTH(homepage_url) > 0)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

CREATE TABLE news_sources (
    source_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    publisher_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    display_name VARCHAR(256) NOT NULL,
    endpoint_url VARCHAR(2048) NOT NULL,
    format_key VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    language_hint VARCHAR(35) CHARACTER SET ascii COLLATE ascii_bin NULL,
    registration_version BIGINT UNSIGNED NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (source_id),
    UNIQUE KEY uq_news_source_publisher (source_id, publisher_id),
    KEY ix_news_source_publisher (publisher_id, enabled, source_id),
    FOREIGN KEY (publisher_id) REFERENCES news_publishers(publisher_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (CHAR_LENGTH(source_id) > 0),
    CHECK (CHAR_LENGTH(display_name) > 0),
    CHECK (CHAR_LENGTH(endpoint_url) > 0),
    CHECK (CHAR_LENGTH(format_key) > 0),
    CHECK (language_hint IS NULL OR CHAR_LENGTH(language_hint) > 0),
    CHECK (registration_version > 0),
    CHECK (updated_at >= created_at)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

CREATE TABLE news_fetch_runs (
    run_id BINARY(16) NOT NULL,
    source_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    publisher_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    registration_version BIGINT UNSIGNED NOT NULL,
    parser_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    parser_version VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    source_snapshot JSON NOT NULL COMMENT 'Reviewed public configuration at fetch start; no credentials',
    started_at DATETIME(6) NOT NULL,
    finished_at DATETIME(6) NULL,
    status VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    item_count INT UNSIGNED NOT NULL DEFAULT 0,
    response_sha256 BINARY(32) NULL,
    error_code VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    active_source_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin
        GENERATED ALWAYS AS (CASE WHEN status = 'running' THEN source_id ELSE NULL END) STORED,
    PRIMARY KEY (run_id),
    UNIQUE KEY uq_news_fetch_publisher (run_id, publisher_id),
    UNIQUE KEY uq_news_fetch_active_source (active_source_id),
    KEY ix_news_fetch_attempt (source_id, started_at, run_id),
    KEY ix_news_fetch_success (source_id, status, started_at, run_id),
    FOREIGN KEY (source_id, publisher_id) REFERENCES news_sources(source_id, publisher_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (registration_version > 0),
    CHECK (CHAR_LENGTH(parser_key) > 0 AND CHAR_LENGTH(parser_version) > 0),
    CHECK (JSON_TYPE(source_snapshot) = 'OBJECT' AND OCTET_LENGTH(source_snapshot) <= 16384),
    CHECK (item_count <= 1000),
    CHECK (
        (status = 'running' AND finished_at IS NULL AND item_count = 0 AND response_sha256 IS NULL AND error_code IS NULL)
        OR (status = 'complete' AND finished_at IS NOT NULL AND finished_at >= started_at AND response_sha256 IS NOT NULL AND error_code IS NULL)
        OR (status = 'failed' AND finished_at IS NOT NULL AND finished_at >= started_at AND item_count = 0 AND error_code IS NOT NULL AND CHAR_LENGTH(error_code) > 0)
    )
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

CREATE TABLE news_articles (
    article_id BINARY(32) NOT NULL,
    publisher_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    identity_namespace VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    identity_value VARBINARY(2048) NOT NULL,
    identity_sha256 BINARY(32) NOT NULL,
    first_observed_at DATETIME(6) NOT NULL,
    PRIMARY KEY (article_id),
    UNIQUE KEY uq_news_article_identity (publisher_id, identity_namespace, identity_sha256),
    UNIQUE KEY uq_news_article_publisher (article_id, publisher_id),
    FOREIGN KEY (publisher_id) REFERENCES news_publishers(publisher_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (CHAR_LENGTH(identity_namespace) > 0),
    CHECK (OCTET_LENGTH(identity_value) > 0)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

CREATE TABLE news_article_revisions (
    article_id BINARY(32) NOT NULL,
    revision_sha256 BINARY(32) NOT NULL,
    record_version SMALLINT UNSIGNED NOT NULL,
    language_tag VARCHAR(35) CHARACTER SET ascii COLLATE ascii_bin NULL,
    published_at DATETIME(6) NULL,
    source_updated_at DATETIME(6) NULL,
    first_observed_at DATETIME(6) NOT NULL,
    record_json MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL
        COMMENT 'Exact normalized JSON bytes; title, URL, source summary and optional source metadata, not article body',
    PRIMARY KEY (article_id, revision_sha256),
    KEY ix_news_revision_publication (published_at, article_id, revision_sha256),
    KEY ix_news_revision_language (language_tag, published_at, article_id),
    FOREIGN KEY (article_id) REFERENCES news_articles(article_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (record_version > 0),
    CHECK (language_tag IS NULL OR CHAR_LENGTH(language_tag) > 0),
    CHECK (OCTET_LENGTH(record_json) BETWEEN 2 AND 16384),
    CHECK (JSON_VALID(record_json)),
    CHECK (JSON_TYPE(record_json) = 'OBJECT')
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

CREATE TABLE news_fetch_run_items (
    run_id BINARY(16) NOT NULL,
    article_id BINARY(32) NOT NULL,
    revision_sha256 BINARY(32) NOT NULL,
    publisher_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    source_item_key VARBINARY(2048) NOT NULL COMMENT 'Source-local GUID/ID or explicit URL fallback',
    PRIMARY KEY (run_id, article_id),
    UNIQUE KEY uq_news_run_revision (run_id, article_id, revision_sha256),
    KEY ix_news_run_item_article (article_id, run_id),
    FOREIGN KEY (run_id, publisher_id) REFERENCES news_fetch_runs(run_id, publisher_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (article_id, publisher_id) REFERENCES news_articles(article_id, publisher_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (article_id, revision_sha256) REFERENCES news_article_revisions(article_id, revision_sha256) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (OCTET_LENGTH(source_item_key) > 0)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

CREATE TABLE news_categories (
    category_key VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Immutable path key, e.g. finance/crypto; values are application registrations',
    parent_key VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NULL,
    display_name VARCHAR(256) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at DATETIME(6) NOT NULL,
    PRIMARY KEY (category_key),
    KEY ix_news_category_parent (parent_key, category_key),
    FOREIGN KEY (parent_key) REFERENCES news_categories(category_key) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (CHAR_LENGTH(category_key) > 0),
    CHECK (CHAR_LENGTH(display_name) > 0),
    CHECK (parent_key IS NULL OR (CHAR_LENGTH(parent_key) > 0 AND parent_key <> category_key))
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

-- A separately versioned set pins all category/entity decisions for ONE complete fetch run.
-- Reclassification creates a new set without contacting the publisher or changing source revisions.
CREATE TABLE news_annotation_sets (
    annotation_set_id BINARY(16) NOT NULL,
    run_id BINARY(16) NOT NULL,
    profile_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    producer_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    producer_version VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    document_version SMALLINT UNSIGNED NOT NULL,
    started_at DATETIME(6) NOT NULL,
    finished_at DATETIME(6) NULL,
    status VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    covered_article_count INT UNSIGNED NOT NULL DEFAULT 0,
    document_json MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL
        COMMENT 'Exact JSON decision set, including explicit empty/unclassified decisions and evidence',
    document_sha256 BINARY(32) NULL,
    error_code VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    active_marker TINYINT GENERATED ALWAYS AS (CASE WHEN status = 'running' THEN 1 ELSE NULL END) STORED,
    PRIMARY KEY (annotation_set_id),
    UNIQUE KEY uq_news_annotation_run (annotation_set_id, run_id),
    UNIQUE KEY uq_news_annotation_active (run_id, profile_key, active_marker),
    KEY ix_news_annotation_success (run_id, profile_key, status, started_at, annotation_set_id),
    FOREIGN KEY (run_id) REFERENCES news_fetch_runs(run_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (CHAR_LENGTH(profile_key) > 0 AND CHAR_LENGTH(producer_key) > 0 AND CHAR_LENGTH(producer_version) > 0),
    CHECK (document_version > 0 AND covered_article_count <= 1000),
    CHECK (document_json IS NULL OR (OCTET_LENGTH(document_json) BETWEEN 2 AND 2097152 AND JSON_VALID(document_json))),
    CHECK (document_json IS NULL OR JSON_TYPE(document_json) = 'OBJECT'),
    CHECK (
        (status = 'running' AND finished_at IS NULL AND covered_article_count = 0 AND document_json IS NULL AND document_sha256 IS NULL AND error_code IS NULL)
        OR (status = 'complete' AND finished_at IS NOT NULL AND finished_at >= started_at AND document_json IS NOT NULL AND document_sha256 IS NOT NULL AND error_code IS NULL)
        OR (status = 'failed' AND finished_at IS NOT NULL AND finished_at >= started_at AND covered_article_count = 0 AND document_json IS NULL AND document_sha256 IS NULL AND error_code IS NOT NULL AND CHAR_LENGTH(error_code) > 0)
    )
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

CREATE TABLE news_article_categories (
    annotation_set_id BINARY(16) NOT NULL,
    run_id BINARY(16) NOT NULL,
    article_id BINARY(32) NOT NULL,
    revision_sha256 BINARY(32) NOT NULL,
    category_key VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    method_key VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    evidence JSON NOT NULL,
    PRIMARY KEY (annotation_set_id, article_id, category_key),
    KEY ix_news_article_category (category_key, annotation_set_id, article_id),
    FOREIGN KEY (annotation_set_id, run_id) REFERENCES news_annotation_sets(annotation_set_id, run_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (run_id, article_id, revision_sha256) REFERENCES news_fetch_run_items(run_id, article_id, revision_sha256) ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (category_key) REFERENCES news_categories(category_key) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (CHAR_LENGTH(method_key) > 0),
    CHECK (JSON_TYPE(evidence) = 'OBJECT' AND OCTET_LENGTH(evidence) <= 4096)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;

CREATE TABLE news_article_entities (
    annotation_set_id BINARY(16) NOT NULL,
    run_id BINARY(16) NOT NULL,
    article_id BINARY(32) NOT NULL,
    revision_sha256 BINARY(32) NOT NULL,
    entity_namespace VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    entity_key VARBINARY(512) NOT NULL,
    method_key VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    evidence JSON NOT NULL COMMENT 'Application-owned relation/scope evidence; storage does not infer its meaning',
    PRIMARY KEY (annotation_set_id, article_id, entity_namespace, entity_key),
    KEY ix_news_article_entity (entity_namespace, entity_key, annotation_set_id, article_id),
    FOREIGN KEY (annotation_set_id, run_id) REFERENCES news_annotation_sets(annotation_set_id, run_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    FOREIGN KEY (run_id, article_id, revision_sha256) REFERENCES news_fetch_run_items(run_id, article_id, revision_sha256) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CHECK (CHAR_LENGTH(entity_namespace) > 0),
    CHECK (OCTET_LENGTH(entity_key) > 0),
    CHECK (CHAR_LENGTH(method_key) > 0),
    CHECK (JSON_TYPE(evidence) = 'OBJECT' AND OCTET_LENGTH(evidence) <= 8192)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;
