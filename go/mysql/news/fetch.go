package news

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	api "github.com/k4k3ru-hub/storage/go/api"
)

// BeginFetch records an explicit fetch attempt and freezes its public source configuration.
// The source must be enabled at the specified registration version. Running attempts
// are serialized by a database unique constraint; recovery requires explicit failure.
// Any error requires the caller to roll back the transaction.
//
// Version:
//   - 2026-10-31: Added.
func (s *Store) BeginFetch(ctx context.Context, tx *sql.Tx, v FetchStart) error {
	const op = "begin news fetch"
	if err := writeDependency(s, ctx, tx, op); err != nil {
		return err
	}
	if err := validateFetchStart(v); err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	old, err := s.getFetch(ctx, tx, v.ID, true)
	if err == nil {
		if sameFetchStart(old.FetchStart, v) {
			return nil
		}
		return conflict(op)
	}
	if !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	source, err := s.getSource(ctx, tx, v.SourceID, true)
	if err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	if !source.Enabled || source.RegistrationVersion != v.RegistrationVersion {
		return conflict(op)
	}
	var name, homepage string
	if err := tx.QueryRowContext(ctx, "SELECT display_name,homepage_url FROM "+s.table("publishers")+" WHERE publisher_id=?", source.PublisherID).Scan(&name, &homepage); err != nil {
		return rowError(err, op)
	}
	snapshot := SourceSnapshot{SourceID: source.ID, PublisherID: source.PublisherID, PublisherName: name, PublisherHomepage: homepage, DisplayName: source.DisplayName, EndpointURL: source.EndpointURL, FormatKey: source.FormatKey, LanguageHint: source.LanguageHint, RegistrationVersion: source.RegistrationVersion}
	b, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	if len(b) > 16384 {
		return invalid(op, "source_snapshot=too_long max_length=16384")
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO "+s.table("fetch_runs")+" (run_id,source_id,publisher_id,registration_version,parser_key,parser_version,source_snapshot,started_at,status) VALUES (?,?,?,?,?,?,?,?,?)", v.ID[:], v.SourceID, source.PublisherID, v.RegistrationVersion, v.ParserKey, v.ParserVersion, b, v.StartedAt.UTC(), StatusRunning)
	if err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	return nil
}

// CompleteFetch atomically stores immutable articles, revisions and run observations.
// First-observed times for new rows use the caller's explicit FinishedAt. Exact
// completion retries succeed, including after an uncertain Commit response.
// Any error requires rollback; the Store never commits, rolls back or retries.
//
// Version:
//   - 2026-10-31: Added.
func (s *Store) CompleteFetch(ctx context.Context, tx *sql.Tx, v FetchCompletion) error {
	const op = "complete news fetch"
	if err := writeDependency(s, ctx, tx, op); err != nil {
		return err
	}
	if v.ID == (ID{}) || !validTime(v.FinishedAt) || v.ResponseSHA256 == (Hash{}) {
		return invalid(op, "completion=invalid")
	}
	items, err := normalizeItems(v.Items)
	if err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	run, err := s.getFetch(ctx, tx, v.ID, true)
	if err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	if v.FinishedAt.Before(run.StartedAt) {
		return invalid(op, "finished_at=out_of_range")
	}
	for _, item := range items {
		if item.Identity.PublisherID != run.PublisherID {
			return conflict(op)
		}
	}
	if run.Status == StatusComplete {
		if !sameTime(run.FinishedAt, &v.FinishedAt) || run.ResponseSHA256 == nil || *run.ResponseSHA256 != v.ResponseSHA256 || run.ItemCount != len(items) {
			return conflict(op)
		}
		stored, err := s.readRunItems(ctx, tx, v.ID)
		if err != nil {
			return fmt.Errorf("failed to %s: %w", op, err)
		}
		if len(stored) != len(items) {
			return integrity(op)
		}
		for i, item := range items {
			if stored[i].ArticleID != item.Revision.ArticleID || stored[i].RevisionSHA256 != item.Revision.SHA256 || !bytes.Equal(stored[i].SourceItemKey, item.SourceItemKey) {
				return conflict(op)
			}
			if err := s.checkStoredItem(ctx, tx, item); err != nil {
				return fmt.Errorf("failed to %s: %w", op, err)
			}
		}
		return nil
	}
	if run.Status != StatusRunning {
		return conflict(op)
	}
	for _, item := range items {
		id := item.Revision.ArticleID
		identityHash := SHA256(item.Identity.Value)
		_, err := tx.ExecContext(ctx, "INSERT INTO "+s.table("articles")+" (article_id,publisher_id,identity_namespace,identity_value,identity_sha256,first_observed_at) VALUES (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE article_id=article_id", id[:], item.Identity.PublisherID, item.Identity.Namespace, item.Identity.Value, identityHash[:], v.FinishedAt.UTC())
		if err != nil {
			return fmt.Errorf("failed to %s: %w", op, err)
		}
		if err := s.checkIdentity(ctx, tx, item.Identity, id); err != nil {
			return fmt.Errorf("failed to %s: %w", op, err)
		}
		r := item.Revision
		_, err = tx.ExecContext(ctx, "INSERT INTO "+s.table("article_revisions")+" (article_id,revision_sha256,record_version,language_tag,published_at,source_updated_at,first_observed_at,record_json) VALUES (?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE revision_sha256=revision_sha256", id[:], r.SHA256[:], r.RecordVersion, nullableString(r.LanguageTag), r.PublishedAt, r.SourceUpdatedAt, v.FinishedAt.UTC(), r.RecordJSON)
		if err != nil {
			return fmt.Errorf("failed to %s: %w", op, err)
		}
		stored, err := s.GetRevision(ctx, tx, id, r.SHA256)
		if err != nil {
			return fmt.Errorf("failed to %s: %w", op, err)
		}
		if !sameRevisionContent(stored, r) {
			return integrity(op)
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO "+s.table("fetch_run_items")+" (run_id,article_id,revision_sha256,publisher_id,source_item_key) VALUES (?,?,?,?,?)", v.ID[:], id[:], r.SHA256[:], run.PublisherID, item.SourceItemKey)
		if err != nil {
			return fmt.Errorf("failed to %s: %w", op, err)
		}
	}
	result, err := tx.ExecContext(ctx, "UPDATE "+s.table("fetch_runs")+" SET status=?,finished_at=?,item_count=?,response_sha256=? WHERE run_id=? AND status=?", StatusComplete, v.FinishedAt.UTC(), len(items), v.ResponseSHA256[:], v.ID[:], StatusRunning)
	if err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	return expectOne(result, op)
}

// FailFetch records an explicit failed or abandoned attempt without erasing prior runs.
// Only a running attempt or an exact failure retry is accepted. Any error requires rollback.
//
// Version:
//   - 2026-10-31: Added.
func (s *Store) FailFetch(ctx context.Context, tx *sql.Tx, v Failure) error {
	const op = "fail news fetch"
	if err := writeDependency(s, ctx, tx, op); err != nil {
		return err
	}
	if err := validateFailure(v); err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	run, err := s.getFetch(ctx, tx, v.ID, true)
	if err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	if v.FinishedAt.Before(run.StartedAt) {
		return invalid(op, "finished_at=out_of_range")
	}
	if run.Status == StatusFailed && run.ErrorCode == v.ErrorCode && sameTime(run.FinishedAt, &v.FinishedAt) {
		return nil
	}
	if run.Status != StatusRunning {
		return conflict(op)
	}
	result, err := tx.ExecContext(ctx, "UPDATE "+s.table("fetch_runs")+" SET status=?,finished_at=?,error_code=? WHERE run_id=? AND status=?", StatusFailed, v.FinishedAt.UTC(), v.ErrorCode, v.ID[:], StatusRunning)
	if err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	return expectOne(result, op)
}

// GetFetch reads a fetch attempt by ID and checks its state and public snapshot.
// ErrNotFound distinguishes an unknown ID from failed acquisition and database errors.
//
// Version:
//   - 2026-10-31: Added.
func (s *Store) GetFetch(ctx context.Context, executor api.Executor, id ID) (Fetch, error) {
	if err := dependency(s, ctx, executor, "read news fetch"); err != nil {
		return Fetch{}, err
	}
	if id == (ID{}) {
		return Fetch{}, invalid("read news fetch", "run_id=empty")
	}
	return s.getFetch(ctx, executor, id, false)
}

// GetRevision reads exact immutable record bytes and verifies their hash and projections.
//
// Version:
//   - 2026-10-31: Added.
func (s *Store) GetRevision(ctx context.Context, executor api.Executor, articleID, revisionHash Hash) (Revision, error) {
	const op = "read news revision"
	if err := dependency(s, ctx, executor, op); err != nil {
		return Revision{}, err
	}
	if articleID == (Hash{}) || revisionHash == (Hash{}) {
		return Revision{}, invalid(op, "revision=empty")
	}
	var v Revision
	var id, hash []byte
	var lang sql.NullString
	var published, updated sql.NullTime
	err := executor.QueryRowContext(ctx, "SELECT article_id,revision_sha256,record_version,language_tag,published_at,source_updated_at,first_observed_at,record_json FROM "+s.table("article_revisions")+" WHERE article_id=? AND revision_sha256=?", articleID[:], revisionHash[:]).Scan(&id, &hash, &v.RecordVersion, &lang, &published, &updated, &v.FirstObservedAt, &v.RecordJSON)
	if err != nil {
		return Revision{}, rowError(err, op)
	}
	if len(id) != 32 || len(hash) != 32 {
		return Revision{}, integrity(op)
	}
	copy(v.ArticleID[:], id)
	copy(v.SHA256[:], hash)
	v.LanguageTag = lang.String
	v.PublishedAt = nullTime(published)
	v.SourceUpdatedAt = nullTime(updated)
	if v.ArticleID != articleID || v.SHA256 != revisionHash || !validTime(v.FirstObservedAt) {
		return Revision{}, integrity(op)
	}
	if err := validateRevision(v); err != nil {
		return Revision{}, fmt.Errorf("failed to %s: %w", op, err)
	}
	return v, nil
}

func validateFetchStart(v FetchStart) error {
	if v.ID == (ID{}) || v.RegistrationVersion == 0 || !validTime(v.StartedAt) {
		return invalid("validate news fetch", "fetch=invalid")
	}
	for _, v := range []string{v.SourceID, v.ParserKey, v.ParserVersion} {
		if err := token(v, 64, false); err != nil {
			return err
		}
	}
	return nil
}
func sameFetchStart(a, b FetchStart) bool {
	return a.ID == b.ID && a.SourceID == b.SourceID && a.RegistrationVersion == b.RegistrationVersion && a.ParserKey == b.ParserKey && a.ParserVersion == b.ParserVersion && a.StartedAt.Equal(b.StartedAt)
}
func validateFailure(v Failure) error {
	if v.ID == (ID{}) || !validTime(v.FinishedAt) {
		return invalid("validate news failure", "failure=invalid")
	}
	return token(v.ErrorCode, 64, false)
}
func nullTime(v sql.NullTime) *time.Time {
	if !v.Valid {
		return nil
	}
	t := v.Time.UTC()
	return &t
}
func (s *Store) getFetch(ctx context.Context, executor api.Executor, id ID, lock bool) (Fetch, error) {
	const op = "read news fetch"
	var v Fetch
	var rawID, response []byte
	var finished sql.NullTime
	var code sql.NullString
	query := "SELECT run_id,source_id,publisher_id,registration_version,parser_key,parser_version,source_snapshot,started_at,finished_at,status,item_count,response_sha256,error_code FROM " + s.table("fetch_runs") + " WHERE run_id=?"
	if lock {
		query += " FOR UPDATE"
	}
	err := executor.QueryRowContext(ctx, query, id[:]).Scan(&rawID, &v.SourceID, &v.PublisherID, &v.RegistrationVersion, &v.ParserKey, &v.ParserVersion, &v.SnapshotJSON, &v.StartedAt, &finished, &v.Status, &v.ItemCount, &response, &code)
	if err != nil {
		return Fetch{}, rowError(err, op)
	}
	if len(rawID) != 16 || !bytes.Equal(rawID, id[:]) {
		return Fetch{}, integrity(op)
	}
	v.ID = id
	v.FinishedAt = nullTime(finished)
	v.ErrorCode = code.String
	if response != nil {
		if len(response) != 32 {
			return Fetch{}, integrity(op)
		}
		var h Hash
		copy(h[:], response)
		v.ResponseSHA256 = &h
	}
	if err := validateFetchStart(v.FetchStart); err != nil {
		return Fetch{}, integrity(op)
	}
	if len(v.SnapshotJSON) > 16384 {
		return Fetch{}, integrity(op)
	}
	if err := decodeStrict(v.SnapshotJSON, &v.Snapshot); err != nil {
		return Fetch{}, integrity(op)
	}
	if v.Snapshot.SourceID != v.SourceID || v.Snapshot.PublisherID != v.PublisherID || v.Snapshot.RegistrationVersion != v.RegistrationVersion {
		return Fetch{}, integrity(op)
	}
	if err := validateSnapshot(v.Snapshot); err != nil {
		return Fetch{}, integrity(op)
	}
	valid := false
	switch v.Status {
	case StatusRunning:
		valid = v.FinishedAt == nil && v.ItemCount == 0 && v.ResponseSHA256 == nil && v.ErrorCode == ""
	case StatusComplete:
		valid = v.FinishedAt != nil && validTime(*v.FinishedAt) && !v.FinishedAt.Before(v.StartedAt) && v.ItemCount >= 0 && v.ItemCount <= MaxItems && v.ResponseSHA256 != nil && v.ErrorCode == ""
	case StatusFailed:
		valid = v.FinishedAt != nil && validTime(*v.FinishedAt) && !v.FinishedAt.Before(v.StartedAt) && v.ItemCount == 0 && v.ResponseSHA256 == nil && v.ErrorCode != "" && token(v.ErrorCode, 64, false) == nil
	}
	if !valid {
		return Fetch{}, integrity(op)
	}
	return v, nil
}
func validateSnapshot(v SourceSnapshot) error {
	for _, v := range []string{v.SourceID, v.PublisherID} {
		if err := token(v, 64, false); err != nil {
			return err
		}
	}
	for _, v := range []string{v.PublisherName, v.DisplayName} {
		if err := text(v, 256, false); err != nil {
			return err
		}
	}
	for _, v := range []string{v.EndpointURL, v.PublisherHomepage} {
		if _, err := CanonicalURL(v); err != nil {
			return err
		}
	}
	if err := token(v.FormatKey, 32, false); err != nil {
		return err
	}
	return language(v.LanguageHint)
}
func normalizeItems(input []FetchItem) ([]FetchItem, error) {
	if len(input) > MaxItems {
		return nil, invalid("validate news fetch items", "items=too_long max_length=1000")
	}
	items := append([]FetchItem(nil), input...)
	for _, v := range items {
		id, err := ArticleID(v.Identity)
		if err != nil {
			return nil, err
		}
		if id != v.Revision.ArticleID {
			return nil, integrity("validate news article identity")
		}
		if len(v.SourceItemKey) == 0 || len(v.SourceItemKey) > 2048 {
			return nil, invalid("validate news fetch items", "source_item_key=out_of_range")
		}
		if err := validateRevision(v.Revision); err != nil {
			return nil, err
		}
	}
	sort.Slice(items, func(i, j int) bool {
		return keyOrder(items[i].Revision.ArticleID[:], items[j].Revision.ArticleID[:]) < 0
	})
	out := make([]FetchItem, 0, len(items))
	for _, v := range items {
		if len(out) > 0 && out[len(out)-1].Revision.ArticleID == v.Revision.ArticleID {
			a := out[len(out)-1]
			if a.Identity.PublisherID != v.Identity.PublisherID || a.Identity.Namespace != v.Identity.Namespace || !bytes.Equal(a.Identity.Value, v.Identity.Value) || !bytes.Equal(a.SourceItemKey, v.SourceItemKey) || !sameRevisionContent(a.Revision, v.Revision) {
				return nil, conflict("validate duplicate news observation")
			}
			continue
		}
		out = append(out, v)
	}
	return out, nil
}
func sameRevisionContent(a, b Revision) bool {
	return a.ArticleID == b.ArticleID && a.SHA256 == b.SHA256 && a.RecordVersion == b.RecordVersion && a.LanguageTag == b.LanguageTag && sameTime(a.PublishedAt, b.PublishedAt) && sameTime(a.SourceUpdatedAt, b.SourceUpdatedAt) && bytes.Equal(a.RecordJSON, b.RecordJSON)
}
func (s *Store) checkIdentity(ctx context.Context, executor api.Executor, identity Identity, id Hash) error {
	var publisher, namespace string
	var value, hash []byte
	err := executor.QueryRowContext(ctx, "SELECT publisher_id,identity_namespace,identity_value,identity_sha256 FROM "+s.table("articles")+" WHERE article_id=?", id[:]).Scan(&publisher, &namespace, &value, &hash)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return integrity("verify news article identity")
		}
		return rowError(err, "verify news article identity")
	}
	expected := SHA256(identity.Value)
	if publisher != identity.PublisherID || namespace != identity.Namespace || !bytes.Equal(value, identity.Value) || !bytes.Equal(hash, expected[:]) {
		return integrity("verify news article identity")
	}
	return nil
}
func (s *Store) checkStoredItem(ctx context.Context, executor api.Executor, v FetchItem) error {
	if err := s.checkIdentity(ctx, executor, v.Identity, v.Revision.ArticleID); err != nil {
		return err
	}
	stored, err := s.GetRevision(ctx, executor, v.Revision.ArticleID, v.Revision.SHA256)
	if err != nil {
		return err
	}
	if !sameRevisionContent(stored, v.Revision) {
		return integrity("verify news revision retry")
	}
	return nil
}

type runItem struct {
	ArticleID, RevisionSHA256 Hash
	SourceItemKey             []byte
}

func (s *Store) readRunItems(ctx context.Context, executor api.Executor, id ID) ([]runItem, error) {
	const op = "read news fetch items"
	rows, err := executor.QueryContext(ctx, "SELECT article_id,revision_sha256,source_item_key FROM "+s.table("fetch_run_items")+" WHERE run_id=? ORDER BY article_id LIMIT 1001", id[:])
	if err != nil {
		return nil, fmt.Errorf("failed to %s: %w", op, err)
	}
	items := []runItem{}
	for rows.Next() {
		var v runItem
		var a, h []byte
		if err := rows.Scan(&a, &h, &v.SourceItemKey); err != nil {
			return nil, fmt.Errorf("failed to %s: %w", op, errors.Join(err, rows.Close()))
		}
		if len(a) != 32 || len(h) != 32 || len(v.SourceItemKey) == 0 || len(v.SourceItemKey) > 2048 {
			return nil, fmt.Errorf("failed to %s: %w", op, errors.Join(ErrIntegrity, rows.Close()))
		}
		copy(v.ArticleID[:], a)
		copy(v.RevisionSHA256[:], h)
		items = append(items, v)
	}
	if err := rowsError(rows, op); err != nil {
		return nil, err
	}
	if len(items) > MaxItems {
		return nil, integrity(op)
	}
	return items, nil
}
