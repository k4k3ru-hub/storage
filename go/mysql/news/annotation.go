package news

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"

	api "github.com/k4k3ru-hub/storage/go/api"
)

// BeginAnnotation records an independently versioned classification attempt for a
// complete fetch. Its run/profile pair permits only one running attempt.
// Exact retries succeed; any error requires caller rollback.
//
// Version:
//   - 2026-10-31: Added.
func (s *Store) BeginAnnotation(ctx context.Context, tx *sql.Tx, v AnnotationStart) error {
	const op = "begin news annotation"
	if err := writeDependency(s, ctx, tx, op); err != nil {
		return err
	}
	if err := validateAnnotationStart(v); err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	old, err := s.getAnnotation(ctx, tx, v.ID, true)
	if err == nil {
		if sameAnnotationStart(old.AnnotationStart, v) {
			return nil
		}
		return conflict(op)
	}
	if !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	run, err := s.getFetch(ctx, tx, v.RunID, true)
	if err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	if run.Status != StatusComplete {
		return conflict(op)
	}
	if run.FinishedAt == nil || v.StartedAt.Before(*run.FinishedAt) {
		return invalid(op, "started_at=out_of_range")
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO "+s.table("annotation_sets")+" (annotation_set_id,run_id,profile_key,producer_key,producer_version,document_version,started_at,status) VALUES (?,?,?,?,?,?,?,?)", v.ID[:], v.RunID[:], v.ProfileKey, v.ProducerKey, v.ProducerVersion, v.DocumentVersion, v.StartedAt.UTC(), StatusRunning)
	if err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	return nil
}

// CompleteAnnotation validates exact input coverage and saves the immutable decision
// document with its category/entity projections. Unclassified and classified-empty
// decisions remain distinct. Any error requires caller rollback.
//
// Version:
//   - 2026-10-31: Added.
func (s *Store) CompleteAnnotation(ctx context.Context, tx *sql.Tx, v AnnotationCompletion) error {
	const op = "complete news annotation"
	if err := writeDependency(s, ctx, tx, op); err != nil {
		return err
	}
	if v.ID == (ID{}) || !validTime(v.FinishedAt) {
		return invalid(op, "completion=invalid")
	}
	d, err := decodeDocument(v.DocumentJSON, v.DocumentSHA256)
	if err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	a, err := s.getAnnotation(ctx, tx, v.ID, true)
	if err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	if v.FinishedAt.Before(a.StartedAt) {
		return invalid(op, "finished_at=out_of_range")
	}
	if err := documentMatchesStart(d, a.AnnotationStart); err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	if err := s.verifyCoverage(ctx, tx, a.RunID, d); err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	if a.Status == StatusComplete {
		if !sameTime(a.FinishedAt, &v.FinishedAt) || a.DocumentSHA256 == nil || *a.DocumentSHA256 != v.DocumentSHA256 || !bytes.Equal(a.DocumentJSON, v.DocumentJSON) {
			return conflict(op)
		}
		if err := s.verifyProjections(ctx, tx, a, d); err != nil {
			return fmt.Errorf("failed to %s: %w", op, err)
		}
		return nil
	}
	if a.Status != StatusRunning {
		return conflict(op)
	}
	for _, decision := range d.Decisions {
		for _, c := range decision.Categories {
			_, err := tx.ExecContext(ctx, "INSERT INTO "+s.table("article_categories")+" (annotation_set_id,run_id,article_id,revision_sha256,category_key,method_key,evidence) VALUES (?,?,?,?,?,?,?)", v.ID[:], a.RunID[:], decision.ArticleID[:], decision.RevisionSHA256[:], c.Key, c.MethodKey, []byte(c.Evidence))
			if err != nil {
				return fmt.Errorf("failed to %s: %w", op, err)
			}
		}
		for _, e := range decision.Entities {
			_, err := tx.ExecContext(ctx, "INSERT INTO "+s.table("article_entities")+" (annotation_set_id,run_id,article_id,revision_sha256,entity_namespace,entity_key,method_key,evidence) VALUES (?,?,?,?,?,?,?,?)", v.ID[:], a.RunID[:], decision.ArticleID[:], decision.RevisionSHA256[:], e.Namespace, []byte(e.Key), e.MethodKey, []byte(e.Evidence))
			if err != nil {
				return fmt.Errorf("failed to %s: %w", op, err)
			}
		}
	}
	result, err := tx.ExecContext(ctx, "UPDATE "+s.table("annotation_sets")+" SET status=?,finished_at=?,covered_article_count=?,document_json=?,document_sha256=? WHERE annotation_set_id=? AND status=?", StatusComplete, v.FinishedAt.UTC(), len(d.Decisions), v.DocumentJSON, v.DocumentSHA256[:], v.ID[:], StatusRunning)
	if err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	return expectOne(result, op)
}

// FailAnnotation records classification failure without converting it into an empty
// successful classification. Only running attempts and exact retries are accepted.
// Any error requires caller rollback.
//
// Version:
//   - 2026-10-31: Added.
func (s *Store) FailAnnotation(ctx context.Context, tx *sql.Tx, v Failure) error {
	const op = "fail news annotation"
	if err := writeDependency(s, ctx, tx, op); err != nil {
		return err
	}
	if err := validateFailure(v); err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	a, err := s.getAnnotation(ctx, tx, v.ID, true)
	if err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	if v.FinishedAt.Before(a.StartedAt) {
		return invalid(op, "finished_at=out_of_range")
	}
	if a.Status == StatusFailed && a.ErrorCode == v.ErrorCode && sameTime(a.FinishedAt, &v.FinishedAt) {
		return nil
	}
	if a.Status != StatusRunning {
		return conflict(op)
	}
	result, err := tx.ExecContext(ctx, "UPDATE "+s.table("annotation_sets")+" SET status=?,finished_at=?,error_code=? WHERE annotation_set_id=? AND status=?", StatusFailed, v.FinishedAt.UTC(), v.ErrorCode, v.ID[:], StatusRunning)
	if err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	return expectOne(result, op)
}

// GetAnnotation reads a classification attempt and verifies complete documents,
// exact input coverage and all category/entity projections before returning success.
//
// Version:
//   - 2026-10-31: Added.
func (s *Store) GetAnnotation(ctx context.Context, executor api.Executor, id ID) (Annotation, error) {
	const op = "read news annotation"
	if err := dependency(s, ctx, executor, op); err != nil {
		return Annotation{}, err
	}
	if id == (ID{}) {
		return Annotation{}, invalid(op, "annotation_set_id=empty")
	}
	a, err := s.getAnnotation(ctx, executor, id, false)
	if err != nil {
		return Annotation{}, err
	}
	if a.Status == StatusComplete {
		d, err := decodeDocument(a.DocumentJSON, *a.DocumentSHA256)
		if err != nil {
			return Annotation{}, fmt.Errorf("failed to %s: %w", op, err)
		}
		if err := s.verifyCoverage(ctx, executor, a.RunID, d); err != nil {
			return Annotation{}, fmt.Errorf("failed to %s: %w", op, err)
		}
		if err := s.verifyProjections(ctx, executor, a, d); err != nil {
			return Annotation{}, fmt.Errorf("failed to %s: %w", op, err)
		}
	}
	return a, nil
}

func validateAnnotationStart(v AnnotationStart) error {
	if v.ID == (ID{}) || v.RunID == (ID{}) || v.DocumentVersion != 1 || !validTime(v.StartedAt) {
		return invalid("validate news annotation", "annotation=invalid")
	}
	for _, v := range []string{v.ProfileKey, v.ProducerKey, v.ProducerVersion} {
		if err := token(v, 64, false); err != nil {
			return err
		}
	}
	return nil
}
func sameAnnotationStart(a, b AnnotationStart) bool {
	return a.ID == b.ID && a.RunID == b.RunID && a.ProfileKey == b.ProfileKey && a.ProducerKey == b.ProducerKey && a.ProducerVersion == b.ProducerVersion && a.DocumentVersion == b.DocumentVersion && a.StartedAt.Equal(b.StartedAt)
}
func documentMatchesStart(d Document, a AnnotationStart) error {
	if d.Version != a.DocumentVersion || d.RunID != a.RunID || d.ProfileKey != a.ProfileKey || d.ProducerKey != a.ProducerKey || d.ProducerVersion != a.ProducerVersion {
		return integrity("validate news annotation input")
	}
	return nil
}
func (s *Store) getAnnotation(ctx context.Context, executor api.Executor, id ID, lock bool) (Annotation, error) {
	const op = "read news annotation"
	var a Annotation
	var rawID, runID, hash []byte
	var finished sql.NullTime
	var code sql.NullString
	query := "SELECT annotation_set_id,run_id,profile_key,producer_key,producer_version,document_version,started_at,finished_at,status,covered_article_count,document_json,document_sha256,error_code FROM " + s.table("annotation_sets") + " WHERE annotation_set_id=?"
	if lock {
		query += " FOR UPDATE"
	}
	err := executor.QueryRowContext(ctx, query, id[:]).Scan(&rawID, &runID, &a.ProfileKey, &a.ProducerKey, &a.ProducerVersion, &a.DocumentVersion, &a.StartedAt, &finished, &a.Status, &a.CoveredArticleCount, &a.DocumentJSON, &hash, &code)
	if err != nil {
		return Annotation{}, rowError(err, op)
	}
	if len(rawID) != 16 || !bytes.Equal(rawID, id[:]) || len(runID) != 16 {
		return Annotation{}, integrity(op)
	}
	a.ID = id
	copy(a.RunID[:], runID)
	a.FinishedAt = nullTime(finished)
	a.ErrorCode = code.String
	if hash != nil {
		if len(hash) != 32 {
			return Annotation{}, integrity(op)
		}
		var h Hash
		copy(h[:], hash)
		a.DocumentSHA256 = &h
	}
	if err := validateAnnotationStart(a.AnnotationStart); err != nil {
		return Annotation{}, integrity(op)
	}
	valid := false
	switch a.Status {
	case StatusRunning:
		valid = a.FinishedAt == nil && a.CoveredArticleCount == 0 && a.DocumentJSON == nil && a.DocumentSHA256 == nil && a.ErrorCode == ""
	case StatusFailed:
		valid = a.FinishedAt != nil && validTime(*a.FinishedAt) && !a.FinishedAt.Before(a.StartedAt) && a.CoveredArticleCount == 0 && a.DocumentJSON == nil && a.DocumentSHA256 == nil && a.ErrorCode != "" && token(a.ErrorCode, 64, false) == nil
	case StatusComplete:
		valid = a.FinishedAt != nil && validTime(*a.FinishedAt) && !a.FinishedAt.Before(a.StartedAt) && a.CoveredArticleCount >= 0 && a.CoveredArticleCount <= MaxItems && a.DocumentJSON != nil && a.DocumentSHA256 != nil && a.ErrorCode == ""
	}
	if !valid {
		return Annotation{}, integrity(op)
	}
	if a.Status == StatusComplete {
		d, err := decodeDocument(a.DocumentJSON, *a.DocumentSHA256)
		if err != nil {
			return Annotation{}, fmt.Errorf("failed to %s: %w", op, err)
		}
		if len(d.Decisions) != a.CoveredArticleCount {
			return Annotation{}, integrity(op)
		}
		if err := documentMatchesStart(d, a.AnnotationStart); err != nil {
			return Annotation{}, err
		}
	}
	return a, nil
}
func (s *Store) verifyCoverage(ctx context.Context, executor api.Executor, runID ID, d Document) error {
	run, err := s.getFetch(ctx, executor, runID, false)
	if err != nil {
		return err
	}
	if run.Status != StatusComplete {
		return integrity("verify news annotation coverage")
	}
	items, err := s.readRunItems(ctx, executor, runID)
	if err != nil {
		return err
	}
	if run.ItemCount != len(items) || len(items) != len(d.Decisions) {
		return integrity("verify news annotation coverage")
	}
	for i, item := range items {
		if d.Decisions[i].ArticleID != item.ArticleID || d.Decisions[i].RevisionSHA256 != item.RevisionSHA256 {
			return integrity("verify news annotation coverage")
		}
	}
	return nil
}

type relationValue struct {
	Revision Hash
	Method   string
	Evidence []byte
}

func relationKey(article Hash, namespace, key string) string {
	return article.String() + "\x00" + namespace + "\x00" + key
}
func (s *Store) verifyProjections(ctx context.Context, executor api.Executor, a Annotation, d Document) error {
	const op = "verify news annotation projections"
	categories := map[string]relationValue{}
	entities := map[string]relationValue{}
	for _, v := range d.Decisions {
		for _, c := range v.Categories {
			categories[relationKey(v.ArticleID, "", c.Key)] = relationValue{v.RevisionSHA256, c.MethodKey, c.Evidence}
		}
		for _, e := range v.Entities {
			entities[relationKey(v.ArticleID, e.Namespace, e.Key)] = relationValue{v.RevisionSHA256, e.MethodKey, e.Evidence}
		}
	}
	for _, kind := range []string{"categories", "entities"} {
		target := categories
		columns := "article_id,revision_sha256,run_id,category_key,method_key,evidence"
		if kind == "entities" {
			target = entities
			columns = "article_id,revision_sha256,run_id,entity_namespace,entity_key,method_key,evidence"
		}
		rows, err := executor.QueryContext(ctx, "SELECT "+columns+" FROM "+s.table("article_"+kind)+" WHERE annotation_set_id=? LIMIT 16001", a.ID[:])
		if err != nil {
			return fmt.Errorf("failed to %s: %w", op, err)
		}
		count := 0
		for rows.Next() {
			var article, revision, run, evidence []byte
			var namespace, key, method string
			if kind == "categories" {
				err = rows.Scan(&article, &revision, &run, &key, &method, &evidence)
			} else {
				err = rows.Scan(&article, &revision, &run, &namespace, &key, &method, &evidence)
			}
			if err != nil {
				closeErr := rows.Close()
				return fmt.Errorf("failed to %s: %w", op, errors.Join(err, closeErr))
			}
			if len(article) != 32 || len(revision) != 32 || !bytes.Equal(run, a.RunID[:]) {
				closeErr := rows.Close()
				return fmt.Errorf("failed to %s: %w", op, errors.Join(ErrIntegrity, closeErr))
			}
			var id, hash Hash
			copy(id[:], article)
			copy(hash[:], revision)
			k := relationKey(id, namespace, key)
			expected, ok := target[k]
			max := 4096
			if kind == "entities" {
				max = 8192
			}
			if !ok || hash != expected.Revision || method != expected.Method || !equivalentEvidence(evidence, expected.Evidence, max) {
				closeErr := rows.Close()
				return fmt.Errorf("failed to %s: %w", op, errors.Join(ErrIntegrity, closeErr))
			}
			delete(target, k)
			count++
		}
		if err := rowsError(rows, op); err != nil {
			return err
		}
		if count > MaxItems*MaxRelations || len(target) != 0 {
			return integrity(op)
		}
	}
	return nil
}
