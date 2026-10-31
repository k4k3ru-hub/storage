package news

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	api "github.com/k4k3ru-hub/storage/go/api"
)

// LatestReady selects the latest complete fetch/classification pair for each source.
// It also reports the latest fetch attempt and that attempt's latest classification,
// including failures. Stale means that a newer attempt exists than the selected pair;
// timestamps expose its age without an implicit clock. At most 16 sources are accepted.
// Use a caller-owned read transaction when cross-query consistency is required.
//
// Version:
//   - 2026-10-31: Added.
func (s *Store) LatestReady(ctx context.Context, executor api.Executor, sourceIDs []string, profile string) ([]Ready, error) {
	const op = "select latest ready news inputs"
	if err := dependency(s, ctx, executor, op); err != nil {
		return nil, err
	}
	if len(sourceIDs) == 0 || len(sourceIDs) > MaxPinnedInputs {
		return nil, invalid(op, "sources=out_of_range min_length=1 max_length=16")
	}
	if err := token(profile, 64, false); err != nil {
		return nil, fmt.Errorf("failed to %s: %w", op, err)
	}
	ids := append([]string(nil), sourceIDs...)
	sort.Strings(ids)
	for i, id := range ids {
		if err := token(id, 64, false); err != nil {
			return nil, fmt.Errorf("failed to %s: %w", op, err)
		}
		if i > 0 && id == ids[i-1] {
			return nil, invalid(op, "sources=invalid")
		}
	}
	result := make([]Ready, 0, len(ids))
	for _, sourceID := range ids {
		var present string
		if err := executor.QueryRowContext(ctx, "SELECT source_id FROM "+s.table("sources")+" WHERE source_id=?", sourceID).Scan(&present); err != nil {
			return nil, rowError(err, op)
		}
		r := Ready{SourceID: sourceID}
		var latestID []byte
		err := executor.QueryRowContext(ctx, "SELECT run_id FROM "+s.table("fetch_runs")+" WHERE source_id=? ORDER BY started_at DESC,run_id DESC LIMIT 1", sourceID).Scan(&latestID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, rowError(err, op)
		}
		if err == nil {
			if len(latestID) != 16 {
				return nil, integrity(op)
			}
			var id ID
			copy(id[:], latestID)
			fetch, e := s.GetFetch(ctx, executor, id)
			if e != nil {
				return nil, fmt.Errorf("failed to %s: %w", op, e)
			}
			r.LatestAttempt = &fetch
			var annotationID []byte
			e = executor.QueryRowContext(ctx, "SELECT annotation_set_id FROM "+s.table("annotation_sets")+" WHERE run_id=? AND profile_key=? ORDER BY started_at DESC,annotation_set_id DESC LIMIT 1", id[:], profile).Scan(&annotationID)
			if e != nil && !errors.Is(e, sql.ErrNoRows) {
				return nil, rowError(e, op)
			}
			if e == nil {
				if len(annotationID) != 16 {
					return nil, integrity(op)
				}
				var aid ID
				copy(aid[:], annotationID)
				a, e := s.GetAnnotation(ctx, executor, aid)
				if e != nil {
					return nil, fmt.Errorf("failed to %s: %w", op, e)
				}
				r.LatestAnnotationAttempt = &a
			}
		}
		var selectedRun, selectedAnnotation []byte
		err = executor.QueryRowContext(ctx, "SELECT f.run_id,a.annotation_set_id FROM "+s.table("fetch_runs")+" f JOIN "+s.table("annotation_sets")+" a ON a.run_id=f.run_id WHERE f.source_id=? AND f.status=? AND a.profile_key=? AND a.status=? ORDER BY f.started_at DESC,f.run_id DESC,a.started_at DESC,a.annotation_set_id DESC LIMIT 1", sourceID, StatusComplete, profile, StatusComplete).Scan(&selectedRun, &selectedAnnotation)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, rowError(err, op)
		}
		if err == nil {
			if len(selectedRun) != 16 || len(selectedAnnotation) != 16 {
				return nil, integrity(op)
			}
			var rid, aid ID
			copy(rid[:], selectedRun)
			copy(aid[:], selectedAnnotation)
			f, e := s.GetFetch(ctx, executor, rid)
			if e != nil {
				return nil, fmt.Errorf("failed to %s: %w", op, e)
			}
			a, e := s.GetAnnotation(ctx, executor, aid)
			if e != nil {
				return nil, fmt.Errorf("failed to %s: %w", op, e)
			}
			if f.Status != StatusComplete || a.Status != StatusComplete || a.RunID != f.ID || a.ProfileKey != profile {
				return nil, integrity(op)
			}
			r.SelectedFetch = &f
			r.SelectedAnnotation = &a
			r.Input = &PinnedInput{RunID: rid, AnnotationSetID: aid, ResponseSHA256: *f.ResponseSHA256, DocumentSHA256: *a.DocumentSHA256}
			r.Stale = r.LatestAttempt == nil || r.LatestAttempt.ID != rid || r.LatestAnnotationAttempt == nil || r.LatestAnnotationAttempt.ID != aid
		}
		result = append(result, r)
	}
	return result, nil
}

// ListPinnedObservations reads only explicitly pinned immutable inputs and verifies
// all hashes, documents, projections and records before returning a page. The cursor
// is bound to the normalized pins, profile, filters and ordering. Duplicate run pins
// are rejected. Categories, entities, language and [from,to) publication filters are
// conjunctive; unknown publication times are counted separately and sorted last.
// At most 16,000 observations are scanned, retaining at most Limit+1 matching records.
// Any read failure returns an empty Page, never partial success.
//
// Version:
//   - 2026-10-31: Added.
func (s *Store) ListPinnedObservations(ctx context.Context, executor api.Executor, request ListRequest) (Page, error) {
	const op = "list pinned news observations"
	if err := dependency(s, ctx, executor, op); err != nil {
		return Page{}, err
	}
	r, scope, err := normalizeListRequest(request)
	if err != nil {
		return Page{}, fmt.Errorf("failed to %s: %w", op, err)
	}
	page := Page{Observations: []Observation{}, Scope: scope}
	candidates := make([]Observation, 0, r.Limit+1)
	for _, pin := range r.Inputs {
		fetch, err := s.GetFetch(ctx, executor, pin.RunID)
		if err != nil {
			return Page{}, fmt.Errorf("failed to %s: %w", op, err)
		}
		annotation, err := s.GetAnnotation(ctx, executor, pin.AnnotationSetID)
		if err != nil {
			return Page{}, fmt.Errorf("failed to %s: %w", op, err)
		}
		if fetch.Status != StatusComplete || fetch.ResponseSHA256 == nil || *fetch.ResponseSHA256 != pin.ResponseSHA256 || annotation.Status != StatusComplete || annotation.RunID != pin.RunID || annotation.ProfileKey != r.ProfileKey || annotation.DocumentSHA256 == nil || *annotation.DocumentSHA256 != pin.DocumentSHA256 {
			return Page{}, integrity(op)
		}
		document, err := decodeDocument(annotation.DocumentJSON, pin.DocumentSHA256)
		if err != nil {
			return Page{}, fmt.Errorf("failed to %s: %w", op, err)
		}
		decisions := make(map[Hash]Decision, len(document.Decisions))
		for _, v := range document.Decisions {
			decisions[v.ArticleID] = v
		}
		query := "SELECT i.article_id,i.revision_sha256,i.source_item_key,a.publisher_id,a.identity_namespace,a.identity_value,a.identity_sha256,v.record_version,v.language_tag,v.published_at,v.source_updated_at,v.first_observed_at,v.record_json FROM " + s.table("fetch_run_items") + " i JOIN " + s.table("articles") + " a ON a.article_id=i.article_id JOIN " + s.table("article_revisions") + " v ON v.article_id=i.article_id AND v.revision_sha256=i.revision_sha256 WHERE i.run_id=? ORDER BY i.article_id LIMIT 1001"
		rows, err := executor.QueryContext(ctx, query, pin.RunID[:])
		if err != nil {
			return Page{}, fmt.Errorf("failed to %s: %w", op, err)
		}
		count := 0
		for rows.Next() {
			var o Observation
			var id, revisionHash, identityHash, value []byte
			var namespace, publisher string
			var lang sql.NullString
			var published, updated sql.NullTime
			err := rows.Scan(&id, &revisionHash, &o.SourceItemKey, &publisher, &namespace, &value, &identityHash, &o.Revision.RecordVersion, &lang, &published, &updated, &o.Revision.FirstObservedAt, &o.Revision.RecordJSON)
			if err != nil {
				closeErr := rows.Close()
				return Page{}, fmt.Errorf("failed to %s: %w", op, errors.Join(err, closeErr))
			}
			if len(id) != 32 || len(revisionHash) != 32 || len(o.SourceItemKey) == 0 || len(o.SourceItemKey) > 2048 {
				closeErr := rows.Close()
				return Page{}, fmt.Errorf("failed to %s: %w", op, errors.Join(ErrIntegrity, closeErr))
			}
			copy(o.Revision.ArticleID[:], id)
			copy(o.Revision.SHA256[:], revisionHash)
			o.Revision.LanguageTag = lang.String
			o.Revision.PublishedAt = nullTime(published)
			o.Revision.SourceUpdatedAt = nullTime(updated)
			identity := Identity{PublisherID: publisher, Namespace: namespace, Value: value}
			derived, e := ArticleID(identity)
			hash := SHA256(value)
			decision, found := decisions[o.Revision.ArticleID]
			if e != nil || derived != o.Revision.ArticleID || !bytes.Equal(hash[:], identityHash) || publisher != fetch.PublisherID || !found || decision.RevisionSHA256 != o.Revision.SHA256 || !validTime(o.Revision.FirstObservedAt) {
				closeErr := rows.Close()
				return Page{}, fmt.Errorf("failed to %s: %w", op, errors.Join(ErrIntegrity, closeErr))
			}
			if err := validateRevision(o.Revision); err != nil {
				closeErr := rows.Close()
				return Page{}, fmt.Errorf("failed to %s: %w", op, errors.Join(err, closeErr))
			}
			count++
			o.RunID = pin.RunID
			o.AnnotationSetID = pin.AnnotationSetID
			o.SourceID = fetch.SourceID
			o.PublisherID = fetch.PublisherID
			o.SourceSnapshot = fetch.Snapshot
			o.Decision = decision
			if !matchesNonTime(o, r.Filter) {
				continue
			}
			if o.Revision.PublishedAt == nil {
				page.UnknownPublishedCount++
				if r.Filter.PublishedFrom != nil || r.Filter.PublishedTo != nil {
					continue
				}
			}
			if !matchesTime(o.Revision.PublishedAt, r.Filter) {
				continue
			}
			page.Total++
			if r.After != nil && compareObservationCursor(o, *r.After) <= 0 {
				continue
			}
			index := sort.Search(len(candidates), func(i int) bool { return compareObservation(o, candidates[i]) < 0 })
			if index > r.Limit {
				continue
			}
			candidates = append(candidates, Observation{})
			copy(candidates[index+1:], candidates[index:])
			candidates[index] = o
			if len(candidates) > r.Limit+1 {
				candidates[len(candidates)-1] = Observation{}
				candidates = candidates[:r.Limit+1]
			}
		}
		if err := rowsError(rows, op); err != nil {
			return Page{}, err
		}
		if count != fetch.ItemCount || count > MaxItems {
			return Page{}, integrity(op)
		}
	}
	page.HasMore = len(candidates) > r.Limit
	if page.HasMore {
		candidates = candidates[:r.Limit]
		last := candidates[len(candidates)-1]
		page.Next = &Cursor{Scope: scope, PublishedAt: last.Revision.PublishedAt, ArticleID: last.Revision.ArticleID, RunID: last.RunID}
	}
	page.Observations = candidates
	return page, nil
}

func normalizeListRequest(request ListRequest) (ListRequest, Hash, error) {
	r := request
	if len(r.Inputs) == 0 || len(r.Inputs) > MaxPinnedInputs {
		return ListRequest{}, Hash{}, invalid("validate pinned news request", "inputs=out_of_range min_length=1 max_length=16")
	}
	if r.Limit < 1 || r.Limit > MaxPageSize {
		return ListRequest{}, Hash{}, invalid("validate pinned news request", "limit=out_of_range min_value=1 max_value=100")
	}
	if err := token(r.ProfileKey, 64, false); err != nil {
		return ListRequest{}, Hash{}, err
	}
	if r.Filter.CategoryKey != "" {
		if err := categoryKey(r.Filter.CategoryKey); err != nil {
			return ListRequest{}, Hash{}, err
		}
	} else if r.Filter.IncludeDescendants {
		return ListRequest{}, Hash{}, invalid("validate pinned news request", "category_key=empty")
	}
	if (r.Filter.EntityNamespace == "") != (r.Filter.EntityKey == "") {
		return ListRequest{}, Hash{}, invalid("validate pinned news request", "entity=invalid")
	}
	if r.Filter.EntityNamespace != "" {
		if err := token(r.Filter.EntityNamespace, 64, false); err != nil {
			return ListRequest{}, Hash{}, err
		}
		if err := text(r.Filter.EntityKey, 512, false); err != nil {
			return ListRequest{}, Hash{}, err
		}
	}
	if err := language(r.Filter.LanguageTag); err != nil {
		return ListRequest{}, Hash{}, err
	}
	var err error
	r.Filter.PublishedFrom, err = normalizeTime(r.Filter.PublishedFrom)
	if err != nil {
		return ListRequest{}, Hash{}, err
	}
	r.Filter.PublishedTo, err = normalizeTime(r.Filter.PublishedTo)
	if err != nil {
		return ListRequest{}, Hash{}, err
	}
	if r.Filter.PublishedFrom != nil && r.Filter.PublishedTo != nil && !r.Filter.PublishedTo.After(*r.Filter.PublishedFrom) {
		return ListRequest{}, Hash{}, invalid("validate pinned news request", "publication_range=invalid")
	}
	r.Inputs = append([]PinnedInput(nil), r.Inputs...)
	for _, v := range r.Inputs {
		if v.RunID == (ID{}) || v.AnnotationSetID == (ID{}) || v.ResponseSHA256 == (Hash{}) || v.DocumentSHA256 == (Hash{}) {
			return ListRequest{}, Hash{}, invalid("validate pinned news request", "input=empty")
		}
	}
	sort.Slice(r.Inputs, func(i, j int) bool { return keyOrder(r.Inputs[i].RunID[:], r.Inputs[j].RunID[:]) < 0 })
	for i := 1; i < len(r.Inputs); i++ {
		if r.Inputs[i-1].RunID == r.Inputs[i].RunID {
			return ListRequest{}, Hash{}, invalid("validate pinned news request", "run_id=invalid")
		}
	}
	b, err := json.Marshal(struct {
		Version int
		Inputs  []PinnedInput
		Profile string
		Filter  Filter
		Order   string
	}{1, r.Inputs, r.ProfileKey, r.Filter, "published_desc_null_last_article_asc_run_asc"})
	if err != nil {
		return ListRequest{}, Hash{}, fmt.Errorf("failed to hash pinned news request: %w", err)
	}
	scope := SHA256(b)
	if r.After != nil {
		if r.After.Scope != scope || r.After.ArticleID == (Hash{}) || r.After.RunID == (ID{}) {
			return ListRequest{}, Hash{}, invalid("validate pinned news cursor", "cursor=invalid")
		}
		if _, err := normalizeTime(r.After.PublishedAt); err != nil {
			return ListRequest{}, Hash{}, err
		}
	}
	return r, scope, nil
}
func matchesNonTime(o Observation, f Filter) bool {
	if f.LanguageTag != "" && o.Revision.LanguageTag != f.LanguageTag {
		return false
	}
	if f.CategoryKey != "" {
		found := false
		for _, c := range o.Decision.Categories {
			if c.Key == f.CategoryKey || f.IncludeDescendants && strings.HasPrefix(c.Key, f.CategoryKey+"/") {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if f.EntityNamespace != "" {
		found := false
		for _, e := range o.Decision.Entities {
			if e.Namespace == f.EntityNamespace && e.Key == f.EntityKey {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
func matchesTime(t *time.Time, f Filter) bool {
	if t == nil {
		return f.PublishedFrom == nil && f.PublishedTo == nil
	}
	return (f.PublishedFrom == nil || !t.Before(*f.PublishedFrom)) && (f.PublishedTo == nil || t.Before(*f.PublishedTo))
}
func compareObservation(a, b Observation) int {
	return compareObservationCursor(a, Cursor{PublishedAt: b.Revision.PublishedAt, ArticleID: b.Revision.ArticleID, RunID: b.RunID})
}
func compareObservationCursor(a Observation, b Cursor) int {
	at, bt := a.Revision.PublishedAt, b.PublishedAt
	if at == nil && bt != nil {
		return 1
	}
	if at != nil && bt == nil {
		return -1
	}
	if at != nil && bt != nil {
		if at.After(*bt) {
			return -1
		}
		if at.Before(*bt) {
			return 1
		}
	}
	if c := keyOrder(a.Revision.ArticleID[:], b.ArticleID[:]); c != 0 {
		return c
	}
	return keyOrder(a.RunID[:], b.RunID[:])
}
