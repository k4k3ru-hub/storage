package news

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	api "github.com/k4k3ru-hub/storage/go/api"
)

// mysqlFixture accepts only an explicitly opted-in disposable loopback database.
// It creates and removes its own unique prefixed tables, never shared tables.
func mysqlFixture(t *testing.T) (*Store, *sql.DB) {
	t.Helper()
	dsn := os.Getenv("K4K3RU_NEWS_TEST_DSN")
	if dsn == "" {
		t.Skip("set K4K3RU_NEWS_TEST_DSN and K4K3RU_NEWS_TEST_ISOLATED=1 for a disposable MySQL 8.4 database")
	}
	if os.Getenv("K4K3RU_NEWS_TEST_ISOLATED") != "1" {
		t.Fatal("explicit isolated-database acknowledgement is required")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal("invalid isolated test DSN")
	}
	host, _, err := net.SplitHostPort(cfg.Addr)
	if err != nil || cfg.Net != "tcp" || (host != "127.0.0.1" && host != "::1") || cfg.DBName != "news_storage_test" {
		t.Fatal("tests require loopback TCP and database news_storage_test")
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	cfg.MultiStatements = true
	cfg.Timeout = 5 * time.Second
	cfg.ReadTimeout = 10 * time.Second
	cfg.WriteTimeout = 10 * time.Second
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	cfg.Params["innodb_lock_wait_timeout"] = "3"
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal("open isolated test database:", err)
	}
	db.SetMaxOpenConns(8)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal("connect isolated test database:", err)
	}
	var version string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(version, "8.4.") {
		t.Fatal("tests require MySQL 8.4")
	}
	s, err := NewStore(fmt.Sprintf("nt_%x_", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, name := range []string{"source_schedules", "article_entities", "article_categories", "annotation_sets", "categories", "fetch_run_items", "article_revisions", "articles", "fetch_runs", "sources", "publishers"} {
			if _, err := db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+s.table(name)); err != nil {
				t.Error(err)
			}
		}
	})
	ddl, err := s.Schema()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		t.Fatal("apply isolated schema:", err)
	}
	return s, db
}
func transact(t *testing.T, db *sql.DB, fn func(*sql.Tx) error) error {
	t.Helper()
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	return tx.Commit()
}
func mustTransact(t *testing.T, db *sql.DB, fn func(*sql.Tx) error) {
	t.Helper()
	if err := transact(t, db, fn); err != nil {
		t.Fatal(err)
	}
}
func seedNews(t *testing.T, s *Store, db *sql.DB) {
	t.Helper()
	now := testTime()
	mustTransact(t, db, func(tx *sql.Tx) error {
		if err := s.RegisterPublisher(t.Context(), tx, Publisher{"science-daily", "Science Daily", "https://example.org/", now}); err != nil {
			return err
		}
		for _, id := range []string{"fr-rss", "fr-api"} {
			if err := s.RegisterSource(t.Context(), tx, Source{ID: id, PublisherID: "science-daily", DisplayName: id, EndpointURL: "https://example.org/" + id, FormatKey: "rss", LanguageHint: "fr", RegistrationVersion: 1, Enabled: true, CreatedAt: now, UpdatedAt: now}, 0); err != nil {
				return err
			}
		}
		for _, c := range []Category{{"science", "", "Science", true, now}, {"science/space", "science", "Space", true, now}, {"science/spaceflight", "science", "Spaceflight", true, now}, {"politics", "", "Politics", true, now}} {
			if err := s.RegisterCategory(t.Context(), tx, c); err != nil {
				return err
			}
		}
		return nil
	})
}
func beginRun(t *testing.T, s *Store, db *sql.DB, n byte, source string) FetchStart {
	t.Helper()
	v := FetchStart{ID: testID(n), SourceID: source, RegistrationVersion: 1, ParserKey: "fixture", ParserVersion: "v1", StartedAt: testTime().Add(time.Duration(n) * time.Minute)}
	mustTransact(t, db, func(tx *sql.Tx) error { return s.BeginFetch(t.Context(), tx, v) })
	return v
}
func completeRun(t *testing.T, s *Store, db *sql.DB, v FetchStart, items []FetchItem) FetchCompletion {
	t.Helper()
	c := FetchCompletion{ID: v.ID, FinishedAt: v.StartedAt.Add(time.Second), ResponseSHA256: SHA256([]byte(fmt.Sprintf("fixture-%d", v.ID[15]))), Items: items}
	mustTransact(t, db, func(tx *sql.Tx) error { return s.CompleteFetch(t.Context(), tx, c) })
	return c
}
func beginAnnotation(t *testing.T, s *Store, db *sql.DB, n byte, run FetchStart) AnnotationStart {
	t.Helper()
	v := AnnotationStart{ID: testID(n), RunID: run.ID, ProfileKey: "general-news-v1", ProducerKey: "rules", ProducerVersion: "v1", DocumentVersion: 1, StartedAt: run.StartedAt.Add(time.Duration(n) * time.Second)}
	mustTransact(t, db, func(tx *sql.Tx) error { return s.BeginAnnotation(t.Context(), tx, v) })
	return v
}
func completeDecisions(t *testing.T, s *Store, db *sql.DB, a AnnotationStart, decisions []Decision) AnnotationCompletion {
	t.Helper()
	d := Document{Version: a.DocumentVersion, RunID: a.RunID, ProfileKey: a.ProfileKey, ProducerKey: a.ProducerKey, ProducerVersion: a.ProducerVersion, Decisions: decisions}
	b, h, err := EncodeDocument(d)
	if err != nil {
		t.Fatal(err)
	}
	v := AnnotationCompletion{ID: a.ID, FinishedAt: a.StartedAt.Add(time.Second), DocumentJSON: b, DocumentSHA256: h}
	mustTransact(t, db, func(tx *sql.Tx) error { return s.CompleteAnnotation(t.Context(), tx, v) })
	return v
}
func itemFor(t *testing.T, key, title string, published *time.Time) FetchItem {
	t.Helper()
	identity := Identity{"science-daily", "publisher-id-v1", []byte(key)}
	r := testRecord()
	r.Title = title
	r.PublishedAt = published
	return FetchItem{Identity: identity, SourceItemKey: []byte("source-" + key), Revision: mustRevision(t, identity, r)}
}
func classified(item FetchItem, category string) Decision {
	d := Decision{ArticleID: item.Revision.ArticleID, RevisionSHA256: item.Revision.SHA256, State: "classified"}
	if category != "" {
		d.Categories = []CategoryDecision{{Key: category, MethodKey: "source-tag", Evidence: json.RawMessage(`{"integer":18446744073709551615,"decimal":1.00,"exponent":1e3,"fraction":0.1,"large":9007199254740993}`)}}
	}
	return d
}
func pinFor(f FetchCompletion, a AnnotationCompletion) PinnedInput {
	return PinnedInput{RunID: f.ID, AnnotationSetID: a.ID, ResponseSHA256: f.ResponseSHA256, DocumentSHA256: a.DocumentSHA256}
}

// TestMySQLLifecycleAndReclassification verifies immutable classification snapshots.
//
// Version:
//   - 2026-10-31: Added.
func TestMySQLLifecycleAndReclassification(t *testing.T) {
	s, db := mysqlFixture(t)
	seedNews(t, s, db)
	ctx := t.Context()
	at := testTime()
	items := []FetchItem{itemFor(t, "one", "Une nouvelle planète", &at), itemFor(t, "two", "Regional election", &at), itemFor(t, "unknown", "Publication time unknown", nil)}
	run := beginRun(t, s, db, 1, "fr-rss")
	fetch := completeRun(t, s, db, run, items)
	mustTransact(t, db, func(tx *sql.Tx) error { return s.BeginFetch(ctx, tx, run) })
	mustTransact(t, db, func(tx *sql.Tx) error { return s.CompleteFetch(ctx, tx, fetch) })
	a := beginAnnotation(t, s, db, 21, run)
	d1 := classified(items[0], "science/space")
	d1.Entities = []EntityDecision{{Namespace: "wikidata", Key: "Q42", MethodKey: "source-id", Evidence: json.RawMessage(`{"score":1.00,"nested":[1e2,0.1,9223372036854775808]}`)}}
	d2 := classified(items[1], "politics")
	d3 := Decision{ArticleID: items[2].Revision.ArticleID, RevisionSHA256: items[2].Revision.SHA256, State: "unclassified", Reason: "Source did not supply a tag"}
	annotation := completeDecisions(t, s, db, a, []Decision{d3, d1, d2})
	mustTransact(t, db, func(tx *sql.Tx) error { return s.CompleteAnnotation(ctx, tx, annotation) })
	got, err := s.GetAnnotation(ctx, db, a.ID)
	if err != nil || got.Status != StatusComplete || got.CoveredArticleCount != 3 {
		t.Fatalf("annotation read: %+v %v", got, err)
	}
	request := ListRequest{Inputs: []PinnedInput{pinFor(fetch, annotation)}, ProfileKey: a.ProfileKey, Limit: 1}
	seen := map[Hash]bool{}
	var previous *Observation
	for {
		page, err := s.ListPinnedObservations(ctx, db, request)
		if err != nil {
			t.Fatal(err)
		}
		if page.Total != 3 || page.UnknownPublishedCount != 1 || len(page.Observations) != 1 {
			t.Fatalf("page counts: %+v", page)
		}
		o := page.Observations[0]
		if seen[o.Revision.ArticleID] {
			t.Fatal("repeated observation")
		}
		seen[o.Revision.ArticleID] = true
		if previous != nil && compareObservation(*previous, o) >= 0 {
			t.Fatal("unstable page order")
		}
		previous = &o
		if !page.HasMore {
			if o.Revision.PublishedAt != nil {
				t.Fatal("unknown time not last")
			}
			break
		}
		if page.Next == nil {
			t.Fatal("missing cursor")
		}
		request.After = page.Next
	}
	request.After = nil
	request.Limit = 100
	request.Filter = Filter{CategoryKey: "science", IncludeDescendants: true, EntityNamespace: "wikidata", EntityKey: "Q42", LanguageTag: "fr"}
	page, err := s.ListPinnedObservations(ctx, db, request)
	if err != nil || page.Total != 1 {
		t.Fatalf("generic filters: %+v %v", page, err)
	}
	request.Filter = Filter{PublishedFrom: &at}
	page, err = s.ListPinnedObservations(ctx, db, request)
	if err != nil || page.Total != 2 || page.UnknownPublishedCount != 1 {
		t.Fatalf("time filter: %+v %v", page, err)
	}
	ready, err := s.LatestReady(ctx, db, []string{"fr-rss"}, a.ProfileKey)
	if err != nil || ready[0].Stale || ready[0].Input.AnnotationSetID != a.ID {
		t.Fatalf("latest: %+v %v", ready, err)
	}
	// Reclassification changes only the annotation version; old pins remain reproducible.
	a2 := beginAnnotation(t, s, db, 22, run)
	annotation2 := completeDecisions(t, s, db, a2, []Decision{classified(items[0], "science/spaceflight"), classified(items[1], ""), d3})
	request.Filter = Filter{CategoryKey: "science/space", IncludeDescendants: true}
	page, err = s.ListPinnedObservations(ctx, db, request)
	if err != nil || page.Total != 1 {
		t.Fatalf("old snapshot changed: %v", err)
	}
	request.Inputs = []PinnedInput{pinFor(fetch, annotation2)}
	page, err = s.ListPinnedObservations(ctx, db, request)
	if err != nil || page.Total != 0 {
		t.Fatalf("category boundary: %+v %v", page, err)
	}
	a3 := beginAnnotation(t, s, db, 23, run)
	failure := Failure{a3.ID, a3.StartedAt.Add(time.Second), "producer_unavailable"}
	mustTransact(t, db, func(tx *sql.Tx) error { return s.FailAnnotation(ctx, tx, failure) })
	mustTransact(t, db, func(tx *sql.Tx) error { return s.FailAnnotation(ctx, tx, failure) })
	ready, err = s.LatestReady(ctx, db, []string{"fr-rss"}, a.ProfileKey)
	if err != nil || !ready[0].Stale || ready[0].LatestAnnotationAttempt.Status != StatusFailed || ready[0].Input.AnnotationSetID != a2.ID {
		t.Fatalf("classification failure hidden: %+v %v", ready, err)
	}
	newRun := beginRun(t, s, db, 2, "fr-rss")
	completeRun(t, s, db, newRun, nil)
	ready, err = s.LatestReady(ctx, db, []string{"fr-rss"}, a.ProfileKey)
	if err != nil || !ready[0].Stale || ready[0].LatestAttempt.ID != newRun.ID || ready[0].LatestAnnotationAttempt != nil || ready[0].Input.RunID != run.ID {
		t.Fatalf("pending classification hidden: %+v %v", ready, err)
	}
	failedRun := beginRun(t, s, db, 3, "fr-rss")
	f := Failure{failedRun.ID, failedRun.StartedAt.Add(time.Second), "http_timeout"}
	mustTransact(t, db, func(tx *sql.Tx) error { return s.FailFetch(ctx, tx, f) })
	ready, err = s.LatestReady(ctx, db, []string{"fr-rss"}, a.ProfileKey)
	if err != nil || ready[0].LatestAttempt.Status != StatusFailed || !ready[0].Stale || ready[0].Input.RunID != run.ID {
		t.Fatalf("fetch failure hidden: %+v %v", ready, err)
	}
	// Disabling current registrations does not hide historical pins.
	mustTransact(t, db, func(tx *sql.Tx) error {
		return s.RegisterCategory(ctx, tx, Category{"science/space", "science", "Space", false, testTime()})
	})
	request.Inputs = []PinnedInput{pinFor(fetch, annotation)}
	page, err = s.ListPinnedObservations(ctx, db, request)
	if err != nil || page.Total != 1 {
		t.Fatalf("disabled category erased history: %v", err)
	}
}

// TestMySQLRevisionsEmptyAndMultipleSources verifies A-B-A, empty input and attribution.
//
// Version:
//   - 2026-10-31: Added.
func TestMySQLRevisionsEmptyAndMultipleSources(t *testing.T) {
	s, db := mysqlFixture(t)
	seedNews(t, s, db)
	ctx := t.Context()
	at := testTime()
	itemA := itemFor(t, "one", "Version A", &at)
	itemB := itemFor(t, "one", "Version B", &at)
	for i, item := range []FetchItem{itemA, itemB, itemA} {
		run := beginRun(t, s, db, byte(i+1), "fr-rss")
		completeRun(t, s, db, run, []FetchItem{item})
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+s.table("article_revisions")).Scan(&count); err != nil || count != 2 {
		t.Fatalf("A-B-A revision count=%d err=%v", count, err)
	}
	run := beginRun(t, s, db, 4, "fr-api")
	other := completeRun(t, s, db, run, []FetchItem{itemA})
	a := beginAnnotation(t, s, db, 31, run)
	ann := completeDecisions(t, s, db, a, []Decision{classified(itemA, "")})
	lastRun := FetchStart{ID: testID(3), StartedAt: testTime().Add(3 * time.Minute)}
	a2 := beginAnnotation(t, s, db, 32, lastRun)
	ann2 := completeDecisions(t, s, db, a2, []Decision{classified(itemA, "")})
	f3, err := s.GetFetch(ctx, db, testID(3))
	if err != nil {
		t.Fatal(err)
	}
	page, err := s.ListPinnedObservations(ctx, db, ListRequest{Inputs: []PinnedInput{pinFor(other, ann), {testID(3), a2.ID, *f3.ResponseSHA256, ann2.DocumentSHA256}}, ProfileKey: a.ProfileKey, Limit: 100})
	if err != nil || page.Total != 2 || page.Observations[0].SourceID == page.Observations[1].SourceID {
		t.Fatalf("source attribution removed: %+v %v", page, err)
	}
	// A failure on the second pinned input discards candidates already read from the first.
	injected := errors.New("injected second input read failure")
	failedExecutor := queryFailExecutor{Executor: db, RunID: run.ID, Err: injected}
	page, err = s.ListPinnedObservations(ctx, failedExecutor, ListRequest{Inputs: []PinnedInput{pinFor(other, ann), {testID(3), a2.ID, *f3.ResponseSHA256, ann2.DocumentSHA256}}, ProfileKey: a.ProfileKey, Limit: 100})
	if !errors.Is(err, injected) || len(page.Observations) != 0 || page.Scope != (Hash{}) {
		t.Fatalf("partial response on later failure: %+v %v", page, err)
	}
	empty := beginRun(t, s, db, 5, "fr-rss")
	emptyFetch := completeRun(t, s, db, empty, nil)
	emptyA := beginAnnotation(t, s, db, 33, empty)
	emptyAnn := completeDecisions(t, s, db, emptyA, nil)
	page, err = s.ListPinnedObservations(ctx, db, ListRequest{Inputs: []PinnedInput{pinFor(emptyFetch, emptyAnn)}, ProfileKey: a.ProfileKey, Limit: 1})
	if err != nil || page.Total != 0 || page.HasMore {
		t.Fatalf("empty feed: %+v %v", page, err)
	}
	// A publisher-local title or upstream ID never merges unrelated publishers.
	mustTransact(t, db, func(tx *sql.Tx) error {
		if err := s.RegisterPublisher(ctx, tx, Publisher{"other", "Other", "https://other.example/", at}); err != nil {
			return err
		}
		return s.RegisterSource(ctx, tx, Source{ID: "other-source", PublisherID: "other", DisplayName: "Other", EndpointURL: "https://other.example/rss", FormatKey: "rss", RegistrationVersion: 1, Enabled: true, CreatedAt: at, UpdatedAt: at}, 0)
	})
	i := itemA
	i.Identity.PublisherID = "other"
	i.Revision = mustRevision(t, i.Identity, testRecord())
	r := beginRun(t, s, db, 6, "other-source")
	completeRun(t, s, db, r, []FetchItem{i})
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+s.table("articles")).Scan(&count); err != nil || count != 2 {
		t.Fatalf("publisher isolation count=%d err=%v", count, err)
	}
}

// TestMySQLValidationRollbackAndConflicts verifies atomic failures and immutable retries.
//
// Version:
//   - 2026-10-31: Added.
func TestMySQLValidationRollbackAndConflicts(t *testing.T) {
	s, db := mysqlFixture(t)
	seedNews(t, s, db)
	ctx := t.Context()
	at := testTime()
	item := itemFor(t, "one", "Version A", &at)
	// Registration CAS and immutable category membership.
	source := Source{ID: "fr-rss", PublisherID: "science-daily", DisplayName: "Updated", EndpointURL: "https://example.org/new", FormatKey: "rss", RegistrationVersion: 2, Enabled: true, CreatedAt: at, UpdatedAt: at.Add(time.Second)}
	mustTransact(t, db, func(tx *sql.Tx) error { return s.RegisterSource(ctx, tx, source, 1) })
	mustTransact(t, db, func(tx *sql.Tx) error { return s.RegisterSource(ctx, tx, source, 1) })
	stale := source
	stale.DisplayName = "Stale"
	if err := transact(t, db, func(tx *sql.Tx) error { return s.RegisterSource(ctx, tx, stale, 1) }); !errors.Is(err, ErrConflict) {
		t.Fatal("stale registration accepted", err)
	}
	if err := transact(t, db, func(tx *sql.Tx) error {
		return s.RegisterCategory(ctx, tx, Category{"science/planets", "politics", "Planets", true, at})
	}); !errors.Is(err, ErrInvalid) {
		t.Fatal("wrong category parent accepted", err)
	}
	run := FetchStart{ID: testID(1), SourceID: "fr-rss", RegistrationVersion: 2, ParserKey: "fixture", ParserVersion: "v1", StartedAt: at.Add(time.Minute)}
	mustTransact(t, db, func(tx *sql.Tx) error { return s.BeginFetch(ctx, tx, run) })
	// Running acquisition must be explicitly recovered, never stolen by elapsed time.
	second := run
	second.ID = testID(2)
	second.StartedAt = second.StartedAt.Add(time.Hour)
	if err := transact(t, db, func(tx *sql.Tx) error { return s.BeginFetch(ctx, tx, second) }); err == nil {
		t.Fatal("parallel running fetch accepted")
	}
	// Conflicting duplicate is rejected before any revision is stored.
	conflicting := item
	conflicting.SourceItemKey = []byte("different")
	c := FetchCompletion{ID: run.ID, FinishedAt: run.StartedAt.Add(time.Second), ResponseSHA256: SHA256([]byte("response")), Items: []FetchItem{item, conflicting}}
	if err := transact(t, db, func(tx *sql.Tx) error { return s.CompleteFetch(ctx, tx, c) }); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+s.table("articles")).Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial articles stored: %d %v", count, err)
	}
	c.Items = []FetchItem{item, item}
	mustTransact(t, db, func(tx *sql.Tx) error { return s.CompleteFetch(ctx, tx, c) })
	f, err := s.GetFetch(ctx, db, run.ID)
	if err != nil || f.ItemCount != 1 {
		t.Fatalf("exact duplicate: %+v %v", f, err)
	}
	badRetry := c
	badRetry.ResponseSHA256 = SHA256([]byte("different"))
	if err := transact(t, db, func(tx *sql.Tx) error { return s.CompleteFetch(ctx, tx, badRetry) }); !errors.Is(err, ErrConflict) {
		t.Fatal("completion rewrite accepted", err)
	}
	if err := transact(t, db, func(tx *sql.Tx) error {
		return s.FailFetch(ctx, tx, Failure{run.ID, c.FinishedAt, "cannot_fail_complete"})
	}); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	a := beginAnnotation(t, s, db, 41, run)
	secondAnnotation := a
	secondAnnotation.ID = testID(42)
	if err := transact(t, db, func(tx *sql.Tx) error { return s.BeginAnnotation(ctx, tx, secondAnnotation) }); err == nil {
		t.Fatal("parallel running annotation accepted")
	}
	wrong := classified(item, "")
	wrong.RevisionSHA256 = testHash(77)
	b, h, err := EncodeDocument(Document{Version: 1, RunID: run.ID, ProfileKey: a.ProfileKey, ProducerKey: a.ProducerKey, ProducerVersion: a.ProducerVersion, Decisions: []Decision{wrong}})
	if err != nil {
		t.Fatal(err)
	}
	if err := transact(t, db, func(tx *sql.Tx) error {
		return s.CompleteAnnotation(ctx, tx, AnnotationCompletion{a.ID, a.StartedAt.Add(time.Second), b, h})
	}); !errors.Is(err, ErrIntegrity) {
		t.Fatal("wrong input revision accepted", err)
	}
	// Missing input coverage never means successful empty classification.
	b, h, err = EncodeDocument(Document{Version: 1, RunID: run.ID, ProfileKey: a.ProfileKey, ProducerKey: a.ProducerKey, ProducerVersion: a.ProducerVersion})
	if err != nil {
		t.Fatal(err)
	}
	if err := transact(t, db, func(tx *sql.Tx) error {
		return s.CompleteAnnotation(ctx, tx, AnnotationCompletion{a.ID, a.StartedAt.Add(time.Second), b, h})
	}); !errors.Is(err, ErrIntegrity) {
		t.Fatal("missing coverage accepted", err)
	}
	// A late FK error after one valid projection must roll back all projections.
	d := classified(item, "science")
	d.Categories = append(d.Categories, CategoryDecision{Key: "unknown", MethodKey: "test", Evidence: json.RawMessage(`{}`)})
	b, h, err = EncodeDocument(Document{Version: 1, RunID: run.ID, ProfileKey: a.ProfileKey, ProducerKey: a.ProducerKey, ProducerVersion: a.ProducerVersion, Decisions: []Decision{d}})
	if err != nil {
		t.Fatal(err)
	}
	if err := transact(t, db, func(tx *sql.Tx) error {
		return s.CompleteAnnotation(ctx, tx, AnnotationCompletion{a.ID, a.StartedAt.Add(time.Second), b, h})
	}); err == nil {
		t.Fatal("missing registered category accepted")
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+s.table("article_categories")).Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial projections stored: %d %v", count, err)
	}
	ann := completeDecisions(t, s, db, a, []Decision{classified(item, "science")})
	// Read corruption returns no partial page and keeps exact document hashes meaningful.
	if _, err := db.ExecContext(ctx, "UPDATE "+s.table("article_categories")+" SET method_key='tampered' WHERE annotation_set_id=?", a.ID[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetAnnotation(ctx, db, a.ID); !errors.Is(err, ErrIntegrity) {
		t.Fatal("projection corruption accepted", err)
	}
	page, err := s.ListPinnedObservations(ctx, db, ListRequest{Inputs: []PinnedInput{pinFor(c, ann)}, ProfileKey: a.ProfileKey, Limit: 1})
	if !errors.Is(err, ErrIntegrity) || len(page.Observations) != 0 || page.Scope != (Hash{}) {
		t.Fatalf("partial corrupt page: %+v %v", page, err)
	}
	if _, err := s.GetFetch(ctx, db, testID(99)); !errors.Is(err, ErrNotFound) || !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
}

type queryFailExecutor struct {
	api.Executor
	RunID ID
	Err   error
}

// QueryContext injects a failure for one pinned input while forwarding other reads.
//
// Version:
//   - 2026-10-31: Added.
func (e queryFailExecutor) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if strings.Contains(query, "SELECT article_id,revision_sha256,source_item_key") && len(args) > 0 {
		if id, ok := args[0].([]byte); ok && bytes.Equal(id, e.RunID[:]) {
			return nil, e.Err
		}
	}
	return e.Executor.QueryContext(ctx, query, args...)
}

// TestMySQLIdentityCollisionAndRecordIntegrity verifies identity and record corruption checks.
//
// Version:
//   - 2026-10-31: Added.
func TestMySQLIdentityCollisionAndRecordIntegrity(t *testing.T) {
	s, db := mysqlFixture(t)
	seedNews(t, s, db)
	ctx := t.Context()
	at := testTime()
	first := itemFor(t, "one", "Version A", &at)
	run := beginRun(t, s, db, 1, "fr-rss")
	completeRun(t, s, db, run, []FetchItem{first})
	second := itemFor(t, "two", "Version B", &at)
	next := beginRun(t, s, db, 2, "fr-rss")
	// Force an impossible SHA collision at the persistence boundary; stored identity
	// values must still be compared rather than trusting the unique digest alone.
	forcedHash := SHA256(second.Identity.Value)
	if _, err := db.ExecContext(ctx, "UPDATE "+s.table("articles")+" SET identity_sha256=? WHERE article_id=?", forcedHash[:], first.Revision.ArticleID[:]); err != nil {
		t.Fatal(err)
	}
	c := FetchCompletion{next.ID, next.StartedAt.Add(time.Second), SHA256([]byte("response")), []FetchItem{second}}
	if err := transact(t, db, func(tx *sql.Tx) error { return s.CompleteFetch(ctx, tx, c) }); !errors.Is(err, ErrIntegrity) {
		t.Fatal("identity collision accepted", err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE "+s.table("article_revisions")+" SET language_tag='en' WHERE article_id=?", first.Revision.ArticleID[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetRevision(ctx, db, first.Revision.ArticleID, first.Revision.SHA256); !errors.Is(err, ErrIntegrity) {
		t.Fatal("corrupt record projection accepted", err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE "+s.table("article_revisions")+" SET language_tag='fr',record_json=REPLACE(record_json,'Version A','Version Z') WHERE article_id=?", first.Revision.ArticleID[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetRevision(ctx, db, first.Revision.ArticleID, first.Revision.SHA256); !errors.Is(err, ErrIntegrity) {
		t.Fatal("corrupt record hash accepted", err)
	}
}

// TestMySQLConcurrentBeginAndCallerRollback verifies serialization and transaction ownership.
//
// Version:
//   - 2026-10-31: Added.
func TestMySQLConcurrentBeginAndCallerRollback(t *testing.T) {
	s, db := mysqlFixture(t)
	seedNews(t, s, db)
	ctx := t.Context()
	at := testTime()
	// The short begin transaction owns serialization; one of two concurrent starts wins.
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, n := range []byte{1, 2} {
		wg.Add(1)
		go func(n byte) {
			defer wg.Done()
			<-start
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				results <- err
				return
			}
			v := FetchStart{testID(n), "fr-rss", 1, "fixture", "v1", at.Add(time.Minute)}
			err = s.BeginFetch(ctx, tx, v)
			if err != nil {
				err = errors.Join(err, tx.Rollback())
			} else {
				err = tx.Commit()
			}
			results <- err
		}(n)
	}
	close(start)
	wg.Wait()
	close(results)
	success, failed := 0, 0
	for err := range results {
		if err == nil {
			success++
		} else {
			failed++
		}
	}
	if success != 1 || failed != 1 {
		t.Fatalf("concurrent starts: %d successes %d failures", success, failed)
	}
	var idBytes []byte
	if err := db.QueryRowContext(ctx, "SELECT run_id FROM "+s.table("fetch_runs")+" WHERE status='running'").Scan(&idBytes); err != nil {
		t.Fatal(err)
	}
	var running ID
	copy(running[:], idBytes)
	failure := Failure{running, at.Add(2 * time.Minute), "abandoned"}
	mustTransact(t, db, func(tx *sql.Tx) error { return s.FailFetch(ctx, tx, failure) })
	run := beginRun(t, s, db, 3, "fr-rss")
	item := itemFor(t, "rollback", "No hidden commit", &at)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	c := FetchCompletion{run.ID, run.StartedAt.Add(time.Second), SHA256([]byte("response")), []FetchItem{item}}
	if err := s.CompleteFetch(ctx, tx, c); err != nil {
		rollbackErr := tx.Rollback()
		t.Fatal(errors.Join(err, rollbackErr))
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	f, err := s.GetFetch(ctx, db, run.ID)
	if err != nil || f.Status != StatusRunning {
		t.Fatalf("Store committed caller transaction: %+v %v", f, err)
	}
	if _, err := s.GetRevision(ctx, db, item.Revision.ArticleID, item.Revision.SHA256); !errors.Is(err, ErrNotFound) {
		t.Fatal("rollback retained revision", err)
	}
	// The same explicit completion can then be applied and reread after uncertain commit.
	mustTransact(t, db, func(tx *sql.Tx) error { return s.CompleteFetch(ctx, tx, c) })
	mustTransact(t, db, func(tx *sql.Tx) error { return s.CompleteFetch(ctx, tx, c) })
}

// TestMySQLSchemaConstraints verifies prefixed DDL, terminal states and foreign keys.
//
// Version:
//   - 2026-10-31: Added.
func TestMySQLSchemaConstraints(t *testing.T) {
	s, db := mysqlFixture(t)
	seedNews(t, s, db)
	ctx := t.Context()
	at := testTime()
	run := beginRun(t, s, db, 1, "fr-rss")
	item := itemFor(t, "one", "Article", &at)
	completeRun(t, s, db, run, []FetchItem{item})
	a := beginAnnotation(t, s, db, 61, run)
	wrongRun, wrongHash := testID(9), testHash(9)
	reject := func(query string, args ...any) {
		t.Helper()
		if _, err := db.ExecContext(ctx, query, args...); err == nil {
			t.Fatal("invalid SQL write accepted")
		}
	}
	reject("UPDATE "+s.table("fetch_runs")+" SET finished_at=NULL WHERE run_id=?", run.ID[:])
	reject("UPDATE "+s.table("annotation_sets")+" SET status='failed',finished_at=NULL,error_code='error' WHERE annotation_set_id=?", a.ID[:])
	reject("INSERT INTO "+s.table("article_categories")+" (annotation_set_id,run_id,article_id,revision_sha256,category_key,method_key,evidence) VALUES (?,?,?,?,?,?,?)", a.ID[:], wrongRun[:], item.Revision.ArticleID[:], item.Revision.SHA256[:], "science", "test", `{}`)
	reject("INSERT INTO "+s.table("article_categories")+" (annotation_set_id,run_id,article_id,revision_sha256,category_key,method_key,evidence) VALUES (?,?,?,?,?,?,?)", a.ID[:], run.ID[:], item.Revision.ArticleID[:], wrongHash[:], "science", "test", `{}`)
	reject("DELETE FROM " + s.table("publishers") + " WHERE publisher_id='science-daily'")
	reject("UPDATE "+s.table("article_revisions")+" SET record_json='[]' WHERE article_id=?", item.Revision.ArticleID[:])
}
