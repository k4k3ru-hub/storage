package news

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

func testID(n byte) ID     { var id ID; id[15] = n; return id }
func testHash(n byte) Hash { var h Hash; h[31] = n; return h }
func testTime() time.Time  { return time.Date(2026, 10, 31, 12, 0, 0, 123456000, time.UTC) }
func testIdentity() Identity {
	return Identity{PublisherID: "science-daily", Namespace: "publisher-id-v1", Value: []byte("article-42")}
}
func testRecord() Record {
	return Record{Version: 1, Title: "Une nouvelle planète", URL: "https://EXAMPLE.org:443/news?id=2&id=1#result", Summary: "Publisher-provided summary", LanguageTag: "fr", Attributes: json.RawMessage(`{"z":2,"a":1}`)}
}
func mustRevision(t *testing.T, identity Identity, r Record) Revision {
	t.Helper()
	v, err := NewRevision(identity, r)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func testDocument() Document {
	return Document{Version: 1, RunID: testID(1), ProfileKey: "general-news-v1", ProducerKey: "rules", ProducerVersion: "2026-10", Decisions: []Decision{{ArticleID: testHash(1), RevisionSHA256: testHash(2), State: "classified", Categories: []CategoryDecision{{Key: "science/space", MethodKey: "source-tag", Evidence: json.RawMessage(`{"z":2,"a":1}`)}}}}}
}

// TestArticleIdentity verifies length-prefixed, publisher-scoped article identity.
//
// Version:
//   - 2026-10-31: Added.
func TestArticleIdentity(t *testing.T) {
	i := testIdentity()
	a, err := ArticleID(i)
	if err != nil {
		t.Fatal(err)
	}
	if a.String() != "2321d10affc543d6e0f30819e588d156d77e510dc89f986c6aeacb3b79d26457" {
		t.Fatal("article ID format changed", a)
	}
	for _, changed := range []Identity{{PublisherID: "other", Namespace: i.Namespace, Value: i.Value}, {PublisherID: i.PublisherID, Namespace: "other", Value: i.Value}, {PublisherID: i.PublisherID, Namespace: i.Namespace, Value: []byte("ARTICLE-42")}} {
		b, err := ArticleID(changed)
		if err != nil || b == a {
			t.Fatalf("identity separation: %v", err)
		}
	}
	x, _ := ArticleID(Identity{"ab", "c", []byte("d")})
	y, _ := ArticleID(Identity{"a", "bc", []byte("d")})
	if x == y {
		t.Fatal("length-prefix ambiguity")
	}
	if got := a.String(); len(got) != 64 {
		t.Fatal(got)
	}
	for _, bad := range []Identity{{}, {PublisherID: "p", Namespace: "n", Value: bytes.Repeat([]byte{'a'}, 2049)}, {PublisherID: "p", Namespace: "canonical-url-v1", Value: []byte("https://EXAMPLE.org")}} {
		if _, err := ArticleID(bad); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid identity: %v", err)
		}
	}
}

// TestCanonicalURL verifies conservative HTTPS normalization and credential rejection.
//
// Version:
//   - 2026-10-31: Added.
func TestCanonicalURL(t *testing.T) {
	got, err := CanonicalURL("https://EXAMPLE.org:443/a%2Fb?b=2&a=1&a=2#fragment")
	if err != nil {
		t.Fatal(err)
	}
	if got != "https://example.org/a%2Fb?b=2&a=1&a=2" {
		t.Fatal(got)
	}
	got, err = CanonicalURL("https://EXAMPLE.org")
	if err != nil || got != "https://example.org/" {
		t.Fatalf("%q %v", got, err)
	}
	for _, bad := range []string{"", "http://example.org/", "https://user:password@example.org/", "https:///x", "https://example.org/\x00", strings.Repeat("x", 2049)} {
		if _, err := CanonicalURL(bad); !errors.Is(err, ErrInvalid) || strings.Contains(err.Error(), "password") {
			t.Fatalf("invalid url: %v", err)
		}
	}
}

// TestCanonicalRecordAndProjection verifies exact record bytes and search projections.
//
// Version:
//   - 2026-10-31: Added.
func TestCanonicalRecordAndProjection(t *testing.T) {
	r := testRecord()
	offset := testTime().In(time.FixedZone("test", 9*3600))
	r.PublishedAt = &offset
	v := mustRevision(t, testIdentity(), r)
	if v.PublishedAt.Location() != time.UTC || !v.PublishedAt.Equal(offset) || !v.FirstObservedAt.IsZero() {
		t.Fatal("time normalization")
	}
	if !bytes.Contains(v.RecordJSON, []byte(`"url":"https://example.org/news?id=2\u0026id=1"`)) || !bytes.Contains(v.RecordJSON, []byte(`"attributes":{"a":1,"z":2}`)) {
		t.Fatal(string(v.RecordJSON))
	}
	if err := validateRevision(v); err != nil {
		t.Fatal(err)
	}
	if string(r.Attributes) != `{"z":2,"a":1}` || offset.Location() == time.UTC {
		t.Fatal("caller values mutated")
	}
	unknown := mustRevision(t, testIdentity(), testRecord())
	if unknown.PublishedAt != nil || bytes.Contains(unknown.RecordJSON, []byte("published_at")) {
		t.Fatal("unknown publication time invented")
	}
	for _, change := range []func(*Revision){func(v *Revision) { v.SHA256[0] ^= 1 }, func(v *Revision) { v.RecordVersion = 2 }, func(v *Revision) { v.LanguageTag = "en" }, func(v *Revision) { v.PublishedAt = nil }, func(v *Revision) { v.RecordJSON = append(v.RecordJSON, ' '); v.SHA256 = SHA256(v.RecordJSON) }} {
		bad := v
		change(&bad)
		if err := validateRevision(bad); err == nil {
			t.Fatal("invalid projection accepted")
		}
	}
	for name, mutate := range map[string]func(*Record){"version": func(r *Record) { r.Version = 0 }, "title": func(r *Record) { r.Title = "" }, "summary": func(r *Record) { r.Summary = strings.Repeat("x", 8193) }, "precision": func(r *Record) { bad := testTime().Add(time.Nanosecond); r.PublishedAt = &bad }, "zero-time": func(r *Record) { r.PublishedAt = &time.Time{} }, "duplicate-json": func(r *Record) { r.Attributes = json.RawMessage(`{"a":1,"a":2}`) }, "non-object": func(r *Record) { r.Attributes = json.RawMessage(`[]`) }, "too-deep": func(r *Record) {
		r.Attributes = json.RawMessage(`{"a":` + strings.Repeat("[", 17) + `0` + strings.Repeat("]", 17) + `}`)
	}} {
		t.Run(name, func(t *testing.T) {
			r := testRecord()
			mutate(&r)
			if _, err := NewRevision(testIdentity(), r); err == nil {
				t.Fatal("invalid record accepted")
			}
		})
	}
}

// TestCanonicalDocument verifies deterministic decisions and bounded annotation input.
//
// Version:
//   - 2026-10-31: Added.
func TestCanonicalDocument(t *testing.T) {
	d := testDocument()
	d.Decisions = append(d.Decisions, Decision{ArticleID: testHash(3), RevisionSHA256: testHash(4), State: "unclassified", Reason: "No reliable source tag"})
	d.Decisions[0].Entities = []EntityDecision{{Namespace: "wikidata", Key: "Q42", MethodKey: "source-id", Evidence: json.RawMessage(`{ "id": "Q42" }`)}}
	b, h, err := EncodeDocument(d)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(b, []byte(`"categories":[],"entities":[]`)) {
		t.Fatal(string(b))
	}
	if _, err := decodeDocument(b, h); err != nil {
		t.Fatal(err)
	}
	reversed := d
	reversed.Decisions = []Decision{d.Decisions[1], d.Decisions[0]}
	b2, h2, err := EncodeDocument(reversed)
	if err != nil || h != h2 || !bytes.Equal(b, b2) {
		t.Fatalf("unstable order: %v", err)
	}
	if string(d.Decisions[0].Categories[0].Evidence) != `{"z":2,"a":1}` {
		t.Fatal("caller document mutated")
	}
	for name, mutate := range map[string]func(*Document){"version": func(d *Document) { d.Version = 2 }, "duplicate-article": func(d *Document) { d.Decisions = append(d.Decisions, d.Decisions[0]) }, "duplicate-category": func(d *Document) {
		d.Decisions[0].Categories = append(d.Decisions[0].Categories, d.Decisions[0].Categories[0])
	}, "parent-category": func(d *Document) { d.Decisions[0].Categories[0].Key = "Science/space" }, "no-reason": func(d *Document) { d.Decisions[0].State = "unclassified"; d.Decisions[0].Categories = nil }, "unclassified-with-relations": func(d *Document) { d.Decisions[0].State = "unclassified"; d.Decisions[0].Reason = "unknown" }, "too-many": func(d *Document) { d.Decisions = make([]Decision, MaxItems+1) }} {
		t.Run(name, func(t *testing.T) {
			d := testDocument()
			mutate(&d)
			if _, _, err := EncodeDocument(d); err == nil {
				t.Fatal("invalid document accepted")
			}
		})
	}
	if _, err := decodeDocument(b, testHash(9)); !errors.Is(err, ErrIntegrity) {
		t.Fatal(err)
	}
}

// TestSchemaAndDependencies verifies reviewed DDL parity, prefixes and construction.
//
// Version:
//   - 2026-10-31: Added.
func TestSchemaAndDependencies(t *testing.T) {
	s, err := NewStore("")
	if err != nil {
		t.Fatal(err)
	}
	ddl, err := s.Schema()
	if err != nil {
		t.Fatal(err)
	}
	proposed, err := os.ReadFile("schema.proposed.sql")
	if err != nil {
		t.Fatal(err)
	}
	unquote := strings.ReplaceAll(ddl, "`", "")
	if strings.SplitN(unquote, "\n", 2)[1] != strings.SplitN(string(proposed), "\n", 2)[1] {
		t.Fatal("executed DDL differs from reviewed DDL")
	}
	prefixed, err := NewStore("market_hub_")
	if err != nil {
		t.Fatal(err)
	}
	ddl, err = prefixed.Schema()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(ddl, "CREATE TABLE `market_hub_news_") != 10 || !strings.Contains(ddl, "REFERENCES `market_hub_news_fetch_runs`") {
		t.Fatal("prefix substitution")
	}
	for _, prefix := range []string{"9invalid", "invalid-", "x`", strings.Repeat("x", 26), "日本_"} {
		if _, err := NewStore(prefix); !errors.Is(err, ErrInvalid) {
			t.Fatalf("prefix accepted: %q %v", prefix, err)
		}
	}
	var nilStore *Store
	if _, err := nilStore.Schema(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := (&Store{}).Schema(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := dependency(s, ctx, newErrorDB(t, errors.New("unused")), "test news dependency"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

// TestPinnedScopeAndOrdering verifies cursor scope, slash boundaries and NULL ordering.
//
// Version:
//   - 2026-10-31: Added.
func TestPinnedScopeAndOrdering(t *testing.T) {
	pins := []PinnedInput{{testID(2), testID(12), testHash(2), testHash(12)}, {testID(1), testID(11), testHash(1), testHash(11)}}
	r := ListRequest{Inputs: pins, ProfileKey: "general-news-v1", Limit: 2}
	normal, scope, err := normalizeListRequest(r)
	if err != nil {
		t.Fatal(err)
	}
	r.Inputs = []PinnedInput{pins[1], pins[0]}
	r.Limit = 100
	_, other, err := normalizeListRequest(r)
	if err != nil || scope != other {
		t.Fatalf("scope instability: %v", err)
	}
	r.After = &Cursor{Scope: scope, ArticleID: testHash(1), RunID: testID(1)}
	r.Filter.CategoryKey = "science"
	if _, _, err := normalizeListRequest(r); !errors.Is(err, ErrInvalid) {
		t.Fatal("foreign cursor accepted", err)
	}
	r = normal
	r.Inputs = []PinnedInput{pins[0], pins[0]}
	if _, _, err := normalizeListRequest(r); err == nil {
		t.Fatal("duplicate run accepted")
	}
	at := testTime()
	a := Observation{RunID: testID(1), Revision: Revision{ArticleID: testHash(1), PublishedAt: &at}}
	b := a
	b.RunID = testID(2)
	if compareObservation(a, b) >= 0 {
		t.Fatal("run tie ordering")
	}
	b.Revision.PublishedAt = nil
	if compareObservation(a, b) >= 0 {
		t.Fatal("NULL must be last")
	}
	a.Decision = Decision{Categories: []CategoryDecision{{Key: "science/spaceflight"}}}
	if matchesNonTime(a, Filter{CategoryKey: "science/space", IncludeDescendants: true}) {
		t.Fatal("descendant crossed slash boundary")
	}
	a.Decision.Categories[0].Key = "science/space/planets"
	if !matchesNonTime(a, Filter{CategoryKey: "science/space", IncludeDescendants: true}) {
		t.Fatal("descendant excluded")
	}
	if matchesNonTime(a, Filter{CategoryKey: "science/space"}) {
		t.Fatal("exact matched descendant")
	}
	to := at.Add(time.Second)
	if !matchesTime(&at, Filter{PublishedFrom: &at, PublishedTo: &to}) || matchesTime(&to, Filter{PublishedFrom: &at, PublishedTo: &to}) || matchesTime(nil, Filter{PublishedFrom: &at}) {
		t.Fatal("publication boundaries")
	}
}

// TestEvidenceNumbersAndUTCYearBounds verifies exact numeric comparison and UTC limits.
//
// Version:
//   - 2026-10-31: Added.
func TestEvidenceNumbersAndUTCYearBounds(t *testing.T) {
	for _, pair := range [][2]string{{`{"n":1.00}`, `{"n":1.0}`}, {`{"n":1e3}`, `{"n":1000.0}`}, {`{"n":18446744073709551615}`, `{"n":18446744073709551615}`}, {`{"n":9007199254740993}`, `{"n":9007199254740993}`}} {
		if !equivalentEvidence([]byte(pair[0]), []byte(pair[1]), 4096) {
			t.Fatalf("numeric normalization differs: %s %s", pair[0], pair[1])
		}
	}
	if equivalentEvidence([]byte(`{"n":9007199254740993}`), []byte(`{"n":9007199254740992}`), 4096) {
		t.Fatal("integer precision lost")
	}
	for _, number := range []string{"18446744073709551616", "-9223372036854775809", "9007199254740993.0", "0.1234567890123456789", "1e309", "1e-400", "1e-1000000000", "0e-1000000000", "0e1000000000"} {
		if _, err := canonicalEvidence([]byte(`{"n":`+number+`}`), 4096); !errors.Is(err, ErrInvalid) {
			t.Fatalf("lossy number accepted: %s %v", number, err)
		}
	}
	for _, v := range []time.Time{time.Date(1000, 1, 1, 0, 0, 0, 0, time.FixedZone("east", 3600)), time.Date(9999, 12, 31, 23, 59, 59, 0, time.FixedZone("west", -3600))} {
		if validTime(v) {
			t.Fatal("UTC timestamp outside MySQL range accepted")
		}
	}
}

// TestCollectionBounds verifies collection, relation and search bounds.
//
// Version:
//   - 2026-10-31: Added.
func TestCollectionBounds(t *testing.T) {
	if _, err := normalizeItems(make([]FetchItem, MaxItems+1)); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	d := testDocument()
	d.Decisions[0].Entities = make([]EntityDecision, MaxRelations+1)
	if _, _, err := EncodeDocument(d); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	d = testDocument()
	d.Decisions = make([]Decision, MaxItems)
	for i := range d.Decisions {
		d.Decisions[i] = Decision{ArticleID: testHash(1), RevisionSHA256: testHash(2), State: "classified", Categories: []CategoryDecision{{Key: "science", MethodKey: "fixture", Evidence: json.RawMessage(`{"text":"` + strings.Repeat("x", 3000) + `"}`)}}}
	}
	if _, _, err := EncodeDocument(d); !errors.Is(err, ErrInvalid) {
		t.Fatal("oversized document accepted", err)
	}
	if err := categoryKey("a/b/c/d/e/f/g/h"); err != nil {
		t.Fatal(err)
	}
	if err := categoryKey("a/b/c/d/e/f/g/h/i"); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := NewStore(strings.Repeat("x", 25)); err != nil {
		t.Fatal(err)
	}
	base := ListRequest{Inputs: []PinnedInput{{testID(1), testID(2), testHash(1), testHash(2)}}, ProfileKey: "profile", Limit: 1}
	for _, mutate := range []func(*ListRequest){func(r *ListRequest) { r.Limit = 0 }, func(r *ListRequest) { r.Limit = 101 }, func(r *ListRequest) { r.Inputs = make([]PinnedInput, 17) }, func(r *ListRequest) { r.Filter.EntityNamespace = "wikidata" }, func(r *ListRequest) { at := testTime(); r.Filter.PublishedFrom = &at; r.Filter.PublishedTo = &at }, func(r *ListRequest) { r.Filter.LanguageTag = "en--US" }} {
		r := base
		mutate(&r)
		if _, _, err := normalizeListRequest(r); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid search accepted", err)
		}
	}
}
