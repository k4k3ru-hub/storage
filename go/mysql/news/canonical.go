package news

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// SHA256 returns the SHA-256 fingerprint of exact bytes.
//
// Version:
//   - 2026-10-31: Added.
func SHA256(data []byte) Hash { return Hash(sha256.Sum256(data)) }

// ArticleID derives a publisher-scoped ID from three uint32 big-endian
// length-prefixed byte strings: publisher ID, identity namespace and identity value.
// Identity namespaces must explicitly describe an application-confirmed identity scheme.
//
// Version:
//   - 2026-10-31: Added.
func ArticleID(identity Identity) (Hash, error) {
	if err := validateIdentity(identity); err != nil {
		return Hash{}, fmt.Errorf("failed to derive news article id: %w", err)
	}
	var b bytes.Buffer
	for _, v := range [][]byte{[]byte(identity.PublisherID), []byte(identity.Namespace), identity.Value} {
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(v)))
		b.Write(size[:])
		b.Write(v)
	}
	return SHA256(b.Bytes()), nil
}

// CanonicalURL normalizes an HTTPS URL's host, default port, empty path and fragment.
// Query order, escaping and path segments are retained because publishers may give
// them meaning. Credentials and non-HTTPS URLs are rejected.
//
// Version:
//   - 2026-10-31: Added.
func CanonicalURL(value string) (string, error) {
	if err := text(value, 2048, false); err != nil {
		return "", invalid("canonicalize news url", "url=invalid")
	}
	u, err := url.Parse(value)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Opaque != "" || u.Hostname() == "" {
		return "", invalid("canonicalize news url", "url=invalid")
	}
	for _, r := range u.Host {
		if r > 127 || r <= 32 {
			return "", invalid("canonicalize news url", "host=invalid")
		}
	}
	u.Host = strings.ToLower(u.Host)
	if u.Port() == "443" {
		u.Host = strings.TrimSuffix(u.Host, ":443")
	}
	if u.Path == "" {
		u.Path = "/"
	}
	u.Fragment = ""
	u.RawFragment = ""
	result := u.String()
	if len(result) > 2048 {
		return "", invalid("canonicalize news url", "url=too_long max_length=2048")
	}
	return result, nil
}

// NewRevision validates and canonically encodes a v1 record for an article identity.
// No observation time is part of its hash; callers supply first-observed time when saving.
//
// Version:
//   - 2026-10-31: Added.
func NewRevision(identity Identity, record Record) (Revision, error) {
	id, err := ArticleID(identity)
	if err != nil {
		return Revision{}, fmt.Errorf("failed to create news revision: %w", err)
	}
	r, data, err := encodeRecord(record)
	if err != nil {
		return Revision{}, fmt.Errorf("failed to create news revision: %w", err)
	}
	return Revision{ArticleID: id, SHA256: SHA256(data), RecordVersion: r.Version, LanguageTag: r.LanguageTag, PublishedAt: r.PublishedAt, SourceUpdatedAt: r.SourceUpdatedAt, RecordJSON: data}, nil
}

// EncodeDocument validates and canonically encodes a complete annotation document.
// Decisions and relations are sorted by their stable keys; explicit empty arrays are retained.
// The Store additionally checks exact coverage against the pinned fetch run.
//
// Version:
//   - 2026-10-31: Added.
func EncodeDocument(document Document) ([]byte, Hash, error) {
	d, err := normalizeDocument(document)
	if err != nil {
		return nil, Hash{}, fmt.Errorf("failed to encode news annotation document: %w", err)
	}
	b, err := json.Marshal(d)
	if err != nil {
		return nil, Hash{}, fmt.Errorf("failed to encode news annotation document: %w", err)
	}
	if len(b) > MaxDocumentBytes {
		return nil, Hash{}, invalid("encode news annotation document", "document=too_long max_length=2097152")
	}
	return b, SHA256(b), nil
}

func text(value string, max int, optional bool) error {
	if value == "" && !optional {
		return invalid("validate news text", "value=empty")
	}
	if len(value) > max {
		return invalid("validate news text", fmt.Sprintf("value=too_long max_length=%d", max))
	}
	if !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return invalid("validate news text", "value=invalid")
	}
	return nil
}
func token(value string, max int, optional bool) error {
	if err := text(value, max, optional); err != nil {
		return err
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("_.:/-", r)) {
			return invalid("validate news token", "value=invalid")
		}
	}
	return nil
}
func categoryKey(value string) error {
	if len(value) == 0 {
		return invalid("validate news category", "category_key=empty")
	}
	if len(value) > 255 {
		return invalid("validate news category", "category_key=too_long max_length=255")
	}
	parts := strings.Split(value, "/")
	if len(parts) > 8 {
		return invalid("validate news category", "depth=out_of_range max_value=8")
	}
	for _, p := range parts {
		if p == "" {
			return invalid("validate news category", "category_key=invalid")
		}
		for _, r := range p {
			if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
				return invalid("validate news category", "category_key=invalid")
			}
		}
	}
	return nil
}
func language(value string) error {
	if err := text(value, 35, true); err != nil {
		return err
	}
	if value == "" {
		return nil
	}
	for _, p := range strings.Split(value, "-") {
		if p == "" || len(p) > 8 {
			return invalid("validate news language", "language_tag=invalid")
		}
		for _, r := range p {
			if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
				return invalid("validate news language", "language_tag=invalid")
			}
		}
	}
	return nil
}
func validTime(v time.Time) bool {
	u := v.UTC()
	return !v.IsZero() && u.Year() >= 1000 && u.Year() <= 9999 && v.Nanosecond()%1000 == 0
}
func normalizeTime(v *time.Time) (*time.Time, error) {
	if v == nil {
		return nil, nil
	}
	if !validTime(*v) {
		return nil, invalid("validate news time", "time=out_of_range")
	}
	u := v.UTC()
	return &u, nil
}
func sameTime(a, b *time.Time) bool {
	return a == nil && b == nil || a != nil && b != nil && a.Equal(*b)
}
func validateIdentity(i Identity) error {
	if err := token(i.PublisherID, 64, false); err != nil {
		return fmt.Errorf("failed to validate news identity: %w", err)
	}
	if err := token(i.Namespace, 64, false); err != nil {
		return fmt.Errorf("failed to validate news identity: %w", err)
	}
	if len(i.Value) == 0 || len(i.Value) > 2048 {
		return invalid("validate news identity", "identity_value=out_of_range min_length=1 max_length=2048")
	}
	if i.Namespace == "canonical-url-v1" {
		v, err := CanonicalURL(string(i.Value))
		if err != nil {
			return fmt.Errorf("failed to validate news identity: %w", err)
		}
		if v != string(i.Value) {
			return invalid("validate news identity", "identity_value=invalid")
		}
	}
	return nil
}
func encodeRecord(r Record) (Record, []byte, error) {
	if r.Version != 1 {
		return Record{}, nil, invalid("encode news record", "record_version=invalid")
	}
	for _, f := range []struct {
		v        string
		max      int
		optional bool
	}{{r.Title, 1024, false}, {r.Summary, 8192, true}} {
		if err := text(f.v, f.max, f.optional); err != nil {
			return Record{}, nil, fmt.Errorf("failed to encode news record: %w", err)
		}
	}
	var err error
	r.URL, err = CanonicalURL(r.URL)
	if err != nil {
		return Record{}, nil, fmt.Errorf("failed to encode news record: %w", err)
	}
	if err = language(r.LanguageTag); err != nil {
		return Record{}, nil, fmt.Errorf("failed to encode news record: %w", err)
	}
	if r.ImageURL != "" {
		r.ImageURL, err = CanonicalURL(r.ImageURL)
		if err != nil {
			return Record{}, nil, fmt.Errorf("failed to encode news record: %w", err)
		}
	}
	if len(r.Authors) > 16 {
		return Record{}, nil, invalid("encode news record", "authors=too_long max_length=16")
	}
	r.Authors = append([]string(nil), r.Authors...)
	for _, v := range r.Authors {
		if err := text(v, 256, false); err != nil {
			return Record{}, nil, fmt.Errorf("failed to encode news record: %w", err)
		}
	}
	if r.PublishedAt, err = normalizeTime(r.PublishedAt); err != nil {
		return Record{}, nil, err
	}
	if r.SourceUpdatedAt, err = normalizeTime(r.SourceUpdatedAt); err != nil {
		return Record{}, nil, err
	}
	if r.EventAt, err = normalizeTime(r.EventAt); err != nil {
		return Record{}, nil, err
	}
	if len(r.Attributes) > 0 {
		r.Attributes, err = canonicalObject(r.Attributes, MaxRecordBytes)
		if err != nil {
			return Record{}, nil, fmt.Errorf("failed to encode news record: %w", err)
		}
	}
	b, err := json.Marshal(r)
	if err != nil {
		return Record{}, nil, fmt.Errorf("failed to encode news record: %w", err)
	}
	if len(b) > MaxRecordBytes {
		return Record{}, nil, invalid("encode news record", "record=too_long max_length=16384")
	}
	return r, b, nil
}
func validateRevision(v Revision) error {
	if len(v.RecordJSON) < 2 || len(v.RecordJSON) > MaxRecordBytes {
		return invalid("validate news revision", "record=out_of_range")
	}
	if SHA256(v.RecordJSON) != v.SHA256 {
		return integrity("validate news revision hash")
	}
	var r Record
	if err := decodeStrict(v.RecordJSON, &r); err != nil {
		return fmt.Errorf("failed to validate news revision: %w", err)
	}
	r, b, err := encodeRecord(r)
	if err != nil {
		return fmt.Errorf("failed to validate news revision: %w", err)
	}
	if !equalJSON(b, v.RecordJSON) || r.Version != v.RecordVersion || r.LanguageTag != v.LanguageTag || !sameTime(r.PublishedAt, v.PublishedAt) || !sameTime(r.SourceUpdatedAt, v.SourceUpdatedAt) {
		return integrity("validate news revision projection")
	}
	return nil
}
func decodeStrict(b []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return invalid("decode news json", "document=invalid")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return invalid("decode news json", "document=invalid")
	}
	return nil
}

// canonicalObject rejects duplicate keys and excessive nesting before encoding sorted keys.
func canonicalObject(b []byte, max int) ([]byte, error) {
	if len(b) > max || len(b) < 2 {
		return nil, invalid("canonicalize news json", "document=out_of_range")
	}
	if !utf8.Valid(b) {
		return nil, invalid("canonicalize news json", "document=invalid")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	v, err := jsonValue(d, 0)
	if err != nil {
		return nil, err
	}
	if _, ok := v.(map[string]any); !ok {
		return nil, invalid("canonicalize news json", "document=invalid")
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, invalid("canonicalize news json", "document=invalid")
	}
	out, err := json.Marshal(v)
	if err != nil {
		return nil, invalid("canonicalize news json", "document=invalid")
	}
	if len(out) > max {
		return nil, invalid("canonicalize news json", "document=too_long")
	}
	return out, nil
}
func jsonValue(d *json.Decoder, depth int) (any, error) {
	if depth > 16 {
		return nil, invalid("canonicalize news json", "depth=out_of_range max_value=16")
	}
	t, err := d.Token()
	if err != nil {
		return nil, invalid("canonicalize news json", "document=invalid")
	}
	if delimiter, ok := t.(json.Delim); ok {
		switch delimiter {
		case '{':
			m := map[string]any{}
			for d.More() {
				k, e := d.Token()
				if e != nil {
					return nil, invalid("canonicalize news json", "document=invalid")
				}
				name, ok := k.(string)
				if !ok {
					return nil, invalid("canonicalize news json", "document=invalid")
				}
				if _, exists := m[name]; exists {
					return nil, invalid("canonicalize news json", "duplicate_key=invalid")
				}
				v, e := jsonValue(d, depth+1)
				if e != nil {
					return nil, e
				}
				m[name] = v
			}
			end, e := d.Token()
			if e != nil || end != json.Delim('}') {
				return nil, invalid("canonicalize news json", "document=invalid")
			}
			return m, nil
		case '[':
			a := []any{}
			for d.More() {
				v, e := jsonValue(d, depth+1)
				if e != nil {
					return nil, e
				}
				a = append(a, v)
			}
			end, e := d.Token()
			if e != nil || end != json.Delim(']') {
				return nil, invalid("canonicalize news json", "document=invalid")
			}
			return a, nil
		default:
			return nil, invalid("canonicalize news json", "document=invalid")
		}
	}
	return t, nil
}
func normalizeDocument(d Document) (Document, error) {
	if d.Version != 1 || d.RunID == (ID{}) {
		return Document{}, invalid("validate news annotation document", "document=invalid")
	}
	for _, v := range []string{d.ProfileKey, d.ProducerKey, d.ProducerVersion} {
		if err := token(v, 64, false); err != nil {
			return Document{}, err
		}
	}
	if len(d.Decisions) > MaxItems {
		return Document{}, invalid("validate news annotation document", "decisions=too_long max_length=1000")
	}
	// Reject oversized caller input before cloning or canonicalizing relation data.
	inputBytes := 0
	for _, v := range d.Decisions {
		if len(v.Categories) > MaxRelations || len(v.Entities) > MaxRelations {
			return Document{}, invalid("validate news annotation document", "relations=too_long max_length=16")
		}
		inputBytes += len(v.Reason)
		for _, c := range v.Categories {
			inputBytes += len(c.Key) + len(c.MethodKey) + len(c.Evidence)
		}
		for _, e := range v.Entities {
			inputBytes += len(e.Namespace) + len(e.Key) + len(e.MethodKey) + len(e.Evidence)
		}
		if inputBytes > MaxDocumentBytes {
			return Document{}, invalid("validate news annotation document", "document=too_long max_length=2097152")
		}
	}
	d.Decisions = append([]Decision{}, d.Decisions...)
	for i := range d.Decisions {
		v := &d.Decisions[i]
		if v.ArticleID == (Hash{}) || v.RevisionSHA256 == (Hash{}) {
			return Document{}, invalid("validate news annotation document", "article=empty")
		}
		if v.State != "classified" && v.State != "unclassified" {
			return Document{}, invalid("validate news annotation document", "state=invalid")
		}
		if err := text(v.Reason, 1024, v.State != "unclassified"); err != nil {
			return Document{}, err
		}
		if len(v.Categories) > MaxRelations || len(v.Entities) > MaxRelations {
			return Document{}, invalid("validate news annotation document", "relations=too_long max_length=16")
		}
		if v.State == "unclassified" && (len(v.Categories) > 0 || len(v.Entities) > 0) {
			return Document{}, invalid("validate news annotation document", "unclassified_relations=invalid")
		}
		v.Categories = append([]CategoryDecision{}, v.Categories...)
		v.Entities = append([]EntityDecision{}, v.Entities...)
		for j := range v.Categories {
			c := &v.Categories[j]
			if err := categoryKey(c.Key); err != nil {
				return Document{}, err
			}
			if err := token(c.MethodKey, 32, false); err != nil {
				return Document{}, err
			}
			var err error
			c.Evidence, err = canonicalEvidence(c.Evidence, 4096)
			if err != nil {
				return Document{}, err
			}
		}
		for j := range v.Entities {
			e := &v.Entities[j]
			if err := token(e.Namespace, 64, false); err != nil {
				return Document{}, err
			}
			if err := text(e.Key, 512, false); err != nil {
				return Document{}, err
			}
			if err := token(e.MethodKey, 32, false); err != nil {
				return Document{}, err
			}
			var err error
			e.Evidence, err = canonicalEvidence(e.Evidence, 8192)
			if err != nil {
				return Document{}, err
			}
		}
		sort.Slice(v.Categories, func(i, j int) bool { return v.Categories[i].Key < v.Categories[j].Key })
		for j := 1; j < len(v.Categories); j++ {
			if v.Categories[j-1].Key == v.Categories[j].Key {
				return Document{}, invalid("validate news annotation document", "category=invalid")
			}
		}
		sort.Slice(v.Entities, func(i, j int) bool {
			a, b := v.Entities[i], v.Entities[j]
			if a.Namespace != b.Namespace {
				return a.Namespace < b.Namespace
			}
			return a.Key < b.Key
		})
		for j := 1; j < len(v.Entities); j++ {
			a, b := v.Entities[j-1], v.Entities[j]
			if a.Namespace == b.Namespace && a.Key == b.Key {
				return Document{}, invalid("validate news annotation document", "entity=invalid")
			}
		}
	}
	sort.Slice(d.Decisions, func(i, j int) bool { return keyOrder(d.Decisions[i].ArticleID[:], d.Decisions[j].ArticleID[:]) < 0 })
	for i := 1; i < len(d.Decisions); i++ {
		if d.Decisions[i-1].ArticleID == d.Decisions[i].ArticleID {
			return Document{}, invalid("validate news annotation document", "article=invalid")
		}
	}
	return d, nil
}

// Evidence is projected into MySQL JSON. Integers must fit signed/unsigned 64-bit;
// decimals must round-trip through binary64's shortest decimal spelling without
// changing mathematical value. Values outside that domain belong in JSON strings.
func canonicalEvidence(b []byte, max int) ([]byte, error) {
	canonical, err := canonicalObject(b, max)
	if err != nil {
		return nil, err
	}
	d := json.NewDecoder(bytes.NewReader(canonical))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return nil, invalid("validate news evidence", "evidence=invalid")
	}
	if err := validateEvidenceNumbers(v); err != nil {
		return nil, err
	}
	return canonical, nil
}
func validateEvidenceNumbers(v any) error {
	switch x := v.(type) {
	case map[string]any:
		for _, e := range x {
			if err := validateEvidenceNumbers(e); err != nil {
				return err
			}
		}
	case []any:
		for _, e := range x {
			if err := validateEvidenceNumbers(e); err != nil {
				return err
			}
		}
	case json.Number:
		s := string(x)
		if index := strings.IndexAny(s, "eE"); index >= 0 {
			exponent, err := strconv.ParseInt(s[index+1:], 10, 32)
			if err != nil || exponent < -324 || exponent > 308 {
				return invalid("validate news evidence", "exponent=out_of_range min_value=-324 max_value=308")
			}
		}
		if !strings.ContainsAny(s, ".eE") {
			if strings.HasPrefix(s, "-") {
				if _, err := strconv.ParseInt(s, 10, 64); err != nil {
					return invalid("validate news evidence", "number=out_of_range")
				}
			} else if _, err := strconv.ParseUint(s, 10, 64); err != nil {
				return invalid("validate news evidence", "number=out_of_range")
			}
			return nil
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
			return invalid("validate news evidence", "number=out_of_range")
		}
		actual, ok := new(big.Rat).SetString(s)
		if !ok {
			return invalid("validate news evidence", "number=invalid")
		}
		expected, ok := new(big.Rat).SetString(strconv.FormatFloat(f, 'g', -1, 64))
		if !ok || actual.Cmp(expected) != 0 {
			return invalid("validate news evidence", "number=out_of_range")
		}
	}
	return nil
}
func equivalentEvidence(a, b []byte, max int) bool {
	x, err := canonicalEvidence(a, max)
	if err != nil {
		return false
	}
	y, err := canonicalEvidence(b, max)
	if err != nil {
		return false
	}
	var left, right any
	d := json.NewDecoder(bytes.NewReader(x))
	d.UseNumber()
	if d.Decode(&left) != nil {
		return false
	}
	d = json.NewDecoder(bytes.NewReader(y))
	d.UseNumber()
	if d.Decode(&right) != nil {
		return false
	}
	return sameJSONValue(left, right)
}
func sameJSONValue(a, b any) bool {
	switch x := a.(type) {
	case json.Number:
		y, ok := b.(json.Number)
		if !ok {
			return false
		}
		left, ok := new(big.Rat).SetString(string(x))
		if !ok {
			return false
		}
		right, ok := new(big.Rat).SetString(string(y))
		return ok && left.Cmp(right) == 0
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for k, v := range x {
			other, ok := y[k]
			if !ok || !sameJSONValue(v, other) {
				return false
			}
		}
		return true
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i, v := range x {
			if !sameJSONValue(v, y[i]) {
				return false
			}
		}
		return true
	default:
		return a == b
	}
}
func decodeDocument(data []byte, hash Hash) (Document, error) {
	if len(data) > MaxDocumentBytes || len(data) < 2 {
		return Document{}, invalid("decode news annotation document", "document=out_of_range")
	}
	if SHA256(data) != hash {
		return Document{}, integrity("validate news annotation hash")
	}
	var d Document
	if err := decodeStrict(data, &d); err != nil {
		return Document{}, err
	}
	b, h, err := EncodeDocument(d)
	if err != nil {
		return Document{}, err
	}
	if h != hash || !equalJSON(b, data) {
		return Document{}, integrity("validate news annotation encoding")
	}
	return d, nil
}
