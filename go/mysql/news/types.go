// Package news stores generic, reproducible news observations and annotations.
// Connections, transactions, IDs, times and external acquisition belong to callers.
package news

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const (
	MaxItems         = 1000
	MaxRecordBytes   = 16 << 10
	MaxDocumentBytes = 2 << 20
	MaxRelations     = 16
	MaxPinnedInputs  = 16
	MaxPageSize      = 100
	StatusRunning    = "running"
	StatusComplete   = "complete"
	StatusFailed     = "failed"
)

var (
	ErrInvalid   = errors.New("invalid news input")
	ErrConflict  = errors.New("news state conflict")
	ErrIntegrity = errors.New("news integrity violation")
	ErrNotFound  = errors.New("news record not found")
)

type ID [16]byte
type Hash [32]byte

// String returns a lowercase hexadecimal ID.
//
// Version:
//   - 2026-10-31: Added.
func (id ID) String() string { return hex.EncodeToString(id[:]) }

// MarshalJSON encodes an ID as lowercase hexadecimal.
//
// Version:
//   - 2026-10-31: Added.
func (id ID) MarshalJSON() ([]byte, error) { return json.Marshal(id.String()) }

// UnmarshalJSON decodes a hexadecimal ID.
//
// Version:
//   - 2026-10-31: Added.
func (id *ID) UnmarshalJSON(data []byte) error {
	if id == nil {
		return fmt.Errorf("failed to decode news id: %w: id=null", ErrInvalid)
	}
	var v string
	if err := json.Unmarshal(data, &v); err != nil {
		return fmt.Errorf("failed to decode news id: %w", err)
	}
	b, err := hex.DecodeString(v)
	if err != nil || len(b) != len(id) {
		return fmt.Errorf("failed to decode news id: %w: id=invalid", ErrInvalid)
	}
	copy(id[:], b)
	return nil
}

// String returns a lowercase hexadecimal digest.
//
// Version:
//   - 2026-10-31: Added.
func (h Hash) String() string { return hex.EncodeToString(h[:]) }

// MarshalJSON encodes a digest as lowercase hexadecimal.
//
// Version:
//   - 2026-10-31: Added.
func (h Hash) MarshalJSON() ([]byte, error) { return json.Marshal(h.String()) }

// UnmarshalJSON decodes a hexadecimal digest.
//
// Version:
//   - 2026-10-31: Added.
func (h *Hash) UnmarshalJSON(data []byte) error {
	if h == nil {
		return fmt.Errorf("failed to decode news hash: %w: hash=null", ErrInvalid)
	}
	var v string
	if err := json.Unmarshal(data, &v); err != nil {
		return fmt.Errorf("failed to decode news hash: %w", err)
	}
	b, err := hex.DecodeString(v)
	if err != nil || len(b) != len(h) {
		return fmt.Errorf("failed to decode news hash: %w: hash=invalid", ErrInvalid)
	}
	copy(h[:], b)
	return nil
}

type Publisher struct {
	ID, DisplayName, HomepageURL string
	CreatedAt                    time.Time
}
type Source struct {
	ID, PublisherID, DisplayName, EndpointURL, FormatKey, LanguageHint string
	RegistrationVersion                                                uint64
	Enabled                                                            bool
	CreatedAt, UpdatedAt                                               time.Time
}
type Category struct {
	Key, ParentKey, DisplayName string
	Enabled                     bool
	CreatedAt                   time.Time
}
type SourceSnapshot struct {
	SourceID            string `json:"source_id"`
	PublisherID         string `json:"publisher_id"`
	PublisherName       string `json:"publisher_name"`
	PublisherHomepage   string `json:"publisher_homepage"`
	DisplayName         string `json:"display_name"`
	EndpointURL         string `json:"endpoint_url"`
	FormatKey           string `json:"format_key"`
	LanguageHint        string `json:"language_hint,omitempty"`
	RegistrationVersion uint64 `json:"registration_version"`
}
type FetchStart struct {
	ID                       ID
	SourceID                 string
	RegistrationVersion      uint64
	ParserKey, ParserVersion string
	StartedAt                time.Time
}
type Fetch struct {
	FetchStart
	PublisherID    string
	Snapshot       SourceSnapshot
	SnapshotJSON   []byte
	FinishedAt     *time.Time
	Status         string
	ItemCount      int
	ResponseSHA256 *Hash
	ErrorCode      string
}
type Identity struct {
	PublisherID, Namespace string
	Value                  []byte
}
type Record struct {
	Version         uint16          `json:"version"`
	Title           string          `json:"title"`
	URL             string          `json:"url"`
	Summary         string          `json:"summary,omitempty"`
	LanguageTag     string          `json:"language_tag,omitempty"`
	PublishedAt     *time.Time      `json:"published_at,omitempty"`
	SourceUpdatedAt *time.Time      `json:"source_updated_at,omitempty"`
	Authors         []string        `json:"authors,omitempty"`
	ImageURL        string          `json:"image_url,omitempty"`
	EventAt         *time.Time      `json:"event_at,omitempty"`
	Attributes      json.RawMessage `json:"attributes,omitempty"`
}
type Revision struct {
	ArticleID, SHA256            Hash
	RecordVersion                uint16
	LanguageTag                  string
	PublishedAt, SourceUpdatedAt *time.Time
	FirstObservedAt              time.Time
	RecordJSON                   []byte
}
type FetchItem struct {
	Identity      Identity
	SourceItemKey []byte
	Revision      Revision
}
type FetchCompletion struct {
	ID             ID
	FinishedAt     time.Time
	ResponseSHA256 Hash
	Items          []FetchItem
}
type Failure struct {
	ID         ID
	FinishedAt time.Time
	ErrorCode  string
}
type AnnotationStart struct {
	ID, RunID                                ID
	ProfileKey, ProducerKey, ProducerVersion string
	DocumentVersion                          uint16
	StartedAt                                time.Time
}
type Annotation struct {
	AnnotationStart
	FinishedAt          *time.Time
	Status              string
	CoveredArticleCount int
	DocumentJSON        []byte
	DocumentSHA256      *Hash
	ErrorCode           string
}
type CategoryDecision struct {
	Key       string          `json:"key"`
	MethodKey string          `json:"method_key"`
	Evidence  json.RawMessage `json:"evidence"`
}
type EntityDecision struct {
	Namespace string          `json:"namespace"`
	Key       string          `json:"key"`
	MethodKey string          `json:"method_key"`
	Evidence  json.RawMessage `json:"evidence"`
}
type Decision struct {
	ArticleID      Hash               `json:"article_id"`
	RevisionSHA256 Hash               `json:"revision_sha256"`
	State          string             `json:"state"`
	Reason         string             `json:"reason,omitempty"`
	Categories     []CategoryDecision `json:"categories"`
	Entities       []EntityDecision   `json:"entities"`
}
type Document struct {
	Version         uint16     `json:"version"`
	RunID           ID         `json:"run_id"`
	ProfileKey      string     `json:"profile_key"`
	ProducerKey     string     `json:"producer_key"`
	ProducerVersion string     `json:"producer_version"`
	Decisions       []Decision `json:"decisions"`
}
type AnnotationCompletion struct {
	ID             ID
	FinishedAt     time.Time
	DocumentJSON   []byte
	DocumentSHA256 Hash
}
type PinnedInput struct {
	RunID, AnnotationSetID         ID
	ResponseSHA256, DocumentSHA256 Hash
}
type Filter struct {
	CategoryKey                             string
	IncludeDescendants                      bool
	EntityNamespace, EntityKey, LanguageTag string
	PublishedFrom, PublishedTo              *time.Time
}
type Cursor struct {
	Scope       Hash
	PublishedAt *time.Time
	ArticleID   Hash
	RunID       ID
}
type ListRequest struct {
	Inputs     []PinnedInput
	ProfileKey string
	Filter     Filter
	Limit      int
	After      *Cursor
}
type Observation struct {
	RunID, AnnotationSetID ID
	SourceID, PublisherID  string
	SourceSnapshot         SourceSnapshot
	SourceItemKey          []byte
	Revision               Revision
	Decision               Decision
}
type Page struct {
	Observations                 []Observation
	Total, UnknownPublishedCount int
	HasMore                      bool
	Next                         *Cursor
	Scope                        Hash
}
type Ready struct {
	SourceID                string
	LatestAttempt           *Fetch
	LatestAnnotationAttempt *Annotation
	Input                   *PinnedInput
	SelectedFetch           *Fetch
	SelectedAnnotation      *Annotation
	Stale                   bool
}
