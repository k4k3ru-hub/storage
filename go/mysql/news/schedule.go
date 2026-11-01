package news

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"regexp"
	"time"

	api "github.com/k4k3ru-hub/storage/go/api"
)

//go:embed collection-schedule.sql
var scheduleSchema string

const (
	MinIntervalSeconds = 60
	MaxIntervalSeconds = 604800
)

type ScheduleSettings struct {
	SourceID         string `json:"sourceId"`
	AutoFetchEnabled bool   `json:"autoFetchEnabled"`
	IntervalSeconds  uint32 `json:"intervalSeconds"`
}

type Schedule struct {
	ScheduleSettings
	SettingsVersion  uint64     `json:"settingsVersion"`
	RetryNotBeforeAt *time.Time `json:"retryNotBeforeAt,omitempty"`
	CreatedAt        time.Time  `json:"createdAt"`
	UpdatedAt        time.Time  `json:"updatedAt"`
}

type FetchAttempt struct {
	ID         ID
	StartedAt  time.Time
	FinishedAt *time.Time
	Status     string
}

type FetchHistory struct {
	Running  bool
	Attempts []FetchAttempt // At most three, ordered by started_at DESC, run_id DESC.
}

// ScheduleSchema returns additive schedule DDL for application-owned migration execution.
// Schema continues to return the original news schema without this table.
//
// Version:
//   - 2026-11-01: Added.
func (s *Store) ScheduleSchema() (string, error) {
	if s == nil || !s.valid {
		return "", invalid("read news schedule schema", "store=null")
	}
	re := regexp.MustCompile(`\bnews_(?:source_schedules|sources)\b`)
	return re.ReplaceAllStringFunc(scheduleSchema, func(name string) string { return "`" + s.prefix + name + "`" }), nil
}

// GetSchedule reads persisted settings without acquiring application scheduling locks.
//
// Version:
//   - 2026-11-01: Added.
func (s *Store) GetSchedule(ctx context.Context, executor api.Executor, sourceID string) (Schedule, error) {
	if err := dependency(s, ctx, executor, "read news schedule"); err != nil {
		return Schedule{}, err
	}
	return s.getSchedule(ctx, executor, sourceID, false)
}

// LockSchedule locks the source followed by its schedule in a caller-owned transaction.
// Acquire these locks before locking fetch rows; any error requires rollback.
//
// Version:
//   - 2026-11-01: Added.
func (s *Store) LockSchedule(ctx context.Context, tx *sql.Tx, sourceID string) (Schedule, error) {
	if err := writeDependency(s, ctx, tx, "lock news schedule"); err != nil {
		return Schedule{}, err
	}
	if _, err := s.getSource(ctx, tx, sourceID, true); err != nil {
		return Schedule{}, fmt.Errorf("failed to lock news schedule: %w", err)
	}
	return s.getSchedule(ctx, tx, sourceID, true)
}

// ConfigureSchedule creates version one for expectedVersion zero or changes settings by CAS.
// It preserves creation time and publisher waiting deadlines; stale versions always conflict.
// The source and schedule are locked in that order. Any error requires rollback.
//
// Version:
//   - 2026-11-01: Added.
func (s *Store) ConfigureSchedule(ctx context.Context, tx *sql.Tx, settings ScheduleSettings, expectedVersion uint64, at time.Time) (Schedule, error) {
	const op = "configure news schedule"
	if err := writeDependency(s, ctx, tx, op); err != nil {
		return Schedule{}, err
	}
	if err := validateScheduleSettings(settings); err != nil {
		return Schedule{}, fmt.Errorf("failed to %s: %w", op, err)
	}
	if !validTime(at) || expectedVersion == ^uint64(0) {
		return Schedule{}, invalid(op, "settings=out_of_range")
	}
	if _, err := s.getSource(ctx, tx, settings.SourceID, true); err != nil {
		return Schedule{}, fmt.Errorf("failed to %s: %w", op, err)
	}
	old, err := s.getSchedule(ctx, tx, settings.SourceID, true)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Schedule{}, fmt.Errorf("failed to %s: %w", op, err)
	}
	if errors.Is(err, ErrNotFound) {
		if expectedVersion != 0 {
			return Schedule{}, conflict(op)
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO "+s.table("source_schedules")+" (source_id,auto_fetch_enabled,interval_seconds,settings_version,created_at,updated_at) VALUES (?,?,?,1,?,?)", settings.SourceID, settings.AutoFetchEnabled, settings.IntervalSeconds, at.UTC(), at.UTC())
		old = Schedule{CreatedAt: at.UTC()}
	} else {
		if old.SettingsVersion != expectedVersion || at.Before(old.UpdatedAt) {
			return Schedule{}, conflict(op)
		}
		var result sql.Result
		result, err = tx.ExecContext(ctx, "UPDATE "+s.table("source_schedules")+" SET auto_fetch_enabled=?,interval_seconds=?,settings_version=?,updated_at=? WHERE source_id=? AND settings_version=?", settings.AutoFetchEnabled, settings.IntervalSeconds, expectedVersion+1, at.UTC(), settings.SourceID, expectedVersion)
		if err == nil {
			err = expectOne(result, op)
		}
	}
	if err != nil {
		return Schedule{}, fmt.Errorf("failed to %s: %w", op, err)
	}
	old.ScheduleSettings, old.SettingsVersion, old.UpdatedAt = settings, expectedVersion+1, at.UTC()
	return old, nil
}

// ExtendRetryNotBefore persists the later publisher deadline without changing settings_version.
// Locks follow source then schedule; call before FailFetch in the same transaction.
// Any error requires rollback.
//
// Version:
//   - 2026-11-01: Added.
func (s *Store) ExtendRetryNotBefore(ctx context.Context, tx *sql.Tx, sourceID string, deadline, at time.Time) error {
	const op = "extend news retry deadline"
	if !validTime(deadline) || !validTime(at) {
		return invalid(op, "time=out_of_range")
	}
	old, err := s.LockSchedule(ctx, tx, sourceID)
	if err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	if at.Before(old.UpdatedAt) {
		return conflict(op)
	}
	if old.RetryNotBeforeAt != nil && !deadline.After(*old.RetryNotBeforeAt) {
		return nil
	}
	_, err = tx.ExecContext(ctx, "UPDATE "+s.table("source_schedules")+" SET retry_not_before_at=?,updated_at=? WHERE source_id=?", deadline.UTC(), at.UTC(), sourceID)
	if err != nil {
		return fmt.Errorf("failed to %s: %w", op, err)
	}
	return nil
}

// LockFetchHistory reads current committed running state and the three latest attempts.
// The caller must first LockSchedule in the same transaction. Reads use FOR UPDATE,
// including a separate running lookup so an older abandoned run cannot be missed.
// Any error requires rollback.
//
// Version:
//   - 2026-11-01: Added.
func (s *Store) LockFetchHistory(ctx context.Context, tx *sql.Tx, sourceID string) (FetchHistory, error) {
	const op = "read news fetch history"
	if err := writeDependency(s, ctx, tx, op); err != nil {
		return FetchHistory{}, err
	}
	if err := token(sourceID, 64, false); err != nil {
		return FetchHistory{}, fmt.Errorf("failed to %s: %w", op, err)
	}
	var history FetchHistory
	var running []byte
	err := tx.QueryRowContext(ctx, "SELECT run_id FROM "+s.table("fetch_runs")+" WHERE active_source_id=? FOR UPDATE", sourceID).Scan(&running)
	if err == nil {
		if len(running) != 16 {
			return history, integrity(op)
		}
		history.Running = true
	} else if !errors.Is(err, sql.ErrNoRows) {
		return history, rowError(err, op)
	}
	rows, err := tx.QueryContext(ctx, "SELECT run_id,started_at,finished_at,status FROM "+s.table("fetch_runs")+" WHERE source_id=? ORDER BY started_at DESC,run_id DESC LIMIT 3 FOR UPDATE", sourceID)
	if err != nil {
		return history, fmt.Errorf("failed to %s: %w", op, err)
	}
	for rows.Next() {
		var attempt FetchAttempt
		var id []byte
		var finished sql.NullTime
		if err := rows.Scan(&id, &attempt.StartedAt, &finished, &attempt.Status); err != nil {
			return history, fmt.Errorf("failed to %s: %w", op, errors.Join(err, rows.Close()))
		}
		if len(id) != 16 {
			return history, errors.Join(integrity(op), rows.Close())
		}
		copy(attempt.ID[:], id)
		attempt.FinishedAt = nullTime(finished)
		history.Attempts = append(history.Attempts, attempt)
	}
	if err := rowsError(rows, op); err != nil {
		return history, err
	}
	return history, nil
}

func validateScheduleSettings(v ScheduleSettings) error {
	if err := token(v.SourceID, 64, false); err != nil {
		return err
	}
	if v.IntervalSeconds < MinIntervalSeconds || v.IntervalSeconds > MaxIntervalSeconds {
		return invalid("validate news schedule", "interval_seconds=out_of_range min_value=60 max_value=604800")
	}
	return nil
}

func (s *Store) getSchedule(ctx context.Context, executor api.Executor, sourceID string, lock bool) (Schedule, error) {
	const op = "read news schedule"
	if err := token(sourceID, 64, false); err != nil {
		return Schedule{}, fmt.Errorf("failed to %s: %w", op, err)
	}
	query := "SELECT source_id,auto_fetch_enabled,interval_seconds,settings_version,retry_not_before_at,created_at,updated_at FROM " + s.table("source_schedules") + " WHERE source_id=?"
	if lock {
		query += " FOR UPDATE"
	}
	var v Schedule
	var retry sql.NullTime
	if err := executor.QueryRowContext(ctx, query, sourceID).Scan(&v.SourceID, &v.AutoFetchEnabled, &v.IntervalSeconds, &v.SettingsVersion, &retry, &v.CreatedAt, &v.UpdatedAt); err != nil {
		return v, rowError(err, op)
	}
	v.RetryNotBeforeAt = nullTime(retry)
	if validateScheduleSettings(v.ScheduleSettings) != nil || v.SourceID != sourceID || v.SettingsVersion == 0 || !validTime(v.CreatedAt) || !validTime(v.UpdatedAt) || v.UpdatedAt.Before(v.CreatedAt) || (v.RetryNotBeforeAt != nil && !validTime(*v.RetryNotBeforeAt)) {
		return Schedule{}, integrity(op)
	}
	return v, nil
}
