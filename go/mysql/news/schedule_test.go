package news

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

// TestScheduleSchemaAndValidation verifies additive reviewed DDL and interval boundaries.
//
// Version:
//   - 2026-11-01: Added.
func TestScheduleSchemaAndValidation(t *testing.T) {
	s, err := NewStore("market_hub_")
	if err != nil {
		t.Fatal(err)
	}
	ddl, err := s.ScheduleSchema()
	if err != nil {
		t.Fatal(err)
	}
	proposed, err := os.ReadFile("collection-schedule.proposed.sql")
	if err != nil {
		t.Fatal(err)
	}
	plain := strings.ReplaceAll(strings.ReplaceAll(ddl, "`", ""), "market_hub_", "")
	if plain[strings.Index(plain, "CREATE TABLE"):] != string(proposed)[strings.Index(string(proposed), "CREATE TABLE"):] {
		t.Fatal("schedule DDL differs from approved proposal")
	}
	if strings.Count(ddl, "CREATE TABLE `market_hub_news_source_schedules`") != 1 || !strings.Contains(ddl, "REFERENCES `market_hub_news_sources`") {
		t.Fatal("schedule prefix drift")
	}
	original, err := s.Schema()
	if err != nil || strings.Contains(original, "source_schedules") {
		t.Fatal("original Schema changed", err)
	}
	for _, seconds := range []uint32{59, 60, 604800, 604801} {
		err := validateScheduleSettings(ScheduleSettings{SourceID: "fr-rss", IntervalSeconds: seconds})
		if (err == nil) != (seconds >= 60 && seconds <= 604800) {
			t.Fatalf("interval %d: %v", seconds, err)
		}
	}
	var absent *Store
	if _, err := absent.ScheduleSchema(); !errors.Is(err, ErrInvalid) {
		t.Fatal("nil store accepted", err)
	}
	if _, err := s.GetSchedule(context.Background(), nil, "fr-rss"); !errors.Is(err, ErrInvalid) {
		t.Fatal("nil executor accepted", err)
	}
}

// TestMySQLSchedules verifies CAS, retained deadlines, current reads and failure rollback.
//
// Version:
//   - 2026-11-01: Added.
func TestMySQLSchedules(t *testing.T) {
	s, db := mysqlFixture(t)
	ddl, err := s.ScheduleSchema()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), ddl); err != nil {
		t.Fatal(err)
	}
	seedNews(t, s, db)
	at := testTime()
	settings := ScheduleSettings{SourceID: "fr-rss", IntervalSeconds: 60}
	configure := func(expected uint64, at time.Time) error {
		return transact(t, db, func(tx *sql.Tx) error {
			_, err := s.ConfigureSchedule(t.Context(), tx, settings, expected, at)
			return err
		})
	}
	if err := configure(0, at); err != nil {
		t.Fatal(err)
	}
	deadline := at.Add(48 * time.Hour)
	mustTransact(t, db, func(tx *sql.Tx) error {
		return s.ExtendRetryNotBefore(t.Context(), tx, settings.SourceID, deadline, at.Add(time.Second))
	})
	settings.IntervalSeconds, settings.AutoFetchEnabled = 604800, true
	if err := configure(1, at.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := configure(1, at.Add(3*time.Second)); !errors.Is(err, ErrConflict) {
		t.Fatal("stale CAS accepted", err)
	}
	mustTransact(t, db, func(tx *sql.Tx) error {
		return s.ExtendRetryNotBefore(t.Context(), tx, settings.SourceID, at.Add(time.Hour), at.Add(3*time.Second))
	})
	saved, err := s.GetSchedule(t.Context(), db, settings.SourceID)
	if err != nil || saved.SettingsVersion != 2 || saved.RetryNotBeforeAt == nil || !saved.RetryNotBeforeAt.Equal(deadline) || !saved.CreatedAt.Equal(at) || !saved.UpdatedAt.Equal(at.Add(2*time.Second)) {
		t.Fatalf("deadline/settings changed incorrectly: %+v %v", saved, err)
	}
	for _, seconds := range []uint32{59, 604801} {
		settings.IntervalSeconds = seconds
		if err := configure(2, at.Add(4*time.Second)); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid interval accepted", err)
		}
	}
	settings.IntervalSeconds = 60
	casResults := make(chan error, 2)
	for range 2 {
		go func() { casResults <- configure(2, at.Add(5*time.Second)) }()
	}
	winners, conflicts := 0, 0
	for range 2 {
		if err := <-casResults; err == nil {
			winners++
		} else if errors.Is(err, ErrConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if winners != 1 || conflicts != 1 {
		t.Fatal("concurrent settings update lost CAS exclusion")
	}
	for i := byte(1); i <= 4; i++ {
		run := beginRun(t, s, db, i, settings.SourceID)
		mustTransact(t, db, func(tx *sql.Tx) error {
			return s.FailFetch(t.Context(), tx, Failure{ID: run.ID, FinishedAt: run.StartedAt.Add(time.Second), ErrorCode: "fixture_failure"})
		})
	}
	// Establish an old repeatable-read snapshot, then commit a new attempt elsewhere.
	old, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := old.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Error(err)
		}
	}()
	if _, err := s.GetSchedule(t.Context(), old, settings.SourceID); err != nil {
		t.Fatal(err)
	}
	running := beginRun(t, s, db, 5, settings.SourceID)
	if _, err := s.LockSchedule(t.Context(), old, settings.SourceID); err != nil {
		t.Fatal(err)
	}
	history, err := s.LockFetchHistory(t.Context(), old, settings.SourceID)
	if err != nil || !history.Running || len(history.Attempts) != 3 || history.Attempts[0].ID != running.ID || history.Attempts[2].ID != testID(3) {
		t.Fatalf("history used stale snapshot: %+v %v", history, err)
	}
	if err := old.Rollback(); err != nil {
		t.Fatal(err)
	}
	// A lost failure transaction cannot persist the deadline separately from failure.
	fixtureErr := errors.New("rollback fixture")
	err = transact(t, db, func(tx *sql.Tx) error {
		if err := s.ExtendRetryNotBefore(t.Context(), tx, settings.SourceID, deadline.Add(time.Hour), at.Add(time.Hour)); err != nil {
			return err
		}
		if err := s.FailFetch(t.Context(), tx, Failure{ID: running.ID, FinishedAt: running.StartedAt.Add(time.Second), ErrorCode: "fixture_failure"}); err != nil {
			return err
		}
		return fixtureErr
	})
	if !errors.Is(err, fixtureErr) {
		t.Fatal(err)
	}
	saved, err = s.GetSchedule(t.Context(), db, settings.SourceID)
	if err != nil || saved.RetryNotBeforeAt == nil || !saved.RetryNotBeforeAt.Equal(deadline) {
		t.Fatal("rolled-back deadline persisted", err)
	}
	fetch, err := s.GetFetch(t.Context(), db, running.ID)
	if err != nil || fetch.Status != StatusRunning {
		t.Fatal("rolled-back failure persisted", err)
	}
	for _, query := range []string{
		"UPDATE " + s.table("source_schedules") + " SET interval_seconds=59",
		"UPDATE " + s.table("source_schedules") + " SET settings_version=0",
		"UPDATE " + s.table("source_schedules") + " SET auto_fetch_enabled=2",
		"DELETE FROM " + s.table("sources") + " WHERE source_id='fr-rss'",
	} {
		if _, err := db.ExecContext(t.Context(), query); err == nil {
			t.Fatal("schedule database constraint missing")
		}
	}
}
