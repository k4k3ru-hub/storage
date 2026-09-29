//go:build mysqlintegration

package ammpool

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func senderCorrection(b Batch) Batch {
	b.AdmitSenderEvents = nil
	b.ReacceptSenderEvents = nil
	b.SenderTransactions = nil
	return b
}

// TestMySQLSenderTimestampPrecision verifies event/sender round trips and restored cancellations.
//
// Version:
//   - 2026-09-29: Added.
func TestMySQLSenderTimestampPrecision(t *testing.T) {
	for _, offset := range []time.Duration{123456789 * time.Nanosecond, time.Minute - time.Nanosecond} {
		t.Run(offset.String(), func(t *testing.T) {
			s, _ := senderMySQL(t)
			b := senderFixture()
			at := b.Cursor.UpdatedAt.Add(offset).In(time.FixedZone("JST", 9*60*60))
			b.Events[0].ObservedAt = at
			b.Events[0].OccurredAt = at
			b.SenderEvents[0].ObservedAt = at
			b.SenderEvents[0].UpdatedAt = at
			if err := s.Commit(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			raw, err := s.Events(t.Context(), b.Cursor.Source, b.Cursor.UpdatedAt)
			if err != nil || len(raw) != 1 {
				t.Fatal("raw event restore", err)
			}
			_, _, rows := readSenderState(t, s)
			want := at.UTC().Truncate(time.Microsecond)
			if !raw[0].ObservedAt.Equal(want) || !raw[0].OccurredAt.Equal(want) || !rows[0].ObservedAt.Equal(want) {
				t.Fatal("inconsistent observation precision", raw[0], rows[0])
			}
			b = senderCorrection(b)
			b.Cursor.Revision = 1
			b.Events[0] = raw[0]
			b.Events[0].Canonical = false
			b.SenderEvents[0].ObservedAt = raw[0].ObservedAt
			b.SenderEvents[0].UpdatedAt = at.Add(time.Second)
			b.SenderEvents[0].Canonical = false
			b.ActivityMinutes[0].Totals = json.RawMessage(`{"swapCount":"0"}`)
			if err := s.Commit(t.Context(), b); err != nil {
				t.Fatal("restored correction rejected", err)
			}
			assertSenderActivityCommit(t, s, b, 2, "0")
			_, _, rows = readSenderState(t, s)
			if rows[0].Canonical {
				t.Fatal("sender cancellation lost")
			}
		})
	}
}

// TestMySQLSenderExplicitReacceptance distinguishes authoritative reorg adoption from stale replay.
//
// Version:
//   - 2026-09-29: Added.
func TestMySQLSenderExplicitReacceptance(t *testing.T) {
	s, _ := senderMySQL(t)
	b := senderFixture()
	now := b.Cursor.UpdatedAt
	if err := s.Commit(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	b = senderCorrection(b)
	b.Cursor.Revision = 1
	b.Events[0].Canonical = false
	b.SenderEvents[0].Canonical = false
	b.SenderEvents[0].UpdatedAt = now.Add(time.Second)
	b.ActivityMinutes[0].Totals = json.RawMessage(`{"swapCount":"0"}`)
	if err := s.Commit(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	b.Cursor.Revision = 2
	b.Events[0].Canonical = true
	b.SenderEvents[0].Canonical = true
	b.SenderEvents[0].UpdatedAt = now.Add(2 * time.Second)
	b.ActivityMinutes[0].Totals = json.RawMessage(`{"swapCount":"1"}`)
	if err := s.Commit(t.Context(), b); !errors.Is(err, ErrSenderConflict) {
		t.Fatal("ordinary replay restored cancellation", err)
	}
	assertSenderActivityCommit(t, s, b, 2, "0")
	b.ReacceptSenderEvents = [][32]byte{b.SenderEvents[0].ID()}
	proof := b.Events
	b.Events = nil
	if err := s.Commit(t.Context(), b); err == nil {
		t.Fatal("reacceptance without Activity evidence accepted")
	}
	b.Events = proof
	b.Events[0].Canonical = false
	if err := s.Commit(t.Context(), b); err == nil {
		t.Fatal("noncanonical evidence allowed reacceptance")
	}
	b.Events[0].Canonical = true
	b.SenderEvents[0].Generation = 2
	if err := s.Commit(t.Context(), b); !errors.Is(err, ErrSenderConflict) {
		t.Fatal("wrong generation reaccepted", err)
	}
	b.SenderEvents[0].Generation = 1
	if err := s.Commit(t.Context(), b); err != nil {
		t.Fatal("explicit canonical adoption blocked", err)
	}
	assertSenderActivityCommit(t, s, b, 3, "1")
	_, _, events := readSenderState(t, s)
	if !events[0].Canonical || !events[0].ObservedAt.Equal(now) || !events[0].ExpiresAt.Equal(now.Add(16*time.Minute)) {
		t.Fatal("canonical adoption changed immutable lifetime")
	}
	b.Cursor.Revision = 3
	if err := s.Commit(t.Context(), b); err != nil {
		t.Fatal("reacceptance was not idempotent", err)
	}
	_, _, events = readSenderState(t, s)
	if len(events) != 1 {
		t.Fatal("reacceptance duplicated contribution")
	}
}

// TestMySQLSenderLateMinuteCancellation applies cancellation without adopting or extending late metadata.
//
// Version:
//   - 2026-09-29: Added.
func TestMySQLSenderLateMinuteCancellation(t *testing.T) {
	for _, prune := range []bool{false, true} {
		name := "retained"
		if prune {
			name = "pruned"
		}
		t.Run(name, func(t *testing.T) {
			s, _ := senderMySQL(t)
			b := senderFixture()
			now := b.Cursor.UpdatedAt
			b.SenderEvents[0].MinuteStartedAt = nil
			b.SenderEvents[0].ExpiresAt = now.Add(2 * time.Minute)
			if err := s.Commit(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			if prune {
				if _, err := s.PruneSenders(t.Context(), now.Add(2*time.Minute), 1000); err != nil {
					t.Fatal(err)
				}
			}
			b = senderCorrection(b)
			b.Cursor.Revision = 1
			b.Events[0].Canonical = false
			b.SenderEvents[0].Canonical = false
			b.SenderEvents[0].MinuteStartedAt = &now
			b.SenderEvents[0].ExpiresAt = now.Add(16 * time.Minute)
			b.SenderEvents[0].UpdatedAt = now.Add(3 * time.Minute)
			b.ActivityMinutes[0].Totals = json.RawMessage(`{"swapCount":"0"}`)
			if err := s.Commit(t.Context(), b); err != nil {
				t.Fatal("late header prevented cancellation", err)
			}
			assertSenderActivityCommit(t, s, b, 2, "0")
			_, _, events := readSenderState(t, s)
			if prune {
				if len(events) != 0 {
					t.Fatal("cancellation recreated pruned row")
				}
			} else if len(events) != 1 || events[0].Canonical || events[0].MinuteStartedAt != nil || !events[0].ExpiresAt.Equal(now.Add(2*time.Minute)) {
				t.Fatal("cancellation adopted late timestamp or prolonged retention")
			}
		})
	}
}

// TestMySQLSenderLateCorrectionAfterPrune never recreates deleted evidence, even with a raw event.
//
// Version:
//   - 2026-09-29: Added.
func TestMySQLSenderLateCorrectionAfterPrune(t *testing.T) {
	for _, pruneAfter := range []time.Duration{2 * time.Minute, 20 * time.Minute} {
		t.Run(pruneAfter.String(), func(t *testing.T) {
			s, _ := senderMySQL(t)
			b := senderFixture()
			now := b.Cursor.UpdatedAt
			b.SenderEvents[0].MinuteStartedAt = nil
			b.SenderEvents[0].ExpiresAt = now.Add(2 * time.Minute)
			if err := s.Commit(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			if _, err := s.PruneSenders(t.Context(), now.Add(pruneAfter), 1000); err != nil {
				t.Fatal(err)
			}
			b = senderCorrection(b)
			b.Cursor.Revision = 1
			b.SenderEvents[0].MinuteStartedAt = &now
			b.SenderEvents[0].ExpiresAt = now.Add(16 * time.Minute)
			b.SenderEvents[0].UpdatedAt = now.Add(pruneAfter + time.Minute)
			if err := s.Commit(t.Context(), b); err != nil {
				t.Fatal("ignored sender correction blocked Activity", err)
			}
			assertSenderActivityCommit(t, s, b, 2, "1")
			_, transactions, events := readSenderState(t, s)
			if len(events) != 0 || pruneAfter == 20*time.Minute && len(transactions) != 0 {
				t.Fatal("late correction recreated expired data")
			}
			b.Cursor.Revision = 2
			b.ReacceptSenderEvents = [][32]byte{b.SenderEvents[0].ID()}
			if err := s.Commit(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			_, _, events = readSenderState(t, s)
			if len(events) != 0 {
				t.Fatal("reacceptance recreated deleted evidence")
			}
		})
	}
}

func assertSenderActivityCommit(t *testing.T, s *Store, b Batch, revision uint64, count string) {
	t.Helper()
	cursor, err := s.Cursor(t.Context(), b.Cursor.Source)
	if err != nil || cursor.Revision != revision {
		t.Fatal("unexpected cursor revision", cursor.Revision, err)
	}
	minutes, err := s.ActivityMinutes(t.Context(), b.Snapshots[0].Identity, b.ActivityMinutes[0].Start, b.ActivityMinutes[0].Start.Add(time.Minute))
	if err != nil || len(minutes) != 1 {
		t.Fatal("missing Activity minute", err)
	}
	var totals map[string]string
	if err := json.Unmarshal(minutes[0].Totals, &totals); err != nil || totals["swapCount"] != count {
		t.Fatal("Activity correction lost", totals, err)
	}
}
