package ammpool

import (
	"context"
	"testing"
	"time"
)

// TestPruneBatchBounds rejects invalid bounds before accessing the database.
//
// Version:
//   - 2026-09-29: Added.
func TestPruneBatchBounds(t *testing.T) {
	s := &Store{}
	for _, v := range []struct {
		before time.Time
		limit  int
	}{{time.Time{}, 1}, {time.Now(), 0}, {time.Now(), -1}, {time.Now(), 1001}} {
		if _, err := s.PruneEventBatch(context.Background(), v.before, v.limit); err == nil {
			t.Fatal("invalid history bounds accepted")
		}
		if _, err := s.PruneSnapshotBatch(context.Background(), v.before, v.limit); err == nil {
			t.Fatal("invalid parent bounds accepted")
		}
	}
}
