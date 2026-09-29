package ammpool

import (
	"testing"
	"time"
)

// TestSenderInvalidationValidation rejects unowned, conflicting or ambiguous invalidation requests.
//
// Version:
//   - 2026-09-29: Added.
func TestSenderInvalidationValidation(t *testing.T) {
	fixture := func() Batch {
		f := senderFixture()
		return Batch{Cursor: f.Cursor, Snapshots: f.Snapshots, SenderInvalidations: []SenderInvalidation{{Pool: f.Snapshots[0].Identity, From: f.Cursor.UpdatedAt.Add(-15 * time.Minute), To: f.Cursor.UpdatedAt.Add(time.Minute), UpdatedAt: f.Cursor.UpdatedAt}}}
	}
	if err := fixture().Validate(); err != nil {
		t.Fatal(err)
	}
	for name, edit := range map[string]func(*Batch){
		"missing parent": func(b *Batch) { b.Snapshots = nil },
		"duplicate":      func(b *Batch) { b.SenderInvalidations = append(b.SenderInvalidations, b.SenderInvalidations[0]) },
		"unaligned":      func(b *Batch) { b.SenderInvalidations[0].From = b.SenderInvalidations[0].From.Add(time.Second) },
		"empty interval": func(b *Batch) { b.SenderInvalidations[0].To = b.SenderInvalidations[0].From },
		"state write":    func(b *Batch) { b.SenderSnapshots = senderFixture().SenderSnapshots },
		"event write":    func(b *Batch) { b.SenderEvents = senderFixture().SenderEvents },
	} {
		t.Run(name, func(t *testing.T) {
			b := fixture()
			edit(&b)
			if err := b.Validate(); err == nil {
				t.Fatal("invalid invalidation accepted")
			}
		})
	}
}
