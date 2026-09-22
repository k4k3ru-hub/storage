package selfcustody

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

func crudStore(t *testing.T) (*Store, *sql.DB, time.Time) {
	t.Helper()
	dsn := os.Getenv("K4K3RU_WALLET_TEST_DSN")
	if dsn == "" {
		t.Skip("set K4K3RU_WALLET_TEST_DSN to an isolated MySQL database")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	prefix := fmt.Sprintf("crud_%d", time.Now().UnixNano())
	s, err := NewStore(prefix+"_a", prefix+"_l", prefix+"_c")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, n := range []string{s.challengeTable, s.linkTable, s.addressTable} {
			if _, err := db.Exec("DROP TABLE IF EXISTS " + quote(n)); err != nil {
				t.Error(err)
			}
		}
	})
	if err := s.CreateTables(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	return s, db, time.Now().UTC().Truncate(time.Microsecond)
}
func transact(ctx context.Context, db *sql.DB, fn func(*sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	return tx.Commit()
}
func challenge(id uint64, subject, address string, now time.Time) LinkChallenge {
	return LinkChallenge{ID: id, Subject: subject, BindingHash: [32]byte{1}, ChainFamily: "evm", Address: address, Nonce: [32]byte{byte(id)}, Message: []byte("ownership message"), CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
}
func proof(c LinkChallenge, now time.Time) ConsumeChallengeParams {
	return ConsumeChallengeParams{ID: c.ID, Subject: c.Subject, BindingHash: c.BindingHash, ChainFamily: c.ChainFamily, Address: c.Address, Now: now}
}
func mustCRUD(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// TestMySQLCRUD verifies scoped reads, challenge validation, rollback and link history.
//
// Version:
//   - 2026-09-22: Added.
func TestMySQLCRUD(t *testing.T) {
	s, db, now := crudStore(t)
	ctx := t.Context()
	a := Address{ID: 1, ChainFamily: "evm", Address: "0x01", CreatedAt: now}
	mustCRUD(t, s.InsertAddress(ctx, db, a))
	duplicate := a
	duplicate.ID = 2
	if err := s.InsertAddress(ctx, db, duplicate); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("duplicate identity: %v", err)
	}
	read, err := s.SelectAddressByIdentity(ctx, db, a.ChainFamily, a.Address)
	mustCRUD(t, err)
	if read.ID != a.ID {
		t.Fatal(read)
	}
	if _, err := s.SelectAddress(ctx, db, 99); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	c := challenge(1, "alice", a.Address, now)
	mustCRUD(t, s.InsertLinkChallenge(ctx, db, c))
	if _, err := s.SelectLinkChallenge(ctx, db, 1, "bob"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	for _, change := range []func(*ConsumeChallengeParams){
		func(p *ConsumeChallengeParams) { p.Subject = "bob" }, func(p *ConsumeChallengeParams) { p.BindingHash = [32]byte{2} },
		func(p *ConsumeChallengeParams) { p.Address = "other" }, func(p *ConsumeChallengeParams) { p.ChainFamily = "solana" },
		func(p *ConsumeChallengeParams) { p.Now = c.ExpiresAt }, func(p *ConsumeChallengeParams) { p.Now = now.Add(-time.Second) },
	} {
		p := proof(c, now)
		change(&p)
		err := transact(ctx, db, func(tx *sql.Tx) error { return s.ConsumeLinkChallenge(ctx, tx, p) })
		if !errors.Is(err, ErrConflict) {
			t.Fatalf("invalid proof: %v", err)
		}
	}
	abort := errors.New("rollback test")
	p := CompleteLinkParams{ID: 1, AddressID: 1, DisplayName: "Primary", Challenge: proof(c, now)}
	err = transact(ctx, db, func(tx *sql.Tx) error {
		_, err := s.CompleteLink(ctx, tx, p)
		if err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatal(err)
	}
	current, err := s.SelectLinkChallenge(ctx, db, 1, "alice")
	mustCRUD(t, err)
	if current.ConsumedAt != nil {
		t.Fatal("rollback consumed challenge")
	}
	if _, err := s.SelectLink(ctx, db, 1, "alice"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("rollback persisted link", err)
	}
	mustCRUD(t, transact(ctx, db, func(tx *sql.Tx) error { _, err := s.CompleteLink(ctx, tx, p); return err }))
	if err := transact(ctx, db, func(tx *sql.Tx) error { return s.ConsumeLinkChallenge(ctx, tx, proof(c, now)) }); !errors.Is(err, ErrConflict) {
		t.Fatal("replayed proof", err)
	}
	if err := transact(ctx, db, func(tx *sql.Tx) error { return s.Unlink(ctx, tx, 1, "bob", now) }); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("foreign unlink", err)
	}
	mustCRUD(t, transact(ctx, db, func(tx *sql.Tx) error { return s.Unlink(ctx, tx, 1, "alice", now.Add(time.Second)) }))
	mustCRUD(t, transact(ctx, db, func(tx *sql.Tx) error { return s.Unlink(ctx, tx, 1, "alice", now.Add(2*time.Second)) }))
	old, err := s.SelectLink(ctx, db, 1, "alice")
	mustCRUD(t, err)
	if old.UnlinkedAt == nil || !old.UnlinkedAt.Equal(now.Add(time.Second)) {
		t.Fatal("unlink not idempotent")
	}
	c2 := challenge(2, "bob", a.Address, now)
	mustCRUD(t, s.InsertLinkChallenge(ctx, db, c2))
	p2 := CompleteLinkParams{ID: 2, AddressID: 1, Challenge: proof(c2, now.Add(3*time.Second))}
	mustCRUD(t, transact(ctx, db, func(tx *sql.Tx) error { _, err := s.CompleteLink(ctx, tx, p2); return err }))
	active, err := s.ListLinks(ctx, db, "alice", 0, 10, false)
	mustCRUD(t, err)
	if len(active) != 0 {
		t.Fatal(active)
	}
	history, err := s.ListLinks(ctx, db, "alice", 0, 10, true)
	mustCRUD(t, err)
	if len(history) != 1 || history[0].Subject != "alice" {
		t.Fatal(history)
	}
	active, err = s.ListLinks(ctx, db, "bob", 0, 1, false)
	mustCRUD(t, err)
	if len(active) != 1 || active[0].DisplayName != "" {
		t.Fatal(active)
	}
	active, err = s.ListLinks(ctx, db, "bob", 2, 1, false)
	mustCRUD(t, err)
	if len(active) != 0 {
		t.Fatal("cursor ignored")
	}
	// A failed insert after consumption must roll back that consumption too.
	mustCRUD(t, s.InsertAddress(ctx, db, Address{ID: 2, ChainFamily: "evm", Address: "0x02", CreatedAt: now}))
	c3 := challenge(3, "bob", "0x02", now)
	mustCRUD(t, s.InsertLinkChallenge(ctx, db, c3))
	err = transact(ctx, db, func(tx *sql.Tx) error {
		_, err := s.CompleteLink(ctx, tx, CompleteLinkParams{ID: 2, AddressID: 2, Challenge: proof(c3, now)})
		return err
	})
	if !errors.Is(err, ErrDuplicate) {
		t.Fatal(err)
	}
	current, err = s.SelectLinkChallenge(ctx, db, 3, "bob")
	mustCRUD(t, err)
	if current.ConsumedAt != nil {
		t.Fatal("failed link consumed proof")
	}
	mustCRUD(t, transact(ctx, db, func(tx *sql.Tx) error {
		_, err := s.CompleteLink(ctx, tx, CompleteLinkParams{ID: 3, AddressID: 2, Challenge: proof(c3, now)})
		return err
	}))
	active, err = s.ListLinks(ctx, db, "bob", 0, 10, false)
	mustCRUD(t, err)
	if len(active) != 2 {
		t.Fatal("multiple wallets not supported")
	}
}

// TestMySQLConcurrentLink verifies exclusive ownership under competing transactions.
//
// Version:
//   - 2026-09-22: Added.
func TestMySQLConcurrentLink(t *testing.T) {
	s, db, now := crudStore(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	mustCRUD(t, s.InsertAddress(ctx, db, Address{ID: 1, ChainFamily: "evm", Address: "0x01", CreatedAt: now}))
	proofs := []LinkChallenge{challenge(1, "alice", "0x01", now), challenge(2, "bob", "0x01", now)}
	for _, c := range proofs {
		mustCRUD(t, s.InsertLinkChallenge(ctx, db, c))
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, c := range proofs {
		go func() {
			<-start
			results <- transact(ctx, db, func(tx *sql.Tx) error {
				_, err := s.CompleteLink(ctx, tx, CompleteLinkParams{ID: c.ID, AddressID: 1, Challenge: proof(c, now)})
				return err
			})
		}()
	}
	close(start)
	wins, conflicts := 0, 0
	for range proofs {
		err := <-results
		if err == nil {
			wins++
		} else if errors.Is(err, ErrConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if wins != 1 || conflicts != 1 {
		t.Fatalf("wins=%d conflicts=%d", wins, conflicts)
	}
	var consumed int
	mustCRUD(t, db.QueryRow("SELECT COUNT(*) FROM "+quote(s.challengeTable)+" WHERE consumed_at IS NOT NULL").Scan(&consumed))
	if consumed != 1 {
		t.Fatal("losing request consumed proof")
	}
}
