package oms

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// TestPerpetualVenueAccountSnapshotValidation rejects invalid scope and non-object restart state.
//
// Version:
//   - 2026-10-04: Added.
func TestPerpetualVenueAccountSnapshotValidation(t *testing.T) {
	for _, name := range []string{"", "bad;table", "bad table"} {
		if _, err := NewPerpetualVenueAccountSnapshotStore(name); err == nil {
			t.Fatal("invalid table accepted")
		}
	}
	scope := PerpetualVenueAccountScope{AccountID: 1, Venue: "hyperliquid", Network: "testnet", TradingAccount: "wallet"}
	must(t, scope.Validate())
	scope.AccountID = 0
	if err := scope.Validate(); !errors.Is(err, ErrInvalidParameter) {
		t.Fatal(err)
	}
	for _, raw := range []string{"null", "[]", "{", ""} {
		if err := (PerpetualVenueAccountCheckpoint{SnapshotVersion: 1, ObservedThroughMS: 100, Snapshot: json.RawMessage(raw)}).Validate(); !errors.Is(err, ErrInvalidParameter) {
			t.Fatal("invalid state accepted", err)
		}
	}
}

// TestMySQLPerpetualVenueAccountSnapshots verifies restore, tenant isolation, conflicts and rollback.
//
// Version:
//   - 2026-10-04: Added.
func TestMySQLPerpetualVenueAccountSnapshots(t *testing.T) {
	dsn := os.Getenv("K4K3RU_OMS_TEST_DSN")
	if dsn == "" {
		t.Skip("set K4K3RU_OMS_TEST_DSN to an isolated MySQL database")
	}
	db, err := sql.Open("mysql", dsn)
	must(t, err)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	s, err := NewPerpetualVenueAccountSnapshotStore(fmt.Sprintf("oms_venue_snapshot_%d", GenerateOrderID()))
	must(t, err)
	must(t, s.CreateTable(t.Context(), db))
	t.Cleanup(func() {
		if _, err := db.Exec("DROP TABLE " + quoted(s.table)); err != nil {
			t.Error(err)
		}
	})
	scope := PerpetualVenueAccountScope{AccountID: 1, Venue: "hyperliquid", Network: "testnet", TradingAccount: "wallet"}
	value := PerpetualVenueAccountCheckpoint{SnapshotVersion: 1, ObservedThroughMS: 100, Snapshot: json.RawMessage(`{"fees":{"numerator":"9","denominator":"4"},"unknown":null}`)}
	save := func(revision uint64, c PerpetualVenueAccountCheckpoint) error {
		return withTx(t.Context(), db, func(tx *sql.Tx) error { return s.Save(t.Context(), tx, scope, revision, c) })
	}
	must(t, save(0, value))
	duplicate := save(0, value)
	var mysqlError *mysql.MySQLError
	if !errors.Is(duplicate, ErrConflict) || !errors.As(duplicate, &mysqlError) || mysqlError.Number != 1062 {
		t.Fatal("insert race accepted or underlying error lost", duplicate)
	}
	read, err := s.Select(t.Context(), db, scope)
	must(t, err)
	if read.Revision != 1 || read.ObservedThroughMS != 100 {
		t.Fatal(read)
	}
	for _, other := range []PerpetualVenueAccountScope{{2, "hyperliquid", "testnet", "wallet"}, {1, "hyperliquid", "mainnet", "wallet"}, {1, "other", "testnet", "wallet"}, {1, "hyperliquid", "testnet", "other"}} {
		if _, err := s.Select(t.Context(), db, other); !errors.Is(err, sql.ErrNoRows) {
			t.Fatal("scope leaked", err)
		}
	}
	value.ObservedThroughMS = 200
	must(t, save(1, value))
	if err := save(1, value); !errors.Is(err, ErrConflict) {
		t.Fatal("stale write accepted", err)
	}
	value.ObservedThroughMS = 99
	if err := save(2, value); !errors.Is(err, ErrConflict) {
		t.Fatal("time regression accepted", err)
	}
	value.ObservedThroughMS = 300
	abort := errors.New("abort")
	err = withTx(t.Context(), db, func(tx *sql.Tx) error {
		if err := s.Save(t.Context(), tx, scope, 2, value); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatal(err)
	}
	fresh, err := NewPerpetualVenueAccountSnapshotStore(s.table)
	must(t, err)
	read, err = fresh.Select(t.Context(), db, scope)
	must(t, err)
	if read.Revision != 2 || read.ObservedThroughMS != 200 {
		t.Fatal("rollback or restart lost state", read)
	}
	var payload map[string]json.RawMessage
	must(t, json.Unmarshal(read.Snapshot, &payload))
	if string(payload["unknown"]) != "null" {
		t.Fatal("unknown changed")
	}
	// Two independent writers of the same revision cannot both publish.
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { results <- save(2, value) }()
	}
	wins, conflicts := 0, 0
	for i := 0; i < 2; i++ {
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
		t.Fatal(wins, conflicts)
	}
}
