package selfcustody

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// TestMySQLSchema verifies wallet identity and link lifecycle constraints in MySQL.
//
// Version:
//   - 2026-09-22: Added.
func TestMySQLSchema(t *testing.T) {
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
	names := [3]string{fmt.Sprintf("wallet_test_%d_addresses", time.Now().UnixNano()), "", ""}
	names[1], names[2] = names[0]+"_links", names[0]+"_challenges"
	s, err := NewStore(names[0], names[1], names[2])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for i := 2; i >= 0; i-- {
			if _, err := db.Exec("DROP TABLE IF EXISTS `" + names[i] + "`"); err != nil {
				t.Error(err)
			}
		}
	})
	if err := s.CreateTables(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateTables(t.Context(), db); err != nil {
		t.Fatal("repeated initial creation:", err)
	}
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := db.Exec(query, args...); err != nil {
			t.Fatal(err)
		}
	}
	reject := func(query string, args ...any) {
		t.Helper()
		if _, err := db.Exec(query, args...); err == nil {
			t.Fatal("invalid write accepted")
		}
	}
	a, l, c := "`"+names[0]+"`", "`"+names[1]+"`", "`"+names[2]+"`"
	exec("INSERT INTO " + a + " (id,chain_family,address,created_at) VALUES (1,'evm','0x01',NOW(6)),(2,'evm','0x02',NOW(6)),(3,'solana','CaseSensitive',NOW(6)),(4,'solana','casesensitive',NOW(6))")
	reject("INSERT INTO " + a + " (id,chain_family,address,created_at) VALUES (5,'evm','0x01',NOW(6))")
	exec("INSERT INTO " + l + " (id,address_id,subject,linked_at) VALUES (1,1,'user-a',NOW(6)),(2,2,'user-a',NOW(6))")
	_, err = db.Exec("INSERT INTO " + l + " (id,address_id,subject,linked_at) VALUES (3,1,'user-b',NOW(6))")
	var duplicate *mysql.MySQLError
	if !errors.As(err, &duplicate) || duplicate.Number != 1062 {
		t.Fatalf("expected duplicate active address: %v", err)
	}
	exec("UPDATE " + l + " SET unlinked_at=NOW(6) WHERE id=1")
	exec("INSERT INTO " + l + " (id,address_id,subject,linked_at) VALUES (3,1,'user-b',NOW(6))")
	var subject string
	if err := db.QueryRow("SELECT subject FROM " + l + " WHERE id=1").Scan(&subject); err != nil {
		t.Fatal(err)
	}
	if subject != "user-a" {
		t.Fatal("historical ownership changed")
	}
	reject("UPDATE " + l + " SET unlinked_at=NULL WHERE id=1")
	reject("DELETE FROM " + a + " WHERE id=1")
	reject("INSERT INTO " + l + " (id,address_id,subject,linked_at) VALUES (4,999,'user-c',NOW(6))")
	reject("UPDATE " + l + " SET unlinked_at='2000-01-01' WHERE id=3")
	now := time.Now().UTC().Truncate(time.Microsecond)
	exec("INSERT INTO "+c+" (id,subject,binding_hash,chain_family,address,nonce,message,created_at,expires_at) VALUES (1,'user-a',?,'evm','0x01',?,?,?,?)", make([]byte, 32), make([]byte, 32), []byte("proof message"), now, now.Add(time.Minute))
	reject("UPDATE " + c + " SET consumed_at=expires_at WHERE id=1")
	reject("UPDATE " + c + " SET expires_at=created_at WHERE id=1")
	reject("UPDATE " + c + " SET message='' WHERE id=1")
	exec("UPDATE "+c+" SET consumed_at=? WHERE id=1", now.Add(time.Second))
	// Explicit table names must allow another store in the same schema.
	other, err := NewStore(names[0]+"_a", names[0]+"_b", names[0]+"_c")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, suffix := range []string{"_c", "_b", "_a"} {
			if _, err := db.Exec("DROP TABLE IF EXISTS `" + names[0] + suffix + "`"); err != nil {
				t.Error(err)
			}
		}
	})
	if err := other.CreateTables(t.Context(), db); err != nil {
		t.Fatal(err)
	}
}
