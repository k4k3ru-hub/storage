# NewPair persistence

This module owns chain-neutral NewPair snapshots, canonical event occurrences and
source checkpoints. It has no dependency on a chain SDK or the public RPC DTOs.
Inject an application-owned `*sql.DB` into `NewStore`; configure the MySQL driver
with `parseTime=true` and use UTC. Constructors do not execute DDL.

`schema.sql` is embedded by `Schema()` and `CreateTables`. `NewStoreWithTablePrefix(db, "market_hub_")` selects application-prefixed
tables; `store.Schema()` returns the matching DDL. The original `NewStore(db)`
uses unprefixed `onchain_amm_pool_new_pair_*` names. This is a table-name change
from the former `onchain_amm_pool_*` tables. An application migration
must apply the configured DDL before starting readers. Do not call CreateTables for each request.

Identifiers and token IDs preserve case. Token IDs are TEXT because Sui coin type
identifiers are not fixed-size EVM addresses. Pool IDs, event transaction IDs and
position IDs are opaque strings. Position numbers support a block, slot or
checkpoint ordinal; the adapter owns the JSON cursor format and recovery rules.

`Commit` performs a cursor revision check, idempotent event writes, snapshot writes
and cursor advancement in a single transaction. Every writer for a source must use
this path. Do not run independent writers for the same pool under different source
keys: their projection state cannot be serialized by the source cursor lock.

For reorgs, the adapter supplies noncanonical event occurrences and replacement
snapshots together. Event IDs include the pool, position ID, transaction and event
index. Canonical status can change; original payloads remain unchanged. Snapshots
are projections, not the sole audit history. Source keys must be stable across
restarts and must not include RPC URLs or credentials.

Retention cleanup is bounded to 100 batches of 1000 rows per table per call. The caller
schedules cleanup and monitors failures/backlog. Cursors remain available even for
blocks without events. Events and snapshots intentionally have independent
retention. This module does not request chain backfill when events are missing.

Amounts in protocol payloads must be decimal strings, never float64. Optional USD
values use DECIMAL(38,18); unknown is NULL. No untrusted token name is used as a SQL
identifier. Table prefixes are validated, identifiers are quoted, and values are bound parameters.

## Observation and evaluation schema (2026-09-19)

`swap_observed_at` and `swap_observed_position_number/id` record an observed swap,
not the first historical swap. `position_kind` identifies a block, slot or
checkpoint. The timestamp and position are either all absent or all present.
`confirmed_at` means the adapter confirmed that observed swap using the chain's
confirmation policy. Confirmation requires an observed position and a canonical
snapshot; it does not require liquidity evidence or a complete historical scan.

`liquidity_usd` and `liquidity_evaluated_at` are either both NULL (unknown) or both
present, including a known zero value. Keep the last successful value and time on
refresh failure or expiry. Freshness and listing eligibility belong to the
application; stale evaluations remain persistable. `state` holds the application's
JSON projection, including valuation method and event transaction coordinates.
The caller must keep that projection consistent with the relational fields.

`Load(since)` selects canonical snapshots by `pool_created_at >= since`, ordered
by creation time and identity. The application supplies `now - 24h` for NewPair.
`Prune` uses creation time regardless of later swaps or valuation updates.
`Events(source, since)` loads canonical history for canonical, unconfirmed pools
created at or after `since`; it is not an event-observation-time cutoff. Confirmed
pools remain available from Get/Load. `Get` itself does not apply age or listing
rules. Deleting snapshots cascades to events; independent source cursors survive.

This is a breaking schema/type update: first-liquidity fields, first-swap fields,
historical event-scan bounds and backfill-abandonment fields are removed. There
is no persisted listing status. The approved initial-migration workflow updates
schema/001 rather than adding 002. `CREATE TABLE IF NOT EXISTS` does **not** upgrade
existing tables. Update the application migration and callers together before
using this version; no running database is changed by editing these files.

Verification: `go test ./...`, `go vet ./...`.
The optional `mysqlintegration` test exercises the new DDL, Commit/Get/Load,
nullable updates, creation-based retention, cursor conflicts and cascade deletion.
It uses a uniquely named temporary database, never application tables. Supply
`AMMPOOL_TEST_DSN` with create/drop-database privileges and `parseTime=true`.
The MySQL driver is test-only. From this module directory, prepare a temporary
module file and run (supply the DSN via your environment, not a tracked file):

```sh
cp go.mod /tmp/ammpool-test.mod
go mod edit -modfile=/tmp/ammpool-test.mod -require=github.com/go-sql-driver/mysql@v1.10.0
go test -mod=mod -modfile=/tmp/ammpool-test.mod -tags=mysqlintegration -run TestMySQLObservationLifecycle -v
```

Validated on 2026-09-19: normal module tests/vet and the opt-in integration test
passed against local Docker MySQL. Application tables were not modified.
