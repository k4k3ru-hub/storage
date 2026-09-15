# AMM pool persistence

This module owns chain-neutral pool snapshots, canonical event occurrences and
source checkpoints. It has no dependency on a chain SDK or the public RPC DTOs.
Inject an application-owned `*sql.DB` into `NewStore`; configure the MySQL driver
with `parseTime=true` and use UTC. Constructors do not execute DDL.

`schema.sql` is embedded by `Schema()` and `CreateTables`. `NewStoreWithTablePrefix(db, "market_hub_")` selects application-prefixed
tables; `store.Schema()` returns the matching DDL. The original `NewStore(db)`
retains the unprefixed names for compatibility. An application migration
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
retention; once events expire, full reconstruction requires chain backfill.

Amounts in protocol payloads must be decimal strings, never float64. Optional USD
values use DECIMAL(38,18); unknown is NULL. No untrusted token name is used as a SQL
identifier. Table prefixes are validated, identifiers are quoted, and values are bound parameters.

Verification: `go test ./...`, `go vet ./...`. The MarketHub package contains an
opt-in MySQL integration test covering DDL, stale writers, rollback and orphaning.
