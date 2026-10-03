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

## Minute activity snapshots (2026-09-27)

`onchain_amm_pool_new_pair_activity_minutes` stores sparse minute snapshots under
`(pool_id, minute_started_at)`. `pool_id` references the parent snapshot's digest,
not its public Pool address. Amounts and counts inside `totals` remain decimal
strings; callers own the payload schema. No empty minutes are preallocated.

Supply changed `Batch.ActivityMinutes` together with their parent snapshots.
`Commit` replaces these minute totals in the same transaction as lifetime totals,
event deduplication evidence and the source cursor. A retry cannot add the same
contribution twice. `Batch.ResetActivity` clears a Pool's previous creation
generation before writing replacement minutes. Each batch accepts at most 1,441
minute rows and each totals payload is bounded to 16 KiB.

`ActivityMinutes` reads one Pool's `[from,to)` interval. `WalkActivityMinutes`
streams a source's retained rows in one query; the source key must match the
parent projection's `source` string. `PruneActivityMinutes` bounds retention
independently of the parent's monitoring lifetime. Parent deletion also cascades
to minute rows. Apply the additional DDL before enabling these readers.

The optional `TestMySQLActivityAtomicity` integration test covers exact large
counts, cursor conflict, rollback after a parent write, correction replacement,
source isolation, creation reset and cascade deletion.

## Short-window transaction senders (2026-09-28)

Three additional tables store the minimum durable evidence for 5m/15m sender
counts. They do not store address arrays in the parent JSON or minute totals.

| Table suffix | Ownership and contents |
|---|---|
| `sender_snapshots` | One row per parent Pool: creation evidence digest, generation and invalid minute interval |
| `sender_transactions` | One shared network transaction: sender, retry reservation, deadline and deletion eligibility |
| `sender_events` | Admitted Swap occurrence: generation, transaction reference, position, minute, direction and canonical flag |

Names begin with `onchain_amm_pool_new_pair_`; MarketHub supplies `market_hub_`
through `NewStoreWithTablePrefix`. Identifiers use the existing length-delimited
SHA-256 convention. `SenderEvent.ID()` equals `Event.ID()` for the same occurrence;
`SenderTransactionKey.ID()` excludes the Venue and Pool so different Pools can
share a lookup. The adapter normalizes EVM identifiers; storage preserves case.

### Atomic admission and corrections

Add `Batch.SenderSnapshots`, `SenderTransactions`, `SenderEvents`, and, when
rebuilding, `ResetSenders` to the existing `Commit`. Every affected Pool must also
have its parent in `Batch.Snapshots`. `SenderEvents` alone updates retained rows;
it never inserts a missing row, even if `Batch.Events` repeats the original event.
A timestamp-only correction of an existing row need not repeat the raw event.

Use explicit, disjoint subsets of `SenderEvent.ID()` for these operations:

| Batch field | Effect |
|---|---|
| `AdmitSenderEvents` | Permit first insertion for newly accepted Swap occurrences; require matching canonical `Batch.Events` evidence |
| `ReacceptSenderEvents` | Permit a retained canceled occurrence to become canonical after the adapter verifies chain re-adoption; require matching canonical `Batch.Events` evidence |

Only explicit admissions may supply `SenderTransactions` for initialization.
Matching proof may be supplied through `Batch.SenderEvidence` instead of
`Batch.Events`. It is validated in the same transaction but is never inserted
into the long-lived raw event archive. Its identities must refer to sender rows
in this batch; conflicting archived and dedicated proof is rejected. Existing
`Batch.Events` callers remain compatible. This avoids a second Swap archive
when Activity already owns the durable observation in its snapshot/minute rows.

Ordinary corrections and reacceptances reuse retained transaction references
and cannot create a new lookup. Reacceptance does not recreate a missing or
logically expired occurrence. The adapter must not mark delayed metadata or
duplicate history as a new admission.

Cancellation requires the matching noncanonical event. It preserves the saved
minute and expiry, ignoring accompanying time corrections, so a late header
cannot block the cancellation or extend retention. Activity/cursor and sender
corrections commit or roll back together.

New collection generations start at one. Changing creation evidence requires an
explicit reset with the previous generation plus one; the transaction deletes
old children before updating the parent. Within a generation, initialization
and creation identity are immutable. A live invalid interval can only expand;
it can be cleared after leaving the completed 15-minute window. Determining
which intervals lost integrity and which Pools are listed belongs to MarketHub.

Initial transaction rows have zero attempts and are pending, or resolved when
the caller already has a verified sender. Duplicate admission preserves the
first row's sender, attempts, status and deadlines. It never reopens an abandoned
transaction. A contradictory resolved sender is rejected. Ordinary replays and
late lookup results cannot make canceled occurrences canonical; only explicit
reacceptance with current Activity evidence may do that.

Known event expiry is `minute_started_at + 16 minutes`; unknown-minute expiry is
`observed_at + 2 minutes`. Minute adoption must happen before both the old and
new expiry. Corrections arriving at/after that deadline are skipped even before
GC, without failing an otherwise valid Activity commit. Callers must apply the
same lifetime checks before adopting corrections into memory: Commit success
does not mean a missing or expired sender row was updated. Original observation
time and occurrence identity are immutable. Both raw Event timestamps and sender
timestamps are truncated to UTC microseconds before SQL writes; MySQL rounding
must not give the same observation two different stored times. An already
expired or non-admitted cancellation does not recreate a row.

### Lookup reservation and completion

1. After admission commits, call `ReserveSenderAttempt` with the expected attempt
   count and the current time. It locks only the shared transaction row, increments
   the durable count and stores a ten-second lease in `next_attempt_at`, capped
   by the fixed deadline. Send the RPC only after this operation commits.
2. Call `CompleteSenderAttempt` with the returned reservation and a success,
   retry, or abandonment result. It checks the attempt, lease, timestamps and
   fixed lifetime against the stored row. A superseded reservation or terminal
   row returns an inspectable `ErrSenderConflict`.
3. A retry must be scheduled strictly before the fixed deadline. At most four
   reservations are permitted. A crash after reservation consumes that attempt;
   it is never rolled back just because the caller cannot prove it sent the RPC.
4. `ExpireSenderTransactions` abandons elapsed deadlines or exhausted attempts
   after their active lease ends. It changes at most 1,000 rows per call.

`SenderTransaction` reads one shared record for recovery. Lookup operations do
not rewrite Pool JSON or advance its revision. All operations return errors;
this package emits no logs and performs no RPC. MarketHub must provide worker
ownership, the rolling 30-request budget, endpoint limits and the restart delay.
It must recheck actual send eligibility after database/endpoint waits. Concurrent
processes sharing one database need application-level coordination, not just
these row locks.

### Consistent restore, capacity and cleanup

`WalkSenderState` streams parent snapshots, collection metadata, transactions and
occurrences in one read-only repeatable-read transaction. Consumers stage their
results and publish only after the entire method succeeds. On any error, discard
the staged result; partially restored data is not a valid zero count. Callbacks
must not write through the read transaction or retain unaccounted copies.

Specify `SenderRestoreLimits` explicitly. Maximum values are 1,024 Pools, 4,096
transactions, 65,536 events and 4,096 events per Pool. Queries read at most each
global limit plus one; overflow returns `ErrSenderCapacity`, never silent
truncation. Expired, canceled and abandoned rows awaiting cleanup count toward
these bounds. Parent creation evidence and Activity observation compatibility
must still be checked by the application before publishing metrics.

`PruneSenders` deletes up to the supplied limit (maximum 1,000) from each table in
one transaction, events first. A transaction's expiry is deletion eligibility,
not permission to remove its remaining references: `ON DELETE RESTRICT` and the
cleanup query retain referenced rows. `ReleaseSenderSnapshot` conditionally
removes unchanged, childless metadata after invalid intervals leave the window;
the caller must first stop collection for that Pool. Deleting the existing parent
Pool cascades to its collection metadata and events, but not the shared result.

For a live in-memory sender collection, call `PruneEventBatch` outside the sender
lock, then call `PruneSnapshotBatch` and adopt its returned deletions under the
same application lock used by admission and sender GC. Both methods accept a
limit from 1 to 1,000 and execute only one batch. Set a context deadline for each
operation. Snapshot identities are returned only after commit; remove their
events, pending intake and lookup references together. An uncertain parent
deletion error must not be treated as a confirmed empty result. A history-only
failure does not invalidate sender evidence.

`PruneSnapshotBatch` reads each canonical range through the existing
`(is_canonical, pool_created_at, id)` index, collecting up to `limit` candidates
per range. It combines at most `2 * limit` candidates and deletes the oldest
`limit` by creation time and binary ID in one transaction. Both ranges use the
same cutoff and context deadline; a failed query or commit returns no deleted
identities. The cutoff itself is retained. Candidate counts do not bound all
InnoDB range or cascade locks.

MarketHub runs one batch per table per maintenance pass (normally once a minute),
using up to 1,000 history rows with a five-second deadline and up to 64 parent
Pools with a one-second deadline covering both range reads, deletion and commit.
Remaining rows wait for subsequent passes. `Prune` and `PruneWithDeletedSnapshots`
retain their combined, maximum-100-batches-per-table behavior for compatibility.
Do not wrap those entire combined methods in a live sender lock. The latter
reports completed batches even when a later batch fails.

Do not predict deletion from age alone, or retain cascaded children in the
process-local row count. Shared transactions retain their own expiry and
references from other Pools.

When collection or restore is unavailable, include `Batch.SenderInvalidations`
with the ordinary Activity commit for each updated parent. Each request contains
the Pool, a half-open minute interval (`From`, `To`), and `UpdatedAt`. It expands
the existing collection metadata's invalid range without needing a trusted copy
of its generation. It never creates metadata for an unobserved Pool. Normal
sender state/event writes for that same Pool cannot coexist with an invalidation.
The invalidation rolls back with Activity and the source cursor. MarketHub marks
the current minute and preceding 15 minutes, so a missed cancellation cannot
reappear after restart; overlapping windows remain null until the interval ages
out. Lookup work also stops at the outbound boundary while collection is disabled.

The application reserves the agreed admission/queue/row/memory budgets, counts
undeleted records, and schedules cleanup. These SQL APIs do not themselves enforce
the process-wide 32 MiB budget or database-wide admission counts. Restores stream
data so the caller can account for its memory without a second full copy. An
InnoDB file need not shrink when expired rows are deleted.

Apply the new tables before enabling sender readers/writers. Old batches with no
sender fields keep their existing behavior and do not query the new tables. The
embedded schema supports fresh or missing-table creation; it is not an upgrade
mechanism for an incompatible table that already exists. MarketHub migration,
public SDK types, runtime connection and UI work follow separately.

Verification uses `GOWORK=off go test ./...` and `GOWORK=off go vet ./...` when the
enclosing workspace does not list this nested module. The optional MySQL tests
use the temporary-modfile/DSN setup described above; add `-tags=mysqlintegration
-race -count=1 ./...`. They create and drop only uniquely named test databases.
Coverage includes rollback, multiple prefixes, concurrent attempts, terminal
protection, consistent reads during writes, restore overflow, generation reset,
unknown-minute expiry and retention of shared referenced results. Regression
tests additionally cover submicrosecond round trips, explicit canonical
reacceptance, cancellation with late timestamps and corrections after GC.

Validated on 2026-09-29: module tests, vet, build and all opt-in MySQL integration
tests with `-race` passed against local Docker MySQL 8.4. Only disposable test
databases were created and removed; application migrations were not applied.

## Restartable LP checkpoints (2026-10-01)

`onchain_amm_pool_new_pair_lp_checkpoints` holds one replaceable checkpoint per
Pool, under the existing snapshot digest. It references the source cursor and
records a creation-event digest, format version, observation position, JSON
payload, byte count and revision. Parent deletion cascades to the checkpoint;
source deletion is restricted while checkpoints reference it. Creation events
are checked when saving/loading, without extending their archive retention by
adding another foreign key.

The application owns the payload format, LP calculations, supported protocols,
decimals, tick limits and chain validation. A successful storage read is **not**
permission to publish a synchronized LP. Preserve the complete-capture boundary,
last applied event and deduplication evidence in the payload. `Position.Index ==
nil` identifies a complete position; a non-nil value, including `"0"`, identifies
the last applied event within that position. The application must verify that
these relational coordinates agree with the payload and current Pool identity.

`SaveLPCheckpoint` accepts one `LPCheckpointSaveParams`. It locks the source
cursor, verifies `ExpectedCursorRevision`, locks the canonical parent and verifies
`ExpectedParentRevision`, and checks the canonical `created` event's Pool/source.
An existing checkpoint must belong to that same source and creation event. Reset
the old checkpoint before changing its creation generation. Checkpoint writes
increment the **source cursor revision**, preserving its history position. They
do not rewrite the parent JSON or increment the parent's revision.

The checkpoint ownership lookup is a nonlocking read, performed only after the
source, parent and creation-event locks. It is the transaction's first consistent
read, so it sees the state committed before those locks were acquired. Source
serialization and the locked parent protect saves and resets without locking a
missing checkpoint's index gap. This lets unrelated sources insert their first
checkpoints concurrently. Capacity checks still use current locking reads under
the source lock.

Compose the store with the existing constructor. After the application has
validated the LP state against a committed parent, save it using that parent's
revision and the actor's current source revision:

```go
store, err := ammpool.NewStoreWithTablePrefix(db, "market_hub_")
if err != nil {
    return err
}
result, err := store.SaveLPCheckpoint(ctx, ammpool.LPCheckpointSaveParams{
    Source:                 cursor.Source,
    ExpectedCursorRevision: cursor.Revision,
    ExpectedParentRevision: parent.Revision,
    Checkpoint:             checkpoint,
})
if err != nil {
    return err // Reconcile an uncertain commit before continuing the actor.
}
cursor = result.Cursor
```

The saved checkpoint's `revision` is the updated source revision. It does not
reset to one after checkpoint deletion/recreation. Source cursors must remain
durable while a source is in use; resetting them requires discarding the actor
state and restoring it again. Parent price/Activity updates after the save do not
invalidate a checkpoint merely because the parent's revision increased.

`ListLPCheckpointMetadata` requires a source and a limit from 1 to 32. It reads
only small columns, ordered by Pool digest. Pass `NextAfterID` as `AfterPoolID`
for the next page; nil means the end. Expired and noncanonical parents are still
visible for application-scheduled cleanup. The listing does not apply NewPair
publication rules or return LP JSON.

`LoadLPCheckpoints` accepts 1 to 32 unique Pool/revision references and uses one
read-only repeatable-read transaction to select metadata and then JSON. Missing,
changed, noncanonical or creation-evidence-less references are omitted. Metadata
pages may change between calls; each load checks the exact selected revisions.
On any database/validation/capacity error, the entire load result is discarded.
Successful independent earlier loads may still be validated by the application.

The limits are 512 KiB per payload, 512 Pool rows and 64 MiB per source, and 4 MiB
per load. Source quotas include records awaiting cleanup and are enforced inside
the save transaction, under the source lock. All checkpoint writers must use this
API. `payload_bytes` is generated from the UTF-8 JSON text output by MySQL, the
same representation returned by the reader. Input size is also checked, because
JSON normalization may change the byte count. These are payload limits, not
physical InnoDB, index or binlog size limits. Saved byte counts are returned to
the caller; reserve the maximum size until the normalized size is known.

`Batch.ResetLPCheckpoints` explicitly deletes up to 32 source-owned Pool rows
inside normal `Commit`, without requiring another parent JSON write. Absent rows
are a no-op; a row belonging to a different source is a conflict. `Commit` also
automatically deletes checkpoints when a parent becomes noncanonical, their
creation event is canceled, or another canonical creation event is adopted.
An old creation cancellation does not remove a replacement checkpoint. Automatic
deletions follow the existing batch's size and do not impose the explicit-reset
limit on existing event batches. Corrections and deletion roll back together.

`ErrCursorConflict`, `ErrLPCheckpointConflict` and `ErrLPCheckpointCapacity` remain
inspectable with `errors.Is`. Database errors are wrapped, and the library emits
no logs, retries or RPC. A commit error can mean the database committed but its
acknowledgment was lost: no successful receipt is returned. Re-read the cursor
and metadata and reconcile actor ownership before another write; do not merely
adopt another writer's revision while retaining stale actor state.

The application supplies save/recovery deadlines, write frequency and byte
budgets, retention cleanup, and fallback to full LP capture. **Apply the new
table before upgrading writers:** canonical creation events and cancellation
batches use it even before the application begins saving checkpoints. Old normal
snapshot/Activity updates that require no reset do not read or write this table.
Existing List/Get queries never join checkpoint payloads.

The optional `TestMySQLLPCheckpoint*` tests use only the isolated test databases
described above. They cover exact large integers, partial-block cursors, parent
and history preservation, revision replacement, rollback of cancellations,
creation generation changes, source/prefix isolation, row/byte capacity races,
JSON normalization, read snapshot consistency and lost commit acknowledgments.
The independent-source test coordinates both ownership reads before either
INSERT, checks that both saves commit, and verifies source isolation and revisions.
It reproduced MySQL deadlock 1213 before the ownership-read fix and passed three
consecutive runs with `-race` after the fix; the full integration suite also passed.

Validated on 2026-10-01: module tests, vet and build passed. All opt-in MySQL
integration tests passed with `-race` against a disposable MySQL 8.4 container;
the additional direct CHECK/FK constraint test also passed with `-race`. No
running application database or service migration was changed. The MySQL driver
was supplied only through a temporary test modfile; production dependencies did
not change. MarketHub connection, rollout and Agent E2E remain the next stage.
