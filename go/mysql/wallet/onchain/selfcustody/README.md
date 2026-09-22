# Self-custody on-chain wallet tables

This module supplies initial MySQL 8.4 DDL and a Store composition boundary for
external wallet address linking. It does not own a database connection, know an
account schema, store private keys, verify signatures, or grant trading authority.
It includes address registration, challenge storage/consumption, link completion,
unlink, and subject-scoped reads. Mutations never commit caller transactions.

## Composition

Use `NewStore(addressTable, linkTable, challengeTable)` with application-owned
table names. For Console, pass `"console_" + DefaultAddressTableName`,
`"console_" + DefaultLinkTableName`, and
`"console_" + DefaultLinkChallengeTableName`. The module never adds `console_`.
`NewDefaultStore` uses unprefixed generic names. Constructors validate distinct
SQL identifiers, including MySQL's 64-character limit, and perform no I/O.

`Schema()` returns configured DDL and an error. `CreateTables(ctx, executor)`
accepts the existing `storage/go/api.Executor`; applications own connections and
migrations. MySQL DDL is not transactional. Initial creation is idempotent but
does not upgrade or validate an existing schema. No application migration is
installed or run by this module.

## Tables

- `wallet_onchain_selfcustody_addresses`: stable identity, unique by chain family
  and canonical address. No account/subject field. Network is deliberately not
  part of identity, preventing EVM network selection from bypassing exclusivity.
- `wallet_onchain_selfcustody_links`: subject, address reference, display name,
  linked time and nullable unlinked time. A stored generated column identifies
  active links, with a unique index allowing at most one active subject per
  address. Multiple different addresses can link to the same subject. Closed
  links remain as history; each successful relink inserts a new row rather than
  reassigning an old one. Foreign keys prohibit deleting a referenced address.
- `wallet_onchain_selfcustody_link_challenges`: subject, opaque authentication
  context hash, family/address, unique random nonce, exact message bytes, expiry,
  and consumed time. Challenges may precede creation of an address record.

`subject` is an application-defined, case-sensitive identifier with no account
foreign key. Callers must use a consistent namespace in each store deployment.
Display names belong to links, not addresses. OMS records and PnL are unaffected;
link row IDs are not a new accounting or PnL partition.

Callers must canonicalize and validate addresses before persistence (including
EVM casing), generate unpredictable challenge nonces, bind the exact signed
message to the action/origin/subject/address/expiry, verify signatures, and apply
expiry and replay checks. Binary address collation preserves case-sensitive
chains; the generic schema alone does not prove canonicalization or ownership.
The database expiry constraints validate timestamps, not current authorization.
No raw session tokens, private keys, or seed phrases belong in these tables.

## Store operations

| Method | Contract |
| --- | --- |
| `InsertAddress` | Register a caller-supplied ID and canonical identity; duplicates are errors, never overwrite. |
| `SelectAddress`, `SelectAddressByIdentity` | Read immutable identity; these are trusted internal reads, not account authorization. |
| `InsertLinkChallenge` | Save an unconsumed proof request with nonzero binding hash/nonce and exact message bytes. |
| `SelectLinkChallenge` | Read by challenge ID and subject, including consumed/expired records for inspection. |
| `ConsumeLinkChallenge` | Conditional single-use update matching subject, binding hash, family, address, and validity window in a caller transaction. |
| `CompleteLink` | Lock the address, reject existing ownership, consume the verified challenge, and insert a new link in a caller transaction. |
| `SelectLink` | Read current or historical link by ID and subject. |
| `ListLinks` | Subject-scoped ascending ID cursor, limit 1–100, optional history. |
| `Unlink` | Lock the address and owned link, set its first unlink timestamp; repeated calls preserve that timestamp. |

The application first reads and verifies the stored ownership message. It then
begins a transaction and calls `CompleteLink` with the same challenge identity,
subject, binding and canonical address. Commit only on success. **Roll back on
every error**, including commit errors where rollback is still possible; never
commit partial work after a failed store operation. Consumption and insertion
are atomic only through the caller's transaction. For unknown commit outcomes,
inspect persisted state before attempting a fresh mutation.

IDs are explicit positive caller-generated values. Times must have microsecond
precision and fit MySQL DATETIME; writes use UTC without silently rounding
authorization boundaries. Equality with expiry is expired. Applications own the
clock and signature verification. No secret values are included in store errors;
`errors.Is`/`errors.As` preserve underlying database errors and `sql.ErrNoRows`.
Duplicate writes wrap `ErrDuplicate`; invalid inputs wrap `ErrInvalidParameter`;
unusable proofs or active ownership conflicts wrap `ErrConflict`.

`Unlink` does not determine whether strategies are running. Applications must
perform lifecycle checks and coordinate execution fencing before calling it.
Unlink retains history and does not touch OMS, token approvals, or execution
permissions. No delete or subject-reassignment method is provided.

## Verification

This repository uses nested Go modules. From this directory:

```sh
GOWORK=off go test ./...
GOWORK=off go vet ./...
```

Set `K4K3RU_WALLET_TEST_DSN` to an **isolated** MySQL 8.4 test database with
`parseTime=true&loc=UTC` to include schema integration tests. Tests create and
drop uniquely named tables. They cover exclusivity, multiple wallets per subject,
unlink/relink history, foreign keys, timestamp constraints, repeated creation,
case-sensitive addresses and multiple application table sets. CRUD integration
tests also cover concurrent ownership claims, proof replay/expiry/context,
unauthorized unlink, cursor pagination, idempotent unlink, and rollback after
both successful operations and failed writes.
