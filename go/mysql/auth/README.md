# MySQL authentication stores

`OTPStore` persists one-time password challenges. `WebSessionStore` persists
authenticated browser sessions identified by opaque cookie tokens.

## WebSessionStore

Construct the store with an application-owned table name:

```go
store, err := auth.NewWebSessionStore("console_web_sessions")
```

Handle the construction error before using the store. Its methods accept an
`Executor` (`*sql.DB` or `*sql.Tx`), preserving caller-owned connections and
transactions. `CreateTable` supplies the initial DDL; application migrations
own deployment and subsequent schema changes. It never alters an existing table.

The table contains `id`, `session_token_hash`, `subject`, `created_at`,
`last_activity_at`, `expires_at`, `absolute_expires_at`, and nullable `revoked_at`.
The token hash is a unique, lowercase hexadecimal SHA-256 digest. The subject
is an application-defined identifier (Console uses a decimal CRM account ID),
with no foreign key to another application's database. Subject comparisons use
`utf8mb4_bin`. The expiry index supports future application-owned cleanup;
this store does not automatically delete session history.

- `Insert` accepts explicit timestamps and generates an omitted record ID.
  It preserves input params. Duplicate IDs or hashes wrap both `ErrDuplicateKey`
  and the underlying MySQL error. It does not replace existing sessions.
- `SelectActiveByTokenHash` checks creation time, idle and absolute expiry, and
  revocation. Unknown or inactive sessions return `(nil, nil)`. Reads do not
  renew sessions. Equality with an expiry timestamp means expired.
- `Touch` advances qualifying activity using a conditional SQL update. It caps
  expiry at the stored absolute limit, never shortens an existing expiry, and
  rejects expired/revoked sessions. Older or equal activity timestamps do not
  overwrite newer activity. Its boolean reports an update, not authentication;
  false can mean either unchanged or invalid. Use one consistent idle timeout
  policy for each session's lifetime.
- `RevokeByTokenHash` sets the first revocation timestamp idempotently. It can
  revoke expired sessions, returns false for missing/already revoked sessions,
  and never resets activity or expiry.

Use UTC microsecond timestamps, for example
`now := time.Now().UTC().Truncate(time.Microsecond)`. Timestamps with finer
precision or outside MySQL's DATETIME range are rejected to prevent silent
rounding of authorization boundaries. Configure MySQL connections with
`parseTime=true`, `loc=UTC`, and strict SQL mode.

The application owns cryptographic token generation and SHA-256 hashing,
Cookie attributes, CSRF protection, authentication, account authorization, and
expiry policy. No raw tokens or browser credentials belong in this table.
Session validity does not independently establish current account entitlement.

For Console, create a session only after successful CRM OTP authentication:
initial idle expiry is `now + 24h`, absolute expiry is `now + 7d`, and qualifying
user requests call `Touch` with `IdleTimeout: 24*time.Hour`. Background polling
does not call `Touch`. Logout revokes the session and clears its Cookie.
Concurrent revocation cannot be undone by Touch; requests already authorized
before revocation may still be in flight.

This module does not create Console migrations, connect to a live database, or
implement the browser login flow.
