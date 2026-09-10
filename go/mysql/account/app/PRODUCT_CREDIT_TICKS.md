# Product credit ticks

## Agreed specification (2026-09-10)

`Product.CreditTicks` / `credit_ticks` represents the **total ticks granted by a
product**, including any purchase incentives. A separate `BonusTicks` /
`bonus_ticks` field is removed from the storage product API and new table schema.
No deprecated product field, bonus range filter or bonus sort column is retained.

For existing products, preserve the previous total:

| Product | Previous credit + bonus | New credit_ticks |
| --- | ---: | ---: |
| Signup bonus | 0 + 10,000,000 | 10,000,000 |
| 10 USDC product | 10,000,000 + 2,500,000 | 12,500,000 |
| 100 USDC product | 100,000,000 + 50,000,000 | 150,000,000 |

Price, product type, expiry and purchase limit do not change. Product descriptions
may still explain a purchase incentive. This does not require a distinct bonus
credit lot. Existing intent snapshots and already granted balances are not
recomputed. Account registration's response field `bonusTicks` describes the
signup grant, not a product field; that response name is retained in the later
CRM/SDK work.

## Implemented scope: workflow steps 1–2

- Remove the bonus field from Product, insert/update parameters, select filters,
  sort allowlist, column constants, INSERT/SELECT/UPDATE SQL and CreateTable DDL.
- Use explicit SELECT columns matching the scanner's field order.
- Keep CreditTicks as uint64; this module does not impose a new minimum or change
  zero-value behavior.

This is a breaking storage API change. SDK, CRM, Gateway, Console, service
dependency versions and existing databases are not changed in this step.

## Follow-up migration and integration requirements

CreateTable only creates a missing table; it does not migrate existing data.
Before switching services to this version, prepare a versioned CRM migration
that checks overflow, preserves `credit_ticks + bonus_ticks`, prevents the sum
from being applied twice, and removes the old column. The split cannot be
reconstructed from the sum; preserve a backup if rollback to the split is needed.
Do not apply a bare column drop before folding existing values into credit_ticks.

Update CRM signup grant handling from product.BonusTicks to product.CreditTicks,
update product RPC/SDK fields, and update initial seed totals and Console display
in later workflow steps. Coordinate those changes before updating the service's
pinned storage dependency. Tests of this module alone do not verify the CRM
migration or end-to-end credit issuance.
