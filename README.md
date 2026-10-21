# storage
Storage

## Candle Parquet datasets

`go/parquet/dataset/finance/market.CandleDataset` stores Candles using
`asset_class / venue / instrument_type / symbol / timeframe / date` partitions.
The flat OHLCV columns remain compatible with historical files. Optional
execution columns retain quote volume, buy/sell/unknown base volume, count,
quality, venue symbol, chain/network/pool identity and run/version metadata.
`Volume` is base volume. Missing legacy totals and quality remain nil; they do
not mean zero volume or complete coverage.

Compose a dataset with `NewCandleDataset(client, CandleDatasetParams{...})`.
For a bounded mutable source/hour snapshot, use a deterministic filename starting
with `CandleSnapshotFilePrefix`, `WriteMode: dataset.WriteModeOverwrite` and
`MaxRowsPerFile: 60`. `Write` atomically replaces the full supplied snapshot;
the caller owns revision merging, retry identity and single-writer exclusion.
The snapshot prefix is reserved and always excluded by Candle compaction.
Ordinary immutable parts keep the existing write/compaction workflow.

Run Parquet module checks from `go/parquet`:

```sh
GOWORK=off go test ./...
GOWORK=off go test -race ./...
GOWORK=off go vet ./...
```

## Funding Rate Parquet datasets

`go/parquet/dataset/finance/market.FundingRate` keeps `FundingTimestamp` as
`time.Time`. For `current_estimate` only, zero means the venue did not supply a
settlement target; the codec writes Parquet `funding_timestamp = NULL`. Both
`settled` and `predicted` observations still require a funding time. Event and
receipt times remain required, with microsecond precision. For snapshots with
no venue publication time, EventTimestamp is the receipt time, not a claim about
when the venue updated the rate.

The updated reader accepts historical required timestamps and the new nullable
column. Funding compaction accepts this specific nullability difference while
preserving timestamp values, nulls, compression checks, and other schema checks.
Existing files do not need rewriting. Upgrade all Funding readers/compactors
before writing null targets: older clients do not understand their semantics.
Go field types and dataset paths are unchanged.
