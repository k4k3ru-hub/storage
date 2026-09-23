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
