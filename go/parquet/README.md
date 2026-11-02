# Parquet market datasets

`dataset/finance/market` owns the public record types, codecs and dataset constructors.

## Explicit Funding/OI observation format

`FundingRateObservation` and `OpenInterestObservation` wrap the existing economic
record with `ObservationContext`: optional `network`, `source_id`, and
`collection_run_id` columns. New observation writes require all three fields.
The observation codecs can also read the recognized legacy schemas, returning
nil provenance fields; missing context never implies Mainnet.

Use `NewFundingRateObservationDataset` / `NewOpenInterestObservationDataset`
with a separate v2 root. Keep legacy and v2 inputs out of the same compaction.
The existing `FundingRate`, `OpenInterest`, legacy codecs and dataset constructors
retain their format and behavior. Context is part of v2 exact-record equality,
so otherwise identical observations from distinct networks or collection runs
are preserved. V2 policies can finalize one input part; legacy policies still
require at least two inputs.

The initial application use is explicitly scoped Hyperliquid/Bluefin SUI
perpetual Funding/OI. The storage types themselves are not asset-specific.
Source registry definitions, sanitized build/config metadata and file commit
records are application responsibilities, not values inferred by this module.

## Optional compaction publication boundary

`dataset.Params.BeforeCompactionInputDeletion` defaults to nil. When composed,
the callback sees validated, published output files while every input is still
present. A callback error rolls back outputs and preserves inputs. This permits
an application to publish immutable file provenance before compaction removes
its inputs. The callback must be idempotent and avoid treating a successful
data-file write alone as a complete provenance publication.
