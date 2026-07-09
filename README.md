# Crypto News Dataset

Public, metadata-only crypto, public-discussion, and macroeconomic sentiment
datasets enriched with FinBERT and partitioned as Parquet.

Signal Harvester is only the generic collection engine. This repository owns:

- RSS source definitions and job schedules
- Programmatic job provisioning
- Tracking for BTC, ETH, SOL, BNB, XRP, TRX, DOGE, ZEC, ADA and BCH
- `GENERAL_CRYPTO` and `GENERAL_MACRO` signals
- FinBERT headline/body scoring
- Temporary-content deletion
- Article-level and daily Parquet exports
- GitHub publishing

## Provision collectors

```bash
python scripts/provision_jobs.py apply
python scripts/provision_jobs.py run
```

Jobs are sent to Signal Harvester through `POST /api/commands`; no source
configuration is compiled into Signal Harvester.

## Build and publish

```bash
python scripts/build_dataset.py --process --export
python scripts/build_dataset.py --publish
```

Feed-provided body text remains private in Postgres only until processing
succeeds, or for seven days after a failure. It is never exported.

## Layout

```text
sentiment/source_type=<type>/asset=<asset>/year=<YYYY>/month=<MM>/day=<DD>/sentiment.parquet
sentiment_daily/source_type=<type>/asset=<asset>/year=<YYYY>/month=<MM>/daily.parquet
```

<!-- AUTO-STATS START -->
## Dataset Stats

_Auto-generated on each publish — do not edit manually._

**Last generated:** 2026-07-07T23:27:49.760804Z
**Coverage:** 2017-12-13 → 2026-07-07

| Metric | Value |
|--------|-------|
| Article-level rows | 29,743 |
| Daily aggregates | 790 |
| Parquet files | 937 |

**Per-asset counts:**

| Asset | Articles | Daily rows |
|-------|----------|------------|
| ADA | 110 | 110 |
| BCH | 39 | 39 |
| BNB | 87 | 87 |
| BTC | 9,537 | 9,537 |
| DOGE | 128 | 128 |
| ETH | 1,248 | 1,248 |
| GENERAL_CRYPTO | 17,170 | 17,170 |
| GENERAL_MACRO | 77 | 77 |
| SOL | 492 | 492 |
| TRX | 49 | 49 |
| XRP | 721 | 721 |
| ZEC | 85 | 85 |

**Per source type:**

| Source Type | Articles |
|-------------|----------|
| macro | 78 |
| news | 29,665 |

**Latest day (2026-07-07):**

| Metric | Value |
|--------|-------|
| Documents | 1963 |
| By source | news: 1963 |
| By asset | ADA: 5, BCH: 1, BNB: 9, BTC: 188, DOGE: 7, ETH: 62, GENERAL_CRYPTO: 1612, SOL: 30, TRX: 4, XRP: 39, ZEC: 6 |
<!-- AUTO-STATS END -->

