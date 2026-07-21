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

**Last generated:** 2026-07-21T23:04:00.445913Z
**Coverage:** 2017-12-13 → 2026-07-09

| Metric | Value |
|--------|-------|
| Article-level rows | 31,812 |
| Daily aggregates | 810 |
| Parquet files | 957 |

**Per-asset counts:**

| Asset | Articles | Daily rows |
|-------|----------|------------|
| ADA | 124 | 124 |
| BCH | 39 | 39 |
| BNB | 100 | 100 |
| BTC | 9,721 | 9,721 |
| DOGE | 133 | 133 |
| ETH | 1,316 | 1,316 |
| GENERAL_CRYPTO | 18,838 | 18,838 |
| GENERAL_MACRO | 77 | 77 |
| SOL | 542 | 542 |
| TRX | 51 | 51 |
| XRP | 778 | 778 |
| ZEC | 93 | 93 |

**Per source type:**

| Source Type | Articles |
|-------------|----------|
| macro | 78 |
| news | 31,734 |

**Latest day (2026-07-09):**

| Metric | Value |
|--------|-------|
| Documents | 254 |
| By source | news: 254 |
| By asset | ADA: 2, BNB: 4, BTC: 25, DOGE: 1, ETH: 14, GENERAL_CRYPTO: 187, SOL: 10, TRX: 1, XRP: 8, ZEC: 2 |
<!-- AUTO-STATS END -->
























