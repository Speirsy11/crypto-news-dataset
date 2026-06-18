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
