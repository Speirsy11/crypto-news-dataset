#!/usr/bin/env python3
"""Score private feed text, delete it, and publish metadata-only sentiment Parquet."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "sentiment"
DAILY_DIR = ROOT / "sentiment_daily"
MANIFEST_PATH = ROOT / "metadata" / "sentiment-manifest.json"
README_PATH = ROOT / "README.md"
README_STATS_START = "<!-- AUTO-STATS START -->"
README_STATS_END = "<!-- AUTO-STATS END -->"
DEFAULT_DATABASE_URL = "postgresql://signal:signal@localhost:5544/signal_harvester"
MODEL_NAME = os.environ.get("SENTIMENT_MODEL", "ProsusAI/finbert")
MODEL_REVISION = os.environ.get("SENTIMENT_MODEL_REVISION", "main")
HEADLINE_WEIGHT = 0.35
BODY_WEIGHT = 0.65
TRACKED_ASSETS = {
    "BTC": ["bitcoin", "btc"],
    "ETH": ["ethereum", "ether", "eth"],
    "SOL": ["solana", "sol"],
    "BNB": ["bnb", "binance coin"],
    "XRP": ["xrp", "ripple"],
    "TRX": ["tron", "trx"],
    "DOGE": ["dogecoin", "doge"],
    "ZEC": ["zcash", "zec"],
    "ADA": ["cardano", "ada"],
    "BCH": ["bitcoin cash", "bch"],
}

ARTICLE_SCHEMA = pa.schema([
    ("document_id", pa.string()),
    ("source_type", pa.string()),
    ("source_name", pa.string()),
    ("asset", pa.string()),
    ("relevance_score", pa.float32()),
    ("headline", pa.string()),
    ("url", pa.string()),
    ("author", pa.string()),
    ("published_at", pa.timestamp("us", tz="UTC")),
    ("collected_at", pa.timestamp("us", tz="UTC")),
    ("headline_positive", pa.float32()),
    ("headline_neutral", pa.float32()),
    ("headline_negative", pa.float32()),
    ("body_positive", pa.float32()),
    ("body_neutral", pa.float32()),
    ("body_negative", pa.float32()),
    ("combined_positive", pa.float32()),
    ("combined_neutral", pa.float32()),
    ("combined_negative", pa.float32()),
    ("combined_score", pa.float32()),
    ("combined_label", pa.string()),
    ("lexicon_score", pa.float32()),
    ("lexicon_label", pa.string()),
    ("model_name", pa.string()),
    ("model_version", pa.string()),
    ("processed_at", pa.timestamp("us", tz="UTC")),
    ("input_sha256", pa.string()),
])

DAILY_SCHEMA = pa.schema([
    ("date", pa.date32()),
    ("source_type", pa.string()),
    ("asset", pa.string()),
    ("document_count", pa.int32()),
    ("source_count", pa.int32()),
    ("relevance_weight", pa.float64()),
    ("weighted_sentiment", pa.float64()),
    ("positive_share", pa.float64()),
    ("neutral_share", pa.float64()),
    ("negative_share", pa.float64()),
    ("model_name", pa.string()),
    ("model_version", pa.string()),
])


def database_url() -> str:
    return os.environ.get("SIGNAL_HARVESTER_DATABASE_URL", DEFAULT_DATABASE_URL)


def source_type(topic: str, source_name: str) -> str:
    if topic == "MACRO":
        return "macro"
    if source_name == "Reddit CryptoCurrency":
        return "public_discussion"
    return "news"


def term_matches(text: str, term: str) -> bool:
    return re.search(rf"(^|[^a-z0-9]){re.escape(term)}([^a-z0-9]|$)", text, re.I) is not None


def detect_assets(topic: str, title: str, body: str | None):
    text = f"{title}\n{body or ''}"
    matches = []
    for asset, terms in TRACKED_ASSETS.items():
        title_terms = [term for term in terms if term_matches(title, term)]
        body_terms = [term for term in terms if term_matches(body or "", term)]
        if title_terms or body_terms:
            matches.append((
                asset,
                min(1.0, (0.65 if title_terms else 0) + (0.35 if body_terms else 0)),
                sorted(set(title_terms + body_terms)),
            ))
    if topic == "CRYPTO":
        matches.append(("GENERAL_CRYPTO", 1.0, []))
    elif topic == "MACRO":
        matches.append(("GENERAL_MACRO", 1.0, []))
    return matches


def refresh_asset_relevance(conn) -> int:
    documents = conn.execute(
        "SELECT id, topic, title, COALESCE(private_content, summary) AS body FROM documents"
    ).fetchall()
    written = 0
    for document in documents:
        for asset, score, terms in detect_assets(
            document["topic"], document["title"], document["body"]
        ):
            conn.execute(
                """
                INSERT INTO document_asset_relevance
                  (document_id, asset, relevance_score, matched_terms)
                VALUES (%s, %s, %s, %s::jsonb)
                ON CONFLICT (document_id, asset) DO UPDATE SET
                  relevance_score = EXCLUDED.relevance_score,
                  matched_terms = EXCLUDED.matched_terms
                """,
                (document["id"], asset, score, json.dumps(terms)),
            )
            written += 1
    conn.commit()
    return written


def load_model():
    try:
        import torch
        from transformers import AutoModelForSequenceClassification, AutoTokenizer
    except ImportError as exc:
        raise RuntimeError(
            "FinBERT dependencies missing; install requirements.txt in the dataset virtualenv"
        ) from exc
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, revision=MODEL_REVISION)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME, revision=MODEL_REVISION)
    model.eval()
    return torch, tokenizer, model


def score_text(text: str, runtime) -> dict[str, float]:
    torch, tokenizer, model = runtime
    encoded = tokenizer(
        text,
        add_special_tokens=False,
        return_attention_mask=False,
        return_token_type_ids=False,
    )["input_ids"]
    if not encoded:
        return {"positive": 0.0, "neutral": 1.0, "negative": 0.0}

    chunks = [encoded[start:start + 510] for start in range(0, len(encoded), 480)]
    totals = defaultdict(float)
    total_weight = 0
    with torch.inference_mode():
        for chunk in chunks:
            token_ids = [tokenizer.cls_token_id, *chunk, tokenizer.sep_token_id]
            inputs = {
                "input_ids": torch.tensor([token_ids]),
                "attention_mask": torch.ones((1, len(token_ids)), dtype=torch.long),
            }
            probabilities = torch.softmax(model(**inputs).logits[0], dim=-1).tolist()
            weight = len(chunk)
            for index, probability in enumerate(probabilities):
                label = str(model.config.id2label[index]).lower()
                totals[label] += probability * weight
            total_weight += weight
    return {
        label: totals[label] / total_weight
        for label in ("positive", "neutral", "negative")
    }


def combined_result(headline: dict[str, float], body: dict[str, float] | None):
    if body is None:
        probabilities = headline
    else:
        probabilities = {
            label: headline[label] * HEADLINE_WEIGHT + body[label] * BODY_WEIGHT
            for label in ("positive", "neutral", "negative")
        }
    score = probabilities["positive"] - probabilities["negative"]
    label = max(probabilities, key=probabilities.get)
    return probabilities, score, label


def process_pending(limit: int | None = None) -> int:
    runtime = load_model()
    model_version = getattr(runtime[2].config, "_commit_hash", None) or MODEL_REVISION
    processed = 0
    with psycopg.connect(database_url(), row_factory=dict_row) as conn:
        refresh_asset_relevance(conn)
        params: list[object] = [MODEL_NAME, model_version]
        limit_sql = ""
        if limit is not None:
            limit_sql = "LIMIT %s"
            params.append(limit)
        rows = conn.execute(
            f"""
            SELECT d.*
            FROM documents d
            WHERE NOT EXISTS (
              SELECT 1 FROM document_sentiments s
              WHERE s.document_id = d.id
                AND s.model_name = %s
                AND s.model_version = %s
            )
            ORDER BY (d.private_content IS NOT NULL) DESC,
                     COALESCE(d.published_at, d.collected_at) DESC,
                     d.id
            {limit_sql}
            """,
            params,
        ).fetchall()

        for row in rows:
            title = row["title"]
            body = row["private_content"] or row["summary"]
            headline_probs = score_text(title, runtime)
            body_probs = score_text(body, runtime) if body else None
            combined_probs, score, label = combined_result(headline_probs, body_probs)
            digest = hashlib.sha256(f"{title}\n{body or ''}".encode()).hexdigest()
            conn.execute(
                """
                INSERT INTO document_sentiments (
                  document_id, model_name, model_version,
                  headline_positive, headline_neutral, headline_negative,
                  body_positive, body_neutral, body_negative,
                  combined_positive, combined_neutral, combined_negative,
                  combined_score, combined_label, input_sha256
                ) VALUES (
                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (document_id, model_name, model_version) DO NOTHING
                """,
                (
                    row["id"], MODEL_NAME, model_version,
                    headline_probs["positive"], headline_probs["neutral"], headline_probs["negative"],
                    body_probs["positive"] if body_probs else None,
                    body_probs["neutral"] if body_probs else None,
                    body_probs["negative"] if body_probs else None,
                    combined_probs["positive"], combined_probs["neutral"], combined_probs["negative"],
                    score, label, digest,
                ),
            )
            conn.execute(
                "UPDATE documents SET private_content = NULL, content_delete_after = NULL WHERE id = %s",
                (row["id"],),
            )
            conn.commit()
            processed += 1

        purged = conn.execute(
            """
            UPDATE documents
            SET private_content = NULL, content_delete_after = NULL
            WHERE private_content IS NOT NULL AND content_delete_after <= NOW()
            """,
        ).rowcount
        conn.commit()
        if purged:
            print(f"Purged {purged} expired private content rows")
    print(f"Processed {processed} documents with {MODEL_NAME}")
    return processed


def article_path(row) -> Path:
    timestamp = row["published_at"] or row["collected_at"]
    kind = source_type(row["topic"], row["source_name"])
    return (
        DATA_DIR / f"source_type={kind}" / f"asset={row['asset']}"
        / f"year={timestamp.year:04d}" / f"month={timestamp.month:02d}"
        / f"day={timestamp.day:02d}" / "sentiment.parquet"
    )


def rows_to_table(rows: list[dict], schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=schema)


def export_dataset() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    with psycopg.connect(database_url(), row_factory=dict_row) as conn:
        refresh_asset_relevance(conn)
        rows = conn.execute(
            """
            SELECT
              d.id::text AS document_id, d.topic, d.source_name, r.asset,
              r.relevance_score, d.title AS headline, d.url, d.author,
              COALESCE(d.published_at, d.collected_at) AS published_at,
              d.collected_at,
              s.headline_positive, s.headline_neutral, s.headline_negative,
              s.body_positive, s.body_neutral, s.body_negative,
              s.combined_positive, s.combined_neutral, s.combined_negative,
              s.combined_score, s.combined_label,
              d.sentiment_score AS lexicon_score, d.sentiment_label AS lexicon_label,
              s.model_name, s.model_version, s.processed_at, s.input_sha256
            FROM documents d
            JOIN document_asset_relevance r ON r.document_id = d.id
            JOIN LATERAL (
              SELECT *
              FROM document_sentiments candidate
              WHERE candidate.document_id = d.id
              ORDER BY candidate.processed_at DESC
              LIMIT 1
            ) s ON true
            ORDER BY COALESCE(d.published_at, d.collected_at), d.id, r.asset
            """
        ).fetchall()

    partitions: dict[Path, list[dict]] = defaultdict(list)
    for row in rows:
        output = dict(row)
        output["source_type"] = source_type(row["topic"], row["source_name"])
        output.pop("topic")
        partitions[article_path(row)].append(output)

    for path, partition_rows in partitions.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(rows_to_table(partition_rows, ARTICLE_SCHEMA), path, compression="zstd")

    daily_groups: dict[tuple[date, str, str, str, str], list[dict]] = defaultdict(list)
    for row in rows:
        timestamp = row["published_at"] or row["collected_at"]
        kind = source_type(row["topic"], row["source_name"])
        key = (timestamp.date(), kind, row["asset"], row["model_name"], row["model_version"])
        daily_groups[key].append(row)

    daily_by_path: dict[Path, list[dict]] = defaultdict(list)
    for (day, kind, asset, model_name, model_version), group in daily_groups.items():
        weight = sum(float(row["relevance_score"]) for row in group)
        weighted = lambda field: sum(float(row[field]) * float(row["relevance_score"]) for row in group) / weight
        daily = {
            "date": day,
            "source_type": kind,
            "asset": asset,
            "document_count": len(group),
            "source_count": len({row["source_name"] for row in group}),
            "relevance_weight": weight,
            "weighted_sentiment": weighted("combined_score"),
            "positive_share": weighted("combined_positive"),
            "neutral_share": weighted("combined_neutral"),
            "negative_share": weighted("combined_negative"),
            "model_name": model_name,
            "model_version": model_version,
        }
        path = (
            DAILY_DIR / f"source_type={kind}" / f"asset={asset}"
            / f"year={day.year:04d}" / f"month={day.month:02d}" / "daily.parquet"
        )
        daily_by_path[path].append(daily)

    for path, partition_rows in daily_by_path.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        partition_rows.sort(key=lambda row: row["date"])
        pq.write_table(rows_to_table(partition_rows, DAILY_SCHEMA), path, compression="zstd")

    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(json.dumps({
        "name": "crypto-and-macro-sentiment",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "model": MODEL_NAME,
        "requested_model_revision": MODEL_REVISION,
        "headline_weight": HEADLINE_WEIGHT,
        "body_weight": BODY_WEIGHT,
        "documents": len(rows),
        "privacy": "Feed content is processed privately and is never exported.",
        "article_layout": "sentiment/source_type=<type>/asset=<asset>/year=<YYYY>/month=<MM>/day=<DD>/sentiment.parquet",
        "daily_layout": "sentiment_daily/source_type=<type>/asset=<asset>/year=<YYYY>/month=<MM>/daily.parquet",
    }, indent=2) + "\n")
    print(f"Exported {len(rows)} document-asset sentiment rows")
    return len(rows)




def _count_article_stats() -> dict:
    """Scan article-level Parquet file footers for counts."""
    if not DATA_DIR.exists():
        return {"article_files": 0, "article_rows": 0, "per_asset": {}, "per_source": {}}
    article_files = 0
    article_rows = 0
    per_asset: dict[str, int] = {}
    per_source: dict[str, int] = {}
    for source_dir in sorted(DATA_DIR.iterdir()):
        if not source_dir.is_dir() or not source_dir.name.startswith("source_type="):
            continue
        source_type = source_dir.name.removeprefix("source_type=")
        for asset_dir in sorted(source_dir.iterdir()):
            if not asset_dir.is_dir() or not asset_dir.name.startswith("asset="):
                continue
            asset = asset_dir.name.removeprefix("asset=")
            for pq_file in asset_dir.rglob("*.parquet"):
                article_files += 1
                num_rows = pq.read_metadata(pq_file).num_rows
                article_rows += num_rows
                per_asset[asset] = per_asset.get(asset, 0) + num_rows
                per_source[source_type] = per_source.get(source_type, 0) + num_rows
    return {"article_files": article_files, "article_rows": article_rows, "per_asset": per_asset, "per_source": per_source}


def _scan_daily_aggregates() -> dict:
    """Scan daily Parquet files for date range and latest-day stats."""
    if not DAILY_DIR.exists():
        return {"daily_files": 0, "daily_rows": 0, "earliest_date": None, "latest_date": None,
                "latest_day_docs": 0, "latest_day_by_source": {}, "latest_day_by_asset": {},
                "per_asset_daily": {}}
    daily_files = 0
    daily_rows = 0
    all_day_rows: list[tuple] = []
    per_asset_daily: dict[str, int] = {}
    for source_dir in sorted(DAILY_DIR.iterdir()):
        if not source_dir.is_dir() or not source_dir.name.startswith("source_type="):
            continue
        source_type = source_dir.name.removeprefix("source_type=")
        for asset_dir in sorted(source_dir.iterdir()):
            if not asset_dir.is_dir() or not asset_dir.name.startswith("asset="):
                continue
            asset = asset_dir.name.removeprefix("asset=")
            for pq_file in asset_dir.rglob("*.parquet"):
                daily_files += 1
                daily_rows += pq.read_metadata(pq_file).num_rows
                table = pq.ParquetFile(pq_file).read(columns=["date", "document_count"])
                dates = table.column("date").to_pylist()
                counts = table.column("document_count").to_pylist()
                for d, c in zip(dates, counts):
                    all_day_rows.append((d, source_type, asset, c))
                    per_asset_daily[asset] = per_asset_daily.get(asset, 0) + c
    if not all_day_rows:
        return {"daily_files": daily_files, "daily_rows": daily_rows, "earliest_date": None,
                "latest_date": None, "latest_day_docs": 0,
                "latest_day_by_source": {}, "latest_day_by_asset": {},
                "per_asset_daily": per_asset_daily}
    earliest_date = min(r[0] for r in all_day_rows)
    latest_date = max(r[0] for r in all_day_rows)
    latest_rows = [r for r in all_day_rows if r[0] == latest_date]
    by_source: dict[str, int] = {}
    by_asset: dict[str, int] = {}
    for _, src, ast, c in latest_rows:
        by_source[src] = by_source.get(src, 0) + c
        by_asset[ast] = by_asset.get(ast, 0) + c
    return {
        "daily_files": daily_files, "daily_rows": daily_rows,
        "earliest_date": earliest_date, "latest_date": latest_date,
        "latest_day_docs": sum(by_source.values()),
        "latest_day_by_source": by_source, "latest_day_by_asset": by_asset,
        "per_asset_daily": per_asset_daily,
    }


def update_readme_stats() -> None:
    """Regenerate the auto-stats section of README.md from Parquet file footers."""
    article = _count_article_stats()
    daily = _scan_daily_aggregates()
    manifest = json.loads(MANIFEST_PATH.read_text()) if MANIFEST_PATH.exists() else {}
    generated_at = manifest.get("generated_at", "N/A")
    earliest = daily["earliest_date"].isoformat() if daily["earliest_date"] else "N/A"
    latest = daily["latest_date"].isoformat() if daily["latest_date"] else "N/A"
    all_assets = sorted(set(article["per_asset"]) | set(daily["per_asset_daily"]))
    asset_rows = "\n".join(
        f"| {asset} | {article['per_asset'].get(asset, 0):,} | {daily['per_asset_daily'].get(asset, 0):,} |"
        for asset in all_assets
    )
    all_sources = sorted(article["per_source"])
    source_rows = "\n".join(
        f"| {src} | {article['per_source'].get(src, 0):,} |"
        for src in all_sources
    ) or "| (none) | 0 |"
    latest_sources = ", ".join(
        f"{src}: {count}" for src, count in sorted(daily["latest_day_by_source"].items())
    ) or "N/A"
    latest_assets = ", ".join(
        f"{ast}: {count}" for ast, count in sorted(daily["latest_day_by_asset"].items())
    ) or "N/A"
    stats_block = f"""{README_STATS_START}
## Dataset Stats

_Auto-generated on each publish — do not edit manually._

**Last generated:** {generated_at}
**Coverage:** {earliest} → {latest}

| Metric | Value |
|--------|-------|
| Article-level rows | {article["article_rows"]:,} |
| Daily aggregates | {daily["daily_rows"]:,} |
| Parquet files | {article["article_files"] + daily["daily_files"]:,} |

**Per-asset counts:**

| Asset | Articles | Daily rows |
|-------|----------|------------|
{asset_rows}

**Per source type:**

| Source Type | Articles |
|-------------|----------|
{source_rows}

**Latest day ({latest}):**

| Metric | Value |
|--------|-------|
| Documents | {daily["latest_day_docs"]} |
| By source | {latest_sources} |
| By asset | {latest_assets} |
{README_STATS_END}
"""
    text = README_PATH.read_text() if README_PATH.exists() else ""
    if README_STATS_START in text and README_STATS_END in text:
        before = text[:text.index(README_STATS_START)]
        after = text[text.index(README_STATS_END) + len(README_STATS_END):]
        README_PATH.write_text(before + stats_block + after)
    else:
        README_PATH.write_text(text.rstrip() + "\n\n" + stats_block)

def commit_and_push() -> None:
    paths = [
        path for path in
        ["README.md", "requirements.txt", "jobs", "scripts", "metadata", "sentiment", "sentiment_daily"]
        if (ROOT / path).exists()
    ]
    subprocess.run(
        ["git", "add", *paths],
        cwd=ROOT, check=True,
    )
    status = subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True)
    if not status.strip():
        print("No sentiment dataset changes to commit")
        return
    day = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    subprocess.run(["git", "commit", "-m", f"Update sentiment dataset through {day}"], cwd=ROOT, check=True)
    subprocess.run(["git", "push"], cwd=ROOT, check=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--process", action="store_true")
    parser.add_argument("--export", action="store_true")
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    if not (args.process or args.export or args.publish):
        parser.error("choose --process, --export, or --publish")
    if args.process or args.publish:
        process_pending(args.limit)
    if args.export or args.publish:
        export_dataset()
    if args.publish:
        update_readme_stats()
        commit_and_push()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
