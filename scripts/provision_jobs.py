#!/usr/bin/env python3
"""Provision and control Signal Harvester jobs through its command API."""
from __future__ import annotations

import argparse
import json
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_URL = "http://localhost:3010"


def command(base_url: str, payload: dict):
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/api/commands",
        data=json.dumps(payload).encode(),
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        body = response.read().decode()
        return json.loads(body) if body else {"ok": True}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["apply", "run", "enable", "disable", "delete"])
    parser.add_argument("--base-url", default=DEFAULT_URL)
    parser.add_argument("--job-id")
    args = parser.parse_args()

    jobs = json.loads((ROOT / "jobs" / "jobs.json").read_text())["jobs"]
    selected = [job for job in jobs if not args.job_id or job["id"] == args.job_id]
    if args.job_id and not selected:
        raise SystemExit(f"Unknown configured job: {args.job_id}")

    for job in selected:
        if args.action == "apply":
            payload = {"action": "upsert_job", "job": job}
        elif args.action == "run":
            payload = {"action": "run_job", "jobId": job["id"]}
        elif args.action in {"enable", "disable"}:
            payload = {
                "action": "set_job_enabled",
                "jobId": job["id"],
                "enabled": args.action == "enable",
            }
        else:
            payload = {"action": "delete_job", "jobId": job["id"]}
        print(job["id"], json.dumps(command(args.base_url, payload), default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
