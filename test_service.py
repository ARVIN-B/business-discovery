#!/usr/bin/env python3
"""Live end-to-end smoke test against this host's real SearXNG and Firecrawl."""
import json
import os
import re
import secrets
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import urljoin, urlsplit

BASE_URL = os.getenv("TEST_BASE_URL", "http://127.0.0.1:8091").rstrip("/")
TEST_COUNT = int(os.getenv("TEST_COUNT", "1"))
JOB_TIMEOUT = int(os.getenv("TEST_JOB_TIMEOUT_SECONDS", "3600"))
ENV_FILE = Path("/opt/business-discovery/.env")


def read_key():
    if os.getenv("API_KEY"):
        return os.environ["API_KEY"]
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        if line.startswith("API_KEY="):
            return line.partition("=")[2].strip().strip('"').strip("'")
    raise RuntimeError("API_KEY was not found in the environment or service environment file")


def request(url, *, payload=None, headers=None, timeout=30):
    data = json.dumps(payload, ensure_ascii=False).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, headers=headers or {}, method="POST" if data is not None else "GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as res:
            raw = res.read().decode()
            return res.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")[:1000]
        raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc
    except TimeoutError as exc:
        raise RuntimeError(f"Request timed out after {timeout}s: {url}") from exc


def read_sse(url, headers, timeout):
    req = urllib.request.Request(url, headers={**headers, "Accept": "text/event-stream"})
    events = []
    current = {}
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            if response.status != 200 or "text/event-stream" not in response.headers.get("Content-Type", ""):
                raise RuntimeError("SSE endpoint did not return HTTP 200 text/event-stream")
            for raw_line in response:
                line = raw_line.decode("utf-8").rstrip("\r\n")
                if not line:
                    if current:
                        current["data"] = json.loads(current.pop("raw_data")) if "raw_data" in current else None
                        events.append(current)
                        current = {}
                    continue
                if line.startswith(":"):
                    continue
                field, _, value = line.partition(":")
                value = value.lstrip()
                if field == "id":
                    current["id"] = int(value)
                elif field == "event":
                    current["event"] = value
                elif field == "data":
                    current["raw_data"] = value
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")[:1000]
        raise RuntimeError(f"SSE HTTP {exc.code}: {detail}") from exc
    return events


def main():
    if TEST_COUNT < 1 or TEST_COUNT > 100:
        raise RuntimeError("TEST_COUNT must be between 1 and 100")
    key = read_key()
    auth = {"Authorization": f"Bearer {key}"}

    status, health = request(BASE_URL + "/health", timeout=10)
    if status != 200 or not isinstance(health, dict) or health.get("status") != "ok":
        raise RuntimeError(f"Health check failed: HTTP {status}, body={health}")
    print("Health: OK")

    status, created = request(
        BASE_URL + "/v1/discover",
        payload={"keyword": "رستوران", "province": "اصفهان", "count": TEST_COUNT},
        headers={**auth, "Content-Type": "application/json", "Idempotency-Key": "live-smoke-" + secrets.token_hex(12)},
        timeout=15,
    )
    if status != 202 or not isinstance(created, dict):
        raise RuntimeError(f"Job creation failed: HTTP {status}, body={created}")
    job_id = created.get("job_id")
    if not job_id or created.get("status_url") is None or created.get("events_url") is None:
        raise RuntimeError("202 response omitted the job ID or endpoint links")
    print(f"Job accepted: {job_id} (initial status: {created.get('status')})")

    status_url = urljoin(BASE_URL + "/", created["status_url"])
    events_url = urljoin(BASE_URL + "/", created["events_url"])
    result_url = urljoin(BASE_URL + "/", created["result_url"])
    deadline = time.monotonic() + JOB_TIMEOUT
    while True:
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Job {job_id} did not finish within {JOB_TIMEOUT}s")
        http_status, job = request(status_url, headers=auth, timeout=30)
        if http_status != 200 or not isinstance(job, dict):
            raise RuntimeError(f"Status endpoint returned an invalid response: HTTP {http_status}")
        if job.get("status") in {"completed", "failed"}:
            break
        time.sleep(1)

    discovered = len(job.get("website_statuses", {}))
    processed = sum(1 for item in job.get("website_statuses", {}).values() if item.get("status") in {"completed", "failed", "skipped"})
    print(f"Terminal status: {job['status']} — round {job.get('current_round')}/{job.get('total_rounds')}")
    print(f"Websites discovered: {discovered}; processed: {processed}; successful: {job.get('successful_websites')}")
    if job["status"] == "failed":
        raise RuntimeError(f"Discovery job failed: {job.get('errors')}")

    http_status, result = request(result_url, headers=auth, timeout=30)
    required = {"job_id", "status", "requested_count", "successful_websites", "total_unique_mobile_numbers", "completed_rounds", "results"}
    if http_status != 200 or not isinstance(result, dict) or not required.issubset(result):
        raise RuntimeError("Final-result endpoint returned an invalid JSON structure")
    if result["job_id"] != job_id or result["status"] != "completed":
        raise RuntimeError("Final result does not match the completed job")
    if result["successful_websites"] != len(result["results"]):
        raise RuntimeError("successful_websites does not match results length")

    seen_numbers = set()
    print(f"Unique Iranian mobile numbers: {result['total_unique_mobile_numbers']}")
    print(f"Completed search rounds: {result['completed_rounds']}")
    for website in result["results"]:
        if not isinstance(website, dict) or not {"website_url", "mobile_numbers"}.issubset(website):
            raise RuntimeError("Website result is missing its URL or mobile list")
        print(f"\nWebsite: {website['website_url']}")
        for phone in website["mobile_numbers"]:
            if not {"number", "source_url"}.issubset(phone):
                raise RuntimeError("A mobile result is missing its source page URL")
            number = phone["number"]
            source = urlsplit(phone["source_url"])
            if not re.fullmatch(r"09\d{9}", number):
                raise RuntimeError(f"Invalid Iranian mobile number: {number}")
            if number in seen_numbers:
                raise RuntimeError(f"Duplicate mobile number in final result: {number}")
            if source.scheme not in {"http", "https"} or not source.hostname:
                raise RuntimeError(f"Invalid source URL for {number}: {phone['source_url']}")
            seen_numbers.add(number)
            print(f"  {number} — source: {phone['source_url']}")
    if len(seen_numbers) != result["total_unique_mobile_numbers"]:
        raise RuntimeError("Number of returned unique mobiles does not match the total")

    # Replay the durable event log after completion and verify IDs/order.
    events = read_sse(events_url, auth, timeout=60)
    event_ids = [event.get("id") for event in events]
    if not events or any(event_id is None for event_id in event_ids):
        raise RuntimeError("SSE replay returned no events or an event without an ID")
    if event_ids != sorted(set(event_ids)):
        raise RuntimeError("SSE event IDs are not strictly increasing")
    event_names = [event.get("event") for event in events]
    if event_names[0] != "job_started" or event_names[-1] != "job_completed":
        raise RuntimeError("SSE replay is missing the expected job lifecycle events")
    if any(event.get("event") == "website_completed" and event.get("data", {}).get("result") for event in events):
        print("SSE: per-website results replayed")
    print(f"SSE: replayed {len(events)} ordered events (IDs {event_ids[0]}–{event_ids[-1]})")
    for warning in result.get("warnings", []):
        print(f"Warning: {warning}")
    print("Live integration test: PASS")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Live integration test: FAIL: {exc}", file=sys.stderr)
        sys.exit(1)
