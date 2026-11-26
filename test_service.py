#!/usr/bin/env python3
"""Live smoke test for the v2 flat-result API contract."""
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
TEST_KEYWORD = os.getenv("TEST_KEYWORD", "رستوران")
TEST_PROVINCE = os.getenv("TEST_PROVINCE", "اصفهان")
_COUNT_VALUE = os.getenv("TEST_COUNT", "1").strip().lower()
TEST_COUNT = None if _COUNT_VALUE in {"", "null", "none", "unlimited"} else int(_COUNT_VALUE)
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


def validate_result(record):
    required = {"number", "source_url", "website_url", "page_title", "page_description"}
    if not isinstance(record, dict) or set(record) != required:
        raise RuntimeError("Each result must have the flat schema_version=2 record fields")
    if not re.fullmatch(r"09\d{9}", record["number"]):
        raise RuntimeError(f"Invalid or non-mobile number: {record['number']}")
    source = urlsplit(record["source_url"])
    website = urlsplit(record["website_url"])
    if source.scheme not in {"http", "https"} or not source.hostname:
        raise RuntimeError(f"Invalid source_url: {record['source_url']}")
    if website.scheme not in {"http", "https"} or not website.hostname:
        raise RuntimeError(f"Invalid website_url: {record['website_url']}")
    if source.scheme != website.scheme or source.hostname != website.hostname or source.port != website.port:
        raise RuntimeError("website_url must be the root URL corresponding to source_url")
    if website.path not in {"", "/"} or website.query or website.fragment:
        raise RuntimeError("website_url must contain only the root website origin")


def main():
    if TEST_COUNT is not None and not 1 <= TEST_COUNT <= 100:
        raise RuntimeError("TEST_COUNT must be between 1 and 100, or null/unlimited")
    if JOB_TIMEOUT < 1:
        raise RuntimeError("TEST_JOB_TIMEOUT_SECONDS must be positive")
    key = read_key()
    auth = {"Authorization": f"Bearer {key}"}

    status, health = request(BASE_URL + "/health", timeout=10)
    if status != 200 or not isinstance(health, dict) or health.get("status") != "ok":
        raise RuntimeError(f"Health check failed: HTTP {status}, body={health}")
    print("Health: OK")

    status, created = request(
        BASE_URL + "/v1/discover",
        payload={"keyword": TEST_KEYWORD, "province": TEST_PROVINCE, "count": TEST_COUNT},
        headers={**auth, "Content-Type": "application/json", "Idempotency-Key": "live-v2-" + secrets.token_hex(12)},
        timeout=15,
    )
    if status != 202 or not isinstance(created, dict) or created.get("schema_version") != 2:
        raise RuntimeError(f"Job creation failed or schema version is not 2: HTTP {status}, body={created}")
    job_id = created.get("job_id")
    if not job_id or not all(created.get(key) for key in ("status_url", "events_url", "result_url")):
        raise RuntimeError("202 response omitted job links")
    print(f"Job accepted: {job_id} (status={created.get('status')})")

    status_url = urljoin(BASE_URL + "/", created["status_url"])
    events_url = urljoin(BASE_URL + "/", created["events_url"])
    result_url = urljoin(BASE_URL + "/", created["result_url"])
    deadline = time.monotonic() + JOB_TIMEOUT
    while True:
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Job did not finish within {JOB_TIMEOUT}s")
        status, job = request(status_url, headers=auth, timeout=30)
        if status != 200 or job.get("schema_version") != 2:
            raise RuntimeError("Status endpoint returned an invalid response")
        if job.get("status") in {"completed", "failed", "stopped"}:
            break
        time.sleep(1)
    print(
        "Pages: discovered={discovered}, processed={processed}, successful={successful}; results={results}/{target}".format(
            discovered=job["discovered_page_count"], processed=job["processed_page_count"],
            successful=job["successful_page_count"], results=job["successful_results"], target=job["requested_count"],
        )
    )
    if job["status"] != "completed":
        raise RuntimeError(f"Discovery job failed: {job.get('errors')}")

    status, final = request(result_url, headers=auth, timeout=30)
    required = {
        "schema_version", "job_id", "status", "mode", "requested_count", "successful_results",
        "total_unique_mobile_numbers", "discovered_page_count", "processed_page_count",
        "successful_page_count", "completed_rounds", "results", "errors", "warnings",
    }
    if status != 200 or not isinstance(final, dict) or not required.issubset(final):
        raise RuntimeError("Final-result endpoint returned an invalid JSON structure")
    if final["schema_version"] != 2 or final["job_id"] != job_id or final["status"] != "completed":
        raise RuntimeError("Final result does not match the completed job")
    count = len(final["results"])
    if not (final["successful_results"] == final["total_unique_mobile_numbers"] == count):
        raise RuntimeError("Unique result counters do not match the flat result array")

    seen = set()
    print(f"Unique mobile records: {count}")
    for index, record in enumerate(final["results"], start=1):
        validate_result(record)
        if record["number"] in seen:
            raise RuntimeError(f"Duplicate mobile number: {record['number']}")
        seen.add(record["number"])
        print(f"\n[{index}] Mobile: {record['number']}")
        print(f"    Page title: {record['page_title']}")
        print(f"    Source URL: {record['source_url']}")
        print(f"    Website: {record['website_url']}")

    events = read_sse(events_url, auth, timeout=60)
    ids = [event.get("id") for event in events]
    if not events or any(value is None for value in ids) or ids != sorted(set(ids)):
        raise RuntimeError("SSE event IDs are missing, duplicated, or out of order")
    event_names = [event.get("event") for event in events]
    if event_names[0] != "job_started" or event_names[-1] != "job_completed":
        raise RuntimeError("SSE lifecycle events are incomplete")
    streamed = [record for event in events if event.get("event") == "url_results_found" for record in event["data"].get("results", [])]
    if streamed != final["results"]:
        raise RuntimeError("url_results_found events do not reproduce the final flat results")
    page_events = [event for event in events if event.get("event") == "page_completed"]
    if any("page_url" not in event["data"] or "website_url" not in event["data"] for event in page_events):
        raise RuntimeError("A page_completed event is missing URL-level identity")
    print(f"SSE: {len([event for event in events if event.get('event') == 'url_results_found'])} URL result events, {len(page_events)} page_completed events, ordered IDs {ids[0]}–{ids[-1]}")
    for warning in final["warnings"]:
        print(f"Warning: {warning}")
    print("Live integration test: PASS")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Live integration test: FAIL: {exc}", file=sys.stderr)
        sys.exit(1)
