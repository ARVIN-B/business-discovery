#!/usr/bin/env python3
"""Live smoke test for the local API using real SearXNG and Firecrawl services."""
import json
import os
import re
from urllib.parse import urlsplit
import sys
import urllib.error
import urllib.request
from pathlib import Path

BASE_URL = os.getenv("TEST_BASE_URL", "http://127.0.0.1:8091")
TEST_COUNT = int(os.getenv("TEST_COUNT", "1"))
ENV_FILE = Path("/opt/business-discovery/.env")


def read_key():
    if os.getenv("API_KEY"):
        return os.environ["API_KEY"]
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        if line.startswith("API_KEY="):
            return line.partition("=")[2].strip().strip('"').strip("'")
    raise RuntimeError("API_KEY not found in environment or service env file")


def request(url, *, payload=None, headers=None, timeout=1800):
    data = json.dumps(payload, ensure_ascii=False).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, headers=headers or {}, method="POST" if data else "GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as res:
            return res.status, json.loads(res.read().decode())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")[:1000]
        raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc


try:
    status, health = request(BASE_URL + "/health", timeout=10)
    if status != 200 or health.get("status") != "ok":
        raise RuntimeError(f"Health check failed: status={status}, body={health}")
    print("Health: OK")

    status, result = request(
        BASE_URL + "/v1/discover",
        payload={"keyword": "رستوران", "province": "اصفهان", "count": TEST_COUNT},
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + read_key()},
        timeout=1800,
    )
    required = {
        "requested_count", "successful_websites", "total_unique_mobile_numbers",
        "completed_rounds", "results",
    }
    if status != 200 or not required.issubset(result) or not isinstance(result["results"], list):
        raise RuntimeError("Discovery response does not match the expected JSON structure")

    if result["successful_websites"] != len(result["results"]):
        raise RuntimeError("successful_websites does not match results length")

    seen_numbers = set()
    print(f"Successful websites: {result['successful_websites']}")
    print(f"Unique mobile numbers: {result['total_unique_mobile_numbers']}")
    print(f"Completed search rounds: {result['completed_rounds']}")
    for website in result["results"]:
        if not {"website_url", "mobile_numbers"}.issubset(website) or not website["mobile_numbers"]:
            raise RuntimeError("A successful website result is missing its mobiles")
        print(f"\nWebsite: {website['website_url']}")
        for phone in website["mobile_numbers"]:
            if not {"number", "source_url"}.issubset(phone):
                raise RuntimeError("A mobile number is missing its source URL")
            number = phone["number"]
            source = urlsplit(phone["source_url"])
            if not re.fullmatch(r"09\d{9}", number):
                raise RuntimeError(f"Invalid or non-mobile number in response: {number}")
            if number in seen_numbers:
                raise RuntimeError(f"Duplicate mobile number in response: {number}")
            if source.scheme not in {"http", "https"} or not source.hostname:
                raise RuntimeError(f"Invalid source_url for {number}: {phone['source_url']}")
            seen_numbers.add(number)
            print(f"  Mobile: {number} (source: {phone['source_url']})")
    if len(seen_numbers) != result["total_unique_mobile_numbers"]:
        raise RuntimeError("total_unique_mobile_numbers does not match returned mobiles")
    for warning in result.get("warnings", []):
        print(f"Warning: {warning}")
    print("Live test: PASS")
except Exception as exc:
    print(f"Live test: FAIL: {exc}", file=sys.stderr)
    sys.exit(1)
