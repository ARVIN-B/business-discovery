#!/usr/bin/env python3
"""Live smoke test for the local API using the real SearXNG and Firecrawl services."""
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

BASE_URL = os.getenv("TEST_BASE_URL", "http://127.0.0.1:8091")
ENV_FILE = Path("/opt/business-discovery/.env")

def read_key():
    if os.getenv("API_KEY"):
        return os.environ["API_KEY"]
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        if line.startswith("API_KEY="):
            return line.partition("=")[2].strip().strip('"').strip("'")
    raise RuntimeError("API_KEY not found in environment or service env file")

def request(url, *, payload=None, headers=None, timeout=1200):
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
    status, result = request(BASE_URL + "/v1/discover", payload={"keyword": "رستوران", "province": "اصفهان", "count": 2}, headers={"Content-Type": "application/json", "Authorization": "Bearer " + read_key()}, timeout=1800)
    if status != 200:
        raise RuntimeError(f"Discovery returned HTTP {status}")
    required = {"keyword", "province", "requested_count", "discovered_urls", "processed_websites", "results", "warnings"}
    if not required.issubset(result) or not isinstance(result["results"], list):
        raise RuntimeError("Response JSON does not match the expected structure")
    print(f"Discovered URLs: {result['discovered_urls']}")
    print(f"Processed websites: {result['processed_websites']}")
    for item in result["results"]:
        if not {"business_name", "website", "discovered_url", "pages_processed", "phones"}.issubset(item):
            raise RuntimeError("A result is missing required fields")
        print(f"\nBusiness: {item['business_name']}\nWebsite: {item['website']}\nDiscovered URL: {item['discovered_url']}\nPages processed: {item['pages_processed']}")
        if item.get("error"):
            print(f"  Crawl error: {item['error']}")
        for phone in item["phones"]:
            print(f"  {phone['type']}: {phone['number']} (source: {phone['source_url']})")
    if result["warnings"]:
        print("Warnings: " + "; ".join(result["warnings"]))
    print("Live test: PASS")
except Exception as exc:
    print(f"Live test: FAIL: {exc}", file=sys.stderr)
    sys.exit(1)
