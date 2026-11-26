# Business Discovery API

Independent FastAPI service that searches through local SearXNG and uses the existing Firecrawl service to discover websites with unique Iranian mobile numbers.

## Runtime

- Application: `/opt/business-discovery`
- Environment: `/opt/business-discovery/.env` (root-owned, mode `0600`)
- Local API: `http://127.0.0.1:8091`
- Health: `GET /health`
- Discovery: `POST /v1/discover` (Bearer token required)
- SearXNG: `http://127.0.0.1:8889`
- Firecrawl: `http://127.0.0.1:3002`
- Process: `systemctl status|restart business-discovery`
- Logs: `journalctl -u business-discovery`

The API binds only to loopback. No public HTTPS endpoint is configured. Nginx was not changed. Publishing requires a DNS hostname and TLS reverse proxy configuration.

## Request

```http
POST /v1/discover
Authorization: Bearer <API_KEY>
Content-Type: application/json
```

```json
{"keyword":"رستوران","province":"اصفهان","count":10}
```

`count` is the target number of unique websites that contribute at least one unique Iranian mobile. The service makes up to three search/crawl rounds. Each round requests `ceil(remaining_count * 1.6)` unique domains, excludes domains processed earlier in the request, and stops early when the target number of successful websites is reached. Timeouts on individual sites do not stop the other sites or later rounds.

Only Iranian mobile numbers are returned, normalized as `09xxxxxxxxx`. Landlines, invalid numbers, and duplicate mobile numbers are omitted. Each number includes the page URL from which it was extracted. Websites with no new unique mobile number do not appear in `results`.

Crawl limits are configurable through `MAX_CRAWL_PAGES`, `MAX_CRAWL_DEPTH`, `MAX_CONCURRENT_SITES`, and `CRAWL_TIMEOUT`.

## Response

```json
{
  "requested_count": 10,
  "successful_websites": 8,
  "total_unique_mobile_numbers": 13,
  "completed_rounds": 3,
  "results": [
    {
      "website_url": "https://example.ir/",
      "mobile_numbers": [
        {"number": "09123456789", "source_url": "https://example.ir/contact"}
      ]
    }
  ]
}
```

A valid response with zero matches has `successful_websites: 0` and `results: []`.

The operation is synchronous. Sites run three at a time by default. At `count=100`, worst-case duration may approach four hours. Use a long-running client worker or small counts for interactive requests.

## Chabokan integration status

Chabokan cannot connect publicly yet: no domain or TLS endpoint is configured. To publish, provide a DNS hostname pointing to this VPS; an authenticated HTTPS reverse proxy will then need to be configured. Keep port 8091 private.

## Tests

Unit tests cover phone normalization, extraction, deduplication, round sizing, stop conditions, and source attribution:

```bash
/opt/business-discovery/.venv/bin/python -m unittest discover -s /opt/business-discovery/tests -v
```

Live smoke test (real SearXNG and Firecrawl, no mocks):

```bash
/opt/business-discovery/.venv/bin/python /opt/business-discovery/test_service.py
```
