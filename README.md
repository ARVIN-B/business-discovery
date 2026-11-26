# Business Discovery API

Independent FastAPI service for discovering Iranian business websites through local SearXNG and extracting contact numbers through the existing Firecrawl API.

## Runtime and endpoints

- Application: `/opt/business-discovery`
- Environment: `/opt/business-discovery/.env` (root-owned, mode `0600`)
- Local API: `http://127.0.0.1:8091`
- Health: `GET /health`
- Discovery: `POST /v1/discover` (Bearer token required)
- SearXNG used by the service: `http://127.0.0.1:8889`
- Firecrawl used by the service: `http://127.0.0.1:3002`
- Process management: `systemctl status|restart business-discovery`
- Logs: `journalctl -u business-discovery`

The API binds only to loopback. It has no public HTTPS URL yet. The existing Nginx configuration was not changed; a public URL needs a DNS name and TLS certificate, then a separately reviewed reverse proxy configuration.

## Request

```http
POST /v1/discover
Authorization: Bearer <API_KEY>
Content-Type: application/json
```

```json
{"keyword":"رستوران","province":"اصفهان","count":10}
```

`count` accepts 1–100. Search collects up to 10% more unique business domains than requested, deduplicates by domain, then Firecrawl processes the discovered sites. Crawl limits and concurrency are configurable with `MAX_CRAWL_PAGES`, `MAX_CRAWL_DEPTH`, `MAX_CONCURRENT_SITES`, and `CRAWL_TIMEOUT`.

## Response shape

```json
{
  "keyword": "رستوران",
  "province": "اصفهان",
  "requested_count": 10,
  "discovered_urls": 11,
  "processed_websites": 11,
  "results": [
    {
      "business_name": "Example",
      "website": "https://example.ir/contact",
      "discovered_url": "https://example.ir/contact",
      "pages_processed": 4,
      "phones": [
        {"number": "09123456789", "type": "mobile", "source_url": "https://example.ir/contact"}
      ]
    }
  ],
  "warnings": []
}
```

The operation is synchronous. Each site can use up to the configured crawl timeout, and sites run three at a time by default. At `count=100`, worst-case timing can approach four hours, especially when Firecrawl must also try contact-page fallbacks. Use a long-running client worker or a small count for interactive requests. Each phone retains the page URL Firecrawl reported as its source. Partial crawl pages are retained and timeout details are reported per website.

## Chabokan integration status

The API is ready for local authenticated requests. Chabokan cannot connect publicly yet: no domain or TLS endpoint is configured, and no Nginx changes have been made. Needed to publish: a DNS hostname pointing at this VPS and approval to add an authenticated HTTPS reverse proxy. Keep port 8091 private; do not open it in the firewall.

## Live smoke test

As root on the VPS:

```bash
/opt/business-discovery/.venv/bin/python /opt/business-discovery/test_service.py
```

This uses the real local SearXNG and Firecrawl services and exits nonzero on any request or response-schema failure.
