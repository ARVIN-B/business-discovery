# Business Discovery API

Independent FastAPI service that searches local SearXNG and uses the existing Firecrawl service to discover Iranian mobile numbers. Discovery runs as a durable job and publishes each newly accepted unique mobile number over SSE as soon as its page-processing task completes.

## Runtime

- Application: `/opt/business-discovery`
- API listener: `http://127.0.0.1:8091` (loopback only)
- Health: `GET /health`
- SearXNG: `http://127.0.0.1:8889`
- Firecrawl: `http://127.0.0.1:3002`
- SQLite job/event store: `/opt/business-discovery/data/jobs.sqlite3`
- Service: `business-discovery.service`
- Logs: `journalctl -u business-discovery`

The API requires Bearer authentication on `/v1`. It uses one Uvicorn worker and a process lock on the SQLite file. Job state and events survive application restart. The database retains records for seven days by default. This is a single-host design, not a multi-instance queue.

## URL-level discovery

Every normalized full URL is an independent search candidate. Multiple listing, product, or seller pages under the same domain are not collapsed together. Tracking parameters such as `utm_*`, `fbclid`, and `gclid` are removed while listing IDs and meaningful query parameters are kept. Previously processed full URLs are excluded on later rounds; other pages on the same domain remain eligible.

Firecrawl may return several crawled pages for one candidate URL. Each extracted mobile number is associated with the actual normalized page URL and metadata returned for that page. `website_url` is the root origin for secondary display; `source_url` is the primary clickable result. The API does not infer names or descriptions. Missing metadata is an empty string.

`count` means the target number of unique valid Iranian mobile numbers. It does not mean websites, domains, or candidate pages. Numbers are normalized to `09xxxxxxxxx`, deduplicated across the whole job, and limited to the first valid source encountered. One source page can produce multiple flat result records. Concurrent page tasks cannot add more results than requested.

## API workflow (schema version 2)

All `/v1` requests require:

```http
Authorization: Bearer <API_KEY>
```

### Create a job

```http
POST /v1/discover
Content-Type: application/json
Idempotency-Key: optional-client-generated-key
```

```json
{"keyword":"آگهی موبایل","province":"اصفهان","count":5}
```

Returns HTTP `202 Accepted` with `job_id`, `schema_version: 2`, and relative `status_url`, `events_url`, and `result_url`. Reusing the same idempotency key and body returns the existing job; reusing it with a different body returns `409`.

### Job status

`GET /v1/jobs/{job_id}` returns status, accumulated flat results, warnings/errors, current round and stage, and page metrics. Status is `queued`, `running`, `completed`, or `failed`.

```json
{
  "schema_version": 2,
  "job_id": "example-job-id",
  "status": "running",
  "requested_count": 5,
  "successful_results": 1,
  "total_unique_mobile_numbers": 1,
  "discovered_page_count": 4,
  "processed_page_count": 2,
  "successful_page_count": 1,
  "results": [
    {
      "number": "09123456789",
      "source_url": "https://divar.ir/v/listing-1",
      "website_url": "https://divar.ir/",
      "page_title": "عنوان واقعی آگهی",
      "page_description": ""
    }
  ]
}
```

Page metrics describe candidate URLs returned by SearXNG and processed by the application. A candidate page is successful if its crawl contributes at least one previously unregistered mobile number. These metrics are separate from `successful_results`.

### SSE and reconnect

`GET /v1/jobs/{job_id}/events` streams persisted events as `text/event-stream`. Processing continues if the SSE client disconnects. Event IDs increase monotonically; reconnect using `Last-Event-ID` or `?last_event_id=...`. The service replays every later event in order and sends heartbeat comments every 15 seconds while idle.

Each unique accepted phone produces one `result_found` event, persisted atomically with the result:

```text
id: 25
event: result_found
data: {"result":{"number":"09123456789","source_url":"https://divar.ir/v/listing-1","website_url":"https://divar.ir/","page_title":"عنوان واقعی آگهی","page_description":""},"successful_results":1,"total_unique_mobile_numbers":1,"requested_count":5}
```

Page lifecycle events are URL-oriented. For example, `page_completed` includes `page_url`, `website_url`, `status`, `new_results_count`, `processed_page_count`, `successful_page_count`, `successful_results`, and `requested_count`. Other events include `job_started`, `round_started`, `search_completed`, `progress`, `warning`, `round_completed`, `job_completed`, and `job_failed`. The terminal `job_completed` event contains the final counters and flat result array.

### Final result

`GET /v1/jobs/{job_id}/result` returns `409` while a job is active, then the final response:

```json
{
  "schema_version": 2,
  "job_id": "example-job-id",
  "status": "completed",
  "requested_count": 5,
  "successful_results": 1,
  "total_unique_mobile_numbers": 1,
  "discovered_page_count": 12,
  "processed_page_count": 9,
  "successful_page_count": 1,
  "completed_rounds": 2,
  "results": [
    {
      "number": "09123456789",
      "source_url": "https://divar.ir/v/listing-1",
      "website_url": "https://divar.ir/",
      "page_title": "عنوان واقعی آگهی",
      "page_description": ""
    }
  ],
  "errors": [],
  "warnings": []
}
```

`successful_results == total_unique_mobile_numbers == len(results)`, and the count never exceeds `requested_count`. When all search rounds/pages are exhausted first, the job finishes as `completed` with its actual results and a warning that the requested count was not reached. Provider errors remain in `errors`/`warnings` and do not erase previously stored results. `404` means unknown or expired job; `401` means invalid/missing token.

## Search, crawl, and recovery behavior

The service retains up to three search rounds, its existing query variations, `ceil(remaining_unique_numbers * 1.6)` URL target, SearXNG pagination limit, Firecrawl page/depth/time limits, and site concurrency. Pagination state is independent for each exact search query. Search result candidates and processed state are deduplicated by normalized full URL, not host.

SQLite state uses `schema_version: 2`. On startup, legacy v1 nested website results are migrated to flat records, keeping the first occurrence of each valid mobile number and adding replayable `result_found` events where absent. Legacy processed domain strings are retained for audit only and are never treated as URL exclusions. Existing event history is kept.

## Configuration and Chabokan

The service reads `.env`; keep `API_KEY` out of source control. Existing settings include `SEARXNG_URL`, `FIRECRAWL_URL`, `SEARCH_TIMEOUT`, `MAX_SEARCH_PAGES`, `CRAWL_TIMEOUT`, `MAX_CRAWL_PAGES`, `MAX_CRAWL_DEPTH`, `MAX_CONCURRENT_SITES`, `JOB_DB_PATH`, `JOB_TTL_SECONDS`, and `JOB_CLEANUP_INTERVAL`.

Chabokan should send `POST /v1/discover` with a Bearer token, store the returned job ID, consume SSE with `Last-Event-ID` on reconnect, and fetch the final result after terminal status. Public HTTPS remains a separate deployment step requiring a DNS hostname and trusted certificate; do not use public HTTP or expose port 8091 directly.

## Tests

Offline tests use deterministic fixtures and mocks:

```bash
/opt/business-discovery/.venv/bin/python -m unittest discover -s /opt/business-discovery/tests -v
```

Live smoke test validates the deployed API, real SearXNG/Firecrawl path, flat records, and SSE:

```bash
/opt/business-discovery/.venv/bin/python /opt/business-discovery/test_service.py
```

Optional live-test variables: `TEST_BASE_URL`, `TEST_KEYWORD`, `TEST_PROVINCE`, `TEST_COUNT`, and `TEST_JOB_TIMEOUT_SECONDS`.

## Deployment note

This code update intentionally does not restart or deploy the running service. After review, deployment commands are:

```bash
sudo systemctl restart business-discovery
sudo systemctl status business-discovery --no-pager
curl --fail http://127.0.0.1:8091/health
```
