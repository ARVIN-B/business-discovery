# Business Discovery API

FastAPI service that searches SearXNG and crawls candidate URLs with Firecrawl. New jobs use a durable batch/search queue and wait for an external Boolean decision after every completed URL. The intended consumer is `info`.

## Runtime

- Application: `/opt/business-discovery`
- API listener: `http://127.0.0.1:8091`
- Health: `GET /health`
- SQLite jobs and events: `/opt/business-discovery/data/jobs.sqlite3`
- Service: `business-discovery.service`

Every `/v1` endpoint requires `Authorization: Bearer <API_KEY>`. The service uses a single Uvicorn worker and SQLite process lock. Job state, URL queue, query cursors, decisions, results, and ordered SSE events are stored durably.

## Create and follow a job

```http
POST /v1/discover
Content-Type: application/json
Authorization: Bearer <API_KEY>
Idempotency-Key: optional-client-generated-key
```

```json
{"keyword":"آگهی موبایل","province":"اصفهان"}
```

`count` remains accepted for request compatibility, but new approval-controlled jobs do not stop based on it. A created job returns `job_id`, `status_url`, `events_url`, `result_url`, and `stop_url`.

The job collects a full URL batch before crawling. `SEARCH_BATCH_SIZE` defaults to 1000. Search consumes pages until that many unique valid URLs are queued or all search queries are exhausted. Any extra URLs from the last SearXNG page remain queued for the next batch. URLs are normalized and added to the durable `seen_urls` set when discovered, so duplicates across pages, queries, and batches are ignored. Distinct listing paths and meaningful query parameters remain separate candidates.

Each URL is crawled sequentially. When it finishes, the service persists its full set of new numbers and a `url_completed` event, then waits in `awaiting_approval`. It does not start another URL or search batch until the decision arrives. URLs with zero numbers and URLs with a Firecrawl error still produce `url_completed` and await a decision.

### Boolean decision protocol

The `url_completed` SSE event includes a unique `decision_id` and `decision_url`. Send a JSON Boolean as the entire request body:

```http
POST /v1/jobs/{job_id}/decisions/{decision_id}
Authorization: Bearer <API_KEY>
Content-Type: application/json
```

Continue:

```json
true
```

Stop:

```json
false
```

Only literal JSON `true` or `false` is accepted. The same decision can be repeated idempotently. Sending the opposite value for an already decided ID, or an unknown/stale decision ID, returns `409`. `true` advances to the next URL, or starts search for another batch when the current batch is done. `false` ends the job as `stopped` and preserves accumulated results.

The existing `POST /v1/jobs/{job_id}/stop` endpoint remains available for immediate cancellation. It can stop a job waiting for approval without needing a decision ID.

## SSE and result contract

`GET /v1/jobs/{job_id}/events` streams durable events. Reconnect with `Last-Event-ID` or `?last_event_id=...`; event IDs are monotonically increasing and replayed in order.

Every completed candidate URL produces `url_completed`, including URLs with no new numbers. When it has new results, the previous `url_results_found` event is also retained for compatibility; consumers should count results from `url_completed` only.

```text
event: url_completed
data: {"candidate_url":"https://example.ir/listing/42","results":[{"number":"09123456789","source_url":"https://example.ir/listing/42","website_url":"https://example.ir/","page_title":"آگهی","page_description":""}],"new_results_count":1,"error":null,"decision_id":"d1...","decision_url":"/v1/jobs/JOB_ID/decisions/d1...","batch_number":1}
```

Each result record has exactly `number`, `source_url`, `website_url`, `page_title`, and `page_description`. Numbers are normalized to `09xxxxxxxxx` and deduplicated across the full job. `source_url` preserves the actual listing/page URL; `website_url` is its root origin.

`GET /v1/jobs/{job_id}` exposes status, current stage, queued and seen URL counts, batch position, `pending_decision_id`, results, and errors. Typical stages are `searching`, `crawling`, and `awaiting_approval`. `GET /v1/jobs/{job_id}/result` returns `409` until the job is terminal (`completed`, `failed`, or `stopped`).

## Search, retries, and recovery

The search strategy keeps Persian location/topic queries and expands common topics into related terms. It tries each Persian query with `site:.ir` and also has a localized fallback without the operator. Candidate validation accepts `.ir` domains; a different top-level domain is accepted only if SearXNG's title/snippet contains the requested location in Persian. This keeps relevant local `.com` businesses discoverable while rejecting unrelated international results. SearXNG's `language` parameter is an engine preference; `en-US` does not translate the query or mean that English businesses are requested. Live validation returned Iranian `.ir` sites for the Persian query `موبایل اصفهان site:.ir`.

Search keeps an independent next-page cursor and exhaustion state for each query. An empty response exhausts only that query. A page with no results while SearXNG reports unresponsive engines is treated as a provider failure; the cursor is retained and the worker tries other local queries before pausing for retry. It does not exhaust that query. Consecutive identical result pages are detected to prevent pagination loops. Search continues into other queries until a full URL batch is ready or every query has truly exhausted. SearXNG reported DuckDuckGo CAPTCHA responses and Google CSE rate suspension during live checks, so these conditions must not be confused with a genuine empty search.

Search requests default to `SEARXNG_LANGUAGE=en-US`. In live checks, `fa-IR` and `all` returned zero results for the tested queries while some `en-US` requests also returned zero when engines were unavailable. The Persian query with the explicit `site:.ir` constraint returned ten Iranian-domain results with `en-US`. The parameter alone does not guarantee results; the query restriction and healthy search engines matter too.

`MAX_SEARCH_PAGES` defaults to `0`, meaning no hard page-count limit. If SearXNG fails, the cursor is retained and retry is scheduled after `SEARCH_RETRY_SECONDS` (default 60), or after a valid SearXNG `Retry-After` value. Authenticated job status and final results include aggregate `search_diagnostics` and per-query `query_diagnostics`: page/raw-result/valid/new/duplicate/rejected totals, cursor, retry state, last HTTP status, rejection reasons, and last-page metrics. These diagnostics contain no phone values.

Internal job model version 4 stores the queue, seen URLs, batch, query cursors, page states, pending decision, decision history, and results in SQLite. On restart, a job awaiting approval keeps the same decision ID. An interrupted crawl URL is returned to pending and safely retried; result rows and their URL events are committed in the same transaction. Existing version 3 jobs keep their prior runner and event history.

## Configuration

Relevant `.env` settings:

- `SEARCH_BATCH_SIZE=1000`
- `SEARCH_RETRY_SECONDS=60`
- `SEARXNG_LANGUAGE=en-US`
- Search candidate policy: `.ir`, or non-`.ir` only when Persian indexed text names the requested location
- `MAX_SEARCH_PAGES=0` (zero means unlimited pagination)
- `SEARXNG_URL`, `SEARCH_TIMEOUT`
- `FIRECRAWL_URL`, `CRAWL_TIMEOUT`, `MAX_CRAWL_PAGES`, `MAX_CRAWL_DEPTH`
- `MAX_CONCURRENT_SITES` (new jobs crawl one URL at a time)
- `JOB_DB_PATH`, `JOB_TTL_SECONDS`, `JOB_CLEANUP_INTERVAL`

The application supplies defaults; no `.env` edit is required unless you want to change batch size or retry delay.

## Info Extractor connection

See [Info Extractor integration](doc/INFO_EXTRACTOR_INTEGRATION.md) for the remote service configuration, request/decision flow, and diagnostics to inspect when a web search ends without candidate URLs.

## Tests

Offline unit and integration tests:

```bash
/opt/business-discovery/.venv/bin/python -m unittest discover -s /opt/business-discovery/tests -v
```

The live smoke test consumes SSE while the job runs and sends `true` for every `url_completed` event. It should only be run against an intentional test environment because it starts real SearXNG and Firecrawl work.

## Deployment

Code changes do not restart or deploy the service automatically. Public access and service restart remain separate operational steps.
