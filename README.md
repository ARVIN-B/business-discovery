# Business Discovery API

FastAPI service that searches SearXNG and crawls Iranian candidate URLs with Firecrawl. New jobs automatically alternate between durable search batches and sequential URL crawls, publishing each completed URL and number as soon as it is checkpointed. The intended consumer is `info`.

## Runtime

- Application: `/opt/business-discovery`
- API listener: `http://127.0.0.1:8091`
- Health: `GET /health`
- SQLite jobs and events: `/opt/business-discovery/data/jobs.sqlite3`
- Service: `business-discovery.service`

Every `/v1` endpoint requires `Authorization: Bearer <API_KEY>`. The service uses a single Uvicorn worker and SQLite process lock. Job phases, URL queue, SearXNG cursors, Firecrawl job IDs/page cursors, numbers, compatibility decisions, and ordered SSE events are stored durably.

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

`count` remains accepted for request compatibility; discovery continues until the search space is exhausted or the caller stops it. A created job returns `job_id`, `status_url`, `events_url`, `result_url`, and `stop_url`.

The job collects up to 100 URLs before crawling. `SEARCH_BATCH_SIZE` defaults to 100 and is capped at 100. Search requests one SearXNG page at a time; each response, query cursor, and discovered URL is committed before the next request. Extra URLs from a response are stored as queued rows for a later batch. SQLite uniqueness checks prevent duplicate URLs across queries and batches without retaining a large `seen_urls` Python set.

Each URL is crawled sequentially, with only one active Firecrawl URL globally. Firecrawl's job ID, pagination cursor, page offset, extracted numbers, and URL status are checkpointed as work proceeds. Each new number emits `number_found`; finishing a URL emits `url_completed`, after which the next URL starts automatically. URLs with no numbers and URLs with a final crawl error also produce `url_completed`.

### Stop and legacy Boolean compatibility

Use `POST /v1/jobs/{job_id}/stop` to cancel a running job. The stop request is checked between crawl pages and search requests, and accumulated results are preserved. A request already in flight may finish before the worker observes cancellation; this can mean at most the current SearXNG response or Firecrawl page response is received and checkpointed.

For compatibility with older Info Extractor workers, `url_completed` continues to include a unique `decision_id` and `decision_url`. Send a JSON Boolean as the entire request body:

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

Only literal JSON `true` or `false` is accepted. On new jobs `true` is an idempotent no-op because work advances automatically. `false` requests a graceful stop. Unknown decision IDs return `409`. This endpoint is deprecated; new Info Extractor code should use the stop endpoint only when the user cancels.

## SSE and result contract

`GET /v1/jobs/{job_id}/events` streams durable events. Reconnect with `Last-Event-ID` or `?last_event_id=...`; event IDs are monotonically increasing and replayed in order.

Every extracted unique number produces `number_found` immediately, and every completed candidate URL produces `url_completed`, including URLs with no numbers. `url_completed.data.results` remains for compatibility; consumers should deduplicate by number because a reconnect can replay events.

```text
event: url_completed
data: {"candidate_url":"https://example.ir/listing/42","results":[{"number":"09123456789","source_url":"https://example.ir/listing/42","website_url":"https://example.ir/","page_title":"آگهی","page_description":""}],"new_results_count":1,"error":null,"decision_id":"d1...","decision_url":"/v1/jobs/JOB_ID/decisions/d1...","batch_number":1}
```

Each result record has exactly `number`, `source_url`, `website_url`, `page_title`, and `page_description`. Numbers are normalized to `09xxxxxxxxx` and deduplicated across the full job. `source_url` preserves the actual listing/page URL; `website_url` is its root origin.

`GET /v1/jobs/{job_id}` exposes status, current phase, batch counts, current query/URL, queued URL count, result count, retry time, memory pause details, and last checkpoint. Typical stages are `searching`, `batch_ready`, `crawling`, and `paused_memory`. `GET /v1/jobs/{job_id}/result` returns `409` until the job is terminal (`completed`, `failed`, or `stopped`).

## Search, retries, and recovery

The search strategy keeps Persian location/topic queries and expands common topics into related terms. It tries each Persian query with `site:.ir` and also has a localized fallback without the operator. Candidate validation accepts `.ir` domains; a different top-level domain is accepted only if SearXNG's title/snippet contains the requested location in Persian. This keeps relevant local `.com` businesses discoverable while rejecting unrelated international results. SearXNG's `language` parameter is an engine preference; `en-US` does not translate the query or mean that English businesses are requested. Live validation returned Iranian `.ir` sites for the Persian query `موبایل اصفهان site:.ir`.

Search keeps an independent next-page cursor and exhaustion state for each query. An empty response exhausts only that query. A page with no results while SearXNG reports unresponsive engines is treated as a provider failure; the cursor is retained and the worker tries other local queries before pausing for retry. It does not exhaust that query. Consecutive identical result pages are detected to prevent pagination loops. Search continues into other queries until a full URL batch is ready or every query has truly exhausted. SearXNG reported DuckDuckGo CAPTCHA responses and Google CSE rate suspension during live checks, so these conditions must not be confused with a genuine empty search.

Search requests default to `SEARXNG_LANGUAGE=en-US`. In live checks, `fa-IR` and `all` returned zero results for the tested queries while some `en-US` requests also returned zero when engines were unavailable. The Persian query with the explicit `site:.ir` constraint returned ten Iranian-domain results with `en-US`. The parameter alone does not guarantee results; the query restriction and healthy search engines matter too.

`MAX_SEARCH_PAGES` defaults to `0`, meaning no hard page-count limit. If SearXNG fails, the cursor is retained and retry is scheduled after `SEARCH_RETRY_SECONDS` (default 60), or after a valid SearXNG `Retry-After` value. Authenticated job status and final results include aggregate `search_diagnostics` and per-query `query_diagnostics`: page/raw-result/valid/new/duplicate/rejected totals, cursor, retry state, last HTTP status, rejection reasons, and last-page metrics. These diagnostics contain no phone values.

Internal job model version 5 stores normalized batch, URL, and result rows in SQLite. Search pages and number events are transactional checkpoints. On restart, the worker resumes the saved query/page cursor or Firecrawl job/pagination cursor; unique constraints make replayed page extraction safe. Existing version 4 approval jobs migrate to automatic execution; a URL that was already awaiting approval is treated as completed, and its decision ID remains valid for delayed compatibility requests. Older job models keep their original runner.

The memory guard reads Linux host `MemAvailable` and cgroup usage. It pauses between search pages and crawl pages when memory pressure is high, persists the phase, and resumes after pressure falls. This is a soft application guard: the service cannot guarantee a hard 7 GiB ceiling or prevent the kernel from killing a process between checks. Set a hard memory cap for this service/container with systemd or Docker for production. To enforce a host-wide ceiling, allocate cgroup limits across this app, SearXNG, Firecrawl, RabbitMQ, PostgreSQL, Redis, Evolution API, and every other container; reserve memory for the OS and filesystem cache. Those external service definitions are outside this repository and were not modified.

## Configuration

Relevant `.env` settings:

- `SEARCH_BATCH_SIZE=100` (hard-capped at 100)
- `SEARCH_RETRY_SECONDS=60`
- `SEARXNG_LANGUAGE=en-US`
- Search candidate policy: `.ir`, or non-`.ir` only when Persian indexed text names the requested location
- `MAX_SEARCH_PAGES=0` (zero means unlimited pagination)
- `SEARXNG_URL`, `SEARCH_TIMEOUT`
- `FIRECRAWL_URL`, `CRAWL_TIMEOUT`, `MAX_CRAWL_PAGES`, `MAX_CRAWL_DEPTH`
- `MAX_CONCURRENT_SITES=1` (one Firecrawl URL globally)
- `HOST_MEMORY_BUDGET_MB=7168`, `MEMORY_AVAILABLE_FLOOR_MB=1024`, `MEMORY_CHECK_INTERVAL_SECONDS=15`
- `JOB_DB_PATH`, `JOB_TTL_SECONDS`, `JOB_CLEANUP_INTERVAL`

The application supplies defaults; no `.env` edit is required unless you want to change batch size or retry delay.

## Info Extractor connection

See [Info Extractor integration](doc/INFO_EXTRACTOR_INTEGRATION.md) for the remote service configuration, automatic event flow, cancellation, and search diagnostics.

## Tests

Offline unit and integration tests:

```bash
/opt/business-discovery/.venv/bin/python -m unittest discover -s /opt/business-discovery/tests -v
```

The live smoke test consumes SSE while the job runs. It should only be run against an intentional test environment because it starts real SearXNG and Firecrawl work.

## Deployment

Code changes do not restart or deploy the service automatically. Public access and service restart remain separate operational steps.
