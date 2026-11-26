# Business Discovery API

Independent FastAPI service that searches the local SearXNG instance and uses the existing Firecrawl service to find unique Iranian mobile numbers. Discovery runs as a durable job: the request returns immediately, status/results are persisted in SQLite, and completed website results are available through polling or Server-Sent Events (SSE).

## Runtime and deployment

- Application: `/opt/business-discovery`
- Environment: `/opt/business-discovery/.env` (root-owned, mode `0600`)
- API listener: `http://127.0.0.1:8091` (loopback only)
- Health: `GET /health` (no authentication)
- SearXNG: `http://127.0.0.1:8889`
- Firecrawl: `http://127.0.0.1:3002`
- Job database: `/opt/business-discovery/data/jobs.sqlite3` (SQLite WAL; private to the service account)
- Process: `systemctl status|restart business-discovery`
- Logs: `journalctl -u business-discovery`
- Local SSE proxy template: `deploy/nginx-business-discovery.conf.example` (not enabled)

The service must run as one Uvicorn worker. A process lock prevents a second worker or service instance from consuming the same SQLite job queue. On restart, queued/running jobs are recovered from persisted state. This is a single-host, single-instance design; SQLite is not intended for multi-host deployment. Job records and events expire after seven days by default.

No public HTTPS endpoint is enabled. The current host name is not a usable public DNS name and port 443 has no trusted TLS listener. Keep port 8091 private. To enable the Nginx template, provide a DNS hostname controlled by you that resolves publicly to `195.248.240.152`, and arrange a trusted TLS certificate (for example, ACME HTTP-01 with port 80 reachable). Do not send Chabokan requests over public HTTP.

## Configuration

The systemd unit loads `.env`. Never put the API key in source control or include it in logs or requests other than the Authorization header.

| Variable | Default | Purpose |
| --- | --- | --- |
| `API_KEY` | required | Bearer token, at least 32 characters |
| `SEARXNG_URL` | `http://127.0.0.1:8889` | Internal SearXNG endpoint |
| `FIRECRAWL_URL` | `http://127.0.0.1:3002` | Internal Firecrawl endpoint |
| `SEARCH_TIMEOUT` | `20` | Search request timeout, seconds |
| `MAX_SEARCH_PAGES` | `50` | Maximum SearXNG pages per exact query |
| `CRAWL_TIMEOUT` | `240` | Firecrawl crawl deadline, seconds |
| `MAX_CRAWL_PAGES` | `10` | Maximum crawl pages per website |
| `MAX_CRAWL_DEPTH` | `2` | Maximum crawl depth |
| `MAX_CONCURRENT_SITES` | `3` | Website processing concurrency |
| `JOB_DB_PATH` | `/opt/business-discovery/data/jobs.sqlite3` | Persistent job/event database |
| `JOB_TTL_SECONDS` | `604800` | Job/event retention (seven days) |
| `JOB_CLEANUP_INTERVAL` | `3600` | Expiration cleanup interval, seconds |
| `LOG_LEVEL` | `INFO` | Application log level |

## API workflow

Every endpoint under `/v1` requires `Authorization: Bearer <API_KEY>`. The API docs and OpenAPI routes are disabled. `count` remains the target number of successful unique websites (a website is successful only when it adds at least one new valid mobile number), not a number of phone numbers.

### 1. Create a job

```http
POST /v1/discover
Authorization: Bearer <API_KEY>
Idempotency-Key: optional-client-generated-key
Content-Type: application/json
```

```json
{"keyword":"رستوران","province":"اصفهان","count":10}
```

Returns HTTP `202 Accepted` with the job ID and relative links. Reusing an `Idempotency-Key` with the same request returns the existing job; reusing it with a different body returns `409`.

```json
{
  "job_id":"a1b2c3...",
  "status":"queued",
  "requested_count":10,
  "current_round":0,
  "total_rounds":3,
  "successful_websites":0,
  "total_unique_mobile_numbers":0,
  "current_stage":"queued",
  "created":true,
  "status_url":"/v1/jobs/a1b2c3...",
  "events_url":"/v1/jobs/a1b2c3.../events",
  "result_url":"/v1/jobs/a1b2c3.../result"
}
```

### 2. Poll status

`GET /v1/jobs/{job_id}` returns the current persisted job state, including current round/stage, sites discovered/processed/remaining for that round, successful website and unique number counts, per-website states, warnings/errors, accumulated results, and `last_event_id`.

Status values are `queued`, `running`, `completed`, or `failed`. `current_stage` reports finer-grained progress such as `searching`, `processing`, and terminal stages. Per-website states include `pending`, `processing`, `completed`, `failed`, and `skipped`.

### 3. Stream results and resume after reconnect

`GET /v1/jobs/{job_id}/events` returns `text/event-stream`. The worker is independent of the client connection, so disconnecting does not stop discovery. Events are persisted and assigned increasing IDs. Reconnect with the last received event ID:

```http
GET /v1/jobs/{job_id}/events
Authorization: Bearer <API_KEY>
Last-Event-ID: 17
```

Or use `?last_event_id=17` when the HTTP client cannot set `Last-Event-ID`. The server replays all later events in order. A heartbeat comment is sent every 15 seconds when there is no new event. Keep the connection open while consuming live results.

Events include `job_started`, `round_started`, `progress`, `search_completed`, `website_started`, `website_completed`, `warning`, `round_completed`, `job_completed`, and `job_failed`. A `website_completed` event includes `result` when the site added one or more previously unregistered numbers:

```text
id: 21
event: website_completed
data: {"website_url":"https://example.ir/","status":"completed","result":{"website_url":"https://example.ir/","mobile_numbers":[{"number":"09123456789","source_url":"https://example.ir/contact"}]}}
```

Numbers are globally deduplicated across all websites and rounds. Only valid Iranian mobile numbers normalized as `09xxxxxxxxx` are returned; landlines are omitted. Each number preserves its source page URL.

### 4. Fetch final result

`GET /v1/jobs/{job_id}/result` returns HTTP `409` with the current status while the job is active. For a terminal job it returns:

```json
{
  "job_id":"a1b2c3...",
  "status":"completed",
  "requested_count":10,
  "successful_websites":8,
  "total_unique_mobile_numbers":13,
  "completed_rounds":3,
  "results":[
    {"website_url":"https://example.ir/","mobile_numbers":[{"number":"09123456789","source_url":"https://example.ir/contact"}]}
  ],
  "errors":[],
  "warnings":[]
}
```

An empty search is a successful completed job with empty `results`. `404` means the job is unknown or expired. `401` means the token is absent or invalid. A failed job still exposes its accumulated result and error details.

## Search and crawl rules

The job runner retains the three-round query strategy. Each round searches for `ceil(remaining_successful_websites * 1.6)` candidate URLs, processes unique domains, and stops once the requested successful website count is reached or all rounds are exhausted. Exact search queries have independent persisted SearXNG page cursors; a later request for that same query starts after its last consumed page. Domains processed in earlier rounds are excluded globally. Empty result pages end that query; HTTP/provider errors and invalid JSON are recorded as warnings/errors rather than silently treated as empty results.

Firecrawl remains bounded by the crawl page/depth/deadline settings above. Website work is concurrent and each finished result is stored and emitted immediately, without waiting for other websites in the same batch.

## Chabokan integration

From Chabokan, call the eventual public HTTPS hostname with the Bearer token and `POST /v1/discover`. Store the token in Chabokan's secret configuration. Persist the returned `job_id`; consume `/events` and reconnect with `Last-Event-ID`, or poll `/jobs/{job_id}` after a reconnect. Fetch `/result` when the status becomes terminal. This workflow is asynchronous: the create call should return quickly, and the client should apply its own bounded timeouts to status/event calls. The public base URL and certificate cannot be supplied until you provide a public DNS hostname for this VPS and trusted TLS is configured. No Chabokan files have been changed.

## Deploy, restart, and verification

```bash
sudo systemctl daemon-reload
sudo systemctl restart business-discovery
sudo systemctl status business-discovery --no-pager
curl --fail http://127.0.0.1:8091/health
/opt/business-discovery/.venv/bin/python -m unittest discover -s /opt/business-discovery/tests -v
/opt/business-discovery/.venv/bin/python /opt/business-discovery/test_service.py
```

The live smoke test reads `API_KEY` from the service environment file or process environment, submits a real request, waits for completion, verifies status/result and SSE replay, and exits nonzero on failure. It uses the configured real local SearXNG and Firecrawl services.
