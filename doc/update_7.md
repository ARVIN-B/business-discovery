# Task: Refactor Business Discovery into a durable, memory-aware batch pipeline

Repository: `https://github.com/ARVIN-B/business-discovery`

The current `main` branch was inspected at commit `d6253a5a77d32808d21b390bc64a089bf28d77ac`.

Act as a senior Python backend engineer specializing in FastAPI, asyncio, SQLite, durable background jobs, external crawling APIs, and memory-constrained Linux services.

Your task is to refactor the existing Business Discovery implementation carefully. This is a production-oriented refactor, not a rewrite from scratch.

## 1. Understand the existing implementation first

Before changing anything, inspect:

- `app/job_manager.py`
- `app/job_store.py`
- `app/main.py`
- `tests/test_jobs.py`
- `tests/test_discovery.py`
- `README.md`
- `doc/INFO_EXTRACTOR_INTEGRATION.md`
- `.env.example`

Inspect the current Git working tree and preserve existing uncommitted changes. Do not reset the repository or overwrite unrelated work.

The current implementation already contains useful functionality that must be preserved:

- SQLite-backed durable jobs and ordered SSE events.
- Search query pagination and per-query cursors.
- URL normalization and deduplication.
- Batch collection and sequential crawling for new jobs.
- Search retry handling and diagnostics.
- Idempotency keys.
- Stop endpoint.
- SSE reconnection using `Last-Event-ID`.
- Job recovery after service restart.
- Legacy job compatibility.

Do not unnecessarily replace these mechanisms.

## 2. Target architecture: one Stage = URL collection + crawling

The main unit of work must be a durable Batch/Stage.

Each Stage consists of two strictly sequential phases:

### Phase A — Collect URLs

1. Search SearXNG until 100 unique, valid candidate URLs have been collected.
2. The batch size must be configurable through `SEARCH_BATCH_SIZE`, with a default of **100**.
3. Do not begin crawling while URL collection for the current batch is still in progress, unless all search queries have been exhausted and a smaller final batch is ready.
4. Persist each successfully processed SearXNG page immediately.
5. Persist:
   - Current query index.
   - Current query text.
   - Next page number for every query.
   - Query exhaustion state.
   - Search diagnostics.
   - Discovered URLs.
   - URL deduplication state.
   - Current Batch number.
6. If a SearXNG page contains more candidate URLs than the remaining batch capacity, preserve the extra URLs durably for the next Batch. Do not lose them or search from the beginning.
7. If the search provider temporarily fails, preserve its cursor and retry later. Do not mark the query exhausted just because a provider failed.

When the Batch reaches 100 URLs, or search is genuinely exhausted, persist the completed URL collection phase and transition to Phase B.

### Phase B — Crawl URLs and extract numbers

1. Process candidate URLs **sequentially**, one active URL at a time.
2. Use Firecrawl to crawl each candidate URL.
3. Extract Iranian mobile numbers.
4. Persist newly extracted numbers immediately, rather than waiting for the entire Batch to finish.
5. Emit SSE events for newly extracted results as soon as they are durably stored.
6. Once a URL has been fully processed, mark it completed and move to the next URL automatically.
7. Once every URL in the Batch is completed or permanently failed:
   - Mark the Batch completed.
   - Persist its final counters and timestamps.
   - Release all unnecessary in-memory references.
   - Clear the active in-memory Batch buffers.
   - Keep durable history and search cursors in SQLite.
   - Start the next URL collection phase using the saved query/page cursors.

Do not repeat already consumed SearXNG pages or already completed URLs.

If all search queries are genuinely exhausted and there are no remaining URLs, mark the job completed.

The job must not terminate merely because one URL failed.

## 3. Remove approval-gated execution

The current flow has a major architectural problem:

`url_completed -> awaiting_approval -> wait for Info decision`

This must no longer control execution.

New jobs must advance automatically:

`searching -> batch_ready -> crawling -> batch_completed -> searching`

Do not wait for a Boolean `true` after each URL.

The Info Extractor integration will be updated separately later. Do not modify the Info Extractor repository or introduce dependencies on changes there.

### Backward compatibility is essential

The currently deployed Info Extractor may still send requests to:

`POST /v1/jobs/{job_id}/decisions/{decision_id}`

Do not break the existing integration while this separate update is pending.

Temporarily retain the endpoint and the `decision_id` / `decision_url` fields in `url_completed` events:

- A valid JSON `true` must be accepted as a backward-compatible no-op. It must not pause or advance the worker because the worker already advances automatically.
- A valid JSON `false` must request the same graceful stop as `POST /v1/jobs/{job_id}/stop`.
- Repeated identical decisions must remain idempotent.
- Unknown or invalid decision IDs must not be allowed to stop unrelated jobs.
- Do not create new `awaiting_approval` states for new jobs.
- Document the decision endpoint as deprecated compatibility functionality.

This allows the existing Info Extractor to continue functioning until its own update is made.

## 4. Durable database checkpoints and Batch history

Every Stage/Batch must have enough durable information in SQLite to resume after:

- Normal completion of a Batch.
- Application restart.
- Unexpected process termination.
- OOM kill.
- Temporary SearXNG failure.
- Temporary Firecrawl failure.
- Explicit stop and later administrative recovery, where applicable.

At minimum, persist the following information for each Batch:

- Batch number.
- Batch size.
- Current phase.
- Batch status.
- Search start and completion timestamps.
- Number of URLs collected.
- Number of URLs crawled.
- Number of URLs failed.
- Current crawl position.
- Search query/page cursor information.
- Error and retry information.
- Batch completion timestamp.

For every URL in a Batch, persist:

- Normalized URL.
- Batch number and position.
- Processing status: pending, processing, completed, failed, or interrupted.
- Attempt count.
- Last error.
- Processing timestamps.
- Firecrawl crawl job ID, once created.
- Firecrawl pagination cursor or next-page reference.
- Number of pages already processed.
- Number of results extracted.

The current implementation stores much of its state as JSON in the `jobs` table. Review whether this remains suitable for growing jobs.

Prefer a normalized SQLite representation for new Batch and URL checkpoint data where that materially improves durability, lookup performance, and memory usage. Avoid repeatedly serializing enormous growing arrays for every small state update.

However, do not create two conflicting sources of truth. Keep transactions atomic and make the migration strategy explicit.

Do not store entire raw HTML pages in SQLite. Persist the information needed to resume and the extracted result data.

Use database transactions so that a checkpoint and its associated SSE event cannot become inconsistent.

## 5. Firecrawl must be resumable

This is one of the most important changes.

Currently, `firecrawl_crawl()` keeps the Firecrawl job ID only in a local Python variable. A Business Discovery restart therefore loses the reference to an active remote crawl.

Change this behavior:

1. As soon as Firecrawl creates a crawl job, persist its remote job ID against the current URL.
2. On application restart, check whether the current URL already has an active Firecrawl job ID.
3. If it does, resume polling that existing Firecrawl job instead of creating another crawl.
4. Persist the Firecrawl pagination cursor as pages are consumed.
5. Process Firecrawl result pages incrementally. Do not accumulate the entire crawl response in a large in-memory list.
6. After processing and saving a page, checkpoint the next pagination cursor.
7. If the process crashes after a page was fetched but before its checkpoint was committed, fetching that page again must be safe and must not produce duplicate stored results or duplicate logical events.
8. If an existing remote crawl cannot be resumed, cancel it when possible and safely retry the same URL.
9. Do not restart the entire Batch because one Firecrawl job failed.

Where Firecrawl supports cancellation, use its cancellation endpoint when the user requests a stop or when an active crawl must be abandoned safely.

Never leave untracked Firecrawl jobs running intentionally.

## 6. Stream extracted numbers incrementally

At the moment, `process_site()` waits for the complete Firecrawl crawl and only then returns all extracted pages and numbers.

Change the processing flow so that each completed Firecrawl result page can be processed independently.

For each newly extracted, normalized number:

1. Persist it immediately.
2. Emit an ordered durable SSE event, such as `number_found`.
3. Include the number, source URL, website URL, page title, and page description.
4. Ensure event persistence and result persistence are atomic.

Keep the existing `url_completed` event for compatibility. It must still contain the aggregate results discovered for that URL, including numbers already emitted through incremental events.

Keep the existing result field names and shape compatible with the current Info Extractor contract:

- `number`
- `source_url`
- `website_url`
- `page_title`
- `page_description`

The Info Extractor currently consumes results from `url_completed`; do not require it to consume the new incremental event yet.

Preserve SSE event IDs and `Last-Event-ID` replay behavior. Reconnecting must not lose committed results.

Keep the current number normalization and deduplication behavior unless a change is strictly necessary. Info Extractor will perform its own global deduplication as well.

## 7. Stop behavior

The existing stop endpoint must remain:

`POST /v1/jobs/{job_id}/stop`

It must work during all phases:

- URL collection.
- Waiting for search retry.
- Crawling an active URL.
- Between URLs.
- Between Batches.
- Memory-pressure pause.

When stop is requested:

1. Persist `stop_requested` immediately.
2. Do not start another URL or another Batch.
3. Cancel an active Firecrawl crawl if supported and appropriate.
4. Preserve all results and checkpoints already committed.
5. Do not mark unfinished URLs as successfully completed.
6. Finish the job with status `stopped`.
7. Emit a durable `job_stopped` event.

A stop request must wake a worker waiting on a retry or memory-pressure pause.

Use graceful cancellation. Do not let cancellation of one job accidentally terminate the entire FastAPI application.

There may be one active URL when the stop request arrives. Handle that situation safely and document the maximum possible additional work if an external request cannot be cancelled immediately.

## 8. Memory-aware execution

The VPS has approximately 7.8 GiB RAM. The desired application-stack operating budget is approximately 7 GiB, leaving a safety reserve.

The application must be conservative with memory.

Required behavior:

- One active Firecrawl URL at a time globally, including across multiple jobs.
- Do not create unbounded asyncio tasks.
- Do not accumulate complete Batch crawl responses in RAM.
- Do not retain raw HTML after its page has been processed.
- Close HTTP clients and release references when a phase or URL finishes.
- Keep search response handling bounded by the configured Batch size.
- Keep `MAX_CRAWL_PAGES` and `MAX_CRAWL_DEPTH` bounded.
- Do not increase concurrency to improve throughput without an explicit memory justification.

Make `MAX_CONCURRENT_SITES` default to 1 for the new execution path. Ensure the limit applies globally across jobs, not independently to each job.

### Memory pressure must pause, not fail the job

Implement a configurable memory guard.

On Linux:

- Prefer cgroup v2 memory information when available.
- Also inspect host memory availability from `/proc/meminfo` where possible.
- Handle environments where cgroup information is unavailable.
- Do not assume the application process can see or control every other container's memory.

Suggested configuration:

- `HOST_MEMORY_BUDGET_MB=7168`
- `MEMORY_AVAILABLE_FLOOR_MB=1024`
- `MEMORY_CHECK_INTERVAL_SECONDS=15`

When the configured memory safety threshold is exceeded:

1. Persist the current phase and checkpoint.
2. Set the current phase to a visible state such as `paused_memory`.
3. Do not start new search requests or new Firecrawl jobs.
4. Wait and periodically check memory availability.
5. Wake immediately if a stop request arrives.
6. When memory is sufficiently available again, restore the exact saved phase and continue.
7. Do not mark the job failed, completed, or stopped merely because memory is temporarily low.

Use separate pause and resume thresholds if needed to prevent rapid toggling.

If the process is killed by the operating system before it can pause, the existing durable checkpoints must allow it to resume after systemd restarts the application.

Important: application-level monitoring is a soft safety mechanism, not a hard guarantee that the entire VPS will never exceed 7 GiB. SearXNG, Firecrawl, RabbitMQ, PostgreSQL, Redis, Evolution API, and other services run outside this Python process. Do not falsely claim that Python code alone can enforce a host-wide memory ceiling.

If the repository does not contain the deployment definitions for those external services, document the additional Docker/cgroup/systemd memory limits required to enforce a true host-wide budget. Do not modify unrelated live services or invent their resource allocations.

Also, do not assume that `gc.collect()` guarantees that the operating system immediately receives all memory back. The primary goal is bounded working memory and process isolation where useful, not merely calling garbage collection.

## 9. Recovery and migration

Introduce a new job model version for the refactored execution flow.

Existing jobs must not be silently lost.

In particular:

- Preserve existing completed job results and event history.
- Preserve existing search query cursors.
- Preserve pending URL queues.
- Preserve completed URL statuses.
- Recover interrupted URLs safely.
- Resume an active Firecrawl job using its persisted remote ID when possible.
- Do not restart completed Batches.
- Do not repeat committed results.
- Do not restart search from page 1.
- Keep legacy job support where necessary.

For existing version-4 jobs that are currently in `awaiting_approval`:

- Treat the URL as already completed.
- Automatically advance the Batch position as though the old consumer had approved continuation.
- If the Batch was completed, transition to the next search phase.
- Clear the obsolete pending approval state.
- Preserve compatibility decision IDs so that a delayed old Info Extractor request does not break the job.
- Do not accidentally process the already completed URL again.

Make all database migrations transactional and test them against representative old job states.

A temporary provider failure or memory pause must never permanently cancel a job.

## 10. Search continuity and configuration

Make the configuration consistent:

- `SEARCH_BATCH_SIZE` default: 100.
- `.env.example`, application defaults, README, and diagnostics must agree.
- `MAX_CONCURRENT_SITES` default: 1 for the new execution path.
- Keep `MAX_SEARCH_PAGES=0` meaning unlimited pagination.
- Keep current per-query cursor and retry behavior.
- Keep the current SearXNG search strategy and URL validation policy unless a clear bug requires changing them.

After a Batch is crawled, the next search phase must continue from the saved cursor and query index. It must not restart the original search.

## 11. API and status observability

Update the job status response to expose useful progress information:

- Job status.
- Current phase.
- Current Batch number.
- Batch size.
- URLs collected in the current Batch.
- URLs processed in the current Batch.
- Failed URLs.
- Current URL.
- Search query index and next page.
- Memory pause state and reason.
- Last checkpoint timestamp.
- Retry status.
- Total results discovered.

Add durable events for meaningful transitions, including:

- `batch_started`
- `search_progress`
- `batch_ready`
- `crawl_started`
- `number_found`
- `url_completed`
- `batch_completed`
- `memory_paused`
- `memory_resumed`
- `job_resumed`
- `job_stopped`
- `job_completed`
- `job_failed`

Avoid excessive high-frequency events.

Keep the existing SSE contract backward-compatible wherever possible.

## 12. Tests

Update and extend the test suite. Do not simply delete tests that currently expect approval behavior; replace them with tests for the new automatic flow and keep compatibility tests for the old decision endpoint.

At minimum, test:

1. A 100-URL Batch is fully collected before crawling starts.
2. Crawling starts only after URL collection is complete.
3. URLs are crawled sequentially, with only one active Firecrawl job globally.
4. Completion of one URL automatically advances to the next URL without waiting for `true`.
5. Completion of a Batch starts the next search phase.
6. The next search phase continues from the saved query/page cursor.
7. URLs left over from a SearXNG response are preserved for the next Batch.
8. A smaller final Batch is crawled when search is exhausted.
9. Results are persisted and streamed incrementally.
10. `url_completed` still contains aggregate results for compatibility.
11. An old `true` decision is accepted without blocking execution.
12. An old `false` decision stops the job.
13. Stop works during search, active crawling, and memory pause.
14. A restart during search resumes from the last committed search checkpoint.
15. A restart during crawling resumes the persisted Firecrawl job rather than creating a duplicate when possible.
16. A restart after a URL is completed does not crawl it again.
17. A crash during Firecrawl pagination does not lose committed results.
18. Temporary SearXNG/Firecrawl errors retry without failing the whole job.
19. Memory pressure pauses and later resumes the same job.
20. A memory pause is not treated as a terminal job status.
21. Existing v4 jobs migrate correctly, including jobs waiting for approval.
22. Existing SSE replay and idempotency behavior still work.

Use mocks/fakes for SearXNG, Firecrawl, memory readings, and process interruptions. The normal unit test suite must not require live external services.

Run the full test suite and fix failures. Also add tests for database migration and restart recovery.

## 13. Documentation

Update README and `.env.example` to reflect the actual implementation.

Document:

- The two-phase Batch architecture.
- Automatic progression between URLs.
- The deprecated compatibility decision endpoint.
- The stop endpoint.
- The durable checkpoint model.
- Firecrawl job recovery.
- Memory-pressure pause/resume behavior.
- What the application can and cannot guarantee about total VPS RAM.
- Which external service memory limits must be configured outside this repository for a hard 7 GiB host-wide ceiling.

Update `doc/INFO_EXTRACTOR_INTEGRATION.md` carefully:

- Do not require an Info Extractor code change in this task.
- Explain that old Boolean decisions are temporarily accepted for compatibility.
- Explain that the future integration should use the stop endpoint instead of sending `true` after every URL.

## 14. Constraints

- Do not modify the Info Extractor repository.
- Do not change the business-search query strategy without necessity.
- Do not remove existing useful diagnostics.
- Do not make `count` a local hard limit; Info Extractor remains responsible for the global target across all sources.
- Do not make the job terminal merely because a provider is temporarily unavailable.
- Do not restart SearXNG or Firecrawl containers after every Batch.
- Do not store raw HTML or entire crawl responses in the database.
- Do not claim that garbage collection alone releases all RAM.
- Do not break the existing API consumer.
- Do not make destructive SQLite migrations.
- Do not silently discard existing jobs or results.

## 15. Completion report

After implementation, provide:

1. A concise explanation of the new architecture.
2. Files changed.
3. Database schema/migration changes.
4. How recovery works during search and Firecrawl crawling.
5. How memory pause/resume works.
6. Any host-level memory limitations that still require deployment changes.
7. Test commands executed and their results.
8. Any remaining limitations.

Implement the changes directly in this repository. Do not stop after producing a design proposal.