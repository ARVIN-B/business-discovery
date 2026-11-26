# Task: Redesign Business Discovery Job Execution — URL-Atomic Results, Limited & Unlimited Modes

Repository: https://github.com/ARVIN-B/business-discovery

You are working on an existing production-oriented FastAPI business discovery service. Do not treat this as a greenfield project. First inspect the entire relevant codebase, understand its current architecture, and then implement the changes carefully.

## 1. Primary Objective

Redesign job execution to support two modes:

1. **Limited mode:** The user specifies a target number of unique Iranian mobile numbers.
2. **Unlimited mode:** The user specifies only a keyword and province. Discovery continues until the user explicitly stops the job or the available search space is exhausted.

In both modes, results must be delivered incrementally, URL by URL, as soon as each URL has been completely processed.

## 2. Critical URL Processing Semantics

Each candidate URL represents an independent processing unit.

For example:
- `a.com/1`
- `a.com/2`
- `a.com/3`

These must be treated as three independent URLs, even though they belong to the same domain.

Do not deduplicate candidate pages merely by their root domain. Preserve meaningful paths, listing identifiers, and query parameters. Remove only safe tracking parameters.

For every candidate URL:

1. Emit `page_started` when processing begins.
2. Fully process that URL.
3. Extract and normalize all valid mobile numbers found on that URL.
4. Remove numbers already registered in the current job.
5. Persist all newly discovered unique numbers.
6. Emit the result events only after the URL has finished processing.
7. Emit `page_completed` with the final URL status and the number of newly discovered results.

A URL may contain multiple businesses and multiple phone numbers. Return all valid unique numbers discovered on that URL.

**Important:** Do not emit partial results from a URL while its extraction is still running. The atomic unit of result publication is one completed URL, not one individual phone number and not the entire job.

## 3. Limited Mode: URL-Atomic Target Behavior

The requested count is a target threshold, not a hard limit inside a URL.

Example:
- Requested count: 5
- URL A finishes with 3 new unique numbers.
- URL B finishes with 8 new unique numbers.

Expected output:
- Publish all 3 results from URL A.
- Publish all 8 results from URL B.
- Total results: 11.
- Do not start processing additional URLs after the target has been reached at a URL completion boundary.

Never truncate the results of a completed URL just because the requested target would be exceeded.

The stopping decision must be coordinated with concurrent workers. If multiple URLs are already in progress when the target is reached, allow those already-started URLs to finish and publish all their unique results. Do not start new URLs after the stop condition has been established.

Document this behavior clearly in the API contract.

## 4. Unlimited Mode

Support a genuine unlimited mode.

Requirements:
- The API must accept a job without a numeric result target.
- Use a clear, explicit representation for unlimited mode, such as `count: null`, if compatible with the existing API design.
- Do not silently convert unlimited mode into an arbitrary numeric target.
- Continue discovering and processing URLs until:
  - the user explicitly requests a stop, or
  - the search space is exhausted, or
  - a genuine unrecoverable failure occurs.
- Continue publishing results after each completed URL.
- Persist all unique results and job progress.

Do not confuse unlimited mode with a very large count.

## 5. Manual Stop and Cancellation

Add a reliable authenticated API operation to request job cancellation/stopping.

Requirements:
- Stop scheduling or starting new candidate URLs after the stop request is accepted.
- Allow already-started URLs to finish gracefully and publish their complete results.
- Persist the stop request so it survives process restarts.
- Expose a clear terminal status such as `cancelled` or `stopped`.
- Emit an appropriate durable SSE lifecycle event.
- Repeated stop requests must be idempotent.
- Do not lose results from URLs that finish during graceful shutdown.
- Do not falsely report a job as fully stopped while workers are still processing URLs.

Inspect the existing task lifecycle and recovery mechanism before designing this feature.

## 6. Concurrency and Data Integrity

This is a critical requirement.

The service may process multiple candidate URLs concurrently.

Implement correct synchronization around:
- job stop conditions,
- target threshold checks,
- result deduplication,
- result persistence,
- event creation,
- page status transitions.

Ensure that:
- Every mobile number is unique within a job.
- A number is registered and published only once.
- A completed URL publishes all its new unique numbers.
- Concurrent URLs cannot corrupt or overwrite one another's state.
- Reaching the limited-mode target prevents new URLs from starting, but does not truncate URLs already in progress.
- A process crash does not leave a result persisted without its corresponding durable event, or vice versa.

Use the existing persistence architecture where possible. If necessary, introduce transactional operations in the job store rather than relying on separate read-modify-write operations.

Do not solve concurrency issues with arbitrary delays or by disabling concurrency entirely.

## 7. SSE and Durable Event Contract

Preserve the existing durable SSE architecture, event IDs, replay support, and `Last-Event-ID` behavior.

Define and implement a consistent event lifecycle, including:

- `job_started`
- `round_started`
- `page_started`
- `page_completed`
- `result_found` (or a new atomic `url_results_found` event if more appropriate)
- `progress`
- `round_completed`
- `job_completed`
- `job_failed`
- `job_stopped` / `job_cancelled`

For URL-atomic publication, prefer one durable event containing the complete list of newly discovered results for a completed URL. If backward compatibility requires individual `result_found` events, ensure they are persisted atomically with the URL completion state and cannot expose a partially committed URL.

Every result event must contain enough information to associate results with the completed candidate URL.

SSE replay must reproduce exactly the same durable events in the same order, without gaps or duplicates.

## 8. Result Data Contract

Keep one flat result record per unique Iranian mobile number.

Each record must contain exactly:

- `number`
- `source_url`
- `website_url`
- `page_title`
- `page_description`

Definitions:
- `number`: normalized Iranian mobile number in the existing accepted format.
- `source_url`: exact page/listing URL from which the number was extracted.
- `website_url`: root origin of the source website.
- `page_title`: page title, or empty string if unavailable.
- `page_description`: page description, or empty string if unavailable.

Preserve the distinction between a listing/page URL and the root website URL.

Do not collapse different listing URLs into a single domain-level record.

## 9. API Compatibility and Schema

Inspect existing endpoints and schemas before modifying them.

Preserve:
- Authentication.
- Idempotency behavior.
- Existing job status and result endpoints.
- Existing SSE replay semantics.
- Existing pagination and search strategies.
- Existing URL discovery and crawling mechanisms.
- Existing retry and timeout behavior, unless a specific change is required.

Extend the API with the smallest reasonable set of changes.

The API must clearly distinguish:
- limited mode with a numeric target;
- unlimited mode with no numeric target.

Do not silently reinterpret old requests.

Update API documentation and README with examples for both modes and the stop operation.

## 10. Search and Crawl Strategy

Preserve the existing search and crawling strategy, including:
- Search pagination.
- Multiple discovery rounds.
- Existing URL expansion logic and URL factors.
- Existing crawl concurrency and retry policies.
- Existing Firecrawl integration.
- Existing timeout handling.

Do not introduce arbitrary per-domain quotas.

Do not stop search prematurely merely because one URL yielded several results. Apply the limited-mode stop condition only at the correct URL-completion boundary.

Do not change the overall discovery strategy unless required by the new execution semantics.

## 11. Persistence, Recovery, and Migration

Inspect the SQLite schema, job store, event store, and recovery logic.

Requirements:
- Existing jobs and events must remain readable.
- Do not delete or reset the database.
- Do not lose historical results.
- Support migration from existing job records.
- Distinguish old job semantics from new job semantics where necessary.
- Preserve idempotency mappings.
- Ensure stop requests and URL processing state survive restarts.
- Ensure unfinished URLs can be recovered safely without duplicate results or events.

Never rewrite historical requested counts based on the number of already-stored results unless the existing schema explicitly requires that behavior and the migration is demonstrably safe.

Do not execute destructive migrations or database resets.

## 12. Testing Requirements

Add comprehensive automated tests.

At minimum, test:

### Limited mode
- A URL returns fewer numbers than the target.
- The next URL exceeds the remaining target.
- All numbers from that completed URL are returned.
- No additional URL starts after the target boundary.
- Already-started concurrent URLs finish and publish their complete results.
- Duplicate numbers across URLs are published only once.

### Unlimited mode
- A job can be created without a numeric target.
- It processes multiple URLs and rounds.
- It continues beyond any arbitrary small number.
- It stops on explicit user cancellation.
- It finishes correctly when the search space is exhausted.

### URL semantics
- Different paths on the same domain are independent candidate URLs.
- Listing IDs and meaningful query parameters are preserved.
- Multiple numbers from one URL are returned together.
- `source_url` is the exact source page URL.
- `website_url` is the root origin.

### SSE and persistence
- URL result events are durable.
- Replay returns the exact original event sequence.
- `Last-Event-ID` resumes correctly.
- Restart recovery does not duplicate results or events.
- Result persistence and event persistence are atomic.
- Concurrent workers cannot exceed the URL-boundary target semantics.

### Stop behavior
- Repeated stop requests are safe.
- No new URLs start after stop acceptance.
- Already-started URLs finish gracefully.
- Final status is not reported before active URLs finish.

Run the full test suite and report the actual results. Do not claim tests passed unless they were executed.

## 13. Implementation Workflow

Follow this order:

1. Inspect the repository and map the current architecture.
2. Identify all relevant code paths:
   - API schemas and endpoints
   - job manager
   - job store
   - event persistence
   - search pagination
   - URL candidate normalization/deduplication
   - concurrent URL processing
   - cancellation and restart recovery
3. Explain the current behavior and identify the exact architectural changes required.
4. Implement the new limited/unlimited execution model.
5. Implement URL-atomic result publication and synchronization.
6. Implement durable manual stopping.
7. Add migrations safely.
8. Add and run automated tests.
9. Update API documentation and README.
10. Provide a concise implementation report.

## 14. Strict Constraints

- Do not reset, delete, or recreate the production database.
- Do not discard existing jobs or events.
- Do not replace the current architecture with a simplified MVP.
- Do not disable concurrency to avoid solving synchronization correctly.
- Do not introduce arbitrary result caps in unlimited mode.
- Do not introduce arbitrary domain quotas.
- Do not truncate results from a completed URL in limited mode.
- Do not publish incomplete URL results.
- Do not change unrelated components.
- Do not claim success without running tests.
- Do not deploy or restart production services automatically.

Before implementation, inspect the existing code and explain the proposed changes. Preserve all unrelated behavior.

The final implementation must be production-safe, restart-safe, concurrency-safe, and backward-compatible wherever reasonably possible.