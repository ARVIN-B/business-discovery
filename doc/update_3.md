# Task: Redesign Business Discovery Around Individual Result URLs and Unique Mobile Records

Repository:
https://github.com/ARVIN-B/business-discovery

Work directly in the existing repository. Inspect the current implementation, implement the changes, update the API contract and documentation, and add comprehensive tests.

This is an important API contract update. The resulting API will later be consumed by another Django application, so the final output structure must be consistent, unambiguous, and suitable for incremental SSE consumption.

Do not modify the separate `info_extractor` repository.

---

# 1. Main objective

Change the discovery model from:

**One result per successful website/domain**

to:

**One result per newly discovered unique Iranian mobile number, with the exact URL of the page where that number was found.**

This distinction is essential for marketplace and classified-ad platforms.

Examples:
- Divar may contain millions of individual listings under `divar.ir`.
- Torob may contain many product or seller pages under `torob.com`.
- Other marketplaces and directories may expose many businesses under the same domain.

These are independent result pages, not one single website result.

The application must treat each distinct page URL as an independent discovery candidate.

Do not deduplicate search candidates by domain.

Do not assume that one domain represents one business.

Do not hardcode special behavior for Divar or Torob. Implement a general URL-level discovery model that works for all websites and marketplaces.

# 2. URL-level discovery instead of domain-level discovery

Inspect and update:

- `app/main.py`
- `app/job_manager.py`
- `app/job_store.py`, if needed
- `tests/test_discovery.py`
- `tests/test_jobs.py`
- `test_service.py`
- `README.md`

Currently, `search_sites()` normalizes search result URLs but stores/deduplicates them using their domain. The job manager also maintains `processed_domains` and excludes previously processed domains.

This must change.

## Required behavior

A candidate must be identified by its complete normalized URL, not its domain.

For example, all of these must be considered independent candidates:

```text
https://divar.ir/v/listing-1
https://divar.ir/v/listing-2
https://divar.ir/v/listing-3
https://divar.ir/v/listing-4
```

Likewise:

```text
https://torob.com/p/123
https://torob.com/p/456
https://torob.com/shop/789
```

All of them must be eligible for processing even though they share a hostname.

Implement the following:

1. Deduplicate candidates by their complete normalized URL.
2. Track processed URLs rather than processed domains.
3. Exclude previously processed URLs from subsequent search rounds, not all URLs belonging to their domains.
4. Preserve URL paths and meaningful query parameters, especially listing IDs and product IDs.
5. Remove tracking parameters only when safe, such as UTM parameters and known advertising click identifiers.
6. Do not replace a listing URL with the homepage URL.
7. Keep the root website URL as separate metadata when useful.

`www.example.com` and `example.com` may be normalized consistently, but distinct listing paths must never collapse into one result.

The SearXNG pagination callback must also deduplicate by complete normalized URL, allowing multiple candidates from the same domain.

Update all relevant method arguments and internal state from domain-based exclusions to URL-based exclusions.

# 3. Crawl and extract individual page results

The current `process_site()` implementation crawls a starting URL through Firecrawl and collects phone numbers in a dictionary.

Update its data model so that every extracted mobile number preserves the exact page from which it was extracted.

A single crawl may discover several pages. Each page must be considered independently for result attribution.

For every crawled page:

1. Resolve its actual source URL from Firecrawl metadata, such as `metadata.sourceURL` or `metadata.url`, or the page's own URL.
2. Normalize that URL without removing meaningful path or query information.
3. Extract valid Iranian mobile numbers from that page's content.
4. Associate every number with that exact page URL.
5. Capture the page title when available.
6. Capture a real page description when available.
7. Preserve the root website URL separately.

Do not use the domain homepage as the source URL when the number was found on a listing, product, or business page.

For example, if a number was found on:

```text
https://divar.ir/v/real-listing-identifier
```

the result must preserve that complete URL, not:

```text
https://divar.ir/
```

If Firecrawl returns multiple pages with different mobile numbers, each number must retain the URL of its own page.

If one page contains multiple different valid mobile numbers, create one individual result record for each number. All those records may share the same `source_url`.

Do not invent business names, addresses, descriptions, or other metadata.

Use actual extracted page metadata only. If a title or description is unavailable, return an empty value or omit the optional field.

Keep the existing safe URL validation and same-site crawl restrictions unless a concrete requirement makes a change necessary.

# 4. New result data contract

This is the most important API change.

The final `results` array must be FLAT.

Do not group multiple numbers inside a `mobile_numbers` array under one website.

Each result must represent exactly one unique mobile number.

Required record structure:

```json
{
  "number": "09123456789",
  "source_url": "https://divar.ir/v/real-listing-identifier",
  "website_url": "https://divar.ir/",
  "page_title": "عنوان واقعی آگهی یا صفحه",
  "page_description": ""
}
```

Field definitions:

- `number`: normalized Iranian mobile number in `09xxxxxxxxx` format.
- `source_url`: complete absolute URL of the actual page where this number was found. This is the primary clickable URL.
- `website_url`: root website URL, useful as secondary metadata.
- `page_title`: actual page or listing title when available.
- `page_description`: actual description when available.

`source_url` must never be replaced with `website_url` just because the latter is easier to obtain.

If a page has two new valid numbers, output two records with the same `source_url`.

Example final response:

```json
{
  "job_id": "example-job-id",
  "status": "completed",
  "schema_version": 2,
  "requested_count": 5,
  "successful_results": 5,
  "total_unique_mobile_numbers": 5,
  "processed_page_count": 12,
  "successful_page_count": 4,
  "completed_rounds": 2,
  "results": [
    {
      "number": "09123456789",
      "source_url": "https://divar.ir/v/listing-1",
      "website_url": "https://divar.ir/",
      "page_title": "فروش گوشی موبایل",
      "page_description": ""
    },
    {
      "number": "09351234567",
      "source_url": "https://divar.ir/v/listing-2",
      "website_url": "https://divar.ir/",
      "page_title": "فروش موبایل در اصفهان",
      "page_description": ""
    },
    {
      "number": "09111111111",
      "source_url": "https://example.com/contact",
      "website_url": "https://example.com/",
      "page_title": "صفحه تماس",
      "page_description": ""
    }
  ],
  "errors": [],
  "warnings": []
}
```

The example is illustrative. Do not hardcode its values.

Update the create response, status response, final-result response, SSE payloads, tests, and documentation consistently.

Add `schema_version: 2` to the public job/result responses.

Remove the misleading website-based success metric from the new contract. Use `successful_results` for the number of unique mobile records returned.

`total_unique_mobile_numbers` must equal `successful_results`.

Keep page-processing metrics separate:
- `processed_page_count`
- `successful_page_count`
- `discovered_page_count`

A page counts as successful if it contributes at least one previously unregistered mobile number. These page metrics must never be confused with the requested result count.

# 5. Counting and deduplication rules

The request field `count` must now mean:

**The target number of unique Iranian mobile numbers.**

It no longer means the number of successful websites.

Rules:

1. Every newly discovered valid mobile number counts as one result.
2. A phone number must be globally unique within the entire job.
3. If the same number appears on multiple URLs, count it only once.
4. Keep the first valid source URL associated with that number.
5. If one page contains multiple new mobile numbers, each number is an independent result.
6. If a page contains only numbers already registered in the job, it contributes zero new results.
7. The job must stop collecting new numbers as soon as `requested_count` is reached.
8. Do not return more results than requested, even when one page contains multiple phone numbers or multiple concurrent page-processing tasks finish close together.

Example:

Requested count: 3.

Page A:
- 09111111111
- 09222222222

Page B:
- 09333333333

Page C:
- 09111111111
- 09444444444

Expected final result:
- 09111111111 from Page A
- 09222222222 from Page A
- 09333333333 from Page B

The job must stop at exactly three unique mobile numbers. The duplicate on Page C must not count, and the fourth number must not be added after the target is reached.

Use deterministic ordering when processing phone candidates.

Keep the existing Iranian mobile normalization and validation behavior.

# 6. Preserve incremental SSE delivery

The service must continue publishing results immediately as individual page-processing tasks finish.

Do not wait for all websites, all URLs, all batches, or the entire search job before emitting newly discovered numbers.

Introduce a clear event contract for individual results.

## New event: `result_found`

Emit one `result_found` event for each newly registered unique mobile number.

Example:

```text
id: 25
event: result_found
data: {"result":{"number":"09123456789","source_url":"https://divar.ir/v/listing-1","website_url":"https://divar.ir/","page_title":"فروش گوشی موبایل","page_description":""},"successful_results":1,"total_unique_mobile_numbers":1,"requested_count":5}

```

This event must be persisted in the SQLite event store and replayable after reconnecting with `Last-Event-ID`.

The result must be emitted only once per unique mobile number within the job.

## Page lifecycle events

Use URL/page-oriented lifecycle events rather than implying that every candidate is an entire website.

Prefer:

- `page_started`
- `page_completed`

A `page_completed` event should include:

```json
{
  "page_url": "https://divar.ir/v/listing-1",
  "website_url": "https://divar.ir/",
  "status": "completed",
  "new_results_count": 1,
  "processed_page_count": 1,
  "successful_results": 1,
  "requested_count": 5
}
```

A failed page must produce a page-level failure/warning without terminating unrelated page-processing tasks.

Keep job lifecycle events such as:

- `job_started`
- `progress`
- `round_started`
- `search_completed`
- `warning`
- `round_completed`
- `job_completed`
- `job_failed`

Use monotonically increasing event IDs and preserve the existing replay behavior.

The final `job_completed` event should include the updated counters and final result summary.

Do not emit duplicate `result_found` events when a page is reprocessed after a restart or SSE replay.

# 7. Job manager and round logic

Refactor the job manager to work with URL-level candidates and unique phone results.

Important current settings/behavior to preserve unless a change is strictly required:

- `SEARCH_URL_FACTOR = 1.6`
- `MAX_SEARCH_ROUNDS = 3`
- `MAX_SEARCH_PAGES`
- `MAX_CONCURRENT_SITES`
- Existing Firecrawl crawl page/depth/deadline limits
- Existing SearXNG query strategy
- Existing durable SQLite jobs and SSE event storage

The search target for each round must be derived from the remaining unique mobile-number target:

```python
round_url_target = ceil(remaining_unique_numbers * 1.6)
```

This target represents candidate page URLs, not domains.

Do not use `processed_domains` to exclude every result from an already processed marketplace.

Use a persisted set/list such as `processed_urls` instead.

Keep processing URLs from the same domain concurrently, subject to the existing concurrency semaphore.

When the requested unique mobile count is reached:
- Stop accepting additional new result records.
- Cancel unfinished page-processing tasks safely where possible.
- Preserve all already persisted results and events.
- Mark unprocessed pages as skipped with a clear reason.
- Complete the job successfully.

When all search rounds/pages are exhausted without reaching the requested count:
- Finish the job with `completed` status, not an artificial failure.
- Return the actual count and explain that the available search results were exhausted.
- Keep errors and warnings separate from normal search exhaustion.

If SearXNG fails, preserve already collected results and record the search error. Do not erase results already emitted or persisted.

A failure processing one page must not stop other candidate URLs from being processed.

Do not introduce artificial per-domain quotas or a one-result-per-domain rule.

# 8. Durable state and restart compatibility

The project persists job state and events in SQLite and recovers running jobs after service restarts.

Do not break this behavior.

Introduce a state/schema version for new jobs.

Consider existing queued or running jobs whose state uses:
- `processed_domains`
- `website_statuses` keyed by homepage
- nested website `results`
- `successful_websites`

Handle these legacy states safely.

You may either:
1. Continue processing legacy jobs using their old state model until they finish, or
2. Implement an explicit, tested migration to the new URL/result model.

Do not silently discard a job, its collected numbers, or its SSE event history.

Do not accidentally treat old processed domain names as processed URL values.

Ensure a service restart during a URL-processing batch does not create duplicate mobile results or duplicate result events.

# 9. API consistency

Update all relevant public endpoints:

- `POST /v1/discover`
- `GET /v1/jobs/{job_id}`
- `GET /v1/jobs/{job_id}/events`
- `GET /v1/jobs/{job_id}/result`

The API should expose the same result record structure in:
- SSE `result_found` events
- Status response accumulated results
- Final result response

The result schema must not differ between streaming and final retrieval.

Keep Bearer authentication, idempotency handling, status codes, persistence, and event replay behavior intact.

# 10. Tests

Update the existing tests. Do not simply delete or weaken tests that assert the old behavior.

Add focused unit and integration tests for all of the following:

### URL discovery

1. Multiple URLs from the same domain are accepted as separate candidates.
2. Duplicate normalized URLs are deduplicated.
3. Tracking parameters are removed without losing listing IDs or meaningful query parameters.
4. Previously processed URLs are excluded.
5. Previously processed domains do not exclude new listing URLs from those domains.
6. SearXNG pagination can return many results from the same marketplace domain.

### Extraction and attribution

7. A Divar-like listing URL remains the exact `source_url` in the result.
8. The root website URL is stored separately.
9. Multiple crawled pages from one domain retain their individual source URLs.
10. Two different listing pages from the same domain can produce two separate result records.
11. Multiple mobile numbers from one page produce multiple flat records with the same `source_url`.
12. Page title and description are preserved when actual metadata is available.
13. Missing metadata does not cause invented business names or descriptions.
14. Invalid and landline numbers are still rejected.

### Counting and deduplication

15. Every new unique mobile number counts as one result.
16. Duplicate numbers across pages and domains count once.
17. A page containing multiple unique phones is counted by its unique phone records, not as one website.
18. The requested target is never exceeded.
19. Concurrent page completion cannot cause result-count overshoot.
20. The first valid source URL for a duplicate number is preserved.

### SSE

21. One `result_found` event is emitted for each newly accepted mobile number.
22. Each event contains the exact complete source URL.
23. Results are emitted before the whole job completes.
24. Replaying events with `Last-Event-ID` returns the correct remaining events in order.
25. Restart/recovery does not duplicate already committed result records or result events.
26. `page_started` and `page_completed` events have the correct URL-level identity.
27. Final result records match the records streamed through SSE.

### Regression

28. Existing SearXNG error behavior remains correct.
29. Existing Firecrawl error handling remains isolated to the affected page.
30. Existing idempotency and authentication behavior still works.
31. Existing job persistence and restart recovery still works.
32. The `1.6` URL factor, three-round strategy, and concurrency settings remain intact.

Use mocks and deterministic fixtures for offline tests. Tests must not depend on live Divar, Torob, SearXNG, or Firecrawl.

# 11. Live test and output verification

Update `test_service.py` to validate the new API contract.

It must verify:

- Job creation.
- Status and final-result endpoints.
- Flat result records.
- One valid normalized mobile per record.
- Absolute HTTP(S) `source_url` per record.
- `website_url` is separate from `source_url`.
- No duplicate mobile numbers.
- `successful_results == total_unique_mobile_numbers == len(results)`.
- `successful_results <= requested_count`.
- `result_found` SSE events reproduce the same final result records.
- SSE event IDs are ordered and unique.
- The final job event is `job_completed` for a successful job.

Allow the test keyword, province, count, and timeout to be configured through environment variables.

For live validation, print results in a human-readable format:

```text
[1] Mobile: 09123456789
    Page title: ...
    Source URL: https://divar.ir/v/real-listing-identifier
    Website: https://divar.ir/

[2] Mobile: 09351234567
    Page title: ...
    Source URL: https://example.com/business/123
    Website: https://example.com/
```

The output must make it obvious that the source URL is the exact clickable business/listing page, not merely the website homepage.

Do not assume every valid result URL must contain a path; a homepage can be a valid source when the number was actually found there. The deterministic fixture tests must specifically prove that marketplace listing paths are preserved.

# 12. Documentation and delivery

Update README to document:

- New per-URL discovery model.
- Global unique-mobile-number deduplication.
- New flat result schema.
- Exact meaning of `count`.
- `result_found` SSE event and payload.
- Difference between source page URL and root website URL.
- Page-level progress metrics.
- Search exhaustion behavior.
- Updated test commands.

At the end, report:

1. Files changed.
2. Exact new API response schema.
3. Exact SSE event schema.
4. Tests added and actual test results.
5. Any compatibility considerations for existing stored jobs.
6. Any remaining limitations.

Do not claim the integration is successful unless the tests actually pass.

Do not deploy to the production server or restart its services as part of this code change. Provide the required deployment/restart commands separately.