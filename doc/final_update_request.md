# Final Production Update — Business Discovery API

Repository: `https://github.com/ARVIN-B/business-discovery`

The current implementation works well. Do not rewrite the existing discovery logic unnecessarily. This is the final production update before integrating the service with Chabokan.

## 1. First: Inspect the actual current state

Before changing anything:

- Inspect the current project on the VPS: `/opt/business-discovery`.
- Inspect Git status, current branch, latest commit and differences between the VPS and GitHub.
- Read the current `main.py`, tests, README, environment configuration and systemd service.
- Do not assume GitHub and the VPS contain identical code.
- Preserve all existing working functionality and the fixes from `doc/update_2.md`.

## 2. Incremental result delivery and progress tracking

Currently, the API waits for the entire discovery operation to finish before returning its JSON response. Change this behavior.

I want results to become available immediately after each website has been processed successfully. Do not wait for the entire batch, round or discovery operation to finish before making completed results available.

Implement a production-suitable job-based architecture:

- `POST /v1/discover` should create a discovery job and return a `job_id` immediately.
- Provide an SSE endpoint for receiving live events for that job.
- Provide a status endpoint so clients can retrieve the current job state, including after reconnecting.
- Provide a final-result endpoint to retrieve the completed output.
- The processing task must continue independently of the SSE client's connection.
- SSE disconnections must not cancel the discovery job.
- Support reconnecting and resuming event consumption without losing already generated results. Use event IDs or an equivalent reliable mechanism.
- Send periodic heartbeat events to keep long-lived connections alive.
- Ensure events are delivered in the correct order.

The application must track and expose at least:

- Job ID and current status.
- Current round (1, 2 or 3).
- Total number of rounds.
- Number of websites discovered for processing in the current round.
- Number of websites processed.
- Number of websites remaining to process in the current round.
- Number of successful websites.
- Total unique mobile numbers extracted.
- Current processing stage.
- Per-website processing status.
- Errors and warnings.
- Final completion status.

Send meaningful events such as:

- `job_started`
- `round_started`
- `search_completed`
- `website_started`
- `website_completed`
- `progress`
- `warning`
- `round_completed`
- `job_completed`
- `job_failed`

A `website_completed` event must include that website's result when it contributes at least one previously unregistered mobile number.

Do not send duplicate numbers in subsequent events. Keep global deduplication across all rounds and websites.

### Important architecture requirements

This must be reliable enough for production, not just an in-memory demonstration.

- Job state and accumulated results must survive SSE disconnections.
- Avoid losing jobs or results if the application process restarts.
- Use a persistent and concurrency-safe job/event storage approach.
- Do not introduce Redis, PostgreSQL, Celery or another external service without first checking the existing infrastructure and explaining why it is necessary.
- If a lightweight local persistence solution is sufficient for the current single-instance deployment, implement it safely and document its operational constraints.
- Prevent multiple workers from processing the same job.
- Add appropriate job expiration and cleanup policies.
- Handle unexpected exceptions and mark jobs as failed without silently losing their state.

Keep the implementation maintainable and avoid unnecessary dependencies.

## 3. Fix search pagination across rounds

Inspect the current SearXNG pagination implementation carefully.

Currently, processed domains are excluded, but `search_sites` starts from page 1 on every invocation and searches at most 8 pages. This means previously encountered results can be requested again from SearXNG.

Improve this behavior:

- Maintain pagination progress for each exact search query.
- When more results are needed for the same query, continue from the next unprocessed page instead of starting at page 1.
- Do not repeatedly request pages already consumed for that query.
- Maintain global domain deduplication across all rounds.
- Keep excluding previously processed domains from subsequent rounds.
- Since round 2 and round 3 may use different search queries, maintain independent pagination state for each query. Do not assume that page numbers from different queries are interchangeable.
- If a page contains too many duplicate or invalid domains, continue fetching subsequent pages until enough new unique domains are collected or the search limit is reached.
- Handle empty pages, provider errors and invalid responses correctly.
- Do not silently treat a SearXNG error as an empty search result.

Preserve the existing three-round strategy and search query variations.

## 4. Preserve the existing discovery logic

These rules must remain unchanged:

- Maximum 3 search rounds.
- Search target per round: `ceil(remaining_successful_websites * 1.6)`.
- The target count represents successful unique websites, not total phone numbers.
- A website is successful only if it contributes at least one previously unregistered mobile number.
- Globally deduplicate mobile numbers.
- Only valid Iranian mobile numbers are returned, normalized as `09xxxxxxxxx`.
- Do not return landlines.
- Preserve the source URL for every extracted number.
- Preserve the existing phone normalization fixes from `update_2.md`.
- Stop when the requested number of successful websites is reached or all search rounds are exhausted.
- Preserve existing Firecrawl crawl limits, concurrency settings and error handling unless a demonstrated issue requires a change.

Do not change the meaning of the existing API's `count` parameter.

## 5. Production deployment and external accessibility

The service currently listens on `127.0.0.1:8091` and is not publicly accessible.

Prepare it for secure external access so Chabokan can call it.

Requirements:

- Keep the application itself bound to `127.0.0.1`.
- Expose it through a dedicated Nginx reverse-proxy configuration.
- Support HTTPS.
- Configure Nginx correctly for SSE: disable response buffering for the SSE endpoint, use suitable proxy read/send timeouts and ensure streaming events are flushed to clients.
- Preserve the existing Bearer authentication.
- Add reasonable request-size limits, connection handling and rate limiting where appropriate.
- Do not expose internal SearXNG or Firecrawl ports.
- Keep the API key out of source code, logs and responses.
- Ensure health checks and API documentation are appropriately handled.
- Update the systemd service and application configuration only when necessary.

### Critical infrastructure restrictions

This VPS also hosts other services.

Do not modify, restart, rebuild, delete or reconfigure:

- Firecrawl
- SearXNG
- Docker or existing Docker Compose stacks
- Evolution API
- Existing databases or Redis instances
- Existing Nginx configurations unrelated to this application

Do not overwrite existing Nginx configuration files. Add a separate configuration and validate it before applying.

Do not change firewall rules or expose ports directly without explaining the change and obtaining approval.

A valid DNS hostname is required for production HTTPS. Inspect the existing configuration first. If no suitable domain is available, do not invent one or configure an insecure public HTTP endpoint as the final solution. Report exactly what domain/DNS information is needed.

## 6. Testing

Add comprehensive tests for the new implementation.

At minimum, test:

- Job creation and status transitions.
- Incremental result availability.
- SSE event ordering and event IDs.
- SSE reconnection and event replay.
- Job processing continuing after an SSE client disconnects.
- Persistent state across application restart.
- Concurrent requests and duplicate job prevention.
- Correct progress counters.
- Per-website result events.
- Global phone-number deduplication.
- Pagination continuation for the same query.
- Independent pagination state for different round queries.
- Exclusion of previously processed domains.
- SearXNG failures and empty pages.
- Firecrawl failures and timeouts.
- Job completion and failure states.
- Existing phone normalization and extraction behavior.

Run all unit tests and the live integration tests using the real SearXNG and Firecrawl services.

Do not claim success for a test that was not actually executed.

## 7. Documentation and deployment

Update `README.md` with:

- New API workflow.
- All new endpoints.
- Request and response examples.
- SSE event examples.
- How to reconnect to an existing job.
- Job status definitions.
- Error handling.
- Chabokan integration instructions.
- Environment variables.
- Deployment and restart instructions.
- Test commands.

Create a detailed update report at:

`doc/final_update.md`

The report must explicitly list:

- Every changed file.
- Every updated directory.
- Architectural changes.
- New endpoints.
- Pagination changes.
- Test results, including actual executed commands.
- Deployment changes.
- Any remaining external requirements or blockers.

## 8. Deployment safety

Before deployment:

1. Back up all files that will be modified.
2. Run tests.
3. Validate the application configuration.
4. Validate the Nginx configuration before reloading it.
5. Deploy only the business-discovery application changes.
6. Verify systemd status and application health.
7. Verify the new endpoints.
8. Verify SSE streaming through the reverse proxy, not just locally.
9. Verify that Firecrawl, SearXNG and other existing services remain healthy.

Do not perform destructive cleanup or broad system changes.

Do not declare the project production-ready until the external HTTPS endpoint and SSE behavior have actually been tested.

**Final objective:** A reliable production API that starts discovery immediately, publishes each completed website result as soon as it is available, exposes accurate progress and job status, avoids repeated SearXNG pagination, and can be securely consumed by Chabokan.