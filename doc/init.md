# Task: Build a Lightweight Business Contact Discovery API on the Germany VPS

You are working as a senior Python backend and infrastructure engineer.

I have two environments:
1. A Germany VPS that hosts Firecrawl and SearXNG.
2. A Chabokan server that currently hosts my existing Django project, `info_extractor`.

My goal is to build a lightweight, independent service on the Germany VPS. Later, I will connect my Chabokan Django application to this service through a secure HTTP API.

Your job is to inspect the existing infrastructure, understand how both environments are connected, implement the new service on the Germany VPS, and provide a standalone SSH test script.

## 1. Germany VPS — Existing Infrastructure

The Germany VPS is already accessible through my current VS Code Remote SSH connection.

Known details:

- Operating System: Ubuntu 26.04.1 LTS
- Public IP: `195.248.240.152`
- CPU: 4 vCPU
- RAM: 7.8 GB
- Available disk: approximately 56 GB
- Firecrawl source directory: `/opt/firecrawl`
- Firecrawl version: `v2.11.809`
- Firecrawl commit: `41b36dfd28ad69df7da221d1d51f3d5fb4838be3`

### Existing Firecrawl

Firecrawl is self-hosted using Docker Compose.

API:
- Internal base URL: `http://127.0.0.1:3002`
- Readiness endpoint: `/v0/health/readiness`
- Docker service: `api`
- Docker network: `firecrawl_backend`
- Authentication currently disabled: `USE_DB_AUTHENTICATION=false`

Important:
- Firecrawl is already working.
- Do not reinstall it.
- Do not rebuild it.
- Do not change its source code.
- Do not expose its API directly to the public internet.
- Do not change its authentication settings without explicit approval.
- Do not modify its existing Docker networks, volumes, or database configuration.

### Existing SearXNG

SearXNG is also installed on the Germany VPS.

Known details:

- Docker container: `info-extractor-searxng`
- Source/configuration directory: `/opt/searxng`
- Internal URL: `http://127.0.0.1:8889/`
- Public URL: `https://195.248.240.152:8888/`
- Docker network: `searxng_default`
- JSON search API is currently working internally.

Use the following exact SearXNG configuration values:

```env
SEARXNG_SECRET=3de0b143e9d265f4f21f71391b9bfd550c1150fe47f235bcca15030331f3b14b
SEARXNG_BASE_URL=https://195.248.240.152:8888/
```

These values must not be regenerated or arbitrarily changed.

Important networking detail:
- Internal SearXNG JSON requests to `http://127.0.0.1:8889/search?format=json` returned HTTP 200.
- Requests through the public Nginx endpoint on port 8888 returned HTTP 403 during the last inspection.
- Investigate the existing Nginx routing and SearXNG configuration.
- Do not disable security restrictions or modify Nginx just to make the public endpoint work.
- Prefer internal SearXNG communication for the new service when possible.
- Preserve the supplied configuration values and document the actual endpoint used by the service.

### Existing extraction script

There is an existing script:

`/opt/firecrawl/extract_mobile_numbers.py`

It already contains:
- Firecrawl API communication
- Persian and Arabic digit normalization
- Iranian mobile number normalization
- URL normalization
- Crawl page collection
- Contact-page fallback scraping
- Phone extraction
- JSON output

Inspect and reuse appropriate logic where practical.

Do not blindly copy the entire script. Refactor only if necessary, and preserve the existing working file.

## 2. Chabokan Server — Existing Application

The Chabokan environment currently runs:

- Python 3.12.12
- Django 5.2.19
- Gunicorn 23.0.0
- Uvicorn 0.54.0
- Celery 5.7.0
- Redis Python client 8.1.0
- HTTPX 0.28.1

Existing project directory:

`/app`

Existing Django project includes:
- `manage.py`
- `config`
- `leads`
- `tests`
- `templates`
- `static`
- `.env`

It already has configuration variables for:
- SearXNG
- Firecrawl
- Celery
- Redis
- Web search and crawling
- Phone extraction

The Chabokan application is a separate environment. Do not assume its filesystem, localhost, Docker network, or private IP is directly accessible from the Germany VPS.

Do not modify the Chabokan project during this task.

The purpose of this stage is to build a standalone service on the Germany VPS and prepare the integration contract for Chabokan.

## 3. What I Want to Build

Create a lightweight Python API on the Germany VPS.

Its purpose is to discover Iranian businesses and extract their contact information.

The API should eventually accept a request similar to:

```json
{
  "keyword": "رستوران",
  "province": "اصفهان",
  "count": 100
}
```

The processing logic:

1. Search using SearXNG.
2. Collect at least 10% more unique URLs than the requested count.
3. Normalize URLs and eliminate duplicates.
4. Identify unique business websites.
5. Submit the websites to the existing Firecrawl service.
6. Prioritize these pages:
   - Homepage
   - Header links
   - Footer links
   - about-us
   - about_us
   - contact-us
   - contact_us
   - درباره ما
   - درباره-ما
   - تماس با ما
   - تماس-با-ما
   - Other equivalent Persian and English URL variations.
7. If contact numbers are not found, perform a limited crawl of other pages.
8. Extract and normalize Iranian mobile and landline numbers.
9. Associate each number with its source URL and business website.
10. Return structured JSON results to the requesting server.

No LLM is required.

Initial crawl limits should be conservative and configurable. Do not crawl entire websites without limits.

Do not assume every discovered website represents a unique business. Deduplicate domains and results carefully.

## 4. Architecture Requirements

Build this as an independent service.

Preferred characteristics:
- Python
- Lightweight HTTP API, preferably FastAPI if appropriate
- HTTPX for external API communication
- Clear separation between search, URL normalization, crawling, extraction, and API layers
- Configurable timeouts, concurrency, retries, and crawl limits
- Structured logging
- Input validation
- Error handling
- API authentication for incoming requests
- No unnecessary database or infrastructure dependencies

Do not add PostgreSQL, Redis, Celery, or another database unless there is a demonstrated need.

Do not introduce a complex architecture for this relatively small service.

The service must not interfere with existing Firecrawl, SearXNG, Evolution API, or other running containers.

## 5. Network Connectivity

The new service will run on the same Germany VPS as Firecrawl and SearXNG.

Investigate the actual networking topology before choosing connection methods.

Important:
- `127.0.0.1` inside a Docker container is not the host's localhost.
- The Firecrawl API is currently published on host loopback port 3002.
- SearXNG is currently published on host loopback port 8889.
- The new service must be able to reach both services reliably.
- If container networking is preferable, inspect existing Docker networks and choose a safe approach.
- Do not expose internal databases, Redis, RabbitMQ, or Firecrawl publicly.

The new service should expose only its own authenticated API.

For eventual Chabokan integration, prepare a secure public HTTPS endpoint. Do not expose an unauthenticated endpoint.

Do not assume a domain name or invent one. If a domain is required, tell me exactly what information is needed.

## 6. Deployment and Safety Rules

Before changing anything:

1. Inspect the current server state.
2. Identify existing services, ports, networks, and directories.
3. Choose an isolated directory for the new application.
4. Back up any existing file before modifying it.
5. Keep all existing services operational.

Strict restrictions:
- Do not delete existing data.
- Do not run `docker system prune`.
- Do not delete or recreate existing Docker volumes.
- Do not reinstall Firecrawl or SearXNG.
- Do not modify the existing Firecrawl source.
- Do not overwrite the existing extraction script.
- Do not print secret values in logs or reports.
- Do not expose Firecrawl directly to the public internet.
- Do not change existing Nginx configuration without first explaining why and obtaining approval.
- Do not make assumptions about the Chabokan server's SSH access.

Use restart-safe deployment commands and log files. Avoid long-running foreground commands that could be interrupted by SSH disconnection.

## 7. Required Deliverables

At the end of this task, I expect:

### A. Running service
A working API on the Germany VPS.

### B. Configuration documentation
Document:
- Application directory
- Configuration file location
- Environment variables
- Internal SearXNG endpoint actually used
- Internal Firecrawl endpoint actually used
- Service port
- Service process management
- Log locations

### C. Chabokan integration contract

Tell me exactly what I will need on Chabokan in the next stage:

- Public HTTPS API URL
- Authentication method
- API key or token
- HTTP method and endpoint
- Request JSON schema
- Response JSON schema
- Timeout expectations
- Whether requests are synchronous or asynchronous
- How to check job status if asynchronous
- Any required CA certificate or network configuration

Do not invent a public domain or API key. Generate credentials securely if needed and show me the generated credential only once, with appropriate file permissions.

### D. SSH test script — mandatory

Create a standalone Python test script that I can run from the Germany VPS through SSH.

For example:

`/opt/business-discovery/test_service.py`

The script must:
1. Check service health.
2. Submit a realistic test request.
3. Verify the HTTP response.
4. Validate the JSON structure.
5. Display the number of discovered URLs.
6. Display the number of processed websites.
7. Display extracted business names and phone numbers.
8. Display the source page for each extracted number.
9. Clearly report failures and timeouts.
10. Exit with a non-zero status if the test fails.

Provide the exact command to run it.

The test must use the real SearXNG and Firecrawl services, not mocked responses.

## 8. Work Procedure

Work carefully and verify each stage.

Do not merely produce a theoretical plan. Implement the service and test it.

However:
- If a critical requirement cannot be met safely, stop and explain the blocker.
- Do not silently change existing infrastructure.
- Do not claim success without actual test results.
- Do not proceed with public exposure before authentication and HTTPS are correctly configured.

At completion, give me a concise report containing:
1. What was implemented.
2. Which services are running.
3. Exact internal and public endpoints.
4. How Chabokan will connect in the next stage.
5. Exact SSH test command.
6. Any remaining limitations.

The immediate goal is to finish the Germany VPS service only. Chabokan integration will be the next separate stage.