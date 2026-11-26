from __future__ import annotations

import asyncio
import html
import ipaddress
import json
import logging
import os
import re
import secrets
import time
from contextlib import asynccontextmanager
from math import ceil
from urllib.parse import parse_qsl, unquote, urlencode, urljoin, urlsplit, urlunsplit

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, field_validator

from .job_manager import IdempotencyConflict, JobManager
from .job_store import JobStore, TERMINAL

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("business_discovery")

SEARXNG_URL = os.getenv("SEARXNG_URL", "http://127.0.0.1:8889")
FIRECRAWL_URL = os.getenv("FIRECRAWL_URL", "http://127.0.0.1:3002")
API_KEY = os.getenv("API_KEY", "")
SEARCH_TIMEOUT = float(os.getenv("SEARCH_TIMEOUT", "20"))
CRAWL_TIMEOUT = float(os.getenv("CRAWL_TIMEOUT", "240"))
MAX_CRAWL_PAGES = int(os.getenv("MAX_CRAWL_PAGES", "10"))
MAX_CRAWL_DEPTH = int(os.getenv("MAX_CRAWL_DEPTH", "2"))
MAX_CONCURRENT_SITES = int(os.getenv("MAX_CONCURRENT_SITES", "3"))
MAX_SEARCH_ROUNDS = 3
SEARCH_URL_FACTOR = 1.6
MAX_SEARCH_PAGES = int(os.getenv("MAX_SEARCH_PAGES", "50"))
JOB_DB_PATH = os.getenv("JOB_DB_PATH", "/opt/business-discovery/data/jobs.sqlite3")
JOB_TTL_SECONDS = int(os.getenv("JOB_TTL_SECONDS", str(7 * 24 * 60 * 60)))
JOB_CLEANUP_INTERVAL = int(os.getenv("JOB_CLEANUP_INTERVAL", "3600"))

MOBILE_RE = re.compile(
    r"(?<!\d)(?:(?:\+?98|0098)[\s().-]*)?0?9\d{2}[\s().-]*\d{3}[\s().-]*\d{4}(?!\d)"
)
DIGITS = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")
CONTACT_HINTS = ("contact", "about", "تماس", "درباره", "ارتباط")


class DiscoverRequest(BaseModel):
    keyword: str = Field(min_length=1, max_length=120)
    province: str = Field(min_length=1, max_length=80, description="Province or location, e.g. اصفهان or ایران")
    count: int = Field(ge=1, le=100)

    @field_validator("keyword", "province")
    @classmethod
    def clean_text(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be blank")
        return value


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not API_KEY or len(API_KEY) < 32:
        raise RuntimeError("API_KEY must be configured as a random secret of at least 32 characters")
    store = JobStore(JOB_DB_PATH, retention_seconds=JOB_TTL_SECONDS)
    manager = JobManager(
        store=store,
        search_fn=search_sites,
        query_fn=search_query_for_round,
        process_fn=process_site_in_background,
        normalize_phone_fn=normalize_mobile,
        max_concurrent_sites=MAX_CONCURRENT_SITES,
        max_rounds=MAX_SEARCH_ROUNDS,
        url_factor=SEARCH_URL_FACTOR,
        max_search_pages=MAX_SEARCH_PAGES,
        cleanup_interval=JOB_CLEANUP_INTERVAL,
    )
    await manager.start()
    app.state.job_manager = manager
    try:
        yield
    finally:
        await manager.stop()


app = FastAPI(
    title="Iran Business Contact Discovery",
    version="2.0.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


async def require_key(authorization: str | None = Header(default=None)) -> None:
    supplied = authorization.removeprefix("Bearer ") if authorization else ""
    if not secrets.compare_digest(supplied, API_KEY):
        raise HTTPException(status_code=401, detail="Invalid or missing bearer token", headers={"WWW-Authenticate": "Bearer"})


@app.get("/health")
async def health():
    return {"status": "ok", "service": "business-discovery"}


def normalize_url(value: str) -> str | None:
    try:
        p = urlsplit(value.strip())
        if p.scheme not in {"http", "https"} or not p.hostname or p.username or p.password:
            return None
        host = p.hostname.lower().rstrip(".")
        try:
            ip = ipaddress.ip_address(host)
            if not ip.is_global:
                return None
        except ValueError:
            if "." not in host or host in {"localhost", "example.com"}:
                return None
        port = p.port
        netloc = host[4:] if host.startswith("www.") else host
        if port and not (p.scheme == "http" and port == 80 or p.scheme == "https" and port == 443):
            netloc += f":{port}"
        path = re.sub(r"/{2,}", "/", p.path or "/").rstrip("/") or "/"
        query = urlencode(sorted((k, v) for k, v in parse_qsl(p.query) if not k.lower().startswith("utm_") and k.lower() not in {"fbclid", "gclid"}))
        return urlunsplit((p.scheme, netloc, path, query, ""))
    except (ValueError, UnicodeError):
        return None


def domain_of(url: str) -> str:
    return (urlsplit(url).hostname or "").lower().removeprefix("www.")


def homepage_url(url: str) -> str:
    parsed = urlsplit(url)
    return urlunsplit((parsed.scheme, parsed.netloc, "/", "", ""))


def search_query_for_round(keyword: str, province: str, round_number: int) -> str:
    query = f"{keyword} {province} ایران"
    if round_number == 2:
        query += " شماره تماس"
    elif round_number == 3:
        query += " سایت رسمی تماس"
    return query


async def search_sites(
    keyword: str,
    province: str,
    needed: int,
    excluded_domains: set[str] | None = None,
    round_number: int = 1,
    start_page: int = 1,
    initial_sites: list[dict] | None = None,
    page_callback=None,
    max_pages: int | None = None,
) -> list[dict]:
    excluded = excluded_domains or set()
    page_limit = max_pages if max_pages is not None else MAX_SEARCH_PAGES
    query = search_query_for_round(keyword, province, round_number)
    sites: dict[str, dict] = {}
    for site in initial_sites or []:
        domain = domain_of(site.get("url", ""))
        if domain and domain not in excluded:
            sites.setdefault(domain, site)

    if len(sites) >= needed:
        return list(sites.values())[:needed]

    async with httpx.AsyncClient(timeout=SEARCH_TIMEOUT, follow_redirects=True) as client:
        for page_no in range(start_page, page_limit + 1):
            try:
                response = await client.get(
                    f"{SEARXNG_URL.rstrip('/')}/search",
                    params={"q": query, "format": "json", "language": "fa-IR", "pageno": page_no},
                )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict) or not isinstance(payload.get("results"), list):
                    raise ValueError("invalid SearXNG JSON response structure")
                items = payload["results"]
            except (httpx.HTTPError, ValueError, TypeError) as exc:
                raise RuntimeError(f"SearXNG request failed: {type(exc).__name__}") from exc

            new_sites: list[dict] = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                normalized = normalize_url(str(item.get("url", "")))
                if not normalized:
                    continue
                domain = domain_of(normalized)
                if domain in excluded or domain in sites:
                    continue
                site = {
                    "url": normalized,
                    "website_url": homepage_url(normalized),
                    "title": str(item.get("title", ""))[:300],
                }
                sites[domain] = site
                new_sites.append(site)

            exhausted = len(items) == 0
            if page_callback:
                await page_callback(page_no + 1, new_sites, exhausted)
            if len(sites) >= needed or exhausted:
                break
    return list(sites.values())[:needed]


def normalize_mobile(raw: str) -> str | None:
    digits = re.sub(r"\D", "", raw.translate(DIGITS))
    if digits.startswith("0098"):
        digits = digits[4:]
    elif digits.startswith("98"):
        digits = digits[2:]
    if len(digits) == 10 and digits.startswith("9"):
        digits = "0" + digits
    return digits if re.fullmatch(r"09\d{9}", digits) else None


def extract_mobile_numbers(content: str) -> list[str]:
    text = html.unescape(content).translate(DIGITS)
    found = {normalized for match in MOBILE_RE.finditer(text) if (normalized := normalize_mobile(match.group()))}
    return sorted(found)


async def firecrawl_crawl(client: httpx.AsyncClient, site: dict) -> tuple[list[dict], str | None]:
    job_id = None
    try:
        created = await client.post(
            f"{FIRECRAWL_URL.rstrip('/')}/v2/crawl",
            json={
                "url": site["url"],
                "limit": MAX_CRAWL_PAGES,
                "maxDiscoveryDepth": MAX_CRAWL_DEPTH,
                "scrapeOptions": {"formats": ["markdown", "rawHtml"], "onlyMainContent": False, "timeout": 45000},
            },
            timeout=45,
        )
        created.raise_for_status()
        job_id = created.json().get("id")
        if not job_id:
            return [], "Firecrawl did not return a crawl job id"
        end = time.monotonic() + CRAWL_TIMEOUT
        pages: list[dict] = []
        while time.monotonic() < end:
            response = await client.get(f"{FIRECRAWL_URL.rstrip('/')}/v2/crawl/{job_id}", timeout=30)
            response.raise_for_status()
            payload = response.json()
            pages = list(payload.get("data") or pages)
            status = payload.get("status")
            if status == "completed":
                next_url = payload.get("next")
                while next_url:
                    parsed = urlsplit(next_url)
                    page_response = await client.get(
                        FIRECRAWL_URL.rstrip("/") + parsed.path + ("?" + parsed.query if parsed.query else ""),
                        timeout=30,
                    )
                    page_response.raise_for_status()
                    page_data = page_response.json()
                    pages.extend(page_data.get("data") or [])
                    next_url = page_data.get("next")
                return pages, None
            if status in {"failed", "cancelled"}:
                return pages, f"Firecrawl job {status}"
            await asyncio.sleep(3)
        try:
            await client.delete(f"{FIRECRAWL_URL.rstrip('/')}/v2/crawl/{job_id}", timeout=10)
        except httpx.HTTPError:
            log.warning("Could not cancel timed-out Firecrawl crawl")
        return pages, "Firecrawl crawl timed out"
    except asyncio.CancelledError:
        if job_id:
            try:
                await client.delete(f"{FIRECRAWL_URL.rstrip('/')}/v2/crawl/{job_id}", timeout=10)
            except httpx.HTTPError:
                log.warning("Could not cancel Firecrawl crawl after worker cancellation")
        raise
    except (httpx.HTTPError, ValueError, TypeError) as exc:
        return [], f"Firecrawl request failed: {type(exc).__name__}"


def page_source(page: dict, fallback: str) -> str:
    metadata = page.get("metadata") or {}
    return normalize_url(str(metadata.get("sourceURL") or metadata.get("url") or page.get("url") or fallback)) or fallback


def page_priority(page: dict, fallback: str) -> tuple[int, int]:
    path = unquote(urlsplit(page_source(page, fallback)).path).strip("/").lower()
    if not path:
        return (0, 0)
    if any(hint in path for hint in CONTACT_HINTS):
        return (1, len(path))
    return (2, len(path))


async def process_site(client: httpx.AsyncClient, site: dict, semaphore: asyncio.Semaphore) -> dict:
    async with semaphore:
        pages, error = await firecrawl_crawl(client, site)
    if error and not pages:
        log.warning("Skipping failed website %s (%s)", site["website_url"], error)
        return {"website_url": site["website_url"], "phones": {}, "error": error}

    pages.sort(key=lambda page: page_priority(page, site["url"]))
    phones: dict[str, str] = {}
    for page in pages:
        source = page_source(page, site["url"])
        if domain_of(source) != domain_of(site["url"]):
            continue
        content = "\n".join(str(page.get(key) or "") for key in ("markdown", "html", "rawHtml"))
        for number in extract_mobile_numbers(content):
            phones.setdefault(number, source)

    if not phones:
        candidates: list[str] = []
        for page in pages:
            source = page_source(page, site["url"])
            raw = "\n".join(str(page.get(key) or "") for key in ("html", "rawHtml"))
            for _, href in re.findall(r'href\s*=\s*(["\'])(.*?)\1', raw, flags=re.I):
                link = normalize_url(urljoin(source, html.unescape(href)))
                if link and domain_of(link) == domain_of(site["url"]):
                    path = unquote(urlsplit(link).path).lower()
                    if any(hint in path for hint in CONTACT_HINTS) and link not in candidates:
                        candidates.append(link)
        async with httpx.AsyncClient(timeout=45, follow_redirects=False) as scrape_client:
            for target in candidates[:3]:
                try:
                    response = await scrape_client.post(
                        f"{FIRECRAWL_URL.rstrip('/')}/v2/scrape",
                        json={"url": target, "formats": ["markdown", "rawHtml"], "onlyMainContent": False, "timeout": 35000},
                    )
                    response.raise_for_status()
                    data = response.json().get("data") or {}
                    content = "\n".join(str(data.get(key) or "") for key in ("markdown", "html", "rawHtml"))
                    metadata = data.get("metadata") or {}
                    source = normalize_url(str(metadata.get("sourceURL") or metadata.get("url") or target)) or target
                    if domain_of(source) == domain_of(site["url"]):
                        for number in extract_mobile_numbers(content):
                            phones.setdefault(number, source)
                except (httpx.HTTPError, ValueError) as exc:
                    log.warning("Contact page scrape failed (%s)", type(exc).__name__)

    if error:
        log.warning("Partial crawl for %s (%s)", site["website_url"], error)
    return {"website_url": site["website_url"], "phones": phones, "error": error}


async def process_site_in_background(site: dict, semaphore: asyncio.Semaphore) -> dict:
    async with httpx.AsyncClient(timeout=httpx.Timeout(60, connect=10), follow_redirects=False) as client:
        return await process_site(client, site, semaphore)


async def run_discovery(keyword: str, province: str, target_count: int, search_fn=None, process_fn=None) -> dict:
    """Synchronous core retained for regression tests and non-API callers."""
    search_fn = search_fn or search_sites
    process_fn = process_fn or process_site
    processed_domains: set[str] = set()
    registered_numbers: set[str] = set()
    results: list[dict] = []
    warnings: list[str] = []
    completed_rounds = 0

    for round_number in range(1, MAX_SEARCH_ROUNDS + 1):
        remaining = target_count - len(results)
        if remaining <= 0:
            break
        url_target = ceil(remaining * SEARCH_URL_FACTOR)
        try:
            sites = await search_fn(keyword, province, url_target, excluded_domains=processed_domains, round_number=round_number)
        except RuntimeError as exc:
            if completed_rounds == 0:
                raise
            warning = f"SearXNG failed in round {round_number}: {exc}"
            warnings.append(warning)
            log.error("%s; returning results from completed rounds", warning)
            break
        completed_rounds = round_number
        unique_sites = []
        for site in sites:
            normalized = normalize_url(str(site.get("url", "")))
            if not normalized:
                continue
            domain = domain_of(normalized)
            if domain in processed_domains:
                continue
            processed_domains.add(domain)
            unique_sites.append({**site, "url": normalized, "website_url": homepage_url(normalized)})
        if not unique_sites:
            continue
        async with httpx.AsyncClient(timeout=httpx.Timeout(60, connect=10), follow_redirects=False) as client:
            semaphore = asyncio.Semaphore(MAX_CONCURRENT_SITES)
            for offset in range(0, len(unique_sites), MAX_CONCURRENT_SITES):
                batch = unique_sites[offset : offset + MAX_CONCURRENT_SITES]
                batch_results = await asyncio.gather(*(process_fn(client, site, semaphore) for site in batch))
                for site_result in batch_results:
                    if site_result.get("error"):
                        log.warning("Website processing issue: %s", site_result["error"])
                    phones = site_result.get("phones") or {}
                    new_numbers = [number for number in phones if number not in registered_numbers]
                    if not new_numbers:
                        continue
                    website_result = {"website_url": site_result["website_url"], "mobile_numbers": []}
                    for number in new_numbers:
                        registered_numbers.add(number)
                        website_result["mobile_numbers"].append({"number": number, "source_url": phones[number]})
                    results.append(website_result)
                    if len(results) >= target_count:
                        break
                if len(results) >= target_count:
                    break
        if len(results) >= target_count:
            break
    response = {
        "requested_count": target_count,
        "successful_websites": len(results),
        "total_unique_mobile_numbers": len(registered_numbers),
        "completed_rounds": completed_rounds,
        "results": results,
    }
    if warnings:
        response["warnings"] = warnings
    return response


def format_sse(event: dict) -> str:
    data = json.dumps(event["data"], ensure_ascii=False, separators=(",", ":"))
    return f"id: {event['id']}\nevent: {event['event']}\ndata: {data}\n\n"


def get_job_manager(request: Request) -> JobManager:
    return request.app.state.job_manager


@app.post("/v1/discover", status_code=202, dependencies=[Depends(require_key)])
async def create_discovery_job(
    body: DiscoverRequest,
    request: Request,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", max_length=200),
):
    manager = get_job_manager(request)
    try:
        job, created = manager.submit(body.keyword, body.province, body.count, idempotency_key=idempotency_key)
    except IdempotencyConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    job.update({
        "created": created,
        "status_url": f"/v1/jobs/{job['job_id']}",
        "events_url": f"/v1/jobs/{job['job_id']}/events",
        "result_url": f"/v1/jobs/{job['job_id']}/result",
    })
    return job


@app.get("/v1/jobs/{job_id}/events", dependencies=[Depends(require_key)])
async def stream_job_events(
    job_id: str,
    request: Request,
    manager: JobManager = Depends(get_job_manager),
    last_event_id_query: int | None = Query(default=None, alias="last_event_id", ge=0),
):
    record = manager.store.get(job_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Job not found or expired")
    last_header = request.headers.get("last-event-id")
    if last_event_id_query is not None:
        cursor = last_event_id_query
    elif last_header is not None:
        try:
            cursor = int(last_header)
            if cursor < 0:
                raise ValueError
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Last-Event-ID must be a non-negative integer") from exc
    else:
        cursor = 0

    async def event_stream():
        nonlocal cursor
        heartbeat_at = time.monotonic()
        while True:
            if await request.is_disconnected():
                return
            events = manager.store.events_after(job_id, cursor)
            if events:
                for event in events:
                    cursor = event["id"]
                    yield format_sse(event)
                continue
            current = manager.store.get(job_id)
            if current is None:
                return
            if current["status"] in TERMINAL and cursor >= current["last_event_id"]:
                return
            if time.monotonic() - heartbeat_at >= 15:
                heartbeat_at = time.monotonic()
                yield ": heartbeat\n\n"
            await asyncio.sleep(0.5)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/v1/jobs/{job_id}/result", dependencies=[Depends(require_key)])
async def get_job_result(job_id: str, request: Request):
    manager = get_job_manager(request)
    result = manager.final_result(job_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Job not found or expired")
    if result["status"] not in TERMINAL:
        raise HTTPException(status_code=409, detail={"status": result["status"], "status_url": f"/v1/jobs/{job_id}"})
    return result


@app.get("/v1/jobs/{job_id}", dependencies=[Depends(require_key)])
async def get_job_status(job_id: str, request: Request):
    status = get_job_manager(request).status(job_id)
    if status is None:
        raise HTTPException(status_code=404, detail="Job not found or expired")
    return status
