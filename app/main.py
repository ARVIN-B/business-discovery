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
from email.utils import parsedate_to_datetime
from contextlib import asynccontextmanager
from math import ceil
from urllib.parse import parse_qsl, unquote, urlencode, urljoin, urlsplit, urlunsplit

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, field_validator

from .job_manager import IdempotencyConflict, JobManager
from .job_store import DecisionConflict, JobStore, TERMINAL

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
SEARCH_URL_FACTOR = 1.6
MAX_SEARCH_ROUNDS = 3  # Retained only by the legacy synchronous helper.
SEARCH_BATCH_SIZE = max(1, int(os.getenv("SEARCH_BATCH_SIZE", "1000")))
SEARCH_RETRY_SECONDS = max(1, int(os.getenv("SEARCH_RETRY_SECONDS", "60")))
MAX_SEARCH_PAGES = int(os.getenv("MAX_SEARCH_PAGES", "0"))
SEARXNG_LANGUAGE = os.getenv("SEARXNG_LANGUAGE", "en-US")
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
    count: int | None = Field(default=None, ge=1, le=100, description="Unique mobile number target; omit or null for unlimited discovery")

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
        max_rounds=3,
        url_factor=SEARCH_URL_FACTOR,
        max_search_pages=MAX_SEARCH_PAGES,
        search_batch_size=SEARCH_BATCH_SIZE,
        search_retry_seconds=SEARCH_RETRY_SECONDS,
        search_queries_fn=search_queries_for_job,
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
    version="5.0.0",
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
        tracking_keys = {"fbclid", "gclid", "dclid", "gbraid", "wbraid", "msclkid", "yclid"}
        query = urlencode([
            (key, item) for key, item in parse_qsl(p.query, keep_blank_values=True)
            if not key.lower().startswith("utm_") and key.lower() not in tracking_keys
        ])
        return urlunsplit((p.scheme, netloc, path, query, ""))
    except (ValueError, UnicodeError):
        return None


def normalize_search_candidate(value: object, *, location_evidence: str = "", search_text: str = "") -> tuple[str | None, str | None]:
    """Accept Iranian domains, or other domains whose indexed text names the requested location."""
    raw = str(value or "").strip()
    if not raw:
        return None, "missing_url"
    try:
        parsed = urlsplit(raw)
        if parsed.scheme.lower() not in {"http", "https"}:
            return None, "unsupported_scheme"
        if not parsed.hostname:
            return None, "missing_hostname"
        if parsed.username or parsed.password:
            return None, "embedded_credentials"
        try:
            ip = ipaddress.ip_address(parsed.hostname)
            if not ip.is_global:
                return None, "non_public_ip"
        except ValueError:
            host = parsed.hostname.lower().rstrip(".")
            if "." not in host or host in {"localhost", "example.com"}:
                return None, "host_policy"
            if not host.removeprefix("www.").endswith(".ir"):
                key = lambda text: " ".join(str(text).casefold().replace("ي", "ی").replace("ك", "ک").replace("\u200c", " ").split())
                location_key = key(location_evidence)
                indexed_text = key(search_text)[:3000]
                has_persian_text = any("\u0600" <= char <= "\u06ff" for char in indexed_text)
                if not location_key or location_key not in indexed_text or not has_persian_text:
                    return None, "non_iranian_domain"
        else:
            return None, "non_iranian_domain"
        if parsed.port is not None and not (1 <= parsed.port <= 65535):
            return None, "invalid_port"
    except (ValueError, UnicodeError):
        return None, "malformed_url"
    normalized = normalize_url(raw)
    if not normalized:
        return None, "normalization_policy"
    return normalized, None


def domain_of(url: str) -> str:
    return (urlsplit(url).hostname or "").lower().removeprefix("www.")


def homepage_url(url: str) -> str:
    parsed = urlsplit(url)
    return urlunsplit((parsed.scheme, parsed.netloc, "/", "", ""))


def search_query_for_round(keyword: str, province: str, round_number: int) -> str:
    strategies = search_queries_for_job(keyword, province)
    return strategies[(round_number - 1) % len(strategies)]


def search_queries_for_job(keyword: str, province: str) -> list[str]:
    normalized_keyword = " ".join(keyword.casefold().split())
    synonym_groups = {
        "موبایل": ["تلفن همراه", "گوشی موبایل", "فروشگاه گوشی", "لوازم جانبی موبایل", "پاساژ موبایل", "mobile phone", "cell phone"],
        "تلفن همراه": ["موبایل", "گوشی", "فروشگاه موبایل", "لوازم جانبی موبایل", "mobile phone", "cell phone"],
        "رستوران": ["غذاخوری", "فست فود", "کافه رستوران", "restaurant", "food restaurant", "dining"],
        "کلینیک": ["درمانگاه", "مرکز درمانی", "مطب", "clinic", "medical center", "healthcare"],
        "پزشک": ["دکتر", "مطب", "کلینیک", "doctor", "physician", "medical clinic"],
        "فروشگاه": ["مرکز خرید", "مغازه", "فروش", "shop", "store", "retail"],
        "خدمات": ["مرکز خدمات", "شرکت خدماتی", "service center", "services", "provider"],
        "mobile": ["mobile phone", "cell phone", "موبایل", "گوشی موبایل", "تلفن همراه", "mobile shop"],
        "mobile phone": ["cell phone", "موبایل", "گوشی موبایل", "تلفن همراه", "mobile store"],
        "restaurant": ["food restaurant", "dining", "رستوران", "غذاخوری", "فست فود", "کافه رستوران"],
        "clinic": ["medical clinic", "medical center", "کلینیک", "درمانگاه", "مرکز درمانی"],
    }
    topic_terms = [keyword.strip(), *synonym_groups.get(normalized_keyword, [])]
    locations = [province.strip(), f"استان {province.strip()}", f"{province.strip()} ایران"]
    local_intents = ["فروشگاه", "مرکز", "تماس با ما", "آدرس", "وب‌سایت", "خرید", "نمایندگی", "شماره تماس"]
    english_intents = ["store", "shop", "business", "contact", "address", "official website", "Iran", "phone"]
    candidates = []
    for location in locations:
        candidates.extend(f"{topic} {location}" for topic in topic_terms)
    for topic in topic_terms[:6]:
        candidates.extend(f"{topic} {province.strip()} {intent} ایران" for intent in local_intents if intent)
    candidates.extend(f"{keyword.strip()} {province.strip()} {intent}" for intent in english_intents)
    candidates.extend(f"{keyword.strip()} {province.strip()} {intent}" for intent in ("ایران", "کسب و کار", "شرکت", "خدمات"))
    english_topics = [topic for topic in topic_terms if any("a" <= char.casefold() <= "z" for char in topic)]
    candidates.extend(f"{topic} {province.strip()} {intent}" for topic in english_topics for intent in english_intents)
    candidates.extend((
        f'"{keyword.strip()}" {province.strip()} Iran',
        f'{keyword.strip()} {province.strip()} Iran contact',
        f'{keyword.strip()} {province.strip()} website',
    ))

    queries = []
    seen = set()
    for query in candidates:
        query = " ".join(query.split()).strip()
        if not query:
            continue
        query_variants = [f"{query} site:.ir"]
        if any("\u0600" <= char <= "\u06ff" for char in query):
            query_variants.append(query)
        for variant in query_variants:
            key = variant.casefold().replace("ي", "ی").replace("ك", "ک")
            if key not in seen:
                queries.append(variant)
                seen.add(key)
    return queries


class SearchProviderError(RuntimeError):
    def __init__(self, message: str, retry_after: float | None = None, *, http_status: int | None = None,
                 diagnostics: dict | None = None):
        super().__init__(message)
        self.retry_after = retry_after
        self.http_status = http_status
        self.diagnostics = diagnostics or {}


async def search_sites(
    keyword: str,
    province: str,
    needed: int | None,
    excluded_urls: set[str] | None = None,
    round_number: int = 1,
    start_page: int = 1,
    initial_sites: list[dict] | None = None,
    page_callback=None,
    max_pages: int | None = None,
    language: str | None = None,
) -> list[dict]:
    excluded = {normalized for url in (excluded_urls or set()) if (normalized := normalize_url(url))}
    page_limit = (None if max_pages == 0 else max_pages) if max_pages is not None else (MAX_SEARCH_PAGES or None)
    query = search_query_for_round(keyword, province, round_number)
    request_language = language or SEARXNG_LANGUAGE
    sites: dict[str, dict] = {}
    for site in initial_sites or []:
        normalized = normalize_url(str(site.get("url", "")))
        if normalized and normalized not in excluded:
            sites.setdefault(normalized, {**site, "url": normalized, "website_url": homepage_url(normalized)})

    if needed is not None and len(sites) >= needed:
        return list(sites.values())[:needed]

    async with httpx.AsyncClient(timeout=SEARCH_TIMEOUT, follow_redirects=True) as client:
        page_no = start_page
        previous_fingerprint = None
        while page_limit is None or page_no <= page_limit:
            requested_at = time.time()
            response = None
            try:
                response = await client.get(
                    f"{SEARXNG_URL.rstrip('/')}/search",
                    params={"q": query, "format": "json", "language": request_language, "pageno": page_no},
                )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict) or not isinstance(payload.get("results"), list):
                    raise ValueError("invalid SearXNG JSON response structure")
                items = payload["results"]
            except (httpx.HTTPError, ValueError, TypeError) as exc:
                retry_after = None
                response = getattr(exc, "response", None)
                if response is not None:
                    try:
                        retry_after = max(0.0, float(response.headers.get("Retry-After", "")))
                    except (TypeError, ValueError):
                        try:
                            retry_after = max(0.0, parsedate_to_datetime(response.headers.get("Retry-After", "")).timestamp() - time.time())
                        except (TypeError, ValueError, OverflowError):
                            pass
                http_status = getattr(response, "status_code", None)
                raise SearchProviderError(f"SearXNG request failed: {type(exc).__name__}", retry_after,
                    http_status=http_status, diagnostics={"requested_at": requested_at, "page": page_no,
                    "language": request_language, "error_type": type(exc).__name__}) from exc

            raw_url_count = 0
            valid_url_count = 0
            rejected: dict[str, int] = {}
            duplicate_count = 0

            new_sites: list[dict] = []
            for item in items:
                if not isinstance(item, dict):
                    rejected["invalid_result_shape"] = rejected.get("invalid_result_shape", 0) + 1
                    continue
                raw_value = item.get("url")
                if raw_value:
                    raw_url_count += 1
                indexed_text = " ".join(str(item.get(field, "")) for field in ("title", "content", "description"))
                normalized, reason = normalize_search_candidate(
                    raw_value, location_evidence=province, search_text=indexed_text,
                )
                if not normalized:
                    rejected[reason or "invalid_url"] = rejected.get(reason or "invalid_url", 0) + 1
                    continue
                valid_url_count += 1
                if normalized in excluded or normalized in sites:
                    duplicate_count += 1
                    continue
                site = {
                    "url": normalized,
                    "website_url": homepage_url(normalized),
                    "search_title": str(item.get("title", ""))[:300],
                }
                sites[normalized] = site
                new_sites.append(site)

            fingerprint = tuple(str(item.get("url", "")) for item in items if isinstance(item, dict))
            repeated_page = bool(fingerprint) and fingerprint == previous_fingerprint
            previous_fingerprint = fingerprint if fingerprint else None
            exhausted = len(items) == 0 or repeated_page
            unresponsive = payload.get("unresponsive_engines", [])
            if not items and unresponsive:
                detail = "; ".join(
                    f"{entry[0]}: {entry[1]}" for entry in unresponsive[:5]
                    if isinstance(entry, (list, tuple)) and len(entry) >= 2
                )
                raise SearchProviderError(
                    "SearXNG returned no results while engines were unresponsive" + (f" ({detail})" if detail else ""),
                    http_status=response.status_code,
                    diagnostics={"requested_at": requested_at, "page": page_no, "http_status": response.status_code,
                        "raw_result_count": 0, "unresponsive_engines": unresponsive[:10],
                        "language": request_language, "provider_failure": True},
                )
            diagnostics = {
                "requested_at": requested_at, "page": page_no, "http_status": response.status_code,
                "language": request_language, "raw_result_count": len(items), "results_with_url": raw_url_count,
                "valid_url_count": valid_url_count, "new_url_count": len(new_sites),
                "duplicate_url_count": duplicate_count, "rejected_url_count": sum(rejected.values()),
                "rejected_reasons": rejected, "unresponsive_engines": unresponsive[:10] if isinstance(unresponsive, list) else [],
                "next_page": page_no + 1, "exhausted": exhausted, "repeated_page": repeated_page,
            }
            if page_callback:
                keep_searching = await page_callback(page_no + 1, new_sites, exhausted, diagnostics)
                if keep_searching is False:
                    break
            if (needed is not None and len(sites) >= needed) or exhausted:
                break
            page_no += 1
    return list(sites.values()) if needed is None else list(sites.values())[:needed]


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
    raw_source = metadata.get("sourceURL") or metadata.get("url") or page.get("url") or fallback
    return normalize_url(str(raw_source)) or ""


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
        log.warning("Skipping failed page %s (%s)", site["url"], error)
        return {"page_url": site["url"], "pages": [], "error": error}

    pages.sort(key=lambda page: page_priority(page, site["url"]))
    extracted_pages: dict[str, dict] = {}
    for page in pages:
        source = page_source(page, site["url"])
        if not source or domain_of(source) != domain_of(site["url"]):
            continue
        content = "\n".join(str(page.get(key) or "") for key in ("markdown", "html", "rawHtml"))
        metadata = page.get("metadata") or {}
        title = metadata.get("title") or page.get("title") or ""
        description = metadata.get("description") or page.get("description") or ""
        entry = extracted_pages.setdefault(source, {
            "source_url": source,
            "website_url": homepage_url(source),
            "page_title": str(title)[:500] if title else "",
            "page_description": str(description)[:2000] if description else "",
            "numbers": [],
        })
        entry["numbers"] = sorted(set(entry["numbers"]) | set(extract_mobile_numbers(content)))
        if not entry["page_title"] and title:
            entry["page_title"] = str(title)[:500]
        if not entry["page_description"] and description:
            entry["page_description"] = str(description)[:2000]

    if not any(entry["numbers"] for entry in extracted_pages.values()):
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
                        title = metadata.get("title") or data.get("title") or ""
                        description = metadata.get("description") or data.get("description") or ""
                        entry = extracted_pages.setdefault(source, {
                            "source_url": source,
                            "website_url": homepage_url(source),
                            "page_title": str(title)[:500] if title else "",
                            "page_description": str(description)[:2000] if description else "",
                            "numbers": [],
                        })
                        entry["numbers"] = sorted(set(entry["numbers"]) | set(extract_mobile_numbers(content)))
                        if not entry["page_title"] and title:
                            entry["page_title"] = str(title)[:500]
                        if not entry["page_description"] and description:
                            entry["page_description"] = str(description)[:2000]
                except (httpx.HTTPError, ValueError) as exc:
                    log.warning("Contact page scrape failed (%s)", type(exc).__name__)

    if error:
        log.warning("Partial crawl for %s (%s)", site["url"], error)
    return {"page_url": site["url"], "pages": list(extracted_pages.values()), "error": error}


async def process_site_in_background(site: dict, semaphore: asyncio.Semaphore) -> dict:
    async with httpx.AsyncClient(timeout=httpx.Timeout(60, connect=10), follow_redirects=False) as client:
        return await process_site(client, site, semaphore)


async def run_discovery(keyword: str, province: str, target_count: int, search_fn=None, process_fn=None) -> dict:
    """Deterministic URL-level runner retained for unit/regression tests."""
    search_fn = search_fn or search_sites
    process_fn = process_fn or process_site
    processed_urls: set[str] = set()
    registered_numbers: set[str] = set()
    results: list[dict] = []
    warnings: list[str] = []
    completed_rounds = 0
    processed_page_count = successful_page_count = discovered_page_count = 0

    for round_number in range(1, MAX_SEARCH_ROUNDS + 1):
        remaining = target_count - len(results)
        if remaining <= 0:
            break
        url_target = ceil(remaining * SEARCH_URL_FACTOR)
        try:
            sites = await search_fn(keyword, province, url_target, excluded_urls=processed_urls, round_number=round_number)
        except RuntimeError as exc:
            if completed_rounds == 0:
                raise
            warning = f"SearXNG failed in round {round_number}: {exc}"
            warnings.append(warning)
            log.error("%s; returning results from completed rounds", warning)
            break
        completed_rounds = round_number
        unique_sites: list[dict] = []
        round_seen: set[str] = set()
        for site in sites:
            normalized = normalize_url(str(site.get("url", "")))
            if not normalized:
                continue
            if normalized in processed_urls or normalized in round_seen:
                continue
            round_seen.add(normalized)
            unique_sites.append({**site, "url": normalized, "website_url": homepage_url(normalized)})
            discovered_page_count += 1
        if not unique_sites:
            continue
        async with httpx.AsyncClient(timeout=httpx.Timeout(60, connect=10), follow_redirects=False) as client:
            semaphore = asyncio.Semaphore(MAX_CONCURRENT_SITES)
            for site in unique_sites:
                if len(results) >= target_count:
                    break
                processed_urls.add(site["url"])
                site_result = await process_fn(client, site, semaphore)
                processed_page_count += 1
                page_added = 0
                for page in site_result.get("pages", []):
                    for number in sorted(set(page.get("numbers", []))):
                        number = normalize_mobile(str(number))
                        if not number or number in registered_numbers or len(results) >= target_count:
                            continue
                        registered_numbers.add(number)
                        results.append({
                            "number": number,
                            "source_url": page["source_url"],
                            "website_url": page.get("website_url") or homepage_url(page["source_url"]),
                            "page_title": page.get("page_title", ""),
                            "page_description": page.get("page_description", ""),
                        })
                        page_added += 1
                successful_page_count += bool(page_added)
        if len(results) >= target_count:
            break
    response = {
        "schema_version": 2,
        "requested_count": target_count,
        "successful_results": len(results),
        "total_unique_mobile_numbers": len(registered_numbers),
        "discovered_page_count": discovered_page_count,
        "processed_page_count": processed_page_count,
        "successful_page_count": successful_page_count,
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
        "stop_url": f"/v1/jobs/{job['job_id']}/stop",
    })
    return job


@app.post("/v1/jobs/{job_id}/stop", status_code=202, dependencies=[Depends(require_key)])
async def stop_discovery_job(job_id: str, request: Request):
    outcome = get_job_manager(request).request_stop(job_id)
    if outcome is None:
        raise HTTPException(status_code=404, detail="Job not found or expired")
    return {"job_id": job_id, **outcome}


@app.post("/v1/jobs/{job_id}/decisions/{decision_id}", status_code=202, dependencies=[Depends(require_key)])
async def submit_job_decision(job_id: str, decision_id: str, request: Request):
    try:
        payload = await request.json()
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Body must be the JSON boolean true or false") from exc
    if type(payload) is not bool:
        raise HTTPException(status_code=422, detail="Body must be the JSON boolean true or false")
    manager = get_job_manager(request)
    try:
        return manager.decide(job_id, decision_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Job not found or expired") from exc
    except DecisionConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


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
