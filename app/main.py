from __future__ import annotations

import asyncio
import html
import ipaddress
import logging
import os
import re
import secrets
import time
from contextlib import asynccontextmanager
from math import ceil
from urllib.parse import parse_qsl, unquote, urlencode, urljoin, urlsplit, urlunsplit

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field, field_validator

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

# Mobile numbers only. Boundaries prevent extracting 11 digits from a longer number.
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
    yield


app = FastAPI(title="Iran Business Contact Discovery", version="1.1.0", lifespan=lifespan)


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


async def search_sites(
    keyword: str,
    province: str,
    needed: int,
    excluded_domains: set[str] | None = None,
    round_number: int = 1,
) -> list[dict]:
    excluded = excluded_domains or set()
    query = f"{keyword} {province} ایران"
    if round_number == 2:
        query += " شماره تماس"
    elif round_number == 3:
        query += " سایت رسمی تماس"
    sites: dict[str, dict] = {}
    async with httpx.AsyncClient(timeout=SEARCH_TIMEOUT, follow_redirects=True) as client:
        for page_no in range(1, 9):
            try:
                response = await client.get(
                    f"{SEARXNG_URL.rstrip('/')}/search",
                    params={"q": query, "format": "json", "language": "fa-IR", "pageno": page_no},
                )
                response.raise_for_status()
                results = response.json().get("results", [])
            except (httpx.HTTPError, ValueError) as exc:
                if not sites and not excluded:
                    raise RuntimeError(f"SearXNG request failed: {type(exc).__name__}") from exc
                log.warning("SearXNG page %s in round %s failed (%s)", page_no, round_number, type(exc).__name__)
                break
            for item in results:
                normalized = normalize_url(str(item.get("url", "")))
                if not normalized:
                    continue
                domain = domain_of(normalized)
                if domain in excluded or domain in sites:
                    continue
                sites[domain] = {
                    "url": normalized,
                    "website_url": homepage_url(normalized),
                    "title": str(item.get("title", ""))[:300],
                }
            if len(sites) >= needed:
                break
    return list(sites.values())[:needed]


def normalize_mobile(raw: str) -> str | None:
    digits = re.sub(r"\D", "", raw.translate(DIGITS))
    if digits.startswith("0098"):
        digits = digits[4:]
    elif digits.startswith("98") and len(digits) == 12:
        digits = digits[2:]
    if len(digits) == 10 and digits.startswith("9"):
        digits = "0" + digits
    return digits if re.fullmatch(r"09\d{9}", digits) else None


def extract_mobile_numbers(content: str) -> list[str]:
    text = html.unescape(content).translate(DIGITS)
    found = {normalized for match in MOBILE_RE.finditer(text) if (normalized := normalize_mobile(match.group()))}
    return sorted(found)


async def firecrawl_crawl(client: httpx.AsyncClient, site: dict) -> tuple[list[dict], str | None]:
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
    except (httpx.HTTPError, ValueError) as exc:
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

    # If the bounded crawl did not find any mobile, scrape up to three explicit
    # contact/about links from the pages already collected.
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


async def run_discovery(
    keyword: str,
    province: str,
    target_count: int,
    search_fn=None,
    process_fn=None,
) -> dict:
    search_fn = search_fn or search_sites
    process_fn = process_fn or process_site
    processed_domains: set[str] = set()
    registered_numbers: set[str] = set()
    results: list[dict] = []
    completed_rounds = 0

    for round_number in range(1, MAX_SEARCH_ROUNDS + 1):
        remaining = target_count - len(results)
        if remaining <= 0:
            break
        url_target = ceil(remaining * SEARCH_URL_FACTOR)
        try:
            sites = await search_fn(
                keyword,
                province,
                url_target,
                excluded_domains=processed_domains,
                round_number=round_number,
            )
        except RuntimeError:
            if completed_rounds == 0:
                raise
            log.exception("SearXNG failed in round %s; returning collected results", round_number)
            break
        completed_rounds = round_number

        unique_sites: list[dict] = []
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
                    site_phones = site_result.get("phones") or {}
                    new_numbers = [number for number in site_phones if number not in registered_numbers]
                    if not new_numbers:
                        continue
                    website_result = {"website_url": site_result["website_url"], "mobile_numbers": []}
                    for number in new_numbers:
                        registered_numbers.add(number)
                        website_result["mobile_numbers"].append(
                            {"number": number, "source_url": site_phones[number]}
                        )
                    results.append(website_result)
                    if len(results) >= target_count:
                        break
                if len(results) >= target_count:
                    break
        if len(results) >= target_count:
            break

    return {
        "requested_count": target_count,
        "successful_websites": len(results),
        "total_unique_mobile_numbers": len(registered_numbers),
        "completed_rounds": completed_rounds,
        "results": results,
    }


@app.post("/v1/discover", dependencies=[Depends(require_key)])
async def discover(body: DiscoverRequest):
    try:
        return await run_discovery(body.keyword, body.province, body.count)
    except RuntimeError as exc:
        log.error("Search failed: %s", exc)
        raise HTTPException(status_code=502, detail="Search provider unavailable") from exc
