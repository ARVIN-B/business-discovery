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
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit, urljoin

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
MOBILE_RE = re.compile(r"(?<!\d)(?:(?:\+?98|0098)[\s().-]*|0)?9\d{2}[\s().-]*\d{3}[\s().-]*\d{4}(?!\d)")
LANDLINE_RE = re.compile(r"(?<!\d)(?:(?:\+?98|0098)[\s().-]*|0)(?:[1-8]\d)[\s().-]*\d{3}[\s().-]*\d{4}(?!\d)")
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

app = FastAPI(title="Iran Business Contact Discovery", version="1.0.0", lifespan=lifespan)

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

async def search_sites(keyword: str, province: str, needed: int) -> list[dict]:
    urls: dict[str, dict] = {}
    query = f"{keyword} {province} ایران"
    async with httpx.AsyncClient(timeout=SEARCH_TIMEOUT, follow_redirects=True) as client:
        for page_no in range(1, 9):
            try:
                response = await client.get(f"{SEARXNG_URL.rstrip('/')}/search", params={"q": query, "format": "json", "language": "fa-IR", "pageno": page_no})
                response.raise_for_status()
                results = response.json().get("results", [])
            except (httpx.HTTPError, ValueError) as exc:
                if not urls:
                    raise RuntimeError(f"SearXNG request failed: {type(exc).__name__}") from exc
                log.warning("SearXNG page %s failed (%s)", page_no, type(exc).__name__)
                break
            for item in results:
                normalized = normalize_url(str(item.get("url", "")))
                if not normalized:
                    continue
                domain = domain_of(normalized)
                if domain not in urls:
                    urls[domain] = {"url": normalized, "search_url": normalized, "title": str(item.get("title", ""))[:300]}
            if len(urls) >= needed:
                break
    return list(urls.values())[:needed]

def normalize_phone(raw: str, mobile: bool) -> str | None:
    digits = re.sub(r"\D", "", raw.translate(DIGITS))
    if digits.startswith("0098"):
        digits = "0" + digits[4:]
    elif digits.startswith("98") and len(digits) >= 11:
        digits = "0" + digits[2:]
    if mobile:
        if len(digits) == 10 and digits.startswith("9"):
            digits = "0" + digits
        return digits if re.fullmatch(r"09\d{9}", digits) else None
    return digits if re.fullmatch(r"0[1-8]\d{9}", digits) else None

def extract_phones(content: str) -> list[dict]:
    text = html.unescape(content).translate(DIGITS)
    found = {}
    for kind, pattern in (("mobile", MOBILE_RE), ("landline", LANDLINE_RE)):
        for match in pattern.finditer(text):
            number = normalize_phone(match.group(), kind == "mobile")
            if number:
                found[number] = kind
    return [{"number": n, "type": found[n]} for n in sorted(found)]

async def firecrawl_crawl(client: httpx.AsyncClient, site: dict) -> tuple[list[dict], str | None]:
    start = site["url"]
    try:
        created = await client.post(f"{FIRECRAWL_URL.rstrip('/')}/v2/crawl", json={
            "url": start, "limit": MAX_CRAWL_PAGES, "maxDiscoveryDepth": MAX_CRAWL_DEPTH,
            "scrapeOptions": {"formats": ["markdown", "rawHtml"], "onlyMainContent": False, "timeout": 45000}
        }, timeout=45)
        created.raise_for_status()
        job = created.json()
        job_id = job.get("id")
        if not job_id:
            return [], "Firecrawl did not return a crawl job id"
        end = time.monotonic() + CRAWL_TIMEOUT
        partial_pages: list[dict] = []
        partial_next = None
        while time.monotonic() < end:
            result = await client.get(f"{FIRECRAWL_URL.rstrip('/')}/v2/crawl/{job_id}", timeout=30)
            result.raise_for_status()
            payload = result.json()
            partial_pages = list(payload.get("data") or partial_pages)
            partial_next = payload.get("next") or partial_next
            status = payload.get("status")
            if status == "completed":
                pages = list(payload.get("data") or [])
                next_url = payload.get("next")
                while next_url:
                    path = urlsplit(next_url)
                    page = await client.get(FIRECRAWL_URL.rstrip("/") + (path.path + ("?" + path.query if path.query else "")), timeout=30)
                    page.raise_for_status()
                    pg = page.json()
                    pages.extend(pg.get("data") or [])
                    next_url = pg.get("next")
                return pages, None
            if status in {"failed", "cancelled"}:
                return [], f"Firecrawl job {status}"
            await asyncio.sleep(3)
        try:
            await client.delete(f"{FIRECRAWL_URL.rstrip('/')}/v2/crawl/{job_id}", timeout=10)
        except httpx.HTTPError:
            log.warning("Could not cancel timed-out Firecrawl crawl")
        message = "Firecrawl crawl timed out with partial results" if partial_pages else "Firecrawl crawl timed out"
        return partial_pages, message
    except (httpx.HTTPError, ValueError) as exc:
        return [], f"Firecrawl request failed: {type(exc).__name__}"

async def process_site(client: httpx.AsyncClient, site: dict, semaphore: asyncio.Semaphore) -> dict:
    async with semaphore:
        pages, error = await firecrawl_crawl(client, site)
    if error and not pages:
        return {"business_name": site.get("title") or domain_of(site["url"]), "website": site["url"], "discovered_url": site.get("search_url", site["url"]), "pages_processed": 0, "phones": [], "error": error}
    title = site.get("title") or domain_of(site["url"])
    contacts: dict[str, dict] = {}

    def priority(page: dict) -> tuple[int, int]:
        metadata = page.get("metadata") or {}
        source = str(metadata.get("sourceURL") or metadata.get("url") or page.get("url") or site["url"])
        path = urlsplit(source).path.strip("/").lower()
        if not path:
            return (0, 0)
        if any(hint in path for hint in CONTACT_HINTS):
            return (1, len(path))
        return (2, len(path))

    pages.sort(key=priority)
    for page in pages:
        metadata = page.get("metadata") or {}
        source = normalize_url(str(metadata.get("sourceURL") or metadata.get("url") or page.get("url") or site["url"])) or site["url"]
        if domain_of(source) != domain_of(site["url"]):
            continue
        content = "\n".join(str(page.get(k) or "") for k in ("markdown", "html", "rawHtml"))
        if not title or title == domain_of(site["url"]):
            title = str(metadata.get("title") or title)
        for number in extract_phones(content):
            contacts.setdefault(number["number"], {**number, "source_url": source})

    if not contacts:
        candidates: list[str] = []
        for page in pages:
            metadata = page.get("metadata") or {}
            source = str(metadata.get("sourceURL") or metadata.get("url") or page.get("url") or site["url"])
            raw = "\n".join(str(page.get(k) or "") for k in ("html", "rawHtml"))
            for _, href in re.findall(r'href\s*=\s*(["\'])(.*?)\1', raw, flags=re.I):
                link = normalize_url(urljoin(source, html.unescape(href)))
                if link and domain_of(link) == domain_of(site["url"]):
                    path = urlsplit(link).path.lower()
                    if any(hint in path for hint in CONTACT_HINTS) and link not in candidates:
                        candidates.append(link)
        async with httpx.AsyncClient(timeout=45, follow_redirects=False) as scrape_client:
            for target in candidates[:3]:
                try:
                    response = await scrape_client.post(f"{FIRECRAWL_URL.rstrip('/')}/v2/scrape", json={
                        "url": target, "formats": ["markdown", "rawHtml"], "onlyMainContent": False, "timeout": 35000
                    })
                    response.raise_for_status()
                    data = response.json().get("data") or {}
                    content = "\n".join(str(data.get(k) or "") for k in ("markdown", "html", "rawHtml"))
                    metadata = data.get("metadata") or {}
                    source = normalize_url(str(metadata.get("sourceURL") or metadata.get("url") or target)) or target
                    for number in extract_phones(content):
                        contacts.setdefault(number["number"], {**number, "source_url": source})
                except (httpx.HTTPError, ValueError) as exc:
                    log.warning("Contact page scrape failed (%s)", type(exc).__name__)
                if contacts:
                    break
    result = {"business_name": title[:300], "website": site["url"], "discovered_url": site.get("search_url", site["url"]), "pages_processed": len(pages), "phones": list(contacts.values())}
    if error:
        result["error"] = error
    return result

@app.post("/v1/discover", dependencies=[Depends(require_key)])
async def discover(body: DiscoverRequest):
    target_count = body.count
    try:
        sites = await search_sites(body.keyword, body.province, max(target_count + max(1, (target_count + 9) // 10), target_count))
    except RuntimeError as exc:
        log.error("Search failed: %s", exc)
        raise HTTPException(status_code=502, detail="Search provider unavailable") from exc
    if not sites:
        return {"keyword": body.keyword, "province": body.province, "requested_count": target_count, "discovered_urls": 0, "processed_websites": 0, "results": [], "warnings": ["No suitable websites found"]}
    async with httpx.AsyncClient(timeout=httpx.Timeout(60, connect=10), follow_redirects=False) as client:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_SITES)
        tasks = [process_site(client, site, semaphore) for site in sites]
        processed = await asyncio.gather(*tasks)
    return {"keyword": body.keyword, "province": body.province, "requested_count": target_count,
            "discovered_urls": len(sites), "processed_websites": len(processed),
            "results": processed, "warnings": [r["error"] for r in processed if r.get("error")]}
