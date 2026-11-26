import asyncio
import math
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from app.main import (
    extract_mobile_numbers,
    firecrawl_crawl,
    normalize_mobile,
    normalize_url,
    process_site,
    run_discovery,
    search_sites,
)


class PhoneExtractionTests(unittest.TestCase):
    def test_english_persian_and_arabic_digits(self):
        for value in ("09123456789", "۰۹۱۲۳۴۵۶۷۸۹", "٠٩١٢٣٤٥٦٧٨٩"):
            self.assertEqual(normalize_mobile(value), "09123456789")

    def test_country_prefix_and_common_separators(self):
        values = (
            "09123456789", "9123456789", "+989123456789", "+98 912 345 6789",
            "+98 09123456789", "+98 0912 345 6789", "00989123456789", "0098 0912 345 6789",
            "989123456789", "98 09123456789", "0912-345-6789", "0912 345 6789", "0912.345.6789",
            "0912(345)6789", "۰۹۱۲۳۴۵۶۷۸۹", "٠٩١٢٣٤٥٦٧٨٩",
        )
        for value in values:
            with self.subTest(value=value):
                self.assertEqual(normalize_mobile(value), "09123456789")
                self.assertIn("09123456789", extract_mobile_numbers(value))

    def test_rejects_invalid_truncated_and_landline_numbers(self):
        for value in ("912345679", "0912345678", "02123456789", "03123456789"):
            with self.subTest(value=value):
                self.assertIsNone(normalize_mobile(value))
                self.assertEqual(extract_mobile_numbers(value), [])

    def test_multiple_numbers_and_equivalent_formats_are_deduplicated(self):
        self.assertEqual(extract_mobile_numbers("09123456789 +989123456789 09351234567"), ["09123456789", "09351234567"])

    def test_long_numeric_string_does_not_yield_partial_number(self):
        self.assertEqual(extract_mobile_numbers("12309123456789123"), [])


class URLNormalizationTests(unittest.TestCase):
    def test_tracking_parameters_removed_but_listing_ids_and_query_preserved(self):
        self.assertEqual(
            normalize_url("https://www.divar.ir/v/listing-1?ad_id=123&utm_source=x&city=esfahan&fbclid=abc"),
            "https://divar.ir/v/listing-1?ad_id=123&city=esfahan",
        )

    def test_distinct_paths_on_same_domain_stay_distinct(self):
        first = normalize_url("https://divar.ir/v/listing-1")
        second = normalize_url("https://divar.ir/v/listing-2")
        self.assertNotEqual(first, second)

    def test_rejects_unsafe_or_invalid_urls(self):
        for value in ("javascript:alert(1)", "http://127.0.0.1/a", "https://user:pass@example.ir/"):
            self.assertIsNone(normalize_url(value))


class FirecrawlFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_firecrawl_timeout_is_returned_as_error(self):
        class TimeoutClient:
            async def post(self, *args, **kwargs):
                raise httpx.TimeoutException("Firecrawl timed out")

        pages, error = await firecrawl_crawl(TimeoutClient(), {"url": "https://timeout.ir/item/1"})
        self.assertEqual(pages, [])
        self.assertIn("TimeoutException", error)

    async def test_process_site_preserves_each_crawled_page_and_real_metadata(self):
        pages = [
            {
                "url": "https://divar.ir/v/real-listing-identifier",
                "metadata": {"sourceURL": "https://www.divar.ir/v/real-listing-identifier?ad_id=9", "title": "فروش گوشی", "description": "شرح واقعی"},
                "markdown": "09123456789 و 09351234567",
            },
            {
                "url": "https://divar.ir/v/other-listing",
                "metadata": {"url": "https://divar.ir/v/other-listing", "title": "آگهی دوم"},
                "markdown": "09111111111",
            },
            {"url": "https://other.ir/contact", "markdown": "09991234567"},
        ]
        with patch("app.main.firecrawl_crawl", new=AsyncMock(return_value=(pages, None))):
            result = await process_site(object(), {"url": "https://divar.ir/v/start", "website_url": "https://divar.ir/"}, asyncio.Semaphore(1))
        self.assertEqual(len(result["pages"]), 2)
        by_url = {page["source_url"]: page for page in result["pages"]}
        first = by_url["https://divar.ir/v/real-listing-identifier?ad_id=9"]
        second = by_url["https://divar.ir/v/other-listing"]
        self.assertEqual(first["website_url"], "https://divar.ir/")
        self.assertEqual(first["numbers"], ["09123456789", "09351234567"])
        self.assertEqual(first["page_title"], "فروش گوشی")
        self.assertEqual(first["page_description"], "شرح واقعی")
        self.assertEqual(second["page_description"], "")

    async def test_invalid_firecrawl_page_source_is_not_replaced_with_homepage(self):
        pages = [{"metadata": {"sourceURL": "javascript:alert(1)"}, "markdown": "09123456789"}]
        with patch("app.main.firecrawl_crawl", new=AsyncMock(return_value=(pages, None))):
            result = await process_site(object(), {"url": "https://market.ir/listing/1", "website_url": "https://market.ir/"}, asyncio.Semaphore(1))
        self.assertEqual(result["pages"], [])

    async def test_firecrawl_page_failure_is_isolated_and_returns_empty_pages(self):
        with patch("app.main.firecrawl_crawl", new=AsyncMock(return_value=([], "Firecrawl job failed"))):
            result = await process_site(object(), {"url": "https://failed.ir/listing", "website_url": "https://failed.ir/"}, asyncio.Semaphore(1))
        self.assertEqual(result["pages"], [])
        self.assertEqual(result["error"], "Firecrawl job failed")

    async def test_firecrawl_failed_status_is_reported(self):
        class FailedClient:
            async def post(self, *args, **kwargs):
                return httpx.Response(200, json={"id": "crawl-1"}, request=httpx.Request("POST", "https://firecrawl.test/v2/crawl"))
            async def get(self, *args, **kwargs):
                return httpx.Response(200, json={"status": "failed", "data": []}, request=httpx.Request("GET", "https://firecrawl.test/v2/crawl/crawl-1"))
        pages, error = await firecrawl_crawl(FailedClient(), {"url": "https://market.ir/item/1"})
        self.assertEqual(pages, [])
        self.assertEqual(error, "Firecrawl job failed")


class SearchPageTests(unittest.IsolatedAsyncioTestCase):
    async def test_accepts_many_same_domain_urls_and_deduplicates_complete_normalized_url(self):
        response = httpx.Response(200, json={"results": [
            {"url": "https://www.divar.ir/v/1?utm_source=x&city=esfahan"},
            {"url": "https://divar.ir/v/2"},
            {"url": "https://divar.ir/v/1?city=esfahan"},
        ]}, request=httpx.Request("GET", "https://searx.test/search"))

        class Client:
            async def __aenter__(self): return self
            async def __aexit__(self, *args): return None
            async def get(self, *args, **kwargs): return response

        with patch("app.main.httpx.AsyncClient", return_value=Client()):
            pages = await search_sites("آگهی", "اصفهان", 10)
        self.assertEqual([page["url"] for page in pages], [
            "https://divar.ir/v/1?city=esfahan", "https://divar.ir/v/2",
        ])

    async def test_excludes_exact_processed_url_without_excluding_domain(self):
        response = httpx.Response(200, json={"results": [
            {"url": "https://divar.ir/v/1"}, {"url": "https://divar.ir/v/2"},
        ]}, request=httpx.Request("GET", "https://searx.test/search"))

        class Client:
            async def __aenter__(self): return self
            async def __aexit__(self, *args): return None
            async def get(self, *args, **kwargs): return response

        with patch("app.main.httpx.AsyncClient", return_value=Client()):
            pages = await search_sites("آگهی", "اصفهان", 5, excluded_urls={"https://divar.ir/v/1"})
        self.assertEqual([page["url"] for page in pages], ["https://divar.ir/v/2"])

    async def test_pagination_callback_emits_multiple_marketplace_candidates(self):
        pages_requested = []

        class Client:
            async def __aenter__(self): return self
            async def __aexit__(self, *args): return None
            async def get(self, _url, params):
                pages_requested.append(params["pageno"])
                items = [{"url": f"https://torob.com/p/{params['pageno']}-{i}"} for i in range(3)] if params["pageno"] == 1 else []
                return httpx.Response(200, json={"results": items}, request=httpx.Request("GET", _url))

        callbacks = []
        async def checkpoint(next_page, pages, exhausted):
            callbacks.append((next_page, [page["url"] for page in pages], exhausted))

        with patch("app.main.httpx.AsyncClient", return_value=Client()):
            found = await search_sites("محصول", "ایران", 3, page_callback=checkpoint, max_pages=4)
        self.assertEqual(len(found), 3)
        self.assertEqual(len({page["url"] for page in found}), 3)
        self.assertEqual(pages_requested, [1])
        self.assertEqual(len(callbacks[0][1]), 3)

    async def test_provider_errors_invalid_json_and_empty_page_are_distinguished(self):
        class Client:
            def __init__(self, response=None, error=None): self.response, self.error = response, error
            async def __aenter__(self): return self
            async def __aexit__(self, *args): return None
            async def get(self, *args, **kwargs):
                if self.error: raise self.error
                return self.response

        request = httpx.Request("GET", "https://searx.test/search")
        for response in (httpx.Response(503, request=request), httpx.Response(200, text="not-json", request=request)):
            with patch("app.main.httpx.AsyncClient", return_value=Client(response=response)):
                with self.assertRaisesRegex(RuntimeError, "SearXNG request failed"):
                    await search_sites("shop", "Iran", 1)
        with patch("app.main.httpx.AsyncClient", return_value=Client(error=httpx.TimeoutException("timeout"))):
            with self.assertRaisesRegex(RuntimeError, "SearXNG request failed"):
                await search_sites("shop", "Iran", 1)
        with patch("app.main.httpx.AsyncClient", return_value=Client(error=httpx.ConnectError("connection failed"))):
            with self.assertRaisesRegex(RuntimeError, "SearXNG request failed"):
                await search_sites("shop", "Iran", 1)
        with patch("app.main.httpx.AsyncClient", return_value=Client(response=httpx.Response(200, json={"results": []}, request=request))):
            self.assertEqual(await search_sites("shop", "Iran", 1), [])


class FlatDiscoveryTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def candidate(url):
        return {"url": url, "website_url": url.split("/", 3)[0] + "//" + url.split("/", 3)[2] + "/"}

    async def test_one_flat_record_per_number_exact_listing_url_and_page_metadata(self):
        urls = ["https://divar.ir/v/listing-1", "https://divar.ir/v/listing-2"]

        async def search(_keyword, _province, _needed, excluded_urls, round_number):
            return [self.candidate(url) for url in urls if url not in excluded_urls]

        async def process(_client, page, _semaphore):
            if page["url"].endswith("1"):
                return {"pages": [{"source_url": page["url"], "website_url": "https://divar.ir/", "page_title": "آگهی یک", "page_description": "شرح", "numbers": ["09351234567", "09123456789"]}]}
            return {"pages": [{"source_url": page["url"], "website_url": "https://divar.ir/", "page_title": "آگهی دو", "page_description": "", "numbers": ["09111111111"]}]}

        response = await run_discovery("موبایل", "اصفهان", 3, search, process)
        self.assertEqual(response["schema_version"], 2)
        self.assertEqual(response["successful_results"], 3)
        self.assertEqual(response["total_unique_mobile_numbers"], 3)
        self.assertEqual([record["source_url"] for record in response["results"]], [urls[0], urls[0], urls[1]])
        self.assertEqual(response["results"][0]["website_url"], "https://divar.ir/")
        self.assertEqual(response["results"][0]["page_title"], "آگهی یک")

    async def test_duplicate_number_keeps_first_source_and_target_never_overshoots(self):
        urls = ["https://market.ir/item/1", "https://market.ir/item/2"]

        async def search(_keyword, _province, _needed, excluded_urls, round_number):
            return [self.candidate(url) for url in urls if url not in excluded_urls]

        async def process(_client, page, _semaphore):
            phones = ["09123456789", "09351234567"] if page["url"].endswith("1") else ["09123456789", "09111111111"]
            return {"pages": [{"source_url": page["url"], "website_url": "https://market.ir/", "numbers": phones}]}

        response = await run_discovery("shop", "Iran", 3, search, process)
        self.assertEqual([record["number"] for record in response["results"]], ["09123456789", "09351234567", "09111111111"])
        self.assertEqual(response["results"][0]["source_url"], urls[0])
        self.assertEqual(len(response["results"]), 3)

    async def test_unique_url_exclusion_allows_same_domain_on_later_round(self):
        calls = []
        async def search(_keyword, _province, needed, excluded_urls, round_number):
            calls.append((needed, set(excluded_urls), round_number))
            if round_number == 1:
                return [self.candidate("https://shop.ir/item/1")]
            if round_number == 2:
                return [self.candidate("https://shop.ir/item/2")]
            return []

        async def process(_client, page, _semaphore):
            number = "09123456789" if page["url"].endswith("1") else "09351234567"
            return {"pages": [{"source_url": page["url"], "website_url": "https://shop.ir/", "numbers": [number]}]}

        response = await run_discovery("shop", "Iran", 2, search, process)
        self.assertEqual(response["successful_results"], 2)
        self.assertEqual(calls[1][1], {"https://shop.ir/item/1"})
        self.assertEqual(response["results"][1]["source_url"], "https://shop.ir/item/2")

    async def test_round_target_is_ceil_of_remaining_number_count_times_1_6(self):
        targets = []
        async def search(_keyword, _province, needed, excluded_urls, round_number):
            targets.append(needed)
            return [self.candidate(f"https://market.ir/p/{round_number}-{i}") for i in range(2)] if round_number == 1 else []
        async def process(_client, page, _semaphore):
            return {"pages": []}
        await run_discovery("shop", "Iran", 5, search, process)
        self.assertEqual(targets, [math.ceil(5 * 1.6), math.ceil(5 * 1.6), math.ceil(5 * 1.6)])

    async def test_search_failure_keeps_partial_records_and_errors(self):
        async def search(_keyword, _province, _needed, excluded_urls, round_number):
            if round_number == 1:
                return [self.candidate("https://market.ir/item/1")]
            raise RuntimeError("SearXNG unavailable")
        async def process(_client, page, _semaphore):
            return {"pages": [{"source_url": page["url"], "website_url": "https://market.ir/", "numbers": ["09123456789"]}]}
        response = await run_discovery("shop", "Iran", 2, search, process)
        self.assertEqual(response["successful_results"], 1)
        self.assertIn("SearXNG unavailable", response["warnings"][0])

    async def test_stops_after_first_round_when_number_target_is_met(self):
        calls = []
        async def search(_keyword, _province, _needed, excluded_urls, round_number):
            calls.append(round_number)
            return [self.candidate("https://market.ir/item/1")]
        async def process(_client, page, _semaphore):
            return {"pages": [{"source_url": page["url"], "website_url": "https://market.ir/", "numbers": ["09123456789"]}]}
        result = await run_discovery("shop", "Iran", 1, search, process)
        self.assertEqual(calls, [1])
        self.assertEqual(result["completed_rounds"], 1)

    async def test_round_three_search_error_preserves_prior_flat_result(self):
        async def search(_keyword, _province, _needed, excluded_urls, round_number):
            if round_number == 1:
                return [self.candidate("https://market.ir/item/1")]
            if round_number == 2:
                return []
            raise RuntimeError("SearXNG invalid response")
        async def process(_client, page, _semaphore):
            return {"pages": [{"source_url": page["url"], "website_url": "https://market.ir/", "numbers": ["09123456789"]}]}
        result = await run_discovery("shop", "Iran", 2, search, process)
        self.assertEqual(result["successful_results"], 1)
        self.assertEqual(result["results"][0]["source_url"], "https://market.ir/item/1")
        self.assertIn("SearXNG invalid response", result["warnings"][0])

    async def test_first_round_search_error_is_not_treated_as_empty(self):
        async def search(*args, **kwargs):
            raise RuntimeError("SearXNG timeout")
        with self.assertRaisesRegex(RuntimeError, "SearXNG timeout"):
            await run_discovery("shop", "Iran", 1, search, AsyncMock())


if __name__ == "__main__":
    unittest.main()
