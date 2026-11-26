import asyncio
import math
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from app.main import extract_mobile_numbers, normalize_mobile, run_discovery, search_sites


class PhoneExtractionTests(unittest.TestCase):
    def test_english_persian_and_arabic_digits(self):
        self.assertEqual(normalize_mobile("09123456789"), "09123456789")
        self.assertEqual(normalize_mobile("۰۹۱۲۳۴۵۶۷۸۹"), "09123456789")
        self.assertEqual(normalize_mobile("٠٩١٢٣٤٥٦٧٨٩"), "09123456789")

    def test_country_prefix_and_without_prefix(self):
        examples = [
            "09123456789",
            "9123456789",
            "+989123456789",
            "+98 912 345 6789",
            "+98 09123456789",
            "+98 0912 345 6789",
            "00989123456789",
            "0098 0912 345 6789",
            "989123456789",
            "98 09123456789",
            "۰۹۱۲۳۴۵۶۷۸۹",
            "٠٩١٢٣٤٥٦٧٨٩",
        ]
        for value in examples:
            with self.subTest(value=value):
                self.assertEqual(normalize_mobile(value), "09123456789")
                self.assertIn("09123456789", extract_mobile_numbers(value))

    def test_common_separators(self):
        examples = [
            "0912-345-6789",
            "0912 345 6789",
            "0912.345.6789",
            "0912(345)6789",
        ]
        for value in examples:
            with self.subTest(value=value):
                self.assertEqual(normalize_mobile(value), "09123456789")
                self.assertIn("09123456789", extract_mobile_numbers(value))

    def test_rejects_invalid_truncated_and_landline_numbers(self):
        for value in ("912345679", "0912345678", "02123456789", "03123456789"):
            with self.subTest(value=value):
                self.assertIsNone(normalize_mobile(value))
                self.assertEqual(extract_mobile_numbers(value), [])

    def test_multiple_distinct_numbers_are_extracted(self):
        self.assertEqual(
            extract_mobile_numbers("تماس: 09123456789 و 09351234567"),
            ["09123456789", "09351234567"],
        )

    def test_equivalent_formats_are_deduplicated(self):
        text = "09123456789 +989123456789 00989123456789 ۰۹۱۲۳۴۵۶۷۸۹"
        self.assertEqual(extract_mobile_numbers(text), ["09123456789"])

    def test_longer_numeric_strings_do_not_yield_partial_mobile(self):
        self.assertEqual(extract_mobile_numbers("12309123456789123"), [])


class DiscoveryAlgorithmTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def site(domain, path="/"):
        url = f"https://{domain}{path}"
        return {"url": url, "website_url": f"https://{domain}/", "title": domain}

    async def test_website_with_multiple_numbers_counts_once(self):
        search = AsyncMock(return_value=[self.site("a.ir"), self.site("b.ir")])

        async def process(_client, site, _semaphore):
            return {
                "website_url": site["website_url"],
                "phones": {"09123456789": "https://a.ir/contact", "09351234567": "https://a.ir/about"},
            }

        response = await run_discovery("رستوران", "اصفهان", 1, search, process)
        self.assertEqual(response["successful_websites"], 1)
        self.assertEqual(len(response["results"]), 1)
        self.assertEqual(len(response["results"][0]["mobile_numbers"]), 2)

    async def test_domains_and_urls_are_not_processed_again_across_rounds(self):
        calls = []
        processed = []

        async def search(_keyword, _province, needed, excluded_domains, round_number):
            calls.append((needed, set(excluded_domains), round_number))
            if round_number == 1:
                return [self.site("a.ir", "/one"), self.site("www.a.ir", "/two")]
            if round_number == 2:
                return [self.site("a.ir", "/again"), self.site("b.ir")]
            return [self.site("b.ir", "/again"), self.site("c.ir")]

        async def process(_client, site, _semaphore):
            domain = site["website_url"].split("//", 1)[1].strip("/")
            processed.append(domain)
            if domain == "a.ir":
                phones = {}
            else:
                phones = {"09123456789" if domain == "b.ir" else "09351234567": site["url"]}
            return {"website_url": site["website_url"], "phones": phones}

        response = await run_discovery("رستوران", "اصفهان", 2, search, process)
        self.assertEqual(processed.count("a.ir"), 1)
        self.assertEqual(processed.count("b.ir"), 1)
        self.assertEqual(processed.count("c.ir"), 1)
        self.assertEqual(response["successful_websites"], 2)
        self.assertIn("a.ir", calls[1][1])
        self.assertIn("b.ir", calls[2][1])

    async def test_stops_after_first_round_when_target_is_met(self):
        search = AsyncMock(return_value=[self.site("a.ir"), self.site("b.ir")])

        async def process(_client, site, _semaphore):
            return {"website_url": site["website_url"], "phones": {"09123456789": site["url"]}}

        response = await run_discovery("کافه", "ایران", 1, search, process)
        self.assertEqual(search.await_count, 1)
        self.assertEqual(response["completed_rounds"], 1)

    async def test_search_targets_use_ceil_of_160_percent_of_remaining(self):
        requested_targets = []

        async def search(_keyword, _province, needed, excluded_domains, round_number):
            requested_targets.append(needed)
            return [self.site(f"r{round_number}-{i}.ir") for i in range(2)] if round_number == 1 else []

        async def process(_client, site, _semaphore):
            domain = site["website_url"].split("//", 1)[1].strip("/")
            suffix = sum(map(ord, domain)) % 1_000_000_000
            return {"website_url": site["website_url"], "phones": {f"09{suffix:09d}": site["url"]}}

        await run_discovery("shop", "Iran", 5, search, process)
        self.assertEqual(requested_targets, [math.ceil(5 * 1.6), math.ceil(3 * 1.6), math.ceil(3 * 1.6)])

    async def test_stops_after_three_rounds(self):
        search = AsyncMock(side_effect=lambda _keyword, _province, _needed, excluded_domains, round_number: [self.site(f"empty{round_number}.ir")])

        async def process(_client, site, _semaphore):
            return {"website_url": site["website_url"], "phones": {}}

        response = await run_discovery("shop", "Iran", 2, search, process)
        self.assertEqual(search.await_count, 3)
        self.assertEqual(response["completed_rounds"], 3)
        self.assertEqual(response["results"], [])

    async def test_zero_mobile_results_are_a_valid_empty_response(self):
        search = AsyncMock(return_value=[self.site("empty.ir")])

        async def process(_client, site, _semaphore):
            return {"website_url": site["website_url"], "phones": {}}

        response = await run_discovery("shop", "Iran", 1, search, process)
        self.assertEqual(response, {
            "requested_count": 1,
            "successful_websites": 0,
            "total_unique_mobile_numbers": 0,
            "completed_rounds": 3,
            "results": [],
        })

    async def test_phone_source_url_is_preserved(self):
        search = AsyncMock(return_value=[self.site("source.ir")])
        source = "https://source.ir/contact-us"

        async def process(_client, site, _semaphore):
            return {"website_url": site["website_url"], "phones": {"09123456789": source}}

        response = await run_discovery("shop", "Iran", 1, search, process)
        self.assertEqual(response["results"][0]["mobile_numbers"], [
            {"number": "09123456789", "source_url": source}
        ])


    async def test_searxng_error_in_first_round_is_raised(self):
        search = AsyncMock(side_effect=RuntimeError("SearXNG request failed: TimeoutException"))
        with self.assertRaisesRegex(RuntimeError, "SearXNG request failed"):
            await run_discovery("shop", "Iran", 1, search, AsyncMock())

    async def test_round_two_search_error_preserves_round_one_results_and_warns(self):
        async def search(_keyword, _province, _needed, excluded_domains, round_number):
            if round_number == 1:
                return [self.site("round-one.ir")]
            raise RuntimeError("SearXNG request failed: HTTPStatusError")

        async def process(_client, site, _semaphore):
            return {"website_url": site["website_url"], "phones": {"09123456789": "https://round-one.ir/contact"}}

        response = await run_discovery("shop", "Iran", 2, search, process)
        self.assertEqual(response["successful_websites"], 1)
        self.assertEqual(response["completed_rounds"], 1)
        self.assertEqual(len(response["results"]), 1)
        self.assertIn("SearXNG failed in round 2", response["warnings"][0])

    async def test_round_three_search_error_preserves_prior_results_and_warns(self):
        async def search(_keyword, _province, _needed, excluded_domains, round_number):
            if round_number == 1:
                return [self.site("round-one.ir")]
            if round_number == 2:
                return []
            raise RuntimeError("SearXNG request failed: invalid JSON")

        async def process(_client, site, _semaphore):
            return {"website_url": site["website_url"], "phones": {"09123456789": "https://round-one.ir/contact"}}

        response = await run_discovery("shop", "Iran", 2, search, process)
        self.assertEqual(response["successful_websites"], 1)
        self.assertEqual(response["completed_rounds"], 2)
        self.assertEqual(len(response["results"]), 1)
        self.assertIn("SearXNG failed in round 3", response["warnings"][0])

    async def test_successful_empty_search_is_not_an_error(self):
        search = AsyncMock(return_value=[])
        response = await run_discovery("shop", "Iran", 1, search, AsyncMock())
        self.assertEqual(response["completed_rounds"], 3)
        self.assertEqual(response["results"], [])
        self.assertNotIn("warnings", response)

    async def test_successful_nonempty_search_is_processed(self):
        search = AsyncMock(return_value=[self.site("found.ir")])

        async def process(_client, site, _semaphore):
            return {"website_url": site["website_url"], "phones": {"09123456789": "https://found.ir/contact"}}

        response = await run_discovery("shop", "Iran", 1, search, process)
        self.assertEqual(response["successful_websites"], 1)
        self.assertEqual(response["results"][0]["website_url"], "https://found.ir/")

    async def test_search_sites_raises_http_timeout_and_invalid_json_even_with_exclusions(self):
        class FakeClient:
            def __init__(self, behavior):
                self.behavior = behavior

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

            async def get(self, *args, **kwargs):
                if isinstance(self.behavior, Exception):
                    raise self.behavior
                return self.behavior

        request = httpx.Request("GET", "http://searx.test/search")
        http_response = httpx.Response(503, request=request)
        invalid_json = httpx.Response(200, text="not-json", request=request)
        cases = [
            httpx.TimeoutException("timed out"),
            httpx.ConnectError("connection failed"),
            http_response,
            invalid_json,
        ]
        for behavior in cases:
            with self.subTest(behavior=type(behavior).__name__):
                with patch("app.main.httpx.AsyncClient", return_value=FakeClient(behavior)):
                    with self.assertRaisesRegex(RuntimeError, "SearXNG request failed"):
                        await search_sites("shop", "Iran", 1, excluded_domains={"previous.ir"})

    async def test_search_sites_distinguishes_successful_empty_and_nonempty_results(self):
        class FakeClient:
            def __init__(self, response):
                self.response = response

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

            async def get(self, *args, **kwargs):
                return self.response

        request = httpx.Request("GET", "http://searx.test/search")
        empty = httpx.Response(200, json={"results": []}, request=request)
        with patch("app.main.httpx.AsyncClient", return_value=FakeClient(empty)):
            self.assertEqual(await search_sites("shop", "Iran", 1), [])

        nonempty = httpx.Response(200, json={"results": [{"url": "https://www.example.org/contact?utm_source=x"}]}, request=request)
        with patch("app.main.httpx.AsyncClient", return_value=FakeClient(nonempty)):
            sites = await search_sites("shop", "Iran", 1)
        self.assertEqual(len(sites), 1)
        self.assertEqual(sites[0]["url"], "https://example.org/contact")

    async def test_duplicate_phone_across_websites_is_only_returned_once(self):
        search = AsyncMock(return_value=[self.site("a.ir"), self.site("b.ir")])

        async def process(_client, site, _semaphore):
            return {"website_url": site["website_url"], "phones": {"09123456789": site["url"]}}

        response = await run_discovery("shop", "Iran", 2, search, process)
        self.assertEqual(response["successful_websites"], 1)
        self.assertEqual(response["total_unique_mobile_numbers"], 1)


if __name__ == "__main__":
    unittest.main()
