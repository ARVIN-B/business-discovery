import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

from app import main
from app.job_manager import IdempotencyConflict, JobManager
from app.job_store import JobStore


def candidate(url):
    from urllib.parse import urlsplit
    parsed = urlsplit(url)
    return {"url": url, "website_url": f"{parsed.scheme}://{parsed.netloc}/"}


def extracted_page(url, numbers, title="", description=""):
    page = candidate(url)
    return {"source_url": url, "website_url": page["website_url"], "page_title": title,
            "page_description": description, "numbers": numbers}


class PaginationTests(unittest.IsolatedAsyncioTestCase):
    async def test_unbounded_page_mode_continues_past_legacy_fifty_page_limit(self):
        requested = []
        class Client:
            async def __aenter__(self): return self
            async def __aexit__(self, *args): return None
            async def get(self, url, params):
                page = params["pageno"]
                requested.append(page)
                entries = [] if page == 53 else [{"url": f"https://pages.ir/{page}"}]
                return httpx.Response(200, json={"results": entries}, request=httpx.Request("GET", url))
        with patch("app.main.httpx.AsyncClient", return_value=Client()):
            pages = await main.search_sites("shop", "Iran", 1000, max_pages=0)
        self.assertEqual(len(pages), 52)
        self.assertEqual(requested, list(range(1, 54)))

    async def test_continues_after_consumed_page_and_skips_exact_url_only(self):
        requested_pages = []
        class Client:
            async def __aenter__(self): return self
            async def __aexit__(self, *args): return None
            async def get(self, url, params):
                page = params["pageno"]
                requested_pages.append(page)
                entries = ([
                    {"url": "https://market.ir/listing/processed"},
                    {"url": "https://market.ir/listing/new"},
                ] if page == 11 else [])
                return httpx.Response(200, json={"results": entries}, request=httpx.Request("GET", url))
        checkpoints = []
        async def checkpoint(next_page, pages, exhausted):
            checkpoints.append((next_page, [item["url"] for item in pages], exhausted))
        with patch("app.main.httpx.AsyncClient", return_value=Client()):
            pages = await main.search_sites(
                "کالا", "ایران", 1, excluded_urls={"https://market.ir/listing/processed"},
                round_number=2, start_page=11, page_callback=checkpoint, max_pages=20,
            )
        self.assertEqual(requested_pages, [11])
        self.assertEqual([item["url"] for item in pages], ["https://market.ir/listing/new"])
        self.assertEqual(checkpoints[0][1], ["https://market.ir/listing/new"])

    async def test_different_queries_keep_independent_page_sequences(self):
        requests = []
        class Client:
            async def __aenter__(self): return self
            async def __aexit__(self, *args): return None
            async def get(self, url, params):
                requests.append((params["q"], params["pageno"]))
                return httpx.Response(200, json={"results": [{"url": f"https://market.ir/p/{len(requests)}"}]}, request=httpx.Request("GET", url))
        with patch("app.main.httpx.AsyncClient", return_value=Client()):
            await main.search_sites("کالا", "ایران", 1, round_number=1, start_page=11, max_pages=20)
            await main.search_sites("کالا", "ایران", 1, round_number=2, start_page=1, max_pages=20)
        self.assertEqual([item[1] for item in requests], [11, 1])
        self.assertNotEqual(requests[0][0], requests[1][0])


class JobManagerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = JobStore(str(Path(self.temp.name) / "jobs.sqlite3"), retention_seconds=3600)
        self.calls = []
        async def search(keyword, province, needed, excluded_urls, round_number, start_page, initial_sites, page_callback, max_pages):
            self.calls.append({"round": round_number, "needed": needed, "start_page": start_page, "excluded": set(excluded_urls)})
            if round_number == 1:
                pages = [candidate("https://market.ir/listing/1"), candidate("https://market.ir/listing/2"), candidate("https://market.ir/listing/3")]
            elif round_number == 2:
                pages = [candidate("https://market.ir/listing/2"), candidate("https://market.ir/listing/4")]
            else:
                pages = [candidate("https://other.ir/listing/5")]
            initial = {page["url"] for page in initial_sites}
            fresh = [page for page in pages if page["url"] not in initial and page["url"] not in excluded_urls]
            await page_callback(start_page + 1, fresh, not fresh)
            return (initial_sites + fresh)[:needed]
        async def process(page, _semaphore):
            numbers = {
                "https://market.ir/listing/1": ["09123456789", "09351234567"],
                "https://market.ir/listing/2": ["09123456789", "09111111111"],
                "https://market.ir/listing/3": ["09444444444"],
                "https://market.ir/listing/4": ["09222222222"],
                "https://other.ir/listing/5": ["09333333333"],
            }.get(page["url"], [])
            return {"page_url": page["url"], "pages": [extracted_page(page["url"], numbers, "Real title", "Real description")]}
        self.manager = JobManager(
            self.store, search_fn=search, query_fn=lambda _keyword, _province, _index: "test query", process_fn=process,
            normalize_phone_fn=main.normalize_mobile, max_concurrent_sites=2,
            search_batch_size=2, search_retry_seconds=1,
        )
        await self.manager.start()
        self.auto_approve = True
        self.approver = asyncio.create_task(self._approve_jobs())

    async def _approve_jobs(self):
        while True:
            for job_id in list(self.manager.tasks):
                status = self.manager.status(job_id)
                if status and status.get("pending_decision_id") and self.auto_approve:
                    self.manager.decide(job_id, status["pending_decision_id"], True)
            await asyncio.sleep(.005)

    async def asyncTearDown(self):
        self.approver.cancel()
        await asyncio.gather(self.approver, return_exceptions=True)
        await self.manager.stop()
        self.temp.cleanup()

    async def wait_terminal(self, job_id, timeout=3):
        end = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < end:
            state = self.manager.status(job_id)
            if state["status"] in {"completed", "failed", "stopped"}:
                return state
            await asyncio.sleep(.01)
        self.fail("Job did not reach a terminal state")

    async def wait_approval(self, job_id, timeout=2):
        end = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < end:
            state = self.manager.status(job_id)
            if state and state.get("pending_decision_id"):
                return state
            await asyncio.sleep(.005)
        self.fail("Job did not request an approval")

    async def test_flat_results_are_published_atomically_per_url(self):
        job, created = self.manager.submit("کالا", "ایران", 3)
        self.assertTrue(created)
        final = await self.wait_terminal(job["job_id"])
        self.assertEqual(final["schema_version"], 2)
        self.assertEqual(final["successful_results"], 4)
        self.assertEqual(final["total_unique_mobile_numbers"], 4)
        self.assertEqual(len(final["results"]), 4)
        self.assertTrue(all(set(item) == {"number", "source_url", "website_url", "page_title", "page_description"} for item in final["results"]))
        events = self.store.events_after(job["job_id"], 0)
        ids = [item["id"] for item in events]
        self.assertEqual(ids, list(range(1, len(ids) + 1)))
        result_events = [item for item in events if item["event"] == "url_results_found"]
        self.assertEqual(len(result_events), 3)
        self.assertEqual([record for item in result_events for record in item["data"]["results"]], final["results"])
        self.assertTrue(all(item["data"]["candidate_url"] for item in result_events))
        self.assertEqual(events[-1]["event"], "job_completed")

    async def test_count_is_informational_and_same_domain_urls_are_independent(self):
        job, _ = self.manager.submit("کالا", "ایران", 3)
        final = await self.wait_terminal(job["job_id"])
        self.assertEqual(final["successful_results"], 4)
        self.assertEqual(final["processed_page_count"], 3)
        self.assertEqual(final["successful_page_count"], 3)
        self.assertEqual(final["discovered_page_count"], 3)
        self.assertEqual(final["results"][0]["source_url"], "https://market.ir/listing/1")
        self.assertEqual(final["results"][2]["source_url"], "https://market.ir/listing/2")
        self.assertEqual(self.calls[0]["needed"], 2)

    async def test_url_processing_is_sequential_and_each_url_waits_for_approval(self):
        slow_started = asyncio.Event()
        release_slow = asyncio.Event()
        started = []
        async def search(keyword, province, needed, excluded_urls, round_number, start_page, initial_sites, page_callback, max_pages):
            pages = [candidate("https://same.ir/fast"), candidate("https://same.ir/slow")]
            await page_callback(start_page + 1, pages, True)
            return pages
        async def process(page, _semaphore):
            started.append(page["url"])
            if page["url"].endswith("slow"):
                slow_started.set()
                await release_slow.wait()
            number = "09123456789" if page["url"].endswith("fast") else "09351234567"
            return {"pages": [extracted_page(page["url"], [number])]}
        self.manager.search_fn, self.manager.process_fn = search, process
        self.auto_approve = False
        job, _ = self.manager.submit("shop", "Iran", 1)
        first = await self.wait_approval(job["job_id"])
        self.assertEqual(started, ["https://same.ir/fast"])
        self.assertEqual(first["successful_results"], 1)
        self.assertEqual(first["results"][0]["source_url"], "https://same.ir/fast")
        self.manager.decide(job["job_id"], first["pending_decision_id"], True)
        await asyncio.wait_for(slow_started.wait(), 1)
        self.assertEqual(started, ["https://same.ir/fast", "https://same.ir/slow"])
        release_slow.set()
        second = await self.wait_approval(job["job_id"])
        self.manager.decide(job["job_id"], second["pending_decision_id"], True)
        final = await self.wait_terminal(job["job_id"])
        self.assertEqual(final["successful_results"], 2)
        self.assertEqual({record["number"] for record in final["results"]}, {"09123456789", "09351234567"})

    async def test_search_batch_fills_before_first_crawl_starts(self):
        first_page_saved = asyncio.Event()
        allow_second_page = asyncio.Event()
        crawled = []
        async def search(keyword, province, needed, excluded_urls, round_number, start_page, initial_sites, page_callback, max_pages):
            if start_page == 1:
                await page_callback(2, [candidate("https://batch.ir/1")], False)
                first_page_saved.set()
                await allow_second_page.wait()
                await page_callback(3, [candidate("https://batch.ir/2")], False)
            else:
                await page_callback(start_page + 1, [], True)
            return []
        async def process(page, _semaphore):
            crawled.append(page["url"])
            return {"pages": []}
        self.manager.search_fn, self.manager.process_fn = search, process
        self.auto_approve = False
        job, _ = self.manager.submit("shop", "Iran", None)
        await asyncio.wait_for(first_page_saved.wait(), 1)
        await asyncio.sleep(.02)
        self.assertEqual(crawled, [])
        allow_second_page.set()
        waiting = await self.wait_approval(job["job_id"])
        self.assertEqual(crawled, ["https://batch.ir/1"])
        completed = [event for event in self.store.events_after(job["job_id"], 0) if event["event"] == "url_completed"]
        self.assertEqual(len(completed), 1)
        self.assertEqual(completed[0]["data"]["results"], [])
        self.assertEqual(completed[0]["data"]["decision_id"], waiting["pending_decision_id"])

    async def test_next_batch_uses_saved_cursor_and_never_queues_duplicate_urls(self):
        pages_requested = []
        crawled = []
        async def search(keyword, province, needed, excluded_urls, round_number, start_page, initial_sites, page_callback, max_pages):
            pages_requested.append(start_page)
            if start_page == 1:
                pages = [candidate(f"https://cursor.ir/{index}") for index in (1, 2, 3)]
                await page_callback(2, pages, False)
            else:
                await page_callback(3, [candidate("https://cursor.ir/1"), candidate("https://cursor.ir/4")], False)
            return []
        async def process(page, _semaphore):
            crawled.append(page["url"])
            return {"pages": []}
        self.manager.search_fn, self.manager.process_fn = search, process
        self.auto_approve = False
        job, _ = self.manager.submit("shop", "Iran", None)
        for expected_url in ("https://cursor.ir/1", "https://cursor.ir/2", "https://cursor.ir/3"):
            waiting = await self.wait_approval(job["job_id"])
            self.assertEqual(crawled[-1], expected_url)
            self.manager.decide(job["job_id"], waiting["pending_decision_id"], True)
        waiting = await self.wait_approval(job["job_id"])
        self.assertEqual(crawled, ["https://cursor.ir/1", "https://cursor.ir/2", "https://cursor.ir/3", "https://cursor.ir/4"])
        self.assertEqual(pages_requested[:2], [1, 2])
        self.assertEqual(waiting["seen_url_count"], 4)
        self.manager.decide(job["job_id"], waiting["pending_decision_id"], False)

    async def test_duplicate_number_keeps_first_url_and_number_target_caps_page(self):
        async def search(keyword, province, needed, excluded_urls, round_number, start_page, initial_sites, page_callback, max_pages):
            pages = [candidate("https://market.ir/one"), candidate("https://other.ir/two")]
            await page_callback(start_page + 1, pages, True)
            return pages
        async def process(page, _semaphore):
            nums = ["09123456789", "09351234567"] if page["url"].endswith("one") else ["09123456789", "09111111111"]
            return {"pages": [extracted_page(page["url"], nums)]}
        self.manager.search_fn, self.manager.process_fn = search, process
        job, _ = self.manager.submit("shop", "Iran", 3)
        final = await self.wait_terminal(job["job_id"])
        self.assertEqual([item["number"] for item in final["results"]], ["09123456789", "09351234567", "09111111111"])
        self.assertEqual(final["results"][0]["source_url"], "https://market.ir/one")
        self.assertEqual(len([event for event in self.store.events_after(job["job_id"], 0) if event["event"] == "url_results_found"]), 2)

    async def test_one_candidate_crawl_can_emit_flat_results_from_multiple_actual_pages(self):
        candidate_url = "https://market.ir/search/category"
        async def search(keyword, province, needed, excluded_urls, round_number, start_page, initial_sites, page_callback, max_pages):
            pages = [candidate(candidate_url)]
            await page_callback(start_page + 1, pages, True)
            return pages
        async def process(page, _semaphore):
            return {"pages": [
                extracted_page("https://market.ir/listing/real-1", ["09123456789"], "Listing 1", "Description 1"),
                extracted_page("https://market.ir/listing/real-2", ["09351234567"], "Listing 2"),
            ]}
        self.manager.search_fn, self.manager.process_fn = search, process
        job, _ = self.manager.submit("shop", "Iran", 2)
        final = await self.wait_terminal(job["job_id"])
        self.assertEqual([item["source_url"] for item in final["results"]], [
            "https://market.ir/listing/real-1", "https://market.ir/listing/real-2",
        ])
        self.assertEqual([item["page_title"] for item in final["results"]], ["Listing 1", "Listing 2"])
        self.assertEqual(final["successful_page_count"], 1)

    async def test_false_decision_stops_after_publishing_complete_url_results(self):
        started = []
        async def search(keyword, province, needed, excluded_urls, round_number, start_page, initial_sites, page_callback, max_pages):
            pages = [candidate("https://market.ir/listing/overshoot"), candidate("https://market.ir/listing/extra")]
            await page_callback(start_page + 1, pages, True)
            return pages
        async def process(page, _semaphore):
            started.append(page["url"])
            return {"pages": [extracted_page(page["url"], ["09123456789", "09351234567", "09444444444"])]}
        self.manager.search_fn, self.manager.process_fn = search, process
        self.auto_approve = False
        job, _ = self.manager.submit("shop", "Iran", 1)
        waiting = await self.wait_approval(job["job_id"])
        self.assertEqual(waiting["successful_results"], 3)
        self.assertEqual(started, ["https://market.ir/listing/overshoot"])
        decision_id = waiting["pending_decision_id"]
        result = self.manager.decide(job["job_id"], decision_id, False)
        self.assertEqual(result["status"], "stopped")
        final = await self.wait_terminal(job["job_id"])
        self.assertEqual(final["status"], "stopped")
        self.assertEqual(started, ["https://market.ir/listing/overshoot"])

    async def test_restart_while_awaiting_approval_preserves_decision_id(self):
        async def search(keyword, province, needed, excluded_urls, round_number, start_page, initial_sites, page_callback, max_pages):
            pages = [candidate("https://resume.ir/listing/1")]
            await page_callback(start_page + 1, pages, True)
            return pages
        async def process(page, _semaphore):
            return {"pages": [extracted_page(page["url"], ["09123456789"])]}
        self.manager.search_fn, self.manager.process_fn = search, process
        self.auto_approve = False
        job, _ = self.manager.submit("shop", "Iran", None)
        waiting = await self.wait_approval(job["job_id"])
        decision_id = waiting["pending_decision_id"]
        await self.manager.stop()
        reopened = JobStore(self.store.path, retention_seconds=3600)
        self.manager.store = reopened
        await self.manager.start()
        resumed = self.manager.status(job["job_id"])
        self.assertEqual(resumed["current_stage"], "awaiting_approval")
        self.assertEqual(resumed["pending_decision_id"], decision_id)
        self.manager.search_fn = AsyncMock(side_effect=AssertionError("recovery must not search again"))
        self.manager.decide(job["job_id"], decision_id, True)
        final = await self.wait_terminal(job["job_id"])
        self.assertEqual(final["status"], "completed")
        self.assertEqual(final["successful_results"], 1)

    async def test_unlimited_mode_processes_multiple_rounds_without_numeric_target(self):
        async def search(keyword, province, needed, excluded_urls, round_number, start_page, initial_sites, page_callback, max_pages):
            self.assertGreaterEqual(needed, 1)
            self.assertLessEqual(needed, 2)
            page = candidate(f"https://round{round_number}.ir/listing/{round_number}")
            await page_callback(start_page + 1, [page], True)
            return [page]
        async def process(page, _semaphore):
            digit = page["url"].split("//round", 1)[1][0]
            return {"pages": [extracted_page(page["url"], [f"0912345678{digit}"])]}
        self.manager.search_fn, self.manager.process_fn = search, process
        self.manager.query_fn = lambda _keyword, _province, index: f"query-{index}" if index <= 3 else "query-3"
        job, _ = self.manager.submit("shop", "Iran", None)
        self.assertEqual(job["mode"], "approval_controlled")
        final = await self.wait_terminal(job["job_id"])
        self.assertIsNone(final["requested_count"])
        self.assertEqual(final["mode"], "approval_controlled")
        self.assertEqual(final["batch_number"], 2)
        self.assertEqual(final["successful_results"], 3)

    async def test_stop_is_idempotent_and_waits_for_active_url(self):
        entered = asyncio.Event()
        release = asyncio.Event()
        started = []
        async def search(keyword, province, needed, excluded_urls, round_number, start_page, initial_sites, page_callback, max_pages):
            pages = [candidate(f"https://market.ir/{index}") for index in range(3)]
            await page_callback(start_page + 1, pages, True)
            return pages
        async def process(page, _semaphore):
            started.append(page["url"])
            entered.set()
            await release.wait()
            return {"pages": [extracted_page(page["url"], ["09123456789"])]}
        self.manager.search_fn, self.manager.process_fn = search, process
        self.manager.max_concurrent_sites = 1
        job, _ = self.manager.submit("shop", "Iran", None)
        await asyncio.wait_for(entered.wait(), 1)
        first = self.manager.request_stop(job["job_id"])
        second = self.manager.request_stop(job["job_id"])
        self.assertEqual(first["status"], "running")
        self.assertTrue(second["stop_requested"])
        self.assertEqual(self.manager.status(job["job_id"])["status"], "running")
        release.set()
        final = await self.wait_terminal(job["job_id"])
        self.assertEqual(final["status"], "stopped")
        self.assertEqual(final["successful_results"], 1)
        self.assertEqual(len(started), 1)
        event_names = [item["event"] for item in self.store.events_after(job["job_id"], 0)]
        self.assertIn("job_stop_requested", event_names)
        self.assertEqual(event_names[-1], "job_stopped")

    async def test_homepage_is_valid_source_when_that_is_the_extracted_page(self):
        page_url = "https://example.ir/"
        async def search(keyword, province, needed, excluded_urls, round_number, start_page, initial_sites, page_callback, max_pages):
            pages = [candidate(page_url)]
            await page_callback(start_page + 1, pages, True)
            return pages
        async def process(page, _semaphore):
            return {"pages": [extracted_page(page_url, ["09123456789"])]}
        self.manager.search_fn, self.manager.process_fn = search, process
        job, _ = self.manager.submit("shop", "Iran", 1)
        final = await self.wait_terminal(job["job_id"])
        self.assertEqual(final["results"][0]["source_url"], "https://example.ir/")

    async def test_page_failure_is_recorded_and_other_pages_continue(self):
        async def search(keyword, province, needed, excluded_urls, round_number, start_page, initial_sites, page_callback, max_pages):
            pages = [candidate("https://bad.ir/page"), candidate("https://good.ir/page")]
            await page_callback(start_page + 1, pages, True)
            return pages
        async def process(page, _semaphore):
            if "bad.ir" in page["url"]:
                return {"pages": [], "error": "Firecrawl timed out"}
            return {"pages": [extracted_page(page["url"], ["09123456789"])]}
        self.manager.search_fn, self.manager.process_fn = search, process
        job, _ = self.manager.submit("shop", "Iran", 1)
        final = await self.wait_terminal(job["job_id"])
        self.assertEqual(final["status"], "completed")
        self.assertEqual(final["successful_results"], 1)
        self.assertEqual(final["page_statuses"]["https://bad.ir/page"]["status"], "failed")
        self.assertTrue(any(error.get("page_url") == "https://bad.ir/page" for error in final["errors"]))

    async def test_searx_failure_is_recorded_without_erasing_result_state(self):
        attempts = 0
        async def retry_search(keyword, province, needed, excluded_urls, round_number, start_page, initial_sites, page_callback, max_pages):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                error = RuntimeError("SearXNG unavailable")
                error.retry_after = 0
                raise error
            await page_callback(start_page + 1, [], True)
            return []
        self.manager.search_fn = retry_search
        job, _ = self.manager.submit("shop", "Iran", 1)
        final = await self.wait_terminal(job["job_id"])
        self.assertEqual(final["status"], "completed")
        self.assertGreaterEqual(attempts, 2)
        self.assertTrue(any("SearXNG unavailable" in warning["message"] for warning in final["warnings"]))

    async def test_search_exhaustion_completes_with_warning(self):
        async def empty_search(keyword, province, needed, excluded_urls, round_number, start_page, initial_sites, page_callback, max_pages):
            await page_callback(start_page + 1, [], True)
            return []
        self.manager.search_fn = empty_search
        job, _ = self.manager.submit("shop", "Iran", 2)
        final = await self.wait_terminal(job["job_id"])
        self.assertEqual(final["status"], "completed")
        self.assertEqual(final["successful_results"], 0)
        self.assertTrue(any("exhausted" in warning["message"].lower() for warning in final["warnings"]))

    async def test_unlimited_search_exhaustion_completes_without_fake_target_warning(self):
        async def empty_search(keyword, province, needed, excluded_urls, round_number, start_page, initial_sites, page_callback, max_pages):
            self.assertGreaterEqual(needed, 1)
            await page_callback(start_page + 1, [], True)
            return []
        self.manager.search_fn = empty_search
        job, _ = self.manager.submit("shop", "Iran", None)
        final = await self.wait_terminal(job["job_id"])
        self.assertEqual(final["status"], "completed")
        self.assertEqual(final["mode"], "approval_controlled")
        self.assertFalse(any("requested unique mobile count" in item["message"] for item in final["warnings"]))

    async def test_idempotency_and_request_conflict(self):
        first, created = self.manager.submit("shop", "Iran", 1, idempotency_key="same-request")
        again, created_again = self.manager.submit("shop", "Iran", 1, idempotency_key="same-request")
        self.assertTrue(created)
        self.assertFalse(created_again)
        self.assertEqual(first["job_id"], again["job_id"])
        with self.assertRaises(IdempotencyConflict):
            self.manager.submit("other", "Iran", 1, idempotency_key="same-request")
        await self.wait_terminal(first["job_id"])

    async def test_recovery_reprocess_does_not_duplicate_committed_result_event(self):
        job_id = "resume-job"
        url = "https://market.ir/listing/1"
        state = {
            "schema_version": 2, "job_id": job_id, "status": "queued", "keyword": "shop", "province": "Iran",
            "requested_count": 2, "current_round": 1, "total_rounds": 3, "round_url_target": 4,
            "discovered_page_count": 1, "processed_page_count": 0, "successful_page_count": 1,
            "successful_page_urls": [url], "discovered_in_round": 1, "processed_in_round": 0, "remaining_in_round": 1,
            "current_stage": "processing", "page_statuses": {url: {"status": "processing", "round": 1}},
            "errors": [], "warnings": [], "results": [{
                "number": "09123456789", "source_url": url, "website_url": "https://market.ir/",
                "page_title": "", "page_description": "",
            }],
            "processed_urls": [url], "registered_numbers": ["09123456789"], "pagination": {}, "round_query": "q",
            "round_pages": [candidate(url)], "round_search_complete": True, "search_exhausted": False,
            "stop_after_round": False, "fail_after_round": False,
        }
        self.store.create(job_id, state, "legacyhash", None)
        self.store.claim(job_id)
        running = self.store.get(job_id)["state"]
        running["current_stage"] = "processing"
        self.store.save(job_id, running, event=("result_found", {"result": state["results"][0], "successful_results": 1, "total_unique_mobile_numbers": 1, "requested_count": 2}))
        await self.manager.stop()
        reopened = JobStore(self.store.path, retention_seconds=3600)
        self.manager.store = reopened
        async def search(*args, **kwargs):
            self.fail("resume must not search again")
        async def process(page, _semaphore):
            return {"pages": [extracted_page(url, ["09123456789", "09351234567"])]}
        self.manager.search_fn, self.manager.process_fn = search, process
        await self.manager.start()
        final = await self.wait_terminal(job_id)
        result_events = [item for item in reopened.events_after(job_id, 0) if item["event"] == "result_found"]
        self.assertEqual(final["successful_results"], 2)
        self.assertEqual(len(result_events), 1)
        atomic = [item for item in reopened.events_after(job_id, 0) if item["event"] == "url_results_found"]
        self.assertEqual([record["number"] for item in atomic for record in item["data"]["results"]], ["09351234567"])
        self.assertEqual(reopened.get(job_id)["state"]["page_statuses"][url]["status"], "completed")

    async def test_v1_migration_flattens_history_and_never_reuses_domain_as_url(self):
        job_id = "legacy-job"
        old = {
            "job_id": job_id, "status": "completed", "keyword": "shop", "province": "Iran", "requested_count": 1,
            "current_round": 1, "total_rounds": 3, "round_url_target": 2, "discovered_in_round": 1,
            "processed_in_round": 1, "remaining_in_round": 0, "successful_websites": 1,
            "total_unique_mobile_numbers": 2, "current_stage": "completed",
            "website_statuses": {"https://market.ir/": {"status": "completed", "round": 1}},
            "results": [{"website_url": "https://market.ir/", "mobile_numbers": [
                {"number": "09123456789", "source_url": "https://market.ir/listing/1"},
                {"number": "09351234567", "source_url": "https://market.ir/listing/2"},
            ]}],
            "processed_domains": ["market.ir"], "registered_numbers": ["09123456789", "09351234567"],
            "pagination": {}, "round_query": "q", "round_sites": [candidate("https://market.ir/listing/1")],
            "round_search_complete": True, "errors": [], "warnings": [],
        }
        self.store.create(job_id, old, "legacyhash", None)
        with self.store._db() as connection:
            connection.execute("UPDATE jobs SET status='completed' WHERE job_id=?", (job_id,))
        self.store.initialize()
        migrated = self.store.get(job_id)["state"]
        self.assertEqual(migrated["schema_version"], 2)
        self.assertEqual(len(migrated["results"]), 2)
        self.assertEqual(migrated["requested_count"], 1)
        self.assertNotIn("market.ir", migrated["processed_urls"])
        self.assertEqual(migrated["legacy_processed_domains"], ["market.ir"])
        self.assertEqual(len(migrated["legacy_results"]), 1)
        migrated_events = self.store.events_after(job_id, 0)
        self.assertEqual(len([event for event in migrated_events if event["event"] == "result_found"]), 2)
        self.assertEqual(migrated_events[-1]["event"], "job_completed")
        self.store.initialize()
        self.assertEqual(len([event for event in self.store.events_after(job_id, 0) if event["event"] == "result_found"]), 2)

    async def test_second_manager_cannot_own_same_database(self):
        other = JobManager(self.store, self.manager.search_fn, self.manager.query_fn, self.manager.process_fn, main.normalize_mobile)
        with self.assertRaisesRegex(RuntimeError, "already owns"):
            await other.start()


class SSEEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_api_status_flat_schema_result_event_and_replay(self):
        temp = tempfile.TemporaryDirectory()
        store = JobStore(str(Path(temp.name) / "jobs.sqlite3"))
        async def search(keyword, province, needed, excluded_urls, round_number, start_page, initial_sites, page_callback, max_pages):
            pages = [candidate("https://divar.ir/v/real-listing")]
            await page_callback(start_page + 1, pages, True)
            return pages
        async def process(page, _semaphore):
            await asyncio.sleep(.02)
            return {"pages": [extracted_page(page["url"], ["09123456789"], "Listing title")]}
        manager = JobManager(store, search, main.search_query_for_round, process, main.normalize_mobile, max_concurrent_sites=1)
        await manager.start()
        main.app.state.job_manager = manager
        token = "test-token-that-is-long-enough-123456"
        try:
            with patch.object(main, "API_KEY", token):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://test") as client:
                    headers = {"Authorization": f"Bearer {token}"}
                    created = await client.post("/v1/discover", json={"keyword": "listing", "province": "Iran", "count": 1}, headers=headers)
                    self.assertEqual(created.status_code, 202)
                    job = created.json()
                    self.assertEqual(job["schema_version"], 2)
                    self.assertEqual(job["requested_count"], 1)
                    while True:
                        status = await client.get(job["status_url"], headers=headers)
                        status_json = status.json()
                        if status_json.get("pending_decision_id"):
                            decision_id = status_json["pending_decision_id"]
                            decision_url = f"/v1/jobs/{job['job_id']}/decisions/{decision_id}"
                            malformed = await client.post(decision_url, content='"true"', headers={**headers, "Content-Type": "application/json"})
                            self.assertEqual(malformed.status_code, 422)
                            approved = await client.post(decision_url, content="true", headers={**headers, "Content-Type": "application/json"})
                            self.assertEqual(approved.status_code, 202)
                            repeated = await client.post(decision_url, content="true", headers={**headers, "Content-Type": "application/json"})
                            self.assertEqual(repeated.status_code, 202)
                            conflict = await client.post(decision_url, content="false", headers={**headers, "Content-Type": "application/json"})
                            self.assertEqual(conflict.status_code, 409)
                            continue
                        if status.json()["status"] in {"completed", "failed"}:
                            break
                        await asyncio.sleep(.01)
                    status_json = status.json()
                    self.assertIn("page_statuses", status_json)
                    self.assertNotIn("successful_websites", status_json)
                    result = await client.get(job["result_url"], headers=headers)
                    self.assertEqual(result.status_code, 200)
                    self.assertEqual(result.json()["results"], status_json["results"])
                    sse = await client.get(job["events_url"], headers=headers)
                    self.assertIn("event: url_results_found", sse.text)
                    self.assertIn("event: url_completed", sse.text)
                    self.assertIn("event: page_started", sse.text)
                    self.assertIn("event: page_completed", sse.text)
                    event_ids = [int(line[4:]) for line in sse.text.splitlines() if line.startswith("id: ")]
                    self.assertEqual(event_ids, list(range(1, len(event_ids) + 1)))
                    replay = await client.get(job["events_url"], headers={**headers, "Last-Event-ID": str(event_ids[0])})
                    replay_ids = [int(line[4:]) for line in replay.text.splitlines() if line.startswith("id: ")]
                    self.assertEqual(replay_ids, event_ids[1:])
                    event_records = []
                    rows = sse.text.split("\n\n")
                    for row in rows:
                        if "event: url_results_found" not in row:
                            continue
                        data_line = next(line[6:] for line in row.splitlines() if line.startswith("data: "))
                        event_records.extend(json.loads(data_line)["results"])
                    self.assertEqual(event_records, result.json()["results"])
                    self.assertEqual(sse.text.split("event: ")[-1].splitlines()[0], "job_completed")
                    result_row = next(row for row in sse.text.split("\n\n") if "event: url_results_found" in row)
                    result_payload = json.loads(next(line[6:] for line in result_row.splitlines() if line.startswith("data: ")))
                    self.assertEqual(result_payload["schema_version"], 2)
                    self.assertEqual(result_payload["candidate_url"], "https://divar.ir/v/real-listing")
                    self.assertEqual(result_payload["results"][0]["source_url"], "https://divar.ir/v/real-listing")
                    unauthorized = await client.get(job["status_url"])
                    self.assertEqual(unauthorized.status_code, 401)

                    unlimited = await client.post("/v1/discover", json={"keyword": "listing", "province": "Iran"}, headers=headers)
                    self.assertEqual(unlimited.status_code, 202)
                    self.assertEqual(unlimited.json()["mode"], "approval_controlled")
                    self.assertIsNone(unlimited.json()["requested_count"])
                    self.assertTrue(unlimited.json()["stop_url"].endswith("/stop"))
                    stopped = await client.post(unlimited.json()["stop_url"], headers=headers)
                    self.assertEqual(stopped.status_code, 202)
                    repeated_stop = await client.post(unlimited.json()["stop_url"], headers=headers)
                    self.assertEqual(repeated_stop.status_code, 202)
        finally:
            await manager.stop()
            temp.cleanup()


if __name__ == "__main__":
    unittest.main()
