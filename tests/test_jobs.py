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
            await page_callback(start_page + 1, fresh[:needed], not fresh)
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
            self.store, search_fn=search, query_fn=main.search_query_for_round, process_fn=process,
            normalize_phone_fn=main.normalize_mobile, max_concurrent_sites=2,
        )
        await self.manager.start()

    async def asyncTearDown(self):
        await self.manager.stop()
        self.temp.cleanup()

    async def wait_terminal(self, job_id, timeout=3):
        end = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < end:
            state = self.manager.status(job_id)
            if state["status"] in {"completed", "failed"}:
                return state
            await asyncio.sleep(.01)
        self.fail("Job did not reach a terminal state")

    async def test_flat_results_result_found_order_and_exact_source(self):
        job, created = self.manager.submit("کالا", "ایران", 3)
        self.assertTrue(created)
        final = await self.wait_terminal(job["job_id"])
        self.assertEqual(final["schema_version"], 2)
        self.assertEqual(final["successful_results"], 3)
        self.assertEqual(final["total_unique_mobile_numbers"], 3)
        self.assertEqual(len(final["results"]), 3)
        self.assertTrue(all(set(item) == {"number", "source_url", "website_url", "page_title", "page_description"} for item in final["results"]))
        events = self.store.events_after(job["job_id"], 0)
        ids = [item["id"] for item in events]
        self.assertEqual(ids, list(range(1, len(ids) + 1)))
        result_events = [item for item in events if item["event"] == "result_found"]
        self.assertEqual(len(result_events), 3)
        self.assertEqual([item["data"]["result"] for item in result_events], final["results"])
        self.assertEqual(events[-1]["event"], "job_completed")

    async def test_same_domain_candidates_are_processed_independently_and_target_not_exceeded(self):
        job, _ = self.manager.submit("کالا", "ایران", 3)
        final = await self.wait_terminal(job["job_id"])
        self.assertEqual(final["successful_results"], 3)
        self.assertLessEqual(final["successful_results"], final["requested_count"])
        self.assertEqual(final["processed_page_count"], 2)
        self.assertEqual(final["successful_page_count"], 2)
        self.assertEqual(final["discovered_page_count"], 3)
        self.assertEqual(final["results"][0]["source_url"], "https://market.ir/listing/1")
        self.assertEqual(final["results"][2]["source_url"], "https://market.ir/listing/2")
        self.assertEqual(self.calls[0]["needed"], 5)

    async def test_result_is_durable_before_slow_page_finishes_without_sse_reader(self):
        slow_started = asyncio.Event()
        release_slow = asyncio.Event()
        async def search(keyword, province, needed, excluded_urls, round_number, start_page, initial_sites, page_callback, max_pages):
            pages = [candidate("https://same.ir/fast"), candidate("https://same.ir/slow")]
            await page_callback(start_page + 1, pages, False)
            return pages
        async def process(page, _semaphore):
            if page["url"].endswith("slow"):
                slow_started.set()
                await release_slow.wait()
            number = "09123456789" if page["url"].endswith("fast") else "09351234567"
            return {"pages": [extracted_page(page["url"], [number])]}
        self.manager.search_fn, self.manager.process_fn = search, process
        job, _ = self.manager.submit("shop", "Iran", 2)
        await asyncio.wait_for(slow_started.wait(), 1)
        for _ in range(100):
            if self.manager.status(job["job_id"])["successful_results"]:
                break
            await asyncio.sleep(.01)
        state = self.manager.status(job["job_id"])
        self.assertEqual(state["successful_results"], 1)
        self.assertEqual(state["results"][0]["source_url"], "https://same.ir/fast")
        release_slow.set()
        await self.wait_terminal(job["job_id"])

    async def test_duplicate_number_keeps_first_url_and_number_target_caps_page(self):
        async def search(keyword, province, needed, excluded_urls, round_number, start_page, initial_sites, page_callback, max_pages):
            pages = [candidate("https://market.ir/one"), candidate("https://other.ir/two")]
            await page_callback(start_page + 1, pages, False)
            return pages
        async def process(page, _semaphore):
            nums = ["09123456789", "09351234567"] if page["url"].endswith("one") else ["09123456789", "09111111111"]
            return {"pages": [extracted_page(page["url"], nums)]}
        self.manager.search_fn, self.manager.process_fn = search, process
        job, _ = self.manager.submit("shop", "Iran", 3)
        final = await self.wait_terminal(job["job_id"])
        self.assertEqual([item["number"] for item in final["results"]], ["09123456789", "09351234567", "09111111111"])
        self.assertEqual(final["results"][0]["source_url"], "https://market.ir/one")
        self.assertEqual(len([event for event in self.store.events_after(job["job_id"], 0) if event["event"] == "result_found"]), 3)

    async def test_one_candidate_crawl_can_emit_flat_results_from_multiple_actual_pages(self):
        candidate_url = "https://market.ir/search/category"
        async def search(keyword, province, needed, excluded_urls, round_number, start_page, initial_sites, page_callback, max_pages):
            pages = [candidate(candidate_url)]
            await page_callback(start_page + 1, pages, False)
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

    async def test_homepage_is_valid_source_when_that_is_the_extracted_page(self):
        page_url = "https://example.ir/"
        async def search(keyword, province, needed, excluded_urls, round_number, start_page, initial_sites, page_callback, max_pages):
            pages = [candidate(page_url)]
            await page_callback(start_page + 1, pages, False)
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
            await page_callback(start_page + 1, pages, False)
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
        async def broken_search(*args, **kwargs):
            raise RuntimeError("SearXNG unavailable")
        self.manager.search_fn = broken_search
        job, _ = self.manager.submit("shop", "Iran", 1)
        final = await self.wait_terminal(job["job_id"])
        self.assertEqual(final["status"], "failed")
        self.assertTrue(any("SearXNG unavailable" in error["message"] for error in final["errors"]))

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
        self.assertEqual(len(result_events), 2)
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
        self.assertEqual(migrated["requested_count"], 2)
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
            await page_callback(start_page + 1, pages, False)
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
                    self.assertIn("event: result_found", sse.text)
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
                        if "event: result_found" not in row:
                            continue
                        data_line = next(line[6:] for line in row.splitlines() if line.startswith("data: "))
                        event_records.append(json.loads(data_line)["result"])
                    self.assertEqual(event_records, result.json()["results"])
                    self.assertEqual(sse.text.split("event: ")[-1].splitlines()[0], "job_completed")
                    result_row = next(row for row in sse.text.split("\n\n") if "event: result_found" in row)
                    result_payload = json.loads(next(line[6:] for line in result_row.splitlines() if line.startswith("data: ")))
                    self.assertEqual(result_payload["schema_version"], 2)
                    self.assertEqual(result_payload["result"]["source_url"], "https://divar.ir/v/real-listing")
                    unauthorized = await client.get(job["status_url"])
                    self.assertEqual(unauthorized.status_code, 401)
        finally:
            await manager.stop()
            temp.cleanup()


if __name__ == "__main__":
    unittest.main()
