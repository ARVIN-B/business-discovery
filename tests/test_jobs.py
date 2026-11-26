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


class PaginationTests(unittest.IsolatedAsyncioTestCase):
    async def test_continues_after_consumed_page_and_skips_duplicate_domains(self):
        requested_pages = []

        class FakeClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

            async def get(self, _url, params):
                page = params["pageno"]
                requested_pages.append(page)
                if page == 11:
                    entries = [
                        {"url": "https://already-seen.ir/page", "title": "duplicate"},
                        {"url": "invalid-url"},
                    ]
                elif page == 12:
                    entries = [{"url": "https://new-site.ir/contact", "title": "new"}]
                else:
                    entries = []
                return httpx.Response(200, json={"results": entries}, request=httpx.Request("GET", _url))

        checkpoints = []

        async def checkpoint(next_page, new_sites, exhausted):
            checkpoints.append((next_page, [site["website_url"] for site in new_sites], exhausted))

        with patch("app.main.httpx.AsyncClient", return_value=FakeClient()):
            sites = await main.search_sites(
                "کافه", "اصفهان", 1,
                excluded_domains={"already-seen.ir"},
                round_number=2,
                start_page=11,
                page_callback=checkpoint,
                max_pages=20,
            )
        self.assertEqual(requested_pages, [11, 12])
        self.assertEqual([site["website_url"] for site in sites], ["https://new-site.ir/"])
        self.assertEqual(checkpoints[0], (12, [], False))
        self.assertEqual(checkpoints[1][0], 13)

    async def test_round_queries_have_independent_pagination_sequences(self):
        requests = []

        class FakeClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

            async def get(self, url, params):
                requests.append((params["q"], params["pageno"]))
                return httpx.Response(200, json={"results": [{"url": f"https://site-{len(requests)}.ir"}]}, request=httpx.Request("GET", url))

        with patch("app.main.httpx.AsyncClient", return_value=FakeClient()):
            await main.search_sites("کافه", "اصفهان", 1, round_number=1, start_page=11, max_pages=20)
            await main.search_sites("کافه", "اصفهان", 1, round_number=2, start_page=1, max_pages=20)
        self.assertEqual(requests[0][1], 11)
        self.assertEqual(requests[1][1], 1)
        self.assertNotEqual(requests[0][0], requests[1][0])


class JobManagerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = JobStore(str(Path(self.temp.name) / "jobs.sqlite3"), retention_seconds=3600)
        self.calls = []

        async def search(keyword, province, needed, excluded_domains, round_number, start_page, initial_sites, page_callback, max_pages):
            self.calls.append({"round": round_number, "needed": needed, "start_page": start_page, "excluded": set(excluded_domains)})
            if round_number == 1:
                candidates = [self.site("a.ir"), self.site("b.ir")]
            elif round_number == 2:
                candidates = [self.site("a.ir"), self.site("c.ir")]
            else:
                candidates = [self.site("d.ir")]
            already = {site["website_url"] for site in initial_sites}
            fresh = [site for site in candidates if site["website_url"] not in already and site["website_url"].split("//", 1)[1].strip("/") not in excluded_domains]
            await page_callback(start_page + 1, fresh[:needed], not fresh)
            return (initial_sites + fresh)[:needed]

        async def process(site, _semaphore):
            host = site["website_url"].split("//", 1)[1].strip("/")
            number = {"a.ir": "09123456789", "b.ir": "09123456789", "c.ir": "09351234567", "d.ir": "09121112233"}.get(host)
            return {"website_url": site["website_url"], "phones": ({number: site["url"]} if number else {})}

        self.manager = JobManager(
            self.store,
            search_fn=search,
            query_fn=main.search_query_for_round,
            process_fn=process,
            normalize_phone_fn=main.normalize_mobile,
            max_concurrent_sites=2,
        )
        await self.manager.start()

    async def asyncTearDown(self):
        await self.manager.stop()
        self.temp.cleanup()

    @staticmethod
    def site(domain):
        return {"url": f"https://{domain}/contact", "website_url": f"https://{domain}/", "title": domain}

    async def wait_terminal(self, job_id, timeout=3):
        end = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < end:
            state = self.manager.status(job_id)
            if state["status"] in {"completed", "failed"}:
                return state
            await asyncio.sleep(0.01)
        self.fail("Job did not reach a terminal state")

    async def test_job_state_events_incrementally_persist_and_replay(self):
        job, created = self.manager.submit("رستوران", "اصفهان", 1)
        self.assertTrue(created)
        state = await self.wait_terminal(job["job_id"])
        self.assertEqual(state["status"], "completed")
        self.assertEqual(state["successful_websites"], 1)
        self.assertEqual(state["total_unique_mobile_numbers"], 1)
        events = self.store.events_after(job["job_id"], 0)
        ids = [event["id"] for event in events]
        self.assertEqual(ids, list(range(1, len(ids) + 1)))
        names = [event["event"] for event in events]
        self.assertEqual(names[0], "job_started")
        self.assertLess(names.index("round_started"), names.index("search_completed"))
        self.assertLess(names.index("search_completed"), names.index("website_started"))
        self.assertLess(names.index("website_started"), names.index("website_completed"))
        self.assertIn("progress", names)
        self.assertEqual(names[-1], "job_completed")
        completed = [event for event in events if event["event"] == "website_completed"]
        self.assertIn("result", completed[0]["data"])
        replay = self.store.events_after(job["job_id"], ids[1])
        self.assertEqual([event["id"] for event in replay], ids[2:])

    async def test_result_is_available_before_other_website_finishes_and_worker_ignores_disconnect(self):
        second_started = asyncio.Event()
        release_second = asyncio.Event()

        async def search(keyword, province, needed, excluded_domains, round_number, start_page, initial_sites, page_callback, max_pages):
            sites = [self.site("first.ir"), self.site("second.ir")]
            await page_callback(start_page + 1, sites, False)
            return sites

        async def process(site, _semaphore):
            if "second.ir" in site["url"]:
                second_started.set()
                await release_second.wait()
            return {"website_url": site["website_url"], "phones": {"09123456789": site["url"]}}

        self.manager.search_fn = search
        self.manager.process_fn = process
        self.manager.max_concurrent_sites = 2
        self.manager.site_semaphore = asyncio.Semaphore(2)
        job, _ = self.manager.submit("shop", "iran", 2)
        await asyncio.wait_for(second_started.wait(), timeout=1)
        # No SSE reader is connected; the completed first result must still be durable.
        end = asyncio.get_running_loop().time() + 1
        while asyncio.get_running_loop().time() < end:
            state = self.manager.status(job["job_id"])
            if state["successful_websites"] >= 1:
                break
            await asyncio.sleep(0.01)
        state = self.manager.status(job["job_id"])
        self.assertEqual(state["successful_websites"], 1)
        self.assertEqual(state["results"][0]["website_url"], "https://first.ir/")
        release_second.set()
        terminal = await self.wait_terminal(job["job_id"])
        self.assertEqual(terminal["status"], "completed")

    async def test_unexpected_exception_marks_job_failed_and_persists_it(self):
        async def broken_search(*args, **kwargs):
            raise ValueError("unexpected provider response")

        self.manager.search_fn = broken_search
        job, _ = self.manager.submit("shop", "Iran", 1)
        state = await self.wait_terminal(job["job_id"])
        self.assertEqual(state["status"], "failed")
        events = self.store.events_after(job["job_id"], 0)
        self.assertEqual(events[-1]["event"], "job_failed")
        self.assertIn("unexpected provider response", state["errors"][0]["message"])

    async def test_site_failure_is_recorded_and_other_sites_continue(self):
        async def search(keyword, province, needed, excluded_domains, round_number, start_page, initial_sites, page_callback, max_pages):
            sites = [self.site("bad.ir"), self.site("good.ir")]
            await page_callback(start_page + 1, sites, False)
            return sites

        async def process(site, _semaphore):
            if "bad.ir" in site["url"]:
                await asyncio.sleep(0.005)
                return {"website_url": site["website_url"], "phones": {}, "error": "Firecrawl timed out"}
            await asyncio.sleep(0.02)
            return {"website_url": site["website_url"], "phones": {"09123456789": "https://good.ir/contact"}}

        self.manager.search_fn = search
        self.manager.process_fn = process
        job, _ = self.manager.submit("shop", "Iran", 1)
        state = await self.wait_terminal(job["job_id"])
        self.assertEqual(state["status"], "completed")
        self.assertEqual(state["successful_websites"], 1)
        self.assertTrue(any(error["website_url"] == "https://bad.ir/" for error in state["errors"]))
        self.assertEqual(state["website_statuses"]["https://bad.ir/"]["status"], "failed")
        self.assertEqual(state["website_statuses"]["https://good.ir/"]["status"], "completed")

    async def test_idempotency_prevents_duplicate_jobs_and_conflicts_on_different_body(self):
        first, created = self.manager.submit("shop", "Iran", 1, idempotency_key="request-123")
        second, created_again = self.manager.submit("shop", "Iran", 1, idempotency_key="request-123")
        self.assertTrue(created)
        self.assertFalse(created_again)
        self.assertEqual(first["job_id"], second["job_id"])
        with self.assertRaises(IdempotencyConflict):
            self.manager.submit("cafe", "Iran", 1, idempotency_key="request-123")
        await self.wait_terminal(first["job_id"])

    async def test_recovery_after_reopening_database_resumes_saved_job_state(self):
        job_id = "recovery-job"
        state = {
            "job_id": job_id, "status": "running", "keyword": "shop", "province": "Iran", "requested_count": 1,
            "current_round": 1, "total_rounds": 3, "round_url_target": 2, "discovered_in_round": 1,
            "processed_in_round": 0, "remaining_in_round": 1, "successful_websites": 0,
            "total_unique_mobile_numbers": 0, "current_stage": "processing", "website_statuses": {
                "https://resume.ir/": {"status": "processing", "round": 1, "error": None, "mobile_numbers_added": 0}
            }, "errors": [], "warnings": [], "results": [], "processed_domains": ["resume.ir"],
            "registered_numbers": [], "pagination": {"shop Iran ایران": {"next_page": 4, "exhausted": False}},
            "round_query": "shop Iran ایران", "round_sites": [self.site("resume.ir")],
            "round_search_complete": True, "stop_after_round": False, "fail_after_round": False,
        }
        self.store.create(job_id, state, "hash", None)
        self.store.claim(job_id)
        running = self.store.get(job_id)["state"]
        running["current_stage"] = "processing"
        self.store.save(job_id, running)
        reopened = JobStore(self.store.path, retention_seconds=3600)
        recovered = reopened.recover()
        self.assertIn(job_id, recovered)
        self.assertEqual(reopened.get(job_id)["state"]["website_statuses"]["https://resume.ir/"]["status"], "pending")
        self.assertEqual(reopened.get(job_id)["state"]["pagination"]["shop Iran ایران"]["next_page"], 4)
        self.assertEqual(reopened.get(job_id)["state"]["resume_stage"], "processing")

        await self.manager.stop()
        self.manager.store = reopened
        self.manager.search_fn = AsyncMock(side_effect=AssertionError("resumed round must not search again"))

        async def resumed_process(site, _semaphore):
            return {"website_url": site["website_url"], "phones": {"09123456789": "https://resume.ir/contact"}}

        self.manager.process_fn = resumed_process
        await self.manager.start()
        resumed = await self.wait_terminal(job_id)
        self.assertEqual(resumed["status"], "completed")
        self.assertEqual(resumed["results"][0]["website_url"], "https://resume.ir/")
        self.assertEqual(self.manager.store.get(job_id)["state"]["pagination"]["shop Iran ایران"]["next_page"], 4)

    async def test_second_manager_cannot_claim_database_while_first_is_running(self):
        second = JobManager(self.store, self.manager.search_fn, self.manager.query_fn, self.manager.process_fn, self.manager.normalize_phone_fn)
        with self.assertRaisesRegex(RuntimeError, "already owns"):
            await second.start()


class SSEEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_sse_event_ids_format_and_reconnect_endpoint(self):
        temp = tempfile.TemporaryDirectory()
        store = JobStore(str(Path(temp.name) / "jobs.sqlite3"))
        manager = JobManager(
            store,
            search_fn=self.search,
            query_fn=main.search_query_for_round,
            process_fn=self.process,
            normalize_phone_fn=main.normalize_mobile,
            max_concurrent_sites=1,
        )
        await manager.start()
        main.app.state.job_manager = manager
        try:
            transport = httpx.ASGITransport(app=main.app)
            with patch.object(main, "API_KEY", "test-token-that-is-long-enough-123456"):
                async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                    created = await client.post(
                        "/v1/discover", json={"keyword": "shop", "province": "Iran", "count": 1},
                        headers={"Authorization": "Bearer test-token-that-is-long-enough-123456"},
                    )
                    self.assertEqual(created.status_code, 202)
                    payload = created.json()
                    job_id = payload["job_id"]
                    status = await client.get(payload["status_url"], headers={"Authorization": "Bearer test-token-that-is-long-enough-123456"})
                    self.assertEqual(status.status_code, 200)
                    final = await client.get(payload["result_url"], headers={"Authorization": "Bearer test-token-that-is-long-enough-123456"})
                    while final.status_code == 409:
                        await asyncio.sleep(0.01)
                        final = await client.get(payload["result_url"], headers={"Authorization": "Bearer test-token-that-is-long-enough-123456"})
                    self.assertEqual(final.status_code, 200)
                    all_events = await client.get(payload["events_url"], headers={"Authorization": "Bearer test-token-that-is-long-enough-123456"})
                    self.assertEqual(all_events.status_code, 200)
                    self.assertIn("text/event-stream", all_events.headers["content-type"])
                    ids = [int(line[4:]) for line in all_events.text.splitlines() if line.startswith("id: ")]
                    self.assertEqual(ids, sorted(ids))
                    self.assertEqual(len(ids), len(set(ids)))
                    replay = await client.get(
                        payload["events_url"],
                        headers={"Authorization": "Bearer test-token-that-is-long-enough-123456", "Last-Event-ID": str(ids[0])},
                    )
                    replay_ids = [int(line[4:]) for line in replay.text.splitlines() if line.startswith("id: ")]
                    self.assertTrue(all(event_id > ids[0] for event_id in replay_ids))
                    self.assertEqual(final.json()["successful_websites"], 1)
        finally:
            await manager.stop()
            temp.cleanup()

    @staticmethod
    async def search(keyword, province, needed, excluded_domains, round_number, start_page, initial_sites, page_callback, max_pages):
        sites = [{"url": "https://sse.ir/", "website_url": "https://sse.ir/", "title": "SSE"}]
        await page_callback(start_page + 1, sites, False)
        return sites

    @staticmethod
    async def process(site, _semaphore):
        await asyncio.sleep(0.02)
        return {"website_url": site["website_url"], "phones": {"09123456789": "https://sse.ir/contact"}}


if __name__ == "__main__":
    unittest.main()
