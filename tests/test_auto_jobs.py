from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from unittest.mock import AsyncMock, patch
from pathlib import Path

from app.job_manager import JobManager
from app.job_store import DecisionConflict, JobStore
from app.memory_guard import MemoryGuard, MemorySnapshot
from app import main


class AutoJobTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = JobStore(str(Path(self.temp.name) / "jobs.sqlite3"))
        self.store.initialize()

    async def asyncTearDown(self):
        if hasattr(self, "manager"):
            try:
                await asyncio.wait_for(self.manager.stop(), timeout=1)
            except asyncio.TimeoutError:
                pass
        self.temp.cleanup()

    async def test_job_automatically_searches_and_crawls_batches_and_true_is_noop(self):
        calls = []

        async def search(keyword, province, needed, excluded_urls, round_number, start_page, initial_sites, page_callback, max_pages):
            urls = [f"https://local.ir/listing/{round_number}-{i}" for i in range(2 if round_number == 1 else 1)]
            candidates = [{"url": url, "website_url": "https://local.ir/", "search_title": "local"} for url in urls]
            await page_callback(start_page + 1, candidates, True, {"raw_result_count": len(candidates), "valid_url_count": len(candidates)})
            calls.append((round_number, start_page))

        async def process(site, job_id, store, guard, stop_requested):
            store.auto_record_number(job_id, site["url"], {
                "number": "09123456789", "source_url": site["url"], "website_url": site["website_url"],
            })
            return {"pages_processed": 1}

        self.manager = JobManager(
            self.store, search, lambda *_: "query", lambda *_: {}, lambda value: value,
            search_batch_size=2, search_queries_fn=lambda *_: ["first", "second"],
            process_incremental_fn=process, memory_guard=MemoryGuard(100000, 1),
        )
        await self.manager.start()
        job, created = self.manager.submit("keyword", "استان", None)
        self.assertTrue(created)
        job_id = job["job_id"]
        for _ in range(200):
            status = self.manager.status(job_id)
            if status and status["status"] in {"completed", "failed", "stopped"}:
                break
            await asyncio.sleep(.01)
        if status["status"] != "completed":
            print("AUTO_DEBUG", status, self.store.get(job_id), calls)
            for task in self.manager.tasks.values():
                task.print_stack()
        self.assertEqual(status["status"], "completed")
        self.assertEqual(status["successful_results"], 1)
        self.assertEqual(calls, [(1, 1), (2, 1)])
        events = self.store.events_after(job_id, 0, 200)
        self.assertEqual(sum(event["event"] == "number_found" for event in events), 1)
        url_event = next(event for event in events if event["event"] == "url_completed")
        result = self.manager.decide(job_id, url_event["data"]["decision_id"], True)
        self.assertTrue(result["compatibility_noop"])
        with self.assertRaises(DecisionConflict):
            self.manager.decide(job_id, "unknown", False)

    async def test_v4_waiting_approval_migrates_and_preserves_old_decision(self):
        state = {
            "schema_version": 2, "job_model_version": 4, "mode": "approval_controlled",
            "keyword": "k", "province": "p", "requested_count": None, "search_batch_size": 100,
            "batch_number": 1, "batch_index": 0, "batch_urls": [{"url": "https://local.ir/a", "website_url": "https://local.ir/"}],
            "url_queue": [], "seen_urls": ["https://local.ir/a"], "current_stage": "awaiting_approval",
            "pending_decision_id": "legacy-id", "decisions": {}, "page_statuses": {"https://local.ir/a": {"status": "completed"}},
            "results": [], "registered_numbers": [], "search_queries": ["q"], "query_index": 0,
            "query_states": {}, "stop_requested": False,
        }
        job_id, _ = self.store.create("legacy-job", state, "hash", None)
        self.store.initialize()
        record = self.store.get(job_id)
        self.assertEqual(record["state"]["job_model_version"], 5)
        self.assertEqual(record["state"]["current_stage"], "searching")
        self.assertTrue(self.store.auto_url_exists(job_id, "https://local.ir/a"))
        decision = self.store.auto_compat_decision(job_id, "legacy-id", True)
        self.assertTrue(decision["compatibility_noop"])

    async def test_incremental_firecrawl_saves_numbers_and_resumes_from_remote_job_id(self):
        job_id = "incremental-crawl"
        state = {
            "schema_version": 3, "job_model_version": 5, "job_id": job_id, "status": "queued",
            "keyword": "کافه", "province": "تهران", "requested_count": None, "mode": "automatic",
            "stop_requested": False, "search_batch_size": 1, "batch_number": 0, "query_index": 0,
            "search_queries": ["query"], "query_states": {}, "search_exhausted": False,
            "current_stage": "searching", "result_count": 0,
        }
        self.store.create(job_id, state, "crawl-hash", None)
        self.store.claim(job_id)
        self.store.auto_begin_batch(job_id)
        self.store.auto_record_search_page(job_id, "query", 2, [{
            "url": "https://local.ir/listing/1", "website_url": "https://local.ir/",
        }], True)
        site = self.store.auto_start_url(job_id)

        class Response:
            def __init__(self, body):
                self.body = body
                self.status_code = 200

            def raise_for_status(self):
                return None

            def json(self):
                return self.body

        client = AsyncMock()
        client.__aenter__.return_value = client
        client.__aexit__.return_value = False
        client.post.return_value = Response({"id": "fc-job-1"})
        client.get.return_value = Response({
            "status": "completed", "data": [{
                "markdown": "تماس 09123456789", "metadata": {"sourceURL": site["url"], "title": "Local listing"},
            }],
        })
        with patch("app.main.httpx.AsyncClient", return_value=client):
            result = await main.process_site_incremental(
                site, job_id, self.store, MemoryGuard(100000, 1), lambda: False,
            )
        self.assertEqual(result["pages_processed"], 1)
        self.assertEqual(self.store.get(job_id)["state"]["result_count"], 1)
        self.assertEqual(client.post.await_count, 1)
        events = self.store.events_after(job_id, 0, 100)
        self.assertTrue(any(event["event"] == "number_found" for event in events))

    async def test_memory_pressure_is_persisted_then_resumed_without_failing(self):
        calls = []

        class FlippingGuard:
            checks = 0

            def snapshot(self):
                self.checks += 1
                pressure = self.checks < 3
                return MemorySnapshot(500 if pressure else 5000, 100, 8192, 7168, 1024,
                                      pressure, "test memory pressure" if pressure else None)

        async def search(*args, **kwargs):
            calls.append(True)
            await kwargs["page_callback"](2, [], True, {"raw_result_count": 0})

        self.manager = JobManager(
            self.store, search, lambda *_: "query", lambda *_: {}, lambda value: value,
            search_queries_fn=lambda *_: ["query"], memory_guard=FlippingGuard(), memory_check_interval=0.01,
        )
        await self.manager.start()
        job, _ = self.manager.submit("keyword", "province", None)
        final = None
        for _ in range(300):
            final = self.manager.status(job["job_id"])
            if final and final["status"] in {"completed", "failed", "stopped"}:
                break
            await asyncio.sleep(.01)
        self.assertEqual(final["status"], "completed")
        events = self.store.events_after(job["job_id"], 0, 100)
        self.assertTrue(any(event["event"] == "memory_paused" for event in events))
        self.assertTrue(any(event["event"] == "memory_resumed" for event in events))

    async def test_legacy_false_decision_requests_stop_without_unknown_ids_stopping_jobs(self):
        job_id = "legacy-stop"
        state = {"schema_version": 3, "job_model_version": 5, "job_id": job_id, "status": "queued",
                 "keyword": "k", "province": "p", "requested_count": None, "mode": "automatic",
                 "stop_requested": False, "search_batch_size": 100, "batch_number": 0,
                 "search_queries": [], "query_index": 0, "query_states": {}, "current_stage": "searching"}
        self.store.create(job_id, state, "stop-hash", None)
        self.store.claim(job_id)
        with self.store._db() as connection:
            connection.execute(
                "INSERT INTO job_compat_decisions(job_id,decision_id,candidate_url,created_at) VALUES(?,?,?,?)",
                (job_id, "old-id", "https://local.ir/a", 1.0),
            )
        decision = self.store.auto_compat_decision(job_id, "old-id", False)
        self.assertFalse(decision["decision"])
        self.assertTrue(self.store.auto_stop_requested(job_id))
        with self.assertRaises(DecisionConflict):
            self.store.auto_compat_decision(job_id, "unknown-id", False)


class MemoryGuardTests(unittest.TestCase):
    def test_cgroup_budget_triggers_soft_pause_condition(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "memory.current").write_text(str(8 * 1024**3))
            (root / "memory.max").write_text(str(9 * 1024**3))
            meminfo = root / "meminfo"
            meminfo.write_text("MemAvailable: 30000000 kB\n")
            snapshot = MemoryGuard(7168, 1024, meminfo_path=str(meminfo), cgroup_root=temp).snapshot()
            self.assertTrue(snapshot.under_pressure)
            self.assertIn("exceeds", snapshot.reason)


if __name__ == "__main__":
    unittest.main()
