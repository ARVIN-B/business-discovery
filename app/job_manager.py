from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import logging
import math
import os
import time
import uuid
from email.utils import parsedate_to_datetime
from typing import Any, Awaitable, Callable
from urllib.parse import urlsplit

from .job_store import IdempotencyConflict, JobStore, TERMINAL

log = logging.getLogger("business_discovery.jobs")


class JobManager:
    """Persistent URL-level discovery runner for one API process."""

    def __init__(
        self,
        store: JobStore,
        search_fn: Callable[..., Awaitable[list[dict]]],
        query_fn: Callable[[str, str, int], str],
        process_fn: Callable[..., Awaitable[dict]],
        normalize_phone_fn: Callable[[str], str | None],
        max_concurrent_sites: int = 3,
        max_rounds: int = 3,
        url_factor: float = 1.6,
        max_search_pages: int = 50,
        cleanup_interval: int = 3600,
        search_batch_size: int = 1000,
        search_retry_seconds: int = 60,
        search_queries_fn: Callable[[str, str], list[str]] | None = None,
    ):
        self.store = store
        self.search_fn = search_fn
        self.query_fn = query_fn
        self.process_fn = process_fn
        self.normalize_phone_fn = normalize_phone_fn
        self.max_concurrent_sites = max_concurrent_sites
        self.max_rounds = max_rounds
        self.url_factor = url_factor
        self.max_search_pages = max_search_pages
        self.cleanup_interval = cleanup_interval
        self.search_batch_size = max(1, search_batch_size)
        self.search_retry_seconds = max(1, search_retry_seconds)
        self.search_queries_fn = search_queries_fn
        self.tasks: dict[str, asyncio.Task] = {}
        self.decision_events: dict[str, asyncio.Event] = {}
        self.cleanup_task: asyncio.Task | None = None
        self.site_semaphore: asyncio.Semaphore | None = None
        self.lock_file = None
        self.stopping = False

    async def start(self) -> None:
        self.store.initialize()
        lock_path = self.store.path.with_suffix(self.store.path.suffix + ".lock")
        self.lock_file = open(lock_path, "a+")
        os.chmod(lock_path, 0o600)
        try:
            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self.lock_file.close()
            self.lock_file = None
            raise RuntimeError("Another business-discovery process already owns the job database") from exc

        self.stopping = False
        self.site_semaphore = asyncio.Semaphore(self.max_concurrent_sites)
        self.store.cleanup_expired()
        for job_id in self.store.recover():
            self._spawn(job_id)
        self.cleanup_task = asyncio.create_task(self._cleanup_loop(), name="job-cleanup")

    async def stop(self) -> None:
        self.stopping = True
        active = list(self.tasks.values())
        for job_id in list(self.tasks):
            self.decision_events.setdefault(job_id, asyncio.Event()).set()
        if active:
            await asyncio.gather(*active, return_exceptions=True)
        if self.cleanup_task:
            self.cleanup_task.cancel()
            await asyncio.gather(self.cleanup_task, return_exceptions=True)
            self.cleanup_task = None
        self.store.requeue_running()
        if self.lock_file:
            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_UN)
            self.lock_file.close()
            self.lock_file = None

    async def _cleanup_loop(self) -> None:
        while True:
            await asyncio.sleep(self.cleanup_interval)
            try:
                removed = self.store.cleanup_expired()
                if removed:
                    log.info("Expired %s discovery jobs", removed)
            except Exception:
                log.exception("Job cleanup failed")

    def _spawn(self, job_id: str) -> None:
        existing = self.tasks.get(job_id)
        if existing and not existing.done():
            return
        if existing and existing.done():
            self.tasks.pop(job_id, None)
        if self.stopping:
            return
        task = asyncio.create_task(self._run(job_id), name=f"discovery-{job_id}")
        self.tasks[job_id] = task
        task.add_done_callback(lambda finished: self.tasks.pop(job_id, None) if self.tasks.get(job_id) is finished else None)

    def _queries_for(self, keyword: str, province: str) -> list[str]:
        if self.search_queries_fn is not None:
            return list(dict.fromkeys(query.strip() for query in self.search_queries_fn(keyword, province) if query.strip()))
        queries = []
        for index in range(1, 513):
            query = self.query_fn(keyword, province, index).strip()
            if not query:
                break
            if query in queries:
                break
            queries.append(query)
        return queries

    def submit(self, keyword: str, province: str, count: int | None, idempotency_key: str | None = None) -> tuple[dict, bool]:
        self.store.cleanup_expired()
        job_id = uuid.uuid4().hex
        state = {
            "schema_version": 2,
            "job_model_version": 4,
            "job_id": job_id,
            "status": "queued",
            "keyword": keyword,
            "province": province,
            "requested_count": count,
            "mode": "approval_controlled",
            "stop_requested": False,
            "active_urls": [],
            "search_batch_size": self.search_batch_size,
            "batch_number": 0,
            "batch_urls": [],
            "batch_index": 0,
            "url_queue": [],
            "seen_urls": [],
            "query_index": 0,
            "search_queries": self._queries_for(keyword, province),
            "query_states": {},
            "provider_failure_queries": [],
            "search_exhausted": False,
            "search_retry_at": None,
            "pending_decision_id": None,
            "decisions": {},
            "active_url": None,
            "current_round": 0,
            "total_rounds": self.max_rounds,
            "round_url_target": 0,
            "discovered_page_count": 0,
            "processed_page_count": 0,
            "successful_page_count": 0,
            "successful_page_urls": [],
            "discovered_in_round": 0,
            "processed_in_round": 0,
            "remaining_in_round": 0,
            "current_stage": "searching",
            "page_statuses": {},
            "errors": [],
            "warnings": [],
            "results": [],
            "processed_urls": [],
            "registered_numbers": [],
            "pagination": {},
            "round_query": "",
            "round_pages": [],
            "round_search_complete": False,
            "search_exhausted": False,
            "stop_after_round": False,
            "fail_after_round": False,
        }
        request_hash = hashlib.sha256(
            json.dumps({"keyword": keyword, "province": province, "count": count}, ensure_ascii=False, sort_keys=True).encode()
        ).hexdigest()
        actual_id, created = self.store.create(job_id, state, request_hash, idempotency_key)
        record = self.store.get(actual_id)
        if created:
            self._spawn(actual_id)
        return self._public_state(record), created

    def _public_state(self, record: dict) -> dict:
        state = record["state"]
        search_diagnostics = self._search_diagnostics(state)
        return {
            "schema_version": 2,
            "job_id": record["job_id"],
            "status": record["status"],
            "created_at": record["created_at"],
            "updated_at": record["updated_at"],
            "last_event_id": record["last_event_id"],
            "requested_count": state["requested_count"],
            "mode": state.get("mode", "limited"),
            "stop_requested": state.get("stop_requested", False),
            "active_url_count": len(state.get("active_urls", [])),
            "current_round": state["current_round"],
            "total_rounds": state["total_rounds"],
            "discovered_page_count": state["discovered_page_count"],
            "processed_page_count": state["processed_page_count"],
            "successful_page_count": state["successful_page_count"],
            "discovered_in_round": state["discovered_in_round"],
            "processed_in_round": state["processed_in_round"],
            "remaining_in_round": state["remaining_in_round"],
            "successful_results": len(state["results"]),
            "total_unique_mobile_numbers": len(state["registered_numbers"]),
            "current_stage": state["current_stage"],
            "batch_number": state.get("batch_number", state.get("current_round", 0)),
            "batch_size": state.get("search_batch_size"),
            "queued_url_count": len(state.get("url_queue", [])),
            "seen_url_count": len(state.get("seen_urls", [])),
            "current_batch_index": state.get("batch_index", 0),
            "current_batch_url_count": len(state.get("batch_urls", [])),
            "pending_decision_id": state.get("pending_decision_id"),
            "active_url": state.get("active_url"),
            "reason": state.get("completion_reason"),
            "search_diagnostics": search_diagnostics,
            "query_diagnostics": {
                query: {"status": value.get("status", "pending"), "next_page": value.get("next_page", 1),
                    "exhausted": bool(value.get("exhausted")), "totals": value.get("totals", {}),
                    "last_page": (value.get("recent_pages") or [None])[-1],
                    "last_error": value.get("last_error"), "retry_at": value.get("retry_at")}
                for query, value in state.get("query_states", {}).items()
            },
            "page_statuses": state["page_statuses"],
            "errors": state["errors"],
            "warnings": state["warnings"],
            "results": state["results"],
        }

    def status(self, job_id: str) -> dict | None:
        record = self.store.get(job_id)
        return self._public_state(record) if record else None

    def final_result(self, job_id: str) -> dict | None:
        status = self.status(job_id)
        if status is None:
            return None
        return {
            "schema_version": 2,
            "job_id": status["job_id"],
            "status": status["status"],
            "requested_count": status["requested_count"],
            "mode": status["mode"],
            "stop_requested": status["stop_requested"],
            "batch_number": status.get("batch_number", 0),
            "batch_size": status.get("batch_size"),
            "reason": status.get("reason"),
            "search_diagnostics": status.get("search_diagnostics"),
            "query_diagnostics": status.get("query_diagnostics"),
            "successful_results": status["successful_results"],
            "total_unique_mobile_numbers": status["total_unique_mobile_numbers"],
            "discovered_page_count": status["discovered_page_count"],
            "processed_page_count": status["processed_page_count"],
            "successful_page_count": status["successful_page_count"],
            "completed_rounds": status["current_round"],
            "results": status["results"],
            "errors": status["errors"],
            "warnings": status["warnings"],
        }

    async def _run(self, job_id: str) -> None:
        record = self.store.get(job_id)
        if record and int(record["state"].get("job_model_version", 3)) >= 4:
            await self._run_approval_job(job_id)
        else:
            await self._run_legacy(job_id)

    async def _run_approval_job(self, job_id: str) -> None:
        try:
            if not self.store.claim(job_id):
                return
            while True:
                if self.stopping:
                    return
                record = self.store.get(job_id)
                if record is None or record["status"] in TERMINAL:
                    return
                state = record["state"]
                if state.get("stop_requested"):
                    self._finish_approval_job(job_id, "stopped", "stop_requested")
                    return
                stage = state["current_stage"]
                if stage == "awaiting_approval":
                    event = self.decision_events.setdefault(job_id, asyncio.Event())
                    event.clear()
                    current = self.store.get(job_id)
                    if current is None or current["status"] in TERMINAL:
                        return
                    if current["state"].get("pending_decision_id"):
                        await event.wait()
                    continue
                if stage == "searching":
                    if len(state["url_queue"]) >= state["search_batch_size"] or state["search_exhausted"]:
                        batch = self.store.freeze_batch(job_id)
                        if batch:
                            continue
                        latest = self.store.get(job_id)
                        if latest and latest["state"].get("search_exhausted") and not latest["state"].get("url_queue"):
                            self._finish_approval_job(job_id, "completed", "search_exhausted")
                            return
                    await self._search_for_batch(job_id)
                    continue
                if stage == "crawling":
                    candidate = self.store.start_sequential_url(job_id)
                    if candidate is None:
                        continue
                    try:
                        page_result = await self.process_fn(candidate, self.site_semaphore)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        page_result = {"pages": [], "error": str(exc)[:500]}
                    records = self._records_for_url(candidate, page_result)
                    self.store.complete_sequential_url(
                        job_id, candidate, records,
                        str(page_result.get("error"))[:500] if page_result.get("error") else None,
                        f"/v1/jobs/{job_id}/decisions",
                    )
                    continue
                if stage in {"completed", "failed", "stopped"}:
                    return
                raise RuntimeError(f"Unexpected approval job stage: {stage}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception("Approval-controlled discovery job %s failed", job_id)
            record = self.store.get(job_id)
            if record and record["status"] not in TERMINAL:
                state = record["state"]
                state["errors"].append({"stage": state.get("current_stage"), "message": str(exc)[:500]})
                state["current_stage"] = "failed"
                self.store.save(job_id, state, status="failed", event=("job_failed", self._summary(state, error=str(exc))))

    async def _search_for_batch(self, job_id: str) -> None:
        record = self.store.get(job_id)
        if record is None:
            return
        state = record["state"]
        if state.get("stop_requested") or state.get("search_exhausted"):
            return
        query_index = int(state["query_index"])
        queries = state["search_queries"]
        if query_index >= len(queries):
            state["search_exhausted"] = True
            self.store.save(job_id, state, event=("search_exhausted", {"query_count": len(queries)}))
            return
        query = queries[query_index]
        cursor = state["query_states"].get(query, {"next_page": 1, "exhausted": False})
        if cursor.get("exhausted"):
            self.store.mark_query_exhausted(job_id, query)
            return
        retry_at = state.get("search_retry_at")
        if retry_at and retry_at > time.time():
            event = self.decision_events.setdefault(job_id, asyncio.Event())
            event.clear()
            current = self.store.get(job_id)
            if current is None or current["state"].get("stop_requested") or self.stopping:
                return
            try:
                await asyncio.wait_for(event.wait(), timeout=max(0.0, retry_at - time.time()))
            except asyncio.TimeoutError:
                pass
            return
        needed = max(1, int(state["search_batch_size"]) - len(state["url_queue"]))

        async def page_checkpoint(next_page: int, new_pages: list[dict], exhausted: bool, diagnostics: dict | None = None) -> bool:
            current = self.store.get(job_id)
            if not current or current["state"].get("stop_requested") or self.stopping:
                return False
            self.store.record_search_page(job_id, query, next_page, new_pages, exhausted, diagnostics)
            return True

        try:
            await self.search_fn(
                state["keyword"], state["province"], needed,
                excluded_urls=set(state["seen_urls"]), round_number=query_index + 1,
                start_page=int(cursor.get("next_page", 1)), initial_sites=[],
                page_callback=page_checkpoint, max_pages=0,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if self.store.get(job_id)["state"].get("stop_requested"):
                return
            retry_after = getattr(exc, "retry_after", None)
            if retry_after is None:
                response = getattr(exc, "response", None)
                header = response.headers.get("Retry-After") if response is not None else None
                if header:
                    try:
                        retry_after = max(0.0, float(header))
                    except ValueError:
                        try:
                            retry_after = max(0.0, parsedate_to_datetime(header).timestamp() - time.time())
                        except (TypeError, ValueError, OverflowError):
                            retry_after = None
            delay = max(0.0, float(retry_after)) if retry_after is not None else float(self.search_retry_seconds)
            error_diagnostics = getattr(exc, "diagnostics", None) or {}
            failed_query = None
            next_query_index = None
            provider_failure_queries = None
            if error_diagnostics.get("provider_failure"):
                latest_state = self.store.get(job_id)["state"]
                queries = latest_state.get("search_queries", [])
                current_index = int(latest_state.get("query_index", 0))
                failed_query = queries[current_index] if current_index < len(queries) else query
                failed_queries = list(latest_state.get("provider_failure_queries", []))
                if failed_query not in failed_queries:
                    failed_queries.append(failed_query)
                pending_indices = [index for index, item in enumerate(queries)
                                   if not latest_state.get("query_states", {}).get(item, {}).get("exhausted")]
                untried_indices = [index for index in pending_indices if queries[index] not in failed_queries]
                if untried_indices and len(failed_queries) < min(3, max(1, len(pending_indices))) and retry_after is None:
                    next_query_index = next((index for index in untried_indices if index > current_index), untried_indices[0])
                    provider_failure_queries = failed_queries
                    delay = min(delay, 0.25)
                else:
                    remaining = [index for index in pending_indices if index > current_index]
                    next_query_index = (remaining or pending_indices or [current_index])[0]
                    provider_failure_queries = []
                    delay = max(delay, float(self.search_retry_seconds))
            self.store.set_search_retry(job_id, time.time() + delay, str(exc),
                http_status=getattr(exc, "http_status", None), diagnostics=error_diagnostics,
                failed_query=failed_query, next_query_index=next_query_index,
                provider_failure_queries=provider_failure_queries)
            event = self.decision_events.setdefault(job_id, asyncio.Event())
            event.clear()
            current = self.store.get(job_id)
            if current is None or current["state"].get("stop_requested") or self.stopping:
                return
            try:
                await asyncio.wait_for(event.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            return
        latest = self.store.get(job_id)
        if latest is None:
            return
        query_state = latest["state"]["query_states"].get(query, {})
        if query_state.get("exhausted"):
            self.store.mark_query_exhausted(job_id, query)

    def _records_for_url(self, candidate: dict, page_result: dict) -> list[dict]:
        pages = page_result.get("pages") or []
        if not pages and isinstance(page_result.get("phones"), dict):
            pages = [{"source_url": candidate["url"], "website_url": candidate.get("website_url"),
                      "page_title": "", "page_description": "", "numbers": list(page_result["phones"]),
                      "phone_sources": page_result["phones"]}]
        records = []
        for page_data in sorted(pages, key=lambda item: str(item.get("source_url", ""))):
            source_url = str(page_data.get("source_url") or candidate["url"])
            parts = urlsplit(source_url)
            if parts.scheme not in {"http", "https"} or not parts.hostname or parts.username or parts.password:
                continue
            website_url = str(page_data.get("website_url") or candidate.get("website_url") or f"{parts.scheme}://{parts.netloc}/")
            phone_sources = page_data.get("phone_sources") or {}
            for raw in sorted(set(page_data.get("numbers") or [])):
                number = self.normalize_phone_fn(str(raw))
                if not number:
                    continue
                records.append({"number": number, "source_url": str(phone_sources.get(number) or source_url),
                    "website_url": website_url, "page_title": str(page_data.get("page_title") or ""),
                    "page_description": str(page_data.get("page_description") or "")})
        return records

    def _finish_approval_job(self, job_id: str, status: str, reason: str) -> None:
        record = self.store.get(job_id)
        if not record:
            return
        state = record["state"]
        state["status"] = state["current_stage"] = status
        state["completion_reason"] = reason
        state["pending_decision_id"] = None
        if status == "completed" and reason == "search_exhausted":
            state.setdefault("warnings", []).append({"message": "All configured search queries were exhausted and the URL queue is empty."})
        self.store.save(job_id, state, status=status, event=("job_" + status, {
            "job_id": job_id, "status": status, "reason": reason,
            "successful_results": len(state["results"]), "processed_page_count": state["processed_page_count"],
            "search_diagnostics": self._search_diagnostics(state),
        }))

    @staticmethod
    def _search_diagnostics(state: dict) -> dict:
        queries = state.get("query_states", {})
        totals = {key: 0 for key in ("pages", "raw_results", "valid_urls", "new_urls", "duplicate_urls", "rejected_urls")}
        for query_state in queries.values():
            for key, value in query_state.get("totals", {}).items():
                if key in totals:
                    totals[key] += int(value or 0)
        query_list = state.get("search_queries", [])
        index = min(int(state.get("query_index", 0)), max(0, len(query_list) - 1))
        consumed = sum(
            1 for item in queries.values()
            if item.get("status") == "exhausted" or int(item.get("totals", {}).get("pages", 0) or 0) > 0
        )
        return {
            "query_count": len(query_list),
            "queries_attempted": len(queries),
            "queries_consumed": consumed,
            "queries_exhausted": sum(item.get("status") == "exhausted" for item in queries.values()),
            "current_query_index": state.get("query_index", 0),
            "current_query": query_list[index] if query_list else "",
            "search_batch_size": state.get("search_batch_size"),
            "queue_url_count": len(state.get("url_queue", [])),
            "seen_url_count": len(state.get("seen_urls", [])),
            "search_retry_at": state.get("search_retry_at"),
            "search_exhausted": bool(state.get("search_exhausted")),
            **totals,
        }

    def decide(self, job_id: str, decision_id: str, decision: bool) -> dict:
        result = self.store.resolve_decision(job_id, decision_id, decision)
        self.decision_events.setdefault(job_id, asyncio.Event()).set()
        return result

    async def _run_legacy(self, job_id: str) -> None:
        try:
            if not self.store.claim(job_id):
                return
            while True:
                record = self.store.get(job_id)
                if record is None:
                    return
                state = record["state"]
                if record["status"] in TERMINAL:
                    return
                if state.get("stop_requested") and not state.get("active_urls"):
                    self._finish(job_id, "stopped")
                    return
                stage = state["current_stage"]

                if stage == "preparing" and state.get("resume_stage"):
                    state["current_stage"] = state.pop("resume_stage")
                    self.store.save(job_id, state, event=("progress", self._summary(state)))
                    continue
                if stage == "preparing" or state["current_round"] == 0:
                    self._begin_round(job_id, 1)
                    continue
                if stage == "goal_reached":
                    self._complete_round(job_id)
                    continue
                if stage == "round_completed":
                    if (
                        (state.get("mode", "limited") == "limited" and len(state["results"]) >= state["requested_count"])
                        or state["stop_after_round"]
                        or state["current_round"] >= self.max_rounds
                    ):
                        status = "failed" if state["fail_after_round"] and not state["results"] else "completed"
                        self._finish(job_id, status)
                        return
                    if state.get("stop_requested"):
                        self._finish(job_id, "stopped")
                        return
                    self._begin_round(job_id, state["current_round"] + 1)
                    continue
                if stage == "searching":
                    await self._search_round(job_id)
                    current = self.store.get(job_id)
                    if current is None or current["status"] in TERMINAL:
                        return
                    continue
                if stage == "processing":
                    await self._process_round(job_id)
                    continue
                if stage in {"completed", "failed"}:
                    return
                raise RuntimeError(f"Unexpected persisted job stage: {stage}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception("Discovery job %s failed unexpectedly", job_id)
            record = self.store.get(job_id)
            if record and record["status"] not in TERMINAL:
                state = record["state"]
                state["errors"].append({"stage": state.get("current_stage"), "message": str(exc)[:500]})
                state["current_stage"] = "failed"
                self.store.save(job_id, state, status="failed", event=("job_failed", self._summary(state, error=str(exc))))

    def _begin_round(self, job_id: str, round_number: int) -> None:
        state = self.store.get(job_id)["state"]
        remaining = max(0, state["requested_count"] - len(state["results"])) if state.get("mode", "limited") == "limited" else None
        query = self.query_fn(state["keyword"], state["province"], round_number)
        state.update({
            "current_round": round_number,
            "round_url_target": math.ceil(remaining * self.url_factor) if remaining is not None else None,
            "discovered_in_round": 0,
            "processed_in_round": 0,
            "remaining_in_round": 0,
            "current_stage": "searching",
            "round_query": query,
            "round_pages": [],
            "round_search_complete": False,
            "stop_after_round": False,
            "fail_after_round": False,
        })
        self.store.save(job_id, state, event=("round_started", {
            "current_round": round_number,
            "total_rounds": self.max_rounds,
            "round_url_target": state["round_url_target"],
            "remaining_unique_numbers": remaining,
            "current_stage": "searching",
        }))

    async def _search_round(self, job_id: str) -> None:
        state = self.store.get(job_id)["state"]
        query = state["round_query"]
        pagination = state["pagination"].get(query, {"next_page": 1, "exhausted": False})
        initial_pages = list(state["round_pages"])
        if pagination.get("exhausted") or pagination.get("next_page", 1) > self.max_search_pages:
            state["current_stage"] = "processing"
            state["round_search_complete"] = True
            self._update_round_counts(state)
            state["search_exhausted"] = True
            self.store.save(job_id, state, event=("search_completed", self._summary(state)))
            return

        async def page_checkpoint(next_page: int, new_pages: list[dict], exhausted: bool, diagnostics: dict | None = None) -> None:
            current = self.store.get(job_id)["state"]
            current_pages = current["round_pages"]
            seen = {page["url"] for page in current_pages}
            processed = set(current["processed_urls"])
            slots = max(0, current["round_url_target"] - len(current_pages)) if current["round_url_target"] is not None else None
            added = 0
            for page in new_pages:
                page_url = str(page.get("url", ""))
                if not page_url or page_url in seen or page_url in processed:
                    continue
                if slots is not None and added >= slots:
                    break
                seen.add(page_url)
                current_pages.append(page)
                added += 1
                current["page_statuses"].setdefault(page_url, {
                    "status": "pending", "round": current["current_round"], "error": None, "new_results_count": 0,
                })
            current["pagination"][query] = {"next_page": next_page, "exhausted": exhausted}
            current["round_pages"] = current_pages
            current["discovered_page_count"] += added
            current["search_exhausted"] = current["search_exhausted"] or exhausted
            self._update_round_counts(current)
            self.store.save(job_id, current, event=("progress", {
                **self._summary(current), "search_page_consumed": next_page - 1,
            }))

        try:
            await self.search_fn(
                state["keyword"],
                state["province"],
                state["round_url_target"],
                excluded_urls=set(state["processed_urls"]),
                round_number=state["current_round"],
                start_page=int(pagination.get("next_page", 1)),
                initial_sites=initial_pages,
                page_callback=page_checkpoint,
                max_pages=self.max_search_pages,
            )
        except RuntimeError as exc:
            current = self.store.get(job_id)["state"]
            warning = {"round": current["current_round"], "message": str(exc)}
            current["errors"].append(warning)
            current["warnings"].append(warning)
            current["stop_after_round"] = True
            current["fail_after_round"] = current["current_round"] == 1
            if current["fail_after_round"] and not current["round_pages"] and not current["results"]:
                current["current_stage"] = "failed"
                self.store.save(job_id, current, status="failed", event=("job_failed", self._summary(current, error=str(exc))))
                return
            current["current_stage"] = "processing"
            current["round_search_complete"] = True
            self._update_round_counts(current)
            self.store.save(job_id, current, event=("warning", {
                "message": str(exc), "current_round": current["current_round"],
                "preserved_result_count": len(current["results"]), **self._summary(current),
            }))
            return

        current = self.store.get(job_id)["state"]
        current["current_stage"] = "processing"
        current["round_search_complete"] = True
        pagination_state = current["pagination"].get(query, {})
        current["search_exhausted"] = current["search_exhausted"] or bool(pagination_state.get("exhausted"))
        self._update_round_counts(current)
        self.store.save(job_id, current, event=("search_completed", self._summary(current)))

    async def _process_round(self, job_id: str) -> None:
        state = self.store.get(job_id)["state"]
        round_pages = list(state["round_pages"])
        statuses = state["page_statuses"]
        pending = [page for page in round_pages if statuses.get(page["url"], {}).get("status") == "pending"]
        if not pending:
            if state.get("stop_requested"):
                self._finish(job_id, "stopped")
                return
            self._complete_round(job_id)
            return

        for offset in range(0, len(pending), self.max_concurrent_sites):
            state = self.store.get(job_id)["state"]
            if state.get("stop_requested"):
                break
            if state.get("mode", "limited") == "limited" and len(state["results"]) >= state["requested_count"]:
                break
            batch = pending[offset : offset + self.max_concurrent_sites]
            started = [page for page in batch if self.store.start_url(job_id, page)]
            if not started:
                break

            async def process_one(page: dict) -> tuple[dict, dict]:
                try:
                    result = await self.process_fn(page, self.site_semaphore)
                    return page, result
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    return page, {"page_url": page["url"], "pages": [], "error": str(exc)[:500]}

            tasks = [asyncio.create_task(process_one(page)) for page in started]
            for completed in asyncio.as_completed(tasks):
                page, page_result = await completed
                self._record_page(job_id, page, page_result)
        state = self.store.get(job_id)["state"]
        if state.get("stop_requested"):
            self._skip_remaining_round_pages(job_id, "stop_requested")
            self._finish(job_id, "stopped")
            return
        if state.get("mode", "limited") == "limited" and len(state["results"]) >= state["requested_count"]:
            self._skip_remaining_round_pages(job_id, "requested_result_count_reached")
            return
        self._complete_round(job_id)

    def _record_page(self, job_id: str, candidate: dict, page_result: dict) -> bool:
        state = self.store.get(job_id)["state"]
        candidate_url = candidate["url"]
        error = page_result.get("error")
        records: list[dict] = []
        if error:
            warning = {"page_url": candidate_url, "round": state["current_round"], "message": str(error)}
            state["errors"].append(warning)
            state["warnings"].append(warning)

        pages = page_result.get("pages") or []
        # Transitional adapter for test/custom processors that still return the old phone mapping.
        if not pages and isinstance(page_result.get("phones"), dict):
            source = candidate_url
            pages = [{
                "source_url": source,
                "website_url": candidate.get("website_url"),
                "page_title": "",
                "page_description": "",
                "numbers": list(page_result["phones"]),
                "phone_sources": page_result["phones"],
            }]

        for page_data in sorted(pages, key=lambda item: str(item.get("source_url", ""))):
            source_url = str(page_data.get("source_url") or candidate_url)
            parts = urlsplit(source_url)
            if parts.scheme not in {"http", "https"} or not parts.hostname or parts.username or parts.password:
                continue
            website_url = str(page_data.get("website_url") or candidate.get("website_url") or f"{parts.scheme}://{parts.netloc}/")
            for raw_number in sorted(set(page_data.get("numbers") or [])):
                number = self.normalize_phone_fn(str(raw_number))
                if not number:
                    continue
                phones = page_data.get("phone_sources") or {}
                record = {
                    "number": number,
                    "source_url": str(phones.get(number) or source_url),
                    "website_url": website_url,
                    "page_title": str(page_data.get("page_title") or ""),
                    "page_description": str(page_data.get("page_description") or ""),
                }
                records.append(record)
        state = self.store.complete_url(job_id, candidate, records, str(error) if error else None)
        return state.get("mode", "limited") == "limited" and len(state["results"]) >= state["requested_count"]

    def _skip_remaining_round_pages(self, job_id: str, reason: str = "requested_result_count_reached") -> None:
        state = self.store.get(job_id)["state"]
        for page in state["round_pages"]:
            info = state["page_statuses"].get(page["url"], {})
            if info.get("status") == "pending":
                info["status"] = "skipped"
                info["reason"] = reason
        state["remaining_in_round"] = 0
        state["current_stage"] = "goal_reached"
        self.store.save(job_id, state, event=("progress", self._summary(state)))

    def _complete_round(self, job_id: str) -> None:
        state = self.store.get(job_id)["state"]
        state["current_stage"] = "round_completed"
        self._update_round_counts(state)
        self.store.save(job_id, state, event=("round_completed", self._summary(state)))

    def _finish(self, job_id: str, status: str) -> None:
        state = self.store.get(job_id)["state"]
        search_finished = state["current_round"] >= self.max_rounds or state["search_exhausted"]
        has_exhaustion_note = any("exhausted before the requested" in item.get("message", "") for item in state["warnings"])
        if (
            status == "completed"
            and state.get("mode", "limited") == "limited"
            and len(state["results"]) < state["requested_count"]
            and search_finished
            and not state["stop_after_round"]
            and not has_exhaustion_note
        ):
            state["warnings"].append({
                "message": "Available search results were exhausted before the requested unique mobile count was reached.",
            })
        state["status"] = status
        state["current_stage"] = status
        event_type = "job_stopped" if status == "stopped" else ("job_failed" if status == "failed" else "job_completed")
        self.store.save(job_id, state, status=status, event=(event_type, self._summary(state)))

    def request_stop(self, job_id: str) -> dict | None:
        outcome = self.store.request_stop(job_id)
        if outcome is not None:
            self.decision_events.setdefault(job_id, asyncio.Event()).set()
        return outcome

    @staticmethod
    def _update_round_counts(state: dict) -> None:
        state["discovered_in_round"] = len(state.get("round_pages", []))
        state["remaining_in_round"] = sum(
            1 for page in state.get("round_pages", [])
            if state.get("page_statuses", {}).get(page["url"], {}).get("status") in {"pending", "processing"}
        )

    @staticmethod
    def _summary(state: dict, error: str | None = None) -> dict:
        data = {
            "schema_version": 2,
            "status": state["status"],
            "current_round": state["current_round"],
            "total_rounds": state["total_rounds"],
            "discovered_page_count": state["discovered_page_count"],
            "processed_page_count": state["processed_page_count"],
            "successful_page_count": state["successful_page_count"],
            "discovered_in_round": state["discovered_in_round"],
            "processed_in_round": state["processed_in_round"],
            "remaining_in_round": state["remaining_in_round"],
            "successful_results": len(state["results"]),
            "total_unique_mobile_numbers": len(state["registered_numbers"]),
            "requested_count": state["requested_count"],
            "current_stage": state["current_stage"],
            "results": state["results"],
        }
        if error:
            data["error"] = error
        if state.get("warnings"):
            data["warnings"] = state["warnings"]
        if state.get("errors"):
            data["errors"] = state["errors"]
        return data
