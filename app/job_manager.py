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
from typing import Any, Awaitable, Callable

import httpx

from .job_store import IdempotencyConflict, JobStore, TERMINAL

log = logging.getLogger("business_discovery.jobs")


class JobManager:
    """Persistent, single-instance job runner backed by SQLite."""

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
        self.tasks: dict[str, asyncio.Task] = {}
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
        for task in active:
            task.cancel()
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
        if job_id in self.tasks or self.stopping:
            return
        task = asyncio.create_task(self._run(job_id), name=f"discovery-{job_id}")
        self.tasks[job_id] = task
        task.add_done_callback(lambda _: self.tasks.pop(job_id, None))

    def submit(self, keyword: str, province: str, count: int, idempotency_key: str | None = None) -> tuple[dict, bool]:
        self.store.cleanup_expired()
        job_id = uuid.uuid4().hex
        state = {
            "job_id": job_id,
            "status": "queued",
            "keyword": keyword,
            "province": province,
            "requested_count": count,
            "current_round": 0,
            "total_rounds": self.max_rounds,
            "round_url_target": 0,
            "discovered_in_round": 0,
            "processed_in_round": 0,
            "remaining_in_round": 0,
            "successful_websites": 0,
            "total_unique_mobile_numbers": 0,
            "current_stage": "queued",
            "website_statuses": {},
            "errors": [],
            "warnings": [],
            "results": [],
            "processed_domains": [],
            "registered_numbers": [],
            "pagination": {},
            "round_query": "",
            "round_sites": [],
            "round_search_complete": False,
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
        return {
            "job_id": record["job_id"],
            "status": record["status"],
            "created_at": record["created_at"],
            "updated_at": record["updated_at"],
            "last_event_id": record["last_event_id"],
            "requested_count": state["requested_count"],
            "current_round": state["current_round"],
            "total_rounds": state["total_rounds"],
            "discovered_in_round": state["discovered_in_round"],
            "processed_in_round": state["processed_in_round"],
            "remaining_in_round": state["remaining_in_round"],
            "successful_websites": state["successful_websites"],
            "total_unique_mobile_numbers": state["total_unique_mobile_numbers"],
            "current_stage": state["current_stage"],
            "website_statuses": state["website_statuses"],
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
            "job_id": status["job_id"],
            "status": status["status"],
            "requested_count": status["requested_count"],
            "successful_websites": status["successful_websites"],
            "total_unique_mobile_numbers": status["total_unique_mobile_numbers"],
            "completed_rounds": status["current_round"],
            "results": status["results"],
            "errors": status["errors"],
            "warnings": status["warnings"],
        }

    async def _run(self, job_id: str) -> None:
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
                stage = state["current_stage"]

                if stage == "preparing" and state.get("resume_stage"):
                    state["current_stage"] = state.pop("resume_stage")
                    self.store.save(job_id, state, event=("progress", self._round_progress(state)))
                    continue
                if stage == "preparing" or state["current_round"] == 0:
                    self._begin_round(job_id, 1)
                    continue
                if stage == "goal_reached":
                    self._complete_round(job_id)
                    continue
                if stage == "round_completed":
                    if (
                        state["successful_websites"] >= state["requested_count"]
                        or state["stop_after_round"]
                        or state["current_round"] >= self.max_rounds
                    ):
                        final_status = "failed" if state["fail_after_round"] else "completed"
                        self._finish(job_id, final_status)
                        return
                    self._begin_round(job_id, state["current_round"] + 1)
                    continue
                if stage == "searching":
                    search_error = await self._search_round(job_id)
                    if search_error and self.store.get(job_id)["state"]["current_stage"] == "failed":
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
        remaining = state["requested_count"] - state["successful_websites"]
        query = self.query_fn(state["keyword"], state["province"], round_number)
        state.update({
            "current_round": round_number,
            "round_url_target": math.ceil(remaining * self.url_factor),
            "discovered_in_round": 0,
            "processed_in_round": 0,
            "remaining_in_round": 0,
            "current_stage": "searching",
            "round_query": query,
            "round_sites": [],
            "round_search_complete": False,
            "stop_after_round": False,
            "fail_after_round": False,
        })
        self.store.save(job_id, state, event=("round_started", {
            "current_round": round_number,
            "total_rounds": self.max_rounds,
            "search_target": state["round_url_target"],
            "remaining_successful_websites": remaining,
            "current_stage": "searching",
        }))

    async def _search_round(self, job_id: str) -> str | None:
        state = self.store.get(job_id)["state"]
        query = state["round_query"]
        pagination = state["pagination"].get(query, {"next_page": 1, "exhausted": False})
        initial_sites = list(state["round_sites"])
        if pagination.get("exhausted") or pagination.get("next_page", 1) > self.max_search_pages:
            state["current_stage"] = "processing"
            state["round_search_complete"] = True
            state["discovered_in_round"] = len(state["round_sites"])
            state["remaining_in_round"] = self._remaining(state)
            self.store.save(job_id, state, event=("search_completed", self._round_progress(state)))
            return None

        async def page_checkpoint(next_page: int, new_sites: list[dict], exhausted: bool) -> None:
            current = self.store.get(job_id)["state"]
            current_sites = current["round_sites"]
            seen = {self._site_domain(site) for site in current_sites}
            processed = set(current["processed_domains"])
            slots = max(0, current["round_url_target"] - len(current_sites))
            added = 0
            for site in new_sites:
                domain = self._site_domain(site)
                if not domain or domain in seen or domain in processed:
                    continue
                if added >= slots:
                    break
                seen.add(domain)
                current_sites.append(site)
                added += 1
                current["website_statuses"].setdefault(site["website_url"], {
                    "status": "pending", "round": current["current_round"], "error": None,
                    "mobile_numbers_added": 0,
                })
            current["pagination"][query] = {"next_page": next_page, "exhausted": exhausted}
            current["round_sites"] = current_sites
            current["discovered_in_round"] = len(current_sites)
            current["remaining_in_round"] = self._remaining(current)
            self.store.save(job_id, current, event=("progress", {
                **self._round_progress(current), "search_page_consumed": next_page - 1,
            }))

        try:
            await self.search_fn(
                state["keyword"],
                state["province"],
                state["round_url_target"],
                excluded_domains=set(state["processed_domains"]),
                round_number=state["current_round"],
                start_page=int(pagination.get("next_page", 1)),
                initial_sites=initial_sites,
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
            if current["fail_after_round"] and not current["round_sites"] and not current["results"]:
                current["current_stage"] = "failed"
                self.store.save(job_id, current, status="failed", event=("job_failed", self._summary(current, error=str(exc))))
                return str(exc)
            current["current_stage"] = "processing"
            current["round_search_complete"] = True
            current["discovered_in_round"] = len(current["round_sites"])
            current["remaining_in_round"] = self._remaining(current)
            self.store.save(job_id, current, event=("warning", {
                "message": str(exc), "current_round": current["current_round"],
                "preserved_successful_websites": current["successful_websites"],
            }))
            return str(exc)

        current = self.store.get(job_id)["state"]
        current["current_stage"] = "processing"
        current["round_search_complete"] = True
        current["discovered_in_round"] = len(current["round_sites"])
        current["remaining_in_round"] = self._remaining(current)
        self.store.save(job_id, current, event=("search_completed", self._round_progress(current)))
        return None

    async def _process_round(self, job_id: str) -> None:
        record = self.store.get(job_id)
        state = record["state"]
        round_number = state["current_round"]
        round_sites = list(state["round_sites"])
        statuses = state["website_statuses"]
        pending = [site for site in round_sites if statuses.get(site["website_url"], {}).get("status") == "pending"]

        if not pending:
            self._complete_round(job_id)
            return

        for offset in range(0, len(pending), self.max_concurrent_sites):
            batch = pending[offset : offset + self.max_concurrent_sites]
            state = self.store.get(job_id)["state"]
            for site in batch:
                website_url = site["website_url"]
                status_info = state["website_statuses"].setdefault(website_url, {})
                status_info.update({"status": "processing", "round": round_number, "error": None})
                domain = self._site_domain(site)
                if domain and domain not in state["processed_domains"]:
                    state["processed_domains"].append(domain)
                state["remaining_in_round"] = self._remaining(state)
                self.store.save(job_id, state, event=("website_started", {
                    "website_url": website_url, **self._round_progress(state),
                }))
                state = self.store.get(job_id)["state"]

            async def process_one(site: dict) -> tuple[dict, dict]:
                try:
                    result = await self.process_fn(site, self.site_semaphore)
                    return site, result
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    return site, {"website_url": site["website_url"], "phones": {}, "error": str(exc)[:500]}

            tasks = [asyncio.create_task(process_one(site)) for site in batch]
            target_reached = False
            for completed in asyncio.as_completed(tasks):
                site, result = await completed
                target_reached = self._record_website(job_id, site, result)
                if target_reached:
                    break

            if target_reached:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                self._skip_remaining_round_sites(job_id)
                break

        self._complete_round(job_id)

    def _record_website(self, job_id: str, site: dict, site_result: dict) -> bool:
        state = self.store.get(job_id)["state"]
        website_url = site["website_url"]
        error = site_result.get("error")
        phones = site_result.get("phones") or {}
        new_numbers: list[tuple[str, str]] = []
        registered = set(state["registered_numbers"])
        for candidate, source in phones.items():
            number = self.normalize_phone_fn(str(candidate))
            if number and number not in registered:
                registered.add(number)
                new_numbers.append((number, str(source)))

        info = state["website_statuses"].setdefault(website_url, {"round": state["current_round"]})
        info["status"] = "failed" if error and not phones else "completed"
        info["error"] = str(error) if error else None
        info["mobile_numbers_added"] = len(new_numbers)
        state["processed_in_round"] = sum(
            1 for value in state["website_statuses"].values()
            if value.get("round") == state["current_round"] and value.get("status") in {"completed", "failed"}
        )
        state["remaining_in_round"] = self._remaining(state)
        if error:
            warning = {"website_url": website_url, "round": state["current_round"], "message": str(error)}
            state["errors"].append(warning)
            state["warnings"].append(warning)
            self.store.save(job_id, state, event=("warning", warning))
            state = self.store.get(job_id)["state"]
            info = state["website_statuses"][website_url]

        result = None
        if new_numbers:
            registered = set(state["registered_numbers"])
            phones_out = []
            for number, source in new_numbers:
                if number in registered:
                    continue
                registered.add(number)
                phones_out.append({"number": number, "source_url": source})
            if phones_out:
                state["registered_numbers"] = sorted(registered)
                result = {"website_url": website_url, "mobile_numbers": phones_out}
                state["results"].append(result)
                state["successful_websites"] = len(state["results"])
                state["total_unique_mobile_numbers"] = len(state["registered_numbers"])

        info["mobile_numbers_added"] = len(result["mobile_numbers"]) if result else 0
        status = info["status"]
        data = {"website_url": website_url, "status": status, **self._round_progress(state)}
        if error:
            data["error"] = str(error)
        if result:
            data["result"] = result
        self.store.save(job_id, state, event=("website_completed", data))
        self.store.save(job_id, self.store.get(job_id)["state"], event=("progress", self._round_progress(self.store.get(job_id)["state"])))
        current = self.store.get(job_id)["state"]
        return current["successful_websites"] >= current["requested_count"]

    def _skip_remaining_round_sites(self, job_id: str) -> None:
        state = self.store.get(job_id)["state"]
        for site in state["round_sites"]:
            info = state["website_statuses"].get(site["website_url"], {})
            if info.get("status") in {"pending", "processing"}:
                info["status"] = "skipped"
                info["reason"] = "requested_success_count_reached"
        state["remaining_in_round"] = 0
        state["current_stage"] = "goal_reached"
        self.store.save(job_id, state, event=("progress", self._round_progress(state)))

    def _complete_round(self, job_id: str) -> None:
        state = self.store.get(job_id)["state"]
        state["current_stage"] = "round_completed"
        state["remaining_in_round"] = self._remaining(state)
        self.store.save(job_id, state, event=("round_completed", self._round_progress(state)))

    def _finish(self, job_id: str, status: str) -> None:
        state = self.store.get(job_id)["state"]
        state["status"] = status
        state["current_stage"] = status
        event_type = "job_failed" if status == "failed" else "job_completed"
        self.store.save(job_id, state, status=status, event=(event_type, self._summary(state)))

    @staticmethod
    def _site_domain(site: dict) -> str:
        from urllib.parse import urlsplit
        return (urlsplit(site.get("url", "")).hostname or "").lower().removeprefix("www.")

    @staticmethod
    def _remaining(state: dict) -> int:
        return sum(
            1 for site in state.get("round_sites", [])
            if state.get("website_statuses", {}).get(site["website_url"], {}).get("status") in {"pending", "processing"}
        )

    @staticmethod
    def _round_progress(state: dict) -> dict:
        return {
            "status": state["status"],
            "current_round": state["current_round"],
            "total_rounds": state["total_rounds"],
            "discovered_in_round": state["discovered_in_round"],
            "processed_in_round": state["processed_in_round"],
            "remaining_in_round": state["remaining_in_round"],
            "successful_websites": state["successful_websites"],
            "total_unique_mobile_numbers": state["total_unique_mobile_numbers"],
            "current_stage": state["current_stage"],
        }

    def _summary(self, state: dict, error: str | None = None) -> dict:
        data = self._round_progress(state)
        data.update({"job_id": state.get("job_id"), "requested_count": state["requested_count"], "results": state["results"]})
        if error:
            data["error"] = error
        if state.get("warnings"):
            data["warnings"] = state["warnings"]
        return data
