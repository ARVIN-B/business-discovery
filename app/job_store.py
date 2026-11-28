from __future__ import annotations

import json
import os
import re
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any

TERMINAL = {"completed", "failed", "stopped"}


class IdempotencyConflict(ValueError):
    pass


class DecisionConflict(ValueError):
    pass


class JobStore:
    """Small SQLite-backed job and ordered event store for one API instance."""

    def __init__(self, path: str, retention_seconds: int = 7 * 24 * 60 * 60):
        self.path = Path(path)
        self.retention_seconds = retention_seconds

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    @contextmanager
    def _db(self):
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
        with self._db() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    state_json TEXT NOT NULL,
                    request_hash TEXT NOT NULL,
                    idempotency_key TEXT UNIQUE,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    last_event_id INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS job_events (
                    job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
                    event_id INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    data_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    PRIMARY KEY (job_id, event_id)
                );
                CREATE INDEX IF NOT EXISTS idx_jobs_expiry ON jobs(status, expires_at);
                CREATE INDEX IF NOT EXISTS idx_events_job_id ON job_events(job_id, event_id);
                CREATE TABLE IF NOT EXISTS job_batches (
                    job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
                    batch_number INTEGER NOT NULL,
                    batch_size INTEGER NOT NULL,
                    phase TEXT NOT NULL,
                    status TEXT NOT NULL,
                    search_started_at REAL,
                    search_completed_at REAL,
                    crawl_started_at REAL,
                    completed_at REAL,
                    urls_collected INTEGER NOT NULL DEFAULT 0,
                    urls_processed INTEGER NOT NULL DEFAULT 0,
                    urls_failed INTEGER NOT NULL DEFAULT 0,
                    current_position INTEGER NOT NULL DEFAULT 0,
                    retry_at REAL,
                    last_error TEXT,
                    last_checkpoint_at REAL NOT NULL,
                    PRIMARY KEY(job_id, batch_number)
                );
                CREATE INDEX IF NOT EXISTS idx_batches_status ON job_batches(job_id, status, batch_number);
                CREATE TABLE IF NOT EXISTS job_urls (
                    job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
                    url TEXT NOT NULL,
                    website_url TEXT NOT NULL,
                    search_title TEXT NOT NULL DEFAULT '',
                    batch_number INTEGER,
                    position INTEGER,
                    status TEXT NOT NULL DEFAULT 'queued',
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    retry_at REAL,
                    discovered_at REAL NOT NULL,
                    started_at REAL,
                    completed_at REAL,
                    firecrawl_job_id TEXT,
                    firecrawl_next_url TEXT,
                    firecrawl_page_offset INTEGER NOT NULL DEFAULT 0,
                    pages_processed INTEGER NOT NULL DEFAULT 0,
                    results_extracted INTEGER NOT NULL DEFAULT 0,
                    compat_decision_id TEXT,
                    PRIMARY KEY(job_id, url),
                    UNIQUE(job_id, batch_number, position)
                );
                CREATE INDEX IF NOT EXISTS idx_job_urls_batch ON job_urls(job_id, batch_number, position, status);
                CREATE INDEX IF NOT EXISTS idx_job_urls_queue ON job_urls(job_id, status, discovered_at);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_job_urls_decision ON job_urls(job_id, compat_decision_id) WHERE compat_decision_id IS NOT NULL;
                CREATE TABLE IF NOT EXISTS job_results (
                    job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
                    number TEXT NOT NULL,
                    candidate_url TEXT NOT NULL,
                    source_url TEXT NOT NULL,
                    website_url TEXT NOT NULL,
                    page_title TEXT NOT NULL DEFAULT '',
                    page_description TEXT NOT NULL DEFAULT '',
                    batch_number INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    PRIMARY KEY(job_id, number)
                );
                CREATE INDEX IF NOT EXISTS idx_job_results_url ON job_results(job_id, candidate_url, created_at);
                CREATE TABLE IF NOT EXISTS job_compat_decisions (
                    job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
                    decision_id TEXT NOT NULL,
                    candidate_url TEXT,
                    decision_value INTEGER,
                    created_at REAL NOT NULL,
                    PRIMARY KEY(job_id, decision_id)
                );
                """
            )
            self._migrate_legacy_jobs(connection)
            self._migrate_job_model(connection)
            self._migrate_auto_jobs(connection)
            connection.execute("PRAGMA user_version=5")
        os.chmod(self.path, 0o600)

    def _migrate_legacy_jobs(self, connection: sqlite3.Connection) -> None:
        """Convert v1 nested website results without interpreting domain strings as URLs."""
        connection.execute("BEGIN IMMEDIATE")
        try:
            rows = connection.execute("SELECT job_id,status,state_json FROM jobs").fetchall()
            for row in rows:
                job_id = str(row["job_id"])
                state = json.loads(row["state_json"])
                if int(state.get("schema_version", 1)) >= 2:
                    continue

                records: list[dict] = []
                registered: set[str] = set()
                for old_result in state.get("results", []):
                    if isinstance(old_result, dict) and "number" in old_result:
                        old_numbers = [old_result]
                    else:
                        old_numbers = [
                            {**phone, "website_url": old_result.get("website_url", "")}
                            for phone in old_result.get("mobile_numbers", [])
                            if isinstance(phone, dict)
                        ] if isinstance(old_result, dict) else []
                    for phone in old_numbers:
                        number = str(phone.get("number", ""))
                        if not re.fullmatch(r"09\d{9}", number) or number in registered:
                            continue
                        registered.add(number)
                        records.append({
                            "number": number,
                            "source_url": str(phone.get("source_url", "")),
                            "website_url": str(phone.get("website_url", "")),
                            "page_title": str(phone.get("page_title", "")),
                            "page_description": str(phone.get("page_description", "")),
                        })

                old_statuses = state.get("website_statuses", {})
                page_statuses: dict[str, dict] = {}
                round_pages: list[dict] = []
                processed_urls: list[str] = []
                for old_page in state.get("round_sites", []):
                    page_url = str(old_page.get("url", ""))
                    if not page_url.startswith(("http://", "https://")):
                        continue
                    root_url = str(old_page.get("website_url", ""))
                    old_info = old_statuses.get(root_url, {})
                    info = {
                        "status": old_info.get("status", "pending"),
                        "round": old_info.get("round", state.get("current_round", 0)),
                        "error": old_info.get("error"),
                        "new_results_count": 0,
                    }
                    page_statuses[page_url] = info
                    round_pages.append({"url": page_url, "website_url": root_url})
                    if info["status"] in {"completed", "failed"}:
                        processed_urls.append(page_url)

                successful_page_urls = list(dict.fromkeys(
                    record["source_url"] for record in records if record["source_url"]
                ))
                page_to_records: dict[str, list[dict]] = {}
                for record in records:
                    page_to_records.setdefault(record["source_url"], []).append(record)
                for page_url, info in page_statuses.items():
                    info["new_results_count"] = len(page_to_records.get(page_url, []))

                legacy_domains = list(state.get("processed_domains", []))
                legacy_results = state.get("results", [])
                legacy_website_statuses = state.get("website_statuses", {})
                state.update({
                    "schema_version": 2,
                    "requested_count": int(state.get("requested_count", 1)),
                    "results": records,
                    "registered_numbers": sorted(registered),
                    "successful_page_urls": successful_page_urls,
                    "successful_page_count": len(successful_page_urls),
                    "processed_page_count": sum(
                        1 for info in old_statuses.values()
                        if info.get("status") in {"completed", "failed", "skipped"}
                    ),
                    "discovered_page_count": len(old_statuses),
                    "page_statuses": page_statuses,
                    "round_pages": round_pages,
                    "processed_urls": processed_urls,
                    "discovered_in_round": len(round_pages),
                    "processed_in_round": sum(1 for info in page_statuses.values() if info["status"] in {"completed", "failed"}),
                    "remaining_in_round": sum(1 for info in page_statuses.values() if info["status"] in {"pending", "processing"}),
                    "total_rounds": int(state.get("total_rounds", 3)),
                    "errors": list(state.get("errors", [])),
                    "warnings": list(state.get("warnings", [])),
                    "pagination": dict(state.get("pagination", {})),
                    "round_search_complete": bool(state.get("round_search_complete", False)),
                    "search_exhausted": False,
                    "stop_after_round": bool(state.get("stop_after_round", False)),
                    "fail_after_round": bool(state.get("fail_after_round", False)),
                    # These are retained for audit only; domain names are never URL exclusions.
                    "legacy_processed_domains": legacy_domains,
                    "legacy_results": legacy_results,
                    "legacy_website_statuses": legacy_website_statuses,
                })
                state.pop("website_statuses", None)
                state.pop("successful_websites", None)
                state.pop("total_unique_mobile_numbers", None)
                state.pop("processed_domains", None)
                state.pop("round_sites", None)
                state.pop("resume_stage", None)
                state["status"] = str(row["status"])

                connection.execute(
                    "UPDATE jobs SET state_json=? WHERE job_id=?",
                    (self._json(state), job_id),
                )
                emitted = set()
                existing_events = connection.execute(
                    "SELECT data_json FROM job_events WHERE job_id=? AND event_type='result_found'", (job_id,)
                ).fetchall()
                for event_row in existing_events:
                    try:
                        event_data = json.loads(event_row["data_json"])
                        emitted.add(event_data.get("result", {}).get("number"))
                    except (ValueError, AttributeError):
                        continue
                for index, record in enumerate(records, start=1):
                    if record["number"] in emitted:
                        continue
                    self._append_event(connection, job_id, "result_found", {
                        "result": record,
                        "successful_results": index,
                        "total_unique_mobile_numbers": index,
                        "requested_count": state["requested_count"],
                        "migration": "v1_to_v2",
                    })
                    emitted.add(record["number"])
                warning = {
                    "message": "Legacy job state migrated to schema_version 2; historical processed domain names were not reused as URL exclusions.",
                }
                state["warnings"].append(warning)
                connection.execute("UPDATE jobs SET state_json=? WHERE job_id=?", (self._json(state), job_id))
                self._append_event(connection, job_id, "warning", warning)
                if row["status"] in TERMINAL:
                    self._append_event(connection, job_id, "job_failed" if row["status"] == "failed" else "job_completed", {
                        "job_id": job_id,
                        "status": row["status"],
                        "requested_count": state["requested_count"],
                        "successful_results": len(records),
                        "total_unique_mobile_numbers": len(registered),
                        "discovered_page_count": state["discovered_page_count"],
                        "processed_page_count": state["processed_page_count"],
                        "successful_page_count": state["successful_page_count"],
                        "completed_rounds": state.get("current_round", 0),
                        "results": records,
                        "errors": state["errors"],
                        "warnings": state["warnings"],
                        "migration": "v1_to_v2",
                    })
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    def _migrate_job_model(self, connection: sqlite3.Connection) -> None:
        connection.execute("BEGIN IMMEDIATE")
        try:
            rows = connection.execute("SELECT job_id,state_json FROM jobs").fetchall()
            for row in rows:
                state = json.loads(row["state_json"])
                if int(state.get("job_model_version", 2)) >= 3:
                    continue
                state.update({"job_model_version": 3, "mode": "limited", "stop_requested": False,
                              "active_urls": [], "requested_count": int(state.get("requested_count", 1))})
                connection.execute("UPDATE jobs SET state_json=? WHERE job_id=?", (self._json(state), row["job_id"]))
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    def _migrate_auto_jobs(self, connection: sqlite3.Connection) -> None:
        """Move v4 approval jobs into normalized v5 batch/URL/result tables atomically."""
        connection.execute("BEGIN IMMEDIATE")
        try:
            rows = connection.execute("SELECT job_id,status,state_json,updated_at FROM jobs").fetchall()
            now = time.time()
            for row in rows:
                job_id = str(row["job_id"])
                state = json.loads(row["state_json"])
                version = int(state.get("job_model_version", 3))
                if version != 4:
                    continue

                batch_urls = list(state.get("batch_urls", []))
                page_statuses = state.get("page_statuses", {}) if isinstance(state.get("page_statuses"), dict) else {}
                batch_number = max(0, int(state.get("batch_number", 0)))
                batch_size = max(1, int(state.get("search_batch_size", 100)))
                batch_index = max(0, int(state.get("batch_index", 0)))
                old_stage = str(state.get("current_stage", "searching"))
                pending_id = state.get("pending_decision_id")
                if old_stage == "awaiting_approval" and batch_index < len(batch_urls):
                    completed_candidate = batch_urls[batch_index]
                    completed_url = str(completed_candidate.get("url", ""))
                    info = page_statuses.setdefault(completed_url, {})
                    info["status"] = "completed"
                    batch_index += 1

                assigned = batch_urls[batch_index:] if batch_urls else []
                current_batch_number = batch_number if assigned else 0
                batch_status = "crawling" if assigned else "completed"
                if old_stage == "awaiting_approval" and not assigned:
                    old_stage = "searching"

                current_queue = list(state.get("url_queue", []))
                if batch_urls:
                    current_queue = current_queue[len(batch_urls):]
                candidates: dict[str, dict] = {}
                if pending_id and batch_index > 0 and batch_index - 1 < len(batch_urls):
                    completed_candidate = batch_urls[batch_index - 1]
                    if isinstance(completed_candidate, dict) and completed_candidate.get("url"):
                        candidates[str(completed_candidate["url"])] = {
                            **completed_candidate, "batch_number": batch_number or None, "position": batch_index,
                        }
                for index, candidate in enumerate(batch_urls[batch_index:], start=batch_index + 1):
                    if isinstance(candidate, dict) and candidate.get("url"):
                        candidates[str(candidate["url"])] = {**candidate, "batch_number": current_batch_number, "position": index}
                for candidate in current_queue:
                    if isinstance(candidate, dict) and candidate.get("url"):
                        candidates.setdefault(str(candidate["url"]), {**candidate, "batch_number": None, "position": None})
                for url in state.get("seen_urls", []):
                    if isinstance(url, str) and url:
                        candidates.setdefault(url, {"url": url, "website_url": "", "batch_number": None, "position": None})

                active_batch_no = current_batch_number or (batch_number + 1 if row["status"] not in TERMINAL else 0)
                for url, candidate in candidates.items():
                    info = page_statuses.get(url, {})
                    status = str(info.get("status", "pending"))
                    if status == "processing":
                        status = "pending"
                    if status == "pending":
                        status = "queued"
                    connection.execute(
                        """INSERT OR IGNORE INTO job_urls(
                            job_id,url,website_url,search_title,batch_number,position,status,attempt_count,last_error,
                            discovered_at,started_at,completed_at,firecrawl_job_id,firecrawl_next_url,firecrawl_page_offset,
                            pages_processed,results_extracted
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (job_id, url, candidate.get("website_url") or "", str(candidate.get("search_title", "")),
                         candidate.get("batch_number"), candidate.get("position"), status,
                         int(info.get("attempt_count", 1 if info.get("status") in {"completed", "failed"} else 0)),
                         info.get("error"), now, info.get("started_at"), info.get("completed_at"),
                         info.get("firecrawl_job_id"), info.get("firecrawl_next_url"), int(info.get("firecrawl_page_offset", 0)),
                         int(info.get("pages_processed", 0)), int(info.get("new_results_count", 0))),
                    )

                if active_batch_no:
                    batch_count = len(assigned)
                    batch_processed = sum(1 for item in assigned if page_statuses.get(item.get("url"), {}).get("status") in {"completed", "failed"})
                    batch_failed = sum(1 for item in assigned if page_statuses.get(item.get("url"), {}).get("status") == "failed")
                    connection.execute(
                        """INSERT OR IGNORE INTO job_batches(
                            job_id,batch_number,batch_size,phase,status,search_started_at,search_completed_at,
                            crawl_started_at,completed_at,urls_collected,urls_processed,urls_failed,current_position,
                            last_checkpoint_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (job_id, active_batch_no, batch_size, "crawl" if assigned else "search", batch_status,
                         row["updated_at"], row["updated_at"] if assigned else None,
                         row["updated_at"] if assigned else None, row["updated_at"] if not assigned and row["status"] in TERMINAL else None,
                         batch_count, batch_processed, batch_failed, batch_processed, now),
                    )

                for decision_id, decision_value in (state.get("decisions", {}) or {}).items():
                    connection.execute(
                        "INSERT OR IGNORE INTO job_compat_decisions(job_id,decision_id,decision_value,created_at) VALUES(?,?,?,?)",
                        (job_id, str(decision_id), int(bool(decision_value)), now),
                    )
                if pending_id:
                    pending_url = None
                    if batch_index > 0 and batch_index - 1 < len(batch_urls):
                        pending_url = batch_urls[batch_index - 1].get("url")
                    connection.execute(
                        "INSERT OR IGNORE INTO job_compat_decisions(job_id,decision_id,candidate_url,created_at) VALUES(?,?,?,?)",
                        (job_id, str(pending_id), pending_url, now),
                    )
                    if pending_url:
                        connection.execute("UPDATE job_urls SET compat_decision_id=? WHERE job_id=? AND url=?",
                                           (str(pending_id), job_id, str(pending_url)))

                for record in state.get("results", []):
                    if not isinstance(record, dict) or not re.fullmatch(r"09\d{9}", str(record.get("number", ""))):
                        continue
                    source_url = str(record.get("source_url", ""))
                    candidate_url = source_url if source_url in candidates else next(iter(candidates), source_url)
                    connection.execute(
                        """INSERT OR IGNORE INTO job_results(
                            job_id,number,candidate_url,source_url,website_url,page_title,page_description,batch_number,created_at
                        ) VALUES(?,?,?,?,?,?,?,?,?)""",
                        (job_id, str(record["number"]), candidate_url, source_url, str(record.get("website_url", "")),
                         str(record.get("page_title", "")), str(record.get("page_description", "")),
                         int(candidates.get(candidate_url, {}).get("batch_number") or max(1, batch_number)), now),
                    )

                is_running = row["status"] not in TERMINAL
                if is_running:
                    next_stage = "crawling" if assigned else "searching"
                    if old_stage == "paused_memory":
                        next_stage = "paused_memory"
                    state.update({
                        "schema_version": 3, "job_model_version": 5, "mode": "automatic",
                        "current_stage": next_stage, "current_phase": "crawl" if assigned else "search",
                        "batch_number": active_batch_no or max(0, batch_number), "batch_size": batch_size,
                        "search_exhausted": bool(state.get("search_exhausted", False)),
                        "stop_requested": bool(state.get("stop_requested", False)),
                        "result_count": len(state.get("results", [])), "last_checkpoint_at": now,
                        "memory_pause": state.get("memory_pause"),
                    })
                    state.pop("pending_decision_id", None)
                    state.pop("batch_urls", None)
                    state.pop("url_queue", None)
                    state.pop("seen_urls", None)
                    state.pop("page_statuses", None)
                    state.pop("active_urls", None)
                    state.pop("active_url", None)
                    state.pop("registered_numbers", None)
                    state.pop("results", None)
                    state.pop("decisions", None)
                    state.pop("provider_failure_queries", None)
                    connection.execute("UPDATE jobs SET state_json=?,updated_at=? WHERE job_id=?", (self._json(state), now, job_id))
                else:
                    state.update({"schema_version": 3, "job_model_version": 5, "mode": "automatic",
                                  "result_count": len(state.get("results", [])), "batch_size": batch_size,
                                  "last_checkpoint_at": now})
                    state.pop("results", None)
                    state.pop("registered_numbers", None)
                    state.pop("decisions", None)
                    connection.execute("UPDATE jobs SET state_json=? WHERE job_id=?", (self._json(state), job_id))
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

    def _append_event(self, connection: sqlite3.Connection, job_id: str, event_type: str, data: dict) -> int:
        row = connection.execute("SELECT last_event_id,state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            raise KeyError(job_id)
        payload = dict(data)
        try:
            if int(json.loads(row["state_json"]).get("schema_version", 1)) >= 2:
                payload.setdefault("schema_version", 2)
        except (TypeError, ValueError):
            pass
        event_id = int(row["last_event_id"]) + 1
        now = time.time()
        connection.execute(
            "INSERT INTO job_events(job_id,event_id,event_type,data_json,created_at) VALUES(?,?,?,?,?)",
            (job_id, event_id, event_type, self._json(payload), now),
        )
        connection.execute("UPDATE jobs SET last_event_id=? WHERE job_id=?", (event_id, job_id))
        return event_id

    def create(self, job_id: str, state: dict, request_hash: str, idempotency_key: str | None) -> tuple[str, bool]:
        now = time.time()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            if idempotency_key:
                existing = connection.execute(
                    "SELECT job_id,request_hash FROM jobs WHERE idempotency_key=?", (idempotency_key,)
                ).fetchone()
                if existing:
                    if existing["request_hash"] != request_hash:
                        raise IdempotencyConflict("Idempotency-Key was already used with a different request")
                    connection.commit()
                    return str(existing["job_id"]), False
            connection.execute(
                "INSERT INTO jobs(job_id,status,state_json,request_hash,idempotency_key,created_at,updated_at,expires_at) VALUES(?,?,?,?,?,?,?,?)",
                (job_id, "queued", self._json(state), request_hash, idempotency_key, now, now, now + self.retention_seconds),
            )
            self._append_event(connection, job_id, "job_started", {
                "schema_version": int(state.get("schema_version", 1)),
                "job_id": job_id, "status": "queued", "current_stage": state.get("current_stage", "queued"),
            })
            connection.commit()
            os.chmod(self.path, 0o600)
            return job_id, True
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def claim(self, job_id: str) -> bool:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT status,state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None or row["status"] != "queued":
                connection.commit()
                return False
            state = json.loads(row["state_json"])
            if state.get("stop_requested"):
                state["status"] = "stopped"
                state["current_stage"] = "stopped"
                connection.execute("UPDATE jobs SET status='stopped',state_json=? WHERE job_id=?", (self._json(state), job_id))
                self._append_event(connection, job_id, "job_stopped", {"job_id": job_id, "status": "stopped"})
                connection.commit()
                return False
            state["status"] = "running"
            resumed_stage = state.get("resume_stage")
            if int(state.get("job_model_version", 3)) < 4:
                state["current_stage"] = "preparing"
            elif state.get("current_stage") in {"queued", "preparing"}:
                state["current_stage"] = "searching"
            else:
                state.pop("resume_stage", None)
            now = time.time()
            connection.execute(
                "UPDATE jobs SET status='running',state_json=?,updated_at=? WHERE job_id=?",
                (self._json(state), now, job_id),
            )
            self._append_event(connection, job_id, "progress", {"status": "running", "current_stage": state["current_stage"]})
            if resumed_stage:
                self._append_event(connection, job_id, "job_resumed", {
                    "status": "running", "current_stage": state["current_stage"],
                    "resume_stage": resumed_stage, "reason": "checkpoint_recovery",
                })
            connection.commit()
            return True
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def save(self, job_id: str, state: dict, status: str | None = None, event: tuple[str, dict] | None = None) -> int | None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT status FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            next_status = status or str(row["status"])
            state["status"] = next_status
            now = time.time()
            expires_at = now + self.retention_seconds if next_status in TERMINAL else now + self.retention_seconds
            connection.execute(
                "UPDATE jobs SET status=?,state_json=?,updated_at=?,expires_at=? WHERE job_id=?",
                (next_status, self._json(state), now, expires_at, job_id),
            )
            event_id = self._append_event(connection, job_id, event[0], event[1]) if event else None
            connection.commit()
            return event_id
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def start_url(self, job_id: str, page: dict) -> bool:
        """Atomically reserve a candidate URL unless stop/limited target blocks it."""
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT status,state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            state = json.loads(row["state_json"])
            url = str(page["url"])
            info = state.get("page_statuses", {}).get(url, {})
            if row["status"] in TERMINAL or state.get("stop_requested") or info.get("status") != "pending":
                connection.commit()
                return False
            if state.get("mode", "limited") == "limited" and len(state["results"]) >= state["requested_count"]:
                connection.commit()
                return False
            info.update({"status": "processing", "round": state["current_round"], "error": None})
            if url not in state["processed_urls"]:
                state["processed_urls"].append(url)
            state.setdefault("active_urls", []).append(url)
            self._update_state_counts(state)
            now = time.time()
            connection.execute("UPDATE jobs SET state_json=?,updated_at=? WHERE job_id=?", (self._json(state), now, job_id))
            self._append_event(connection, job_id, "page_started", {
                "page_url": url, "website_url": page.get("website_url"), **self._state_summary(state),
            })
            connection.commit()
            return True
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def complete_url(self, job_id: str, candidate: dict, records: list[dict], error: str | None = None) -> dict:
        """Commit all records and completion event for one candidate URL atomically."""
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT status,state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            state = json.loads(row["state_json"])
            url = str(candidate["url"])
            registered = set(state["registered_numbers"])
            unique: list[dict] = []
            for record in records:
                number = record["number"]
                if number in registered:
                    continue
                registered.add(number)
                unique.append(record)
            state["results"].extend(unique)
            state["registered_numbers"] = sorted(registered)
            info = state["page_statuses"].setdefault(url, {})
            info.update({"status": "failed" if error and not unique else "completed", "error": error,
                         "new_results_count": len(unique)})
            state["active_urls"] = [item for item in state.get("active_urls", []) if item != url]
            state["processed_page_count"] = state.get("processed_page_count", 0) + 1
            state["processed_in_round"] = state.get("processed_in_round", 0) + 1
            if unique and url not in state["successful_page_urls"]:
                state["successful_page_urls"].append(url)
            state["successful_page_count"] = len(state["successful_page_urls"])
            self._update_state_counts(state)
            now = time.time()
            connection.execute("UPDATE jobs SET state_json=?,updated_at=? WHERE job_id=?", (self._json(state), now, job_id))
            if error:
                warning = {"page_url": url, "round": state["current_round"], "message": error}
                state["errors"].append(warning)
                state["warnings"].append(warning)
                connection.execute("UPDATE jobs SET state_json=? WHERE job_id=?", (self._json(state), job_id))
                self._append_event(connection, job_id, "warning", warning)
            if unique:
                self._append_event(connection, job_id, "url_results_found", {
                    "candidate_url": url, "results": unique, "successful_results": len(state["results"]),
                    "total_unique_mobile_numbers": len(state["registered_numbers"]),
                    "requested_count": state["requested_count"], "mode": state.get("mode", "limited"),
                })
            self._append_event(connection, job_id, "page_completed", {
                "page_url": url, "website_url": candidate.get("website_url"), "status": info["status"],
                "new_results_count": len(unique), "processed_page_count": state["processed_page_count"],
                "successful_page_count": state["successful_page_count"], "successful_results": len(state["results"]),
                "requested_count": state["requested_count"], "error": error,
            })
            connection.commit()
            return state
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def request_stop(self, job_id: str) -> dict | None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT status,state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                connection.commit()
                return None
            state = json.loads(row["state_json"])
            status = str(row["status"])
            if status in TERMINAL:
                connection.commit()
                return {"status": status, "stop_requested": bool(state.get("stop_requested")), "active_url_count": len(state.get("active_urls", []))}
            already = bool(state.get("stop_requested"))
            state["stop_requested"] = True
            now = time.time()
            if status == "queued":
                state["status"] = state["current_stage"] = "stopped"
                state["completion_reason"] = "stop_requested"
                connection.execute("UPDATE jobs SET status='stopped',state_json=?,updated_at=? WHERE job_id=?", (self._json(state), now, job_id))
                self._append_event(connection, job_id, "job_stopped", {"job_id": job_id, "status": "stopped", **self._state_summary(state)})
                status = "stopped"
            else:
                if int(state.get("job_model_version", 3)) >= 4 and state.get("current_stage") == "awaiting_approval":
                    state["status"] = state["current_stage"] = "stopped"
                    state["completion_reason"] = "stop_requested"
                    state["pending_decision_id"] = None
                    connection.execute("UPDATE jobs SET status='stopped',state_json=?,updated_at=? WHERE job_id=?", (self._json(state), now, job_id))
                    self._append_event(connection, job_id, "job_stopped", {"job_id": job_id, "status": "stopped", "reason": "stop_requested"})
                    status = "stopped"
                else:
                    if int(state.get("job_model_version", 3)) < 4:
                        state["current_stage"] = "stopping"
                    connection.execute("UPDATE jobs SET state_json=?,updated_at=? WHERE job_id=?", (self._json(state), now, job_id))
                    if not already:
                        self._append_event(connection, job_id, "job_stop_requested", {"status": "running", "active_url_count": len(state.get("active_urls", []))})
            connection.commit()
            return {"status": status, "stop_requested": True, "active_url_count": len(state.get("active_urls", []))}
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _update_state_counts(state: dict) -> None:
        state["discovered_in_round"] = len(state.get("round_pages", []))
        state["remaining_in_round"] = sum(1 for page in state.get("round_pages", [])
            if state.get("page_statuses", {}).get(page["url"], {}).get("status") in {"pending", "processing"})

    @staticmethod
    def _state_summary(state: dict) -> dict:
        return {"status": state["status"], "current_round": state["current_round"],
            "discovered_page_count": state["discovered_page_count"], "processed_page_count": state["processed_page_count"],
            "successful_page_count": state["successful_page_count"], "discovered_in_round": state["discovered_in_round"],
            "processed_in_round": state["processed_in_round"], "remaining_in_round": state["remaining_in_round"],
            "successful_results": len(state["results"]), "requested_count": state["requested_count"]}

    def record_search_page(self, job_id: str, query: str, next_page: int, candidates: list[dict], exhausted: bool,
                           diagnostics: dict | None = None) -> dict:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            state = json.loads(row["state_json"])
            if state.get("stop_requested"):
                connection.commit()
                return state
            seen = set(state["seen_urls"])
            added = []
            for candidate in candidates:
                url = str(candidate.get("url", ""))
                if not url or url in seen:
                    continue
                seen.add(url)
                added.append(candidate)
                state["url_queue"].append(candidate)
                state["page_statuses"].setdefault(url, {
                    "status": "pending", "round": state.get("batch_number", 0) + 1,
                    "error": None, "new_results_count": 0,
                })
            state["seen_urls"] = sorted(seen)
            query_state = state["query_states"].setdefault(query, {})
            query_state.update({"next_page": next_page, "exhausted": bool(exhausted)})
            query_state["status"] = "exhausted" if exhausted else "active"
            page_diagnostic = dict(diagnostics or {})
            page_diagnostic.update({"next_page": next_page, "new_url_count": len(added),
                                    "queue_url_count": len(state["url_queue"]), "exhausted": bool(exhausted)})
            recent = query_state.setdefault("recent_pages", [])
            recent.append(page_diagnostic)
            query_state["recent_pages"] = recent[-100:]
            totals = query_state.setdefault("totals", {"pages": 0, "raw_results": 0, "valid_urls": 0,
                "new_urls": 0, "duplicate_urls": 0, "rejected_urls": 0})
            totals["pages"] += 1
            for target, source in (("raw_results", "raw_result_count"), ("valid_urls", "valid_url_count"),
                                   ("new_urls", "new_url_count"), ("duplicate_urls", "duplicate_url_count"),
                                   ("rejected_urls", "rejected_url_count")):
                totals[target] += int(page_diagnostic.get(source, 0) or 0)
            query_state["last_error"] = None
            query_state["retry_at"] = None
            state["provider_failure_queries"] = []
            state["discovered_page_count"] += len(added)
            state["search_exhausted"] = False
            state["search_retry_at"] = None
            now = time.time()
            connection.execute("UPDATE jobs SET state_json=?,updated_at=? WHERE job_id=?", (self._json(state), now, job_id))
            self._append_event(connection, job_id, "search_progress", {
                "query": query, "search_page_consumed": next_page - 1, "new_urls": len(added),
                "queued_urls": len(state["url_queue"]), "search_batch_size": state["search_batch_size"],
                "diagnostics": page_diagnostic,
            })
            connection.commit()
            return state
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def mark_query_exhausted(self, job_id: str, query: str) -> dict:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            state = json.loads(row["state_json"])
            state["query_states"].setdefault(query, {}).update({"exhausted": True})
            state["query_states"][query]["status"] = "exhausted"
            queries = state.get("search_queries", [])
            current_index = queries.index(query) if query in queries else int(state.get("query_index", 0))
            pending = [index for index, item in enumerate(queries)
                       if not state.get("query_states", {}).get(item, {}).get("exhausted")]
            later = [index for index in pending if index > current_index]
            state["query_index"] = (later or pending or [len(queries)])[0]
            state["search_exhausted"] = not pending
            connection.execute("UPDATE jobs SET state_json=?,updated_at=? WHERE job_id=?", (self._json(state), time.time(), job_id))
            self._append_event(connection, job_id, "search_query_exhausted", {
                "query": query, "query_index": state["query_index"],
                "query_count": len(state["search_queries"]), "search_exhausted": state["search_exhausted"],
            })
            connection.commit()
            return state
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def set_search_retry(self, job_id: str, retry_at: float, error: str, *, http_status: int | None = None,
                         diagnostics: dict | None = None, failed_query: str | None = None,
                         next_query_index: int | None = None,
                         provider_failure_queries: list[str] | None = None) -> None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            state = json.loads(row["state_json"])
            state["search_retry_at"] = retry_at
            state["last_search_error"] = error[:500]
            if next_query_index is not None:
                state["query_index"] = next_query_index
            if provider_failure_queries is not None:
                state["provider_failure_queries"] = provider_failure_queries
            query = failed_query
            if query is None and state.get("query_index", 0) < len(state.get("search_queries", [])):
                query = state["search_queries"][state["query_index"]]
            if query is not None:
                query_state = state["query_states"].setdefault(query, {"next_page": 1, "exhausted": False})
                query_state.update({"status": "retry_wait", "retry_at": retry_at, "last_error": error[:500],
                                    "last_http_status": http_status, "last_error_diagnostics": diagnostics or {}})
            warning = {"stage": "searching", "message": error[:500], "retry_at": retry_at,
                       "http_status": http_status, "query_index": state.get("query_index"),
                       "diagnostics": diagnostics or {}}
            state.setdefault("warnings", []).append(warning)
            connection.execute("UPDATE jobs SET state_json=?,updated_at=? WHERE job_id=?", (self._json(state), time.time(), job_id))
            self._append_event(connection, job_id, "warning", warning)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def freeze_batch(self, job_id: str) -> dict | None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            state = json.loads(row["state_json"])
            if state.get("batch_urls"):
                connection.commit()
                return state
            size = int(state["search_batch_size"])
            queue = state["url_queue"]
            if len(queue) < size and not state["search_exhausted"]:
                connection.commit()
                return None
            if not queue:
                connection.commit()
                return None
            state["batch_urls"] = queue[:size]
            state["batch_index"] = 0
            state["batch_number"] += 1
            state["current_stage"] = "crawling"
            connection.execute("UPDATE jobs SET state_json=?,updated_at=? WHERE job_id=?", (self._json(state), time.time(), job_id))
            self._append_event(connection, job_id, "batch_ready", {
                "batch_number": state["batch_number"], "url_count": len(state["batch_urls"]),
                "queued_url_count": len(queue), "search_exhausted": state["search_exhausted"],
            })
            connection.commit()
            return state
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def start_sequential_url(self, job_id: str) -> dict | None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT status,state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            state = json.loads(row["state_json"])
            if row["status"] in TERMINAL or state.get("stop_requested") or state.get("current_stage") != "crawling":
                connection.commit()
                return None
            index = int(state["batch_index"])
            if index >= len(state["batch_urls"]):
                connection.commit()
                return None
            candidate = state["batch_urls"][index]
            url = candidate["url"]
            info = state["page_statuses"].setdefault(url, {})
            if info.get("status") != "pending":
                connection.commit()
                return None
            info.update({"status": "processing", "error": None})
            state["active_url"] = url
            state["active_urls"] = [url]
            state["processed_urls"] = list(dict.fromkeys([*state.get("processed_urls", []), url]))
            now = time.time()
            connection.execute("UPDATE jobs SET state_json=?,updated_at=? WHERE job_id=?", (self._json(state), now, job_id))
            self._append_event(connection, job_id, "page_started", {
                "page_url": url, "website_url": candidate.get("website_url"),
                "batch_number": state["batch_number"], "batch_index": index,
            })
            connection.commit()
            return candidate
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def complete_sequential_url(self, job_id: str, candidate: dict, records: list[dict], error: str | None,
                                decision_url_prefix: str) -> dict:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            state = json.loads(row["state_json"])
            url = candidate["url"]
            unique = []
            registered = set(state["registered_numbers"])
            for record in records:
                if record["number"] in registered:
                    continue
                registered.add(record["number"])
                unique.append(record)
            state["registered_numbers"] = sorted(registered)
            state["results"].extend(unique)
            state["active_urls"] = []
            state["active_url"] = None
            status = "failed" if error else "completed"
            state["page_statuses"].setdefault(url, {}).update({
                "status": status, "error": error, "new_results_count": len(unique),
            })
            state["processed_page_count"] += 1
            state["processed_in_round"] = state["processed_page_count"]
            if unique:
                state["successful_page_urls"].append(url)
                state["successful_page_urls"] = list(dict.fromkeys(state["successful_page_urls"]))
                state["successful_page_count"] = len(state["successful_page_urls"])
            state["successful_results"] = len(state["results"])
            decision_id = uuid.uuid4().hex
            decision_url = f"{decision_url_prefix.rstrip('/')}/{decision_id}"
            stopped = bool(state.get("stop_requested"))
            if stopped:
                state["status"] = "stopped"
                state["current_stage"] = "stopped"
            else:
                state["pending_decision_id"] = decision_id
                state["current_stage"] = "awaiting_approval"
            now = time.time()
            next_status = "stopped" if stopped else "running"
            connection.execute("UPDATE jobs SET status=?,state_json=?,updated_at=?,expires_at=? WHERE job_id=?",
                (next_status, self._json(state), now, now + self.retention_seconds, job_id))
            if error:
                warning = {"page_url": url, "message": error, "batch_number": state["batch_number"]}
                state["errors"].append(warning)
                state["warnings"].append(warning)
                connection.execute("UPDATE jobs SET state_json=? WHERE job_id=?", (self._json(state), job_id))
                self._append_event(connection, job_id, "warning", warning)
            if unique:
                self._append_event(connection, job_id, "url_results_found", {
                    "candidate_url": url, "results": unique, "new_results_count": len(unique),
                    "successful_results": len(state["results"]), "mode": "approval_controlled",
                })
            self._append_event(connection, job_id, "url_completed", {
                "candidate_url": url, "results": unique, "new_results_count": len(unique),
                "error": error, "decision_id": decision_id if not stopped else None,
                "decision_url": decision_url if not stopped else None,
                "batch_number": state["batch_number"],
            })
            self._append_event(connection, job_id, "page_completed", {
                "page_url": url, "website_url": candidate.get("website_url"), "status": status,
                "new_results_count": len(unique), "processed_page_count": state["processed_page_count"],
                "successful_page_count": state["successful_page_count"], "successful_results": len(state["results"]),
                "error": error,
            })
            if stopped:
                self._append_event(connection, job_id, "job_stopped", {"job_id": job_id, "status": "stopped", "reason": "stop_requested"})
            connection.commit()
            return state
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def auto_snapshot(self, job_id: str, *, include_results: bool = True) -> dict | None:
        """Read normalized v5 progress without rehydrating URL/result arrays into stored state."""
        with self._db() as connection:
            row = connection.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                return None
            state = json.loads(row["state_json"])
            batch_no = int(state.get("batch_number", 0) or 0)
            batch = connection.execute(
                "SELECT * FROM job_batches WHERE job_id=? AND batch_number=?", (job_id, batch_no)
            ).fetchone() if batch_no else None
            url_rows = connection.execute(
                "SELECT * FROM job_urls WHERE job_id=? AND batch_number=? ORDER BY position",
                (job_id, batch_no),
            ).fetchall() if batch_no else []
            queued = connection.execute(
                "SELECT COUNT(*) FROM job_urls WHERE job_id=? AND batch_number IS NULL AND status='queued'", (job_id,)
            ).fetchone()[0]
            active = connection.execute(
                "SELECT * FROM job_urls WHERE job_id=? AND status='processing' ORDER BY started_at LIMIT 1", (job_id,)
            ).fetchone()
            total_urls = connection.execute("SELECT COUNT(*) FROM job_urls WHERE job_id=?", (job_id,)).fetchone()[0]
            completed_urls = connection.execute(
                "SELECT COUNT(*) FROM job_urls WHERE job_id=? AND status='completed'", (job_id,)
            ).fetchone()[0]
            failed_urls = connection.execute(
                "SELECT COUNT(*) FROM job_urls WHERE job_id=? AND status='failed'", (job_id,)
            ).fetchone()[0]
            result_count = int(connection.execute("SELECT COUNT(*) FROM job_results WHERE job_id=?", (job_id,)).fetchone()[0])
            results = [dict(item) for item in connection.execute(
                "SELECT number,source_url,website_url,page_title,page_description FROM job_results WHERE job_id=? ORDER BY created_at,number",
                (job_id,),
            ).fetchall()] if include_results else []
        current_urls = [dict(item) for item in url_rows]
        batch_data = dict(batch) if batch is not None else None
        query_list = state.get("search_queries", [])
        idx = int(state.get("query_index", 0))
        return {
            "job_id": row["job_id"], "status": row["status"], "state": state,
            "created_at": row["created_at"], "updated_at": row["updated_at"],
            "last_event_id": row["last_event_id"], "batch": batch_data,
            "batch_urls": current_urls, "queued_url_count": int(queued),
            "total_url_count": int(total_urls), "completed_url_count": int(completed_urls),
            "failed_url_count": int(failed_urls), "results": results,
            "result_count": result_count,
            "current_query": query_list[idx] if 0 <= idx < len(query_list) else None,
            "current_url": dict(active) if active else None,
        }

    def auto_url_exists(self, job_id: str, url: str) -> bool:
        with self._db() as connection:
            return connection.execute("SELECT 1 FROM job_urls WHERE job_id=? AND url=?", (job_id, url)).fetchone() is not None

    def auto_active_batch(self, job_id: str) -> dict | None:
        with self._db() as connection:
            row = connection.execute(
                "SELECT b.* FROM job_batches b JOIN jobs j ON j.job_id=b.job_id "
                "WHERE b.job_id=? AND b.batch_number=json_extract(j.state_json,'$.batch_number') "
                "AND b.status IN ('collecting','ready','crawling')",
                (job_id,),
            ).fetchone()
            return dict(row) if row else None

    def auto_search_complete(self, job_id: str) -> bool:
        with self._db() as connection:
            row = connection.execute("SELECT state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            return bool(row and json.loads(row["state_json"]).get("search_exhausted"))

    def auto_has_pending_urls(self, job_id: str) -> bool:
        with self._db() as connection:
            return connection.execute("SELECT 1 FROM job_urls WHERE job_id=? AND batch_number IS NULL AND status='queued' LIMIT 1", (job_id,)).fetchone() is not None

    def auto_seal_batch(self, job_id: str) -> None:
        """Promote a partial collecting batch when search has exhausted its configured queries."""
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            state = json.loads(row["state_json"])
            batch_no = int(state.get("batch_number", 0))
            batch = connection.execute("SELECT * FROM job_batches WHERE job_id=? AND batch_number=?", (job_id, batch_no)).fetchone()
            if batch and batch["status"] == "collecting" and state.get("search_exhausted"):
                count = int(connection.execute("SELECT COUNT(*) FROM job_urls WHERE job_id=? AND batch_number=?", (job_id, batch_no)).fetchone()[0])
                now = time.time()
                if count:
                    connection.execute("UPDATE job_batches SET status='ready',phase='crawl',search_completed_at=?,urls_collected=?,last_checkpoint_at=? WHERE job_id=? AND batch_number=?", (now, count, now, job_id, batch_no))
                    state.update({"current_stage": "batch_ready", "current_phase": "crawl"})
                    self._append_event(connection, job_id, "batch_ready", {"batch_number": batch_no, "url_count": count, "search_exhausted": True})
                else:
                    state.update({"current_stage": "completed", "completion_reason": "search_exhausted"})
                    connection.execute("UPDATE jobs SET status='completed' WHERE job_id=?", (job_id,))
                state["last_checkpoint_at"] = now
                connection.execute("UPDATE jobs SET state_json=?,updated_at=? WHERE job_id=?", (self._json(state), now, job_id))
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def auto_mark_search_exhausted(self, job_id: str) -> None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            state = json.loads(row["state_json"])
            state["search_exhausted"] = True
            state["query_index"] = len(state.get("search_queries", []))
            state["last_checkpoint_at"] = time.time()
            connection.execute("UPDATE jobs SET state_json=?,updated_at=? WHERE job_id=?", (self._json(state), time.time(), job_id))
            self._append_event(connection, job_id, "search_exhausted", self._auto_search_summary(state))
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def auto_begin_batch(self, job_id: str) -> dict | None:
        """Create the next durable batch and move any preserved overflow URLs into it."""
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT status,state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            state = json.loads(row["state_json"])
            if row["status"] in TERMINAL or state.get("stop_requested"):
                connection.commit()
                return None
            active = connection.execute(
                "SELECT * FROM job_batches WHERE job_id=? AND status IN ('collecting','ready','crawling') ORDER BY batch_number DESC LIMIT 1",
                (job_id,),
            ).fetchone()
            if active:
                connection.commit()
                return dict(active)
            next_number = int(connection.execute(
                "SELECT COALESCE(MAX(batch_number),0)+1 FROM job_batches WHERE job_id=?", (job_id,)
            ).fetchone()[0])
            batch_size = max(1, int(state.get("search_batch_size", 100)))
            now = time.time()
            candidates = connection.execute(
                "SELECT url FROM job_urls WHERE job_id=? AND batch_number IS NULL AND status='queued' ORDER BY discovered_at,url LIMIT ?",
                (job_id, batch_size),
            ).fetchall()
            for position, candidate in enumerate(candidates, start=1):
                connection.execute(
                    "UPDATE job_urls SET batch_number=?,position=? WHERE job_id=? AND url=? AND batch_number IS NULL",
                    (next_number, position, job_id, candidate["url"]),
                )
            count = len(candidates)
            exhausted = bool(state.get("search_exhausted"))
            ready = count >= batch_size or exhausted
            status = "ready" if ready and count else "collecting"
            phase = "crawl" if status == "ready" else "search"
            state.update({"batch_number": next_number, "current_stage": "batch_ready" if status == "ready" else "searching",
                          "current_phase": phase, "last_checkpoint_at": now})
            connection.execute(
                "INSERT INTO job_batches(job_id,batch_number,batch_size,phase,status,search_started_at,search_completed_at,urls_collected,last_checkpoint_at) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (job_id, next_number, batch_size, phase, status, now, now if ready else None, count, now),
            )
            connection.execute(
                "UPDATE jobs SET state_json=?,updated_at=? WHERE job_id=?", (self._json(state), now, job_id)
            )
            self._append_event(connection, job_id, "batch_started", {
                "batch_number": next_number, "batch_size": batch_size, "urls_already_queued": count,
                "phase": phase,
            })
            if status == "ready":
                self._append_event(connection, job_id, "batch_ready", {
                    "batch_number": next_number, "url_count": count, "search_exhausted": exhausted,
                })
            connection.commit()
            return dict(connection.execute(
                "SELECT * FROM job_batches WHERE job_id=? AND batch_number=?", (job_id, next_number)
            ).fetchone())
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def auto_record_search_page(self, job_id: str, query: str, next_page: int, candidates: list[dict],
                                exhausted: bool, diagnostics: dict | None = None) -> dict:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT state_json,status FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            state = json.loads(row["state_json"])
            if state.get("stop_requested") or row["status"] in TERMINAL:
                connection.commit()
                return {"stopped": True, "batch_ready": False, "search_exhausted": state.get("search_exhausted", False)}
            batch_no = int(state.get("batch_number", 0))
            batch = connection.execute(
                "SELECT * FROM job_batches WHERE job_id=? AND batch_number=? AND status='collecting'", (job_id, batch_no)
            ).fetchone()
            if batch is None:
                raise RuntimeError("Search page arrived without an active collecting batch")
            size = int(batch["batch_size"])
            current = int(connection.execute(
                "SELECT COUNT(*) FROM job_urls WHERE job_id=? AND batch_number=?", (job_id, batch_no)
            ).fetchone()[0])
            added = 0
            queued_extra = 0
            for candidate in candidates:
                url = str(candidate.get("url") or "")
                if not url:
                    continue
                existing = connection.execute("SELECT 1 FROM job_urls WHERE job_id=? AND url=?", (job_id, url)).fetchone()
                if existing:
                    continue
                assigned = current < size
                position = current + 1 if assigned else None
                connection.execute(
                    """INSERT INTO job_urls(job_id,url,website_url,search_title,batch_number,position,status,discovered_at)
                       VALUES(?,?,?,?,?,?,?,?)""",
                    (job_id, url, str(candidate.get("website_url") or ""), str(candidate.get("search_title") or ""),
                     batch_no if assigned else None, position, "queued", time.time()),
                )
                if assigned:
                    current += 1
                    added += 1
                else:
                    queued_extra += 1
            query_state = state.setdefault("query_states", {}).setdefault(query, {"next_page": 1, "exhausted": False})
            page_data = dict(diagnostics or {})
            page_data.update({"next_page": int(next_page), "new_url_count": added, "queued_extra_count": queued_extra})
            query_state.update({"next_page": int(next_page), "exhausted": bool(exhausted),
                                "status": "exhausted" if exhausted else "active", "last_error": None})
            if page_data.get("result_fingerprint"):
                query_state["last_fingerprint"] = page_data["result_fingerprint"]
            recent = query_state.setdefault("recent_pages", [])
            recent.append(page_data)
            query_state["recent_pages"] = recent[-20:]
            totals = query_state.setdefault("totals", {"pages": 0, "raw_results": 0, "valid_urls": 0, "new_urls": 0,
                "duplicate_urls": 0, "rejected_urls": 0})
            totals["pages"] += 1
            for target, source in (("raw_results", "raw_result_count"), ("valid_urls", "valid_url_count"),
                                   ("new_urls", "new_url_count"), ("duplicate_urls", "duplicate_url_count"),
                                   ("rejected_urls", "rejected_url_count")):
                totals[target] += int(page_data.get(source, 0) or 0)
            queries = state.get("search_queries", [])
            if exhausted and query in queries:
                pending = [i for i, item in enumerate(queries) if not state["query_states"].get(item, {}).get("exhausted")]
                after = [i for i in pending if i > queries.index(query)]
                state["query_index"] = (after or pending or [len(queries)])[0]
            state["search_exhausted"] = bool(queries) and all(
                state["query_states"].get(item, {}).get("exhausted", False) for item in queries
            )
            now = time.time()
            count = current
            ready = count >= size or state["search_exhausted"]
            connection.execute(
                "UPDATE job_batches SET urls_collected=?,last_checkpoint_at=?,search_completed_at=CASE WHEN ? THEN ? ELSE search_completed_at END,phase=CASE WHEN ? THEN 'crawl' ELSE phase END,status=CASE WHEN ? THEN 'ready' ELSE status END WHERE job_id=? AND batch_number=?",
                (count, now, int(ready), now, int(ready), int(ready and count > 0), job_id, batch_no),
            )
            if ready and count == 0:
                state["current_stage"] = "completed"
                state["completion_reason"] = "search_exhausted"
                connection.execute("UPDATE jobs SET status='completed' WHERE job_id=?", (job_id,))
                self._append_event(connection, job_id, "job_completed", {
                    "job_id": job_id, "status": "completed", "reason": "search_exhausted",
                    "search_diagnostics": self._auto_search_summary(state),
                })
            else:
                state["current_stage"] = "batch_ready" if ready else "searching"
                state["current_phase"] = "crawl" if ready else "search"
            state["last_search_error"] = None
            state["search_retry_at"] = None
            state["provider_failure_queries"] = []
            state["last_checkpoint_at"] = now
            connection.execute("UPDATE jobs SET state_json=?,updated_at=? WHERE job_id=?", (self._json(state), now, job_id))
            self._append_event(connection, job_id, "search_progress", {
                "batch_number": batch_no, "query": query, "query_index": state.get("query_index"),
                "search_page_consumed": int(next_page) - 1, "urls_collected": count,
                "queued_extra_count": queued_extra, "diagnostics": page_data,
            })
            if ready and count:
                self._append_event(connection, job_id, "batch_ready", {
                    "batch_number": batch_no, "url_count": count, "search_exhausted": state["search_exhausted"],
                })
            connection.commit()
            return {"stopped": False, "batch_ready": ready, "search_exhausted": state["search_exhausted"], "urls_collected": count}
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _auto_search_summary(state: dict) -> dict:
        query_states = state.get("query_states", {})
        totals = {key: 0 for key in ("pages", "raw_results", "valid_urls", "new_urls", "duplicate_urls", "rejected_urls")}
        for item in query_states.values():
            for key in totals:
                totals[key] += int(item.get("totals", {}).get(key, 0) or 0)
        return {"query_count": len(state.get("search_queries", [])),
                "queries_attempted": len(query_states),
                "queries_consumed": sum(bool(item.get("exhausted")) for item in query_states.values()),
                "queries_exhausted": sum(bool(item.get("exhausted")) for item in query_states.values()),
                "current_query_index": int(state.get("query_index", 0)),
                "search_exhausted": bool(state.get("search_exhausted")), **totals}

    def auto_search_retry(self, job_id: str, query: str, error: str, retry_at: float,
                          http_status: int | None, diagnostics: dict | None,
                          next_query_index: int | None = None, failure_cycle: list[str] | None = None) -> None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            state = json.loads(row["state_json"])
            now = time.time()
            state["search_retry_at"] = retry_at
            state["last_search_error"] = error[:500]
            state["last_checkpoint_at"] = now
            if next_query_index is not None:
                state["query_index"] = int(next_query_index)
            if failure_cycle is not None:
                state["provider_failure_queries"] = failure_cycle
            query_state = state.setdefault("query_states", {}).setdefault(query, {"next_page": 1, "exhausted": False})
            query_state.update({"status": "retry_wait", "retry_at": retry_at, "last_error": error[:500],
                                "last_http_status": http_status, "last_error_diagnostics": diagnostics or {}})
            connection.execute("UPDATE jobs SET state_json=?,updated_at=? WHERE job_id=?", (self._json(state), now, job_id))
            self._append_event(connection, job_id, "warning", {"stage": "searching", "query_index": state.get("query_index"),
                "message": error[:500], "retry_at": retry_at, "http_status": http_status,
                "diagnostics": diagnostics or {}})
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def auto_start_url(self, job_id: str) -> dict | None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT status,state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            state = json.loads(row["state_json"])
            if row["status"] in TERMINAL or state.get("stop_requested"):
                connection.commit()
                return None
            batch_no = int(state.get("batch_number", 0))
            batch = connection.execute("SELECT * FROM job_batches WHERE job_id=? AND batch_number=?", (job_id, batch_no)).fetchone()
            if not batch or batch["status"] not in {"ready", "crawling"}:
                connection.commit()
                return None
            url_row = connection.execute(
                "SELECT * FROM job_urls WHERE job_id=? AND batch_number=? AND status IN ('queued','pending','processing') "
                "AND (retry_at IS NULL OR retry_at<=?) ORDER BY position LIMIT 1",
                (job_id, batch_no, time.time()),
            ).fetchone()
            if url_row is None:
                connection.commit()
                return None
            resumed = url_row["status"] == "processing" or bool(url_row["firecrawl_job_id"])
            now = time.time()
            connection.execute(
                "UPDATE job_urls SET status='processing',attempt_count=attempt_count+1,started_at=COALESCE(started_at,?) WHERE job_id=? AND url=?",
                (now, job_id, url_row["url"]),
            )
            connection.execute(
                "UPDATE job_batches SET status='crawling',phase='crawl',crawl_started_at=COALESCE(crawl_started_at,?),current_position=?,last_checkpoint_at=? WHERE job_id=? AND batch_number=?",
                (now, url_row["position"], now, job_id, batch_no),
            )
            state.update({"current_stage": "crawling", "current_phase": "crawl", "current_url": url_row["url"],
                          "last_checkpoint_at": now})
            connection.execute("UPDATE jobs SET state_json=?,updated_at=? WHERE job_id=?", (self._json(state), now, job_id))
            self._append_event(connection, job_id, "crawl_started", {
                "batch_number": batch_no, "position": url_row["position"], "candidate_url": url_row["url"], "resumed": resumed,
            })
            connection.commit()
            value = dict(url_row)
            value["attempt_count"] = int(url_row["attempt_count"]) + 1
            value["status"] = "processing"
            return value
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def auto_set_firecrawl_job(self, job_id: str, url: str, remote_id: str | None) -> None:
        with self._db() as connection:
            now = time.time()
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "UPDATE job_urls SET firecrawl_job_id=?,firecrawl_next_url=NULL,firecrawl_page_offset=0,last_error=NULL WHERE job_id=? AND url=?",
                (remote_id, job_id, url),
            )
            connection.execute("UPDATE jobs SET updated_at=? WHERE job_id=?", (now, job_id))
            connection.execute("COMMIT")

    def auto_set_firecrawl_cursor(self, job_id: str, url: str, next_url: str | None, page_offset: int,
                                  page_completed: bool = False) -> None:
        with self._db() as connection:
            now = time.time()
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "UPDATE job_urls SET firecrawl_next_url=?,firecrawl_page_offset=?,pages_processed=pages_processed+?,last_error=NULL WHERE job_id=? AND url=?",
                (next_url, max(0, int(page_offset)), int(page_completed), job_id, url),
            )
            connection.execute("UPDATE jobs SET updated_at=? WHERE job_id=?", (now, job_id))
            connection.execute("UPDATE job_batches SET last_checkpoint_at=? WHERE job_id=? AND batch_number=(SELECT batch_number FROM job_urls WHERE job_id=? AND url=?)",
                                (now, job_id, job_id, url))
            connection.execute("COMMIT")

    def auto_record_number(self, job_id: str, candidate_url: str, record: dict) -> bool:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            job = connection.execute("SELECT state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            url_row = connection.execute("SELECT batch_number FROM job_urls WHERE job_id=? AND url=?", (job_id, candidate_url)).fetchone()
            if job is None or url_row is None:
                raise KeyError(job_id if job is None else candidate_url)
            state = json.loads(job["state_json"])
            number = str(record["number"])
            cursor = connection.execute(
                """INSERT OR IGNORE INTO job_results(
                    job_id,number,candidate_url,source_url,website_url,page_title,page_description,batch_number,created_at
                ) VALUES(?,?,?,?,?,?,?,?,?)""",
                (job_id, number, candidate_url, str(record.get("source_url", candidate_url)),
                 str(record.get("website_url", "")), str(record.get("page_title", "")),
                 str(record.get("page_description", "")), int(url_row["batch_number"] or 0), time.time()),
            )
            inserted = cursor.rowcount == 1
            if inserted:
                state["result_count"] = int(state.get("result_count", 0)) + 1
                state["last_checkpoint_at"] = time.time()
                connection.execute("UPDATE job_urls SET results_extracted=results_extracted+1 WHERE job_id=? AND url=?",
                                   (job_id, candidate_url))
                connection.execute("UPDATE jobs SET state_json=?,updated_at=? WHERE job_id=?", (self._json(state), time.time(), job_id))
                self._append_event(connection, job_id, "number_found", {
                    "result": {key: str(record.get(key, "")) for key in
                               ("number", "source_url", "website_url", "page_title", "page_description")},
                    "candidate_url": candidate_url, "batch_number": url_row["batch_number"],
                    "successful_results": state["result_count"],
                })
            connection.commit()
            return inserted
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def auto_retry_url(self, job_id: str, url: str, error: str, retry_at: float, diagnostics: dict | None = None) -> None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "UPDATE job_urls SET status='pending',last_error=?,retry_at=? WHERE job_id=? AND url=?",
                (error[:500], retry_at, job_id, url),
            )
            row = connection.execute("SELECT state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            state = json.loads(row["state_json"])
            state["retry_at"] = retry_at
            state["retry_stage"] = "crawling"
            state["last_error"] = error[:500]
            state["last_checkpoint_at"] = time.time()
            connection.execute("UPDATE jobs SET state_json=?,updated_at=? WHERE job_id=?", (self._json(state), time.time(), job_id))
            connection.execute("UPDATE job_batches SET retry_at=?,last_error=?,last_checkpoint_at=? WHERE job_id=? AND batch_number=?",
                               (retry_at, error[:500], time.time(), job_id, int(state.get("batch_number", 0))))
            self._append_event(connection, job_id, "warning", {"stage": "crawling", "candidate_url": url,
                "message": error[:500], "retry_at": retry_at, "diagnostics": diagnostics or {}})
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def auto_complete_url(self, job_id: str, url: str, error: str | None = None,
                          *, permanent_failure: bool = False) -> dict:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT status,state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            url_row = connection.execute("SELECT * FROM job_urls WHERE job_id=? AND url=?", (job_id, url)).fetchone()
            if row is None or url_row is None:
                raise KeyError(job_id if row is None else url)
            state = json.loads(row["state_json"])
            batch_no = int(url_row["batch_number"] or 0)
            status = "failed" if permanent_failure else "completed"
            now = time.time()
            connection.execute(
                "UPDATE job_urls SET status=?,last_error=?,retry_at=NULL,completed_at=?,firecrawl_job_id=NULL,firecrawl_next_url=NULL,firecrawl_page_offset=0 WHERE job_id=? AND url=?",
                (status, error[:500] if error else None, now, job_id, url),
            )
            state["current_url"] = None
            state["processed_page_count"] = int(state.get("processed_page_count", 0)) + 1
            if not permanent_failure:
                state["successful_page_count"] = int(state.get("successful_page_count", 0)) + 1
            state.pop("retry_at", None)
            state.pop("retry_stage", None)
            state["last_checkpoint_at"] = now
            compat_id = uuid.uuid4().hex
            connection.execute("UPDATE job_urls SET compat_decision_id=? WHERE job_id=? AND url=?", (compat_id, job_id, url))
            connection.execute(
                "INSERT INTO job_compat_decisions(job_id,decision_id,candidate_url,created_at) VALUES(?,?,?,?)",
                (job_id, compat_id, url, now),
            )
            results = [dict(item) for item in connection.execute(
                "SELECT number,source_url,website_url,page_title,page_description FROM job_results WHERE job_id=? AND candidate_url=? ORDER BY created_at,number",
                (job_id, url),
            ).fetchall()]
            connection.execute(
                "UPDATE job_batches SET urls_processed=urls_processed+1,urls_failed=urls_failed+?,current_position=?,last_checkpoint_at=? WHERE job_id=? AND batch_number=?",
                (int(permanent_failure), int(url_row["position"] or 0), now, job_id, batch_no),
            )
            connection.execute("UPDATE jobs SET state_json=?,updated_at=? WHERE job_id=?", (self._json(state), now, job_id))
            if error:
                self._append_event(connection, job_id, "warning", {"stage": "crawling", "candidate_url": url, "message": error[:500]})
            if results:
                self._append_event(connection, job_id, "url_results_found", {
                    "candidate_url": url, "results": results, "new_results_count": len(results),
                    "successful_results": int(state.get("result_count", 0)), "mode": "automatic",
                })
            self._append_event(connection, job_id, "url_completed", {
                "candidate_url": url, "results": results, "new_results_count": len(results), "error": error,
                "decision_id": compat_id, "decision_url": f"/v1/jobs/{job_id}/decisions/{compat_id}",
                "batch_number": batch_no,
            })
            self._append_event(connection, job_id, "page_completed", {
                "page_url": url, "website_url": url_row["website_url"], "status": status,
                "new_results_count": len(results), "processed_page_count": int(state.get("processed_page_count", 0)),
                "successful_results": int(state.get("result_count", 0)), "error": error,
            })
            remaining = int(connection.execute(
                "SELECT COUNT(*) FROM job_urls WHERE job_id=? AND batch_number=? AND status IN ('queued','pending','processing')",
                (job_id, batch_no),
            ).fetchone()[0])
            batch = connection.execute("SELECT * FROM job_batches WHERE job_id=? AND batch_number=?", (job_id, batch_no)).fetchone()
            batch_completed = remaining == 0
            if batch_completed:
                connection.execute("UPDATE job_batches SET status='completed',phase='completed',completed_at=?,last_checkpoint_at=? WHERE job_id=? AND batch_number=?",
                                   (now, now, job_id, batch_no))
                state.update({"current_stage": "searching", "current_phase": "search", "last_checkpoint_at": now})
                connection.execute("UPDATE jobs SET state_json=? WHERE job_id=?", (self._json(state), job_id))
                self._append_event(connection, job_id, "batch_completed", {
                    "batch_number": batch_no, "urls_collected": batch["urls_collected"],
                    "urls_processed": batch["urls_processed"] + 1, "urls_failed": batch["urls_failed"] + int(permanent_failure),
                    "result_count": int(state.get("result_count", 0)),
                })
            connection.commit()
            return {"results": results, "batch_completed": batch_completed, "status": status, "decision_id": compat_id}
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def auto_pause_memory(self, job_id: str, reason: str, snapshot: dict) -> bool:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT status,state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None or row["status"] in TERMINAL:
                connection.commit()
                return False
            state = json.loads(row["state_json"])
            if state.get("current_stage") == "paused_memory":
                connection.commit()
                return False
            prior = state.get("current_stage", "searching")
            state["resume_stage"] = prior
            state["current_stage"] = "paused_memory"
            state["memory_pause"] = {"reason": reason[:500], "snapshot": snapshot, "paused_at": time.time()}
            state["last_checkpoint_at"] = time.time()
            connection.execute("UPDATE jobs SET state_json=?,updated_at=? WHERE job_id=?", (self._json(state), time.time(), job_id))
            connection.execute("UPDATE job_batches SET phase='paused_memory',last_checkpoint_at=? WHERE job_id=? AND batch_number=? AND status IN ('collecting','ready','crawling')",
                               (time.time(), job_id, int(state.get("batch_number", 0))))
            self._append_event(connection, job_id, "memory_paused", {
                "batch_number": state.get("batch_number"), "resume_stage": prior, "reason": reason[:500], "memory": snapshot,
            })
            connection.commit()
            return True
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def auto_resume_memory(self, job_id: str, snapshot: dict) -> bool:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                connection.commit()
                return False
            state = json.loads(row["state_json"])
            if state.get("current_stage") != "paused_memory" or state.get("stop_requested"):
                connection.commit()
                return False
            now = time.time()
            stage = state.pop("resume_stage", "searching")
            state["current_stage"] = stage
            state["memory_pause"] = None
            state["last_checkpoint_at"] = now
            connection.execute("UPDATE jobs SET state_json=?,updated_at=? WHERE job_id=?", (self._json(state), now, job_id))
            batch_no = int(state.get("batch_number", 0))
            batch_phase = "crawl" if stage in {"crawling", "batch_ready"} else "search"
            connection.execute("UPDATE job_batches SET phase=?,last_checkpoint_at=? WHERE job_id=? AND batch_number=? AND status IN ('collecting','ready','crawling')",
                               (batch_phase, now, job_id, batch_no))
            self._append_event(connection, job_id, "memory_resumed", {"batch_number": batch_no, "stage": stage, "memory": snapshot})
            connection.commit()
            return True
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def auto_finish(self, job_id: str, status: str, reason: str) -> None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            state = json.loads(row["state_json"])
            now = time.time()
            state.update({"status": status, "current_stage": status, "current_phase": status,
                          "completion_reason": reason, "last_checkpoint_at": now})
            active = connection.execute("SELECT url,batch_number FROM job_urls WHERE job_id=? AND status='processing'", (job_id,)).fetchall()
            for item in active:
                connection.execute("UPDATE job_urls SET status='interrupted',last_error=?,retry_at=NULL WHERE job_id=? AND url=?",
                                   (reason, job_id, item["url"]))
            connection.execute("UPDATE jobs SET status=?,state_json=?,updated_at=?,expires_at=? WHERE job_id=?",
                               (status, self._json(state), now, now + self.retention_seconds, job_id))
            event_name = {"completed": "job_completed", "failed": "job_failed", "stopped": "job_stopped"}[status]
            self._append_event(connection, job_id, event_name, {
                "job_id": job_id, "status": status, "reason": reason,
                "successful_results": int(state.get("result_count", 0)),
                "search_diagnostics": self._auto_search_summary(state),
            })
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def auto_stop_requested(self, job_id: str) -> bool:
        with self._db() as connection:
            row = connection.execute("SELECT state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        return bool(row and json.loads(row["state_json"]).get("stop_requested"))

    def auto_compat_decision(self, job_id: str, decision_id: str, decision: bool) -> dict:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT status,state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            state = json.loads(row["state_json"])
            prior = connection.execute(
                "SELECT decision_value,candidate_url FROM job_compat_decisions WHERE job_id=? AND decision_id=?",
                (job_id, decision_id),
            ).fetchone()
            if prior is None:
                raise DecisionConflict("decision_id is unknown or belongs to a different job")
            if prior["decision_value"] is not None:
                if bool(prior["decision_value"]) is not decision:
                    raise DecisionConflict("A different decision was already recorded for this decision_id")
                connection.commit()
                return {"job_id": job_id, "decision_id": decision_id, "decision": decision,
                        "status": row["status"], "idempotent": True}
            if row["status"] in TERMINAL:
                connection.commit()
                return {"job_id": job_id, "decision_id": decision_id, "decision": decision,
                        "status": row["status"], "idempotent": True, "compatibility_noop": bool(decision)}
            state["last_checkpoint_at"] = time.time()
            connection.execute("UPDATE job_compat_decisions SET decision_value=? WHERE job_id=? AND decision_id=?",
                               (int(decision), job_id, decision_id))
            if not decision:
                state["stop_requested"] = True
                connection.execute("UPDATE jobs SET state_json=?,updated_at=? WHERE job_id=?",
                                   (self._json(state), time.time(), job_id))
                self._append_event(connection, job_id, "job_stop_requested", {
                    "status": row["status"], "reason": "legacy_boolean_false", "candidate_url": prior["candidate_url"],
                })
            else:
                connection.execute("UPDATE jobs SET state_json=?,updated_at=? WHERE job_id=?",
                                   (self._json(state), time.time(), job_id))
                self._append_event(connection, job_id, "decision_received", {
                    "decision_id": decision_id, "decision": True, "compatibility_noop": True,
                })
            connection.commit()
            return {"job_id": job_id, "decision_id": decision_id, "decision": decision,
                    "status": row["status"], "idempotent": False, "compatibility_noop": decision}
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def resolve_decision(self, job_id: str, decision_id: str, decision: bool) -> dict:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT status,state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            state = json.loads(row["state_json"])
            prior = state.get("decisions", {}).get(decision_id)
            if prior is not None:
                if prior is not decision:
                    raise DecisionConflict("A different decision was already recorded for this decision_id")
                connection.commit()
                return {"job_id": job_id, "decision_id": decision_id, "decision": decision,
                        "status": row["status"], "idempotent": True}
            if row["status"] in TERMINAL or state.get("pending_decision_id") != decision_id:
                raise DecisionConflict("decision_id is unknown, stale, or no longer pending")
            state.setdefault("decisions", {})[decision_id] = decision
            state["pending_decision_id"] = None
            if decision:
                state["batch_index"] += 1
                if state["batch_index"] >= len(state["batch_urls"]):
                    batch_count = len(state["batch_urls"])
                    state["url_queue"] = state["url_queue"][batch_count:]
                    state["batch_urls"] = []
                    state["batch_index"] = 0
                    state["current_stage"] = "searching"
                else:
                    state["current_stage"] = "crawling"
                status = "queued" if row["status"] == "queued" else "running"
            else:
                state["stop_requested"] = True
                state["status"] = "stopped"
                state["current_stage"] = "stopped"
                state["completion_reason"] = "external_approval_declined"
                status = "stopped"
            now = time.time()
            connection.execute("UPDATE jobs SET status=?,state_json=?,updated_at=?,expires_at=? WHERE job_id=?",
                (status, self._json(state), now, now + self.retention_seconds, job_id))
            self._append_event(connection, job_id, "decision_received", {
                "decision_id": decision_id, "decision": decision, "next_stage": state["current_stage"],
            })
            if not decision:
                self._append_event(connection, job_id, "job_stopped", {"job_id": job_id, "status": "stopped", "decision_id": decision_id})
            connection.commit()
            return {"job_id": job_id, "decision_id": decision_id, "decision": decision,
                    "status": status, "idempotent": False}
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def get(self, job_id: str) -> dict | None:
        with self._db() as connection:
            row = connection.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            return None
        return {
            "job_id": row["job_id"],
            "status": row["status"],
            "state": json.loads(row["state_json"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "last_event_id": row["last_event_id"],
        }

    def recover(self) -> list[str]:
        """Requeue interrupted work and return all jobs that should be scheduled."""
        connection = self._connect()
        recovered: list[str] = []
        try:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute("SELECT job_id,status,state_json FROM jobs WHERE status IN ('queued','running')").fetchall()
            now = time.time()
            for row in rows:
                job_id = str(row["job_id"])
                state = json.loads(row["state_json"])
                was_running = row["status"] == "running"
                if was_running:
                    state["status"] = "queued"
                    prior_stage = state.get("current_stage", "searching")
                    state["resume_stage"] = prior_stage
                    if int(state.get("job_model_version", 3)) >= 4:
                        state["current_stage"] = prior_stage
                    else:
                        state["current_stage"] = "queued"
                    for website in state.get("website_statuses", {}).values():
                        if website.get("status") == "processing":
                            website["status"] = "pending"
                    for page in state.get("page_statuses", {}).values():
                        if page.get("status") == "processing":
                            page["status"] = "pending"
                    state["active_urls"] = []
                    state["active_url"] = None
                    if state.get("stop_requested"):
                        state["status"] = "stopped"
                        state["current_stage"] = "stopped"
                        connection.execute("UPDATE jobs SET status='stopped',state_json=?,updated_at=? WHERE job_id=?", (self._json(state), now, job_id))
                        self._append_event(connection, job_id, "job_stopped", {"job_id": job_id, "status": "stopped", "reason": "application_restart"})
                        continue
                    connection.execute(
                        "UPDATE jobs SET status='queued',state_json=?,updated_at=? WHERE job_id=?",
                        (self._json(state), now, job_id),
                    )
                    self._append_event(connection, job_id, "job_resumed", {"status": "queued", "reason": "application_restart"})
                recovered.append(job_id)
            connection.commit()
            return recovered
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def requeue_running(self) -> None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute("SELECT job_id,state_json FROM jobs WHERE status='running'").fetchall()
            now = time.time()
            for row in rows:
                job_id = str(row["job_id"])
                state = json.loads(row["state_json"])
                state["status"] = "queued"
                prior_stage = state.get("current_stage", "searching")
                state["resume_stage"] = prior_stage
                if int(state.get("job_model_version", 3)) >= 4:
                    state["current_stage"] = prior_stage
                else:
                    state["current_stage"] = "queued"
                for website in state.get("website_statuses", {}).values():
                    if website.get("status") == "processing":
                        website["status"] = "pending"
                for page in state.get("page_statuses", {}).values():
                    if page.get("status") == "processing":
                        page["status"] = "pending"
                state["active_urls"] = []
                state["active_url"] = None
                connection.execute(
                    "UPDATE jobs SET status='queued',state_json=?,updated_at=? WHERE job_id=?",
                    (self._json(state), now, job_id),
                )
                self._append_event(connection, job_id, "job_requeued", {"status": "queued", "reason": "graceful_shutdown"})
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def events_after(self, job_id: str, event_id: int, limit: int = 200) -> list[dict]:
        with self._db() as connection:
            rows = connection.execute(
                "SELECT event_id,event_type,data_json,created_at FROM job_events WHERE job_id=? AND event_id>? ORDER BY event_id LIMIT ?",
                (job_id, event_id, limit),
            ).fetchall()
        return [
            {"id": row["event_id"], "event": row["event_type"], "data": json.loads(row["data_json"]), "created_at": row["created_at"]}
            for row in rows
        ]

    def cleanup_expired(self) -> int:
        with self._db() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                "DELETE FROM jobs WHERE status IN ('completed','failed','stopped') AND expires_at<?", (time.time(),)
            )
            deleted = cursor.rowcount
            connection.commit()
        return deleted
