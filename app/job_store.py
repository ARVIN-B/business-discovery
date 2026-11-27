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
                """
            )
            self._migrate_legacy_jobs(connection)
            self._migrate_job_model(connection)
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
            if int(state.get("job_model_version", 3)) < 4:
                state["current_stage"] = "preparing"
            elif state.get("current_stage") in {"queued", "preparing"}:
                state["current_stage"] = "searching"
            now = time.time()
            connection.execute(
                "UPDATE jobs SET status='running',state_json=?,updated_at=? WHERE job_id=?",
                (self._json(state), now, job_id),
            )
            self._append_event(connection, job_id, "progress", {"status": "running", "current_stage": state["current_stage"]})
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
                connection.execute("UPDATE jobs SET status='stopped',state_json=?,updated_at=? WHERE job_id=?", (self._json(state), now, job_id))
                self._append_event(connection, job_id, "job_stopped", {"job_id": job_id, "status": "stopped", **self._state_summary(state)})
                status = "stopped"
            else:
                if int(state.get("job_model_version", 3)) >= 4 and state.get("current_stage") == "awaiting_approval":
                    state["status"] = state["current_stage"] = "stopped"
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

    def record_search_page(self, job_id: str, query: str, next_page: int, candidates: list[dict], exhausted: bool) -> dict:
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
            state["discovered_page_count"] += len(added)
            state["search_exhausted"] = False
            state["search_retry_at"] = None
            now = time.time()
            connection.execute("UPDATE jobs SET state_json=?,updated_at=? WHERE job_id=?", (self._json(state), now, job_id))
            self._append_event(connection, job_id, "search_progress", {
                "query": query, "search_page_consumed": next_page - 1, "new_urls": len(added),
                "queued_urls": len(state["url_queue"]), "search_batch_size": state["search_batch_size"],
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
            state["query_index"] += 1
            state["search_exhausted"] = state["query_index"] >= len(state["search_queries"])
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

    def set_search_retry(self, job_id: str, retry_at: float, error: str) -> None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            state = json.loads(row["state_json"])
            state["search_retry_at"] = retry_at
            state["last_search_error"] = error[:500]
            warning = {"stage": "searching", "message": error[:500], "retry_at": retry_at}
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
