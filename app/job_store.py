from __future__ import annotations

import json
import os
import re
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

TERMINAL = {"completed", "failed"}


class IdempotencyConflict(ValueError):
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
                    "requested_count": max(int(state.get("requested_count", 1)), len(records)),
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
                "job_id": job_id, "status": "queued", "current_stage": "queued",
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
            state["status"] = "running"
            state["current_stage"] = "preparing"
            now = time.time()
            connection.execute(
                "UPDATE jobs SET status='running',state_json=?,updated_at=? WHERE job_id=?",
                (self._json(state), now, job_id),
            )
            self._append_event(connection, job_id, "progress", {"status": "running", "current_stage": "preparing"})
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
                    state["resume_stage"] = state.get("current_stage", "searching")
                    state["current_stage"] = "queued"
                    for website in state.get("website_statuses", {}).values():
                        if website.get("status") == "processing":
                            website["status"] = "pending"
                    for page in state.get("page_statuses", {}).values():
                        if page.get("status") == "processing":
                            page["status"] = "pending"
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
                state["resume_stage"] = state.get("current_stage", "searching")
                state["current_stage"] = "queued"
                for website in state.get("website_statuses", {}).values():
                    if website.get("status") == "processing":
                        website["status"] = "pending"
                for page in state.get("page_statuses", {}).values():
                    if page.get("status") == "processing":
                        page["status"] = "pending"
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
                "DELETE FROM jobs WHERE status IN ('completed','failed') AND expires_at<?", (time.time(),)
            )
            deleted = cursor.rowcount
            connection.commit()
        return deleted
