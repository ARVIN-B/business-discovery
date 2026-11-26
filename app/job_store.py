from __future__ import annotations

import json
import os
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
        os.chmod(self.path, 0o600)

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

    def _append_event(self, connection: sqlite3.Connection, job_id: str, event_type: str, data: dict) -> int:
        row = connection.execute("SELECT last_event_id FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            raise KeyError(job_id)
        event_id = int(row["last_event_id"]) + 1
        now = time.time()
        connection.execute(
            "INSERT INTO job_events(job_id,event_id,event_type,data_json,created_at) VALUES(?,?,?,?,?)",
            (job_id, event_id, event_type, self._json(data), now),
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
            self._append_event(connection, job_id, "job_started", {"job_id": job_id, "status": "queued", "current_stage": "queued"})
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
