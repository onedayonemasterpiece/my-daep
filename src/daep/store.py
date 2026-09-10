from __future__ import annotations

import hashlib
import json
import re
import shutil
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .models import JobSpec, LaunchAttempt


ACTIVE_ATTEMPT_STATES = ("LAUNCHING", "RUNNING", "WAITING_MODEL", "STOPPING")
TERMINAL_ATTEMPT_STATES = ("COMPLETED", "FAILED", "CANCELLED")


def _now() -> float:
    return time.time()


def _slug(value: str, limit: int = 28) -> str:
    value = re.sub(r"[^a-zA-Z0-9._-]+", "-", value.strip()).strip("-.").lower()
    return (value or "task")[:limit]


class Store:
    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.migrate()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    @contextmanager
    def tx(self, *, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
                yield conn
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()

    def migrate(self) -> None:
        schema = """
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            idempotency_key TEXT NOT NULL UNIQUE,
            repository TEXT NOT NULL,
            base_sha TEXT NOT NULL,
            base_branch TEXT NOT NULL,
            integration_branch TEXT NOT NULL,
            status TEXT NOT NULL,
            max_workers INTEGER NOT NULL CHECK(max_workers BETWEEN 1 AND 4),
            plan_version INTEGER NOT NULL DEFAULT 1,
            coordinator_session_id TEXT,
            coordinator_server_url TEXT,
            coordinator_cursor INTEGER NOT NULL DEFAULT 0,
            result_sha TEXT,
            result_pr_number INTEGER,
            result_pr_url TEXT,
            checks_json TEXT,
            error TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY,
            job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
            ordinal INTEGER NOT NULL,
            name TEXT NOT NULL,
            instruction TEXT NOT NULL,
            model TEXT NOT NULL,
            fallback_models_json TEXT NOT NULL,
            depends_json TEXT NOT NULL,
            status TEXT NOT NULL,
            max_attempts INTEGER NOT NULL CHECK(max_attempts BETWEEN 1 AND 8),
            current_generation INTEGER NOT NULL DEFAULT 0,
            result_sha TEXT,
            result_branch TEXT,
            error TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            UNIQUE(job_id, name),
            UNIQUE(job_id, ordinal)
        );
        CREATE TABLE IF NOT EXISTS attempts (
            id TEXT PRIMARY KEY,
            job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
            task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
            generation INTEGER NOT NULL,
            branch TEXT NOT NULL,
            base_sha TEXT NOT NULL,
            model TEXT NOT NULL,
            provider_ref TEXT NOT NULL,
            status TEXT NOT NULL,
            worker_id TEXT,
            lease_until REAL,
            last_heartbeat REAL,
            checkpoint_sha TEXT,
            checkpoint_seq INTEGER NOT NULL DEFAULT 0,
            launch_intent_at REAL NOT NULL,
            launched_at REAL,
            finished_at REAL,
            reason TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            UNIQUE(task_id, generation),
            UNIQUE(provider_ref)
        );
        CREATE TABLE IF NOT EXISTS effects (
            id TEXT PRIMARY KEY,
            effect_key TEXT NOT NULL UNIQUE,
            kind TEXT NOT NULL,
            job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
            task_id TEXT REFERENCES tasks(id) ON DELETE CASCADE,
            attempt_id TEXT REFERENCES attempts(id) ON DELETE CASCADE,
            status TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            result_json TEXT,
            error TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_key TEXT NOT NULL UNIQUE,
            job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
            task_id TEXT REFERENCES tasks(id) ON DELETE CASCADE,
            attempt_id TEXT REFERENCES attempts(id) ON DELETE CASCADE,
            seq INTEGER,
            kind TEXT NOT NULL,
            significant INTEGER NOT NULL DEFAULT 0,
            payload_json TEXT NOT NULL,
            created_at REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_events_job_id ON events(job_id, id);
        CREATE INDEX IF NOT EXISTS idx_attempts_job_status ON attempts(job_id, status);
        CREATE TABLE IF NOT EXISTS commands (
            id TEXT PRIMARY KEY,
            job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
            task_id TEXT REFERENCES tasks(id) ON DELETE CASCADE,
            attempt_id TEXT REFERENCES attempts(id) ON DELETE CASCADE,
            kind TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at REAL NOT NULL,
            acked_at REAL
        );
        CREATE INDEX IF NOT EXISTS idx_commands_attempt_status ON commands(attempt_id, status, created_at);
        """
        conn = self._connect()
        try:
            conn.executescript(schema)
        finally:
            conn.close()

    def submit_job(self, spec: JobSpec) -> tuple[dict[str, Any], bool]:
        if not spec.tasks:
            raise ValueError("a distributed job needs at least one task")
        if not 1 <= spec.max_workers <= 4:
            raise ValueError("max_workers must be in range 1..4")
        task_names = [t.name for t in spec.tasks]
        if len(task_names) != len(set(task_names)):
            raise ValueError("task names must be unique")
        names = set(task_names)
        for task in spec.tasks:
            unknown = set(task.depends_on) - names
            if unknown:
                raise ValueError(f"task {task.name} has unknown dependencies: {sorted(unknown)}")
            if task.name in task.depends_on:
                raise ValueError(f"task {task.name} cannot depend on itself")
            if not 1 <= task.max_attempts <= 8:
                raise ValueError("max_attempts must be in range 1..8")
        now = _now()
        job_id = f"job_{uuid.uuid4().hex[:16]}"
        integration_branch = f"daep/result/{job_id}"
        with self.tx(immediate=True) as conn:
            existing = conn.execute(
                "SELECT * FROM jobs WHERE idempotency_key=?", (spec.idempotency_key,)
            ).fetchone()
            if existing:
                if (
                    existing["repository"] != spec.repository
                    or existing["base_sha"] != spec.base_sha
                    or existing["base_branch"] != spec.base_branch
                ):
                    raise ValueError("idempotency key already belongs to a different job input")
                return self._job_snapshot_conn(conn, existing["id"]), False
            conn.execute(
                """
                INSERT INTO jobs(
                    id,idempotency_key,repository,base_sha,base_branch,integration_branch,status,
                    max_workers,coordinator_session_id,coordinator_server_url,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    job_id,
                    spec.idempotency_key,
                    spec.repository,
                    spec.base_sha,
                    spec.base_branch,
                    integration_branch,
                    "RUNNING",
                    spec.max_workers,
                    spec.coordinator_session_id,
                    spec.coordinator_server_url,
                    now,
                    now,
                ),
            )
            for ordinal, task in enumerate(spec.tasks):
                conn.execute(
                    """
                    INSERT INTO tasks(
                        id,job_id,ordinal,name,instruction,model,fallback_models_json,depends_json,
                        status,max_attempts,created_at,updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        f"task_{uuid.uuid4().hex[:16]}",
                        job_id,
                        ordinal,
                        task.name,
                        task.instruction,
                        task.model,
                        json.dumps(task.fallback_models),
                        json.dumps(task.depends_on),
                        "PENDING",
                        task.max_attempts,
                        now,
                        now,
                    ),
                )
            self._event_conn(
                conn,
                event_key=f"job:{job_id}:submitted",
                job_id=job_id,
                kind="job_submitted",
                payload={"repository": spec.repository, "base_sha": spec.base_sha},
                significant=True,
            )
            return self._job_snapshot_conn(conn, job_id), True

    def _job_snapshot_conn(self, conn: sqlite3.Connection, job_id: str) -> dict[str, Any]:
        job = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if job is None:
            raise KeyError(job_id)
        tasks = conn.execute("SELECT * FROM tasks WHERE job_id=? ORDER BY ordinal", (job_id,)).fetchall()
        attempts = conn.execute(
            "SELECT * FROM attempts WHERE job_id=? ORDER BY created_at, generation", (job_id,)
        ).fetchall()
        return {
            "job": self._row(job),
            "tasks": [self._task_row(row) for row in tasks],
            "attempts": [self._row(row) for row in attempts],
        }

    @staticmethod
    def _row(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return dict(row)

    @staticmethod
    def _task_row(row: sqlite3.Row) -> dict[str, Any]:
        item = dict(row)
        item["fallback_models"] = json.loads(item.pop("fallback_models_json"))
        item["depends_on"] = json.loads(item.pop("depends_json"))
        return item

    def get_job(self, job_id: str) -> dict[str, Any]:
        with self.tx() as conn:
            return self._job_snapshot_conn(conn, job_id)

    def list_running_jobs(self) -> list[str]:
        with self.tx() as conn:
            rows = conn.execute(
                "SELECT id FROM jobs WHERE status IN ('RUNNING','AWAITING_INTEGRATION','CANCELLING') ORDER BY created_at"
            ).fetchall()
            return [str(row["id"]) for row in rows]

    def active_attempt_count(self, conn: sqlite3.Connection, job_id: str | None = None) -> int:
        placeholders = ",".join("?" for _ in ACTIVE_ATTEMPT_STATES)
        params: list[Any] = list(ACTIVE_ATTEMPT_STATES)
        sql = f"SELECT COUNT(*) AS n FROM attempts WHERE status IN ({placeholders})"
        if job_id:
            sql += " AND job_id=?"
            params.append(job_id)
        return int(conn.execute(sql, params).fetchone()["n"])

    def global_active_attempt_count(self) -> int:
        with self.tx() as conn:
            return self.active_attempt_count(conn)

    def _deps_satisfied(self, conn: sqlite3.Connection, task: sqlite3.Row) -> bool:
        deps = json.loads(task["depends_json"])
        if not deps:
            return True
        placeholders = ",".join("?" for _ in deps)
        rows = conn.execute(
            f"SELECT name,status FROM tasks WHERE job_id=? AND name IN ({placeholders})",
            [task["job_id"], *deps],
        ).fetchall()
        return len(rows) == len(deps) and all(row["status"] == "COMPLETED" for row in rows)

    def prepare_launches(self, *, global_limit: int, provider_ref_factory) -> list[LaunchAttempt]:
        if not 1 <= global_limit <= 4:
            raise ValueError("global_limit must be in range 1..4")
        prepared: list[LaunchAttempt] = []
        with self.tx(immediate=True) as conn:
            global_free = global_limit - self.active_attempt_count(conn)
            if global_free <= 0:
                return []
            jobs = conn.execute(
                "SELECT * FROM jobs WHERE status='RUNNING' ORDER BY created_at"
            ).fetchall()
            for job in jobs:
                if global_free <= 0:
                    break
                job_free = int(job["max_workers"]) - self.active_attempt_count(conn, job["id"])
                if job_free <= 0:
                    continue
                tasks = conn.execute(
                    """
                    SELECT * FROM tasks
                    WHERE job_id=? AND status IN ('PENDING','RETRY')
                    ORDER BY ordinal
                    """,
                    (job["id"],),
                ).fetchall()
                for task in tasks:
                    if global_free <= 0 or job_free <= 0:
                        break
                    if not self._deps_satisfied(conn, task):
                        continue
                    generation = int(task["current_generation"]) + 1
                    if generation > int(task["max_attempts"]):
                        conn.execute(
                            "UPDATE tasks SET status='FAILED',error=?,updated_at=? WHERE id=?",
                            ("attempt budget exhausted", _now(), task["id"]),
                        )
                        continue
                    previous = conn.execute(
                        "SELECT checkpoint_sha FROM attempts WHERE task_id=? AND checkpoint_sha IS NOT NULL ORDER BY generation DESC LIMIT 1",
                        (task["id"],),
                    ).fetchone()
                    base_sha = str(previous["checkpoint_sha"]) if previous else str(job["base_sha"])
                    models = [task["model"], *json.loads(task["fallback_models_json"])]
                    model = str(models[min(generation - 1, len(models) - 1)])
                    attempt_id = f"att_{uuid.uuid4().hex[:16]}"
                    branch = f"daep/work/{job['id']}/{_slug(task['name'])}/a{generation}"
                    provider_ref = str(provider_ref_factory(job["id"], task["name"], generation, attempt_id))
                    now = _now()
                    conn.execute(
                        """
                        INSERT INTO attempts(
                            id,job_id,task_id,generation,branch,base_sha,model,provider_ref,status,
                            launch_intent_at,created_at,updated_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            attempt_id,
                            job["id"],
                            task["id"],
                            generation,
                            branch,
                            base_sha,
                            model,
                            provider_ref,
                            "LAUNCHING",
                            now,
                            now,
                            now,
                        ),
                    )
                    conn.execute(
                        "UPDATE tasks SET status='RUNNING',current_generation=?,updated_at=? WHERE id=?",
                        (generation, now, task["id"]),
                    )
                    effect_id = f"eff_{uuid.uuid4().hex[:16]}"
                    conn.execute(
                        """
                        INSERT INTO effects(id,effect_key,kind,job_id,task_id,attempt_id,status,payload_json,created_at,updated_at)
                        VALUES(?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            effect_id,
                            f"launch:{attempt_id}",
                            "provider_launch",
                            job["id"],
                            task["id"],
                            attempt_id,
                            "INTENT",
                            json.dumps({"provider_ref": provider_ref, "base_sha": base_sha, "branch": branch}),
                            now,
                            now,
                        ),
                    )
                    self._event_conn(
                        conn,
                        event_key=f"attempt:{attempt_id}:prepared",
                        job_id=job["id"],
                        task_id=task["id"],
                        attempt_id=attempt_id,
                        kind="attempt_prepared",
                        payload={"generation": generation, "base_sha": base_sha, "branch": branch, "model": model},
                    )
                    prepared.append(
                        LaunchAttempt(
                            id=attempt_id,
                            job_id=str(job["id"]),
                            task_id=str(task["id"]),
                            task_name=str(task["name"]),
                            repository=str(job["repository"]),
                            base_sha=base_sha,
                            branch=branch,
                            generation=generation,
                            model=model,
                            instruction=str(task["instruction"]),
                            provider_ref=provider_ref,
                        )
                    )
                    global_free -= 1
                    job_free -= 1
        return prepared

    def mark_launch_success(self, attempt_id: str, result: dict[str, Any] | None = None, *, startup_lease_seconds: int = 300) -> None:
        now = _now()
        with self.tx(immediate=True) as conn:
            row = conn.execute("SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()
            if row is None:
                raise KeyError(attempt_id)
            if row["status"] not in ("LAUNCHING", "RUNNING"):
                return
            conn.execute(
                "UPDATE attempts SET status='RUNNING',launched_at=COALESCE(launched_at,?),lease_until=COALESCE(lease_until,?),updated_at=? WHERE id=?",
                (now, now + startup_lease_seconds, now, attempt_id),
            )
            conn.execute(
                "UPDATE effects SET status='DONE',result_json=?,updated_at=? WHERE effect_key=?",
                (json.dumps(result or {}), now, f"launch:{attempt_id}"),
            )
            self._event_conn(
                conn,
                event_key=f"attempt:{attempt_id}:launched",
                job_id=row["job_id"],
                task_id=row["task_id"],
                attempt_id=attempt_id,
                kind="attempt_launched",
                payload={"provider_ref": row["provider_ref"]},
                significant=True,
            )

    def mark_launch_error(self, attempt_id: str, error: str, *, retryable: bool = True) -> None:
        now = _now()
        with self.tx(immediate=True) as conn:
            row = conn.execute("SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()
            if row is None:
                raise KeyError(attempt_id)
            conn.execute(
                "UPDATE effects SET status='FAILED',error=?,updated_at=? WHERE effect_key=?",
                (error[:2000], now, f"launch:{attempt_id}"),
            )
            self._fail_attempt_conn(conn, row, reason=f"launch: {error}", retryable=retryable)

    def register_worker(self, attempt_id: str, worker_id: str, lease_seconds: int) -> dict[str, Any]:
        now = _now()
        with self.tx(immediate=True) as conn:
            row = conn.execute("SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()
            if row is None:
                raise KeyError(attempt_id)
            if row["status"] not in ("LAUNCHING", "RUNNING", "WAITING_MODEL"):
                raise ValueError(f"attempt is not registerable: {row['status']}")
            if row["worker_id"] and row["worker_id"] != worker_id:
                raise ValueError("attempt already belongs to another worker identity")
            conn.execute(
                """
                UPDATE attempts SET worker_id=?,status='RUNNING',last_heartbeat=?,lease_until=?,
                    launched_at=COALESCE(launched_at,?),updated_at=? WHERE id=?
                """,
                (worker_id, now, now + lease_seconds, now, now, attempt_id),
            )
            self._event_conn(
                conn,
                event_key=f"attempt:{attempt_id}:worker:{worker_id}:registered",
                job_id=row["job_id"],
                task_id=row["task_id"],
                attempt_id=attempt_id,
                kind="worker_registered",
                payload={"worker_id": worker_id},
                significant=True,
            )
            return self._bootstrap_conn(conn, attempt_id)

    def heartbeat(self, attempt_id: str, worker_id: str, lease_seconds: int, payload: dict[str, Any]) -> None:
        now = _now()
        with self.tx(immediate=True) as conn:
            row = conn.execute("SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()
            if row is None:
                raise KeyError(attempt_id)
            if row["worker_id"] != worker_id:
                raise ValueError("worker identity mismatch")
            if row["status"] not in ("RUNNING", "WAITING_MODEL", "STOPPING"):
                return
            conn.execute(
                "UPDATE attempts SET last_heartbeat=?,lease_until=?,updated_at=? WHERE id=?",
                (now, now + lease_seconds, now, attempt_id),
            )
            seq = int(payload.get("seq") or 0)
            if seq and seq % 10 == 0:
                self._event_conn(
                    conn,
                    event_key=f"attempt:{attempt_id}:heartbeat:{seq}",
                    job_id=row["job_id"],
                    task_id=row["task_id"],
                    attempt_id=attempt_id,
                    seq=seq,
                    kind="heartbeat",
                    payload=payload,
                )

    def _bootstrap_conn(self, conn: sqlite3.Connection, attempt_id: str) -> dict[str, Any]:
        row = conn.execute(
            """
            SELECT a.*, t.name AS task_name, t.instruction, t.fallback_models_json,
                   j.repository, j.base_branch, j.status AS job_status
            FROM attempts a
            JOIN tasks t ON t.id=a.task_id
            JOIN jobs j ON j.id=a.job_id
            WHERE a.id=?
            """,
            (attempt_id,),
        ).fetchone()
        if row is None:
            raise KeyError(attempt_id)
        return {
            "attempt_id": row["id"],
            "job_id": row["job_id"],
            "task_id": row["task_id"],
            "task_name": row["task_name"],
            "repository": row["repository"],
            "base_sha": row["base_sha"],
            "base_branch": row["base_branch"],
            "branch": row["branch"],
            "generation": row["generation"],
            "model": row["model"],
            "instruction": row["instruction"],
            "provider_ref": row["provider_ref"],
            "job_status": row["job_status"],
        }

    def bootstrap(self, attempt_id: str) -> dict[str, Any]:
        with self.tx() as conn:
            return self._bootstrap_conn(conn, attempt_id)

    def assert_worker(self, attempt_id: str, worker_id: str) -> dict[str, Any]:
        with self.tx() as conn:
            row = conn.execute("SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()
            if row is None:
                raise KeyError(attempt_id)
            if row["worker_id"] != worker_id:
                raise ValueError("worker identity mismatch")
            return dict(row)

    def record_event(
        self,
        *,
        attempt_id: str,
        event_key: str,
        seq: int | None,
        kind: str,
        payload: dict[str, Any],
        significant: bool = False,
    ) -> tuple[int, bool]:
        with self.tx(immediate=True) as conn:
            attempt = conn.execute("SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()
            if attempt is None:
                raise KeyError(attempt_id)
            return self._event_conn(
                conn,
                event_key=event_key,
                job_id=attempt["job_id"],
                task_id=attempt["task_id"],
                attempt_id=attempt_id,
                seq=seq,
                kind=kind,
                payload=payload,
                significant=significant,
            )

    def _event_conn(
        self,
        conn: sqlite3.Connection,
        *,
        event_key: str,
        job_id: str,
        kind: str,
        payload: dict[str, Any],
        task_id: str | None = None,
        attempt_id: str | None = None,
        seq: int | None = None,
        significant: bool = False,
    ) -> tuple[int, bool]:
        now = _now()
        try:
            cur = conn.execute(
                """
                INSERT INTO events(event_key,job_id,task_id,attempt_id,seq,kind,significant,payload_json,created_at)
                VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (event_key, job_id, task_id, attempt_id, seq, kind, int(significant), json.dumps(payload), now),
            )
            return int(cur.lastrowid), True
        except sqlite3.IntegrityError:
            row = conn.execute("SELECT id FROM events WHERE event_key=?", (event_key,)).fetchone()
            if row is None:
                raise
            return int(row["id"]), False

    def record_checkpoint(
        self,
        *,
        attempt_id: str,
        worker_id: str,
        seq: int,
        sha: str,
        branch: str,
        readback_sha: str,
    ) -> dict[str, Any]:
        if sha != readback_sha:
            raise ValueError("checkpoint remote readback does not match local SHA")
        now = _now()
        with self.tx(immediate=True) as conn:
            attempt = conn.execute("SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()
            if attempt is None:
                raise KeyError(attempt_id)
            if attempt["worker_id"] != worker_id:
                raise ValueError("worker identity mismatch")
            if branch != attempt["branch"]:
                raise ValueError("checkpoint branch does not belong to attempt")
            if int(seq) <= int(attempt["checkpoint_seq"]):
                return {"accepted": False, "checkpoint_sha": attempt["checkpoint_sha"], "duplicate": True}
            conn.execute(
                """
                UPDATE attempts SET checkpoint_sha=?,checkpoint_seq=?,updated_at=?
                WHERE id=?
                """,
                (sha, seq, now, attempt_id),
            )
            self._event_conn(
                conn,
                event_key=f"attempt:{attempt_id}:checkpoint:{seq}:{sha}",
                job_id=attempt["job_id"],
                task_id=attempt["task_id"],
                attempt_id=attempt_id,
                seq=seq,
                kind="checkpoint",
                payload={"sha": sha, "branch": branch, "readback_sha": readback_sha},
                significant=True,
            )
            return {"accepted": True, "checkpoint_sha": sha, "duplicate": False}

    def complete_attempt(self, attempt_id: str, worker_id: str, result_sha: str | None) -> None:
        now = _now()
        with self.tx(immediate=True) as conn:
            attempt = conn.execute("SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()
            if attempt is None:
                raise KeyError(attempt_id)
            if attempt["worker_id"] != worker_id:
                raise ValueError("worker identity mismatch")
            task = conn.execute("SELECT * FROM tasks WHERE id=?", (attempt["task_id"],)).fetchone()
            if task is None:
                raise KeyError(attempt["task_id"])
            if int(task["current_generation"]) != int(attempt["generation"]):
                self._event_conn(
                    conn,
                    event_key=f"attempt:{attempt_id}:late-result",
                    job_id=attempt["job_id"],
                    task_id=attempt["task_id"],
                    attempt_id=attempt_id,
                    kind="late_result_ignored",
                    payload={"result_sha": result_sha},
                    significant=True,
                )
                return
            accepted_sha = result_sha or attempt["checkpoint_sha"]
            if not accepted_sha:
                self._fail_attempt_conn(conn, attempt, reason="worker completed without a Git checkpoint", retryable=True)
                return
            conn.execute(
                "UPDATE attempts SET status='COMPLETED',finished_at=?,updated_at=? WHERE id=?",
                (now, now, attempt_id),
            )
            conn.execute(
                """
                UPDATE tasks SET status='COMPLETED',result_sha=?,result_branch=?,error=NULL,updated_at=?
                WHERE id=?
                """,
                (accepted_sha, attempt["branch"], now, attempt["task_id"]),
            )
            self._event_conn(
                conn,
                event_key=f"attempt:{attempt_id}:completed:{accepted_sha}",
                job_id=attempt["job_id"],
                task_id=attempt["task_id"],
                attempt_id=attempt_id,
                kind="task_completed",
                payload={"result_sha": accepted_sha, "branch": attempt["branch"]},
                significant=True,
            )
            remaining = conn.execute(
                "SELECT COUNT(*) AS n FROM tasks WHERE job_id=? AND status!='COMPLETED'",
                (attempt["job_id"],),
            ).fetchone()["n"]
            if int(remaining) == 0:
                conn.execute(
                    "UPDATE jobs SET status='AWAITING_INTEGRATION',updated_at=? WHERE id=? AND status='RUNNING'",
                    (now, attempt["job_id"]),
                )
                self._event_conn(
                    conn,
                    event_key=f"job:{attempt['job_id']}:ready-for-integration",
                    job_id=attempt["job_id"],
                    kind="integration_ready",
                    payload={"task_count": conn.execute("SELECT COUNT(*) n FROM tasks WHERE job_id=?", (attempt["job_id"],)).fetchone()["n"]},
                    significant=True,
                )

    def fail_attempt(self, attempt_id: str, reason: str, *, retryable: bool = True) -> None:
        with self.tx(immediate=True) as conn:
            row = conn.execute("SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()
            if row is None:
                raise KeyError(attempt_id)
            self._fail_attempt_conn(conn, row, reason=reason, retryable=retryable)

    def _fail_attempt_conn(
        self, conn: sqlite3.Connection, attempt: sqlite3.Row, *, reason: str, retryable: bool
    ) -> None:
        if attempt["status"] in TERMINAL_ATTEMPT_STATES:
            return
        now = _now()
        conn.execute(
            "UPDATE attempts SET status='FAILED',reason=?,finished_at=?,updated_at=? WHERE id=?",
            (reason[:2000], now, now, attempt["id"]),
        )
        task = conn.execute("SELECT * FROM tasks WHERE id=?", (attempt["task_id"],)).fetchone()
        exhausted = int(task["current_generation"]) >= int(task["max_attempts"])
        next_status = "FAILED" if exhausted or not retryable else "RETRY"
        conn.execute(
            "UPDATE tasks SET status=?,error=?,updated_at=? WHERE id=?",
            (next_status, reason[:2000], now, attempt["task_id"]),
        )
        self._event_conn(
            conn,
            event_key=f"attempt:{attempt['id']}:failed:{hashlib.sha1(reason.encode()).hexdigest()[:10]}",
            job_id=attempt["job_id"],
            task_id=attempt["task_id"],
            attempt_id=attempt["id"],
            kind="attempt_failed",
            payload={
                "reason": reason,
                "retryable": bool(retryable and not exhausted),
                "checkpoint_sha": attempt["checkpoint_sha"],
                "generation": attempt["generation"],
            },
            significant=True,
        )
        if next_status == "FAILED":
            conn.execute(
                "UPDATE jobs SET status='FAILED',error=?,updated_at=? WHERE id=? AND status='RUNNING'",
                (f"task {task['name']} failed: {reason[:1000]}", now, attempt["job_id"]),
            )

    def mark_model_quota(self, attempt_id: str, reason: str) -> None:
        with self.tx(immediate=True) as conn:
            attempt = conn.execute("SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()
            if attempt is None:
                raise KeyError(attempt_id)
            self._event_conn(
                conn,
                event_key=f"attempt:{attempt_id}:model-quota:{attempt['generation']}",
                job_id=attempt["job_id"],
                task_id=attempt["task_id"],
                attempt_id=attempt_id,
                kind="model_quota",
                payload={"reason": reason, "checkpoint_sha": attempt["checkpoint_sha"]},
                significant=True,
            )
            self._fail_attempt_conn(conn, attempt, reason=f"model quota: {reason}", retryable=True)

    def stale_attempts(self, now: float | None = None) -> list[dict[str, Any]]:
        now = _now() if now is None else now
        placeholders = ",".join("?" for _ in ACTIVE_ATTEMPT_STATES)
        with self.tx() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM attempts
                WHERE status IN ({placeholders}) AND lease_until IS NOT NULL AND lease_until < ?
                ORDER BY lease_until
                """,
                [*ACTIVE_ATTEMPT_STATES, now],
            ).fetchall()
            return [dict(row) for row in rows]

    def launch_intents(self, older_than: float) -> list[dict[str, Any]]:
        with self.tx() as conn:
            rows = conn.execute(
                """
                SELECT a.* FROM attempts a
                JOIN effects e ON e.attempt_id=a.id AND e.effect_key=('launch:' || a.id)
                WHERE a.status='LAUNCHING' AND e.status='INTENT' AND a.launch_intent_at < ?
                ORDER BY a.launch_intent_at
                """,
                (older_than,),
            ).fetchall()
            return [dict(r) for r in rows]

    def events(self, job_id: str, after: int = 0, limit: int = 500, *, significant_only: bool = False) -> list[dict[str, Any]]:
        with self.tx() as conn:
            sql = "SELECT * FROM events WHERE job_id=? AND id>?"
            params: list[Any] = [job_id, after]
            if significant_only:
                sql += " AND significant=1"
            sql += " ORDER BY id LIMIT ?"
            params.append(limit)
            rows = conn.execute(sql, params).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                item["payload"] = json.loads(item.pop("payload_json"))
                result.append(item)
            return result

    def queue_command(
        self,
        *,
        job_id: str,
        kind: str,
        payload: dict[str, Any],
        task_id: str | None = None,
        attempt_id: str | None = None,
    ) -> str:
        command_id = f"cmd_{uuid.uuid4().hex[:16]}"
        now = _now()
        with self.tx(immediate=True) as conn:
            if attempt_id is None and task_id is not None:
                row = conn.execute(
                    "SELECT id FROM attempts WHERE task_id=? AND status IN ('LAUNCHING','RUNNING','WAITING_MODEL','STOPPING') ORDER BY generation DESC LIMIT 1",
                    (task_id,),
                ).fetchone()
                attempt_id = str(row["id"]) if row else None
            conn.execute(
                "INSERT INTO commands(id,job_id,task_id,attempt_id,kind,payload_json,status,created_at) VALUES(?,?,?,?,?,?,?,?)",
                (command_id, job_id, task_id, attempt_id, kind, json.dumps(payload), "PENDING", now),
            )
            self._event_conn(
                conn,
                event_key=f"command:{command_id}:queued",
                job_id=job_id,
                task_id=task_id,
                attempt_id=attempt_id,
                kind="command_queued",
                payload={"command_id": command_id, "kind": kind},
            )
        return command_id

    def pending_commands(self, attempt_id: str, limit: int = 50) -> list[dict[str, Any]]:
        with self.tx() as conn:
            rows = conn.execute(
                "SELECT * FROM commands WHERE attempt_id=? AND status='PENDING' ORDER BY created_at LIMIT ?",
                (attempt_id, limit),
            ).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                item["payload"] = json.loads(item.pop("payload_json"))
                result.append(item)
            return result

    def ack_command(self, attempt_id: str, command_id: str, result: dict[str, Any]) -> bool:
        now = _now()
        with self.tx(immediate=True) as conn:
            row = conn.execute(
                "SELECT * FROM commands WHERE id=? AND attempt_id=?", (command_id, attempt_id)
            ).fetchone()
            if row is None:
                raise KeyError(command_id)
            if row["status"] == "ACKED":
                return False
            conn.execute("UPDATE commands SET status='ACKED',acked_at=? WHERE id=?", (now, command_id))
            self._event_conn(
                conn,
                event_key=f"command:{command_id}:acked",
                job_id=row["job_id"],
                task_id=row["task_id"],
                attempt_id=attempt_id,
                kind="command_acked",
                payload={"command_id": command_id, "result": result},
                significant=True,
            )
            return True

    def request_cancel(self, job_id: str) -> list[str]:
        now = _now()
        with self.tx(immediate=True) as conn:
            job = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if job is None:
                raise KeyError(job_id)
            if job["status"] in ("COMPLETED", "FAILED", "CANCELLED"):
                return []
            conn.execute("UPDATE jobs SET status='CANCELLING',updated_at=? WHERE id=?", (now, job_id))
            attempts = conn.execute(
                "SELECT * FROM attempts WHERE job_id=? AND status IN ('LAUNCHING','RUNNING','WAITING_MODEL','STOPPING')",
                (job_id,),
            ).fetchall()
            command_ids: list[str] = []
            for attempt in attempts:
                conn.execute("UPDATE attempts SET status='STOPPING',updated_at=? WHERE id=?", (now, attempt["id"]))
                cid = f"cmd_{uuid.uuid4().hex[:16]}"
                conn.execute(
                    "INSERT INTO commands(id,job_id,task_id,attempt_id,kind,payload_json,status,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (cid, job_id, attempt["task_id"], attempt["id"], "stop", "{}", "PENDING", now),
                )
                command_ids.append(cid)
            self._event_conn(
                conn,
                event_key=f"job:{job_id}:cancel-requested",
                job_id=job_id,
                kind="cancel_requested",
                payload={"attempts": [a["id"] for a in attempts]},
                significant=True,
            )
            if not attempts:
                conn.execute("UPDATE jobs SET status='CANCELLED',updated_at=? WHERE id=?", (now, job_id))
            return command_ids

    def worker_stopped(self, attempt_id: str, worker_id: str, *, cancelled: bool = False, reason: str = "stopped") -> None:
        now = _now()
        with self.tx(immediate=True) as conn:
            attempt = conn.execute("SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()
            if attempt is None:
                raise KeyError(attempt_id)
            if attempt["worker_id"] != worker_id:
                raise ValueError("worker identity mismatch")
            job = conn.execute("SELECT * FROM jobs WHERE id=?", (attempt["job_id"],)).fetchone()
            status = "CANCELLED" if cancelled or job["status"] == "CANCELLING" else "FAILED"
            conn.execute(
                "UPDATE attempts SET status=?,reason=?,finished_at=?,updated_at=? WHERE id=?",
                (status, reason[:2000], now, now, attempt_id),
            )
            if status == "FAILED":
                task = conn.execute("SELECT * FROM tasks WHERE id=?", (attempt["task_id"],)).fetchone()
                next_status = "RETRY" if int(task["current_generation"]) < int(task["max_attempts"]) else "FAILED"
                conn.execute(
                    "UPDATE tasks SET status=?,error=?,updated_at=? WHERE id=?",
                    (next_status, reason[:2000], now, attempt["task_id"]),
                )
            self._event_conn(
                conn,
                event_key=f"attempt:{attempt_id}:stopped:{status}",
                job_id=attempt["job_id"],
                task_id=attempt["task_id"],
                attempt_id=attempt_id,
                kind="worker_stopped",
                payload={"status": status, "reason": reason, "checkpoint_sha": attempt["checkpoint_sha"]},
                significant=True,
            )
            active = self.active_attempt_count(conn, attempt["job_id"])
            if job["status"] == "CANCELLING" and active == 0:
                conn.execute("UPDATE jobs SET status='CANCELLED',updated_at=? WHERE id=?", (now, attempt["job_id"]))
                conn.execute("UPDATE tasks SET status='CANCELLED',updated_at=? WHERE job_id=? AND status!='COMPLETED'", (now, attempt["job_id"]))

    def update_coordinator(self, job_id: str, *, session_id: str, server_url: str | None) -> None:
        with self.tx(immediate=True) as conn:
            if conn.execute("SELECT 1 FROM jobs WHERE id=?", (job_id,)).fetchone() is None:
                raise KeyError(job_id)
            conn.execute(
                "UPDATE jobs SET coordinator_session_id=?,coordinator_server_url=COALESCE(?,coordinator_server_url),updated_at=? WHERE id=?",
                (session_id, server_url, _now(), job_id),
            )

    def coordinator_delivery_state(self, job_id: str) -> dict[str, Any]:
        with self.tx() as conn:
            row = conn.execute(
                "SELECT coordinator_session_id,coordinator_server_url,coordinator_cursor FROM jobs WHERE id=?",
                (job_id,),
            ).fetchone()
            if row is None:
                raise KeyError(job_id)
            return dict(row)

    def advance_coordinator_cursor(self, job_id: str, event_id: int) -> None:
        with self.tx(immediate=True) as conn:
            conn.execute(
                "UPDATE jobs SET coordinator_cursor=MAX(coordinator_cursor,?),updated_at=? WHERE id=?",
                (event_id, _now(), job_id),
            )

    def finalize_job(
        self,
        job_id: str,
        *,
        result_sha: str,
        checks: list[dict[str, Any]],
        pr_number: int | None = None,
        pr_url: str | None = None,
    ) -> None:
        now = _now()
        with self.tx(immediate=True) as conn:
            job = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if job is None:
                raise KeyError(job_id)
            if job["status"] not in ("AWAITING_INTEGRATION", "COMPLETED"):
                raise ValueError(f"job not ready for finalization: {job['status']}")
            if not checks:
                raise ValueError("at least one real check result is required")
            conn.execute(
                """
                UPDATE jobs SET status='COMPLETED',result_sha=?,checks_json=?,result_pr_number=COALESCE(?,result_pr_number),
                    result_pr_url=COALESCE(?,result_pr_url),updated_at=? WHERE id=?
                """,
                (result_sha, json.dumps(checks), pr_number, pr_url, now, job_id),
            )
            self._event_conn(
                conn,
                event_key=f"job:{job_id}:completed:{result_sha}",
                job_id=job_id,
                kind="job_completed",
                payload={"result_sha": result_sha, "checks": checks, "pr_number": pr_number, "pr_url": pr_url},
                significant=True,
            )

    def export_job(self, job_id: str) -> dict[str, Any]:
        snapshot = self.get_job(job_id)
        events = self.events(job_id, after=0, limit=2000)
        job = snapshot["job"]
        if job.get("checks_json"):
            job["checks"] = json.loads(job.pop("checks_json"))
        else:
            job.pop("checks_json", None)
            job["checks"] = []
        return {
            "schema": "daep.handoff.v1",
            "job": job,
            "tasks": snapshot["tasks"],
            "attempts": snapshot["attempts"],
            "events": events,
            "next_actions": self._next_actions(snapshot),
        }

    @staticmethod
    def _next_actions(snapshot: dict[str, Any]) -> list[str]:
        status = snapshot["job"]["status"]
        if status == "AWAITING_INTEGRATION":
            return ["Integrate exact task result_sha commits", "Run project checks on the integrated SHA", "Finalize the DAEP job"]
        if status == "FAILED":
            return ["Inspect failed attempt and checkpoint", "Fix the concrete blocker in ChatGPT or resume with a new plan version"]
        if status == "RUNNING":
            return ["Continue following significant events"]
        return []

    def backup_to(self, destination_dir: Path | str) -> Path:
        destination = Path(destination_dir)
        destination.mkdir(parents=True, exist_ok=True)
        timestamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        target = destination / f"daep-{timestamp}.sqlite3"
        with self._lock:
            source_conn = self._connect()
            target_conn = sqlite3.connect(target)
            try:
                source_conn.backup(target_conn)
            finally:
                target_conn.close()
                source_conn.close()
        verify = sqlite3.connect(target)
        try:
            names = {r[0] for r in verify.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not {"jobs", "tasks", "attempts", "events"}.issubset(names):
                raise RuntimeError("backup verification failed: missing tables")
        finally:
            verify.close()
        return target

    def restore_from(self, backup_file: Path | str) -> None:
        source = Path(backup_file)
        if not source.is_file():
            raise FileNotFoundError(source)
        verify = sqlite3.connect(source)
        try:
            names = {r[0] for r in verify.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not {"jobs", "tasks", "attempts", "events"}.issubset(names):
                raise ValueError("restore source is not a DAEP database")
        finally:
            verify.close()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            shutil.copy2(source, self.path)
            self.migrate()
