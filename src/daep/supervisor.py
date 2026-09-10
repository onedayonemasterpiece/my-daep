from __future__ import annotations

import json
import threading
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Response
from pydantic import BaseModel, Field

from .config import Settings
from .coordinator import OpenCodeCoordinator
from .github_app import GitHubAppTokenBroker
from .models import JobSpec, TaskSpec
from .providers.kaggle import KaggleProvider
from .security import AttemptTokenSigner
from .store import Store
from .worker_bundle import render_worker_bundle


class TaskIn(BaseModel):
    name: str
    instruction: str
    model: str
    depends_on: list[str] = Field(default_factory=list)
    fallback_models: list[str] = Field(default_factory=list)
    max_attempts: int = 3


class JobIn(BaseModel):
    repository: str
    base_sha: str
    base_branch: str
    idempotency_key: str
    max_workers: int = 2
    coordinator_session_id: str | None = None
    coordinator_server_url: str | None = None
    tasks: list[TaskIn]


class WorkerBody(BaseModel):
    worker_id: str
    seq: int = 0
    payload: dict[str, Any] = Field(default_factory=dict)


class CheckpointBody(BaseModel):
    worker_id: str
    seq: int
    sha: str
    branch: str
    readback_sha: str


class EventBody(BaseModel):
    worker_id: str
    event_key: str
    seq: int | None = None
    kind: str
    significant: bool = False
    payload: dict[str, Any] = Field(default_factory=dict)


class CompleteBody(BaseModel):
    worker_id: str
    result_sha: str | None = None


class CommandAckBody(BaseModel):
    worker_id: str
    result: dict[str, Any] = Field(default_factory=dict)


class FinalizeBody(BaseModel):
    result_sha: str
    checks: list[dict[str, Any]]
    pr_number: int | None = None
    pr_url: str | None = None


class AttachBody(BaseModel):
    session_id: str
    server_url: str | None = None


class Runtime:
    def __init__(self, settings: Settings):
        settings.validate_runtime()
        self.settings = settings
        self.store = Store(settings.db_path)
        self.signer = AttemptTokenSigner(settings.worker_hmac_secret)
        self.github = None
        if settings.github_configured:
            self.github = GitHubAppTokenBroker(
                app_id=settings.github_app_id or "",
                private_key=settings.github_private_key or "",
                installation_id=settings.github_installation_id,
                api_url=settings.github_api_url,
                refresh_skew_seconds=settings.github_token_refresh_skew_seconds,
            )
        self.kaggle = None
        if settings.kaggle_username:
            self.kaggle = KaggleProvider(
                username=settings.kaggle_username,
                command=settings.kaggle_cli,
                timeout_seconds=settings.kaggle_cli_timeout_seconds,
            )
        self.coordinator = OpenCodeCoordinator(
            self.store,
            default_server_url=settings.coordinator_server_url,
            username=settings.coordinator_server_username,
            password=settings.coordinator_server_password,
        )
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="daep-scheduler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _loop(self) -> None:
        while not self._stop.wait(self.settings.scheduler_seconds):
            try:
                self.tick()
            except Exception:
                # Per-job failures are persisted below; an unexpected scheduler error must not kill the controller.
                continue

    def tick(self) -> dict[str, Any]:
        launched: list[str] = []
        stale: list[str] = []
        reconciled: list[str] = []
        if self.kaggle:
            for row in self.store.launch_intents(time.time() - max(30, self.settings.callback_grace_seconds)):
                state = self.kaggle.status(str(row["provider_ref"]))
                if state.state in {"RUNNING", "COMPLETE"}:
                    self.store.mark_launch_success(str(row["id"]), {"reconciled": True, "state": state.state})
                    reconciled.append(str(row["id"]))
                elif state.state in {"NOT_FOUND", "ERROR"}:
                    self.store.mark_launch_error(str(row["id"]), f"launch reconciliation: {state.state}", retryable=True)
            for row in self.store.stale_attempts():
                self.store.fail_attempt(str(row["id"]), "worker lease expired", retryable=True)
                stale.append(str(row["id"]))
            launches = self.store.prepare_launches(
                global_limit=self.settings.max_workers,
                provider_ref_factory=self.kaggle.identity_for,
            )
            for attempt in launches:
                try:
                    result = self.kaggle.launch(
                        attempt,
                        supervisor_url=self.settings.public_url,
                        attempt_token=self.signer.token_for(attempt.id),
                    )
                    self.store.mark_launch_success(attempt.id, result)
                    launched.append(attempt.id)
                except Exception as exc:
                    self.store.mark_launch_error(attempt.id, str(exc), retryable=True)
        deliveries: dict[str, Any] = {}
        for job_id in self.store.list_running_jobs():
            deliveries[job_id] = self.coordinator.deliver_pending(job_id)
        if self.settings.backup_dir:
            self.store.backup_to(self.settings.backup_dir)
        return {"launched": launched, "stale": stale, "reconciled": reconciled, "deliveries": deliveries}


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    runtime = Runtime(settings)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        runtime.start()
        try:
            yield
        finally:
            runtime.stop()

    app = FastAPI(title="my-daep", version="1", lifespan=lifespan)
    app.state.runtime = runtime

    def control(auth: str | None) -> None:
        expected = f"Bearer {settings.control_token}"
        if auth != expected:
            raise HTTPException(401, "invalid control credential")

    def worker(attempt_id: str, auth: str | None) -> None:
        if not auth or not auth.startswith("Bearer ") or not runtime.signer.verify(attempt_id, auth[7:]):
            raise HTTPException(401, "invalid attempt credential")

    @app.get("/healthz")
    def health() -> dict[str, Any]:
        return {"ok": True, "github": bool(runtime.github), "kaggle": bool(runtime.kaggle)}

    @app.post("/v1/jobs")
    def submit(body: JobIn, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        control(authorization)
        spec = JobSpec(
            repository=body.repository,
            base_sha=body.base_sha,
            base_branch=body.base_branch,
            idempotency_key=body.idempotency_key,
            max_workers=body.max_workers,
            coordinator_session_id=body.coordinator_session_id,
            coordinator_server_url=body.coordinator_server_url,
            tasks=tuple(
                TaskSpec(
                    name=t.name,
                    instruction=t.instruction,
                    model=t.model,
                    depends_on=tuple(t.depends_on),
                    fallback_models=tuple(t.fallback_models),
                    max_attempts=t.max_attempts,
                )
                for t in body.tasks
            ),
        )
        snapshot, created = runtime.store.submit_job(spec)
        return {"created": created, **snapshot}

    @app.get("/v1/jobs/{job_id}")
    def status(job_id: str, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        control(authorization)
        try:
            return runtime.store.get_job(job_id)
        except KeyError:
            raise HTTPException(404, "job not found")

    @app.get("/v1/jobs/{job_id}/events")
    def events(job_id: str, after: int = 0, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        control(authorization)
        return {"events": runtime.store.events(job_id, after=after)}

    @app.post("/v1/jobs/{job_id}/cancel")
    def cancel(job_id: str, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        control(authorization)
        return {"commands": runtime.store.request_cancel(job_id)}

    @app.post("/v1/jobs/{job_id}/attach")
    def attach(job_id: str, body: AttachBody, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        control(authorization)
        runtime.store.update_coordinator(job_id, session_id=body.session_id, server_url=body.server_url)
        return runtime.coordinator.deliver_pending(job_id)

    @app.post("/v1/jobs/{job_id}/finalize")
    def finalize(job_id: str, body: FinalizeBody, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        control(authorization)
        runtime.store.finalize_job(job_id, result_sha=body.result_sha, checks=body.checks, pr_number=body.pr_number, pr_url=body.pr_url)
        return runtime.store.get_job(job_id)

    @app.get("/v1/jobs/{job_id}/export")
    def export(job_id: str, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        control(authorization)
        return runtime.store.export_job(job_id)

    @app.post("/v1/tick")
    def tick(authorization: str | None = Header(default=None)) -> dict[str, Any]:
        control(authorization)
        return runtime.tick()

    @app.get("/v1/worker/{attempt_id}/runtime")
    def worker_runtime(attempt_id: str, authorization: str | None = Header(default=None)) -> Response:
        worker(attempt_id, authorization)
        return Response(render_worker_bundle(settings), media_type="text/x-python")

    @app.post("/v1/worker/{attempt_id}/register")
    def register(attempt_id: str, body: WorkerBody, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        worker(attempt_id, authorization)
        return runtime.store.register_worker(attempt_id, body.worker_id, settings.lease_seconds)

    @app.post("/v1/worker/{attempt_id}/heartbeat")
    def heartbeat(attempt_id: str, body: WorkerBody, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        worker(attempt_id, authorization)
        runtime.store.heartbeat(attempt_id, body.worker_id, settings.lease_seconds, {"seq": body.seq, **body.payload})
        return {"ok": True}

    @app.post("/v1/worker/{attempt_id}/event")
    def event(attempt_id: str, body: EventBody, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        worker(attempt_id, authorization)
        runtime.store.assert_worker(attempt_id, body.worker_id)
        event_id, created = runtime.store.record_event(
            attempt_id=attempt_id,
            event_key=body.event_key,
            seq=body.seq,
            kind=body.kind,
            payload=body.payload,
            significant=body.significant,
        )
        return {"id": event_id, "created": created}

    @app.post("/v1/worker/{attempt_id}/checkpoint")
    def checkpoint(attempt_id: str, body: CheckpointBody, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        worker(attempt_id, authorization)
        return runtime.store.record_checkpoint(
            attempt_id=attempt_id,
            worker_id=body.worker_id,
            seq=body.seq,
            sha=body.sha,
            branch=body.branch,
            readback_sha=body.readback_sha,
        )

    @app.get("/v1/worker/{attempt_id}/github-token")
    def github_token(attempt_id: str, worker_id: str, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        worker(attempt_id, authorization)
        attempt = runtime.store.assert_worker(attempt_id, worker_id)
        if not runtime.github:
            raise HTTPException(503, "GitHub App is not configured")
        bootstrap = runtime.store.bootstrap(attempt_id)
        token = runtime.github.get_token(str(bootstrap["repository"]))
        return {"token": token.token, "expires_at": token.expires_at, "repository": token.repository, "generation": attempt["generation"]}

    @app.get("/v1/worker/{attempt_id}/commands")
    def commands(attempt_id: str, worker_id: str, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        worker(attempt_id, authorization)
        runtime.store.assert_worker(attempt_id, worker_id)
        return {"commands": runtime.store.pending_commands(attempt_id)}

    @app.post("/v1/worker/{attempt_id}/commands/{command_id}/ack")
    def command_ack(attempt_id: str, command_id: str, body: CommandAckBody, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        worker(attempt_id, authorization)
        runtime.store.assert_worker(attempt_id, body.worker_id)
        return {"acked": runtime.store.ack_command(attempt_id, command_id, body.result)}

    @app.post("/v1/worker/{attempt_id}/complete")
    def complete(attempt_id: str, body: CompleteBody, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        worker(attempt_id, authorization)
        runtime.store.complete_attempt(attempt_id, body.worker_id, body.result_sha)
        return {"ok": True}

    @app.post("/v1/worker/{attempt_id}/model-quota")
    def model_quota(attempt_id: str, body: CompleteBody, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        worker(attempt_id, authorization)
        runtime.store.assert_worker(attempt_id, body.worker_id)
        runtime.store.mark_model_quota(attempt_id, "free usage exceeded")
        return {"ok": True}

    @app.post("/v1/worker/{attempt_id}/stopped")
    def stopped(attempt_id: str, body: WorkerBody, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        worker(attempt_id, authorization)
        runtime.store.worker_stopped(attempt_id, body.worker_id, cancelled=bool(body.payload.get("cancelled")), reason=str(body.payload.get("reason") or "stopped"))
        return {"ok": True}

    return app
