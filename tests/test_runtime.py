from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from daep.config import Settings
from daep.models import JobSpec, TaskSpec
from daep.store import Store
from daep.supervisor import create_app


def spec(key: str = "k1", workers: int = 2) -> JobSpec:
    return JobSpec(
        repository="onedayonemasterpiece/example",
        base_sha="a" * 40,
        base_branch="master",
        idempotency_key=key,
        max_workers=workers,
        tasks=(
            TaskSpec(name="alpha", instruction="change alpha", model="opencode/a"),
            TaskSpec(name="beta", instruction="change beta", model="opencode/b"),
        ),
    )


def provider(job_id: str, task_name: str, generation: int, attempt_id: str) -> str:
    return f"owner/{task_name}-{generation}-{attempt_id[-4:]}"


def test_idempotent_submit_and_parallel_limit(tmp_path: Path):
    store = Store(tmp_path / "state.db")
    first, created = store.submit_job(spec())
    second, created2 = store.submit_job(spec())
    assert created is True and created2 is False
    assert first["job"]["id"] == second["job"]["id"]
    launches = store.prepare_launches(global_limit=4, provider_ref_factory=provider)
    assert len(launches) == 2
    assert len({a.branch for a in launches}) == 2


def test_failed_worker_recovers_from_checkpoint_without_losing_other_result(tmp_path: Path):
    store = Store(tmp_path / "state.db")
    snap, _ = store.submit_job(spec())
    attempts = store.prepare_launches(global_limit=4, provider_ref_factory=provider)
    a, b = attempts
    store.mark_launch_success(a.id)
    store.mark_launch_success(b.id)
    store.register_worker(a.id, "wa", 120)
    store.register_worker(b.id, "wb", 120)
    sha_a = "1" * 40
    sha_b = "2" * 40
    store.record_checkpoint(attempt_id=a.id, worker_id="wa", seq=1, sha=sha_a, branch=a.branch, readback_sha=sha_a)
    store.record_checkpoint(attempt_id=b.id, worker_id="wb", seq=1, sha=sha_b, branch=b.branch, readback_sha=sha_b)
    store.complete_attempt(b.id, "wb", sha_b)
    store.fail_attempt(a.id, "notebook vanished", retryable=True)
    retry = store.prepare_launches(global_limit=4, provider_ref_factory=provider)
    assert len(retry) == 1
    assert retry[0].task_name == a.task_name
    assert retry[0].base_sha == sha_a
    after = store.get_job(snap["job"]["id"])
    beta = next(t for t in after["tasks"] if t["name"] == b.task_name)
    assert beta["status"] == "COMPLETED"
    assert beta["result_sha"] == sha_b


def test_max_four_is_enforced(tmp_path: Path):
    store = Store(tmp_path / "state.db")
    tasks = tuple(TaskSpec(name=f"t{i}", instruction="x", model="m") for i in range(5))
    store.submit_job(JobSpec(repository="o/r", base_sha="a" * 40, base_branch="master", idempotency_key="cap", max_workers=4, tasks=tasks))
    assert len(store.prepare_launches(global_limit=4, provider_ref_factory=provider)) == 4
    assert store.prepare_launches(global_limit=4, provider_ref_factory=provider) == []


def test_api_auth_submit_status_export(tmp_path: Path):
    settings = Settings(
        db_path=tmp_path / "state.db",
        control_token="control-secret",
        worker_hmac_secret="w" * 32,
        max_workers=4,
    )
    with TestClient(create_app(settings)) as client:
        assert client.get("/healthz").status_code == 200
        body = {
            "repository": "o/r",
            "base_sha": "a" * 40,
            "base_branch": "master",
            "idempotency_key": "api-1",
            "max_workers": 2,
            "tasks": [{"name": "a", "instruction": "edit", "model": "opencode/a"}],
        }
        assert client.post("/v1/jobs", json=body).status_code == 401
        headers = {"Authorization": "Bearer control-secret"}
        made = client.post("/v1/jobs", json=body, headers=headers)
        assert made.status_code == 200
        job_id = made.json()["job"]["id"]
        assert client.get(f"/v1/jobs/{job_id}", headers=headers).status_code == 200
        exported = client.get(f"/v1/jobs/{job_id}/export", headers=headers).json()
        assert exported["schema"] == "daep.handoff.v1"


def test_attempt_token_is_bound_to_attempt(tmp_path: Path):
    settings = Settings(db_path=tmp_path / "state.db", control_token="c", worker_hmac_secret="z" * 32)
    app = create_app(settings)
    signer = app.state.runtime.signer
    assert signer.verify("att_a", signer.token_for("att_a"))
    assert not signer.verify("att_b", signer.token_for("att_a"))
