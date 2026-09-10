from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class TaskSpec:
    name: str
    instruction: str
    model: str
    depends_on: tuple[str, ...] = ()
    fallback_models: tuple[str, ...] = ()
    max_attempts: int = 3


@dataclass(frozen=True)
class JobSpec:
    repository: str
    base_sha: str
    base_branch: str
    tasks: tuple[TaskSpec, ...]
    idempotency_key: str
    max_workers: int = 2
    coordinator_session_id: str | None = None
    coordinator_server_url: str | None = None


@dataclass(frozen=True)
class LaunchAttempt:
    id: str
    job_id: str
    task_id: str
    task_name: str
    repository: str
    base_sha: str
    branch: str
    generation: int
    model: str
    instruction: str
    provider_ref: str
    event_cursor: int = 0


@dataclass(frozen=True)
class ProviderState:
    state: str
    raw: str = ""
    terminal: bool = False
    succeeded: bool = False


@dataclass(frozen=True)
class GitHubToken:
    token: str
    expires_at: str
    repository: str


@dataclass
class CheckResult:
    name: str
    command: str
    outcome: str
    detail: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
