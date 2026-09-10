from __future__ import annotations

import threading
from collections import defaultdict
from typing import Any

import httpx

from .store import Store


class OpenCodeCoordinator:
    """Delivers significant DAEP events back into the coordinator OpenCode session serially."""

    def __init__(
        self,
        store: Store,
        *,
        default_server_url: str | None = None,
        username: str = "opencode",
        password: str | None = None,
        client: httpx.Client | None = None,
    ):
        self.store = store
        self.default_server_url = default_server_url
        self.username = username
        self.password = password
        self.client = client or httpx.Client(timeout=20.0)
        self._locks: dict[str, threading.Lock] = defaultdict(threading.Lock)

    def _auth(self) -> tuple[str, str] | None:
        return (self.username, self.password) if self.password else None

    def deliver_pending(self, job_id: str) -> dict[str, Any]:
        with self._locks[job_id]:
            state = self.store.coordinator_delivery_state(job_id)
            session_id = state.get("coordinator_session_id")
            server_url = state.get("coordinator_server_url") or self.default_server_url
            if not session_id or not server_url:
                return {"delivered": 0, "waiting": "coordinator_not_attached"}
            events = self.store.events(
                job_id,
                after=int(state.get("coordinator_cursor") or 0),
                limit=100,
                significant_only=True,
            )
            delivered = 0
            for event in events:
                message = self._format_event(job_id, event)
                response = self.client.post(
                    f"{str(server_url).rstrip('/')}/session/{session_id}/prompt_async",
                    auth=self._auth(),
                    json={"parts": [{"type": "text", "text": message}]},
                )
                if response.status_code in (404, 410):
                    return {"delivered": delivered, "waiting": "coordinator_session_missing", "event_id": event["id"]}
                response.raise_for_status()
                self.store.advance_coordinator_cursor(job_id, int(event["id"]))
                delivered += 1
            return {"delivered": delivered, "waiting": None}

    @staticmethod
    def _format_event(job_id: str, event: dict[str, Any]) -> str:
        kind = event["kind"]
        payload = event.get("payload") or {}
        return (
            f"[DAEP distributed job {job_id}] significant event #{event['id']}: {kind}. "
            f"Payload: {payload}. Continue coordination from durable DAEP state; do not assume task success from model self-report."
        )
