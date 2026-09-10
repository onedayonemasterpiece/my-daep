from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx
import jwt

from .models import GitHubToken


@dataclass
class _CachedToken:
    value: GitHubToken
    expires_epoch: float


class GitHubAppTokenBroker:
    """Mints repository-scoped GitHub App installation tokens and refreshes them."""

    def __init__(
        self,
        *,
        app_id: str,
        private_key: str,
        installation_id: int | None = None,
        api_url: str = "https://api.github.com",
        refresh_skew_seconds: int = 180,
        client: httpx.Client | None = None,
    ):
        self.app_id = str(app_id)
        self.private_key = private_key.replace("\\n", "\n")
        self.installation_id = installation_id
        self.api_url = api_url.rstrip("/")
        self.refresh_skew_seconds = refresh_skew_seconds
        self.client = client or httpx.Client(timeout=20.0)
        self._cache: dict[tuple[int, str], _CachedToken] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _split_repo(repository: str) -> tuple[str, str]:
        owner, sep, name = repository.partition("/")
        if not sep or not owner or not name or "/" in name:
            raise ValueError("repository must be owner/name")
        return owner, name

    def _app_jwt(self) -> str:
        now = int(time.time())
        return jwt.encode(
            {"iat": now - 30, "exp": now + 540, "iss": self.app_id},
            self.private_key,
            algorithm="RS256",
        )

    def _app_headers(self) -> dict[str, str]:
        return {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self._app_jwt()}",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    @staticmethod
    def _token_headers(token: str) -> dict[str, str]:
        return {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def _resolve_installation(self, repository: str) -> int:
        if self.installation_id is not None:
            return self.installation_id
        owner, name = self._split_repo(repository)
        response = self.client.get(
            f"{self.api_url}/repos/{owner}/{name}/installation", headers=self._app_headers()
        )
        response.raise_for_status()
        return int(response.json()["id"])

    def _mint(self, repository: str, installation_id: int) -> _CachedToken:
        _, repo_name = self._split_repo(repository)
        payload: dict[str, Any] = {
            "repositories": [repo_name],
            "permissions": {
                "contents": "write",
                "pull_requests": "write",
                "actions": "read",
                "checks": "read",
                "statuses": "read",
            },
        }
        response = self.client.post(
            f"{self.api_url}/app/installations/{installation_id}/access_tokens",
            headers=self._app_headers(),
            json=payload,
        )
        response.raise_for_status()
        body = response.json()
        expires_at = str(body["expires_at"])
        expires_epoch = datetime.fromisoformat(expires_at.replace("Z", "+00:00")).timestamp()
        return _CachedToken(
            value=GitHubToken(str(body["token"]), expires_at, repository),
            expires_epoch=expires_epoch,
        )

    def get_token(self, repository: str, *, force_refresh: bool = False) -> GitHubToken:
        self._split_repo(repository)
        with self._lock:
            installation_id = self._resolve_installation(repository)
            cache_key = (installation_id, repository)
            cached = self._cache.get(cache_key)
            if (
                not force_refresh
                and cached is not None
                and cached.expires_epoch - time.time() > self.refresh_skew_seconds
            ):
                return cached.value
            cached = self._mint(repository, installation_id)
            self._cache[cache_key] = cached
            return cached.value

    def preflight(self, repository: str) -> dict[str, Any]:
        token = self.get_token(repository)
        owner, name = self._split_repo(repository)
        response = self.client.get(
            f"{self.api_url}/repos/{owner}/{name}", headers=self._token_headers(token.token)
        )
        response.raise_for_status()
        body = response.json()
        return {
            "repository": repository,
            "installation_id": self._resolve_installation(repository),
            "default_branch": body.get("default_branch"),
            "expires_at": token.expires_at,
        }

    def ensure_pull_request(
        self,
        *,
        repository: str,
        head_branch: str,
        base_branch: str,
        title: str,
        body: str,
    ) -> dict[str, Any]:
        token = self.get_token(repository)
        headers = self._token_headers(token.token)
        owner, name = self._split_repo(repository)
        existing = self.client.get(
            f"{self.api_url}/repos/{owner}/{name}/pulls",
            headers=headers,
            params={"state": "open", "head": f"{owner}:{head_branch}", "base": base_branch},
        )
        if existing.status_code == 401:
            token = self.get_token(repository, force_refresh=True)
            headers = self._token_headers(token.token)
            existing = self.client.get(
                f"{self.api_url}/repos/{owner}/{name}/pulls",
                headers=headers,
                params={"state": "open", "head": f"{owner}:{head_branch}", "base": base_branch},
            )
        existing.raise_for_status()
        rows = existing.json()
        if rows:
            row = rows[0]
            return {"number": row["number"], "url": row["html_url"], "created": False}
        response = self.client.post(
            f"{self.api_url}/repos/{owner}/{name}/pulls",
            headers=headers,
            json={"title": title, "head": head_branch, "base": base_branch, "body": body},
        )
        response.raise_for_status()
        row = response.json()
        return {"number": row["number"], "url": row["html_url"], "created": True}

    def remote_ref_sha(self, repository: str, branch: str) -> str | None:
        token = self.get_token(repository)
        owner, name = self._split_repo(repository)
        response = self.client.get(
            f"{self.api_url}/repos/{owner}/{name}/git/ref/heads/{branch}",
            headers=self._token_headers(token.token),
        )
        if response.status_code == 404:
            return None
        if response.status_code == 401:
            token = self.get_token(repository, force_refresh=True)
            response = self.client.get(
                f"{self.api_url}/repos/{owner}/{name}/git/ref/heads/{branch}",
                headers=self._token_headers(token.token),
            )
        response.raise_for_status()
        return str(response.json()["object"]["sha"])
