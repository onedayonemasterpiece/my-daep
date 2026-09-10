from __future__ import annotations

import hashlib
import hmac


class AttemptTokenSigner:
    """Derives restart-stable, attempt-scoped bearer tokens from one server secret."""

    def __init__(self, secret: str):
        if len(secret) < 24:
            raise ValueError("worker HMAC secret must be at least 24 characters")
        self._secret = secret.encode("utf-8")

    def token_for(self, attempt_id: str) -> str:
        digest = hmac.new(self._secret, ("attempt:" + attempt_id).encode(), hashlib.sha256).hexdigest()
        return f"daep_a_{digest}"

    def verify(self, attempt_id: str, token: str) -> bool:
        return hmac.compare_digest(self.token_for(attempt_id), token)
