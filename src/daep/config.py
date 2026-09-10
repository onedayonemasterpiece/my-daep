from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _read_secret(env_name: str, file_env_name: str) -> str | None:
    direct = os.getenv(env_name)
    if direct:
        return direct.replace("\\n", "\n")
    filename = os.getenv(file_env_name)
    if filename:
        return Path(filename).expanduser().read_text(encoding="utf-8")
    return None


@dataclass(frozen=True)
class Settings:
    db_path: Path = Path(".daep/state.sqlite3")
    bind_host: str = "127.0.0.1"
    bind_port: int = 8765
    public_url: str = "http://127.0.0.1:8765"
    control_token: str = ""
    worker_hmac_secret: str = ""
    max_workers: int = 4
    lease_seconds: int = 120
    heartbeat_seconds: int = 20
    checkpoint_seconds: int = 45
    callback_grace_seconds: int = 150
    scheduler_seconds: int = 5
    github_app_id: str | None = None
    github_installation_id: int | None = None
    github_private_key: str | None = None
    github_api_url: str = "https://api.github.com"
    github_token_refresh_skew_seconds: int = 180
    kaggle_username: str | None = None
    kaggle_cli: str = "kaggle"
    kaggle_cli_timeout_seconds: int = 120
    opencode_auth: dict[str, Any] | None = None
    opencode_install: bool = True
    opencode_package: str = "opencode-ai"
    coordinator_server_url: str | None = None
    coordinator_server_username: str = "opencode"
    coordinator_server_password: str | None = None
    backup_dir: Path | None = None
    backup_keep: int = 24
    extra_worker_env: dict[str, str] = field(default_factory=dict)

    @property
    def github_configured(self) -> bool:
        return bool(self.github_app_id and self.github_private_key)

    @classmethod
    def from_env(cls) -> "Settings":
        max_workers = int(os.getenv("DAEP_MAX_WORKERS", "4"))
        if not 1 <= max_workers <= 4:
            raise ValueError("DAEP_MAX_WORKERS must be in range 1..4")
        opencode_auth: dict[str, Any] | None = None
        auth_path = os.getenv("DAEP_OPENCODE_AUTH_FILE")
        if auth_path:
            parsed = json.loads(Path(auth_path).expanduser().read_text(encoding="utf-8"))
            if not isinstance(parsed, dict):
                raise ValueError("DAEP_OPENCODE_AUTH_FILE must contain a JSON object")
            opencode_auth = parsed
        elif os.getenv("DAEP_OPENCODE_AUTH_JSON"):
            parsed = json.loads(os.environ["DAEP_OPENCODE_AUTH_JSON"])
            if not isinstance(parsed, dict):
                raise ValueError("DAEP_OPENCODE_AUTH_JSON must be a JSON object")
            opencode_auth = parsed

        extra_worker_env: dict[str, str] = {}
        if os.getenv("DAEP_WORKER_ENV_JSON"):
            parsed_env = json.loads(os.environ["DAEP_WORKER_ENV_JSON"])
            if not isinstance(parsed_env, dict) or not all(
                isinstance(k, str) and isinstance(v, str) for k, v in parsed_env.items()
            ):
                raise ValueError("DAEP_WORKER_ENV_JSON must be an object of string values")
            extra_worker_env = parsed_env

        installation_raw = os.getenv("DAEP_GITHUB_INSTALLATION_ID")
        backup_raw = os.getenv("DAEP_BACKUP_DIR")
        return cls(
            db_path=Path(os.getenv("DAEP_DB_PATH", ".daep/state.sqlite3")).expanduser(),
            bind_host=os.getenv("DAEP_BIND_HOST", "127.0.0.1"),
            bind_port=int(os.getenv("DAEP_BIND_PORT", "8765")),
            public_url=os.getenv("DAEP_PUBLIC_URL", "http://127.0.0.1:8765").rstrip("/"),
            control_token=os.getenv("DAEP_CONTROL_TOKEN", ""),
            worker_hmac_secret=os.getenv("DAEP_WORKER_HMAC_SECRET", ""),
            max_workers=max_workers,
            lease_seconds=int(os.getenv("DAEP_LEASE_SECONDS", "120")),
            heartbeat_seconds=int(os.getenv("DAEP_HEARTBEAT_SECONDS", "20")),
            checkpoint_seconds=int(os.getenv("DAEP_CHECKPOINT_SECONDS", "45")),
            callback_grace_seconds=int(os.getenv("DAEP_CALLBACK_GRACE_SECONDS", "150")),
            scheduler_seconds=int(os.getenv("DAEP_SCHEDULER_SECONDS", "5")),
            github_app_id=os.getenv("DAEP_GITHUB_APP_ID"),
            github_installation_id=int(installation_raw) if installation_raw else None,
            github_private_key=_read_secret("DAEP_GITHUB_PRIVATE_KEY", "DAEP_GITHUB_PRIVATE_KEY_FILE"),
            github_api_url=os.getenv("DAEP_GITHUB_API_URL", "https://api.github.com").rstrip("/"),
            github_token_refresh_skew_seconds=int(os.getenv("DAEP_GITHUB_TOKEN_REFRESH_SKEW_SECONDS", "180")),
            kaggle_username=os.getenv("DAEP_KAGGLE_USERNAME"),
            kaggle_cli=os.getenv("DAEP_KAGGLE_CLI", "kaggle"),
            kaggle_cli_timeout_seconds=int(os.getenv("DAEP_KAGGLE_CLI_TIMEOUT_SECONDS", "120")),
            opencode_auth=opencode_auth,
            opencode_install=_env_bool("DAEP_OPENCODE_INSTALL", True),
            opencode_package=os.getenv("DAEP_OPENCODE_PACKAGE", "opencode-ai"),
            coordinator_server_url=os.getenv("DAEP_OPENCODE_SERVER_URL"),
            coordinator_server_username=os.getenv("DAEP_OPENCODE_SERVER_USERNAME", "opencode"),
            coordinator_server_password=os.getenv("DAEP_OPENCODE_SERVER_PASSWORD"),
            backup_dir=Path(backup_raw).expanduser() if backup_raw else None,
            backup_keep=int(os.getenv("DAEP_BACKUP_KEEP", "24")),
            extra_worker_env=extra_worker_env,
        )

    def validate_runtime(self) -> None:
        if not self.control_token:
            raise ValueError("DAEP_CONTROL_TOKEN is required")
        if len(self.worker_hmac_secret) < 24:
            raise ValueError("DAEP_WORKER_HMAC_SECRET must be at least 24 characters")
        if not self.public_url.startswith("https://") and self.bind_host not in {"127.0.0.1", "localhost"}:
            raise ValueError("DAEP_PUBLIC_URL must use HTTPS when exposed outside localhost")
