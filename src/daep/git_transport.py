from __future__ import annotations

import os
import subprocess
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


@contextmanager
def askpass_environment(token: str) -> Iterator[dict[str, str]]:
    """Expose a Git token through GIT_ASKPASS, never through argv or a remote URL."""
    with tempfile.TemporaryDirectory(prefix="daep-askpass-") as temp:
        script = Path(temp) / "askpass.sh"
        script.write_text(
            "#!/bin/sh\ncase \"$1\" in\n*Username*) printf '%s\\n' \"$DAEP_GIT_USERNAME\" ;;\n*) printf '%s\\n' \"$DAEP_GIT_PASSWORD\" ;;\nesac\n",
            encoding="utf-8",
        )
        script.chmod(0o700)
        env = os.environ.copy()
        env.update(
            {
                "GIT_ASKPASS": str(script),
                "GIT_TERMINAL_PROMPT": "0",
                "DAEP_GIT_USERNAME": "x-access-token",
                "DAEP_GIT_PASSWORD": token,
            }
        )
        yield env


def confirmed_push(
    repo_dir: Path,
    *,
    branch: str,
    token: str,
    runner=subprocess.run,
) -> str:
    """Push HEAD and require remote readback; non-zero push alone is not proof of failure."""
    head = runner(
        ["git", "rev-parse", "HEAD"], cwd=repo_dir, text=True, capture_output=True, check=False
    )
    if head.returncode != 0:
        raise RuntimeError("cannot read local HEAD")
    sha = head.stdout.strip()
    with askpass_environment(token) as env:
        push = runner(
            ["git", "push", "origin", f"HEAD:refs/heads/{branch}"],
            cwd=repo_dir,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
        readback = runner(
            ["git", "ls-remote", "origin", f"refs/heads/{branch}"],
            cwd=repo_dir,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
    remote_sha = readback.stdout.split()[0] if readback.returncode == 0 and readback.stdout.strip() else ""
    if remote_sha != sha:
        raise RuntimeError(f"push not confirmed (push_rc={push.returncode}, readback={remote_sha or 'missing'})")
    return sha
