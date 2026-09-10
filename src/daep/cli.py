from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import httpx
import uvicorn

from .config import Settings
from .supervisor import create_app


def _client() -> tuple[httpx.Client, str]:
    url = os.getenv("DAEP_URL", os.getenv("DAEP_PUBLIC_URL", "http://127.0.0.1:8765")).rstrip("/")
    token = os.getenv("DAEP_CONTROL_TOKEN", "")
    if not token:
        raise SystemExit("DAEP_CONTROL_TOKEN is required")
    return httpx.Client(base_url=url, headers={"Authorization": f"Bearer {token}"}, timeout=60.0), url


def _load_json(path: str | None) -> dict[str, Any]:
    if path in (None, "-"):
        return json.load(sys.stdin)
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _print(value: Any) -> None:
    print(json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True))


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="daep", description="my-daep distributed OpenCode/Kaggle controller")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("serve", help="run supervisor service")
    s = sub.add_parser("submit", help="submit a durable distributed job from JSON")
    s.add_argument("spec", nargs="?", default="-")
    st = sub.add_parser("status")
    st.add_argument("job_id")
    f = sub.add_parser("follow")
    f.add_argument("job_id")
    f.add_argument("--after", type=int, default=0)
    a = sub.add_parser("attach")
    a.add_argument("job_id")
    a.add_argument("session_id")
    a.add_argument("--server-url")
    c = sub.add_parser("cancel")
    c.add_argument("job_id")
    e = sub.add_parser("export")
    e.add_argument("job_id")
    e.add_argument("--output")
    fin = sub.add_parser("finalize")
    fin.add_argument("job_id")
    fin.add_argument("result_sha")
    fin.add_argument("checks", help="JSON file containing a list of actual check results")
    tick = sub.add_parser("tick", help="run one scheduler/reconciliation iteration")
    args = p.parse_args(argv)

    if args.cmd == "serve":
        settings = Settings.from_env()
        settings.validate_runtime()
        uvicorn.run(create_app(settings), host=settings.bind_host, port=settings.bind_port)
        return

    client, _ = _client()
    try:
        if args.cmd == "submit":
            r = client.post("/v1/jobs", json=_load_json(args.spec))
        elif args.cmd == "status":
            r = client.get(f"/v1/jobs/{args.job_id}")
        elif args.cmd == "follow":
            r = client.get(f"/v1/jobs/{args.job_id}/events", params={"after": args.after})
        elif args.cmd == "attach":
            r = client.post(f"/v1/jobs/{args.job_id}/attach", json={"session_id": args.session_id, "server_url": args.server_url})
        elif args.cmd == "cancel":
            r = client.post(f"/v1/jobs/{args.job_id}/cancel")
        elif args.cmd == "export":
            r = client.get(f"/v1/jobs/{args.job_id}/export")
            r.raise_for_status()
            value = r.json()
            if args.output:
                Path(args.output).write_text(json.dumps(value, indent=2, ensure_ascii=False), encoding="utf-8")
                print(args.output)
                return
            _print(value)
            return
        elif args.cmd == "finalize":
            checks = json.loads(Path(args.checks).read_text(encoding="utf-8"))
            r = client.post(f"/v1/jobs/{args.job_id}/finalize", json={"result_sha": args.result_sha, "checks": checks})
        elif args.cmd == "tick":
            r = client.post("/v1/tick")
        else:
            raise AssertionError(args.cmd)
        r.raise_for_status()
        _print(r.json())
    finally:
        client.close()


if __name__ == "__main__":
    main()
