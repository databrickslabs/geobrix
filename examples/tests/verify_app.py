#!/usr/bin/env python3
"""Check that a deployed Databricks App runs and answers (gbx:app:verify).

1. With ``--local-checks``: ``pnpm lint`` and ``pnpm test --run`` in the app folder (host, no workspace).
2. With ``--deploy``: ``gbx:app:deploy --profile <p>`` (build, ``bundle deploy``, ``bundle run``, Genie re-seed).
3. ``apps.get(<name>)`` until the app is RUNNING on ACTIVE compute, within ``--wait-min``; CRASHED, ERROR or
   STOPPED fail at once, as does a failed active deployment.
4. One GET of ``<app url><--path>`` with the profile's OAuth token (an app accepts the workspace OAuth token, not a
   PAT). A 2xx answer passes; anything else, a redirect to a login page included, fails.

The app name defaults to the bundle's (``resources.apps.<key>.name`` in ``examples/apps/genie_map/databricks.yml``).
"""

from __future__ import annotations

import argparse
import pathlib
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from typing import Callable, Optional

_SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
PROJECT_ROOT = (_SCRIPT_DIR / ".." / "..").resolve()
APP_DIR = PROJECT_ROOT / "examples" / "apps" / "genie_map"
DEPLOY_SH = PROJECT_ROOT / "scripts" / "commands" / "gbx-app-deploy.sh"
_FAILED_APP = {"CRASHED"}
_FAILED_COMPUTE = {"ERROR", "STOPPED", "DELETING"}


def bundle_app_name(app_dir: pathlib.Path = APP_DIR) -> Optional[str]:
    """The ``name:`` of the first app under ``resources: apps:`` of the bundle."""
    text = (app_dir / "databricks.yml").read_text(encoding="utf-8")
    m = re.search(r"^\s+apps:\s*\n(?:\s*#.*\n)*\s+[\w-]+:\s*\n(?:\s*#.*\n)*\s+name:\s*['\"]?([\w-]+)", text, re.M)
    return m.group(1) if m else None


def _v(obj, *names):
    for n in names:
        obj = getattr(obj, n, None) if obj is not None else None
    return getattr(obj, "value", obj)


def wait_running(w, name: str, wait_s: int, poll_s: int = 15, say=print,
                 now: Callable[[], float] = time.time, sleep: Callable[[float], None] = time.sleep):
    """(app or None, problem or None): the app once RUNNING on ACTIVE compute, else why not."""
    start, last = now(), None
    while True:
        app = w.apps.get(name=name)
        st, cs = _v(app, "app_status", "state"), _v(app, "compute_status", "state")
        dep = _v(app, "active_deployment", "status", "state")
        line = f"app {name}: app={st} compute={cs} deployment={dep}"
        if line != last:
            say(f"    [{int(now() - start)}s] {line}")
            last = line
        if st == "RUNNING" and cs == "ACTIVE":
            return app, None
        if st in _FAILED_APP or cs in _FAILED_COMPUTE or dep == "FAILED":
            msg = _v(app, "app_status", "message") or _v(app, "compute_status", "message") or \
                _v(app, "active_deployment", "status", "message") or ""
            return app, f"{line}: {msg[:300]}"
        if now() - start > wait_s:
            return app, f"not RUNNING within {wait_s // 60} min (--wait-min): {line}"
        sleep(poll_s)


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):  # a redirect is the login page: report it, do not follow it
        return None


def probe(url: str, headers: dict, timeout_s: int = 30, opener=None) -> tuple[int, int, str]:
    """(status, body bytes, location) of one GET."""
    req = urllib.request.Request(url, headers=headers)
    opener = opener or urllib.request.build_opener(_NoRedirect)
    try:
        with opener.open(req, timeout=timeout_s) as resp:
            return resp.status, len(resp.read()), ""
    except urllib.error.HTTPError as exc:
        return exc.code, 0, exc.headers.get("Location", "") if exc.headers else ""


def local_checks(app_dir: pathlib.Path, run=subprocess.run, say=print) -> list[str]:
    problems = []
    for cmd in (["pnpm", "lint"], ["pnpm", "test", "--run"]):
        say("    $ " + " ".join(cmd))
        res = run(cmd, cwd=str(app_dir))
        if res.returncode != 0:
            problems.append(f"{' '.join(cmd)} failed (exit {res.returncode})")
    return problems


def main(argv=None, workspace_client=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--profile", default=None, help="Databricks CLI profile (required)")
    p.add_argument("--app", default=None, help="the app name (default: the bundle's)")
    p.add_argument("--deploy", action="store_true", help="run gbx:app:deploy first")
    p.add_argument("--local-checks", action="store_true", help="run pnpm lint and pnpm test first")
    p.add_argument("--path", default="/", help="the URL path to GET (default /)")
    p.add_argument("--wait-min", type=int, default=10, help="how long to wait for RUNNING (default 10)")
    p.add_argument("--poll-secs", type=int, default=15)
    args = p.parse_args(argv)
    if not args.profile:
        p.error("--profile is required: no profile is picked for you")
    name = args.app or bundle_app_name()
    if not name:
        p.error("no app name in the bundle: pass --app")
    print(f"=== gbx:app:verify {name} ===")
    print(f"    profile: {args.profile}; checks: {'local lint+test, ' if args.local_checks else ''}"
          f"{'deploy, ' if args.deploy else ''}RUNNING within {args.wait_min} min, GET {args.path} answers 2xx")

    if args.local_checks:
        problems = local_checks(APP_DIR)
        if problems:
            print("!!! " + "; ".join(problems))
            return 1
    if args.deploy:
        res = subprocess.run(["bash", str(DEPLOY_SH), "--profile", args.profile])
        if res.returncode != 0:
            print(f"!!! gbx:app:deploy failed (exit {res.returncode})")
            return 1

    if workspace_client is None:
        from databricks.sdk import WorkspaceClient

        workspace_client = WorkspaceClient
    w = workspace_client(profile=args.profile)
    app, problem = wait_running(w, name, args.wait_min * 60, args.poll_secs)
    if problem:
        print(f"!!! {problem}")
        return 1
    url = (getattr(app, "url", None) or "").rstrip("/")
    if not url:
        print("!!! the app has no URL")
        return 1
    status, size, location = probe(url + "/" + args.path.lstrip("/"), w.config.authenticate())
    print(f"    GET {url}/{args.path.lstrip('/')}: HTTP {status}, {size} bytes" + (f", Location {location}" if location else ""))
    if not 200 <= status < 300:
        print("!!! the app did not answer 2xx" + (" (a redirect: the token was not accepted)" if 300 <= status < 400 else ""))
        return 1
    print("=== APP RUNNING AND ANSWERING ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
