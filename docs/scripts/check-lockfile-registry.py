#!/usr/bin/env python3
"""Guard: every ``resolved`` tarball URL in ``docs/package-lock.json`` must use
the canonical public registry host (``registry.npmjs.org``).

Why this exists
---------------
An agent once hard-coded ``npm-proxy.cloud.databricks.com`` into a few lockfile
``resolved`` URLs. CI authenticates npm to ``databricks.jfrog.io/.../db-npm/``
(via ``.github/actions/jfrog-auth``), and npm's default
``replace-registry-host=npmjs`` rewrites **only** ``registry.npmjs.org`` hosts
to the configured registry. So a hard-coded non-npmjs host is fetched directly,
which the ``databrickslabs-protected-runner-group`` egress cannot reach
(pre-TLS reset) — and the Deploy documentation job dies on that package while
every other package installs fine. That cost a multi-hour, multi-PR chase.

Keeping every ``resolved`` host as ``registry.npmjs.org`` lets CI route them
through the JFrog registry; ``docs/.npmrc`` (``replace-registry-host=always``)
makes that robust at runtime. This check surfaces any regression at push time
so it never silently ships again.

Exit 0 = clean (or lockfile absent); exit 1 = a non-canonical host was found.
"""
import re
import sys
from pathlib import Path

LOCK = Path(__file__).resolve().parents[1] / "package-lock.json"
ALLOWED = {"registry.npmjs.org"}


def main() -> int:
    if not LOCK.exists():
        print(f"{LOCK} absent; skip")
        return 0
    hosts = set(re.findall(r'"resolved":\s*"https?://([^/"]+)', LOCK.read_text()))
    bad = sorted(h for h in hosts if h not in ALLOWED)
    if bad:
        print("FAIL: docs/package-lock.json has non-canonical resolved host(s): "
              + ", ".join(bad))
        print("Fix: normalize to registry.npmjs.org so CI rewrites them to the "
              "configured JFrog registry (see docs/.npmrc replace-registry-host). "
              "e.g.  sed -i '' 's#https://BAD_HOST/#https://registry.npmjs.org/#g' "
              "docs/package-lock.json")
        return 1
    print(f"ok: {len(hosts)} distinct resolved host(s), all registry.npmjs.org")
    return 0


if __name__ == "__main__":
    sys.exit(main())
