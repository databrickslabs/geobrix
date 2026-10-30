#!/usr/bin/env python3
"""Fetch one sdist file from the package index pip is configured with, sha256-checked against its pin.

    index_fetch.py NAME FILE SHA256 DEST

CI's hardened runners reach PyPI only through the JFrog PyPI remote that .github/actions/jfrog-pip-bootstrap
configures for pip (PIP_CONFIG_FILE, and the credentials in the NETRC file). `pip download` cannot fetch these sdists:
it builds an sdist's metadata first, and the bindings' builds need GDAL, PROJ and their build requirements compiled
from source. So this reads the index as pip does: the index URL from PIP_INDEX_URL, else pip's own configuration
(`pip config list`: download., then global. index-url), the PEP 503 page of NAME there, the link named FILE (its
#sha256= fragment, when present, must be the pin), with the netrc credentials of each host it calls (never sent to
another host after a redirect). The file is kept only when its sha256 is the pin; a mismatch is a hard stop, as in
fetch-lib.sh's fetch_verified. An existing DEST with the pinned sum is kept. No URL, host or credential is printed.
Used by fetch-lib.sh's fetch_sdist when GBX_SDISTS_FROM=index (the CI builder sets it). Stdlib only.
"""

import base64
import hashlib
import html.parser
import netrc
import os
import re
import subprocess
import sys
import urllib.request
from pathlib import Path
from urllib.parse import urldefrag, urljoin, urlsplit


class Fail(Exception):
    pass


def index_url(env=os.environ, pip_config=None):
    if env.get("PIP_INDEX_URL"):
        return env["PIP_INDEX_URL"]
    if pip_config is None:
        res = subprocess.run(
            [sys.executable, "-m", "pip", "config", "list"], capture_output=True, text=True, env=dict(env)
        )
        pip_config = res.stdout if res.returncode == 0 else ""
    found = dict(re.findall(r"^([a-z]+\.index-url)='([^']*)'$", pip_config, re.M))
    for key in ("download.index-url", "global.index-url"):
        if found.get(key):
            return found[key]
    raise Fail("no package index is configured for pip (PIP_INDEX_URL or pip config's index-url)")


def credentials(env=os.environ):
    """host -> (login, password) from the NETRC file (else ~/.netrc), as pip reads it."""
    path = env.get("NETRC") or os.path.expanduser("~/.netrc")
    if not os.path.isfile(path):
        return {}
    hosts = netrc.netrc(path).hosts
    return {h: (v[0], v[2]) for h, v in hosts.items()}


class _Links(html.parser.HTMLParser):
    def __init__(self):
        super().__init__()
        self.links, self._href = [], None

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self._href = dict(attrs).get("href")

    def handle_data(self, data):
        if self._href is not None:
            self.links.append((data.strip(), self._href))

    def handle_endtag(self, tag):
        if tag == "a":
            self._href = None


class _SameHostAuth(urllib.request.HTTPRedirectHandler):
    """Drop the credentials on a redirect to another host (a signed storage URL must not get them)."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        new = super().redirect_request(req, fp, code, msg, headers, newurl)
        if new is not None and urlsplit(newurl).hostname != urlsplit(req.full_url).hostname:
            new.headers.pop("Authorization", None)
            new.unredirected_hdrs.pop("Authorization", None)
        return new


def get(url, creds, timeout=120):
    req = urllib.request.Request(url, headers={"User-Agent": "geobrix-prefetch"})
    auth = creds.get(urlsplit(url).hostname)
    if auth:
        token = base64.b64encode(f"{auth[0]}:{auth[1]}".encode()).decode()
        req.add_header("Authorization", f"Basic {token}")
    try:
        with urllib.request.build_opener(_SameHostAuth).open(req, timeout=timeout) as resp:
            return resp.geturl(), resp.read()
    except OSError as e:  # URLError and HTTPError are OSErrors; their text can carry the URL: not printed
        raise Fail(f"the package index did not serve it ({type(e).__name__}{getattr(e, 'code', '') and f' {e.code}'})")


def fetch(name, file, want, dest, env=os.environ, pip_config=None):
    dest = Path(dest)
    if dest.is_file() and hashlib.sha256(dest.read_bytes()).hexdigest() == want:
        return "kept"
    creds = credentials(env)
    project = re.sub(r"[-_.]+", "-", name).lower()
    page_url, page = get(index_url(env, pip_config).rstrip("/") + f"/{project}/", creds)
    parser = _Links()
    parser.feed(page.decode("utf-8", "replace"))
    hrefs = [h for text, h in parser.links if text == file or urldefrag(h)[0].rsplit("/", 1)[-1] == file]
    if not hrefs:
        raise Fail(f"the package index lists no {file} for {project}")
    url, frag = urldefrag(urljoin(page_url, hrefs[0]))
    if frag.startswith("sha256=") and frag[len("sha256="):] != want:
        raise Fail(f"{file}: the package index says sha256 {frag[len('sha256='):]}, pins.conf says {want}")
    _, data = get(url, creds)
    got = hashlib.sha256(data).hexdigest()
    if got != want:
        raise Fail(f"{file} sha256 {got}, pins.conf says {want} (from the package index)")
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    part.write_bytes(data)
    part.replace(dest)
    return "fetched"


def main(argv):
    if len(argv) != 4:
        print(__doc__.split("\n\n")[1].strip(), file=sys.stderr)
        return 2
    name, file, want, dest = argv
    try:
        how = fetch(name, file, want, dest)
    except Fail as e:
        print(f"PREFETCH FAILED: {e}", file=sys.stderr)
        return 1
    print(f"sdist {file}: {how} (package index, sha256 ok)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
