#!/usr/bin/env python3
"""Print a pip log with the package index taken out of it.

The DCS Dockerfile runs the wheel install with its output in a file and prints the file through
this script, so pip's warnings and errors (for example "Could not fetch URL <index>/...") reach
the build log without the index URL, its host or its credentials. The index is the value of
PIP_INDEX_URL, which the build passes as a secret; the value is only used as the text to remove
and is never printed.

Usage: python3 redact_pip_log.py <log file>     (PIP_INDEX_URL in the environment)
"""

import os
import sys
from urllib.parse import urlsplit

PLACEHOLDER = "<package-index>"


def secrets_in(url: str) -> list[str]:
    """The pieces of the index URL to remove, longest first so a host inside a URL is not
    replaced before the URL is."""
    parts = urlsplit(url)
    host = parts.hostname or ""
    try:
        port = parts.port
    except ValueError:  # a malformed port: the URL and netloc are still removed whole
        port = None
    found = {
        url,
        url.rstrip("/"),
        parts.netloc,
        f"{host}:{port}" if host and port else "",
        host,
        parts.username or "",
        parts.password or "",
    }
    return sorted((s for s in found if s), key=len, reverse=True)


def redact(text: str, url: str) -> str:
    for secret in secrets_in(url):
        text = text.replace(secret, PLACEHOLDER)
    return text


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: redact_pip_log.py <log file>", file=sys.stderr)
        return 2
    with open(argv[1], errors="replace") as fh:
        text = fh.read()
    sys.stdout.write(redact(text, os.environ.get("PIP_INDEX_URL", "")))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
