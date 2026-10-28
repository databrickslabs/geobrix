#!/usr/bin/env python3
"""Redact package-index locations from tool output (pip, uv, the env-wheel builder).

The configured index may be an internal one, and this repository is public: no URL and no host
may reach a log. Replaced: every ``scheme://...`` URL, pip's ``host='...'``, a host after
``Connection to`` / ``connect to`` / ``lookup address information for``, and any other
dotted host name ending in a common top-level domain (pip and uv print some without a scheme).
Standard library only, any Python 3. As a filter: ``... 2>&1 | python3 scripts/native/redact.py``.
"""

import re
import sys

_TLD = r"(?:com|net|org|io|dev|cloud|internal|local|corp|lan)"
PATTERNS = (
    (re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s'\"<>`]+"), "<url>"),
    (re.compile(r"host='[^']*'"), "host='<host>'"),
    (
        re.compile(
            r"((?:Connection to|connect to|lookup address information for) )[^\s:,;'\"]+",
            re.I,
        ),
        r"\1<host>",
    ),
    (
        re.compile(
            r"(?<![\w./@-])(?:[A-Za-z0-9-]+\.)+" + _TLD + r"(?::\d+)?(?![\w.-])", re.I
        ),
        "<host>",
    ),
)


def redact(text):
    for pattern, repl in PATTERNS:
        text = pattern.sub(repl, text)
    return text


if __name__ == "__main__":
    for line in iter(sys.stdin.readline, ""):
        sys.stdout.write(redact(line))
        sys.stdout.flush()
