"""Patterns of workspace-specific values that no shipped file of the series may hold. The tests that guard the
files match these patterns, so the guards hold no real id, host or user name themselves."""

import re

PATTERNS = {
    "a SQL warehouse id": re.compile(r"\b[0-9a-f]{16}\b"),
    "a Genie space id": re.compile(r"\b01f[0-9a-f]{29}\b"),
    "a user home": re.compile(r"/Users/[^/\s]+@"),
    "an e-mail address": re.compile(r"\b[\w.+-]+@[\w-]+(?:\.[\w-]+)*\.(?:com|net|org|io)\b"),
    "a workspace host": re.compile(r"\.cloud\.databricks\.com"),
    "a named profile": re.compile(r"--profile[\s=]+[^<\s]|\bprofile\s*=\s*[\"'][^\"'<]"),
}


def findings(text: str) -> list[str]:
    """`<what>: <match>` for each workspace-specific value in `text`."""
    return [f"{what}: {m.group(0)}" for what, rx in PATTERNS.items() for m in rx.finditer(text)]
