#!/usr/bin/env python3
"""Enforce that every registered function has a `since` (introduced-in) version.

Single docs site = latest release only, so a "Since vX.Y.Z" marker is what tells a
user on an older version whether a call exists for them. This check keeps that map
complete: every name in registered_functions.txt must have a row in
function-since.tsv. Run in CI / QC before a release; a new function without a
`since` fails here (the same way check-binding-parity guards bindings).

Exit 0 = every registered function has a since; non-zero + a list otherwise.
"""
import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
REGISTERED = os.path.join(
    REPO_ROOT, "docs", "tests-function-info", "registered_functions.txt"
)
SINCE_TSV = os.path.join(REPO_ROOT, "docs", "tests-function-info", "function-since.tsv")
READERS_SINCE_TSV = os.path.join(
    REPO_ROOT, "docs", "tests-function-info", "readers-since.tsv"
)


def _load_registered():
    names = []
    with open(REGISTERED) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                names.append(line)
    return names


def _load_since(path, key_col=0, header_first_col="function_name"):
    since = {}
    if not os.path.exists(path):
        return since
    with open(path) as f:
        for i, line in enumerate(f):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if not parts:
                continue
            if i == 0 and parts[0].strip() in (header_first_col, "format_name"):
                continue
            name = parts[key_col].strip()
            ver = parts[-1].strip()
            if name and ver:
                since[name] = ver
    return since


def main() -> int:
    registered = _load_registered()
    since = _load_since(SINCE_TSV)
    missing = sorted(n for n in registered if n not in since)
    extra = sorted(n for n in since if n not in registered)

    if missing:
        print(
            "ERROR: %d registered function(s) missing a `since` version in %s:"
            % (len(missing), os.path.relpath(SINCE_TSV, REPO_ROOT)),
            file=sys.stderr,
        )
        for n in missing:
            print("  %s" % n, file=sys.stderr)
        print(
            "\nAdd a `<name>\\t<version>` row (the version that first shipped it).",
            file=sys.stderr,
        )
        return 1

    # Stale entries are a warning, not a hard failure (a removed function leaves a row).
    if extra:
        print(
            "WARNING: %d `since` row(s) name a function not in registered_functions.txt "
            "(removed/renamed?): %s" % (len(extra), ", ".join(extra)),
            file=sys.stderr,
        )

    readers_since = _load_since(READERS_SINCE_TSV, header_first_col="format_name")
    print(
        "OK: all %d registered functions have a `since`; %d readers/writers mapped."
        % (len(registered), len(readers_since))
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
