#!/usr/bin/env python3
"""CI path filter: did anything under the given paths change between two commits?

    paths_changed.py BASE HEAD PATH [PATH ...]

Prints one line, ``<true|false>: <reason>``, and, when GITHUB_OUTPUT is set, appends
``changed=<true|false>`` to it. The exit status is 0 whatever the answer.

It fails SAFE: whenever the answer cannot be established it says ``true``, so the work the
filter gates still runs. That covers an empty or all-zero BASE (the first push of a branch,
a manual run), a BASE commit that is not in the clone and cannot be fetched (force-pushed
away), an unresolvable HEAD, and any git error.

A shallow checkout is enough: a missing BASE is fetched on its own (``--depth=1``), and the
comparison reads only the two commits' trees. Renames are not paired, so moving a file out of
a watched path still counts as a change under it.
"""

import os
import subprocess
import sys


def _git(*args):
    return subprocess.run(["git", *args], capture_output=True, text=True)


def _has_commit(rev):
    return _git("cat-file", "-e", f"{rev}^{{commit}}").returncode == 0


def paths_changed(base, head, paths):
    """Return ``(changed, reason)`` for the diff of ``base`` to ``head`` limited to ``paths``."""
    if not base or set(base) == {"0"}:
        return True, "no base commit (first push of a branch, or a manual run)"
    if not _has_commit(base):
        _git("fetch", "--quiet", "--no-tags", "--depth=1", "origin", base)
        if not _has_commit(base):
            return True, f"base commit {base[:12]} is not available"
    if not _has_commit(head):
        return True, f"head commit {head[:12]} is not available"
    diff = _git("diff", "--name-only", "--no-renames", base, head, "--", *paths)
    if diff.returncode != 0:
        return True, f"git diff failed: {diff.stderr.strip()[:120]}"
    files = diff.stdout.split()
    if files:
        return (
            True,
            f"{len(files)} changed file(s) under the watched paths, e.g. {files[0]}",
        )
    return (
        False,
        f"nothing changed under the watched paths between {base[:12]} and {head[:12]}",
    )


def main(argv):
    if len(argv) < 4:
        print(__doc__.strip().splitlines()[0], file=sys.stderr)
        print(f"usage: {argv[0]} BASE HEAD PATH [PATH ...]", file=sys.stderr)
        return 2
    changed, reason = paths_changed(argv[1], argv[2], argv[3:])
    answer = "true" if changed else "false"
    print(f"{answer}: {reason}")
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a") as f:
            f.write(f"changed={answer}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
