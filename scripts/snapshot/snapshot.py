#!/usr/bin/env python3
"""Local project snapshot for gbx:local:snapshot: a rolling zip of what git does not hold.

Stdlib only and Python 3.8+: it runs on the host, outside the dev container.

A snapshot is the working tree (untracked and gitignored files too, such as local planning
notes and editor settings), minus run state that can be rebuilt (see EXCLUDES), plus a git
bundle of the commits no remote has yet and the repository's local git settings. It is written
to a local folder. Nothing touches the network unless you turn on the Drive upload (--upload or
GBX_SNAPSHOT_UPLOAD=1, default off); that code lives in drive_upload.py and is loaded only then.
"""

from __future__ import annotations

import argparse
import fcntl
import fnmatch
import importlib.util
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import zipfile
import zlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# --- what a snapshot leaves out --------------------------------------------------------------------
# Run state and build output that a command recreates. Each row is (kind, pattern, why).
#   root  - a path from the repository root: a directory (all of it) or a file; a * matches within a name
#   dir   - a directory of this name at any depth
#   file  - a file of this name at any depth
# A directory that holds a pyvenv.cfg is a virtual environment and is left out whatever its name.
EXCLUDES: List[Tuple[str, str, str]] = [
    ("root", "sample-data", "test data, restaged by gbx:data:*"),
    ("root", "build", "build output"),
    ("root", "target", "Maven output"),
    ("root", "dist", "release packaging output"),
    ("root", "python/geobrix/lib/*.jar", "staged JARs"),
    ("root", "python/geobrix/build", "wheel build output"),
    ("root", "python/geobrix/dist", "wheel build output"),
    ("root", "rust/gbxrs/target", "Cargo output"),
    ("root", "scripts/docker/m2", "Maven repository cache"),
    ("root", "docs/node_modules", "npm packages"),
    ("root", "docs/.docusaurus", "docs build cache"),
    ("root", "docs/build", "docs build output"),
    ("root", "docs/build-static-zip", "docs build output"),
    ("root", "docs/tests/coverage-report", "coverage output"),
    ("root", "python/coverage-report", "coverage output"),
    ("root", "artifacts", "Spark scratch"),
    ("root", "spark-warehouse", "Spark scratch"),
    ("root", "metastore_db", "Spark scratch"),
    ("root", "derby.log", "Spark scratch"),
    ("root", ".cache", "tool caches"),
    ("root", ".codeql", "CodeQL databases"),
    ("root", ".tmp", "scratch"),
    ("root", ".coverage", "coverage data"),
    ("root", "test-logs", "run logs"),
    ("root", "ci-logs", "fetched CI logs"),
    ("root", "mutants", "mutation-test copies"),
    ("root", ".claude/worktrees", "agent worktrees (own checkouts)"),
    (
        "root",
        "examples/tests/databricks_cluster_config.env",
        "real cluster config; has secrets, recreate from the .example.env",
    ),
    ("root", ".superpowers/input", "scratch input; may hold pasted tokens"),
    ("dir", ".git", "git objects (the bundle covers unpushed commits)"),
    ("dir", "node_modules", "npm packages"),
    ("dir", "__pycache__", "Python bytecode"),
    ("dir", ".pytest_cache", "pytest cache"),
    ("dir", ".mypy_cache", "type-checker cache"),
    ("dir", ".ruff_cache", "linter cache"),
    ("dir", ".tox", "tox environments"),
    ("dir", ".ipynb_checkpoints", "notebook autosaves"),
    ("dir", ".venv*", "virtual environments"),
    ("dir", "*.egg-info", "packaging metadata"),
    ("file", ".git", "worktree pointer"),
    ("file", "*.pyc", "Python bytecode"),
    ("file", "*.pyo", "Python bytecode"),
    ("file", ".DS_Store", "Finder metadata"),
    ("file", ".coverage.*", "coverage data"),
    ("file", "hs_err_pid*.log", "JVM crash logs"),
    ("file", "replay_pid*.log", "JVM replay logs"),
]

ROOT_EXCLUDES = [(p, why) for kind, p, why in EXCLUDES if kind == "root"]
DIR_EXCLUDES = [(p, why) for kind, p, why in EXCLUDES if kind == "dir"]
FILE_EXCLUDES = [(p, why) for kind, p, why in EXCLUDES if kind == "file"]

SNAPSHOT_RE = re.compile(
    r"^geobrix-snapshot-(\d{8}T\d{6}Z)-[A-Za-z0-9._-]+-[0-9a-f]{4,40}\.zip$"
)
TREE = "worktree"  # the working tree inside the zip
GITMETA = "git-meta"  # the git bundle and local git settings inside the zip
ENV_FILE = ".gbx-local.env"
CONFIG_KEYS = (
    "GBX_SNAPSHOT_DIR",
    "GBX_SNAPSHOT_KEEP",
    "GBX_SNAPSHOT_MAX_FILE_MB",
    "GBX_SNAPSHOT_GITLEAKS",
    "GBX_SNAPSHOT_SCAN_ALLOW",
    "GBX_SNAPSHOT_UPLOAD",
    "GBX_SNAPSHOT_DRIVE_FOLDER_ID",
    "GBX_SNAPSHOT_DRIVE_KEEP",
    "GBX_SNAPSHOT_GCP_QUOTA_PROJECT",
    "GBX_SNAPSHOT_DRIVE_ALLOW_SHARED",
    "GBX_SNAPSHOT_UPLOAD_MIN_INTERVAL_MIN",
)
# gitleaks rules left out of the verdict: entropy guesses that flag run ids and heartbeat fields in
# local logs. The token-shaped rules (keys, JWTs, cloud and vendor tokens) all stay on.
GITLEAKS_IGNORED_RULES = {"generic-api-key"}
HOOK_BEGIN = "# >>> gbx-local-snapshot (managed by gbx:local:snapshot) >>>"
HOOK_END = "# <<< gbx-local-snapshot <<<"
SCAN_MAX_BYTES = 20 * 1024 * 1024

EXIT_REFUSED = 3  # the secret scan found something
EXIT_DRIVE = 4  # the local snapshot is fine; the Drive step failed or was refused
EXIT_BUSY = 75  # another snapshot is running (the repository's lock convention)


def say(msg: str = "") -> None:
    print(msg, flush=True)


# --- configuration ---------------------------------------------------------------------------------
class Config:
    def __init__(self, root: Path, env: Optional[Dict[str, str]] = None):
        env = os.environ if env is None else env
        values: Dict[str, str] = {}
        path = root / ENV_FILE
        if path.is_file():
            for line in path.read_text().splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key, val = key.strip(), val.strip()
                if len(val) >= 2 and val[0] == val[-1] == '"':
                    val = val[1:-1]
                if key in CONFIG_KEYS:
                    values[key] = val
        for key in CONFIG_KEYS:  # the environment wins over the file
            if env.get(key):
                values[key] = env[key]
        self.dest = Path(
            os.path.expanduser(values.get("GBX_SNAPSHOT_DIR") or "~/GeoBrix-Snapshots")
        )
        self.keep = self._int(values, "GBX_SNAPSHOT_KEEP", 3, minimum=1)
        self.max_file_mb = self._int(values, "GBX_SNAPSHOT_MAX_FILE_MB", 500, minimum=1)
        self.gitleaks = values.get("GBX_SNAPSHOT_GITLEAKS", "")
        self.scan_allow = [
            g.strip()
            for g in values.get("GBX_SNAPSHOT_SCAN_ALLOW", "").split(",")
            if g.strip()
        ]
        # Drive upload: off unless asked for. The folder id and quota project are read here and
        # never printed or logged; they live only in the untracked .gbx-local.env or the environment.
        self.upload = self._flag(values, "GBX_SNAPSHOT_UPLOAD")
        self.drive_folder_id = values.get("GBX_SNAPSHOT_DRIVE_FOLDER_ID", "")
        self.drive_keep = self._int(values, "GBX_SNAPSHOT_DRIVE_KEEP", 3, minimum=1)
        self.quota_project = values.get("GBX_SNAPSHOT_GCP_QUOTA_PROJECT", "")
        # The Drive folder must be private (only its owner) unless this is set. An upload that the
        # setting (not --upload) turned on, as from the push hook, runs at most once per interval.
        self.allow_shared = self._flag(values, "GBX_SNAPSHOT_DRIVE_ALLOW_SHARED")
        self.upload_min_interval_min = self._int(
            values, "GBX_SNAPSHOT_UPLOAD_MIN_INTERVAL_MIN", 60, minimum=0
        )

    @staticmethod
    def _flag(values: Dict[str, str], key: str) -> bool:
        return values.get(key, "").lower() in ("1", "true", "yes", "on")

    @staticmethod
    def _int(values: Dict[str, str], key: str, default: int, minimum: int) -> int:
        raw = values.get(key)
        if not raw:
            return default
        try:
            n = int(raw)
        except ValueError:
            raise SystemExit("%s must be a whole number (got %r)" % (key, raw))
        if n < minimum:
            raise SystemExit("%s must be at least %d (got %d)" % (key, minimum, n))
        return n

    @property
    def max_file_bytes(self) -> int:
        return self.max_file_mb * 1024 * 1024


# --- git -------------------------------------------------------------------------------------------
def git(root: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", "-C", str(root)] + list(args),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        check=check,
    )


def git_out(root: Path, *args: str) -> str:
    return git(root, *args, check=False).stdout.strip()


def git_identity(root: Path) -> Tuple[str, str]:
    """(branch, short sha) for the file name; detached HEAD is 'detached'."""
    branch = git_out(root, "rev-parse", "--abbrev-ref", "HEAD")
    if not branch or branch == "HEAD":
        branch = "detached"
    sha = git_out(root, "rev-parse", "--short", "HEAD") or "0000000"
    return re.sub(r"[^A-Za-z0-9._-]+", "-", branch).strip("-") or "detached", sha


def git_common_dir(root: Path) -> Path:
    out = git_out(root, "rev-parse", "--path-format=absolute", "--git-common-dir")
    return Path(out)


def hooks_dir(root: Path) -> Path:
    configured = git_out(root, "config", "--get", "core.hooksPath")
    if configured:
        p = Path(os.path.expanduser(configured))
        return p if p.is_absolute() else root / p
    return git_common_dir(root) / "hooks"


NOTHING_TO_BUNDLE = "every local commit is already on a remote"


def bundle_args(root: Path) -> Optional[List[str]]:
    """git arguments naming every commit no remote has (branches, tags, the stash, HEAD if
    detached), or None when there are none."""
    refs = ["--branches", "--tags"]
    if (
        git(root, "rev-parse", "--verify", "-q", "refs/stash", check=False).returncode
        == 0
    ):
        refs.append("refs/stash")
    if git_out(root, "rev-parse", "--abbrev-ref", "HEAD") == "HEAD":
        refs.append("HEAD")
    args = refs + ["--not", "--remotes"]
    return args if git_out(root, "rev-list", "--max-count=1", *args) else None


def hooks_are_local(root: Path) -> bool:
    """True when the hooks directory is inside this repository's git directory. A hooksPath that
    points elsewhere (shared tooling) is neither saved nor edited."""
    try:
        hooks_dir(root).resolve().relative_to(git_common_dir(root).resolve())
        return True
    except ValueError:
        return False


def make_bundle(root: Path, out: Path) -> Optional[str]:
    """Write the bundle. Returns None when written, or the reason nothing was bundled."""
    args = bundle_args(root)
    if args is None:
        return NOTHING_TO_BUNDLE
    res = git(root, "bundle", "create", str(out), *args, check=False)
    if res.returncode != 0:
        return "git bundle failed: " + res.stderr.strip().splitlines()[-1]
    return None


def git_meta_files(root: Path) -> List[Tuple[Path, str]]:
    """(source, name inside git-meta/) for the local git settings worth keeping."""
    common = git_common_dir(root)
    found: List[Tuple[Path, str]] = []
    if (common / "config").is_file():
        found.append((common / "config", "config"))
    if (common / "info" / "exclude").is_file():
        found.append((common / "info" / "exclude", "info/exclude"))
    hdir = hooks_dir(root)
    if hdir.is_dir() and hooks_are_local(root):
        for p in sorted(hdir.iterdir()):
            if p.is_file() and not p.name.endswith(".sample"):
                found.append((p, "hooks/" + p.name))
    return found


# --- what goes in ----------------------------------------------------------------------------------
class Plan:
    def __init__(self, root: Path, max_file_bytes: int):
        self.root = root
        self.max_file_bytes = max_file_bytes
        self.files: List[Tuple[Path, str, int]] = []  # (path, name in worktree/, bytes)
        self.excluded: Dict[str, Tuple[str, int]] = (
            {}
        )  # path -> (why, how often it matched)
        self.skipped_large: List[Tuple[str, int]] = []
        self.top_bytes: Dict[str, int] = {}

    @property
    def total_bytes(self) -> int:
        return sum(size for _, _, size in self.files)


def _root_excluded(rel: str) -> Optional[str]:
    for pattern, why in ROOT_EXCLUDES:
        if rel == pattern or fnmatch.fnmatchcase(rel, pattern):
            return why
    return None


def _name_excluded(name: str, is_dir: bool) -> Optional[Tuple[str, str]]:
    """(pattern, why) when a file or directory of this name is left out at any depth."""
    for pattern, why in DIR_EXCLUDES if is_dir else FILE_EXCLUDES:
        if fnmatch.fnmatchcase(name, pattern):
            return pattern, why
    return None


def collect(root: Path, max_file_bytes: int) -> Plan:
    plan = Plan(root, max_file_bytes)

    def note(key: str, why: str) -> None:
        # Matches at any depth (every __pycache__) fold into one row per pattern.
        prev = plan.excluded.get(key, (why, 0))
        plan.excluded[key] = (why, prev[1] + 1)

    stack = [""]
    while stack:
        rel_dir = stack.pop()
        here = root / rel_dir if rel_dir else root
        try:
            entries = sorted(os.scandir(here), key=lambda e: e.name)
        except OSError:
            continue
        for entry in entries:
            rel = rel_dir + "/" + entry.name if rel_dir else entry.name
            is_link = entry.is_symlink()
            is_dir = entry.is_dir(follow_symlinks=False)
            why = _root_excluded(rel)
            if why:
                note(rel, why)
                continue
            by_name = _name_excluded(entry.name, is_dir)
            if by_name:
                note("**/" + by_name[0], by_name[1])
                continue
            if is_dir and (Path(entry.path) / "pyvenv.cfg").is_file():
                note("**/(has pyvenv.cfg)", "virtual environment")
                continue
            if is_dir:
                stack.append(rel)
                continue
            try:
                st = entry.stat(follow_symlinks=False)
            except OSError:
                continue
            size = st.st_size
            if not (is_link or stat.S_ISREG(st.st_mode)):
                continue  # sockets, fifos
            if not is_link and size > plan.max_file_bytes:
                plan.skipped_large.append((rel, size))
                continue
            plan.files.append((Path(entry.path), rel, size))
            top = rel.split("/", 1)[0] if "/" in rel else "(root files)"
            plan.top_bytes[top] = plan.top_bytes.get(top, 0) + size
    plan.files.sort(key=lambda f: f[1])
    return plan


def estimate_zip_bytes(plan: Plan, sample_bytes: int = 32 * 1024 * 1024) -> int:
    """Deflate a spread of files to measure the ratio, then scale to the whole plan."""
    candidates = [
        f for f in plan.files if 0 < f[2] <= 8 * 1024 * 1024 and not f[0].is_symlink()
    ]
    if not candidates:
        return plan.total_bytes
    step = max(1, len(candidates) // 400)
    raw = packed = 0
    for path, _, size in candidates[::step]:
        try:
            data = path.read_bytes()
        except OSError:
            continue
        raw += len(data)
        packed += len(zlib.compress(data, 6))
        if raw >= sample_bytes:
            break
    return int(plan.total_bytes * (packed / raw)) if raw else plan.total_bytes


# --- secret scan -----------------------------------------------------------------------------------
# High-confidence token shapes only, so a hit is worth stopping for. The gitleaks pass, when a
# binary is found, adds its broader rule set.
SECRET_PATTERNS: List[Tuple[str, str]] = [
    (
        "private key",
        r"-----BEGIN (?:RSA |EC |DSA |OPENSSH |PGP |ENCRYPTED )?PRIVATE KEY(?: BLOCK)?-----",
    ),
    ("AWS access key id", r"\b(?:AKIA|ASIA)(?![0-9A-Z]{0,12}EXAMPLE)[0-9A-Z]{16}\b"),
    ("GitHub token", r"\bgh[pousr]_[A-Za-z0-9]{36,}\b"),
    ("GitHub token", r"\bgithub_pat_[A-Za-z0-9_]{50,}\b"),
    ("Slack token", r"\bxox[abprs]-[A-Za-z0-9-]{10,}\b"),
    ("Databricks token", r"\bdapi[0-9a-f]{32}\b"),
    ("Google API key", r"\bAIza[0-9A-Za-z_-]{35}\b"),
    ("Anthropic key", r"\bsk-ant-[A-Za-z0-9_-]{20,}"),
]
# Only in git settings: a remote URL that carries a password or token.
GITMETA_PATTERNS: List[Tuple[str, str]] = [
    ("credentials in a URL", r"https?://[^/\s:@]+:[^/\s:@]{8,}@"),
]


def _compile(patterns: List[Tuple[str, str]]) -> Tuple["re.Pattern[bytes]", List[str]]:
    names = [name for name, _ in patterns]
    alt = b"|".join(
        b"(?P<p%d>%s)" % (i, pat.encode()) for i, (_, pat) in enumerate(patterns)
    )
    return re.compile(alt), names


class Finding:
    def __init__(self, path: str, line: int, rule: str):
        self.path, self.line, self.rule = path, line, rule

    def __str__(self) -> str:  # never the matched text
        return "%s:%d  %s" % (self.path, self.line, self.rule)


def scan_file(path: Path, shown: str, extra: bool = False) -> List[Finding]:
    try:
        if path.is_symlink() or path.stat().st_size > SCAN_MAX_BYTES:
            return []
        data = path.read_bytes()
    except OSError:
        return []
    if b"\0" in data[:8192]:
        return []
    rx, names = _compile(SECRET_PATTERNS + (GITMETA_PATTERNS if extra else []))
    out = []
    for m in rx.finditer(data):
        idx = int(m.lastgroup[1:])  # type: ignore[index]
        out.append(Finding(shown, data.count(b"\n", 0, m.start()) + 1, names[idx]))
    return out


def find_gitleaks(configured: str) -> Optional[str]:
    for candidate in (configured, "gitleaks"):
        if not candidate:
            continue
        expanded = os.path.expanduser(candidate)
        found = (
            shutil.which(expanded)
            if os.sep not in expanded
            else (expanded if os.access(expanded, os.X_OK) else None)
        )
        if found:
            return found
    return None


def run_gitleaks(
    binary: str, root: Path, items: List[Tuple[Path, str]], workdir: Path
) -> Tuple[List[Finding], Optional[str]]:
    """Hard-link (else copy) the files into a scratch tree and scan it; (findings, error)."""
    stage = workdir / "gitleaks-stage"
    for src, name in items:
        if src.is_symlink():
            continue
        try:
            if src.stat().st_size > SCAN_MAX_BYTES:
                continue
        except OSError:
            continue
        dst = stage / name
        dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.link(src, dst)
        except OSError:
            try:
                shutil.copyfile(src, dst)
            except OSError:
                continue
    report = workdir / "gitleaks.json"
    cmd = [
        binary,
        "detect",
        "--no-git",
        "--source",
        str(stage),
        "--no-banner",
        "--report-format",
        "json",
        "--report-path",
        str(report),
    ]
    cfg = root / ".gitleaks.toml"
    if cfg.is_file():
        cmd += ["--config", str(cfg)]
    try:
        res = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=1800,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return [], "gitleaks did not run: %s" % exc
    if res.returncode not in (0, 1):
        tail = (res.stderr.strip().splitlines() or ["no output"])[-1]
        return [], "gitleaks exited %d: %s" % (res.returncode, tail)
    findings: List[Finding] = []
    if report.is_file() and report.stat().st_size:
        for hit in json.loads(report.read_text()):
            if hit.get("RuleID") in GITLEAKS_IGNORED_RULES:
                continue
            if "EXAMPLE" in str(
                hit.get("Secret", "")
            ):  # the vendors' documented sample keys
                continue
            rel = os.path.relpath(hit.get("File", ""), str(stage))
            findings.append(
                Finding(
                    rel,
                    int(hit.get("StartLine", 0)),
                    "gitleaks:" + str(hit.get("RuleID")),
                )
            )
    return findings, None


class ScanFacts:
    """What the scan did beyond its findings; a Drive upload needs a complete, unwaived scan."""

    def __init__(self, allowed: List[str], gitleaks_ran: bool):
        self.allowed, self.gitleaks_ran = allowed, gitleaks_ran


def scan_secrets(
    plan: Plan, meta: List[Tuple[Path, str]], cfg: Config, workdir: Path
) -> Tuple[List[Finding], List[str], ScanFacts]:
    findings: List[Finding] = []
    notes: List[str] = []
    gitleaks_ran = False
    for path, name, _ in plan.files:
        findings += scan_file(path, name)
    for path, name in meta:
        findings += scan_file(path, GITMETA + "/" + name, extra=True)
    binary = find_gitleaks(cfg.gitleaks)
    if binary:
        items = [(p, n) for p, n, _ in plan.files] + [
            (p, GITMETA + "/" + n) for p, n in meta
        ]
        more, err = run_gitleaks(binary, plan.root, items, workdir)
        findings += more
        if err:
            notes.append(err + " (the built-in scan still ran)")
        else:
            gitleaks_ran = True
    else:
        notes.append("gitleaks not found: built-in token scan only")
    seen = set()
    unique = []
    allowed = set()
    for f in findings:
        if any(fnmatch.fnmatchcase(f.path, g) for g in cfg.scan_allow):
            allowed.add(f.path)
        elif (f.path, f.line) not in seen:
            seen.add((f.path, f.line))
            unique.append(f)
    for path in sorted(allowed):
        notes.append(
            "secret scan skipped findings in %s (GBX_SNAPSHOT_SCAN_ALLOW)" % path
        )
    return unique, notes, ScanFacts(sorted(allowed), gitleaks_ran)


def rescan_zip(zip_path: Path) -> List[Finding]:
    """Scan the bytes inside the finished zip with the built-in patterns. The files can change
    between the scan and the write; this checks what will actually leave the machine."""
    plain, _ = _compile(SECRET_PATTERNS)
    meta, _ = _compile(SECRET_PATTERNS + GITMETA_PATTERNS)
    out: List[Finding] = []
    with zipfile.ZipFile(str(zip_path)) as zf:
        for info in zf.infolist():
            if info.file_size == 0 or info.file_size > SCAN_MAX_BYTES:
                continue
            if stat.S_ISLNK((info.external_attr >> 16) & 0xFFFF):
                continue
            data = zf.read(info)
            if b"\0" in data[:8192]:
                continue
            rx = meta if info.filename.startswith(GITMETA + "/") else plain
            for m in rx.finditer(data):
                out.append(
                    Finding(
                        info.filename, data.count(b"\n", 0, m.start()) + 1, "rescan"
                    )
                )
    return out


# --- the cluster config, minus its secrets ---------------------------------------------------------
# The real examples/tests/databricks_cluster_config.env holds tokens, so a snapshot leaves it out. A
# derived copy with every secret line removed goes in instead (gitignored, private snapshot only),
# and restore rebuilds the real file from the example's defaults plus those values.
CONFIG_DIR = "examples/tests"
CONFIG_REAL = CONFIG_DIR + "/databricks_cluster_config.env"
CONFIG_NONSECRET = CONFIG_DIR + "/databricks_cluster_config.nonsecret.env"
CONFIG_EXAMPLE = CONFIG_DIR + "/databricks_cluster_config.example.env"
DROPPED_PREFIX = "# Secret keys dropped: "
SECRET_KEY_RE = re.compile(
    r"(TOKEN|SECRET|PASSWORD|PASSWD|PRIVATE_?KEY|API_?KEY|CREDENTIAL)", re.I
)
# Secret-shaped values beyond the scan's patterns: JWTs, and any dapi token whatever its length.
DERIVE_EXTRA_PATTERNS = [
    r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}",
    r"\bdapi[0-9a-zA-Z]{20,}",
]
_CONFIG_LINE = re.compile(r"^\s*(#\s*)?(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$")


def _secret_shaped(text: str) -> bool:
    rx, _ = _compile(SECRET_PATTERNS)
    extra = re.compile("|".join(DERIVE_EXTRA_PATTERNS).encode())
    data = text.encode()
    return bool(rx.search(data) or extra.search(data))


def _placeholder(value: str) -> bool:
    v = value.strip().strip("\"'")
    return (not v) or "..." in v or v.startswith("<") or v.lower().startswith("your")


def derive_nonsecret_config(text: str) -> Tuple[str, List[str], int]:
    """(file text, secret key names dropped, other secret-shaped lines dropped) for the real
    config text. A line goes when its key looks like a secret and its value is not a placeholder,
    or when anything on it (commented or not) has the shape of a token."""
    kept: List[str] = []
    names: List[str] = []
    anonymous = 0
    for line in text.splitlines():
        m = _CONFIG_LINE.match(line)
        key_secret = bool(
            m and SECRET_KEY_RE.search(m.group(2)) and not _placeholder(m.group(3))
        )
        if key_secret or _secret_shaped(line):
            if m and m.group(2) not in names:
                names.append(m.group(2))
            elif not m:
                anonymous += 1
            continue
        kept.append(line)
    header = [
        "# Derived by gbx:local:snapshot from databricks_cluster_config.env with the secret lines removed.",
        "# Gitignored; it lives only in the private snapshot. Restore merges it over the example's defaults.",
        DROPPED_PREFIX + (", ".join(names) if names else "(none)"),
        "",
    ]
    return "\n".join(header + kept) + "\n", names, anonymous


def write_nonsecret_config(root: Path) -> Optional[Tuple[Path, List[str]]]:
    """Refresh the derived copy from the real config; None when there is no real config."""
    real = root / CONFIG_REAL
    if not real.is_file():
        return None
    body, names, _ = derive_nonsecret_config(real.read_text(errors="replace"))
    out = root / CONFIG_NONSECRET
    if not out.is_file() or out.read_text(errors="replace") != body:
        out.write_text(body)
        out.chmod(0o600)
    return out, names


def _config_values(text: str) -> Dict[str, str]:
    """The active KEY=value lines of a config file (a later line wins)."""
    out: Dict[str, str] = {}
    for line in text.splitlines():
        m = _CONFIG_LINE.match(line)
        if m and not m.group(1):
            out[m.group(2)] = m.group(3)
    return out


def rebuild_config(example: str, nonsecret: str) -> str:
    """The example text with the nonsecret values laid over it: an active line is replaced in
    place, the rest are appended."""
    values = _config_values(nonsecret)
    done = set()
    lines = []
    for line in example.splitlines():
        m = _CONFIG_LINE.match(line)
        if m and not m.group(1) and m.group(2) in values:
            lines.append("%s=%s" % (m.group(2), values[m.group(2)]))
            done.add(m.group(2))
        else:
            lines.append(line)
    rest = [k for k in values if k not in done]
    if rest:
        lines += ["", "# Restored from the snapshot"]
        lines += ["%s=%s" % (k, values[k]) for k in rest]
    return "\n".join(lines) + "\n"


def dropped_keys(nonsecret: str) -> List[str]:
    for line in nonsecret.splitlines():
        if line.startswith(DROPPED_PREFIX):
            rest = line[len(DROPPED_PREFIX) :].strip()
            return [] if rest == "(none)" else [k.strip() for k in rest.split(",")]
    return []


# --- zip -------------------------------------------------------------------------------------------
def zip_name(root: Path, now: Optional[datetime] = None) -> str:
    now = now or datetime.now(timezone.utc)
    branch, sha = git_identity(root)
    return "geobrix-snapshot-%s-%s-%s.zip" % (
        now.strftime("%Y%m%dT%H%M%SZ"),
        branch,
        sha,
    )


ZIP_EPOCH = (
    315532800 + 2 * 86400
)  # 1980-01-03: zip stores nothing before 1980, whatever the zone


def _zip_info(arcname: str, st: os.stat_result) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(arcname, time.localtime(max(st.st_mtime, ZIP_EPOCH))[:6])
    info.create_system = 3  # Unix: the mode bits below are honored on extract
    return info


def _add_symlink(zf: zipfile.ZipFile, link: Path, arcname: str) -> None:
    info = _zip_info(arcname, link.lstat())
    info.external_attr = (stat.S_IFLNK | 0o777) << 16
    zf.writestr(info, os.readlink(str(link)))


def _add_file(zf: zipfile.ZipFile, path: Path, arcname: str) -> bool:
    if path.is_symlink():
        _add_symlink(zf, path, arcname)
        return True
    try:
        st = path.stat()
        info = _zip_info(arcname, st)
        info.external_attr = (stat.S_IMODE(st.st_mode) | stat.S_IFREG) << 16
        info.compress_type = zipfile.ZIP_DEFLATED
        with (
            open(str(path), "rb") as fin,
            zf.open(info, "w", force_zip64=st.st_size >= zipfile.ZIP64_LIMIT) as fout,
        ):
            shutil.copyfileobj(fin, fout, 1024 * 1024)
        return True
    except OSError as exc:
        say("  warning: skipped %s (%s)" % (arcname, exc.strerror or exc))
        return False


def write_zip(
    out: Path,
    plan: Plan,
    meta: List[Tuple[Path, str]],
    bundle: Optional[Path],
    info: str,
) -> int:
    part = out.with_name("." + out.name + ".part")
    count = 0
    try:
        with zipfile.ZipFile(
            str(part), "w", zipfile.ZIP_DEFLATED, compresslevel=6, allowZip64=True
        ) as zf:
            zf.writestr("SNAPSHOT-INFO.txt", info)
            for src, name in meta:
                count += _add_file(zf, src, GITMETA + "/" + name)
            if bundle is not None:
                count += _add_file(zf, bundle, GITMETA + "/unpushed.bundle")
            for path, name, _ in plan.files:
                count += _add_file(zf, path, TREE + "/" + name)
        os.replace(str(part), str(out))
    finally:
        if part.exists():
            part.unlink()
    return count


# --- retention -------------------------------------------------------------------------------------
def snapshots(dest: Path) -> List[Path]:
    """Snapshot zips in dest, newest first (by the timestamp in the name)."""
    if not dest.is_dir():
        return []
    found = [p for p in dest.iterdir() if p.is_file() and SNAPSHOT_RE.match(p.name)]
    return sorted(found, key=lambda p: SNAPSHOT_RE.match(p.name).group(1), reverse=True)  # type: ignore[union-attr]


def prune(dest: Path, keep: int) -> List[Path]:
    """Delete snapshot zips beyond the newest `keep`. Other files are never touched."""
    removed = []
    for p in snapshots(dest)[keep:]:
        p.unlink()
        removed.append(p)
    return removed


def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return "%d B" % n if unit == "B" else "%.1f %s" % (n, unit)
        n /= 1024.0
    return "%.1f GB" % n


# --- Drive (off unless asked for) ------------------------------------------------------------------
DRIVE_MODULE = "gbx_snapshot_drive"


def drive_module():
    """Load drive_upload.py from beside this file. Called only when a Drive step is requested, so
    a plain snapshot never imports the network code."""
    if DRIVE_MODULE not in sys.modules:
        path = Path(__file__).with_name("drive_upload.py")
        spec = importlib.util.spec_from_file_location(DRIVE_MODULE, str(path))
        mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
        sys.modules[DRIVE_MODULE] = mod
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return sys.modules[DRIVE_MODULE]


def make_drive_client(cfg: Config):
    """The real client. The tests replace this function with one backed by a fake Drive."""
    drive = drive_module()
    return drive.DriveClient(
        drive.GcloudToken(),
        quota_project=cfg.quota_project,
        say=say,
        private=[cfg.drive_folder_id],
        allow_shared=cfg.allow_shared,
    )


# The last verified upload, as epoch seconds, kept beside the lock in the snapshot folder.
UPLOAD_STAMP = ".last-drive-upload"


def minutes_since_upload(cfg: Config) -> Optional[float]:
    """Minutes since the last verified Drive upload; None if none, unreadable, or in the future."""
    try:
        then = float((cfg.dest / UPLOAD_STAMP).read_text().strip())
    except (OSError, ValueError):
        return None
    age = (time.time() - then) / 60.0
    return age if age >= 0 else None


def record_upload(cfg: Config) -> None:
    """Note a verified upload. Best effort: a failed write only means the next run is not throttled."""
    try:
        tmp = cfg.dest / (UPLOAD_STAMP + ".tmp")
        tmp.write_text("%.0f\n" % time.time())
        os.replace(str(tmp), str(cfg.dest / UPLOAD_STAMP))
    except OSError as exc:
        say("drive: could not record the upload time (%s)" % exc.__class__.__name__)


def upload_throttled(cfg: Config) -> Optional[float]:
    """Minutes since the last upload if that is inside the interval (so skip), else None."""
    if cfg.upload_min_interval_min <= 0:
        return None
    age = minutes_since_upload(cfg)
    return age if age is not None and age < cfg.upload_min_interval_min else None


def drive_problem(cfg: Config) -> Optional[str]:
    problems = drive_module().config_problems(cfg.drive_folder_id, cfg.quota_project)
    return "; ".join(problems) or None


def drive_failure(cfg: Config, exc: Exception) -> str:
    """One printable line for a failed Drive step: no token, no folder id, no quota project."""
    drive = drive_module()
    if isinstance(exc, drive.DriveError):
        return drive.redact(str(exc), [cfg.drive_folder_id, cfg.quota_project])
    return "unexpected %s" % type(exc).__name__


def drive_upload_step(
    cfg: Config, out: Path, findings: List[Finding], facts: ScanFacts
) -> Tuple[int, str]:
    """Upload the zip run just wrote, verify it, then prune the Drive folder.
    Returns (exit code, short text for the RESULT line). Never raises."""
    drive = drive_module()
    problem = drive_problem(cfg)
    if problem:
        say("drive: not uploading: " + problem)
        return EXIT_DRIVE, "not-configured"
    try:
        produced = drive.Produced.after_scan(
            out,
            findings,
            rescan_zip(out),
            facts.allowed,
            facts.gitleaks_ran,
            SNAPSHOT_RE,
        )
        client = make_drive_client(cfg)
        say("drive: uploading %s (%s)" % (out.name, human(produced.size)))
        info = client.upload(produced, cfg.drive_folder_id, SNAPSHOT_RE)
    except Exception as exc:  # a Drive failure must never break the push
        say("drive: FAILED: " + drive_failure(cfg, exc))
        return EXIT_DRIVE, (
            "refused" if isinstance(exc, drive.UploadRefused) else "failed"
        )
    say("drive: uploaded %s" % out.name)
    record_upload(cfg)
    try:
        res = client.prune(
            cfg.drive_folder_id, cfg.drive_keep, SNAPSHOT_RE, protect_ids=[info["id"]]
        )
    except Exception as exc:
        say("drive: uploaded, but the prune did not run: " + drive_failure(cfg, exc))
        return EXIT_DRIVE, "uploaded,prune-failed"
    if res.deferred:
        say(
            "drive: %d older snapshot(s) remain; the next run continues"
            % len(res.deferred)
        )
    return 0, "uploaded,trashed=%d" % len(res.trashed)


def drive_dry_check(cfg: Config) -> int:
    """--upload --dry-run: read-only checks of the Drive settings, folder, and what a prune would do."""
    drive = drive_module()
    say("Drive upload (dry run: reads only; nothing is uploaded, trashed, or deleted):")
    problem = drive_problem(cfg)
    if problem:
        say("  not ready: " + problem)
        return EXIT_DRIVE
    say(
        "  gitleaks: %s"
        % (
            "found"
            if find_gitleaks(cfg.gitleaks)
            else "NOT found: an upload is refused until it is"
        )
    )
    say("  quota project: %s" % ("set" if cfg.quota_project else "not set"))
    age = minutes_since_upload(cfg)
    if cfg.upload_min_interval_min <= 0:
        say("  throttle: off (GBX_SNAPSHOT_UPLOAD_MIN_INTERVAL_MIN=0)")
    elif age is None:
        say("  throttle: no upload recorded; the next hook-started upload would run")
    else:
        wait = cfg.upload_min_interval_min - age
        say(
            "  throttle: last upload %.0f min ago, interval %d min; the next hook-started "
            "upload would %s"
            % (
                age,
                cfg.upload_min_interval_min,
                "be skipped for %.0f more min" % wait if wait > 0 else "run",
            )
        )
    say(
        "  an explicit --upload ignores the throttle (add --respect-throttle to keep it)"
    )
    refused = False
    try:
        client = make_drive_client(cfg)
        folder = client.get_folder(cfg.drive_folder_id)
        can_add = folder.get("capabilities", {}).get("canAddChildren", False)
        say(
            "  folder: %r   you can add files: %s"
            % (folder.get("name"), "yes" if can_add else "NO")
        )
        try:
            others = drive.other_access(
                folder, client.list_permissions(cfg.drive_folder_id)
            )
        except drive.DriveTimeout:
            raise
        except drive.DriveError as exc:
            others = ["its sharing could not be read (%s)" % drive_failure(cfg, exc)]
        say("  shared with: " + ("; ".join(others) if others else "no one (private)"))
        if not others:
            say("  guard: ok, the folder is private")
        elif cfg.allow_shared:
            say(
                "  guard: the folder is shared; accepted by GBX_SNAPSHOT_DRIVE_ALLOW_SHARED"
            )
        else:
            refused = True
            say("  guard: WOULD REFUSE (exit %d). %s" % (EXIT_DRIVE, drive.SHARED_FIX))
        listing = client.list_folder(cfg.drive_folder_id)
        owned, foreign = drive.snapshot_files(listing, SNAPSHOT_RE)
        say(
            "  in the folder: %d snapshot(s) of yours, %d snapshot-named file(s) not yours, %d item(s) in all"
            % (len(owned), len(foreign), len(listing))
        )
        drive.plan_prune(
            listing, cfg.drive_keep, SNAPSHOT_RE
        )  # raises if the prune would refuse
        stale = owned[
            cfg.drive_keep - 1 :
        ]  # the new upload takes one of the kept places
        say(
            "  after the next upload (keep %d): would trash %d"
            % (cfg.drive_keep, len(stale))
        )
        for f in stale:
            say("    would trash %s" % f["name"])
    except Exception as exc:
        say("  FAILED: " + drive_failure(cfg, exc))
        return EXIT_DRIVE
    return EXIT_DRIVE if refused else 0


def drive_list(cfg: Config) -> int:
    drive = drive_module()
    problem = drive_problem(cfg)
    if problem:
        say("drive: " + problem)
        return EXIT_DRIVE
    try:
        listing = make_drive_client(cfg).list_folder(cfg.drive_folder_id)
    except Exception as exc:
        say("drive: FAILED: " + drive_failure(cfg, exc))
        return EXIT_DRIVE
    owned, foreign = drive.snapshot_files(listing, SNAPSHOT_RE)
    if not owned and not foreign:
        say("no snapshots in the Drive folder")
    for f in owned:
        say("%-70s %10s" % (f["name"], human(int(f.get("size", 0)))))
    for f in foreign:
        say(
            "%-70s %10s  (not yours; never pruned or restored)"
            % (f["name"], human(int(f.get("size", 0))))
        )
    return 0


def drive_prune(cfg: Config, keep: int, dry_run: bool) -> int:
    problem = drive_problem(cfg)
    if problem:
        say("drive: " + problem)
        return EXIT_DRIVE
    try:
        res = make_drive_client(cfg).prune(
            cfg.drive_folder_id, keep, SNAPSHOT_RE, dry_run=dry_run
        )
    except Exception as exc:
        say("drive: nothing removed: " + drive_failure(cfg, exc))
        return EXIT_DRIVE
    say(
        "%skept %d, %s %d"
        % (
            "dry run: " if dry_run else "",
            len(res.kept),
            "would trash" if dry_run else "trashed",
            len(res.trashed),
        )
    )
    if res.deferred:
        say("%d older snapshot(s) remain; run again to continue" % len(res.deferred))
    if res.foreign:
        say(
            "%d snapshot-named file(s) not owned by you were left alone"
            % len(res.foreign)
        )
    return 0


def drive_restore(cfg: Config, name: str, target: Path) -> int:
    drive = drive_module()
    if not SNAPSHOT_RE.fullmatch(name):
        say("not a snapshot name: %s" % name)
        return 2
    problem = drive_problem(cfg)
    if problem:
        say("drive: " + problem)
        return EXIT_DRIVE
    if target.exists() and any(target.iterdir()):
        say("%s is not empty; restore into a new or empty directory" % target)
        return 2
    try:
        client = make_drive_client(cfg)
        owned, _ = drive.snapshot_files(
            client.list_folder(cfg.drive_folder_id), SNAPSHOT_RE
        )
        match = [f for f in owned if f["name"] == name]
        if not match:
            say("no snapshot of yours named %s in the Drive folder" % name)
            return 2
        cfg.dest.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".restore-", dir=str(cfg.dest)) as tmp:
            zip_path = Path(tmp) / name
            say("downloading %s (%s)" % (name, human(int(match[0].get("size", 0)))))
            client.download(match[0], zip_path)
            say("downloaded; md5 matches Drive")
            return cmd_restore(cfg, str(zip_path), target)
    except Exception as exc:
        say("drive: FAILED: " + drive_failure(cfg, exc))
        return EXIT_DRIVE


# --- commands --------------------------------------------------------------------------------------
def report_plan(plan: Plan, est: Optional[int]) -> None:
    say("Include (working tree, top level, uncompressed):")
    for top, size in sorted(plan.top_bytes.items(), key=lambda kv: -kv[1]):
        say("  %-28s %10s" % (top, human(size)))
    say("Exclude (re-creatable run state):")
    for path, (why, n) in sorted(plan.excluded.items()):
        say("  %-36s %s%s" % (path, why, "  (x%d)" % n if n > 1 else ""))
    say(
        "Files: %d   uncompressed: %s%s"
        % (
            len(plan.files),
            human(plan.total_bytes),
            "   estimated zip: ~%s" % human(est) if est is not None else "",
        )
    )
    if plan.skipped_large:
        say("Skipped (over %d MB):" % (plan.max_file_bytes // (1024 * 1024)))
        for rel, size in plan.skipped_large:
            say("  %s  %s" % (rel, human(size)))


def cmd_dry_run(root: Path, cfg: Config, check_drive: bool = False) -> int:
    plan = collect(root, cfg.max_file_bytes)
    report_plan(plan, estimate_zip_bytes(plan))
    meta = git_meta_files(root)
    say("Git settings: %s" % (", ".join(n for _, n in meta) or "none"))
    say(
        "Bundle: %s"
        % (
            "unpushed commits will be bundled"
            if bundle_args(root)
            else NOTHING_TO_BUNDLE
        )
    )
    say("Destination: %s   keep newest %d" % (cfg.dest, cfg.keep))
    if check_drive:
        return drive_dry_check(cfg)
    if cfg.upload:
        say(
            "Drive upload: on (GBX_SNAPSHOT_UPLOAD); add --upload to this --dry-run to check the folder (reads only)"
        )
    return 0


def cmd_run(
    root: Path, cfg: Config, upload: bool = False, throttle: bool = False
) -> int:
    started = time.time()
    cfg.dest.mkdir(parents=True, exist_ok=True)
    lock = open(str(cfg.dest / ".snapshot.lock"), "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        say("RESULT busy: another snapshot is running")
        return EXIT_BUSY
    with tempfile.TemporaryDirectory(prefix=".snapshot-", dir=str(cfg.dest)) as tmp:
        work = Path(tmp)
        say("snapshot of %s -> %s" % (root.name, cfg.dest))
        derived = write_nonsecret_config(root)
        if derived:
            say(
                "cluster config: kept %s (dropped %d secret key(s))"
                % (CONFIG_NONSECRET, len(derived[1]))
            )
        plan = collect(root, cfg.max_file_bytes)
        say("collected %d files, %s" % (len(plan.files), human(plan.total_bytes)))
        for rel, size in plan.skipped_large:
            say(
                "  warning: skipped %s (%s, over the %d MB cap)"
                % (rel, human(size), cfg.max_file_mb)
            )
        meta = git_meta_files(root)
        bundle: Optional[Path] = work / "unpushed.bundle"
        reason = make_bundle(root, bundle)  # type: ignore[arg-type]
        if reason:
            say("bundle: none (%s)" % reason)
            bundle = None
        else:
            say("bundle: %s" % human(bundle.stat().st_size))  # type: ignore[union-attr]
        findings, notes, facts = scan_secrets(plan, meta, cfg, work)
        for n in notes:
            say("  note: " + n)
        if findings:
            say(
                "REFUSED: %d possible secret(s); nothing was written. Move or remove them, then run again:"
                % len(findings)
            )
            for f in findings[:50]:
                say("  " + str(f))
            if len(findings) > 50:
                say("  ... and %d more" % (len(findings) - 50))
            say("RESULT refused: secret scan")
            return EXIT_REFUSED
        branch, sha = git_identity(root)
        out = cfg.dest / zip_name(root)
        info = "\n".join(
            [
                "GeoBrix snapshot",
                "created: %s"
                % datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "branch: %s" % branch,
                "commit: %s" % sha,
                "worktree/: the working tree, minus rebuildable run state",
                "git-meta/: local git settings%s"
                % (" and unpushed.bundle (commits no remote had)" if bundle else ""),
                "restore: gbx:local:snapshot restore <this zip> <empty dir>",
                "",
            ]
        )
        n = write_zip(out, plan, meta, bundle, info)
        removed = prune(cfg.dest, cfg.keep)
    secs = time.time() - started
    say(
        "wrote %s (%s, %d entries) in %.0fs"
        % (out.name, human(out.stat().st_size), n, secs)
    )
    for p in removed:
        say("pruned %s" % p.name)
    drive_code, drive_text = 0, ""
    age = upload_throttled(cfg) if upload and throttle else None
    if age is not None:  # the local snapshot is done; only the upload is skipped
        say(
            "drive: skipping the upload: the last one was %.0f min ago "
            "(GBX_SNAPSHOT_UPLOAD_MIN_INTERVAL_MIN=%d); run with --upload to force one"
            % (age, cfg.upload_min_interval_min)
        )
        drive_text = "throttled"
    elif upload:  # only a zip written just now, and only past the clean scan above
        drive_code, drive_text = drive_upload_step(cfg, out, findings, facts)
        secs = time.time() - started
    result = "%s %s %.0fs" % (out.name, human(out.stat().st_size), secs)
    if drive_code:
        say("RESULT drive-failed %s drive=%s" % (result, drive_text))
        return drive_code
    say("RESULT ok %s%s" % (result, " drive=" + drive_text if drive_text else ""))
    return 0


def cmd_list(cfg: Config, drive: bool = False) -> int:
    if drive:
        return drive_list(cfg)
    found = snapshots(cfg.dest)
    if not found:
        say("no snapshots in %s" % cfg.dest)
        return 0
    for p in found:
        say("%-70s %10s" % (p.name, human(p.stat().st_size)))
    return 0


def cmd_prune(
    cfg: Config, keep: int, drive: bool = False, dry_run: bool = False
) -> int:
    if drive:
        return drive_prune(cfg, keep, dry_run)
    removed = prune(cfg.dest, keep)
    for p in removed:
        say("pruned %s" % p.name)
    say("kept %d, pruned %d" % (len(snapshots(cfg.dest)), len(removed)))
    return 0


def cmd_restore(cfg: Config, name: str, target: Path, from_drive: bool = False) -> int:
    if from_drive:
        return drive_restore(cfg, name, target)
    src = Path(name)
    if not src.is_file():
        src = cfg.dest / name
    if not src.is_file():
        say("no such snapshot: %s" % name)
        return 2
    if target.exists() and any(target.iterdir()):
        say("%s is not empty; restore into a new or empty directory" % target)
        return 2
    target.mkdir(parents=True, exist_ok=True)
    root = target.resolve()
    with zipfile.ZipFile(str(src)) as zf:
        for info in zf.infolist():
            dest = (root / info.filename).resolve()
            if root != dest and root not in dest.parents:
                say("refusing member outside the target: %s" % info.filename)
                return 2
            mode = (info.external_attr >> 16) & 0xFFFF
            dest.parent.mkdir(parents=True, exist_ok=True)
            if stat.S_ISLNK(mode):
                if dest.is_symlink() or dest.exists():
                    dest.unlink()
                os.symlink(zf.read(info).decode(), str(dest))
            else:
                with zf.open(info) as fin, open(str(dest), "wb") as fout:
                    shutil.copyfileobj(fin, fout)
                if mode:
                    os.chmod(str(dest), stat.S_IMODE(mode))
    say("restored into %s" % target)
    say("  %s/%s/   the working tree" % (target, TREE))
    restore_cluster_config(target / TREE)
    bundle = target / GITMETA / "unpushed.bundle"
    if bundle.is_file():
        say("  Unpushed commits: clone the repository, then in the clone run")
        say(
            "    git fetch %s '+refs/heads/*:refs/heads/*' 'refs/tags/*:refs/tags/*'"
            % bundle
        )
        say(
            "    (the stash, if any, is refs/stash: git fetch %s refs/stash:refs/stash)"
            % bundle
        )
    else:
        say("  No bundle: every local commit was on a remote.")
    say(
        "  Local git settings are in %s/%s/ (config, info/exclude, hooks/)."
        % (target, GITMETA)
    )
    say(
        "  Copy the working-tree files over the clone; do not copy git-meta/config blindly."
    )
    return 0


def restore_cluster_config(tree: Path) -> None:
    """Rebuild the real cluster config in a restored tree when it is missing. Never overwrites."""
    real = tree / CONFIG_REAL
    derived = tree / CONFIG_NONSECRET
    if real.exists() or not derived.is_file():
        return
    example = tree / CONFIG_EXAMPLE
    nonsecret = derived.read_text(errors="replace")
    real.write_text(
        rebuild_config(
            example.read_text(errors="replace") if example.is_file() else "", nonsecret
        )
    )
    real.chmod(0o600)
    say(
        "  rebuilt %s from the example defaults and the snapshot's values" % CONFIG_REAL
    )
    keys = dropped_keys(nonsecret)
    if keys:
        say("  Re-enter these secret keys there: %s" % ", ".join(keys))
    say(
        "  Tokens normally come from the profiles in ~/.databrickscfg (DATABRICKS_CONFIG_PROFILE)."
    )


def last_log(root: Path) -> Optional[Path]:
    logs = (
        sorted((root / "test-logs" / "snapshot").glob("*.log"))
        if (root / "test-logs" / "snapshot").is_dir()
        else []
    )
    return logs[-1] if logs else None


def cmd_status(root: Path, cfg: Config) -> int:
    log = last_log(root)
    if log is None:
        say("last run: none logged (test-logs/snapshot/)")
    else:
        lines = [
            ln for ln in log.read_text(errors="replace").splitlines() if ln.strip()
        ]
        result = next((ln for ln in reversed(lines) if ln.startswith("RESULT ")), None)
        age = time.time() - log.stat().st_mtime
        say("last run: %s (%.0f min ago)" % (log.name, age / 60))
        say(
            "  " + (result or "no RESULT line: still running, or it died (see the log)")
        )
    found = snapshots(cfg.dest)
    say("snapshots in %s: %d (keep %d)" % (cfg.dest, len(found), cfg.keep))
    if found:
        say("  newest %s  %s" % (found[0].name, human(found[0].stat().st_size)))
    say(
        "pre-push hook: %s" % ("installed" if hook_installed(root) else "not installed")
    )
    return 0


# --- hook ------------------------------------------------------------------------------------------
def hook_block() -> str:
    return "\n".join(
        [
            HOOK_BEGIN,
            "gbx_snapshot_rc=$?  # exit status of the hook lines above (git lfs)",
            'if [ "$gbx_snapshot_rc" -eq 0 ]; then',
            "  gbx_snapshot_root=$(git rev-parse --show-toplevel 2>/dev/null)",
            '  gbx_snapshot_cmd="$gbx_snapshot_root/scripts/commands/gbx-local-snapshot.sh"',
            '  if [ -n "$gbx_snapshot_root" ] && [ -x "$gbx_snapshot_cmd" ]; then',
            "    # Detached so the push does not wait for the snapshot; a failure never fails the push.",
            '    nohup "$gbx_snapshot_cmd" run --log "snapshot/$(date -u +%Y%m%dT%H%M%SZ).log" </dev/null >/dev/null 2>&1 &',
            "  fi",
            "fi",
            'exit "$gbx_snapshot_rc"',
            HOOK_END,
            "",
        ]
    )


def _strip_block(text: str) -> str:
    out, skipping = [], False
    for line in text.splitlines(keepends=True):
        if line.strip() == HOOK_BEGIN:
            skipping = True
        if not skipping:
            out.append(line)
        if skipping and line.strip() == HOOK_END:
            skipping = False
    return "".join(out)


def hook_path(root: Path) -> Path:
    return hooks_dir(root) / "pre-push"


def hook_installed(root: Path) -> bool:
    p = hook_path(root)
    return p.is_file() and HOOK_BEGIN in p.read_text()


def install_hook(root: Path) -> int:
    if not hooks_are_local(root):
        say(
            "core.hooksPath points outside this repository (%s); not editing shared hooks"
            % hooks_dir(root)
        )
        return 2
    p = hook_path(root)
    p.parent.mkdir(parents=True, exist_ok=True)
    text = p.read_text() if p.is_file() else "#!/bin/sh\n"
    text = _strip_block(text)
    if text and not text.endswith("\n"):
        text += "\n"
    p.write_text(text + hook_block())
    p.chmod(p.stat().st_mode | 0o111)
    say("installed the snapshot block in %s" % p)
    return 0


def uninstall_hook(root: Path) -> int:
    p = hook_path(root)
    if not hook_installed(root):
        say("no snapshot block in %s" % p)
        return 0
    p.write_text(_strip_block(p.read_text()))
    say("removed the snapshot block from %s" % p)
    return 0


# --- main ------------------------------------------------------------------------------------------
def repo_root(start: Path) -> Path:
    out = git_out(start, "rev-parse", "--show-toplevel")
    if not out:
        raise SystemExit("not inside a git repository: %s" % start)
    return Path(out)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        prog="gbx-local-snapshot", description=__doc__.splitlines()[0]
    )
    ap.add_argument(
        "--root",
        default=".",
        help="repository to snapshot (default: the current directory's)",
    )
    sub = ap.add_subparsers(dest="cmd")
    run = sub.add_parser("run", help="write a snapshot, then prune (default)")
    run.add_argument(
        "--dry-run",
        action="store_true",
        help="report what would be included, write nothing",
    )
    run.add_argument(
        "--upload",
        action="store_true",
        help="after the local write, upload the zip to Google Drive (or GBX_SNAPSHOT_UPLOAD=1); "
        "with --dry-run, check the Drive folder (reads only). Ignores the upload throttle",
    )
    run.add_argument(
        "--respect-throttle",
        action="store_true",
        help="with --upload: skip the upload if one was made within "
        "GBX_SNAPSHOT_UPLOAD_MIN_INTERVAL_MIN",
    )
    ls = sub.add_parser("list", help="list snapshots")
    ls.add_argument(
        "--drive", action="store_true", help="list the Drive folder instead"
    )
    pr = sub.add_parser("prune", help="delete snapshots beyond the newest N")
    pr.add_argument(
        "--keep",
        type=int,
        help="how many to keep (default GBX_SNAPSHOT_KEEP, or GBX_SNAPSHOT_DRIVE_KEEP with --drive)",
    )
    pr.add_argument(
        "--drive", action="store_true", help="prune the Drive folder (to its trash)"
    )
    pr.add_argument(
        "--dry-run", action="store_true", help="with --drive: print, remove nothing"
    )
    rs = sub.add_parser("restore", help="unpack a snapshot into an empty directory")
    rs.add_argument(
        "--from-drive",
        action="store_true",
        help="download the named snapshot from the Drive folder first",
    )
    rs.add_argument("name")
    rs.add_argument("dir")
    sub.add_parser("status", help="show the last run")
    sub.add_parser("install-hook", help="snapshot after each successful push")
    sub.add_parser("uninstall-hook", help="remove that hook block")
    args = ap.parse_args(argv)

    root = repo_root(Path(args.root).resolve())
    cfg = Config(root)
    cmd = args.cmd or "run"
    if cmd == "run":
        if getattr(args, "dry_run", False):
            return cmd_dry_run(root, cfg, check_drive=getattr(args, "upload", False))
        explicit = getattr(args, "upload", False)  # asked for on the command line
        return cmd_run(
            root,
            cfg,
            upload=explicit or cfg.upload,
            throttle=getattr(args, "respect_throttle", False) or not explicit,
        )
    if cmd == "list":
        return cmd_list(cfg, drive=args.drive)
    if cmd == "prune":
        if args.dry_run and not args.drive:
            raise SystemExit("prune --dry-run needs --drive")
        default = cfg.drive_keep if args.drive else cfg.keep
        keep = args.keep if args.keep is not None else default
        if keep < 1:
            raise SystemExit("--keep must be at least 1")
        return cmd_prune(cfg, keep, drive=args.drive, dry_run=args.dry_run)
    if cmd == "restore":
        return cmd_restore(cfg, args.name, Path(args.dir), from_drive=args.from_drive)
    if cmd == "status":
        return cmd_status(root, cfg)
    if cmd == "install-hook":
        return install_hook(root)
    return uninstall_hook(root)


if __name__ == "__main__":
    sys.exit(main())
