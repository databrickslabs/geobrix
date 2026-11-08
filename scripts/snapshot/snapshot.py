#!/usr/bin/env python3
"""Local project snapshot for gbx:local:snapshot: a rolling zip of what git does not hold.

Stdlib only and Python 3.8+: it runs on the host, outside the dev container.

A snapshot is the working tree (untracked and gitignored files too, such as local planning
notes and editor settings), minus run state that can be rebuilt (see EXCLUDES), plus a git
bundle of the commits no remote has yet and the repository's local git settings. It is written
to a local folder; nothing here touches the network.
"""

from __future__ import annotations

import argparse
import fcntl
import fnmatch
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
)
# gitleaks rules left out of the verdict: entropy guesses that flag run ids and heartbeat fields in
# local logs. The token-shaped rules (keys, JWTs, cloud and vendor tokens) all stay on.
GITLEAKS_IGNORED_RULES = {"generic-api-key"}
HOOK_BEGIN = "# >>> gbx-local-snapshot (managed by gbx:local:snapshot) >>>"
HOOK_END = "# <<< gbx-local-snapshot <<<"
SCAN_MAX_BYTES = 20 * 1024 * 1024

EXIT_REFUSED = 3  # the secret scan found something
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


def scan_secrets(
    plan: Plan, meta: List[Tuple[Path, str]], cfg: Config, workdir: Path
) -> Tuple[List[Finding], List[str]]:
    findings: List[Finding] = []
    notes: List[str] = []
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
    return unique, notes


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


def cmd_dry_run(root: Path, cfg: Config) -> int:
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
    return 0


def cmd_run(root: Path, cfg: Config) -> int:
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
        findings, notes = scan_secrets(plan, meta, cfg, work)
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
    say("RESULT ok %s %s %.0fs" % (out.name, human(out.stat().st_size), secs))
    return 0


def cmd_list(cfg: Config) -> int:
    found = snapshots(cfg.dest)
    if not found:
        say("no snapshots in %s" % cfg.dest)
        return 0
    for p in found:
        say("%-70s %10s" % (p.name, human(p.stat().st_size)))
    return 0


def cmd_prune(cfg: Config, keep: int) -> int:
    removed = prune(cfg.dest, keep)
    for p in removed:
        say("pruned %s" % p.name)
    say("kept %d, pruned %d" % (len(snapshots(cfg.dest)), len(removed)))
    return 0


def cmd_restore(cfg: Config, name: str, target: Path) -> int:
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
    sub.add_parser("list", help="list snapshots")
    pr = sub.add_parser("prune", help="delete snapshots beyond the newest N")
    pr.add_argument(
        "--keep", type=int, help="how many to keep (default GBX_SNAPSHOT_KEEP)"
    )
    rs = sub.add_parser("restore", help="unpack a snapshot into an empty directory")
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
        return (
            cmd_dry_run(root, cfg)
            if getattr(args, "dry_run", False)
            else cmd_run(root, cfg)
        )
    if cmd == "list":
        return cmd_list(cfg)
    if cmd == "prune":
        keep = args.keep if args.keep is not None else cfg.keep
        if keep < 1:
            raise SystemExit("--keep must be at least 1")
        return cmd_prune(cfg, keep)
    if cmd == "restore":
        return cmd_restore(cfg, args.name, Path(args.dir))
    if cmd == "status":
        return cmd_status(root, cfg)
    if cmd == "install-hook":
        return install_hook(root)
    return uninstall_hook(root)


if __name__ == "__main__":
    sys.exit(main())
