"""Deferred cleanup of the workspace items the launchers make (``gbx:workspace:sweep``).

A run page opens the run's notebook from the workspace: once the notebook is deleted, the page cannot show
it. So a launcher keeps its run's notebooks when the run ends and records them here; the sweep removes them
later.

* **The window.** An item is removed once it is older than the window (``--older-than``, default 60 min,
  by its modified time) AND past its hold. A launcher sets the hold when the run ends: 60 min for a run
  that succeeded, 24 h for one that did not (a repair run re-reads the notebook, and a failure is usually
  looked at the same day or the next morning).
* **The prefixes.** Only items under ``/Users/<you>/jobs/geobrix/`` (``jobs_root``) are ever removed, and
  only these:

  - ``bench``: ``jobs/geobrix/bench/geobrix_bench_runner_*`` and ``geobrix_gen_corpus_*`` (gbx:bench:cluster,
    gbx:bench:gen-data);
  - ``runners``: ``tests_*``, ``runner_*`` and ``mutation_*`` notebooks in the runner folder (``GBX_RUNNER_DIR``,
    default ``jobs/geobrix/runners``; gbx:test:databricks, gbx:test:mutation-databricks), and a failed mutation
    run's Volume stage (``.../mutation-runs/<run>``) once recorded;
  - ``uc``: the UC gate's ``jobs/geobrix/uc-it-<sha>`` folders, whole;
  - ``notebooks``: the notebooks gbx:test:notebooks-serverless imported, by the path it recorded (its
    ``--ws-dir`` folders also hold notebooks of your own, so nothing there is removed by name).

* **The legacy root.** Earlier launchers used ``/Users/<you>/GeoBrix/``, which also holds your own notebooks.
  The sweep applies the same prefixes there during the move, so old runs still go. It never removes an
  item there that is not a runner item.

* **Empty folders.** After a sweep with no error (not the launch-time sweep), every empty folder under the job
  root and under the legacy root is removed, bottom-up, whatever its name. The job root itself stays; the
  legacy root goes once it is empty. A folder that holds anything, or that an active run uses, stays.

* **Active runs.** The active runs' notebook (and Python file) paths are read first. An item an active run
  uses is kept, a folder that holds one is kept, and a recorded import beside one is kept (a ``%run``
  target). When the active runs cannot be read, nothing is deleted.

The record is a local JSON-lines file (``GBX_WS_SWEEP_LEDGER``, default
``~/.cache/geobrix/workspace-sweep.jsonl``), one line per record, keyed by workspace host and path; the
latest line for a path wins. An item with no record is judged by its modified time alone.

Each launcher calls ``sweep_at_launch`` for its own scope (at most ``LAUNCH_MAX_DELETES`` deletes, never
fails the launch; ``GBX_WS_SWEEP_AT_LAUNCH=0`` turns it off), so cleanup happens without a scheduler.

Pure Python: no databricks-sdk import here, so it is testable with a fake client.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import fnmatch
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence

from _job_submit import jobs_root  # sibling module (script dir is on sys.path): the one job root

WINDOW_S = 60 * 60
FAILED_WINDOW_S = 24 * 60 * 60
LAUNCH_MAX_DELETES = 25
SCOPES = ("bench", "runners", "uc", "notebooks")
# A recorded Volume path is removed only inside one of these stage folders.
VOLUME_STAGE_SEGMENTS = ("/mutation-runs/",)


def ledger_file() -> Path:
    return Path(os.environ.get("GBX_WS_SWEEP_LEDGER") or Path.home() / ".cache" / "geobrix" / "workspace-sweep.jsonl")


def ws_path(p: str) -> str:
    """A workspace path as the API lists it: no ``/Workspace`` prefix, one leading slash."""
    p = (p or "").strip()
    if p == "/Workspace" or p.startswith("/Workspace/"):
        p = p[len("/Workspace") :]
    return "/" + p.lstrip("/")


def host_of(w) -> Optional[str]:
    try:
        return (w.config.host or "").rstrip("/") or None
    except Exception:  # noqa: BLE001 - a client with no config (a test fake)
        return None


def root_of(me: str) -> str:
    """The job root: ``/Users/<me>/jobs/geobrix``."""
    return jobs_root(me)


def legacy_root_of(me: str) -> str:
    """The folder the launchers used before the job root moved; swept for runner items only, never whole."""
    return f"/Users/{me}/GeoBrix"


def _under(path: str, root: str) -> bool:
    return path.startswith(root.rstrip("/") + "/")


def parse_age(text: str) -> float:
    """``60m``, ``2h``, ``1d``, ``45s``; a bare number is minutes."""
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([smhd]?)\s*", str(text))
    if not m:
        raise ValueError(f"not an age: {text!r} (use e.g. 60m, 2h, 1d)")
    return float(m.group(1)) * {"s": 1, "m": 60, "": 60, "h": 3600, "d": 86400}[m.group(2)]


def _span(s: float) -> str:
    return f"{s / 3600:g} h" if s >= 7200 and s % 3600 == 0 else f"{s / 60:g} min"


def _age(s: float) -> str:
    return f"{s / 3600:.1f} h" if s >= 7200 else f"{round(s / 60):d} min"


# ---- the record ----------------------------------------------------------------------------------------------------


@contextlib.contextmanager
def _locked():
    f = ledger_file()
    f.parent.mkdir(parents=True, exist_ok=True)
    with open(str(f) + ".lock", "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield f
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def _read(f: Path) -> List[dict]:
    if not f.exists():
        return []
    out = []
    for line in f.read_text().splitlines():
        try:
            out.append(json.loads(line))
        except ValueError:
            continue  # a torn line from a killed writer
    return out


def record(w, paths: Iterable[str], *, scope: str, run_id: Optional[str] = None, keep_s: Optional[float] = None,
           kind: str = "workspace", now: Optional[float] = None) -> None:
    """Record items a launcher made. ``keep_s`` holds them that long from ``now`` (None: no hold beyond the
    window). ``kind`` is ``workspace`` (a notebook, file or folder) or ``volume`` (a Volume folder)."""
    now = time.time() if now is None else now
    host = host_of(w)
    lines = []
    for p in paths:
        path = p.rstrip("/") if kind == "volume" else ws_path(p)
        lines.append(json.dumps({"host": host, "path": path, "scope": scope, "kind": kind, "run_id": run_id,
                                 "recorded_at": now, "keep_until": None if keep_s is None else now + keep_s}))
    if not lines:
        return
    with _locked() as f:
        with open(f, "a") as fh:
            fh.write("\n".join(lines) + "\n")


def release(w, paths: Sequence[str], *, scope: str, succeeded: bool, run_id: Optional[str] = None, kind: str = "workspace",
            now: Optional[float] = None, out: Callable[[str], None] = print) -> None:
    """At the end of a run: keep ``paths`` and hold them for the window (60 min, or 24 h when the run did not
    succeed); the sweep removes them after that. Never raises: a record that cannot be written is said."""
    keep = WINDOW_S if succeeded else FAILED_WINDOW_S
    try:
        record(w, paths, scope=scope, run_id=run_id, keep_s=keep, kind=kind, now=now)
    except Exception as exc:  # noqa: BLE001 - the run's result stands; the sweep still judges by modified time
        print(f"WARNING: could not record {', '.join(paths)} for the sweep ({type(exc).__name__}: {exc}).", file=sys.stderr)
    why = "" if succeeded else " (the run did not succeed)"
    for p in paths:
        out(f"Kept {p} for {_span(keep)}{why}; gbx:workspace:sweep removes it after that.")


def load_ledger(host: Optional[str]) -> Dict[str, dict]:
    """The latest record per path for this workspace host."""
    got: Dict[str, dict] = {}
    for e in _read(ledger_file()):
        if e.get("host") == host and e.get("path"):
            got[e["path"]] = e
    return got


def forget(w, paths: Iterable[str]) -> None:
    """Drop the records of ``paths``: the sweep then never removes them by record (a kept import)."""
    _forget(host_of(w), {ws_path(p) for p in paths})


def _forget(host: Optional[str], paths: set) -> None:
    if not paths:
        return
    with _locked() as f:
        keep = [e for e in _read(f) if not (e.get("host") == host and e.get("path") in paths)]
        tmp = f.with_suffix(".tmp")
        tmp.write_text("".join(json.dumps(e) + "\n" for e in keep))
        tmp.replace(f)


# ---- the sweep -----------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Rule:
    scope: str
    folder: str
    patterns: tuple
    folders: bool = False  # True: the matches are folders, removed whole


_RUNNER_PATTERNS = ("tests_*.ipynb", "runner_*.ipynb", "mutation_*.ipynb")


def rules(me: str, runner_dir: Optional[str] = None) -> List[Rule]:
    """The prefixes of the job root, then the same runner prefixes under the legacy root. ``runner_dir`` replaces
    the job root's default runner folder; the legacy default runner folder is always swept."""
    root, legacy = root_of(me), legacy_root_of(me)
    out = [
        Rule("bench", f"{root}/bench", ("geobrix_bench_runner_*", "geobrix_gen_corpus_*")),
        Rule("runners", ws_path(runner_dir) if runner_dir else f"{root}/runners", _RUNNER_PATTERNS),
        Rule("uc", root, ("uc-it-*",), folders=True),
    ]
    out += [
        Rule("bench", f"{legacy}/bench", ("geobrix_bench_runner_*", "geobrix_gen_corpus_*")),
        Rule("runners", f"{legacy}/runners", _RUNNER_PATTERNS),
        Rule("uc", legacy, ("uc-it-*",), folders=True),
    ]
    return out


@dataclass
class Item:
    path: str
    scope: str
    kind: str  # "item", "folder" or "volume"
    mtime: Optional[float] = None  # seconds
    keep_until: Optional[float] = None
    run_id: Optional[str] = None
    recorded: bool = False


@dataclass
class Report:
    deleted: List[str] = field(default_factory=list)
    pruned: List[str] = field(default_factory=list)  # empty legacy folders removed (not counted as deletes)
    kept: List[str] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    error: Optional[str] = None


def _otype(o) -> str:
    t = getattr(o, "object_type", None)
    return str(getattr(t, "value", t) or "")


def _missing(exc: Exception) -> bool:
    s = f"{type(exc).__name__} {exc}".lower()
    return "notfound" in s.replace("_", "") or "does not exist" in s or "doesn't exist" in s or "resourcedoesnotexist" in s.replace("_", "")


def _list(w, folder: str) -> list:
    try:
        return list(w.workspace.list(folder))
    except Exception as exc:  # noqa: BLE001
        if _missing(exc):
            return []
        raise


def active_paths(w) -> set:
    """The notebook and Python-file paths of every active run (raises when they cannot be read)."""
    out = set()
    for run in w.jobs.list_runs(active_only=True, expand_tasks=True):
        for t in getattr(run, "tasks", None) or []:
            for attr, key in (("notebook_task", "notebook_path"), ("spark_python_task", "python_file")):
                p = getattr(getattr(t, attr, None), key, None)
                if p:
                    out.add(ws_path(p))
    return out


def _prune_empty(w, folder: str, keep: set, protected: set, gone: set, rep: Report, dry_run: bool,
                 out: Callable[[str], None]) -> bool:
    """Remove ``folder`` when nothing is left in it, children first; True when it is (or would be) gone.

    ``gone`` holds the paths this sweep deleted (a dry run did not remove them, but they count as gone).
    A folder in ``keep`` is never removed (the job root) and neither is one in ``protected`` (an active
    run uses it). Any other object, or a folder that cannot go, keeps its parents; every child is still
    judged, so one item does not stop the empty folders beside it going."""
    try:
        kids = list(w.workspace.list(folder))
    except Exception as exc:  # noqa: BLE001
        return _missing(exc)  # already gone counts as empty; anything else keeps the folder
    left = False
    for k in kids:
        p = ws_path(k.path)
        if p in gone:
            continue
        if _otype(k) != "DIRECTORY" or not _prune_empty(w, p, keep, protected, gone, rep, dry_run, out):
            left = True
    if left or folder in keep or folder in protected:
        return False
    if dry_run:
        rep.pruned.append(folder)
        out(f"WOULD DELETE {folder} (empty folder)")
        return True
    try:
        w.workspace.delete(folder, recursive=False)
    except Exception as exc:  # noqa: BLE001 - not empty after all (a run raced us), or no right: keep it
        return _missing(exc)
    rep.pruned.append(folder)
    out(f"DELETED {folder} (empty folder)")
    return True


def _rm_volume(w, path: str) -> None:
    for e in list(w.files.list_directory_contents(path)):
        if e.is_directory:
            _rm_volume(w, e.path)
        else:
            w.files.delete(e.path)
    w.files.delete_directory(path)


def sweep(w, *, me: str, older_than_s: float = WINDOW_S, scopes: Sequence[str] = SCOPES, runner_dir: Optional[str] = None,
          dry_run: bool = False, max_deletes: Optional[int] = None, now: Optional[float] = None,
          out: Callable[[str], None] = print, verbose: bool = True, prune_empty: bool = True) -> Report:
    """Remove the GeoBrix-runner items of ``scopes`` that are older than ``older_than_s`` and past their hold.

    Prints one line per item it judged (``verbose``; else only the deletions): ``DELETED`` (``WOULD DELETE``
    on a dry run), ``KEPT <path>: <why>`` or ``SKIPPED <path>: outside the sweep's prefixes``."""
    now = time.time() if now is None else now
    rep = Report()
    host = host_of(w)
    root = root_of(me)
    legacy = legacy_root_of(me)
    roots = (root, legacy)
    say = out if verbose else (lambda s: None)
    try:
        active = active_paths(w)
    except Exception as exc:  # noqa: BLE001 - never delete what a run might be using
        rep.error = f"the active runs could not be read ({type(exc).__name__}: {str(exc)[:200]})"
        out(f"sweep: {rep.error}; nothing deleted.")
        return rep
    ledger = load_ledger(host)
    items: Dict[str, Item] = {}
    for rule in rules(me, runner_dir):
        if rule.scope not in scopes:
            continue
        if not any(rule.folder == r or _under(rule.folder, r) for r in roots):
            rep.skipped.append(rule.folder)
            say(f"SKIPPED {rule.folder}/: outside the sweep's prefixes ({root}/, legacy {legacy}/)")
            continue
        for o in _list(w, rule.folder):
            name = o.path.rstrip("/").rsplit("/", 1)[-1]
            is_dir = _otype(o) == "DIRECTORY"
            if is_dir != rule.folders or not any(fnmatch.fnmatchcase(name, pat) for pat in rule.patterns):
                continue
            if rule.folders:
                times = [c.modified_at for c in _list(w, o.path) if getattr(c, "modified_at", None)]
                mtime = max(times) / 1000 if times else None
            else:
                mtime = o.modified_at / 1000 if getattr(o, "modified_at", None) else None
            items[ws_path(o.path)] = Item(ws_path(o.path), rule.scope, "folder" if rule.folders else "item", mtime)
    gone = set()
    for path, e in ledger.items():
        if e.get("scope") not in scopes:
            continue
        kind = "volume" if e.get("kind") == "volume" else "item"
        it = items.get(path)
        if it is None:
            it = Item(path, e["scope"], kind, run_id=e.get("run_id"))
            if kind == "item":
                if not any(_under(path, r) for r in roots):
                    rep.skipped.append(path)
                    say(f"SKIPPED {path}: outside the sweep's prefixes ({root}/, legacy {legacy}/)")
                    continue
                try:
                    st = w.workspace.get_status(path)
                except Exception as exc:  # noqa: BLE001
                    if _missing(exc):
                        gone.add(path)
                        continue
                    raise
                it.kind = "folder" if _otype(st) == "DIRECTORY" else "item"
                it.mtime = st.modified_at / 1000 if getattr(st, "modified_at", None) else None
            items[path] = it
        it.recorded = True
        it.keep_until = e.get("keep_until")
        it.run_id = e.get("run_id")
    # a folder follows the holds of the records inside it
    for it in items.values():
        if it.kind == "folder":
            inner = [e.get("keep_until") or 0 for p, e in ledger.items() if _under(p, it.path)]
            if inner:
                it.keep_until = max([it.keep_until or 0] + inner) or None
    active_dirs = {p.rsplit("/", 1)[0] for p in active}
    active_runs = {e.get("run_id") for p, e in ledger.items() if p in active and e.get("run_id")}
    removed: List[str] = []
    order = sorted(items.values(), key=lambda i: (i.kind != "folder", i.path))
    for it in order:
        if any(_under(it.path, d) for d in removed):
            gone.add(it.path)
            continue
        why = None
        if it.kind == "volume":
            if not (it.path.startswith("/Volumes/") and any(seg in it.path + "/" for seg in VOLUME_STAGE_SEGMENTS)):
                rep.skipped.append(it.path)
                say(f"SKIPPED {it.path}: outside the sweep's prefixes (a Volume stage folder: {', '.join(VOLUME_STAGE_SEGMENTS)})")
                continue
            if it.run_id and it.run_id in active_runs:
                why = "its run is active"
            elif not it.keep_until:
                why = "no hold recorded"
        elif it.path in active or any(_under(a, it.path) for a in active):
            why = "used by an active run"
        elif it.scope == "notebooks" and it.path.rsplit("/", 1)[0] in active_dirs:
            why = "beside an active run's notebook"
        if why is None and it.keep_until and it.keep_until > now:
            why = f"held until {time.strftime('%Y-%m-%d %H:%M', time.localtime(it.keep_until))}"
        if why is None and it.kind != "volume":
            if it.mtime is None:
                if not it.keep_until:
                    why = "age unknown"
            elif now - it.mtime < older_than_s:
                why = f"younger than {_span(older_than_s)}"
        if why:
            rep.kept.append(it.path)
            say(f"KEPT {it.path}: {why}")
            continue
        if max_deletes is not None and len(rep.deleted) >= max_deletes:
            rep.kept.append(it.path)
            say(f"KEPT {it.path}: this sweep's cap of {max_deletes} deletes is reached")
            continue
        age = f"{_age(now - it.mtime)} old" if it.mtime else "hold ended"
        if dry_run:
            rep.deleted.append(it.path)
            out(f"WOULD DELETE {it.path} ({age})")
            continue
        try:
            if it.kind == "volume":
                _rm_volume(w, it.path)
            else:
                w.workspace.delete(it.path, recursive=it.kind == "folder")
        except Exception as exc:  # noqa: BLE001 - say it and go on
            if _missing(exc):
                gone.add(it.path)
                continue
            rep.errors.append(it.path)
            out(f"ERROR {it.path}: {type(exc).__name__}: {str(exc)[:200]}")
            continue
        rep.deleted.append(it.path)
        removed.append(it.path)
        out(f"DELETED {it.path} ({age})")
    if not dry_run:
        done = set(rep.deleted) | gone
        done |= {p for p in ledger if any(_under(p, d) for d in removed)}
        _forget(host, done)
    # Empty folders: after a sweep with no error, every empty folder under the job root (not the root itself) and
    # under the legacy root (the root too, once it is empty), bottom-up. A folder an active run uses stays.
    if prune_empty and not rep.errors:
        gone_now = set(rep.deleted) | gone
        protected = set()
        for a_path in active:  # the folder of an active run's notebook and every folder above it
            d = a_path
            while "/" in d.strip("/"):
                d = d.rsplit("/", 1)[0]
                protected.add(d)
        _prune_empty(w, root, {root}, protected, gone_now, rep, dry_run, out)
        _prune_empty(w, legacy, set(), protected, gone_now, rep, dry_run, out)
    return rep


def sweep_at_launch(w, scopes: Sequence[str], *, me: Optional[str] = None, runner_dir: Optional[str] = None,
                    now: Optional[float] = None) -> Optional[Report]:
    """The bounded sweep a launcher runs for its own scope before it uploads anything. Never fails the launch."""
    if os.environ.get("GBX_WS_SWEEP_AT_LAUNCH", "1").strip() == "0":
        return None
    try:
        me = me or w.current_user.me().user_name
        # no folder pruning at launch: another launch may have made its folder and not uploaded into it yet
        rep = sweep(w, me=me, scopes=scopes, runner_dir=runner_dir, max_deletes=LAUNCH_MAX_DELETES, now=now, verbose=False,
                    prune_empty=False)
        if rep.deleted:
            print(f"Swept {len(rep.deleted)} old run item(s) ({', '.join(scopes)}; older than {_span(WINDOW_S)} and past their hold).")
        return rep
    except Exception as exc:  # noqa: BLE001 - cleanup must never stop a launch
        print(f"WARNING: the launch-time workspace sweep did not run ({type(exc).__name__}: {str(exc)[:200]}); the launch goes on.",
              file=sys.stderr)
        return None


# ---- the command ---------------------------------------------------------------------------------------------------


def _load_config() -> None:
    """GBX_RUNNER_DIR and friends from the untracked cluster config, as the launchers read it."""
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from _cluster_config import cluster_config_file

    f = cluster_config_file(Path(__file__).resolve().parent)
    if f.exists():
        for line in f.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                if k.strip() and v.strip() and not os.environ.get(k.strip()):
                    os.environ[k.strip()] = v.strip()


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(prog="gbx:workspace:sweep", description="Remove old GeoBrix runner notebooks and folders.")
    p.add_argument("--profile", required=True)
    p.add_argument("--older-than", default="60m")
    p.add_argument("--scope", action="append", help=f"one of {', '.join(SCOPES)}; repeat or comma-separate (default all)")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args(argv)
    try:
        older = parse_age(a.older_than)
    except ValueError as exc:
        print(f"ERROR: --older-than: {exc}", file=sys.stderr)
        return 2
    scopes = tuple(s.strip() for v in (a.scope or [",".join(SCOPES)]) for s in v.split(",") if s.strip())
    bad = [s for s in scopes if s not in SCOPES]
    if bad:
        print(f"ERROR: unknown --scope {', '.join(bad)} (use {', '.join(SCOPES)})", file=sys.stderr)
        return 2
    _load_config()
    from databricks.sdk import WorkspaceClient
    from _job_submit import runner_dir

    w = WorkspaceClient(profile=a.profile)
    me = w.current_user.me().user_name
    rd = runner_dir(w)
    print(f"Sweeping {', '.join(scopes)} under {root_of(me)}/ and the legacy {legacy_root_of(me)}/ (runner folder {ws_path(rd)}): older than {_span(older)} and "
          f"past their hold{'; DRY RUN, nothing is deleted' if a.dry_run else ''}.", flush=True)
    rep = sweep(w, me=me, older_than_s=older, scopes=scopes, runner_dir=rd, dry_run=a.dry_run)
    verb = "would delete" if a.dry_run else "deleted"
    print(f"Summary: {verb} {len(rep.deleted)}, kept {len(rep.kept)}, skipped {len(rep.skipped)}, errors {len(rep.errors)}"
          + (f"; {rep.error}" if rep.error else ""))
    return 1 if rep.error or rep.errors else 0


if __name__ == "__main__":
    sys.exit(main())
