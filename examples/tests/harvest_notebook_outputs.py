"""Write ONE run's cell outputs back into the local source .ipynb, with the wheel and runtime that produced them.

``databricks jobs export-run <RUN_ID> --views-to-export CODE`` returns the executed notebook as HTML; it embeds
``__DATABRICKS_NOTEBOOK_MODEL = '<base64>'``, which decodes (base64, then URL-unquote) to the notebook model JSON.
Each command carries its source (``command``) and its ``results``: a ``listResults`` of items (``ansi`` text,
``mimeBundle`` display/execute results, ``table`` for ``display(df)``, ``html``/``htmlSandbox``), or one such item
on its own, or ``raw`` (the exit value of a ``dbutils.notebook.exit`` cell: no output). That export is the single
source: nothing of an older run survives a harvest.

The rules, for every code cell the harvest covers (the whole notebook, or the piece ``cells`` of a ``--cells`` run):

- a cell matched to an executed command (by normalized SOURCE, not index) gets exactly that command's outputs,
  converted to nbformat; ``execution_count`` 0, as is the repo convention for executed cells;
- a cell with no executed command (edited since the launch, or after an exit) gets no outputs and
  ``execution_count`` None;
- the ``%pip`` / ``%restart_python`` cells the Serverless runner strips get no outputs, except the geobrix install
  cell, which gets ONE line: the GeoBrix version, build and wheel sha256 that ran, and the runtime;
- a ``%run ./config_nb`` cell gets what the dependency printed. The export holds the dependency's run as
  sub-commands of the ``%run`` command (empty ``command`` source, ``parentHierarchy`` naming the ``%run``
  command's ``guid``), not as its cells, so those outputs cannot be matched to the dependency's own cells: they go into
  the parent's ``%run`` cell, which is where Databricks shows them. ``config_nb.ipynb`` itself keeps no outputs (it is
  never run on its own);
- a result this tool cannot convert fails the harvest, naming the cells, and NOTHING is written: a notebook never
  mixes two runs.

The runner's own cells are never matched: the wheel guard (``# Wheel guard:``), the ``--set-var`` override cell and
a piece's carried setup statements.
The guard's ``WHEEL GUARD provenance: {json}`` line is the provenance; it lands in ``metadata.geobrix_run`` (version,
git_sha, wheel, wheel_sha256, runtime) and in the install cell's line. Each piece (run id and cell range) records its own
wheel and runtime, so pieces run on different wheels all keep theirs: ``wheels`` lists the distinct wheel sha256s,
``mixed_wheels`` is true when there are several, and the install line then names each build.
The file's JSON serialization is preserved.
"""

from __future__ import annotations

import base64
import datetime
import html as _html
import json
import pathlib
import re
import subprocess
import sys
import time
import urllib.parse
from dataclasses import dataclass, field
from typing import Optional

_MODEL_RE = re.compile(r"__DATABRICKS_NOTEBOOK_MODEL\s*=\s*'([^']+)'")
GUARD_PREFIX = "# Wheel guard:"
OVERRIDE_PREFIX = "# --- validation override"
CARRIED_PREFIX = "# --- piece setup carried from skipped cells"  # _cell_range.CARRIED_PREFIX
PROVENANCE_TAG = "WHEEL GUARD provenance: "
META_KEY = "geobrix_run"

# a forced install the classic runner puts before each geobrix %pip line (see _wheel_guard.force_line)
_FORCED_PIP = re.compile(r'^%pip install (--quiet )?--force-reinstall --no-deps "geobrix @ file://[^"]+"\s*$')
_PIP_WHEEL = re.compile(r"@\s*file://(/[^\"'\s]+\.whl)")
_GEOBRIX_PIP = re.compile(r"^\s*%pip\s+install\b.*geobrix.*@\s*file://", re.M)
_SETUP_MAGIC = ("%pip", "%restart_python")


class HarvestError(RuntimeError):
    """The run's outputs cannot be written without losing or mixing outputs; nothing was written."""


class UnconvertibleResult(ValueError):
    """A result item of a type this tool does not convert."""


@dataclass
class HarvestReport:
    updated: int = 0
    not_run: list = field(default_factory=list)  # 0-based indices of covered code cells with no executed command
    install_cell: Optional[int] = None
    leftover: list = field(default_factory=list)  # executed commands no cell took (markdown, edited cells)
    provenance: Optional[dict] = None


def decode_model(html: str) -> dict:
    """Decode the notebook model embedded in an exported run's HTML."""
    m = _MODEL_RE.search(html)
    if not m:
        raise ValueError("__DATABRICKS_NOTEBOOK_MODEL not found in exported HTML")
    raw = base64.b64decode(m.group(1)).decode("utf-8")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(urllib.parse.unquote(raw))


def _src(src) -> str:
    return "".join(src) if isinstance(src, list) else (src or "")


def _norm(src) -> str:
    """The matching key of a cell or command: trailing spaces off, the runner's forced install lines out, and every
    ``@ file://<wheel>`` the same (``--pip-wheel`` repoints them in the uploaded copy)."""
    lines = [line.rstrip() for line in _src(src).strip().splitlines()]
    lines = [_PIP_WHEEL.sub("@ file://<wheel>", line) for line in lines if not _FORCED_PIP.match(line.strip())]
    return "\n".join(lines).strip()


def _lines(text: str) -> list[str]:
    return text.splitlines(keepends=True)


# ---------------------------------------------------------------------------------------------- conversion

# the table markup Databricks' own .ipynb export gives a display(df) result (the committed notebooks carry it)
_TABLE_STYLE = (
    "<style scoped>\n  .table-result-container {\n    max-height: 300px;\n    overflow: auto;\n  }\n"
    "  table, th, td {\n    border: 1px solid black;\n    border-collapse: collapse;\n  }\n"
    "  th, td {\n    padding: 5px;\n  }\n  th {\n    text-align: left;\n  }\n</style>"
)


def _cell_text(v) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


def _table_output(item: dict) -> dict:
    cols = [c.get("name", "") if isinstance(c, dict) else str(c) for c in item.get("schema") or []]
    rows = item.get("data") or []
    head = "".join("<th>%s</th>" % _html.escape(c) for c in cols)
    body = "".join("<tr>%s</tr>" % "".join("<td>%s</td>" % _html.escape(_cell_text(v)) for v in row) for row in rows)
    markup = (_TABLE_STYLE + "<div class='table-result-container'><table class='table-result'>"
              "<thead style='background-color: white'><tr>%s</tr></thead><tbody>%s</tbody></table></div>" % (head, body))
    plain = "\t".join(cols) + "\n" + "".join("\t".join(_cell_text(v) for v in row) + "\n" for row in rows)
    if item.get("truncated"):
        plain += "(truncated)\n"
    return {"data": {"text/html": _lines(markup), "text/plain": _lines(plain)}, "metadata": {},
            "output_type": "display_data"}


def _mime_output(item: dict) -> dict:
    # nbformat stores text/* as a list of lines; images and other types (the h3 hint's empty string) as they are
    data = {k: (_lines(v) if k.startswith("text/") and isinstance(v, str) else v) for k, v in item["data"].items()}
    if item.get("executionCount") is not None:
        return {"data": data, "execution_count": 0, "metadata": {}, "output_type": "execute_result"}
    return {"data": data, "metadata": {}, "output_type": "display_data"}


def _item_outputs(item: dict) -> list[dict]:
    kind = item.get("type")
    data = item.get("data")
    if kind == "ansi":
        return [{"name": item.get("name") or "stdout", "output_type": "stream", "text": _lines(data)}] if data else []
    if kind == "mimeBundle":
        return [_mime_output(item)] if data else []
    if kind == "table":
        return [_table_output(item)]
    if kind in ("html", "htmlSandbox"):
        return [{"data": {"text/html": _lines(data)}, "metadata": {}, "output_type": "display_data"}] if data else []
    if kind in ("text", "raw"):  # a text item inside a list is printed text; a raw top-level result is an exit value
        return [{"name": "stdout", "output_type": "stream", "text": _lines(data)}] if isinstance(data, str) and data else []
    raise UnconvertibleResult(f"result type {kind!r}")


def to_outputs(results: Optional[dict]) -> list[dict]:
    """A command's ``results`` as nbformat v4 outputs. UnconvertibleResult on an item type this tool does not know."""
    if not isinstance(results, dict):
        return []
    kind = results.get("type")
    if kind == "raw":  # the exit value of a dbutils.notebook.exit cell: no output of its own
        return []
    if kind == "listResults" or (kind is None and isinstance(results.get("data"), list)):
        out = []
        for item in results.get("data") or []:
            out += _item_outputs(item)
        return out
    if kind == "error":
        return [_error_output(results.get("summary") or "", results.get("data") or results.get("cause") or "")]
    return _item_outputs(results)


_ANSI_ESC = re.compile(r"\x1b\[[0-9;]*m")
_TAGS = re.compile(r"<[^>]+>")


def _error_output(summary: str, detail) -> dict:
    text = _TAGS.sub("", _ANSI_ESC.sub("", _src(summary))).strip()
    ename, _, evalue = text.partition(": ")
    tb = _src(detail) if not isinstance(detail, (dict, list)) else json.dumps(detail)
    return {"ename": ename or "Error", "evalue": evalue or text, "output_type": "error",
            "traceback": _html.unescape(_TAGS.sub("", tb)).splitlines() or [text]}


def command_outputs(cmd: dict) -> list[dict]:
    """The outputs of one executed command: its results, then its error (a failed cell) when it has one."""
    out = to_outputs(cmd.get("results"))
    if cmd.get("error") or cmd.get("errorSummary"):
        out.append(_error_output(cmd.get("errorSummary") or cmd.get("error") or "", cmd.get("error") or ""))
    return out


def _n_items(model: dict) -> int:
    n = 0
    for c in model.get("commands", []):
        r = c.get("results")
        if isinstance(r, dict):
            n += len(r["data"]) if isinstance(r.get("data"), list) else 1
    return n


# ---------------------------------------------------------------------------------------------- provenance


def _is_runner_cell(src: str) -> bool:
    return src.lstrip().startswith((GUARD_PREFIX, OVERRIDE_PREFIX, CARRIED_PREFIX))


def provenance_from_model(model: dict) -> Optional[dict]:
    """The ``WHEEL GUARD provenance`` record the guard cell printed, or None (no guard ran)."""
    for cmd in model.get("commands", []):
        if not _src(cmd.get("command")).lstrip().startswith(GUARD_PREFIX):
            continue
        texts = []
        for out in to_outputs(cmd.get("results")):
            texts.append(_src(out.get("text")) if out.get("output_type") == "stream" else _src(out.get("data", {}).get("text/plain")))
        for line in "".join(texts).splitlines():
            if line.startswith(PROVENANCE_TAG):
                try:
                    return json.loads(line[len(PROVENANCE_TAG):])
                except json.JSONDecodeError:
                    return None
    return None


def provenance_line(prov: Optional[dict], run_context: Optional[dict]) -> Optional[str]:
    """The one line the install cell shows: what ran, on what."""
    if not prov:
        return None
    where = (run_context or {}).get("label") or prov.get("runtime") or "Databricks"
    parts = []
    if prov.get("git_sha"):
        parts.append("build %s" % prov["git_sha"][:12])
    if prov.get("wheel_sha256"):
        parts.append("wheel sha256 %s" % prov["wheel_sha256"][:12])
    detail = " (%s)" % ", ".join(parts) if parts else ""
    return "GeoBrix %s installed%s on %s\n" % (prov.get("version") or "?", detail, where)


# what a piece records of the wheel and runtime that ran it (beside its run id and cell range)
_PIECE_FIELDS = ("version", "git_sha", "dirty", "wheel", "wheel_sha256", "runtime", "runtime_version",
                 "environment_version", "spark_version", "harvested", "guard")


def _wheel_id(piece: dict):
    return piece.get("wheel_sha256") or piece.get("git_sha")


def _cell_start(piece: dict) -> int:
    return int(str(piece["cells"]).split(":")[0])


def _inside(piece: dict, rng: range) -> bool:
    """True when every cell of ``piece`` (its 1-based ``A:B``) is in ``rng`` (0-based, stop exclusive)."""
    start, _, stop = str(piece["cells"]).partition(":")
    return rng.start + 1 <= int(start) and int(stop or start) <= rng.stop


def _cell_set(piece: dict) -> set:
    """The 1-based cell numbers of ``piece`` (its ``A:B``)."""
    start, _, stop = str(piece["cells"]).partition(":")
    return set(range(int(start), int(stop or start) + 1))


def _drop_superseded(runs: list[dict], entry: dict) -> list[dict]:
    """Drop each kept entry whose cells were all re-run by later harvests: the new ``entry`` together with the entries
    harvested after it. No cell holds its outputs any more, so naming it would claim a run (and a wheel) the notebook no
    longer shows. An entry harvested in the same second as another is kept (the order between them is unknown)."""
    kept = []
    for r in runs:
        newer = [o for o in runs if o is not r and (o.get("harvested") or "") > (r.get("harvested") or "")]
        covered = set().union(_cell_set(entry), *(_cell_set(o) for o in newer))
        if not _cell_set(r) <= covered:
            kept.append(r)
    return kept


def _upgrade_runs(old: dict) -> list[dict]:
    """The piece entries of a recorded block. The older shape named one wheel at the top and only the run id and cells
    per piece: each piece takes that wheel (and runtime) as its own."""
    pieces = []
    for r in old.get("runs") or []:
        if not r.get("cells"):
            continue
        piece = dict(r)
        for k in _PIECE_FIELDS:
            if k not in piece and old.get(k) is not None:
                piece[k] = old[k]
        pieces.append(piece)
    return pieces


def mixed_provenance_line(pieces: list[dict], run_context: Optional[dict] = None) -> str:
    """The install cell's line when the pieces ran on more than one wheel: each build, and the cells it ran."""
    builds: dict = {}
    for p in sorted(pieces, key=_cell_start):
        builds.setdefault(_wheel_id(p), []).append(p)
    parts = []
    for ps in builds.values():
        first = ps[0]
        who = "%s build %s" % (first.get("version") or "?", (first.get("git_sha") or first.get("wheel_sha256") or "?")[:12])
        parts.append("%s (cells %s)" % (who, ", ".join(str(p["cells"]) for p in ps)))
    runtimes = {p.get("runtime") for p in pieces}
    where = (run_context or {}).get("label") or (runtimes.pop() if len(runtimes) == 1 else None) or "Databricks"
    return "GeoBrix installed from %d wheels: %s, on %s\n" % (len(builds), "; ".join(parts), where)


def stamp_provenance(nb: dict, prov: Optional[dict], run_id, cells: Optional[range], run_context: Optional[dict],
                     n_cells: int) -> None:
    """Record the run in ``metadata.geobrix_run``: one entry per piece, each with the run id, the cell range and the wheel
    and runtime that ran it, and a top-level summary (``wheels``, the distinct wheel sha256s, and ``mixed_wheels`` when
    there is more than one).

    A piece replaces the entry of the same cells (or run id) and every entry inside its range; a whole-notebook run
    replaces them all. With one wheel
    the top level also names it (version, git_sha, wheel, wheel_sha256, runtime); with several it does not, as no one
    wheel is the notebook's."""
    meta = nb.setdefault("metadata", {})
    prov = prov or {}
    ctx = run_context or {}
    rng = cells if cells is not None else range(0, n_cells)
    harvested = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    entry = {
        "run_id": run_id,
        "cells": f"{rng.start + 1}:{rng.stop}",
        "version": prov.get("version"),
        "git_sha": prov.get("git_sha"),
        "dirty": prov.get("dirty"),
        "wheel": prov.get("wheel") or ctx.get("wheel"),
        "wheel_sha256": prov.get("wheel_sha256"),
        "runtime": ctx.get("label") or prov.get("runtime"),
        "runtime_version": prov.get("runtime"),
        "harvested": harvested,
    }
    if ctx.get("environment_version"):
        entry["environment_version"] = str(ctx["environment_version"])
    if ctx.get("spark_version"):
        entry["spark_version"] = ctx["spark_version"]
    if not prov:
        entry["guard"] = "off: the wheel was not checked on the compute"
    entry = {k: v for k, v in entry.items() if v is not None}
    runs = _upgrade_runs(meta.get(META_KEY) or {}) if cells is not None else []
    # Drop an entry whose cells all lie inside this piece (the same range included): those cells now hold this run's
    # outputs, so a kept entry would name a wheel they no longer ran on. A partial overlap still covers other cells.
    runs = [r for r in runs if not _inside(r, rng) and (run_id is None or r.get("run_id") != run_id)]
    # An entry no single piece contains can still be covered by this piece and later ones together (a whole-notebook run
    # re-run as two pieces): it no longer owns any cell either.
    runs = _drop_superseded(runs, entry) + [entry]
    runs.sort(key=_cell_start)
    wheels = list(dict.fromkeys(r["wheel_sha256"] for r in runs if r.get("wheel_sha256")))
    mixed = len({_wheel_id(r) for r in runs if _wheel_id(r)}) > 1
    record = {"harvested": harvested, "wheels": wheels or None, "mixed_wheels": True if mixed else None}
    if not mixed:
        record.update({k: entry.get(k) for k in _PIECE_FIELDS if k != "harvested"})
    record["runs"] = runs
    meta[META_KEY] = {k: v for k, v in record.items() if v is not None}


def refresh_install_line(nb: dict, run_context: Optional[dict] = None) -> Optional[int]:
    """Make the install cell's provenance line agree with ``metadata.geobrix_run``: the one-wheel line, or, when the pieces
    ran on several wheels, the line naming each build. It also corrects a piece that was not rerun (its cell is outside the
    harvested piece), which would otherwise keep a line from an earlier mix. Returns the cell's index, or None."""
    rec = (nb.get("metadata") or {}).get(META_KEY) or {}
    runs = rec.get("runs") or []
    if not runs:
        return None
    for i, cell in enumerate(nb.get("cells", [])):
        outs = cell.get("outputs") or []
        if cell.get("cell_type") != "code" or not outs or not _GEOBRIX_PIP.search(_src(cell.get("source"))):
            continue
        last = outs[-1]
        text = _src(last.get("text"))
        if last.get("output_type") != "stream" or not text.startswith("GeoBrix ") or " installed" not in text:
            continue
        if rec.get("mixed_wheels"):
            line = mixed_provenance_line(runs, run_context)
        else:
            known = [r for r in runs if _wheel_id(r)]  # a piece run without the guard names no wheel
            line = provenance_line(known[-1], run_context) if known else None
        if line:
            last["text"] = [line]
        return i
    return None


# ---------------------------------------------------------------------------------------------- applying one run


def _is_setup(src: str) -> bool:
    return any(line.lstrip().startswith(_SETUP_MAGIC) for line in src.splitlines()) or "restartPython()" in src


def apply_outputs(nb: dict, model: dict, *, cells: Optional[range] = None, provenance: Optional[dict] = None,
                  run_context: Optional[dict] = None) -> HarvestReport:
    """Set, on every code cell of ``nb`` in ``cells`` (all when None), exactly this run's outputs (see the module doc).

    Raises HarvestError, before changing ``nb``, when a matched command has a result that cannot be converted."""
    queues: dict[str, list] = {}
    children: dict[str, list] = {}  # a %run command's guid -> the sub-commands of the notebook it ran
    for cmd in sorted(model.get("commands", []), key=lambda c: c.get("position", 0)):
        src = _src(cmd.get("command"))
        if cmd.get("parentHierarchy"):
            for parent in cmd["parentHierarchy"]:
                children.setdefault(parent, []).append(cmd)
            continue
        key = _norm(src)
        if key and not _is_runner_cell(src):
            queues.setdefault(key, []).append(cmd)
    rep = HarvestReport(provenance=provenance)
    plan, bad = [], []
    line = provenance_line(provenance, run_context)
    for i, cell in enumerate(nb.get("cells", [])):
        if cell.get("cell_type") != "code" or (cells is not None and i not in cells):
            continue
        src = _src(cell.get("source"))
        key = _norm(src)
        cmd = queues.get(key, []).pop(0) if queues.get(key) else None
        if cmd is None and _is_setup(src):  # stripped for Serverless (its deps are the environment spec)
            outs = []
            if line and rep.install_cell is None and _GEOBRIX_PIP.search(src):
                rep.install_cell = i
                outs = [{"name": "stdout", "output_type": "stream", "text": [line]}]
            plan.append((i, outs, 0 if outs else None, bool(outs)))
            continue
        if cmd is None:
            if key:
                rep.not_run.append(i)
            plan.append((i, [], None, False))
            continue
        try:
            outs = command_outputs(cmd)
            for child in children.get(cmd.get("guid") or "", []):  # what the %run notebook printed
                outs += command_outputs(child)
        except UnconvertibleResult as exc:
            bad.append(f"cell {i + 1} ({exc})")
            continue
        if line and rep.install_cell is None and _GEOBRIX_PIP.search(src):
            rep.install_cell = i
            outs = outs + [{"name": "stdout", "output_type": "stream", "text": [line]}]
        plan.append((i, outs, 0, True))
    if bad:
        raise HarvestError("cannot convert the outputs of " + "; ".join(bad) +
                           ". Nothing was written. Fix: teach harvest_notebook_outputs.to_outputs the result type")
    for i, outs, count, ran in plan:
        cell = nb["cells"][i]
        cell["outputs"] = outs
        cell["execution_count"] = count
        for o in outs:
            if o.get("output_type") == "execute_result":
                o["execution_count"] = count
        rep.updated += ran
    rep.leftover = [c for q in queues.values() for c in q]
    rep.leftover.sort(key=lambda c: c.get("position", 0))
    return rep


def _dump_like(nb: dict, original: str) -> str:
    """Serialize ``nb`` the way ``original`` was serialized (indent=1 Jupyter style)."""
    for ensure_ascii in (False, True):
        probe = json.dumps(json.loads(original), indent=1, ensure_ascii=ensure_ascii)
        for tail in ("", "\n"):
            if probe + tail == original:
                return json.dumps(nb, indent=1, ensure_ascii=ensure_ascii) + tail
    # An original that json does not reproduce byte for byte (a repeated key, a key order or an escape mix of another
    # writer): the notebook is still valid JSON, so write it in the repo's style rather than lose the run's outputs.
    print(
        "    note: the notebook's JSON serialization is not one json.dumps reproduces; "
        "rewritten in the repo style (indent 1, UTF-8), so the diff may touch more than the outputs",
        file=sys.stderr,
        flush=True,
    )
    return json.dumps(nb, indent=1, ensure_ascii=False) + "\n"


def _export_model(run_id, profile: str) -> dict:
    proc = subprocess.run(
        ["databricks", "jobs", "export-run", str(run_id), "--views-to-export", "CODE",
         "--profile", profile, "-o", "json"],
        capture_output=True, text=True, check=True)
    views = [v for v in json.loads(proc.stdout).get("views", []) if v.get("type") == "NOTEBOOK"]
    if not views:
        raise RuntimeError(f"export-run {run_id} returned no NOTEBOOK view")
    return decode_model(views[0]["content"])


def harvest_run_outputs(run_id, profile: str, local_nb_path, settle_s: int = 20, *, cells: Optional[range] = None,
                        run_context: Optional[dict] = None, model: Optional[dict] = None) -> HarvestReport:
    """Export ``run_id`` and write its outputs into ``local_nb_path`` in place, with the provenance in the metadata.
    HarvestError leaves the file as it was.

    An export taken right as the run terminates can lack large results (a multi-MB plot was observed missing), so it
    is re-exported until the result count stops growing. ``model`` skips the export (a saved one)."""
    path = pathlib.Path(local_nb_path)
    if model is None:
        model = _export_model(run_id, profile)
        for _ in range(4):
            time.sleep(settle_s)
            again = _export_model(run_id, profile)
            if _n_items(again) <= _n_items(model):
                break
            model = again
    prov = provenance_from_model(model)
    original = path.read_text(encoding="utf-8")
    nb = json.loads(original)
    rep = apply_outputs(nb, model, cells=cells, provenance=prov, run_context=run_context)
    stamp_provenance(nb, prov, run_id, cells, run_context, len(nb.get("cells", [])))
    refresh_install_line(nb, run_context)
    path.write_text(_dump_like(nb, original), encoding="utf-8")
    return rep
