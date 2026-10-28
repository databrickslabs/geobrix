"""Write a Serverless run's cell outputs back into the local source .ipynb.

``databricks jobs export-run <RUN_ID> --views-to-export CODE`` returns the executed
notebook as HTML; it embeds ``__DATABRICKS_NOTEBOOK_MODEL = '<base64>'``, which decodes
(base64, then URL-unquote) to the notebook model JSON. Each command carries its source
(``command``) and ``results.data`` items: ``ansi`` (stdout/stderr text) and
``mimeBundle`` (display / execute results, e.g. matplotlib PNGs).

Commands are matched to local code cells by normalized SOURCE, not index: the runner
strips ``%pip`` / ``%restart_python`` cells and may inject a ``--set-var`` override
cell, and ``%run ./config_nb`` inlines the dependency's commands into the model. Only
matched cells are touched (outputs + ``execution_count``); everything else, and the
file's JSON serialization, is preserved.
"""

from __future__ import annotations

import base64
import json
import pathlib
import re
import subprocess
import time
import urllib.parse

_MODEL_RE = re.compile(r"__DATABRICKS_NOTEBOOK_MODEL\s*=\s*'([^']+)'")


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


def _norm(src) -> str:
    if isinstance(src, list):
        src = "".join(src)
    return "\n".join(line.rstrip() for line in src.strip().splitlines())


def _lines(text: str) -> list[str]:
    return text.splitlines(keepends=True)


SUPPORTED = ("ansi", "mimeBundle")


def _items(results) -> list:
    """The result items of a command's ``results``.

    A cell that ends in ``dbutils.notebook.exit(value)`` comes back with ``results`` of type
    ``raw`` whose ``data`` is the exit value itself (a string), not a list of items: it has
    none.
    """
    data = results.get("data") if isinstance(results, dict) else None
    return data if isinstance(data, list) else []


def _readable(results) -> bool:
    """False for a result of a command this tool cannot read: one that is neither a list of items
    nor the ``raw`` exit value of a ``dbutils.notebook.exit`` cell (an error result, for one). Its
    cell keeps the outputs it has, instead of being cleared as if it had printed nothing."""
    if not isinstance(results, dict) or results.get("type") == "raw":
        return True
    return isinstance(results.get("data"), list)


def to_outputs(results: dict | None) -> list[dict]:
    """Convert a command's ``results`` to nbformat v4 outputs (``SUPPORTED`` item types)."""
    outs = []
    for item in _items(results):
        kind = item.get("type")
        if kind == "ansi" and item.get("data"):
            outs.append({"name": item.get("name") or "stdout", "output_type": "stream",
                         "text": _lines(item["data"])})
        elif kind == "mimeBundle" and item.get("data"):
            data = {k: (v if k.startswith("image/") or not isinstance(v, str) else _lines(v))
                    for k, v in item["data"].items()}
            if item.get("executionCount") is not None:
                outs.append({"data": data, "execution_count": 0, "metadata": {},
                             "output_type": "execute_result"})
            else:
                outs.append({"data": data, "metadata": {}, "output_type": "display_data"})
    return outs


def apply_outputs(nb: dict, model: dict) -> tuple[int, list[int]]:
    """Set outputs on local code cells matched (by source) to executed commands.

    A cell whose results include an item type ``to_outputs`` can't convert (e.g. a
    ``display()`` table), or whose result is not a list of items (an error result), is left
    untouched rather than losing that output.
    Returns ``(n_updated, indices_of_local_code_cells_not_updated)``.
    """
    executed = {}
    for cmd in sorted(model.get("commands", []), key=lambda c: c.get("position", 0)):
        key = _norm(cmd.get("command", ""))
        if key and not key.startswith(("%md", "%run")):
            executed.setdefault(key, cmd)
    updated, unmatched = 0, []
    for i, cell in enumerate(nb["cells"]):
        if cell.get("cell_type") != "code":
            continue
        key = _norm(cell.get("source", ""))
        if key.startswith(("%pip", "%restart_python", "%run")):
            continue
        cmd = executed.pop(key, None)
        results = (cmd or {}).get("results")
        items = _items(results)
        if (
            cmd is None
            or not _readable(results)
            or any(it.get("type") not in SUPPORTED for it in items)
        ):
            unmatched.append(i)
            continue
        cell["outputs"] = to_outputs(cmd.get("results"))
        cell["execution_count"] = 0
        updated += 1
    return updated, unmatched


def _dump_like(nb: dict, original: str) -> str:
    """Serialize ``nb`` the way ``original`` was serialized (indent=1 Jupyter style)."""
    for ensure_ascii in (False, True):
        probe = json.dumps(json.loads(original), indent=1, ensure_ascii=ensure_ascii)
        for tail in ("", "\n"):
            if probe + tail == original:
                return json.dumps(nb, indent=1, ensure_ascii=ensure_ascii) + tail
    raise ValueError("could not detect the notebook's JSON serialization; not rewriting it")


def _export_model(run_id, profile: str) -> dict:
    proc = subprocess.run(
        ["databricks", "jobs", "export-run", str(run_id), "--views-to-export", "CODE",
         "--profile", profile, "-o", "json"],
        capture_output=True, text=True, check=True)
    views = [v for v in json.loads(proc.stdout).get("views", []) if v.get("type") == "NOTEBOOK"]
    if not views:
        raise RuntimeError(f"export-run {run_id} returned no NOTEBOOK view")
    return decode_model(views[0]["content"])


def _n_items(model: dict) -> int:
    return sum(len(_items(c.get("results"))) for c in model.get("commands", []))


def harvest_run_outputs(run_id, profile: str, local_nb_path, settle_s: int = 20) -> tuple[int, list[int]]:
    """Export ``run_id`` and write its cell outputs into ``local_nb_path`` in place.

    An export taken right as the run terminates can lack large results (a multi-MB
    plot was observed missing), so re-export until the result count stops growing.
    """
    path = pathlib.Path(local_nb_path)
    model = _export_model(run_id, profile)
    for _ in range(4):
        time.sleep(settle_s)
        again = _export_model(run_id, profile)
        if _n_items(again) <= _n_items(model):
            break
        model = again
    original = path.read_text(encoding="utf-8")
    nb = json.loads(original)
    updated, unmatched = apply_outputs(nb, model)
    path.write_text(_dump_like(nb, original), encoding="utf-8")
    return updated, unmatched
