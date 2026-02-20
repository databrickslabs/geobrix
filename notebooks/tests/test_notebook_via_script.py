"""
Test notebooks by converting to Python and running them (no Jupyter kernel/ZMQ).

Supports verbosity via env GBX_NOTEBOOK_VERBOSITY:
  quiet     - print only notebook name and status
  truncated - (default) notebook name; per cell: label source/result (full) or (truncated),
              then print actual source/result if (full), or truncated content if (truncated)
  full      - full notebook contents, full cell source, full results per cell

Write-path remapping (when workdir is set and GBX_NOTEBOOK_ALLOW_ABSOLUTE_WRITES is not 1):
  Absolute paths used in write operations (open(..., 'w'), Path.write_text, etc.) are
  rewritten to be under the notebook workdir so runs are undoable. Temp paths and
  relative paths are left unchanged. Use --allow-absolute-writes to disable remapping.

Run: gbx:test:notebooks --path test_notebook_via_script.py
     GBX_NOTEBOOK_VERBOSITY=full gbx:test:notebooks --path test_notebook_via_script.py
"""

import ast
import builtins
import os
import re
import shutil
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path
from typing import Any

try:
    import pytest
except ImportError:
    pytest = None  # optional: only needed when running as pytest test

TESTS_DIR = Path(__file__).resolve().parent
FIXTURES_DIR = TESTS_DIR / "fixtures"
MINIMAL_NB = FIXTURES_DIR / "minimal.ipynb"

TRUNCATE_LEN = 300


def _verbosity() -> str:
    v = (os.environ.get("GBX_NOTEBOOK_VERBOSITY") or "truncated").strip().lower()
    return v if v in ("quiet", "truncated", "full") else "truncated"


def _truncate(text: str, max_len: int = TRUNCATE_LEN) -> str:
    if not text:
        return ""
    text = text.rstrip()
    if len(text) <= max_len:
        return text
    return text[:max_len] + "\n... [truncated]"


def _within_limit(text: str, max_len: int = TRUNCATE_LEN) -> bool:
    """True if text fits within limit (would not be truncated)."""
    if not text:
        return True
    return len(text.rstrip()) <= max_len


# --- Write-path detection and remapping ---

_WRITE_MODES = frozenset({"w", "wb", "a", "ab", "x", "xb", "w+", "wb+", "a+", "ab+"})


def _is_write_mode(mode: str) -> bool:
    """True if open() mode string indicates a write (or read-write) operation."""
    if not mode or not isinstance(mode, str):
        return False
    m = mode.strip().strip("'\"")
    return m in _WRITE_MODES or (len(m) >= 1 and m[0] in "wax")


def _is_absolute_path(path: Any) -> bool:
    """True if path is an absolute path (str or Path)."""
    if path is None:
        return False
    if isinstance(path, Path):
        return path.is_absolute()
    if isinstance(path, str):
        return path.startswith("/") or (len(path) >= 2 and path[1] == ":")
    return False


def _under_temp(path: Any) -> bool:
    """True if path is under a temp directory (so we do not remap it)."""
    try:
        s = str(path).strip()
        if not s:
            return False
        resolved = os.path.realpath(s) if os.path.isabs(s) else s
        tmp = tempfile.gettempdir()
        return resolved.startswith(tmp) or resolved.startswith("/tmp/") or resolved.startswith("/tmp")
    except Exception:
        return False


def _remap_absolute_to_workdir(path: Any, workdir: Path) -> str:
    """If path is absolute and not under temp, return path under workdir (no leading /). Else return str(path)."""
    if path is None:
        return ""
    s = str(path).strip()
    if not s:
        return s
    if not _is_absolute_path(s):
        return s
    if _under_temp(s):
        return s
    rel = s.lstrip("/")
    return str(workdir / rel)


def _remap_write_path(path: Any, workdir: Path) -> str:
    """Alias for _remap_absolute_to_workdir; used for write operations."""
    return _remap_absolute_to_workdir(path, workdir)


def _remap_read_path(path: Any, workdir: Path) -> str:
    """Alias for _remap_absolute_to_workdir; used for read operations."""
    return _remap_absolute_to_workdir(path, workdir)


def _make_wrapped_open(
    workdir: Path,
    original_open: Any,
    remapped_count: list[int],
    allow_absolute_reads: bool,
) -> Any:
    """Return an open() that remaps absolute write/read paths to workdir; appends to remapped_count when a write path is remapped."""

    def wrapped_open(file, mode="r", *args, **kwargs):
        use_path = file
        if use_path is None:
            return original_open(file, mode, *args, **kwargs)
        if _is_write_mode(mode):
            remapped = _remap_write_path(use_path, workdir)
            if remapped != str(use_path):
                remapped_count.append(1)
                file = remapped
        elif not allow_absolute_reads:
            remapped = _remap_read_path(use_path, workdir)
            if remapped != str(use_path):
                file = remapped
        return original_open(file, mode, *args, **kwargs)

    return wrapped_open


def _install_read_write_remap_patches(
    workdir: Path,
    allow_absolute_writes: bool,
    allow_absolute_reads: bool,
) -> tuple[list[Any], list[Any]]:
    """Patch os and shutil so absolute read/write paths are remapped under workdir.
    Returns (orig_os_attrs, orig_shutil_attrs) to restore in finally.
    """
    os_mod = sys.modules.get("os")
    shutil_mod = sys.modules.get("shutil")
    if os_mod is None or shutil_mod is None:
        return [], []

    orig_os, orig_shutil = [], []

    def remap_read(p: Any) -> Any:
        if allow_absolute_reads or workdir is None:
            return p
        return _remap_read_path(p, workdir)

    def remap_write(p: Any) -> Any:
        if allow_absolute_writes or workdir is None:
            return p
        return _remap_write_path(p, workdir)

    if os_mod is not None and workdir is not None and not allow_absolute_reads:
        _orig_stat = os_mod.stat
        _orig_listdir = os_mod.listdir
        _orig_scandir = os_mod.scandir
        _orig_walk = os_mod.walk

        def _wrapped_stat(path: Any, *args: Any, **kwargs: Any) -> Any:
            return _orig_stat(remap_read(path), *args, **kwargs)

        def _wrapped_listdir(path: Any = None, *args: Any, **kwargs: Any) -> Any:
            if path is None:
                return _orig_listdir(*args, **kwargs)
            return _orig_listdir(remap_read(path), *args, **kwargs)

        def _wrapped_scandir(path: Any = None, *args: Any, **kwargs: Any) -> Any:
            if path is None:
                return _orig_scandir(*args, **kwargs)
            return _orig_scandir(remap_read(path), *args, **kwargs)

        def _wrapped_walk(top: Any, *args: Any, **kwargs: Any) -> Any:
            return _orig_walk(remap_read(top), *args, **kwargs)

        os_mod.stat = _wrapped_stat
        os_mod.listdir = _wrapped_listdir
        os_mod.scandir = _wrapped_scandir
        os_mod.walk = _wrapped_walk
        orig_os = [("stat", _orig_stat), ("listdir", _orig_listdir), ("scandir", _orig_scandir), ("walk", _orig_walk)]

    if shutil_mod is not None and workdir is not None:
        _orig_copy = shutil_mod.copy
        _orig_copy2 = shutil_mod.copy2
        _orig_copytree = shutil_mod.copytree
        _orig_move = shutil_mod.move

        def _wrapped_copy(src: Any, dst: Any, *args: Any, **kwargs: Any) -> Any:
            return _orig_copy(remap_read(src), remap_write(dst), *args, **kwargs)

        def _wrapped_copy2(src: Any, dst: Any, *args: Any, **kwargs: Any) -> Any:
            return _orig_copy2(remap_read(src), remap_write(dst), *args, **kwargs)

        def _wrapped_copytree(src: Any, dst: Any, *args: Any, **kwargs: Any) -> Any:
            return _orig_copytree(remap_read(src), remap_write(dst), *args, **kwargs)

        def _wrapped_move(src: Any, dst: Any, *args: Any, **kwargs: Any) -> Any:
            return _orig_move(remap_read(src), remap_write(dst), *args, **kwargs)

        shutil_mod.copy = _wrapped_copy
        shutil_mod.copy2 = _wrapped_copy2
        shutil_mod.copytree = _wrapped_copytree
        shutil_mod.move = _wrapped_move
        orig_shutil = [("copy", _orig_copy), ("copy2", _orig_copy2), ("copytree", _orig_copytree), ("move", _orig_move)]

    return orig_os, orig_shutil


def _restore_read_write_patches(orig_os: list, orig_shutil: list) -> None:
    """Restore os and shutil to original functions."""
    os_mod = sys.modules.get("os")
    shutil_mod = sys.modules.get("shutil")
    for name, orig in orig_os:
        if os_mod is not None:
            setattr(os_mod, name, orig)
    for name, orig in orig_shutil:
        if shutil_mod is not None:
            setattr(shutil_mod, name, orig)


def get_notebook_write_paths(notebook_path: Path) -> list[tuple[int, int, str]]:
    """Identify every path in a notebook used in a write operation.

    Scans code cells for open(..., write_mode), Path(...).write_text, Path(...).write_bytes.
    Returns list of (cell_index, 1-based_line, description) for reporting.
    """
    import nbformat

    nb = nbformat.read(str(notebook_path), as_version=4)
    code_cells = [(i, c) for i, c in enumerate(nb.cells) if c.cell_type == "code"]
    results: list[tuple[int, int, str]] = []

    for cell_idx, cell in code_cells:
        source = "".join(cell.source) if hasattr(cell.source, "__iter__") and not isinstance(cell.source, str) else (cell.source or "")
        lines = source.splitlines()
        try:
            tree = ast.parse(source)
        except SyntaxError:
            continue

        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                # open(path, mode) or open(path, mode=...)
                if isinstance(node.func, ast.Name) and node.func.id == "open":
                    if node.args:
                        line_no = node.lineno
                        mode_val = "r"
                        for kw in node.keywords:
                            if kw.arg == "mode" and isinstance(kw.value, ast.Constant):
                                mode_val = str(kw.value.value) if kw.value.value else "r"
                                break
                        if len(node.args) >= 2 and isinstance(node.args[1], ast.Constant):
                            mode_val = str(node.args[1].value) if node.args[1].value else "r"
                        if _is_write_mode(mode_val):
                            snippet = lines[line_no - 1].strip() if line_no <= len(lines) else "open(...)"
                            results.append((cell_idx, line_no, f"open(..., {mode_val!r}) — {snippet[:60]}"))

                # Path(...).write_text(...) or Path(...).write_bytes(...)
                if isinstance(node.func, ast.Attribute):
                    if node.func.attr in ("write_text", "write_bytes"):
                        line_no = node.lineno
                        snippet = lines[line_no - 1].strip() if line_no <= len(lines) else "Path(...).write_*"
                        results.append((cell_idx, line_no, f"{node.func.attr} — {snippet[:60]}"))

    return results


def _remap_absolute_output_paths(cmd: str, workdir: Path) -> str:
    """Rewrite redirections to absolute paths so they write under workdir (undoable)."""
    def repl(m: re.Match) -> str:
        redir, abspath = m.group(1), m.group(2).rstrip()
        sanitized = abspath.replace("/", "_").lstrip("_") or "out"
        return f"{redir} {workdir / sanitized}"
    return re.sub(r"(\s(?:>>?|2>>?)\s+)(/[^\s]*)", repl, cmd)


def _run_shell(cmd: str, workdir: Path, remap: bool = True) -> tuple[bool, str]:
    """Run shell command with cwd=workdir; optionally remap absolute output paths. Return (success, combined stdout+stderr)."""
    if remap:
        cmd = _remap_absolute_output_paths(cmd, workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    r = subprocess.run(
        cmd,
        shell=True,
        cwd=str(workdir),
        capture_output=True,
        text=True,
        timeout=300,
    )
    out = (r.stdout or "") + (r.stderr or "")
    return r.returncode == 0, out


def _run_pip_install(args_str: str) -> tuple[bool, str]:
    """Run pip install in the current interpreter (e.g. isolated venv). Return (success, output)."""
    args_str = args_str.strip()
    if args_str.startswith("install"):
        args_str = args_str[7:].strip()
    args = [a for a in re.split(r"\s+", args_str) if a] if args_str else []
    r = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-q"] + args,
        capture_output=True,
        text=True,
        timeout=120,
    )
    out = (r.stdout or "") + (r.stderr or "")
    return r.returncode == 0, out


def _get_cell_magic_type(source: str) -> str:
    """Return 'pip' | 'bash' | 'python' | 'python_with_shell' for cell handling."""
    lines = [ln.strip() for ln in (source or "").splitlines()]
    first = (lines[0] or "").strip()
    if first.startswith("%%bash") or first.startswith("%%sh"):
        return "bash"
    if first.startswith("%pip") or first.startswith("%conda"):
        return "pip"
    if first.startswith("%") and not first.startswith("%%python"):
        return "python"  # other magics: skip or treat as Python later
    # Check for any line starting with !
    if any(ln.startswith("!") for ln in lines if ln):
        return "python_with_shell"
    return "python"


def _is_path_like_literal(s: str) -> bool:
    """True if string looks like an absolute path we should rewrite (not URL, not temp)."""
    if not s or not s.startswith("/"):
        return False
    if s.startswith("//"):  # URL or protocol
        return False
    if s.startswith("/tmp") or s.startswith("/var/"):
        return False
    return True


def _line_col_to_offset(lines: list[str], lineno: int, col: int) -> int:
    """Convert 1-based lineno and 0-based col to character offset (lines from splitlines(keepends=True))."""
    offset = 0
    for i in range(lineno - 1):
        if i < len(lines):
            offset += len(lines[i])
    if lineno <= len(lines):
        offset += min(col, len(lines[lineno - 1]))
    return offset


def _rewrite_absolute_path_literals_in_source(source: str, workdir_var: str) -> str:
    """Rewrite string literals that are absolute path-like to str(workdir_var / \"path_without_leading_slash\").
    So /Volumes/... and other leading-/ strings become relative to workdir. Temp paths and URLs are skipped.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return source
    lines = source.splitlines(keepends=True)
    if not lines:
        return source

    replacements: list[tuple[int, int, str]] = []  # (start, end, new_text)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
            continue
        val = node.value
        if not _is_path_like_literal(val):
            continue
        rest = val.lstrip("/")
        if not rest:
            continue
        new_expr = f"str({workdir_var} / {repr(rest)})"
        start_lineno = node.lineno
        start_col = node.col_offset
        end_lineno = getattr(node, "end_lineno", start_lineno)
        end_col = getattr(node, "end_col_offset", start_col + len(repr(val)))
        start = _line_col_to_offset(lines, start_lineno, start_col)
        end = _line_col_to_offset(lines, end_lineno, end_col)
        replacements.append((start, end, new_expr))

    # Apply from end to start so offsets stay valid
    for start, end, new_text in sorted(replacements, key=lambda r: -r[0]):
        source = source[:start] + new_text + source[end:]
    return source


def _transform_shell_lines(source: str, run_shell_name: str = "__run_shell__") -> str:
    """Replace lines starting with ! by calls to run_shell_name('...')."""
    out = []
    for line in source.splitlines():
        s = line.strip()
        if s.startswith("!"):
            cmd = line[line.index("!") + 1 :].strip()
            # Escape single quotes in cmd
            cmd_esc = cmd.replace("\\", "\\\\").replace("'", "\\'")
            out.append(f"{run_shell_name}('{cmd_esc}')")
        else:
            out.append(line)
    return "\n".join(out)


def _is_magic_only(source: str) -> bool:
    """True if cell is only Jupyter magics (e.g. %pip, %%bash); not valid for exec()."""
    line = (source.split("\n")[0] or "").strip()
    return line.startswith("%") and not line.startswith("%%python")


def _run_notebook_cells(
    notebook_path: Path,
    cwd: Path,
    verbosity: str,
    workdir: Path | None = None,
    max_code_cells: int | None = None,
) -> tuple[bool, str]:
    """Execute notebook code cells one by one; return (success, error_message).
    When workdir is set: pip runs in current env (expect isolated venv), shell/! run with cwd=workdir and absolute output paths remapped under workdir.
    When max_code_cells is set, only the first max_code_cells code cells are run (e.g. 2 = config + pip only).
    """
    import nbformat

    nb = nbformat.read(str(notebook_path), as_version=4)
    code_cells = [(i, c) for i, c in enumerate(nb.cells) if c.cell_type == "code"]
    if max_code_cells is not None:
        code_cells = code_cells[:max_code_cells]
    name = notebook_path.name
    isolated = workdir is not None
    isolated_venv = os.environ.get("GBX_NOTEBOOK_ISOLATED") == "1"  # pip only when in venv

    if verbosity == "quiet":
        pass
    elif verbosity == "truncated":
        print(f"\n--- Notebook: {name} ---")
        print(f"Source/result: (full) = printed in full; (truncated) = printed up to {TRUNCATE_LEN} chars.")
        if isolated:
            print(f"Working dir (isolated): {workdir}")
        print("--- Executing code cells ---")
    else:
        print(f"\n--- Notebook: {name} ---")
        if isolated:
            print(f"Working dir (isolated): {workdir}")
        for i, cell in enumerate(nb.cells):
            src = "".join(cell.source) if hasattr(cell.source, "__iter__") and not isinstance(cell.source, str) else (cell.source or "")
            label = "code" if cell.cell_type == "code" else cell.cell_type
            print(f"  Cell {i} ({label}):\n{src}")
        print("--- Executing code cells ---")

    project_root_str = str(cwd)
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)
    globals_dict = {"__name__": "__main__", "__file__": str(notebook_path)}

    def make_run_shell(wd: Path):
        def __run_shell__(cmd: str) -> None:
            ok, out = _run_shell(cmd, wd)
            print(out, end="")
            if not ok:
                raise RuntimeError(f"Shell command failed: {cmd!r}")
        return __run_shell__

    if workdir is not None:
        workdir.mkdir(parents=True, exist_ok=True)
        globals_dict["__run_shell__"] = make_run_shell(workdir)
        globals_dict["__gbx_workdir__"] = workdir

    # Remap absolute read/write paths to workdir unless user opted out
    allow_absolute_writes = os.environ.get("GBX_NOTEBOOK_ALLOW_ABSOLUTE_WRITES") == "1"
    allow_absolute_reads = os.environ.get("GBX_NOTEBOOK_ALLOW_ABSOLUTE_READS") == "1"
    real_open = builtins.open
    write_paths_remapped: list[int] = []  # per-cell count of remapped writes (cleared each cell)
    if workdir is not None:
        wrapped_open = _make_wrapped_open(workdir, real_open, write_paths_remapped, allow_absolute_reads)
        builtins.open = wrapped_open
        globals_dict["open"] = wrapped_open
    orig_os_attrs, orig_shutil_attrs = _install_read_write_remap_patches(workdir, allow_absolute_writes, allow_absolute_reads)

    # When workdir is set, use workdir-based path for get_volumes_path so notebook and bundle use temp, not real /Volumes.
    # Inject __gbx_get_volumes_path__ and rewrite cell source (always when workdir set; independent of allow_absolute_reads).
    # Also patch the sample module so run_*_bundle see the same path.
    _gbx_sample_mod = None
    _gbx_bundle_mod = None
    _orig_get_volumes_path = None
    _use_patched_get_volumes_path = False
    if workdir is not None:
        def _patched_get_volumes_path(catalog: str, schema: str, volume: str) -> str:
            return str(workdir / "Volumes" / catalog / schema / volume / "geobrix-examples")

        globals_dict["__gbx_get_volumes_path__"] = _patched_get_volumes_path
        _use_patched_get_volumes_path = True
        if not allow_absolute_reads:
            try:
                import databricks.labs.gbx.sample as _gbx_sample_mod
                _orig_get_volumes_path = _gbx_sample_mod.get_volumes_path
                _gbx_sample_mod.get_volumes_path = _patched_get_volumes_path
                _gbx_bundle_mod = sys.modules.get("databricks.labs.gbx.sample._bundle")
                if _gbx_bundle_mod is not None:
                    _gbx_bundle_mod.get_volumes_path = _patched_get_volumes_path
            except ImportError:
                _gbx_sample_mod = None
                _gbx_bundle_mod = None
                _orig_get_volumes_path = None

    def _source_to_exec(src: str) -> str:
        """Return source to exec: optionally rewrite absolute path literals and get_volumes_path calls under workdir."""
        if workdir is None:
            return src
        src = _rewrite_absolute_path_literals_in_source(src, "__gbx_workdir__") if not allow_absolute_reads else src
        if _use_patched_get_volumes_path:
            src = src.replace("get_volumes_path(", "__gbx_get_volumes_path__(")
        return src

    def _cell_result_with_remap_msg(base_msg: str) -> str:
        n = len(write_paths_remapped)
        if n > 0 and workdir is not None and not allow_absolute_writes:
            return f"{base_msg}  📁 {n} write path(s) remapped under workdir"
        return base_msg

    try:
        for idx, cell in code_cells:
            write_paths_remapped.clear()
            source = "".join(cell.source) if hasattr(cell.source, "__iter__") and not isinstance(cell.source, str) else (cell.source or "")
            source_label = "full" if _within_limit(source) else "truncated"
            if verbosity == "full":
                print(f"\n  Cell {idx} (code):\n{source}")

            magic_type = _get_cell_magic_type(source) if isolated else "python"
            if magic_type == "pip" and isolated and isolated_venv:
                # %pip install ... -> run in current (venv) interpreter
                first_line = (source.split("\n")[0] or "").strip()
                if first_line.startswith("%pip"):
                    rest = first_line[4:].strip()
                else:
                    rest = first_line[10:].strip() if first_line.startswith("%conda") else first_line
                ok, out = _run_pip_install(rest)
                result_msg = "OK" if ok else out
                if not ok:
                    if verbosity == "quiet":
                        return False, result_msg
                    result_label = "full" if _within_limit(result_msg) else "truncated"
                    if verbosity == "truncated":
                        print(f"\n  Cell {idx}: FAILED — source ({source_label}), result ({result_label})")
                        display_src = source if source_label == "full" else _truncate(source)
                        print(f"  source:\n{display_src}")
                        display_res = result_msg if result_label == "full" else _truncate(result_msg)
                        print(f"  result:\n{display_res}")
                    elif verbosity == "full":
                        print(f"  Result: {result_msg}")
                    return False, result_msg
            elif magic_type == "pip":
                # Not in isolated venv (e.g. pytest-run test with workdir); skip to avoid system pip
                result_msg = "OK (skipped: magic)"
            elif magic_type == "bash" and isolated:
                lines = source.split("\n")
                body = "\n".join(lines[1:] if lines and lines[0].strip().startswith("%%") else lines).strip()
                ok, out = _run_shell(body, workdir)
                result_msg = "OK" if ok else out
                if not ok:
                    if verbosity == "quiet":
                        return False, result_msg
                    result_label = "full" if _within_limit(result_msg) else "truncated"
                    if verbosity == "truncated":
                        print(f"\n  Cell {idx}: FAILED — source ({source_label}), result ({result_label})")
                        display_src = source if source_label == "full" else _truncate(source)
                        print(f"  source:\n{display_src}")
                        display_res = result_msg if result_label == "full" else _truncate(result_msg)
                        print(f"  result:\n{display_res}")
                    elif verbosity == "full":
                        print(f"  Result: {result_msg}")
                    return False, result_msg
            elif magic_type == "python_with_shell" and isolated:
                transformed = _transform_shell_lines(source)
                code_to_run = _source_to_exec(transformed)
                try:
                    exec(compile(code_to_run, f"<cell {idx}>", "exec"), globals_dict)
                    result_msg = _cell_result_with_remap_msg("OK")
                except Exception as e:
                    result_msg = "".join(traceback.format_exception(type(e), e, e.__traceback__))
                    if verbosity == "quiet":
                        return False, result_msg
                    result_label = "full" if _within_limit(result_msg) else "truncated"
                    if verbosity == "truncated":
                        print(f"\n  Cell {idx}: FAILED — source ({source_label}), result ({result_label})")
                        display_src = source if source_label == "full" else _truncate(source)
                        print(f"  source:\n{display_src}")
                        display_res = result_msg if result_label == "full" else _truncate(result_msg)
                        print(f"  result:\n{display_res}")
                    elif verbosity == "full":
                        print(f"  Result: {result_msg}")
                    return False, result_msg
            elif magic_type == "python":
                if _is_magic_only(source) and not isolated:
                    result_msg = "OK (skipped: magic)"
                elif _is_magic_only(source) and isolated:
                    # Other magics (e.g. %load_ext) we skip
                    result_msg = "OK (skipped: magic)"
                else:
                    code_to_run = _source_to_exec(source)
                    # Sanity check: when workdir is set and cell uses get_volumes_path, replace must have been applied
                    if (
                        workdir is not None
                        and "SAMPLE_DATA_PATH" in source
                        and "get_volumes_path(" in source
                        and "__gbx_get_volumes_path__(" not in code_to_run
                    ):
                        raise AssertionError(
                            "Path rewrite not applied: code_to_run still has get_volumes_path(. "
                            f"workdir={workdir!r} _use_patched={_use_patched_get_volumes_path}"
                        )
                    try:
                        exec(compile(code_to_run, f"<cell {idx}>", "exec"), globals_dict)
                        result_msg = _cell_result_with_remap_msg("OK")
                    except Exception as e:
                        result_msg = "".join(traceback.format_exception(type(e), e, e.__traceback__))
                        if verbosity == "quiet":
                            return False, result_msg
                        result_label = "full" if _within_limit(result_msg) else "truncated"
                        if verbosity == "truncated":
                            print(f"\n  Cell {idx}: FAILED — source ({source_label}), result ({result_label})")
                            display_src = source if source_label == "full" else _truncate(source)
                            print(f"  source:\n{display_src}")
                            display_res = result_msg if result_label == "full" else _truncate(result_msg)
                            print(f"  result:\n{display_res}")
                        elif verbosity == "full":
                            print(f"  Result: {result_msg}")
                        return False, result_msg
            else:
                result_msg = "OK"

            result_label = "full" if _within_limit(result_msg) else "truncated"
            if verbosity == "truncated":
                print(f"\n  Cell {idx}: OK — source ({source_label}), result ({result_label})")
                display_src = source if source_label == "full" else _truncate(source)
                print(f"  source:\n{display_src}")
                display_res = result_msg if result_label == "full" else _truncate(result_msg)
                print(f"  result:\n{display_res}")
            elif verbosity == "full":
                print(f"  Result: {result_msg}")

        return True, ""
    finally:
        if _orig_get_volumes_path is not None:
            if _gbx_sample_mod is not None:
                _gbx_sample_mod.get_volumes_path = _orig_get_volumes_path
            if _gbx_bundle_mod is not None:
                _gbx_bundle_mod.get_volumes_path = _orig_get_volumes_path
        if workdir is not None:
            builtins.open = real_open
        _restore_read_write_patches(orig_os_attrs, orig_shutil_attrs)


def run_notebook_cell_by_cell(
    notebook_path: Path,
    cwd: Path,
    verbosity: str | None = None,
    workdir: Path | None = None,
    max_code_cells: int | None = None,
) -> bool:
    """Execute notebook code cells one by one. Print according to verbosity. Return True if all OK.
    When workdir is set: pip runs in current env (use with isolated venv), shell/! run with cwd=workdir and output paths remapped under workdir.
    When max_code_cells is set, only the first max_code_cells code cells are run (e.g. 2 = config + pip only).
    """
    v = verbosity or _verbosity()
    success, err = _run_notebook_cells(
        notebook_path, cwd, v, workdir=workdir, max_code_cells=max_code_cells
    )
    if v == "quiet":
        status = "PASSED" if success else "FAILED"
        print(f"  {notebook_path.name} ... {status}")
    elif not success and v != "quiet":
        print(f"  FAILED: {err}")
    return success


def notebook_to_script(notebook_path: Path, out_dir: Path) -> Path:
    """Convert notebook to .py using nbconvert; return path to the script."""
    import nbformat
    from nbconvert import PythonExporter

    nb = nbformat.read(str(notebook_path), as_version=4)
    exporter = PythonExporter()
    body, _ = exporter.from_notebook_node(nb)
    out_dir.mkdir(parents=True, exist_ok=True)
    script_path = out_dir / f"{notebook_path.stem}.py"
    script_path.write_text(body, encoding="utf-8")
    return script_path


def run_notebook_as_script(
    notebook_path: Path,
    cwd: Path,
    timeout: int = 60,
) -> subprocess.CompletedProcess:
    """Convert notebook to script and run it with python; return CompletedProcess."""
    with tempfile.TemporaryDirectory(prefix="nb_script_") as tmp:
        script_path = notebook_to_script(notebook_path, Path(tmp))
        return subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=timeout,
        )


def test_minimal_notebook_runs_as_script() -> None:
    """Run minimal.ipynb cell-by-cell with verbosity from GBX_NOTEBOOK_VERBOSITY."""
    assert MINIMAL_NB.is_file(), f"Notebook not found: {MINIMAL_NB}"
    project_root = TESTS_DIR.parent.parent
    ok = run_notebook_cell_by_cell(MINIMAL_NB, cwd=project_root)
    assert ok, "Notebook cell execution failed (see output above)"
