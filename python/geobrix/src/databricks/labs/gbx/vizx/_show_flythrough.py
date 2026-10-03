"""Display helper: show_flythrough — render and inline a fly-through animation.

``show_flythrough`` drives ``plot_point_cloud_flythrough`` to produce a
GIF (always) and optionally an MP4.  Two modes are supported:

- ``persist=True`` (default) — writes the GIF (and optional MP4) to ``out_dir``
  on the notebook's Workspace file-system, inlines the result, and returns an
  ``md_snippet`` the author can paste into a ``%md`` cell.
- ``persist=False`` (live/ephemeral) — renders to a driver-local ``/tmp``
  temp directory, inlines base64 in the current cell, leaves **no file in
  ``out_dir``**, and returns ``md_snippet=None``.

All file writes use the **local /tmp temp-then-copy** pattern to avoid the
FUSE-seek issue that produces an 8-byte stub on UC Volume and Workspace FUSE
paths.

Pure Python, light-tier — no JAR required.
"""

from __future__ import annotations

import base64
import os
import pathlib
import shutil
import tempfile
import warnings
from typing import Optional
from urllib.parse import quote as urlquote

from databricks.labs.gbx.vizx import _pointcloud_flythrough as _pft_module

# ---------------------------------------------------------------------------
# Internal helpers — small, individually testable
# ---------------------------------------------------------------------------


def _derive_name(source) -> str:
    """Derive a meaningful file-stem from a path-like source.

    Returns the ``pathlib.Path.stem`` of ``source`` when it is a string,
    :class:`pathlib.Path`, or any :class:`os.PathLike`.

    Raises
    ------
    ValueError
        When ``source`` has no derivable basename (e.g. a loaded
        ``DataFrame`` or in-memory array).  The caller should pass an
        explicit ``name=`` in this case.
    """
    if isinstance(source, (str, pathlib.Path, os.PathLike)):
        stem = pathlib.Path(str(source)).stem
        if stem:
            return stem
    raise ValueError(
        "show_flythrough: pass an explicit name=; cannot derive one from the source"
    )


def _try_display_html(html: str) -> None:
    """Emit an HTML string via IPython.display.HTML (no-op outside a notebook)."""
    try:
        from IPython.display import HTML, display

        display(HTML(html))
    except Exception:  # noqa: BLE001
        pass


def _file_mb(path: pathlib.Path) -> float:
    """Return file size in megabytes."""
    return path.stat().st_size / (1024 * 1024)


def _base64_file(path: pathlib.Path) -> str:
    """Return the base64-encoded content of a file as an ASCII string."""
    return base64.b64encode(path.read_bytes()).decode("ascii")


def _parse_volume_parts(volume_dir: str):
    """Parse /Volumes/<cat>/<schema>/<vol>[/<rest>] → (cat, schema, vol).

    Returns ``(None, None, None)`` for any path that does not match the pattern.
    """
    parts = pathlib.PurePosixPath(volume_dir).parts
    # Expected form: ('/', 'Volumes', cat, schema, vol, ...)
    if len(parts) >= 5 and parts[1] == "Volumes":
        return parts[2], parts[3], parts[4]
    return None, None, None


def _workspace_url() -> str:
    """Best-effort: return the Databricks workspace URL from the environment.

    Tries ``DATABRICKS_HOST`` env var first, then falls back to
    ``spark.databricks.workspaceUrl`` (not available on Serverless).
    Returns an empty string when neither source is reachable.
    """
    url = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    if url:
        return url
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is not None:
            host = spark.conf.get("spark.databricks.workspaceUrl", "")
            if host:
                return f"https://{host}"
    except Exception:  # noqa: BLE001
        pass
    return ""


def _volume_explorer_link(mp4_path: pathlib.Path, volume_dir: str) -> str:
    """Build a Databricks Data-Explorer URL for an MP4 on a UC Volume.

    Returns the plain path string as a fallback when the workspace URL
    cannot be determined.
    """
    cat, schema, vol = _parse_volume_parts(volume_dir)
    if not (cat and schema and vol):
        return str(mp4_path)

    host = _workspace_url()
    if not host:
        return str(mp4_path)

    vol_root = f"/Volumes/{cat}/{schema}/{vol}"
    parent_dir = str(mp4_path.parent)
    subdir = parent_dir[len(vol_root) :] if parent_dir.startswith(vol_root) else ""

    volume_path_param = urlquote(subdir or "/", safe="")
    file_param = urlquote(mp4_path.name, safe="")

    return (
        f"{host}/explore/data/volumes/{cat}/{schema}/{vol}"
        f"?volumePath={volume_path_param}&filePreviewPath={file_param}"
    )


def _render_to_temp(
    source,
    name: str,
    need_mp4: bool,
    flythrough_kwargs: dict,
) -> tuple:
    """Render frames into a ``/tmp`` temp directory and return paths.

    Returns ``(tmp_gif, tmp_mp4_or_None, tmp_dir)`` — the caller owns the
    temp directory and must call ``shutil.rmtree(tmp_dir)`` after use.
    """
    tmp_dir = pathlib.Path(tempfile.mkdtemp(prefix="gbx_flythrough_", dir="/tmp"))
    formats = tuple(["gif"] + (["mp4"] if need_mp4 else []))
    _pft_module.plot_point_cloud_flythrough(
        source,
        out_path=str(tmp_dir / name),
        formats=formats,
        **flythrough_kwargs,
    )
    tmp_gif = tmp_dir / f"{name}.gif"
    tmp_mp4_candidate = tmp_dir / f"{name}.mp4"
    tmp_mp4 = tmp_mp4_candidate if (need_mp4 and tmp_mp4_candidate.exists()) else None
    return tmp_gif, tmp_mp4, tmp_dir


def _maybe_copy_to_volume(
    mp4_path: Optional[pathlib.Path],
    volume_dir: Optional[str],
    name: str,
) -> Optional[pathlib.Path]:
    """Copy an MP4 to a UC Volume directory and return the destination path.

    Returns ``None`` when either argument is ``None``.
    """
    if mp4_path is None or volume_dir is None:
        return None
    vol_dir = pathlib.Path(volume_dir)
    vol_dir.mkdir(parents=True, exist_ok=True)
    dest = vol_dir / f"{name}.mp4"
    shutil.copy(str(mp4_path), str(dest))
    return dest


def _build_display_html(
    gif_path: Optional[pathlib.Path],
    mp4_path: Optional[pathlib.Path],
    vol_mp4_path: Optional[pathlib.Path],
    mp4_volume_dir: Optional[str],
    preview_mp4: bool,
    cap_mb: float,
    name: str,
    md_snippet: Optional[str],
) -> tuple:
    """Build HTML display parts and the optional preview-video snippet.

    Returns ``(html_parts: list[str], preview_html: str | None)``.

    Emits :class:`UserWarning` when GIF or MP4 exceeds ``cap_mb``.
    """
    html_parts: list[str] = []
    preview_html: Optional[str] = None

    # GIF inline (base64 <img>)
    if gif_path is not None and gif_path.exists():
        gif_mb = _file_mb(gif_path)
        if gif_mb <= cap_mb:
            b64 = _base64_file(gif_path)
            html_parts.append(
                f'<img src="data:image/gif;base64,{b64}" '
                f'alt="{name} fly-through" style="max-width:100%;border-radius:4px"/>'
            )
        else:
            suffix = f" — embed via the md_snippet: {md_snippet}" if md_snippet else ""
            warnings.warn(
                f"[show_flythrough] GIF is {gif_mb:.1f} MB, exceeds the "
                f"~{cap_mb} MB inline limit{suffix}",
                stacklevel=3,
            )

    # Data-Explorer link for Volume MP4
    if vol_mp4_path is not None:
        link = _volume_explorer_link(vol_mp4_path, mp4_volume_dir)  # type: ignore[arg-type]
        if link.startswith("http"):
            html_parts.append(
                f'<p><a href="{link}" target="_blank">'
                f"&#x1F4C2; Open MP4 in Data Explorer</a></p>"
            )
        else:
            html_parts.append(f"<p>MP4 written to: <code>{link}</code></p>")

    # MP4 inline preview (base64 <video>) behind <details>
    if preview_mp4 and mp4_path is not None and mp4_path.exists():
        mp4_mb = _file_mb(mp4_path)
        if mp4_mb <= cap_mb:
            b64_mp4 = _base64_file(mp4_path)
            preview_html = (
                "<details><summary>&#x25B6; Preview video here</summary>"
                '<video controls autoplay loop style="max-width:100%;border-radius:4px">'
                f'<source src="data:video/mp4;base64,{b64_mp4}" type="video/mp4"/>'
                "</video></details>"
            )
            html_parts.append(preview_html)
        else:
            warnings.warn(
                f"[show_flythrough] MP4 is {mp4_mb:.1f} MB, exceeds the "
                f"~{cap_mb} MB inline limit — use the link above",
                stacklevel=3,
            )

    return html_parts, preview_html


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def show_flythrough(
    source,
    *,
    kind: str = "point_cloud",
    out_dir: str = "./resources/geobrix",
    name: Optional[str] = None,
    overwrite: bool = False,
    persist: bool = True,
    mp4: bool = False,
    mp4_volume_dir: Optional[str] = None,
    preview_mp4: bool = False,
    cap_mb: float = 7.5,
    **flythrough_kwargs,
) -> dict:
    """Render and inline a fly-through animation in a Databricks notebook.

    Drives :func:`~databricks.labs.gbx.vizx.plot_point_cloud_flythrough` to
    produce a GIF (always) and optionally an MP4.  All file writes use a
    **local /tmp temp-then-copy** pattern: content is rendered into a
    temporary directory under ``/tmp`` first, then ``shutil.copy``-ed to the
    target.  This avoids the FUSE-seek issue that would silently produce an
    8-byte stub on UC Volume and Workspace FUSE paths (see *uc-volumes* rule).

    Parameters
    ----------
    source :
        Point-cloud input — same as
        :func:`~databricks.labs.gbx.vizx.plot_point_cloud_flythrough` accepts
        (LAS/LAZ path, pandas ``DataFrame`` with x/y/z columns, or
        ``GeoDataFrame``).
    kind :
        Only ``"point_cloud"`` is supported now; raises ``ValueError`` for any
        other value (intentional forward-compatibility gate).
    out_dir :
        Target directory for the GIF and a local MP4 copy (``persist=True``
        only).  Relative paths are resolved from the current working directory
        (the notebook's WFS dir at runtime).  Created automatically when it
        does not exist.  Unused when ``persist=False``.
    name :
        File-stem for output files: ``<name>.gif``, ``<name>.mp4``.
        Defaults to the **basename of the source path** when ``source`` is
        a string, :class:`pathlib.Path`, or :class:`os.PathLike`
        (e.g. ``goldengate.laz`` → ``goldengate``).  When the source has no
        derivable basename (a loaded ``DataFrame`` or in-memory array),
        ``name`` is **required** — a clear :class:`ValueError` is raised if
        omitted.  Using meaningful names (the LAZ stem) enables
        ``overwrite=False`` idempotency across multiple sections of the same
        notebook that write to the same ``out_dir``.
    overwrite :
        ``False`` (default) — skip re-rendering if all requested outputs
        already exist in ``out_dir``.  ``True`` — always re-render.
        Ignored when ``persist=False`` (ephemeral mode never caches).
    persist :
        ``True`` (default) — write the GIF (and optional MP4) to ``out_dir``
        on the WFS, inline the result, and return an ``md_snippet`` for
        pasting into a ``%md`` cell.  ``False`` — render to a driver-local
        ``/tmp`` temp, inline base64 in the current cell, leave **no file**
        in ``out_dir``, and return ``md_snippet=None`` (ephemeral "show it
        here, leave nothing behind" mode).
    mp4 :
        Opt-in: also render and write an MP4.
    mp4_volume_dir :
        UC Volume directory to copy the MP4 into (enables the Data-Explorer
        preview link).  ``None`` → MP4 stays in ``out_dir`` only (or
        discarded in ephemeral mode).  Implies ``mp4=True``.
    preview_mp4 :
        Opt-in: embed the MP4 inline as a base64 ``<video>`` inside a
        collapsible ``<details>`` block.  Silently capped at ``cap_mb``
        (a :class:`UserWarning` is emitted for oversized files).
        Implies ``mp4=True``.
    cap_mb :
        Inline base64 ceiling in megabytes (default 7.5).  Files larger
        than this value are not inlined; a :class:`UserWarning` is issued.
    **flythrough_kwargs :
        Forwarded to
        :func:`~databricks.labs.gbx.vizx.plot_point_cloud_flythrough`
        (``path``, ``fps``, ``seconds``, ``z_exaggeration``,
        ``max_points``, ``point_size``, ``dpi``, ``title``, …).

    Returns
    -------
    dict
        ``{"gif": pathlib.Path | None, "mp4": pathlib.Path | None,
        "md_snippet": str | None}``.

        - ``"gif"`` — path to the written GIF (``persist=True``) or ``None``
          (``persist=False``).
        - ``"mp4"`` — path to the written MP4 in ``out_dir`` or on the
          Volume, or ``None``.
        - ``"md_snippet"`` — Markdown image tag for ``%md`` cells when
          ``persist=True``; ``None`` when ``persist=False``.
        - ``"_preview_html"`` — (internal) the base64 ``<video>`` HTML string
          when an MP4 was embedded inline, or ``None``.

    Raises
    ------
    ValueError
        If ``kind`` is not ``"point_cloud"``, or if ``name`` is omitted
        and cannot be derived from ``source``.
    """
    # kind guard
    if kind != "point_cloud":
        raise ValueError(
            f"show_flythrough currently supports kind='point_cloud' only; "
            f"{kind!r} is not supported yet."
        )

    # name resolution: derive from source path when not supplied
    resolved_name: str = name if name is not None else _derive_name(source)

    need_mp4 = mp4 or preview_mp4 or (mp4_volume_dir is not None)

    # Remove caller-supplied keys that we own; never forward them.
    flythrough_kwargs.pop("out_path", None)
    flythrough_kwargs.pop("formats", None)

    # ------------------------------------------------------------------ #
    # persist=False  (ephemeral / live mode)                              #
    # ------------------------------------------------------------------ #
    if not persist:
        vol_mp4: Optional[pathlib.Path] = None
        preview_html: Optional[str] = None
        tmp_gif, tmp_mp4, tmp_dir = _render_to_temp(
            source, resolved_name, need_mp4, flythrough_kwargs
        )
        try:
            vol_mp4 = _maybe_copy_to_volume(tmp_mp4, mp4_volume_dir, resolved_name)
            html_parts, preview_html = _build_display_html(
                tmp_gif,
                tmp_mp4,
                vol_mp4,
                mp4_volume_dir,
                preview_mp4,
                cap_mb,
                resolved_name,
                None,
            )
            if html_parts:
                _try_display_html("\n".join(html_parts))
        finally:
            shutil.rmtree(str(tmp_dir), ignore_errors=True)
        return {
            "gif": None,
            "mp4": vol_mp4,
            "md_snippet": None,
            "_preview_html": preview_html,
        }

    # ------------------------------------------------------------------ #
    # persist=True  (default / write-to-WFS mode)                        #
    # ------------------------------------------------------------------ #
    out_dir_path = pathlib.Path(out_dir)
    out_dir_path.mkdir(parents=True, exist_ok=True)
    gif_path = out_dir_path / f"{resolved_name}.gif"
    local_mp4_path = out_dir_path / f"{resolved_name}.mp4"

    all_exist = gif_path.exists() and (not need_mp4 or local_mp4_path.exists())
    if not (all_exist and not overwrite):
        tmp_gif_p, tmp_mp4_p, tmp_dir_p = _render_to_temp(
            source, resolved_name, need_mp4, flythrough_kwargs
        )
        try:
            if tmp_gif_p.exists():
                shutil.copy(str(tmp_gif_p), str(gif_path))
            if tmp_mp4_p is not None:
                shutil.copy(str(tmp_mp4_p), str(local_mp4_path))
        finally:
            shutil.rmtree(str(tmp_dir_p), ignore_errors=True)

    result_mp4: Optional[pathlib.Path] = (
        local_mp4_path if (need_mp4 and local_mp4_path.exists()) else None
    )
    vol_mp4_p = _maybe_copy_to_volume(result_mp4, mp4_volume_dir, resolved_name)
    md_snippet: Optional[str] = f"![flythrough]({out_dir}/{resolved_name}.gif)"

    html_parts_p, preview_html_p = _build_display_html(
        gif_path,
        result_mp4,
        vol_mp4_p,
        mp4_volume_dir,
        preview_mp4,
        cap_mb,
        resolved_name,
        md_snippet,
    )
    if html_parts_p:
        _try_display_html("\n".join(html_parts_p))
    print(f"md_snippet: {md_snippet}")

    return {
        "gif": gif_path,
        "mp4": result_mp4,
        "md_snippet": md_snippet,
        "_preview_html": preview_html_p,
    }
