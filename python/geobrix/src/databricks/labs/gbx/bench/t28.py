"""The files and commands of the batched workspace pass (Task 28), written by ``gbx:bench:uc-probes``.

One call to :func:`build` turns the pass's parameters (catalog, scratch schema, Volume folder, JAR, wheels) into the
notebooks and statement files each run reads, and the exact command line each run is started with. Nothing here
touches Databricks. :func:`validate` holds every file to static checks (a notebook is JSON whose code compiles and whose
probe call binds to its signature; a statement file round-trips through the warehouse runner's parser, names only
functions that exist and qualifies them to three levels), and :func:`judge` reads a results file back.

The schemas are ``<schema>_w`` (the warehouse), ``_s6`` (Serverless env 6), ``_s6j`` (Serverless with the JAR) and
``_se`` (the engine probe), so no two runs share a routine.
"""

from __future__ import annotations

import argparse
import ast
import inspect
import json
import os
import re
import shlex
import sys
import tempfile
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

DEFAULT_CATALOG = "geospatial_docs"
DEFAULT_SCHEMA = "geobrix_t28"
DEFAULT_VOLROOT = "/Volumes/geospatial_docs/geobrix/sample-data"
WS_ROOT_PLACEHOLDER = "/Users/<current-user>/jobs/geobrix"  # what an un-resolved Params prints; main() resolves it
DEFAULT_PROFILE = "oauth-fe"
SCHEMA_KEYS = ("w", "s6", "s6j", "se")
UNQUALIFIED_OK = (
    "gbx_probe_t28_tmp_t",
)  # a temporary table made inside a procedure has no schema
FIXTURE_PATH = "/tmp/t28-fixtures"  # the path a virtual-tile row names; the function refuses the tile before opening it
# The notebook run for each series whose first cells the pass confirms (the dependency notebook is imported beside it).
SERIES = (
    ("eo-series", "04. Band Stacking + Clipping.ipynb", "config_nb.ipynb"),
    ("h3-rasterize", "h3_rasterize_isobands.ipynb", None),
    ("helios", "03. Analytical Core (COG + STAC).ipynb", "config_nb.ipynb"),
    ("orthomosaic", "03_segment.ipynb", "config_nb.ipynb"),
    ("vapor-eyes", "02_sentinel2_detection.ipynb", "config_nb.ipynb"),
    ("wireless-coverage", "04_tower_viewsheds.ipynb", "config_nb.ipynb"),
    ("xview", "Clipping - xView.ipynb", None),
    ("zarr-weather-features", "01_zarr_weather_features.ipynb", None),
)
# Per-measurement files for the warehouse: M9 is the procedure-warm pair, M11 is dropped (its fused leg is excluded).
W_PERF_UNITS = tuple(f"m{i}" for i in (0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 13))


def head_sha12(cwd: Optional[str] = None) -> str:
    """The first 12 hex digits of the checkout's HEAD. Raises ValueError outside a git checkout."""
    import subprocess

    try:
        res = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ValueError(
            f"cannot read the checkout's HEAD ({type(exc).__name__}); pass --volume-dir"
        ) from exc
    sha = res.stdout.strip()
    if res.returncode != 0 or not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise ValueError(
            "cannot read the checkout's HEAD (not a git checkout); pass --volume-dir"
        )
    return sha[:12]


def default_volume_dir(
    volroot: str = DEFAULT_VOLROOT, sha12: Optional[str] = None
) -> str:
    """The pass's UC folder: ``<volroot>/t28/<sha12>``, one per commit as gbx:uc:tests stages ``uc-it/<sha12>/``.

    A restaged wheel of a new commit lands in a new folder, so an install never relies on UC's cache of a wheel that
    had the same path before."""
    return f"{volroot.rstrip('/')}/t28/{sha12 or head_sha12()}"


@dataclass
class Params:
    catalog: str = DEFAULT_CATALOG
    schema: str = DEFAULT_SCHEMA  # the base name: <schema>_w, _s6, _s6j, _se
    # the folder that holds the env 6 wheel alone: "" means <volroot>/t28/<sha12> of the checkout's HEAD
    volume_dir: str = ""
    jar: str = (
        f"{DEFAULT_VOLROOT}/geobrix/0.6.0/jar/geobrix-0.6.0-jar-with-dependencies.jar"
    )
    volroot: str = (
        DEFAULT_VOLROOT  # where the flat env wheels are staged (gbx:data:stage-wheel --env both)
    )
    out_dir: str = ".superpowers/streamline/t28"
    profile: str = DEFAULT_PROFILE
    ws_root: str = WS_ROOT_PLACEHOLDER
    cluster_id: Optional[str] = (
        None  # the P1 cluster; its notebooks are written only when it is known
    )

    def __post_init__(self) -> None:
        # Where HEAD cannot be read (no checkout, a worktree whose git dir is a host path) the folder stays unset:
        # a caller that builds only statements needs none, and main() refuses to write files without one.
        if not self.volume_dir:
            try:
                self.volume_dir = default_volume_dir(self.volroot)
            except ValueError:
                pass

    def schema_of(self, key: str) -> str:
        return f"{self.schema}_{key}"

    def wheel(self, env: int) -> str:
        from databricks.labs.gbx import uc

        return f"{self.volroot.rstrip('/')}/geobrix-{uc._public_version()}+env{env}-py3-none-any.whl"

    def path(self, name: str) -> str:
        return os.path.join(self.out_dir, name)


@dataclass
class File:
    name: str
    kind: str  # "ipynb" | "sql"
    content: str
    purpose: str
    statements: int = 0  # sql: the number of statements
    schema: Optional[str] = None  # sql: the one schema its statements use
    notes: List[str] = field(
        default_factory=list
    )  # what a reader of the file should know


@dataclass
class Run:
    id: str
    title: str
    surface: str
    files: Tuple[str, ...] = ()
    commands: Tuple[str, ...] = ()
    needs: Tuple[str, ...] = ()
    items: Tuple[str, ...] = ()
    wall: str = ""


# ---- statement files ---------------------------------------------------------------------------------------------


def _q(*parts: str) -> str:
    from databricks.labs.gbx.uc.ddl import quote_ident

    kinds = ("catalog", "schema")
    return ".".join(quote_ident(k, v) for k, v in zip(kinds, parts))


def _header(catalog: str, schema: str) -> List[Tuple[str, str]]:
    """The first statements of every warehouse file: the scratch schema and the ``SELECT 1`` floor."""
    return [
        ("schema:create", f"CREATE SCHEMA IF NOT EXISTS {_q(catalog, schema)}"),
        ("floor:select1", "SELECT 1"),
    ]


def _sql_file(name: str, pairs, purpose: str, schema: str) -> File:
    from databricks.labs.gbx.bench import oracles

    return File(name, "sql", oracles.uc_sql_text(pairs), purpose, len(pairs), schema)


def _install_pairs(p: Params, schema: str) -> List[Tuple[str, str]]:
    from databricks.labs.gbx import uc

    stmts = uc.install(
        p.catalog, schema, p.volume_dir, dry_run=True, jar=p.jar, procedures=True
    )
    out = []
    for i, s in enumerate(stmts):
        m = re.match(r"CREATE OR REPLACE (FUNCTION|PROCEDURE) (\S+?)[( \n]", s)
        out.append(
            (f"install:{i:03d}:{m.group(2).split('.')[-1] if m else 'statement'}", s)
        )
    return out


def _slice_pairs(
    p: Params, schema: str, tables: Sequence[str], notes: List[str]
) -> List[Tuple[str, str]]:
    from databricks.labs.gbx.bench import oracles, probes

    with tempfile.TemporaryDirectory(prefix="t28-fixtures-") as d:
        pairs = oracles.uc_sql(probes.uc_slice_cases(tables), p.catalog, schema, d)
    # A virtual-tile row names a fixture path of this machine. The function refuses the tile before it opens the path,
    # so the path only has to be stable: it is the same text on every machine.
    pairs = [(cid, sql.replace(d, FIXTURE_PATH)) for cid, sql in pairs]
    stray = [cid for cid, sql in pairs if FIXTURE_PATH in sql and "virtual" not in cid]
    if (
        stray
    ):  # a row that is not a refusal and names it reads a file the warehouse does not have: the answer is the finding
        notes.append(
            f"{len(stray)} rows name the fixture path {FIXTURE_PATH} though they are no virtual-tile refusal: {stray}"
        )
    return pairs


def _proc_warm_files(p: Params, schema: str) -> List[File]:
    from databricks.labs.gbx.bench import uc_perf

    plan = uc_perf.proc_warm_plan(p.catalog, schema)
    calls = list(plan["setup"])
    for i in (1, 2, 3):
        calls += [
            (f"drop_tgt_{i}", plan["call"][0][1]),
            (f"call_{i}", plan["call"][1][1]),
        ]
    calls += list(plan["create"])
    idle = [
        ("drop_tgt_idle", plan["call"][0][1]),
        ("call_after_idle", plan["call"][1][1]),
    ] + list(plan["drops"])
    head = _header(p.catalog, schema)
    return [
        _sql_file(
            "w_m9_calls.sql",
            head + [(f"m9:{c}", s) for c, s in calls],
            "M9: the polygons-to-BNG CALL three times back to back, then the two target-creation forms",
            schema,
        ),
        _sql_file(
            "w_m9_idle.sql",
            head + [(f"m9:{c}", s) for c, s in idle],
            "M9: the same CALL after a 10 minute idle gap, then drop the M9 tables (start 10 min after w_m9_calls)",
            schema,
        ),
    ]


def _cleanup_pairs(p: Params) -> List[Tuple[str, str]]:
    pairs = [
        (
            f"drop_schema:{k}",
            f"DROP SCHEMA IF EXISTS {_q(p.catalog, p.schema_of(k))} CASCADE",
        )
        for k in SCHEMA_KEYS
    ]
    return pairs + [
        (
            "left:probe_routines",
            f"SELECT count(*) AS n FROM `{p.catalog}`.information_schema.routines "
            f"WHERE routine_schema LIKE '{p.schema}!_%' ESCAPE '!' AND routine_name LIKE 'gbx!_mvp!_%' ESCAPE '!'",
        )
    ]


# ---- notebooks -----------------------------------------------------------------------------------------------------


def _calls_nb(path: str, calls, wheel: str, extras: str = "") -> str:
    from databricks.labs.gbx.bench import probes

    with tempfile.TemporaryDirectory() as d:
        f = os.path.join(d, "nb.ipynb")
        probes.write_calls_notebook(f, calls, wheel, extras)
        return open(f).read()


def _cells_nb(cells: Sequence[str], wheel: str) -> str:
    from databricks.labs.gbx.bench import probes

    with tempfile.TemporaryDirectory() as d:
        f = os.path.join(d, "nb.ipynb")
        probes.write_cells_notebook(f, cells, wheel)
        return open(f).read()


# ---- command lines -------------------------------------------------------------------------------------------------


def _nb_cmd(
    p: Params, nb: str, ws: str, minutes: int, log: str, env: int = 6, extra: str = ""
) -> str:
    wheel = f" --wheel {p.wheel(env)}" if env != 6 else ""
    return (
        f"bash scripts/commands/gbx-test-notebooks-serverless.sh --notebook {shlex.quote(p.path(nb))} --env-version {env}{wheel}"
        f"{extra} --harvest-outputs --profile {p.profile} --ws-dir {p.ws_root}/{ws} --timeout-min {minutes} --log {log}"
    )


def _cluster_cmd(
    p: Params, nb_path: str, ws: str, access: str, log: str, minutes: int = 90
) -> str:
    return (
        f"bash scripts/commands/gbx-test-notebooks-serverless.sh --new-cluster 19.x-scala2.13 --jar {p.jar} "
        f"--access-mode {access} --node-type m5d.xlarge --num-workers 1 --notebook {shlex.quote(nb_path)} "
        f"--wheel {p.wheel(6)} --harvest-outputs --profile {p.profile} --ws-dir {p.ws_root}/{ws} "
        f"--timeout-min {minutes} --log {log}"
    )


def _sql_cmd(
    p: Params,
    name: str,
    log: str,
    repeat: int = 1,
    timeout_s: int = 900,
    dry: bool = False,
) -> str:
    stem = name[: -len(".sql")]
    out = "" if dry else f" --out {shlex.quote(p.path(stem + '.json'))}"
    rep = f" --repeat {repeat}" if repeat != 1 and not dry else ""
    return (
        f"bash scripts/commands/gbx-uc-sql.sh --profile {p.profile} --file {shlex.quote(p.path(name))}{out} "
        f"--catalog {p.catalog}{rep} --timeout-s {timeout_s}{' --dry-run' if dry else ''} --log {log}"
    )


def _judge_cmd(p: Params, stem: str) -> str:
    return (
        f"bash scripts/commands/gbx-bench-uc-probes.sh --catalog {p.catalog} --schema {p.schema} "
        f"--volume-dir {p.volume_dir} --judge {shlex.quote(p.path(stem + '.json'))}"
    )


# ---- the plan ------------------------------------------------------------------------------------------------------


def build(p: Params) -> Tuple[List[File], List[Run]]:
    """Every file and every run of the pass for the parameters ``p``. A run's ``commands`` are the exact lines to start
    it; ``needs`` are its preconditions; ``items`` the checklist ids it answers."""
    from databricks.labs.gbx import manifest, uc
    from databricks.labs.gbx.bench import probes, uc_perf

    w, s6, s6j, se = (p.schema_of(k) for k in SCHEMA_KEYS)
    deps = manifest.load().uc_dependencies
    wheel6, wheel5 = p.wheel(6), p.wheel(5)
    wheel_uri = "file://" + f"{p.volume_dir.rstrip('/')}/{uc._env_wheel_name()}"
    # the fixtures of the file-reading rows: a Volume folder, which the workers see (the driver's /tmp they do not)
    tmp = f"{p.volume_dir.rstrip('/')}/oracle_tmp"
    files: List[File] = []
    runs: List[Run] = []

    def nb(name, content, purpose):
        files.append(File(name, "ipynb", content, purpose))

    def run(id_, title, surface, fs, cmds, needs, items, wall):
        runs.append(
            Run(
                id_,
                title,
                surface,
                tuple(fs),
                tuple(cmds),
                tuple(needs),
                tuple(items),
                wall,
            )
        )

    # -- Serverless env 6 --
    nb("stage_inputs.ipynb", _calls_nb("", [("run_stage_inputs", {"volume_dir": p.volume_dir})], wheel6),
       "write the raster inputs the UC legs read (perf, mechanism and engine tiles) once")  # fmt: skip
    run("S6-0", "stage the raster inputs", "s6", ["stage_inputs.ipynb"],
        [_nb_cmd(p, "stage_inputs.ipynb", "t28-stage", 60, "t28-s6-stage.log")],
        ["P-2 P-3 P-5: the env 6 wheel staged, the UC folder holds the wheel alone"], ["inputs of W-B, W-C, W-E, S6-D, S6-F"],
        "10-25 min")  # fmt: skip
    reg = [("run_register_all", {"fromfile_dir": f"{p.volume_dir.rstrip('/')}/fromfile_tiles"}),
           ("run_family_oracles", {"tmpdir": tmp})]  # fmt: skip
    nb(
        "register_s6.ipynb",
        _calls_nb("", reg, wheel6),
        "register(spark) over all names, the fromfile 64 MiB cap check, every oracle table at the manager's level",
    )
    run("S6-A", "register and oracles", "s6", ["register_s6.ipynb"],
        [_nb_cmd(p, "register_s6.ipynb", "t28-register", 90, "t28-s6-register.log")],
        ["P-2 P-3: the env 6 wheel staged", "no schema"], ["S6-A1", "S6-A2", "S6-A3 (report lines)", "S6-A4"], "30-50 min")  # fmt: skip
    slice_args = {"catalog": p.catalog, "volume_dir": p.volume_dir, "tmpdir": tmp}
    nb("uc_slice_s6j.ipynb", _calls_nb("", [("run_uc_slice", {**slice_args, "schema": s6j, "jar": p.jar})], wheel6),
       "the UC slice with the JAR named: Scala functions for the JVM-only BNG rows")  # fmt: skip
    run("S6-B", "UC slice, Scala form", "s6", ["uc_slice_s6j.ipynb"],
        [_nb_cmd(p, "uc_slice_s6j.ipynb", "t28-slice-j", 120, "t28-s6-slice-jar.log")],
        ["P-2 P-3 P-4 P-5", "a FRESH session (the notebook is its own run)"],
        ["S6-B1..B9 (B2-B6 and B8 are rows of the slice; B7 and B9 are units f8 and f9 of S6-F)"], "35-60 min")  # fmt: skip
    nb("uc_slice_s6.ipynb", _calls_nb("", [("run_uc_slice", {**slice_args, "schema": s6})], wheel6),
       "the UC slice with no JAR: the Python-only form")  # fmt: skip
    run("S6-C", "UC slice, Python-only form", "s6", ["uc_slice_s6.ipynb"],
        [_nb_cmd(p, "uc_slice_s6.ipynb", "t28-slice", 90, "t28-s6-slice.log")],
        ["P-2 P-3 P-5", "the schema is kept for S6-D, S6-E, S6-F"], ["S6-C1"], "25-45 min")  # fmt: skip
    with tempfile.TemporaryDirectory(prefix="t28-perf-") as d:
        written = uc_perf.write_uc_perf_notebooks(
            d, wheel6, p.catalog, s6, p.volume_dir, p.jar
        )
        perf_nb, warm_nb = (open(written[k]).read() for k in ("uc_perf", "proc_warm"))
    nb(
        "uc_perf.ipynb",
        perf_nb,
        "M0-M8 and M10-M13 on Serverless (M9 is the next file)",
    )
    nb(
        "uc_proc_warm.ipynb",
        warm_nb,
        "M9: the procedure CALL cold, warm, after an idle gap; two target-creation forms",
    )
    run("S6-D", "UC performance", "s6", ["uc_perf.ipynb"],
        [_nb_cmd(p, "uc_perf.ipynb", "t28-perf", 240, "t28-s6-perf.log")],
        ["S6-C done and its schema kept (the product UC functions are installed)", "S6-0 done (tiles)"],
        ["S6-D1..D12"], "90-180 min")  # fmt: skip
    run("S6-E", "procedure CALL warm vs cold", "s6", ["uc_proc_warm.ipynb"],
        [_nb_cmd(p, "uc_proc_warm.ipynb", "t28-procwarm", 90, "t28-s6-procwarm.log")],
        ["S6-C done and its schema kept"], ["S6-E1"], "20-35 min")  # fmt: skip
    nb(
        "mech_s6.ipynb",
        _calls_nb(
            "",
            [
                (
                    "run_t28_mechanisms",
                    {"catalog": p.catalog, "schema": s6, "volume_dir": p.volume_dir},
                )
            ],
            wheel6,
        ),
        "ten workspace mechanism checks (temporary function, TVF stub, projection TVF, GEOMETRY array, procedure temp table, read_files, install time, ESCAPE, merge at size)",
    )
    run("S6-F", "workspace mechanism checks", "s6", ["mech_s6.ipynb"],
        [_nb_cmd(p, "mech_s6.ipynb", "t28-mech", 60, "t28-s6-mech.log")],
        ["S6-C done and its schema kept (units f7, f8 and f10 call product functions)", "S6-0 done (tiles)"],
        ["S6-F1..F7", "S6-B7", "S6-B9", "W-A12 on S6"], "15-30 min")  # fmt: skip
    nb(
        "m6_s6.ipynb",
        _calls_nb(
            "",
            [
                (
                    "run_m6",
                    {"catalog": p.catalog, "schema": se, "volume_dir": p.volume_dir},
                )
            ],
            wheel6,
        ),
        "M-6: a UC Python function whose handler runs the slope engine on the 1 MiB tile (Serverless leg)",
    )
    run("S6-G", "engine probes on Serverless env 6", "s6", ["m6_s6.ipynb"],
        [_nb_cmd(p, "m6_s6.ipynb", "t28-m6", 30, "t28-m6-s6.log"),
         f"bash scripts/commands/gbx-bench-cluster.sh --levels SE --levels-only --levels-scales 1 --serverless --env-version 6 --profile {p.profile} --run-id t28-se-s6 --log t28-se-s6.log"],
        ["P-2 P-3 P-5", "S6-0 done (the 1 MiB tile)", f"the M-6 notebook creates schema {p.catalog}.{se}"],
        ["S6-G1 (the notebook)", "S6-G2 (the bench-cluster line)"], "15 min + 20-40 min")  # fmt: skip
    hcmds = []
    for series, nbk, dep in SERIES:
        dep_arg = f' --dep-notebook "examples/notebooks/{series}/{dep}"' if dep else ""
        hcmds.append(
            f'bash scripts/commands/gbx-test-notebooks-serverless.sh --notebook "examples/notebooks/{series}/{nbk}"{dep_arg} '
            f"--env-version 6 --harvest-outputs --profile {p.profile} --ws-dir {p.ws_root}/t28-nb-{series} --timeout-min 60 --log t28-nb-{series}.log"
        )
    run(
        "S6-H",
        "series notebooks",
        "s6",
        [],
        hcmds,
        ["P-2 P-3"],
        ["S6-H1..H8"],
        "8 x 10-25 min",
    )

    # -- the warehouse --
    head_w = _header(p.catalog, w)
    files.append(_sql_file("w_install.sql", head_w + _install_pairs(p, w),
                           "the gbx.uc.install statements (functions, the Scala forms on the named JAR, procedures)", w))  # fmt: skip
    for name, tables, purpose in (
        (
            "w_slice.sql",
            probes.UC_CORE_TABLES,
            "the UC rows of the grid and vector tables, one SELECT per case",
        ),
        (
            "w_tierb.sql",
            probes.UC_TIER_B_TABLES,
            "the UC rows of the raster, cell, aggregate and vector tables, one SELECT per case",
        ),
    ):
        notes: List[str] = []
        files.append(
            _sql_file(name, head_w + _slice_pairs(p, w, tables, notes), purpose, w)
        )
        files[-1].notes = notes
    run("W-A", "warehouse install and UC slice", "w", ["w_install.sql", "w_slice.sql", "w_tierb.sql"],
        [_sql_cmd(p, "w_install.sql", "t28-w-install.log", dry=True), _sql_cmd(p, "w_install.sql", "t28-w-install.log", timeout_s=900),
         _sql_cmd(p, "w_slice.sql", "t28-w-slice.log"), _judge_cmd(p, "w_slice"),
         _sql_cmd(p, "w_tierb.sql", "t28-w-tierb.log"), _judge_cmd(p, "w_tierb")],
        ["P-4 P-5: the wheel and the JAR staged", "the warehouse id in examples/tests/databricks_cluster_config.env"],
        ["W-A1..A15 (A12-A15 are units of w_mech.sql)"], "see the statement counts")  # fmt: skip
    perf_files = []
    for u in W_PERF_UNITS:
        pairs = uc_perf.uc_perf_sql(p.catalog, w, p.jar, f"{p.volume_dir.rstrip('/')}/{uc._env_wheel_name()}",
                                    tile_dir=f"{p.volume_dir.rstrip('/')}/perf_tiles", only=[u])  # fmt: skip
        files.append(
            _sql_file(
                f"w_{u}.sql",
                _header(p.catalog, w) + pairs,
                f"{u.upper()} on the warehouse",
                w,
            )
        )
        perf_files.append(f"w_{u}.sql")
    run("W-B", "warehouse UC performance", "w", perf_files,
        [_sql_cmd(p, f, f"t28-w-{f[2:-4]}.log", repeat=3, timeout_s=1800) for f in perf_files],
        ["W-A done (product UC functions in the schema)", "S6-0 done (perf tiles)", "NOTHING else on the warehouse while these run"],
        ["W-B1..B12"], "2-4 h")  # fmt: skip
    mech = probes.mech_statements(
        p.catalog, w, f"{p.volume_dir.rstrip('/')}/mech_tiles", wheel_uri, deps, "w"
    )
    files.append(
        _sql_file(
            "w_mech.sql",
            head_w + mech,
            "the mechanism checks that run on the warehouse (units f3-f10)",
            w,
        )
    )
    run("W-C", "warehouse mechanism checks", "w", ["w_mech.sql"],
        [_sql_cmd(p, "w_mech.sql", "t28-w-mech.log"), _judge_cmd(p, "w_mech")],
        ["W-A done", "S6-0 done (mechanism tiles)"], ["W-C1..C4", "W-A8..A15"], "15-30 min")  # fmt: skip
    files += _proc_warm_files(p, w)
    run("W-D", "warehouse procedure warm vs cold", "w", ["w_m9_calls.sql", "w_m9_idle.sql"],
        [_sql_cmd(p, "w_m9_calls.sql", "t28-w-m9-calls.log"), "# wait 10 minutes, then:", _sql_cmd(p, "w_m9_idle.sql", "t28-w-m9-idle.log")],
        ["W-A done (the procedures are installed)"], ["W-D1"], "25-40 min")  # fmt: skip
    m6 = probes.m6_statements(
        p.catalog, se, f"{p.volume_dir.rstrip('/')}/m6_tiles", wheel_uri, deps
    )
    files.append(
        _sql_file(
            "w_engine_m6.sql",
            _header(p.catalog, se) + m6,
            "M-6: the engine probe function, one call on the 1 MiB tile, the DROP",
            se,
        )
    )
    run("W-E", "engine M-6, warehouse", "w", ["w_engine_m6.sql"],
        [_sql_cmd(p, "w_engine_m6.sql", "t28-m6-warehouse.log"), _judge_cmd(p, "w_engine_m6")],
        ["S6-0 done (the 1 MiB tile)"], ["W-E1"], "5-10 min")  # fmt: skip

    # -- classic clusters --
    reg_ded = [
        ("run_register_all", {}),
        ("run_family_oracles", {"tmpdir": tmp, "level": "L1"}),
    ]
    nb(
        "register_ded.ipynb",
        _calls_nb("", reg_ded, wheel6, "vizx"),
        "register at L1 where the table says, and the L1 legs of the oracles",
    )
    nb(
        "mixed_lang_ded.ipynb",
        _cells_nb(probes.mixed_language_cells(), wheel6),
        "register in Python, call by name from a %scala cell",
    )
    run("D-A", "dedicated DBR 19 job cluster, JAR as a library", "ded", ["register_ded.ipynb", "mixed_lang_ded.ipynb"],
        [_cluster_cmd(p, p.path("register_ded.ipynb"), "t28-ded", "dedicated", "t28-ded.log"),
         _cluster_cmd(p, p.path("mixed_lang_ded.ipynb"), "t28-ded-mix", "dedicated", "t28-ded-mix.log", 60),
         f"bash scripts/commands/gbx-bench-cluster.sh --levels SE --levels-only --levels-scales 1 --new-cluster --access-mode dedicated --num-workers 4 --node-type m5d.2xlarge --on-demand --spark-version 19.x-scala2.13 --run-id t28-se-ded --profile {p.profile} --log t28-se-ded.log",
         "bash scripts/commands/gbx-test-python-dbr.sh --log t28-dbr-docs.log",
         _cluster_cmd(p, "examples/tests/classic_proj_grid_validation.ipynb", "t28-ded-proj", "dedicated", "t28-ded-proj.log", 60)],
        ["P-2 P-3 P-4", "no leftover cluster; the job clusters end with their runs"], ["D-A items 1 to 6"], "60-100 min")  # fmt: skip
    if p.cluster_id:
        for phase in ("install", "after"):
            what = {
                "install": "P1 install phase: capabilities, the JAR as a library on the running cluster, capabilities again, then the restart",
                "after": "P1 after phase: on the restarted cluster, the library status and capabilities",
            }[phase]
            nb(
                f"p1_{phase}.ipynb",
                _calls_nb(
                    "",
                    [
                        (
                            "run_p1",
                            {
                                "cluster_id": p.cluster_id,
                                "jar_path": p.jar,
                                "phase": phase,
                            },
                        )
                    ],
                    wheel6,
                ),
                what,
            )
        run("D-B", "probe P1 (JAR install route)", "ded", ["p1_install.ipynb", "p1_after.ipynb"],
            [f"databricks clusters get {p.cluster_id} --profile {p.profile}  # snapshot to .superpowers/streamline/clusters/t28-p1-before.json",
             _nb_cmd(p, "p1_install.ipynb", "t28-p1", 40, "t28-p1-install.log", extra=f" --existing-cluster-id {p.cluster_id}"),
             "# the install run ends when the cluster restarts (its RESULT line is in the log); then:",
             _nb_cmd(p, "p1_after.ipynb", "t28-p1", 40, "t28-p1-after.log", extra=f" --existing-cluster-id {p.cluster_id}")],
            ["P-11: a running dedicated cluster WITHOUT the JAR; its id is --cluster-id"], ["D-B1"], "25-40 min")  # fmt: skip
    else:
        run("D-B", "probe P1 (JAR install route)", "ded", [], [],
            ["P-11: create the JAR-less dedicated cluster, then rerun this command with --cluster-id <id> to write p1_install.ipynb and p1_after.ipynb"],
            ["D-B1"], "25-40 min")  # fmt: skip
    reg_std = [
        ("run_register_all", {}),
        (
            "run_family_oracles",
            {"tmpdir": tmp, "level": "L2", "tables": ["GRID_SCALAR"]},
        ),
    ]
    nb(
        "register_std.ipynb",
        _calls_nb("", reg_std, wheel6, "vizx"),
        "register on standard access (no _jvm), GRID_SCALAR at L2",
    )
    nb(
        "temp_share_std.ipynb",
        _cells_nb(probes.temp_share_cells(), wheel6),
        "do Python and Scala cells share temporary functions?",
    )
    nb(
        "mixed_lang_std.ipynb",
        _cells_nb(probes.mixed_language_cells(), wheel6),
        "the mixed-language route on standard access",
    )
    run("STD-A", "standard DBR 19 job cluster", "std", ["register_std.ipynb", "temp_share_std.ipynb", "mixed_lang_std.ipynb"],
        [_cluster_cmd(p, p.path("register_std.ipynb"), "t28-std", "standard", "t28-std.log"),
         _cluster_cmd(p, p.path("temp_share_std.ipynb"), "t28-std-share", "standard", "t28-std-share.log", 40),
         _cluster_cmd(p, p.path("mixed_lang_std.ipynb"), "t28-std-mix", "standard", "t28-std-mix.log", 40),
         f"bash scripts/commands/gbx-bench-cluster.sh --levels SE --levels-only --levels-scales 1 --new-cluster --access-mode standard --num-workers 4 --node-type m5d.2xlarge --on-demand --spark-version 19.x-scala2.13 --run-id t28-se-std --profile {p.profile} --log t28-se-std.log"],
        ["P-2 P-3 P-4: a metastore admin must allowlist the JAR path for standard access"], ["STD-A1..A5"], "40-70 min")  # fmt: skip
    nb(
        "register_s5.ipynb",
        _calls_nb(
            "",
            [("run_register_all", {}), ("run_family_oracles", {"tmpdir": tmp})],
            wheel5,
        ),
        "env 5: register and the oracles through session SQL",
    )
    run("S5-A", "Serverless env 5", "s5", ["register_s5.ipynb"],
        [_nb_cmd(p, "register_s5.ipynb", "t28-s5", 90, "t28-s5-register.log", env=5)],
        ["P-2 P-3: the env 5 wheel staged"], ["S5-A1", "S5-A2"], "30-50 min")  # fmt: skip
    files.append(
        _sql_file(
            "w_cleanup.sql",
            _cleanup_pairs(p),
            "after every run: drop the four scratch schemas, count the probe routines left",
            w,
        )
    )
    run("END", "clean up", "w", ["w_cleanup.sql"],
        [_sql_cmd(p, "w_cleanup.sql", "t28-w-cleanup.log")],
        ["every other run is done; this DROPS the four scratch schemas with CASCADE"], ["post-run step 4"], "2 min")  # fmt: skip
    run(
        "X-1",
        "DCS cluster leg",
        "held",
        [],
        [],
        ["held: never touch cluster 1016-152458-i5qbli6q without an explicit release"],
        ["X-1"],
        "-",
    )
    return files, runs


# ---- selection, validation, judging -------------------------------------------------------------------------------


def select(
    files: List[File], runs: List[Run], only: Optional[Sequence[str]]
) -> Tuple[List[File], List[Run]]:
    """The runs named in ``only`` (ids, any case) and the files they use; ValueError for an id the plan has not."""
    if not only:
        return files, runs
    want = {o.strip().upper() for o in only if o.strip()}
    known = {r.id.upper() for r in runs}
    if want - known:
        raise ValueError(
            f"--only names no run: {sorted(want - known)}; the runs are {sorted(known)}"
        )
    chosen = [r for r in runs if r.id.upper() in want]
    names = {f for r in chosen for f in r.files}
    return [f for f in files if f.name in names], chosen


_LITERAL = re.compile(r"'(?:[^'\\]|\\.|'')*'")
_BODY = re.compile(r"\$\$.*?\$\$", re.S)


def code_of(sql: str) -> str:
    """The statement without its ``$$`` bodies and quoted strings."""
    return _LITERAL.sub("''", _BODY.sub("$$$$", sql))


def validate_notebook(content: str) -> List[str]:
    """Why a notebook is wrong, or []: valid JSON, nbformat 4, Python cells compile, a probe call binds to its signature."""
    from databricks.labs.gbx.bench import probes

    bad: List[str] = []
    try:
        nb = json.loads(content)
    except ValueError as exc:
        return [f"not JSON: {exc}"]
    if (
        nb.get("nbformat") != 4
        or not isinstance(nb.get("cells"), list)
        or not nb["cells"]
    ):
        return ["not an nbformat 4 notebook with cells"]
    for i, cell in enumerate(nb["cells"]):
        src = cell.get("source", "")
        if not isinstance(src, str):
            src = "".join(src)
        first = src.lstrip().splitlines()[0] if src.strip() else ""
        if first.startswith("%scala"):
            if src.count("{") != src.count("}") or src.count("(") != src.count(")"):
                bad.append(f"cell {i}: unbalanced brackets in the Scala cell")
            continue
        if first.startswith("%pip") or first.startswith("%"):
            continue
        try:
            compile(src, f"<cell {i}>", "exec")
        except SyntaxError as exc:
            bad.append(f"cell {i}: {exc}")
        calls = re.findall(r"probes\.(\w+)\(spark, \*\*(\{.*?\})\), default=str", src)
        if src.count("probes.") != len(calls):
            bad.append(
                f"cell {i}: a probe call is not in the form write_calls_notebook writes"
            )
        for name, literal in calls:
            fn = getattr(probes, name, None)
            if fn is None:
                bad.append(f"cell {i}: probes.{name} does not exist")
                continue
            try:
                kwargs = ast.literal_eval(
                    literal
                )  # the dict literal write_calls_notebook printed with repr
                inspect.signature(fn).bind(None, **kwargs)
            except (TypeError, ValueError, SyntaxError) as exc:
                bad.append(f"cell {i}: probes.{name} does not take {literal}: {exc}")
    return bad


def _known_routines() -> set:
    from databricks.labs.gbx import manifest, uc

    man = manifest.load()
    names = set(man.functions) | set(man.private)
    names |= {
        re.match(
            r"CREATE OR REPLACE FUNCTION \S*?\.?(gbx_\w+)", s.replace("`", "")
        ).group(1)
        for s in uc.install(
            "c", "s", "/Volumes/c/s/v/f", dry_run=True, jar="/Volumes/c/s/v/j.jar"
        )
        if s.startswith("CREATE OR REPLACE FUNCTION")
    }
    from databricks.labs.gbx.uc import procedures

    return names | set(procedures.RECIPES)


def validate_sql(
    text: str, catalog: str, schema: str, unqualified_ok: Sequence[str] = ()
) -> List[str]:
    """Why a statement file is wrong, or []: it round-trips through the runner's parser with unique cids, no statement
    is empty, every ``gbx_`` routine is qualified ``catalog.schema`` and exists in the manifest or was created earlier in
    the file (a ``gbx_probe_`` probe), and every Python body compiles."""
    from databricks.labs.gbx.bench import oracles

    pairs = oracles.parse_uc_sql(text)
    bad: List[str] = []
    cids = [c for c, _ in pairs]
    if len(set(cids)) != len(cids):
        bad.append(
            f"duplicate cids: {sorted({c for c in cids if cids.count(c) > 1})[:5]}"
        )
    if not pairs:
        return ["no statements"]
    again = oracles.parse_uc_sql(oracles.uc_sql_text(pairs))
    if again != pairs:
        bad.append("the file does not round-trip through the runner's parser")
    known = _known_routines()
    qual = f"`{catalog}`.`{schema}`."
    created: set = set()
    for cid, sql in pairs:
        if not sql.strip():
            bad.append(f"{cid}: empty statement")
            continue
        code = code_of(sql)
        for m in re.finditer(r"(?<![\w`])gbx_\w+", code):
            name, start = m.group(0), m.start()
            qualified = code[max(0, start - len(qual)) : start] == qual
            before = code[: start - (len(qual) if qualified else 0)]
            is_ddl = re.search(
                r"(?:FUNCTION|PROCEDURE|TABLE)(?: IF (?:NOT )?EXISTS)?\s*$", before
            )
            if name.startswith("gbx_probe_"):
                if is_ddl:
                    created.add(name)
                elif name not in created and code[m.end() :].lstrip().startswith("("):
                    bad.append(f"{cid}: {name} is called before the file creates it")
            elif name not in known:
                bad.append(f"{cid}: {name} is no function of the manifest")
            if name not in unqualified_ok and not qualified:
                bad.append(f"{cid}: {name} is not qualified {qual}")
        for body in _BODY.findall(sql):
            src = body[2:-2].strip("\n")
            try:
                compile(
                    "def _f(*a, **k):\n"
                    + "".join("    " + ln + "\n" for ln in src.splitlines()),
                    cid,
                    "exec",
                )
            except SyntaxError as exc:
                bad.append(f"{cid}: the Python body does not compile: {exc}")
    return bad


def validate(files: Sequence[File], p: Params) -> Dict[str, List[str]]:
    """``{file name: [problems]}`` for the files that have any."""
    out = {}
    for f in files:
        probs = (
            validate_notebook(f.content)
            if f.kind == "ipynb"
            else validate_sql(
                f.content,
                p.catalog,
                f.schema or p.schema_of("w"),
                unqualified_ok=UNQUALIFIED_OK,
            )
        )
        if probs:
            out[f.name] = probs[:20]
    return out


def judge(path: str, p: Params) -> Tuple[List[str], str]:
    """Read a results file (``gbx-uc-sql --out``) back: ``(failures, summary)``. The file's stem picks the judge."""
    from databricks.labs.gbx.bench import oracles, probes

    stem = os.path.splitext(os.path.basename(path))[0]
    results = oracles.load_uc_results(path)
    w = p.schema_of("w")
    if stem in ("w_slice", "w_tierb"):
        tables = probes.UC_CORE_TABLES if stem == "w_slice" else probes.UC_TIER_B_TABLES
        cases = probes.uc_slice_cases(tables)
        mine = {c.cid for c in cases}
        sub = {k: v for k, v in results.items() if k in mine}
        with tempfile.TemporaryDirectory(prefix="t28-judge-") as d:
            bad = oracles.check_uc_results(cases, sub, d)
        return (
            bad,
            f"{stem}: {len(sub)} of {len(mine)} cases have a result, {len(bad)} failures",
        )
    if stem == "w_mech":
        from databricks.labs.gbx import manifest, uc

        wheel_uri = "file://" + f"{p.volume_dir.rstrip('/')}/{uc._env_wheel_name()}"
        done = probes.check_mech_results(
            results,
            p.catalog,
            w,
            f"{p.volume_dir.rstrip('/')}/mech_tiles",
            wheel_uri,
            manifest.load().uc_dependencies,
        )
        bad = [
            f"{u}: {why}"
            for u, r in done.items()
            for why in (r["failures"].values() or [])
        ]
        return (
            bad,
            f"{stem}: {sum(r['ok'] for r in done.values())} of {len(done)} units ok",
        )
    if stem == "w_engine_m6":
        entry = results.get("m6:call") or {}
        ok = probes.m6_ok(entry.get("value"))
        return (
            [] if ok else [f"m6:call: {entry.get('error') or entry.get('value')}"]
        ), f"{stem}: {'pass' if ok else 'FAIL'}"
    return (
        [],
        f"{stem}: no judge for this file; read the values and wall times in {path}",
    )


# ---- output --------------------------------------------------------------------------------------------------------


def manifest_md(files: Sequence[File], runs: Sequence[Run], p: Params) -> str:
    by = {f.name: f for f in files}
    lines = [
        "# Task 28 files",
        "",
        f"Catalog `{p.catalog}`; schemas "
        + ", ".join(f"`{p.schema_of(k)}`" for k in SCHEMA_KEYS)
        + f"; UC folder `{p.volume_dir}`; JAR `{p.jar}`.",
        "",
    ]
    for r in runs:
        lines += [f"## {r.id} - {r.title} ({r.surface}, {r.wall})", ""]
        for n in r.files:
            f = by.get(n)
            if f:
                count = f" ({f.statements} statements)" if f.kind == "sql" else ""
                lines.append(f"- `{n}`{count}: {f.purpose}")
                lines += [f"  - note: {x}" for x in f.notes]
        if r.needs:
            lines += ["", "Needs:"] + [f"- {x}" for x in r.needs]
        if r.commands:
            lines += ["", "Commands:", "```"] + list(r.commands) + ["```"]
        lines.append("")
    return "\n".join(lines)


def write(out_dir: str, files: Sequence[File], runs: Sequence[Run], p: Params) -> None:
    os.makedirs(out_dir, exist_ok=True)
    for f in files:
        with open(os.path.join(out_dir, f.name), "w") as fh:
            fh.write(f.content)
    with open(os.path.join(out_dir, "manifest.json"), "w") as fh:
        json.dump(
            {
                "files": {
                    f.name: {
                        "kind": f.kind,
                        "purpose": f.purpose,
                        "statements": f.statements,
                        "schema": f.schema,
                        "notes": f.notes,
                    }
                    for f in files
                },
                "runs": [r.__dict__ for r in runs],
            },
            fh,
            indent=1,
        )
    with open(os.path.join(out_dir, "manifest.md"), "w") as fh:
        fh.write(manifest_md(files, runs, p))


_GEOM_TYPE = re.compile(r"GEOMETRY\((?:ANY|\d+)\)", re.I)
_TVF_CALL = re.compile(
    r"(\b(?:FROM|JOIN|LATERAL)\s+|,\s*)`[^`]+`\.`[^`]+`\.(gbx_\w+\s*\()", re.I
)


def spark_view(sql: str) -> str:
    """The statement as open-source Spark can parse it. Two Databricks forms are rewritten, nothing else: a GEOMETRY(n)
    type becomes BINARY and a table function in FROM / LATERAL loses its catalog.schema qualifier.
    """
    return _TVF_CALL.sub(r"\1\2", _GEOM_TYPE.sub("BINARY", sql))


def parse_locally(files: Sequence[File]) -> Dict[str, str]:
    """``{file:cid: parser error}`` for the statements Spark's own SQL parser rejects (after :func:`spark_view`). The
    Databricks-only DDL (``LANGUAGE PYTHON``, ``ENVIRONMENT``, ``CREATE PROCEDURE``) is among them: a human reads it.
    """
    from pyspark.sql import SparkSession

    from databricks.labs.gbx.bench import oracles

    spark = SparkSession.builder.master("local[1]").appName("t28-parse").getOrCreate()
    parser = spark._jsparkSession.sessionState().sqlParser()
    bad = {}
    for f in files:
        if f.kind != "sql":
            continue
        for cid, sql in oracles.parse_uc_sql(f.content):
            try:
                parser.parsePlan(spark_view(sql))
            except (
                Exception
            ) as exc:  # noqa: BLE001 - the parser's message is the answer
                first = next(
                    (ln for ln in str(exc).splitlines() if "[" in ln and "]" in ln),
                    str(exc),
                )
                bad[f"{f.name}:{cid}"] = " ".join(first.split())[:160]
    return bad


def resolve_ws_root(profile: str) -> str:
    """The workspace folder the runners use: /Users/<current user>/jobs/geobrix.

    Same root as examples/tests/_job_submit.py (jobs_root); python/geobrix/src cannot import examples/tests. Raises RuntimeError when the SDK or a
    session is not available, so the caller can ask for --ws-root.
    """
    try:
        from databricks.sdk import WorkspaceClient

        me = WorkspaceClient(profile=profile).current_user.me().user_name
    except Exception as exc:  # no SDK, no profile, or no valid session
        raise RuntimeError(
            f"cannot resolve the current user for profile {profile!r} ({type(exc).__name__}); pass --ws-root"
        ) from exc
    if not me:
        raise RuntimeError("the session has no user name; pass --ws-root")
    return f"/Users/{me}/jobs/geobrix"


def _ws_root_of(a: argparse.Namespace) -> str:
    if a.ws_root is not None:
        return a.ws_root
    if a.judge:
        return WS_ROOT_PLACEHOLDER  # judging prints no commands, so it needs no session
    return resolve_ws_root(a.profile)


def _params_of(a: argparse.Namespace) -> "Params":
    """The pass's parameters from the command line; the UC folder defaults to the HEAD commit's (ValueError when
    HEAD cannot be read and no --volume-dir names one)."""
    p = Params(
        a.catalog,
        a.schema,
        a.volume_dir,
        a.jar,
        a.volroot,
        a.out_dir,
        a.profile,
        a.ws_root,
        a.cluster_id,
    )
    if not p.volume_dir:  # Params left it unset: HEAD could not be read; say so
        p.volume_dir = default_volume_dir(p.volroot)
    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        prog="gbx-bench-uc-probes", description=__doc__.split("\n\n")[0]
    )
    ap.add_argument("--catalog", default=DEFAULT_CATALOG)
    ap.add_argument(
        "--schema",
        default=DEFAULT_SCHEMA,
        help="base name of the four scratch schemas (default %(default)s)",
    )
    ap.add_argument(
        "--volume-dir",
        default="",
        help="the folder that holds the env 6 wheel alone (default <volroot>/t28/<sha12 of HEAD>)",
    )
    ap.add_argument("--jar", default=Params.jar)
    ap.add_argument("--volroot", default=DEFAULT_VOLROOT)
    ap.add_argument("--out-dir", default=Params.out_dir)
    ap.add_argument("--profile", default=DEFAULT_PROFILE)
    ap.add_argument(
        "--ws-root",
        default=None,
        help="workspace folder in the printed commands (default /Users/<current user>/jobs/geobrix, resolved from --profile)",
    )
    ap.add_argument("--cluster-id", default=None)
    ap.add_argument("--only", default="", help="comma-separated run ids, e.g. S6-C,W-A")
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="build and check everything; write no file",
    )
    ap.add_argument(
        "--parse",
        action="store_true",
        help="also run Spark's SQL parser over every statement (starts a local Spark)",
    )
    ap.add_argument(
        "--judge",
        default=None,
        help="read a results file written by gbx-uc-sql and judge it",
    )
    a = ap.parse_args(argv)
    try:
        a.ws_root = _ws_root_of(a)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    try:
        p = _params_of(a)
        for kind, value in (("catalog", p.catalog), ("schema", p.schema)):
            from databricks.labs.gbx.uc.ddl import (
                check_jar_path,
                check_volume_dir,
                quote_ident,
            )

            quote_ident(kind, value)
        check_volume_dir(p.volume_dir)
        check_jar_path(p.jar)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    if a.judge:
        bad, summary = judge(a.judge, p)
        print(summary)
        for line in bad[:40]:
            print("  " + line)
        return 1 if bad else 0
    try:
        files, runs = select(*build(p), [x for x in a.only.split(",") if x])
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    problems = validate(files, p)
    if a.parse:
        rejected = parse_locally(files)
        print(
            f"Spark parser: {len(rejected)} statements rejected locally (Databricks-only syntax is expected)"
        )
    if not a.dry_run:
        write(p.out_dir, files, runs, p)
    print(
        f"{'Would write' if a.dry_run else 'Wrote'} {len(files)} files to {p.out_dir}:"
    )
    for f in files:
        print(
            f"  {f.name:28s} {f.statements if f.kind == 'sql' else '':>5} {f.purpose}"
        )
        for x in f.notes:
            print(f"    note: {x}")
    for r in runs:
        print(f"\n== {r.id}  {r.title}  [{r.surface}, {r.wall}]")
        for n in r.needs:
            print(f"   needs: {n}")
        for c in r.commands:
            print(f"   {c}")
    if problems:
        print("\nVALIDATION FAILED:", file=sys.stderr)
        for name, probs in problems.items():
            for line in probs:
                print(f"  {name}: {line}", file=sys.stderr)
        return 1
    print(f"\nvalidated {len(files)} files: ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
