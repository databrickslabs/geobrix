"""Forms + inputs per set of functions (S, G, A, H); each set appends its forms. Inputs are cached Delta tables.

An input builder is ``f(spark, n, corpus, schema) -> table name`` (None when the input cannot
be built here). With ``schema`` it is a Delta table, created once and reused by every form and
run; without one, a temp view. The seeds are fixed, so a form sees the same rows every time.
"""

from __future__ import annotations

import ast
import hashlib
import inspect
import os
import re
import sys
import textwrap
import time
from typing import Callable, Dict, List, Optional

from databricks.labs.gbx._gdal_policy import open_data
from databricks.labs.gbx.bench.clientcap import (
    check_estimate,
    client_dataframe,
    size_of,
)
from databricks.labs.gbx.bench.levels import Form

# Bump when an input recipe (a seed or a shape) changes: it is part of the cached table's name,
# so a stale table from an older recipe is never reused.
RECIPE_VERSION = 2
BASE_ROWS = {
    "bng_points": 1_000_000,
    "invalid_polys": 250_000,
    "tiles_mat": 250,
    "tiles_virt": 250,
    # DataFrame helpers (H): groups of tiles (merge: two offset copies of a corpus tile, combine: two aligned copies)
    # and the corpus tiles on the British National Grid (the others have no BNG cells).
    "tiles_grouped_mat": 250,
    "tiles_aligned_mat": 250,
    "tiles_gb_mat": 250,
    "tiles_gb_virt": 250,
    # Generators (G): BNG polygons that each cover a handful of cells, and a few big tiles (4096 x 4096)
    "bng_polys": 100_000,
    "tiles_big": 4,
    # Aggregates (A): tiles in groups of about 50, and (key, BNG cell, value) rows; each also with half of its rows in one key
    "tiles_grouped": 2000,
    "tiles_grouped_skew": 112,  # half of it is one key: 56 tiles, 896 MiB decoded
    # The same plans as the two above, as virtual tiles (a path each: nothing is held), for the by-reference merge; the
    # family-sized one is the 250 tiles the family sets and the H set read
    "tiles_grouped_virt": 2000,
    "tiles_grouped_virt_skew": 112,
    "tiles_grouped_family_virt": 250,
    # One key of 56 tiles at 1x and 896 at 16x (all of one CRS, so one footprint), and groups of tiles in three CRSs
    "tiles_stack_virt": 56,
    "tiles_mixed_crs_virt": 250,
    "bng_cells": 1_000_000,
    "bng_cells_skew": 1_000_000,
    # DataFrame helpers (H): towers on a DSM of 16 tiles (the tiles are the same at every scale)
    "dsm_towers": 64,
}


# What an input's identity holds, besides its name: the recipe version, the source of the builder that makes it and of the
# helpers every builder shares, and (for a builder that reads the corpus) which tiles the corpus holds. A table whose
# stamp differs is another table: it is built under its own name, the old one is left alone (other runs may be reading
# it), so a changed generator or corpus can never serve a stale input.
_STAMP_HELPERS = ("_tile_column", "_plan_grouped_tiles", "_epsgs", "_epsg_of")
# Planners that only some builders use: their source is part of the stamp of a builder that names them (adding one to
# _STAMP_HELPERS would change the stamp of every input).
_STAMP_PLANNERS = ("_plan_stack_tiles", "_plan_mixed_tiles")
_STAMP_CORPUS: List[str] = [""]  # the corpus of the run (set before an input is built)


def _source_of(obj) -> str:
    """The source of a function or module, as its syntax: comments, docstrings and layout do not count, so a reformat or a
    reworded comment does not make every input a new table. "" when there is no source.
    """
    try:
        src = inspect.getsource(obj)
    except (
        OSError,
        TypeError,
    ):  # no source (a frozen build): the recipe version alone speaks for it
        return ""
    try:
        tree = ast.parse(textwrap.dedent(src))
        for node in ast.walk(tree):
            body = getattr(node, "body", None)
            if (
                isinstance(body, list)
                and body
                and isinstance(body[0], ast.Expr)
                and isinstance(getattr(body[0], "value", None), ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                body[0] = ast.Pass()  # a docstring
        return ast.dump(tree)
    except SyntaxError:
        return src


_CORPUS_FP: Dict[int, tuple] = (
    {}
)  # id(rows list) -> (the list, its fingerprint): the listing is memoized, so is this


def _corpus_fingerprint(corpus: str) -> str:
    """Which tiles the corpus holds: the count and a hash of their names. Computed once per listing."""
    rows = _corpus_rows(corpus) or []
    held = _CORPUS_FP.get(id(rows))
    if held is None or held[0] is not rows:
        fp = (
            f"{len(rows)}:"
            + hashlib.sha1(
                "\n".join(os.path.basename(p) for p in rows).encode()
            ).hexdigest()
        )
        held = _CORPUS_FP[id(rows)] = (rows, fp)
    return held[1]


def input_stamp(builder_src: str, corpus: str = "") -> str:
    """8 hex digits that change when the recipe version, the builder's source or the helpers' source change, and, for a
    builder that reads the corpus rows, when the set of tiles in the corpus changes. (`builder_src` is the builder's
    source as `_source_of` gives it.)"""
    parts = [f"v{RECIPE_VERSION}", builder_src]
    parts += [_source_of(globals()[h]) for h in _STAMP_HELPERS if h in globals()]
    parts += [_source_of(globals()[h]) for h in _STAMP_PLANNERS if h in builder_src]
    if "synth" in builder_src:
        from databricks.labs.gbx.bench import synth

        parts.append(_source_of(synth))
    if "_corpus_rows" in builder_src and corpus:
        parts.append(_corpus_fingerprint(corpus))
    return hashlib.sha1("\n".join(parts).encode()).hexdigest()[:8]


def _stamp_of(frame) -> str:
    return input_stamp(_source_of(frame.f_code), _STAMP_CORPUS[0])


def stamped_name(schema: Optional[str], name: str) -> str:
    """`name` with the stamp of the calling builder appended (a temp view, schema None, keeps its name: it lives one session)."""
    return name if schema is None else f"{name}_{_stamp_of(sys._getframe(1))}"


def _write_input(
    spark, df, full: str, rows: Optional[int], per_file: Optional[int] = None
) -> None:
    """Write `df` as the Delta table `full`, in at least as many files as the compute has slots when `rows` is given.

    A scan task reads whole files (a Parquet row group does not split), so the files an input is written in cap the
    parallelism of every form that reads it. Delta's optimized write packs a few million narrow rows into ONE file: the
    4M-row `bng_points` input had one file and the 16M-row one four, so the BNG scalar's 4x rung ran on one task and the
    16x on four, and its time came out flat from 4x to 16x (the floor too). `maxRecordsPerFile` splits the packed file
    again, so the scan of an input has the compute's slots in tasks at every scale and the time follows the rows.

    `per_file` caps the rows of a file instead: an input of tiles that a pixel-producing UDF reads (a task holds all its
    tiles' outputs in one Arrow batch) is written in files of a few rows, because optimized write packed 1000 virtual
    tiles (13 KB) into ONE file and 250 into files of up to 63 rows: one scan task then held 12 GB of outputs on a
    Serverless 1 GB limit.
    """
    w = df.write.format("delta")
    if per_file:
        w = w.option("maxRecordsPerFile", max(1, int(per_file)))
    elif rows:
        from databricks.labs.gbx.bench.runner import _bench_parallelism

        w = w.option(
            "maxRecordsPerFile", max(1, -(-int(rows) // _bench_parallelism(spark)))
        )
    w.saveAsTable(full)


def _table(
    spark,
    schema: Optional[str],
    name: str,
    build,
    exact: bool = False,
    rows: Optional[int] = None,
    per_file: Optional[int] = None,
) -> str:
    """The input as a table of `schema` (or a temp view): built once under a name that holds its stamp, so every form and
    every run that asks for the same input at the same scale shares it. `exact` names the table as given (a companion
    table of an input that already carries its stamp). `rows` is the row count of an input of narrow rows (a range,
    not a tile per row): it is written in a file per compute slot (`_write_input`); `per_file` writes at most that many rows
    in a file (virtual tiles)."""
    if schema is None:
        build().createOrReplaceTempView(name)
        return name
    if not exact:
        name = f"{name}_{_stamp_of(sys._getframe(1))}"
    full = f"{schema}.{name}"
    if not spark.catalog.tableExists(full):
        t0 = time.perf_counter()
        try:
            _write_input(spark, build(), full, rows, per_file)
            # a tile input of thousands of files takes minutes: say so, so a form cap hit while building is seen as that
            print(
                f"[LEVELS input] {full} built in {time.perf_counter() - t0:.1f} s",
                flush=True,
            )
        except Exception as exc:  # noqa: BLE001
            # Runs on one schema share the cached inputs (their names hold the recipe version, the row count and the
            # stamp, not the run): a run that loses the race to create one reads the other's, which has the same rows.
            if "ALREADY_EXISTS" not in str(exc) or not spark.catalog.tableExists(full):
                _drop_partial(spark, full)
                raise
    if per_file:
        return _spread_if_compacted(spark, full)
    return full


def _spread_if_compacted(spark, full: str) -> str:
    """`full`, or a session view of it spread over the partitions of a tile family. Table maintenance rewrites the 13 KB
    table of virtual tiles as ONE file after it was written in files of a few rows, and a one-file scan is one task: the
    op then runs serially (a warmup of 1019 s at 4x, where the spread table took 61 s). A table with fewer files than
    partitions is read through the same repartition its builder used."""
    from databricks.labs.gbx.bench.levels_family import family_partitions

    want = family_partitions(spark, spark.table(full).count())
    if (
        want <= 1
        or spark.sql(f"DESCRIBE DETAIL {full}").collect()[0]["numFiles"] >= want
    ):
        return full
    view = full.split(".")[-1] + "_spread"
    spark.table(full).repartition(want).createOrReplaceTempView(view)
    print(
        f"[LEVELS input] {full} has fewer files than {want} partitions: read spread as {view}",
        flush=True,
    )
    return view


_INPUT_NAME = re.compile(r"^(mvp_in_.+?)_([0-9a-f]{8})(_tiles)?$")


def superseded_inputs(spark, schema: str, keep: int = 1) -> List[str]:
    """The input tables of `schema` that a newer stamp has replaced: for each input (name, scale) all but the `keep` newest
    stamps, by creation time. The stamps pile up as the generators change; nothing drops them but this, and only when asked
    (`drop_superseded`). A table whose name carries no stamp is not an input of this kind and is never listed.
    """
    names = [
        r[1] if len(r) > 1 else r[0]
        for r in spark.sql(f"SHOW TABLES IN {schema} LIKE 'mvp_in_*'").collect()
    ]
    by_input: Dict[tuple, List[tuple]] = {}
    for name in names:
        m = _INPUT_NAME.match(name)
        if not m:
            continue
        created = spark.sql(f"DESCRIBE DETAIL {schema}.{name}").collect()[0][
            "createdAt"
        ]
        by_input.setdefault((m.group(1), m.group(3) or ""), []).append(
            (created, m.group(2), name)
        )
    old: List[str] = []
    for versions in by_input.values():
        versions.sort(reverse=True)
        old += [f"{schema}.{name}" for _, _, name in versions[max(keep, 1) :]]
    return sorted(old)


def drop_superseded(
    spark, schema: str, keep: int = 1, dry_run: bool = True
) -> List[str]:
    """`superseded_inputs`, dropped unless `dry_run` (the default: they are shared, so this lists first). The tables."""
    old = superseded_inputs(spark, schema, keep)
    if not dry_run:
        for full in old:
            spark.sql(f"DROP TABLE IF EXISTS {full}")
    return old


def _drop_partial(spark, full: str) -> None:
    """A build that failed or was interrupted (a setup past its cap) must not leave a table behind: `_table` takes an
    existing table for a complete one. A Delta create-table-as-select should not leave one, but the cost of being wrong
    is a later run timing a short input as ok, so the target is dropped (it did not exist when this build began).
    """
    try:
        spark.sql(f"DROP TABLE IF EXISTS {full}")
    except Exception:  # noqa: BLE001 - the build's own error is the one to report
        pass


def _bng_points(spark, n, corpus, schema):
    from pyspark.sql import functions as F

    return _table(
        spark,
        schema,
        f"mvp_in_bng_points_v{RECIPE_VERSION}_{n}",
        lambda: spark.range(n).select(
            (F.rand(7) * 700000).alias("e"), (F.rand(8) * 1300000).alias("n")
        ),
        rows=n,
    )


def _invalid_polys(spark, n, corpus, schema):
    from pyspark.sql import functions as F

    x, y = (F.rand(3) * 1000).alias("x"), (F.rand(4) * 1000).alias("y")
    # A bow-tie ring: self-intersecting, so every polygon needs repairing.
    return _table(
        spark,
        schema,
        f"mvp_in_invalid_polys_v{RECIPE_VERSION}_{n}",
        lambda: spark.range(n)
        .select(x, y)
        .select(
            F.format_string(
                "POLYGON((%1$f %2$f, %3$f %4$f, %3$f %2$f, %1$f %4$f, %1$f %2$f))",
                "x",
                "y",
                F.col("x") + 1,
                F.col("y") + 1,
            ).alias("wkt")
        ),
        rows=n,
    )


def _bng_polys(spark, n, corpus, schema):
    """2800 x 1800 m rectangles at random places inside Great Britain (EPSG:27700): each one covers
    about 10.6 1 km cells on average, ((2.8 + 1) x (1.8 + 1): a side of w km crosses w + 1 cells at a random offset), so
    a tessellation of them makes about 10 to 11 rows per input row (the fixed oracle polygon is the 6-cell case).
    """
    from pyspark.sql import functions as F

    x = (F.rand(5) * 500000 + 100000).alias("x")
    y = (F.rand(6) * 800000 + 100000).alias("y")
    return _table(
        spark,
        schema,
        f"mvp_in_bng_polys_v{RECIPE_VERSION}_{n}",
        lambda: spark.range(n)
        .select(x, y)
        .select(
            F.format_string(
                "POLYGON((%1$f %2$f, %3$f %2$f, %3$f %4$f, %1$f %4$f, %1$f %2$f))",
                "x",
                "y",
                F.col("x") + 2800,
                F.col("y") + 1800,
            ).alias("wkt")
        ),
        rows=n,
    )


SMALL_INPUT_ROWS = 64  # a tile input of at most this many rows is built from the first files, not by a limit

_CORPUS_SEEN: set = set()


def corpus_guard(label: str, rows_dir: str, held: int, need: int) -> None:
    """Refuse a scale the corpus cannot supply, and say once per run how many tiles it holds.

    A short corpus must not cap silently: every scale of a ladder would read the same tiles under its own label. The
    error names both numbers and the generator that fills the gap on Databricks (no upload).
    """
    key = (label, rows_dir)
    if key not in _CORPUS_SEEN:
        _CORPUS_SEEN.add(key)
        print(f"[LEVELS {label}] corpus {rows_dir} holds {held} tiles", flush=True)
    if need > held:
        raise ValueError(
            f"the scale asks for {need} tiles but {rows_dir} holds only {held}. "
            f"Fix: gbx:bench:gen-data --on-databricks --rows-from {held} --rows-to {need - 1} "
            "(generated on Databricks, not uploaded)"
        )


def _tiles(virtual: bool, subdir: str = "rows", name: Optional[str] = None):
    """The corpus tiles under `subdir` as a one-column `tile` table (`name` labels the cached table)."""

    def build(spark, n, corpus, schema):
        from databricks.labs.gbx import manager as gbx_manager

        rows = corpus.rstrip("/") + "/" + subdir
        if not os.path.isdir(rows):
            return None

        gbx_manager.register_sources(spark)  # the light raster reader
        if subdir == "rows":
            corpus_guard(name or "tiles", rows, len(_corpus_rows(corpus) or []), n)
        # a virtual table's rows are paths: its few KB would be read by ONE task (a serial run that also holds every
        # output of the op in one batch), so it is written spread like the materialized tile families: at most
        # FAMILY_ROWS_PER_PARTITION tiles in a partition, because a pixel-producing op decodes each virtual tile it
        # is given and holds the whole partition's outputs in one batch (8 to a partition at 4000 rows lost the
        # executors on a dedicated cluster and passed the 1 GB limit on Serverless; the materialized input, 2 to a
        # partition, ran)
        # virtfps: the reader's tiles carry path_sidecar_free (a table of "virtfp" tiles was built without it)
        label = f"mvp_in_tiles_{name or ('virtfps' if virtual else 'mat')}_v{RECIPE_VERSION}_{n}"

        def spread(df):
            if not virtual:
                return df
            from databricks.labs.gbx.bench.levels_family import family_partitions

            return df.repartition(family_partitions(spark, n))

        # the files are written that spread too: optimized write packs the repartitioned paths back into a few files
        per_file = None
        if virtual:
            from databricks.labs.gbx.bench.levels_family import (
                FAMILY_ROWS_PER_PARTITION,
            )

            per_file = FAMILY_ROWS_PER_PARTITION

        first = (
            _corpus_rows(corpus) if subdir == "rows" and n <= SMALL_INPUT_ROWS else None
        )
        if first:
            # A few rows (the smoke): the first n files, not `limit(n)` over the whole directory, which reads every
            # file (one row each) before it keeps n of them: minutes for a table of 16 tiles.
            pairs = [(str(i), p) for i, p in enumerate(first[:n])]
            return _table(
                spark,
                schema,
                label,
                lambda: spread(_tile_column(spark, pairs, virtual, keyed=False)),
                per_file=per_file,
            )
        # The scan is given the first n files themselves (the `filterRegex` IO's reads use), so it plans n files: a bare
        # `limit(n)` leaves a task per file of the folder (10,000 for the big corpus) when the table is built or read.
        only = _only_files_regex(_first_files(rows, n))
        return _table(
            spark,
            schema,
            label,
            lambda: spread(
                spark.read.format("raster_gbx")
                .option("virtualTiles", str(virtual).lower())
                .option("filterRegex", only)
                .load(rows)
                .select("tile")
                .limit(n)
            ),
            per_file=per_file,
        )

    return build


_CORPUS_ROWS: Dict[str, List[str]] = {}


def _corpus_rows(corpus: str) -> Optional[List[str]]:
    """The corpus' row tiles, sorted; None when the corpus has none. Listed once per process: a corpus of tens of
    thousands of tiles on a Volume takes seconds to list, and every input build asks."""
    rows = corpus.rstrip("/") + "/rows"
    if not os.path.isdir(rows):
        return None
    if rows not in _CORPUS_ROWS:
        from databricks.labs.gbx.ds._listing import list_files

        _CORPUS_ROWS[rows] = list_files(
            rows, filter_regex=r".*\.tif$", recursive=False, raise_on_empty=False
        )
    return _CORPUS_ROWS[rows]


_FOLDER_NAMES: Dict[str, List[str]] = {}


def _sidecar_free_paths(paths) -> set:
    """The files among `paths` that the raster reader would stamp ``path_sidecar_free``: nothing beside them in a folder
    past GDAL's listing limit that a GDAL open could pick up (the reader's own rule, over the names of each folder, listed
    once per process)."""
    from databricks.labs.gbx.ds.raster import RasterGbxReader

    paths = list(paths)
    for d in {os.path.dirname(p) for p in paths}:
        if d not in _FOLDER_NAMES:
            _FOLDER_NAMES[d] = os.listdir(d)
    return RasterGbxReader._files_without_sidecars(paths, _FOLDER_NAMES)


def _tile_column(spark, pairs, virtual: bool, keyed: bool):
    """A DataFrame of ``[key,] tile`` from ``(key, path)`` pairs: virtual tiles, or tiles read now."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.manifest import TILE_SQL
    from databricks.labs.gbx.pyrx._env import SIDECAR_FREE_KEY
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    if virtual:
        # stamped as the reader stamps its tiles: a worker that is not told a file has no sidecar lists the folder of the
        # corpus (tens of thousands of names) at its second open, or checks every sidecar name of each open
        free = _sidecar_free_paths([p for _, p in pairs])
        rows = [
            (
                k,
                VirtualTile(
                    cellid=0,
                    path=p,
                    metadata={SIDECAR_FREE_KEY: "1"} if p in free else {},
                ).to_row(),
            )
            for k, p in pairs
        ]
        df = client_dataframe(
            spark, rows, f"key string, tile {TILE_SQL}", what="virtual tile rows"
        )
    else:
        from databricks.labs.gbx.bench.levels_family import family_partitions
        from databricks.labs.gbx.rasterx import functions as rx

        # The path rows are a few KB, which one task would read file by file (the tiles are read where the rows are):
        # spread them so the n tiles are read, held and written by many tasks, as the family inputs are. The table
        # that results then has the same many input partitions for every form that reads it.
        df = (
            client_dataframe(
                spark, pairs, "key string, path string", what="tile path rows"
            )
            .repartition(family_partitions(spark, len(pairs)))
            .select(
                "key",
                rx.rst_fromfile(F.col("path"), "GTiff", materialize=True).alias("tile"),
            )
        )
    return df if keyed else df.select("tile")


def _grouped(recipe: str, virtual: bool):
    """Groups of tiles for the by-key helpers: each corpus row tile with the members the multi-tile
    recipe of ``bench.synth`` writes beside it (merge: two offset copies; combineavg: two aligned
    copies), keyed by the recipe directory's hash.

    The merge members are files under the corpus, written once, by tasks (one row per source tile, spread over the
    compute's slots; writing them one after another on the driver took 540-825 s at 1x) in a job of their own, so the
    write and the table build are timed apart. The combineavg members are the source tile twice: the recipe's two
    copies hold the source's pixels, grid, CRS and NoData unchanged (``synth._synth_combineavg`` writes the same
    arrays with the source's profile), so the group is `explode(array(src, src))` and nothing is written to the Volume.
    """

    def build(spark, n, corpus, schema):
        sources = _corpus_rows(corpus)
        if not sources:
            return None
        from pyspark.sql import functions as F

        from databricks.labs.gbx.bench import synth
        from databricks.labs.gbx.bench.levels_family import family_partitions
        from databricks.labs.gbx.rasterx import functions as rx

        corpus_guard("tiles_grouped", os.path.join(corpus, "rows"), len(sources), n)
        plan = []
        for src in sources[:n]:
            out = synth.synth_dir(corpus, "rows/" + os.path.basename(src), recipe)
            plan.append((os.path.basename(out), src, out))
        kind = "virt" if virtual else "mat"

        def tiles():
            spread = family_partitions(spark, len(plan))
            plan_df = client_dataframe(
                spark,
                plan,
                "key string, src string, out string",
                what="grouped tile plan",
            )
            if recipe == "combineavg":
                members = plan_df.select("key", F.col("src").alias("path"))
                members = members.union(members)
            else:
                writer = F.udf(
                    lambda src, out: synth.synthesize(src, recipe, out), "array<string>"
                )
                t0 = time.perf_counter()
                written = (
                    plan_df.repartition(spread)
                    .select("key", F.explode(writer("src", "out")).alias("path"))
                    .collect()
                )
                print(
                    f"[LEVELS input] {len(written)} group members written by tasks in "
                    f"{time.perf_counter() - t0:.1f} s",
                    flush=True,
                )
                members = client_dataframe(
                    spark,
                    [(r["key"], r["path"]) for r in written],
                    "key string, path string",
                    what="group member paths",
                )
            return members.repartition(spread).select(
                "key",
                rx.rst_fromfile(F.col("path"), "GTiff", materialize=not virtual).alias(
                    "tile"
                ),
            )

        return _table(
            spark,
            schema,
            f"mvp_in_tiles_grouped_{recipe}_{kind}_v{RECIPE_VERSION}_{n}",
            tiles,
        )

    build.recipe = recipe
    build.writes_under_corpus = (
        recipe != "combineavg"
    )  # the merge members land in `<corpus>/_synth`
    return build


_GB_FOUND: Dict[tuple, List[str]] = (
    {}
)  # (rows dir, n) -> the EPSG:27700 tiles found, once per process


def _gb_tiles(virtual: bool):
    """The corpus row tiles that are EPSG:27700 (the BNG cells of the others fall outside Britain)."""

    def build(spark, n, corpus, schema):
        sources = _corpus_rows(corpus)
        if not sources:
            return None

        kind = "virtfps" if virtual else "mat"
        name = stamped_name(schema, f"mvp_in_tiles_gb_{kind}_v{RECIPE_VERSION}_{n}")
        if schema is not None and spark.catalog.tableExists(f"{schema}.{name}"):
            return f"{schema}.{name}"  # built by an earlier form or run: no header of the corpus is read again
        memo = (
            os.path.join(corpus, "rows"),
            n,
            _corpus_fingerprint(corpus),
        )  # a corpus that changes is not the one found in
        if memo not in _GB_FOUND:
            t0 = time.perf_counter()
            # the corpus cycles four systems, so about a quarter are EPSG:27700: look at 4n + 64 files, no more (a
            # corpus with none must not open every file). A header read on a Volume costs tens to hundreds of
            # milliseconds, and a 1x rung looks at a thousand: they are read by the cluster's tasks in ONE pass (as
            # the grouped planner reads its headers). Stopping early would save nothing: the n-th BNG tile is
            # about the 4n-th file.
            cand = sources[: 4 * n + 64]
            gb = [p for p, c in zip(cand, _epsgs(spark, cand)) if c == 27700][:n]
            print(
                f"[LEVELS input] BNG tiles found ({len(gb)}) in {time.perf_counter() - t0:.1f} s",
                flush=True,
            )
            _GB_FOUND[memo] = gb
        gb = _GB_FOUND[memo]
        if not gb:
            return None
        if len(gb) < n:
            # about a quarter of the corpus is EPSG:27700, so the gap in BNG tiles is about four times as many corpus tiles
            held = len(sources)
            raise ValueError(
                f"the scale asks for {n} British National Grid tiles but the {held} tiles of "
                f"{os.path.join(corpus, 'rows')} hold only {len(gb)}. "
                f"Fix: gbx:bench:gen-data --on-databricks --rows-from {held} --rows-to {held + 4 * (n - len(gb)) - 1} "
                "(generated on Databricks, not uploaded)"
            )
        return _table(
            spark,
            schema,
            name,
            lambda: _tile_column(spark, [(p, p) for p in gb], virtual, keyed=False),
            exact=True,
        )

    return build


_GROUP_SIZE = 50  # tiles a group holds, at most
_REST_GROUP_SIZE = 4  # tiles a group holds, at most, in the half of the skew input that is not the hot key
_CELL_KEYS = 200  # groups of the BNG cell rows


def _epsg_of(path: str) -> Optional[int]:
    """The EPSG code of the tile at `path` (one header read), or None."""
    with open_data(path) as ds:
        return ds.crs.to_epsg() if ds.crs else None


def _epsgs(spark, paths: List[str]) -> List[Optional[int]]:
    """The EPSG code of each of `paths`, in order. A header read on a Volume costs tens of milliseconds, so the
    thousands of a ladder rung are read by the cluster's tasks, not one after another on the driver (one rung of the
    merge aggregate waited on that for its whole form cap); a short list, or no session, is read here.
    """
    if spark is None or len(paths) < SMALL_INPUT_ROWS:
        return [_epsg_of(p) for p in paths]
    from pyspark.sql import functions as F

    probe = F.udf(_epsg_of, "int")
    rows = (
        client_dataframe(
            spark,
            list(enumerate(paths)),
            "i int, p string",
            what="corpus paths",
        )
        .repartition(io_partitions(spark, len(paths)))
        .select("i", probe("p").alias("crs"))
        .collect()
    )
    got = {r["i"]: r["crs"] for r in rows}
    return [got[i] for i in range(len(paths))]


def _plan_grouped_tiles(sources, n, skew, spark=None):
    """`(key, path)` pairs for the first `n` of the corpus row tiles `sources`, every group of ONE CRS.

    The corpus cycles four systems over four places (two cities), so a group of mixed ones is a mosaic of
    several places: terabytes of NoData for the light merge, and one tile for the heavy one. Same-system tiles
    share their place, so a group merges to one tile and what the shapes differ on is how they hold the members.
    The groups are sized from the tiles there are (`n` is only an upper bound), so the 1x, 4x and 16x inputs
    have groups of one size until the corpus runs out.

    With `skew`, half the rows are ONE key of one system (`hot`) and the other half are small groups of up to 4
    tiles, so the hot key is 14 times the typical group at the base size and more beyond it. The base size
    (`BASE_ROWS`) makes `hot` 56 tiles, 896 MiB decoded (16 MiB each): the skew is in how the rows are spread,
    not one group too big to hold. At 4x and 16x `hot` is 3.5 and 14 GiB, which no merge shape can hold: those
    points are the memory cliff, not a comparison.
    """

    corpus_guard("tiles_grouped", "the corpus rows", len(sources), n)
    half = n // 2
    size = _REST_GROUP_SIZE if skew else _GROUP_SIZE
    by_crs: Dict[Optional[int], List[str]] = {}
    hot_crs = None
    seen, batch, done = 0, max(int(n), 64), False
    while not done and seen < len(sources):
        chunk = sources[seen : seen + batch]
        seen += len(chunk)
        for p, crs in zip(chunk, _epsgs(spark, chunk)):
            hot_crs = crs if not by_crs else hot_crs
            by_crs.setdefault(crs, []).append(p)
            have = sum(map(len, by_crs.values()))
            if have >= n and (not skew or len(by_crs[hot_crs]) >= half):
                done = True
                break
    pairs = []
    if skew:
        hot, by_crs[hot_crs] = by_crs[hot_crs][:half], by_crs[hot_crs][half:]
        pairs += [("hot", p) for p in hot]
    left = n - len(pairs)
    for crs, paths in sorted(by_crs.items(), key=lambda kv: str(kv[0])):
        for i, p in enumerate(paths[: max(0, left)]):
            pairs.append((f"{crs}-{i // size}", p))
        left -= len(paths)
    return pairs


def _grouped_tiles(skew: bool, label: str = ""):
    """`(key, tile)`: the corpus row tiles in groups of one CRS (see :func:`_plan_grouped_tiles`). `label` keeps the
    cached table of another consumer of the same recipe apart (the tile family reads fewer tiles than the aggregates).
    """

    def build(spark, n, corpus, schema):
        sources = _corpus_rows(corpus)
        if not sources:
            return None
        kind = "skew" if skew else "even"

        def tiles():
            # planned only when the table is built: reading the CRS of every header (thousands, on the cluster) took
            # minutes, and it ran for every form that asked for a table that was already there
            t0 = time.perf_counter()
            pairs = _plan_grouped_tiles(sources, n, skew, spark)
            print(
                f"[LEVELS input] grouped tiles planned (CRS of {len(pairs)} headers) in {time.perf_counter() - t0:.1f} s",
                flush=True,
            )
            return _tile_column(spark, pairs, False, keyed=True)

        return _table(
            spark,
            schema,
            f"mvp_in_tiles_grouped{label}_{kind}_v{RECIPE_VERSION}_{n}",
            tiles,
        )

    return build


def _scan_epsgs(spark, paths: List[str], batch: int, take):
    """`(path, EPSG)` of `paths` in order, read a batch at a time (see :func:`_epsgs`) until `take(found)` is true, so
    that a search which finds what it wants in the first batch reads no more headers."""
    found: List[tuple] = []
    for at in range(0, len(paths), batch):
        chunk = paths[at : at + batch]
        found += list(zip(chunk, _epsgs(spark, chunk)))
        if take(found):
            break
    return found


def _plan_stack_tiles(sources, n, spark=None):
    """`("stack", path)` pairs: the first `n` corpus row tiles of the CRS of the first one that has a CRS. The tiles of
    one system share their place, so the `n` references are one deep stack on one footprint (`n` is the depth of the
    merge's VRT).
    """
    corpus_guard("tiles_stack", "the corpus rows", len(sources), n)
    batch = max(int(n), SMALL_INPUT_ROWS)

    def anchor_of(found):
        # the first CRS a tile has: a tile whose header gives no EPSG is no anchor (the stack would then be every other
        # tile without one). Inline, not a module helper: a planner's stamp is its own source.
        return next((c for _, c in found if c is not None), None)

    # the corpus cycles four systems: the n-th of the first one is about the 4n-th file
    got = _scan_epsgs(
        spark,
        sources[: 4 * n + 64],
        batch,
        lambda f: anchor_of(f) is not None
        and sum(1 for _, c in f if c == anchor_of(f)) >= n,
    )
    anchor = anchor_of(got)
    if anchor is None:
        raise ValueError(
            f"a stack of {n} tiles on one footprint needs tiles with a CRS, but none of the first {len(got)} corpus "
            "tiles gives an EPSG code in its header"
        )
    same = [p for p, c in got if c == anchor][:n]
    if len(same) < n:
        held = len(sources)
        raise ValueError(
            f"a stack of {n} tiles on one footprint needs {n} corpus tiles of one CRS, but the {held} tiles hold "
            f"{len(same)} of the first one's. "
            f"Fix: gbx:bench:gen-data --on-databricks --rows-from {held} --rows-to {held + 4 * (n - len(same)) - 1} "
            "(generated on Databricks, not uploaded)"
        )
    return [("stack", p) for p in same]


def _plan_mixed_tiles(sources, n, spark=None):
    """`(key, path)` pairs for `n` corpus row tiles in groups of up to `_GROUP_SIZE` that each hold SEVERAL CRSs. The
    corpus' systems 4326, 3857 and 32618 are one place (New York) and 27700 is another (London); a group across the two
    places is a mosaic of both with a continent of NoData between them. So the groups take the three of one place, in
    corpus order, which alternates them.
    """
    corpus_guard("tiles_mixed_crs", "the corpus rows", len(sources), n)
    batch = max(int(n), SMALL_INPUT_ROWS)
    # three of the four systems qualify: about 4n / 3 files hold n of them
    got = _scan_epsgs(
        spark,
        sources[: 2 * n + 64],
        batch,
        lambda f: sum(1 for _, c in f if c not in (None, 27700)) >= n,
    )
    paths = [(p, c) for p, c in got if c not in (None, 27700)][:n]
    if len(paths) < n or len({c for _, c in paths}) < 2:
        held = len(sources)
        raise ValueError(
            f"the scale asks for {n} tiles in mixed systems but the {held} tiles of the corpus hold {len(paths)} "
            f"outside EPSG:27700, in {len({c for _, c in paths})} system(s). "
            f"Fix: gbx:bench:gen-data --on-databricks --rows-from {held} --rows-to {held + 2 * (n - len(paths)) - 1} "
            "(generated on Databricks, not uploaded)"
        )
    return [(f"mix-{i // _GROUP_SIZE}", p) for i, (p, _) in enumerate(paths)]


def _grouped_virt_tiles(mode: str, label: str = ""):
    """`(key, tile)` of VIRTUAL tiles (a path each, nothing read): the by-reference merge's inputs. `mode` picks the plan:
    `even` and `skew` are those of :func:`_grouped_tiles` (the same groups of one CRS, as references), `stack` is one key
    holding all `n` tiles of one CRS (:func:`_plan_stack_tiles`), `mixed` is groups of several CRSs
    (:func:`_plan_mixed_tiles`). `label` keeps the cached table of another consumer of the same plan apart.
    """

    def build(spark, n, corpus, schema):
        sources = _corpus_rows(corpus)
        if not sources:
            return None

        def tiles():
            # planned only when the table is built (the CRS of every header is read, by tasks)
            from databricks.labs.gbx.bench.runner import _bench_parallelism

            t0 = time.perf_counter()
            if mode == "stack":
                pairs = _plan_stack_tiles(sources, n, spark)
            elif mode == "mixed":
                pairs = _plan_mixed_tiles(sources, n, spark)
            else:
                pairs = _plan_grouped_tiles(sources, n, mode == "skew", spark)
            print(
                f"[LEVELS input] {mode} virtual tiles planned (CRS of {len(pairs)} headers) in "
                f"{time.perf_counter() - t0:.1f} s",
                flush=True,
            )
            # the rows are a few KB: written spread over the compute's slots, so the table is not one file read by one task
            return _tile_column(spark, pairs, True, keyed=True).repartition(
                max(1, min(len(pairs), _bench_parallelism(spark)))
            )

        return _table(
            spark,
            schema,
            # virtfps: the references carry path_sidecar_free, as the reader's tiles do
            f"mvp_in_tiles_grouped{label}_{mode}_virtfps_v{RECIPE_VERSION}_{n}",
            tiles,
        )

    return build


def _bng_cells(skew: bool):
    """`(key, cellid, value)`: random 1 km cells inside the 100 km square TQ, 200 keys; with `skew`, half the rows
    are key 0 (and so the same few thousand cells over and over)."""

    def build(spark, n, corpus, schema):
        from pyspark.sql import functions as F

        key = F.floor(F.rand(5) * _CELL_KEYS)
        if skew:
            key = F.when(F.rand(6) < 0.5, F.lit(0)).otherwise(key)
        cell = F.format_string(
            "TQ%02d%02d", F.floor(F.rand(7) * 100), F.floor(F.rand(8) * 100)
        )
        return _table(
            spark,
            schema,
            f"mvp_in_bng_cells_{'skew' if skew else 'even'}_v{RECIPE_VERSION}_{n}",
            lambda: spark.range(n).select(
                key.cast("string").alias("key"),
                cell.alias("cellid"),
                (F.rand(9) * 100).alias("value"),
            ),
            rows=n,
        )

    return build


_DSM_GRID, _DSM_TILE_PX = (
    4,
    2000,
)  # tiles per side, and pixels per tile side (1 m pixels)
_TOWER_RADIUS, _TOWER_MAST = 500.0, 30.0  # metres


def _dsm_towers(spark, n, corpus, schema):
    """`n` towers on a DSM of `_DSM_GRID` x `_DSM_GRID` tiles of `_DSM_TILE_PX` pixels (EPSG:3857, x0 = y0 = 0), the
    towers at fixed random places at least a radius inside it. The tiles are files under the corpus, written once;
    their `(tx, ty, path)` manifest is the table beside the towers', named `<towers table>_tiles`.
    """
    if not os.path.isdir(corpus):
        return None
    from pyspark.sql import functions as F

    from databricks.labs.gbx.bench import oracles

    tiles = oracles.dsm_tile_rows(
        os.path.join(corpus, "_synth", "dsm_towers", f"v{RECIPE_VERSION}"),
        _DSM_GRID,
        _DSM_TILE_PX,
    )
    name = stamped_name(schema, f"mvp_in_dsm_towers_v{RECIPE_VERSION}_{n}")
    _table(
        spark,
        schema,
        name
        + "_tiles",  # beside the towers, under the same stamp (the form reads `<towers table>_tiles`)
        lambda: client_dataframe(
            spark, tiles, "tx int, ty int, path string", what="tower tile paths"
        ),
        exact=True,
    )
    reach = _DSM_GRID * _DSM_TILE_PX - 2 * _TOWER_RADIUS
    return _table(
        spark,
        schema,
        name,
        lambda: spark.range(n).select(
            F.concat(F.lit("t"), F.col("id").cast("string")).alias("tower_cellid"),
            (F.rand(11) * reach + _TOWER_RADIUS).alias("x"),
            (F.rand(12) * reach + _TOWER_RADIUS).alias("y"),
        ),
        exact=True,
    )


_dsm_towers.writes_under_corpus = True  # the DSM tiles land in `<corpus>/_synth`

INPUTS: Dict[str, Callable] = {
    "bng_points": _bng_points,
    "invalid_polys": _invalid_polys,
    "tiles_mat": _tiles(False),
    "tiles_virt": _tiles(True),
    "tiles_grouped_mat": _grouped("merge", False),
    "tiles_aligned_mat": _grouped("combineavg", False),
    "tiles_gb_mat": _gb_tiles(False),
    "tiles_gb_virt": _gb_tiles(True),
    "bng_polys": _bng_polys,
    "tiles_grouped": _grouped_tiles(False),
    "tiles_grouped_skew": _grouped_tiles(True),
    "tiles_grouped_virt": _grouped_virt_tiles("even"),
    "tiles_grouped_virt_skew": _grouped_virt_tiles("skew"),
    "tiles_grouped_family_virt": _grouped_virt_tiles("even", "_family"),
    "tiles_stack_virt": _grouped_virt_tiles("stack"),
    "tiles_mixed_crs_virt": _grouped_virt_tiles("mixed"),
    "bng_cells": _bng_cells(False),
    "bng_cells_skew": _bng_cells(True),
    "dsm_towers": _dsm_towers,
    "tiles_big": _tiles(
        False, "big", "big"
    ),  # absent until the 4096 x 4096 tiles are copied to {corpus}/big
}


def forms_writing_under_corpus(forms) -> List[Form]:
    """The forms whose input builder writes files under the corpus root (the multi-tile and DSM inputs write `_synth/`
    there): a corpus read in place, a shared folder, must not be given to them. The builders carry the mark
    (`writes_under_corpus`); a test keeps it in step with the builders that call the synthesiser.
    """
    return [f for f in forms if getattr(INPUTS[f.input], "writes_under_corpus", False)]


# Each query answers one row (n, fp, n_out): the output count, an order-independent fingerprint,
# and count(<the output>), which skips NULLs: n_out != n means some rows came out NULL. The hash
# is reduced before it is summed: a sum of raw 64-bit hashes overflows under ANSI.
_BNG = (
    "SELECT count(*) AS n, sum(pmod(xxhash64(c), 2147483647)) AS fp, count(c) AS n_out "
    "FROM (SELECT {fn}(e, n, 3) AS c FROM {src})"
)
_TCRS = (
    "SELECT count(*) AS n, sum(length(t.raster)) AS fp, count(t.raster) AS n_out "
    "FROM (SELECT {fn}(tile, 'EPSG:3857') AS t FROM {src})"
)
# the value-level twin (value_fp): the decoded pixels of a few tiles, not the length of every encoding. The few are the
# first 64 by a key of the tile (see `levels_family._TILE_V`): an unordered LIMIT would hash whichever tiles the scan met.
_TCRS_V = (
    "SELECT count(*) AS n, sum(pmod(gbx_bench_vfp_raster(t.raster), 2147483647)) AS fp, count(t.raster) AS n_out "
    "FROM (SELECT {fn}(tile, 'EPSG:3857') AS t FROM "
    "(SELECT * FROM {src} ORDER BY tile.path, xxhash64(tile.raster) LIMIT 64))"
)
_MV = (
    "SELECT count(*) AS n, sum(length(g)) AS fp, count(g) AS n_out "
    "FROM (SELECT {fn}(wkt) AS g FROM {src})"
)
_MV_UC = (
    "SELECT count(*) AS n, sum(length(st_asewkb(g))) AS fp, count(g) AS n_out FROM "
    "(SELECT {fn}(CAST(st_geomfromwkt(wkt, 4326) AS GEOMETRY(ANY))) AS g FROM {src})"
)


_FLOOR = "TQ3080"


def register_floor(spark):
    """The BNG function's Arrow UDF shell with nothing inside it: the same three input columns
    and string output as ``gbx_bng_eastnorthasbng``, and a constant body. Registered once, by the
    form's ``setup``, outside the timed region."""
    import pandas as pd
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    const = _FLOOR

    def floor(e, n, res):
        return pd.Series(const, index=e.index)

    # Type hints as objects, not the strings this module's `from __future__ import annotations` would make: the
    # pandas UDF reads them to pick the Series -> Series form (no deprecated PandasUDFType argument).
    floor.__annotations__ = {
        "e": pd.Series,
        "n": pd.Series,
        "res": pd.Series,
        "return": pd.Series,
    }
    spark.udf.register("gbx_bng_floor", F.pandas_udf(floor, StringType()))


def floor_bng(spark, src):
    """The floor query. Its median is what the shell costs without a kernel, so
    ``floor_median_s`` beside a variant's median says how much of that variant is the kernel
    (the part a faster kernel can win back)."""
    df = spark.sql(_BNG.replace("{fn}", "gbx_bng_floor").format(src=src))
    r = df.collect()[0]
    return r[0], r[1], r[2], df


def _f(level, variant, fn, inp, sql, uc=False, vsql="", **kw):
    name = ("{uc}." + fn) if uc else fn  # an L4 function is addressed by its UC schema
    return Form(
        "S", fn, level, variant, inp, sql.replace("{fn}", name), register=not uc,
        vsql=vsql.replace("{fn}", name), **kw,
    )  # fmt: skip


B, T, M = "gbx_bng_eastnorthasbng", "gbx_rst_transformcrs", "gbx_st_makevalid"
FORMS: Dict[str, List[Form]] = {
    "S": [
        # the BNG scalar has no JVM form (L1 and L2 measured slower than the light batch form): L3 forms and the floor
        _f("L3", "c0", B, "bng_points", _BNG),
        _f("L3", "c1", B, "bng_points", _BNG),
        _f("L3", "d", B, "bng_points", _BNG),
        Form(
            "S",
            B,
            "L3",
            "floor",
            "bng_points",
            run="databricks.labs.gbx.bench.levels_forms:floor_bng",
            setup="databricks.labs.gbx.bench.levels_forms:register_floor",
            register=False,
        ),
        _f(
            "L4",
            "uc-scalar",
            B,
            "bng_points",
            _BNG,
            uc=True,
            computes=("serverless", "dedicated"),
        ),
        _f("L1", "-", T, "tiles_mat", _TCRS, computes=("dedicated",), vsql=_TCRS_V),
        _f(
            "L1",
            "dispatch-virtual",
            T,
            "tiles_virt",
            _TCRS,
            computes=("dedicated",),
            vsql=_TCRS_V,
        ),
        _f("L3", "-", T, "tiles_mat", _TCRS, vsql=_TCRS_V),
        _f("L3", "virtual", T, "tiles_virt", _TCRS, vsql=_TCRS_V),
        _f("L3", "-", M, "invalid_polys", _MV),
        _f(
            "L4",
            "uc-scalar",
            M,
            "invalid_polys",
            _MV_UC,
            uc=True,
            computes=("serverless", "dedicated"),
        ),
    ]
}


# --- H, the DataFrame helpers: against the per-row UDTFs and the grouped aggregates --------------
#
# Forms run in this order: the light ones first, then the JVM ones, because both register the same
# `gbx_rst_*` names and a registration lasts for the session (a setup runs once).
# Each timed query reads the whole output: tile-returning forms hand it to `gbx_rst_transformcrs`
# (a downstream op that reads the pixels), and the fingerprint is `sum(length(raster))` with
# `count(raster)` as n_out, so a NULL raster shows. The retile size is the same on both sides.

_RETILE_PX = 512
_DOWN = "gbx_rst_transformcrs(tile, 'EPSG:3857')"
_TILE_FP = (
    "SELECT count(*) AS n, sum(length(x.raster)) AS fp, count(x.raster) AS n_out FROM "
    "(SELECT {down} AS x FROM {{src}}, LATERAL {udtf} AS t)"
)
_RETILE_UDTF = _TILE_FP.format(
    down="gbx_rst_transformcrs(struct(t.*), 'EPSG:3857')",
    udtf=f"gbx_rst_retile(tile, {_RETILE_PX}, {_RETILE_PX})",
)
_TESS_UDTF = _TILE_FP.format(
    down="gbx_rst_transformcrs(struct(t.*), 'EPSG:3857')",
    udtf="gbx_rst_bng_tessellate(tile, 3)",
)
# The aggregate returns the mosaic as BINARY.
_MERGE_L3 = (
    "SELECT count(*) AS n, sum(length(x)) AS fp, count(x) AS n_out FROM "
    "(SELECT gbx_rst_merge_agg(tile) AS x FROM {src} GROUP BY key)"
)
_COMBINE_SQL = (
    "SELECT count(*) AS n, sum(length(x.raster)) AS fp, count(x.raster) AS n_out FROM "
    "(SELECT gbx_rst_combineavg(collect_list(tile)) AS x FROM {src} GROUP BY key)"
)


def _register_light(spark, *names):
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_family(
        spark, ("gbx_rst_", "gbx_pmtiles_"), only=list(names), level="L3"
    )


def register_h_downstream(spark):
    _register_light(spark, "gbx_rst_transformcrs")


def register_h_retile_udtf(spark):
    _register_light(spark, "gbx_rst_transformcrs", "gbx_rst_retile")


def register_h_tess_udtf(spark):
    _register_light(spark, "gbx_rst_transformcrs", "gbx_rst_bng_tessellate")


# What each tessellate setup leaves installed, so the run re-installs it when the other level's form put its own there.
register_h_tess_udtf.installs = {
    "gbx_rst_bng_tessellate": ("L3", "-"),
    "gbx_rst_transformcrs": ("L3", "-"),
}


def register_h_merge_l3(spark):
    _register_light(spark, "gbx_rst_merge_agg")


def register_h_combine_l3(spark):
    _register_light(spark, "gbx_rst_combineavg")


_WIDE_SHUFFLE = 1000


def h_combine_l3_sp1000(spark, src):
    """`h_combine_l3` with `spark.sql.shuffle.partitions` at 1000 for this query only (bench-only A/B on the width of
    the group-by shuffle: the default width packs ~16 groups of two 13 MB tiles into a task). The conf is set on classic
    and dedicated sessions and put back after; it is never set on Serverless."""
    from databricks.labs.gbx.manager.capabilities import is_serverless

    key = "spark.sql.shuffle.partitions"
    guarded = not is_serverless()
    before = spark.conf.get(key, None) if guarded else None
    if guarded:
        spark.conf.set(key, str(_WIDE_SHUFFLE))
    try:
        df = spark.sql(_COMBINE_SQL.format(src=src))
        r = df.collect()[0]
    finally:
        if guarded:
            if before is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, before)
    return r[0], r[1], r[2], df


def _stats(df, tile_expr: Optional[str]):
    """(n, fp, n_out, df): the row count, the summed raster length and the non-NULL raster count of
    `tile_expr` over `df` (or of its `raster` column), read in full. `df` here is the query that ran.
    """
    from pyspark.sql import functions as F

    if tile_expr is not None:
        df = df.select(F.expr(tile_expr).alias("x")).select(
            F.col("x.raster").alias("raster")
        )
    out = df.agg(F.count(F.lit(1)), F.sum(F.length("raster")), F.count("raster"))
    r = out.collect()[0]
    return r[0], r[1], r[2], out


def h_retile_helper(spark, src):
    from databricks.labs.gbx.rasterx import helpers

    tiles = helpers.retile_virtual(spark.table(src), "tile", _RETILE_PX, _RETILE_PX)
    return _stats(tiles, _DOWN)


def h_tess_helper(spark, src):
    from databricks.labs.gbx.rasterx import helpers

    return _stats(helpers.bng_tessellate_virtual(spark.table(src), "tile", 3), _DOWN)


def h_merge_fold(spark, src):
    from databricks.labs.gbx.rasterx import helpers

    return _stats(helpers.merge_by_key(spark.table(src), ["key"], method="fold"), None)


def _group_stats(spark, src, merged):
    """`_stats` of a merge of the groups of `src`, with `n` the INPUT rows: the groups are a few dozen whatever the
    scale (the planned inputs hold up to 50 tiles a group), and their count would read as a short input at every rung.
    `n_out` is `n` unless a group came out NULL."""
    rows = spark.table(src).count()
    n, fp, n_out, out = _stats(merged, None)
    return rows, fp, (rows if n_out == n else n_out), out


def _merge(spark, src, **kw):
    from databricks.labs.gbx.rasterx import helpers

    return _group_stats(
        spark, src, helpers.merge_by_key(spark.table(src), ["key"], **kw)
    )


def h_merge_vrt(spark, src):
    """`merge_by_key` (vrt) over virtual tiles: no pixel is shuffled, each group is a VRT of its references."""
    return _merge(spark, src, method="vrt")


def h_merge_fold_virt(spark, src):
    """`merge_by_key` (fold) over virtual tiles: the merge holds no member, the shuffle moves paths."""
    return _merge(spark, src, method="fold")


def h_merge_fold_virt_np8(spark, src):
    """`h_merge_fold_virt` with the shuffle at 8 partitions (the default is the session's slots, 64 at least)."""
    return _merge(spark, src, method="fold", num_partitions=8)


def h_merge_fold_virt_npkeys(spark, src):
    """`h_merge_fold_virt` with one partition per key (the count is read from the table, a few KB)."""
    keys = spark.table(src).select("key").distinct().count()
    return _merge(spark, src, method="fold", num_partitions=keys)


def h_merge_stack(spark, src):
    """One key of n virtual tiles on one footprint (n = 56 at 1x, 896 at 16x): the depth of the merge's VRT grows
    with n, and the whole stack is the work of one task."""
    return _merge(spark, src, method="vrt")


def h_merge_mixed_crs(spark, src):
    """Groups of virtual tiles in several CRSs: each group is put on the grid of its first tile of the lowest EPSG
    code, so every other member is read through a warp."""
    return _merge(spark, src, method="vrt")


def h_combine_avg(spark, src):
    from databricks.labs.gbx.rasterx import helpers

    return _stats(helpers.combine_by_key(spark.table(src), ["key"], stat="avg"), None)


def h_towers_l3(spark, src):
    """(n, fp, n_out, df) of `rst_viewshed_towers` over the towers of `src` and the DSM beside them: the tower
    count, the summed visible pixels, and the non-NULL count of them."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.rasterx.helpers import rst_viewshed_towers

    done = rst_viewshed_towers(
        spark.table(src), spark.table(src + "_tiles"), _TOWER_RADIUS, _TOWER_MAST,
        x0=0.0, y0=0.0, tile_m=float(_DSM_TILE_PX),
    )  # fmt: skip
    out = done.agg(F.count(F.lit(1)), F.sum("visible_px"), F.count("visible_px"))
    r = out.collect()[0]
    return r[0], r[1], r[2], out


_H = "databricks.labs.gbx.bench.levels_forms:"


def _h(fn, level, variant, inp, *, run="", sql="", setup="", **kw):
    return Form(
        "H",
        fn,
        level,
        variant,
        inp,
        sql=sql,
        run=_H + run if run else "",
        setup=_H + setup if setup else "",
        register=False,
        **kw,
    )


FORMS["H"] = [
    _h(
        "retile_virtual",
        "DF",
        "h_retile_helper",
        "tiles_virt",
        run="h_retile_helper",
        setup="register_h_downstream",
    ),
    _h(
        "gbx_rst_retile",
        "L3",
        "h_retile_udtf",
        "tiles_mat",
        sql=_RETILE_UDTF,
        setup="register_h_retile_udtf",
    ),
    _h(
        "bng_tessellate_virtual",
        "DF",
        "h_tess_helper",
        "tiles_gb_virt",
        run="h_tess_helper",
        setup="register_h_downstream",
    ),
    _h(
        "gbx_rst_bng_tessellate",
        "L3",
        "h_tess_udtf",
        "tiles_gb_mat",
        sql=_TESS_UDTF,
        setup="register_h_tess_udtf",
    ),
    _h("merge_by_key", "DF", "h_merge_fold", "tiles_grouped_mat", run="h_merge_fold"),
    # The merge inputs overlap (offset copies), and where members overlap the vrt winner is the last
    # path, the fold winner the last bytes: compare the times of fold, vrt and the aggregate, not
    # their pixels. The vrt result is a COG, so its fingerprint differs from fold's anyway.
    _h(
        "merge_by_key",
        "DF",
        "h_merge_vrt",
        "tiles_grouped_family_virt",
        run="h_merge_vrt",
    ),
    # The same plan as `h_merge_fold`'s groups, as references: what the by-reference merge costs against the bytes of
    # `h_merge_fold` (its input holds the members), and the shuffle's partition count (default, 8, one per key)
    _h(
        "merge_by_key",
        "DF",
        "h_merge_fold_virt",
        "tiles_grouped_family_virt",
        run="h_merge_fold_virt",
    ),
    _h(
        "merge_by_key",
        "DF",
        "h_merge_fold_virt_np8",
        "tiles_grouped_family_virt",
        run="h_merge_fold_virt_np8",
    ),
    _h(
        "merge_by_key",
        "DF",
        "h_merge_fold_virt_npkeys",
        "tiles_grouped_family_virt",
        run="h_merge_fold_virt_npkeys",
    ),
    _h("merge_by_key", "DF", "h_merge_stack", "tiles_stack_virt", run="h_merge_stack"),
    _h(
        "merge_by_key",
        "DF",
        "h_merge_mixed_crs",
        "tiles_mixed_crs_virt",
        run="h_merge_mixed_crs",
    ),
    _h(
        "gbx_rst_merge_agg",
        "L3",
        "h_merge_l3_agg",
        "tiles_grouped_mat",
        sql=_MERGE_L3,
        setup="register_h_merge_l3",
    ),
    _h(
        "combine_by_key",
        "DF",
        "h_combine_avg",
        "tiles_aligned_mat",
        run="h_combine_avg",
    ),
    _h(
        "gbx_rst_combineavg",
        "L3",
        "h_combine_l3",
        "tiles_aligned_mat",
        sql=_COMBINE_SQL,
        setup="register_h_combine_l3",
    ),
    _h(
        "gbx_rst_combineavg",
        "L3",
        "h_combine_l3_sp1000",
        "tiles_aligned_mat",
        run="h_combine_l3_sp1000",
        setup="register_h_combine_l3",
    ),
    _h("rst_viewshed_towers", "L3", "h_towers_l3", "dsm_towers", run="h_towers_l3"),
]


# --- G, generators: one form per shape --------------------------------------------------------------
#
# Each query answers (n, fp, n_out) over the OUTPUT rows. The tessellate fingerprint is the summed (reduced)
# hash of the cell id: the same cells give the same fingerprint in every shape at every level. The retile
# fingerprint is the summed length of the encoded pieces, and each level encodes with its own writer (rasterio
# on L3, GDAL with the tile's own options on L1, plain GDAL GTiff on L2), so it agrees between the shapes of ONE
# level and not between levels: across levels compare `n` (the window count) and the oracle rows.
# A shape that is not a product function runs under a `gbx_mvp_*` name that its `setup` registers (outside the
# timed region).

_RT_CELL = (
    128,
    128,
)  # (window width, height): the corpus' small tiles get a handful of windows each
_RT_BIG = (128, 256)  # a 4096 x 4096 tile gets 32 x 16 = 512 windows
_RT_FP = "SELECT count(*) AS n, sum(length({x})) AS fp, count({x}) AS n_out FROM "
_RT_LATERAL = (
    _RT_FP.format(x="t.raster")
    + "{{src}} s, LATERAL {fn}(s.tile, {size}) t"  # the light UDTF: the tile fields
)
_RT_ARRAY = (
    _RT_FP.format(x="x.raster")
    + "(SELECT explode(gbx_mvp_rst_retile_array(tile, {size})) AS x FROM {{src}})"
)
_RT_PART = (
    _RT_FP.format(x="x.raster")
    + "(SELECT gbx_mvp_rst_retile_part(tile, {size}, i) AS x FROM {{src}} "
    "LATERAL VIEW explode(sequence(0, gbx_mvp_rst_retile_count(tile, {size}) - 1)) v AS i)"
)
# The session TVF over the array form: a SQL body, so no Python between the rows and the plan.
_RT_TVF = (
    _RT_FP.format(x="t.tile.raster")
    + "{{src}} s, LATERAL gbx_mvp_rst_retile_tvf(s.tile, {w}, {h}) t"
)

_TE_FP = "SELECT count(*) AS n, sum(pmod(xxhash64({x}), 2147483647)) AS fp, count({x}) AS n_out FROM "
_TE_LATERAL = _TE_FP.format(x="t.cellid") + "{{src}} s, LATERAL {fn}(s.wkt, 3) t"
_TE_ARRAY = _TE_FP.format(x="c.cellid") + "(SELECT inline({fn}(wkt, 3)) FROM {{src}}) c"
_TE_UC = (
    _TE_FP.format(x="t.cellid")
    + "{{src}} s, LATERAL {{uc}}.gbx_bng_tessellateexplode(CAST(st_geomfromwkt(s.wkt, 27700) AS GEOMETRY(ANY)), '3') t"
).format()


def setup_g_probes(spark):
    """The light probe forms (array, count+part, Arrow UDTFs)."""
    from databricks.labs.gbx.pyrx import _mvp_forms

    _mvp_forms.register_probe_forms(spark)


def setup_g_tvf(spark):
    """The session SQL table function over the array probe; its body calls the probe UDF."""
    from databricks.labs.gbx.manifest import TILE_SQL

    setup_g_probes(spark)
    spark.sql(
        f"CREATE OR REPLACE TEMPORARY FUNCTION gbx_mvp_rst_retile_tvf(tile {TILE_SQL}, w INT, h INT) "
        f"RETURNS TABLE (tile {TILE_SQL}) "
        "RETURN SELECT explode(gbx_mvp_rst_retile_array(tile, w, h)) AS tile"
    )


_G = "databricks.labs.gbx.bench.levels_forms:"
RT, TE, TS = "gbx_rst_retile", "gbx_bng_tessellateexplode", "gbx_bng_tessellate"


def _g(fn, level, variant, inp, sql, *, setup="", register=False, **kw):
    # `register` runs the manager's own registration of `fn` at `level`; a probe shape sets `setup` instead.
    return Form(
        "G",
        fn,
        level,
        variant,
        inp,
        sql=sql,
        setup=_G + setup if setup else "",
        register=register,
        **kw,
    )


def _retile(sql, window, **fmt):
    w, h = window
    return sql.format(size=f"{w}, {h}", w=w, h=h, **fmt)


FORMS["G"] = [
    # retile on the corpus' small tiles
    _g(
        RT,
        "L3",
        "udtf",
        "tiles_mat",
        _retile(_RT_LATERAL, _RT_CELL, fn=RT),
        register=True,
    ),
    _g(
        RT,
        "L3",
        "udtf-arrow",
        "tiles_mat",
        _retile(_RT_LATERAL, _RT_CELL, fn="gbx_rst_retile_mvp_arrow"),
        setup="setup_g_probes",
    ),
    _g(
        RT,
        "L3",
        "array_inline",
        "tiles_mat",
        _retile(_RT_ARRAY, _RT_CELL),
        setup="setup_g_probes",
    ),
    _g(
        RT,
        "L3",
        "count_part",
        "tiles_mat",
        _retile(_RT_PART, _RT_CELL),
        setup="setup_g_probes",
        memory_probe=True,
    ),
    _g(
        RT,
        "L3",
        "session-tvf",
        "tiles_mat",
        _retile(_RT_TVF, _RT_CELL),
        setup="setup_g_tvf",
    ),
    # the same three light shapes on big tiles (512 windows each): what the shapes cost in memory
    _g(
        RT,
        "L3",
        "udtf-big",
        "tiles_big",
        _retile(_RT_LATERAL, _RT_BIG, fn=RT),
        register=True,
        memory_probe=True,
    ),
    _g(
        RT,
        "L3",
        "array_inline-big",
        "tiles_big",
        _retile(_RT_ARRAY, _RT_BIG),
        setup="setup_g_probes",
        memory_probe=True,
    ),
    _g(
        RT,
        "L3",
        "count_part-big",
        "tiles_big",
        _retile(_RT_PART, _RT_BIG),
        setup="setup_g_probes",
        memory_probe=True,
    ),
    # BNG tessellate on polygons
    _g(TE, "L3", "udtf", "bng_polys", _TE_LATERAL.format(fn=TE), register=True),
    _g(
        TE,
        "L3",
        "udtf-arrow",
        "bng_polys",
        _TE_LATERAL.format(fn="gbx_bng_tessellateexplode_mvp_arrow"),
        setup="setup_g_probes",
    ),
    _g(
        TE,
        "L1",
        "tvf",
        "bng_polys",
        _TE_LATERAL.format(fn=TE),
        register=True,
        computes=("dedicated",),
    ),
    _g(TS, "L3", "array_inline", "bng_polys", _TE_ARRAY.format(fn=TS), register=True),
    _g(
        TS,
        "L1",
        "array_inline",
        "bng_polys",
        _TE_ARRAY.format(fn=TS),
        register=True,
        computes=("dedicated",),
    ),
    _g(TE, "L4", "uc-tvf", "bng_polys", _TE_UC, computes=("serverless", "dedicated")),
    # form (b): the tessellation as one Scala mapPartitions, called over py4j (no SQL function in between)
    Form(
        "G",
        TE,
        "JVM-DF",
        "scala-mapPartitions",
        "bng_polys",
        run=_G + "g_tessellate_jvm",
        computes=("dedicated",),
        register=False,
    ),
]


def g_tessellate_jvm(spark, src):
    """(n, fp, n_out, df) of `BngTessellateJob.transform` over `src` at resolution 3: the cell count, the same
    cellid fingerprint as the SQL forms, and the non-NULL cellid count."""
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F

    job = spark._jvm.com.databricks.labs.gbx.kernels.df.BngTessellateJob
    cells = DataFrame(
        job.transform(spark._jsparkSession, spark.table(src)._jdf, 3), spark
    )
    out = cells.agg(
        F.count(F.lit(1)),
        F.sum(F.pmod(F.xxhash64("cellid"), F.lit(2147483647))),
        F.count("cellid"),
    )
    r = out.collect()[0]
    return r[0], r[1], r[2], out


# --- A, aggregates: one form per shape and input ----------------------------------------------------------
#
# Each query answers (n, fp, n_out) over the groups: the fingerprint is the summed length of the group's raster
# and n_out counts the rasters that are not NULL, so a form that fails on some groups cannot pass on size.
# The light forms come first and the JVM ones last: both register the same `gbx_rst_*` names, and a `setup`
# runs once. The forms of one aggregate do not agree on the bytes of the result (each level encodes with its
# own writer), so across forms compare `n` and the oracle rows. A form that is not a product function runs
# under a `gbx_mvp_*` name that its `setup` registers (outside the timed region). The light forms are all
# `L3`, so the runner records the peak Python worker memory of each: the table function and the finalizes are
# kept or dropped against the grouped aggregate on that column.

_A = "databricks.labs.gbx.bench.levels_forms:"
_MA, _BR = "gbx_rst_merge_agg", "gbx_rst_bng_rasterize_agg"
# xmin, ymin, xmax, ymax, width, height: the 100 km square TQ (where `bng_cells` puts its cells) at 100 m
_A_CANVAS = (500000.0, 100000.0, 600000.0, 200000.0, 1000, 1000)
_PAIRS = "collect_list(struct(cellid, value))"


def _ma_shapes():
    """The SQL of each shape of the merge over `(key, tile)`. A result that is a tile struct gives its raster."""
    return {
        "grouped": f"SELECT key, {_MA}(tile) AS out FROM {{src}} GROUP BY key",
        "finalize": "SELECT key, gbx_rst_merge(collect_list(tile)).raster AS out FROM {src} GROUP BY key",
        "tvf": (
            "SELECT key, raster AS out FROM gbx_rst_merge_tvf("
            "TABLE(SELECT key, tile FROM {src}) PARTITION BY (key))"
        ),
    }


def _br_shapes(canvas=_A_CANVAS):
    """The SQL of each shape of the BNG rasterize over `(key, cellid, value)` on `canvas`."""
    types = ("DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "INT", "INT")
    lits = [f"CAST({v} AS {t})" for v, t in zip(canvas, types)]
    on = ", ".join(lits)
    args = f"cellid, value, 27700, NULL, {on}, 'centroids', 0"
    finalize = (
        "SELECT key, {fn}(" + _PAIRS + ", " + on + ") AS out FROM {{src}} GROUP BY key"
    )
    return {
        "grouped": f"SELECT key, {_BR}({args}) AS out FROM {{src}} GROUP BY key",
        "udaf": f"SELECT key, {_BR}({args}) AS out FROM {{src}} GROUP BY key",
        "finalize": finalize.format(fn="gbx_mvp_rst_bng_rasterize_cells"),
        "finalize_l2": finalize.format(fn="gbx_mvp_rst_bng_rasterize_cells_l2"),
        # partial canvases per (key, salt), then one per-pixel max per key
        "tree": (
            "SELECT key, gbx_mvp_rst_canvas_max(collect_list(p)) AS out FROM ("
            f"SELECT key, gbx_mvp_rst_bng_rasterize_cells({_PAIRS}, {on}) AS p FROM ("
            "SELECT *, pmod(xxhash64(cellid, value), 2) AS salt FROM {src}) GROUP BY key, salt) GROUP BY key"
        ),
        "tvf": (
            "SELECT key, raster AS out FROM gbx_rst_bng_rasterize_tvf("
            f"TABLE(SELECT key, cellid, value FROM {{src}}) PARTITION BY (key), {on})"
        ),
    }


def setup_a_probes(spark):
    """The light probes (finalizes, tree step), the product's table functions and the light `gbx_rst_merge` finalize."""
    from databricks.labs.gbx import manager as gbx_manager
    from databricks.labs.gbx import register
    from databricks.labs.gbx.pyrx import _mvp_forms

    _mvp_forms.register_probe_forms(spark)
    gbx_manager.register_family(
        spark, ("gbx_rst_", "gbx_pmtiles_"), only=["gbx_rst_merge"], level="L3"
    )
    register(spark, only=["gbx_rst_merge_tvf", "gbx_rst_bng_rasterize_tvf"])


# What each of the two setups below leaves under the name `gbx_rst_merge`. The JVM merge replaces the light one in the
# session, and a setup that is not judged by what is installed runs once: the light finalizes that came after the JVM
# one then timed the JVM merge (and, given a reference, raised VirtualTileException), whatever level they were labelled.
setup_a_probes.installs = {"gbx_rst_merge": ("L3", "-")}


def setup_a_l2(spark):
    """The Java UDF finalizes under probe names (needs the JAR)."""
    from databricks.labs.gbx.pyrx import _mvp_forms

    _mvp_forms.register_l2_probes(spark)


def setup_a_udaf(spark):
    """The Catalyst aggregates under their product names (needs the JAR; classic only)."""
    from databricks.labs.gbx.pyrx import _mvp_forms

    _mvp_forms.register_l1_udafs(spark)


def setup_a_l1_merge(spark):
    """The Catalyst `gbx_rst_merge` over an array of tiles (needs the JAR; replaces the light finalize)."""
    inst = spark._jvm.com.databricks.labs.gbx.kernels.L1Installer
    got = str(
        inst.install(
            spark._jsparkSession,
            "gbx_rst_merge",
            "com.databricks.labs.gbx.rasterx.expressions.RST_Merge",
        )
    )
    if got != "ok":
        raise RuntimeError(f"gbx_rst_merge: L1 install gave {got}")


setup_a_l1_merge.installs = {"gbx_rst_merge": ("L1", "-")}


def _a(fn, level, variant, inp, inner, *, setup="", register=False, **kw):
    # n is the INPUT rows (a scalar subquery over the same table, as the family `_AGG` shape): the query's own count(*)
    # is the groups, which is a few hundred whatever the scale and would read as a short input at every rung
    sql = (
        "SELECT (SELECT count(*) FROM {src}) AS n, sum(length(out)) AS fp, "
        "CASE WHEN count(out) = count(*) THEN (SELECT count(*) FROM {src}) ELSE count(out) END AS n_out "
        f"FROM ({inner})"
    )
    return Form(
        "A",
        fn,
        level,
        variant,
        inp,
        sql=sql,
        setup=_A + setup if setup else "",
        register=register,
        **kw,
    )


def _a_light(fn, shapes, inputs):
    """The light forms of one aggregate over each of its inputs. An input's suffix (`-skew`) labels its forms,
    since a result row carries the form and not the input."""
    out = []
    for inp, sfx in inputs:
        out += [
            _a(fn, "L3", "c0" + sfx, inp, shapes["grouped"], register=True),
            _a(
                fn,
                "L3",
                "finalize" + sfx,
                inp,
                shapes["finalize"],
                setup="setup_a_probes",
            ),
        ]
        if "tree" in shapes:
            out.append(
                _a(fn, "L3", "tree" + sfx, inp, shapes["tree"], setup="setup_a_probes")
            )
        # Serverless's table-function engine fails PARTITION BY on a task's second row (the function says so)
        out.append(
            _a(
                fn,
                "L3",
                "tvf" + sfx,
                inp,
                shapes["tvf"],
                setup="setup_a_probes",
                computes=("dedicated", "standard"),
            )
        )
    return out


def _a_jvm(fn, shapes, inputs):
    both = ("dedicated",)  # no Java handler for the finalize on standard access mode
    out = []
    for inp, sfx in inputs:
        if (
            fn == _MA
        ):  # the merge aggregate has no JVM form; the Catalyst finalize over an array of tiles does
            out.append(
                _a(
                    fn,
                    "L1",
                    "finalize" + sfx,
                    inp,
                    shapes["finalize"],
                    setup="setup_a_l1_merge",
                    computes=("dedicated",),
                )
            )
            continue
        out += [
            _a(
                fn,
                "L2",
                "finalize" + sfx,
                inp,
                shapes["finalize_l2"],
                setup="setup_a_l2",
                computes=both,
            ),
            _a(
                fn,
                "L1",
                "udaf" + sfx,
                inp,
                shapes["udaf"],
                setup="setup_a_udaf",
                computes=("dedicated",),
            ),
        ]
    return out


_A_MERGE = (_MA, _ma_shapes(), (("tiles_grouped", ""), ("tiles_grouped_skew", "-skew")))
# The light shapes again over the same plans as references (a path each): `c0` on the bytes stays as the documented cliff,
# `c0-virt` is the by-reference form of it. The Catalyst finalize has no by-reference form to time.
_A_MERGE_VIRT = (
    _MA,
    _ma_shapes(),
    (("tiles_grouped_virt", "-virt"), ("tiles_grouped_virt_skew", "-virt-skew")),
)
_A_RASTERIZE = (_BR, _br_shapes(), (("bng_cells", ""), ("bng_cells_skew", "-skew")))
FORMS["A"] = (
    _a_light(*_A_MERGE)
    + _a_light(*_A_MERGE_VIRT)
    + _a_light(*_A_RASTERIZE)
    + _a_jvm(*_A_MERGE)
    + _a_jvm(*_A_RASTERIZE)
)


# --- SE, the GDAL spec engine: each of its functions on the engine and on its own implementation --------------------
#
# Inputs: DEM tiles (one band, Float32, EPSG:3857) of 256, 512 and 1024 pixels for slope and the floor; pairs of
# them for map algebra; the corpus tiles for transformcrs (as the S forms). The floor is the engine running
# `raster select --band=1`: the per-row cost of the engine itself (the /vsimem write, the header read, the
# algorithm, one GTiff encode), with next to no raster work. The forms of today's implementations go under private
# names, so a later engine registration never measures them by mistake.

_SE_SIZES = (256, 512, 1024)
BASE_ROWS.update(
    {"se_dem_256": 64, "se_dem_512": 32, "se_dem_1024": 16, "se_pair_512": 32}
)


def _se_dem_tile(size: int, seed: int) -> dict:
    import numpy as np

    from databricks.labs.gbx.bench import oracles

    rng = np.random.default_rng(seed)
    yy, xx = np.mgrid[0:size, 0:size].astype("float64")
    z = (
        100.0
        + 0.5 * xx
        + 0.3 * yy
        + 10.0 * np.sin(xx / (7.0 + seed)) * np.cos(yy / 5.0)
        + rng.normal(0.0, 0.1, (size, size))
    )
    return oracles._tile(oracles._gtiff(z[np.newaxis], "EPSG:3857"))


def _se_dems(size: int):
    def build(spark, n, corpus, schema):
        from databricks.labs.gbx.manifest import TILE_SQL

        tiles = [_se_dem_tile(size, s) for s in range(4)]  # four distinct tiles, cycled
        check_estimate(f"se_dem_{size} x {n}", n, size_of(tiles[0]))
        return _table(
            spark,
            schema,
            f"mvp_in_se_dem{size}_v{RECIPE_VERSION}_{n}",
            lambda: client_dataframe(
                spark,
                [(tiles[i % 4],) for i in range(n)],
                f"tile {TILE_SQL}",
                what=f"se_dem_{size} tiles",
            ).repartition(max(1, min(n, 8))),
        )

    return build


def _se_pairs(size: int):
    def build(spark, n, corpus, schema):
        from databricks.labs.gbx.manifest import TILE_SQL

        tiles = [_se_dem_tile(size, s) for s in range(4)]
        check_estimate(f"se_pair_{size} x {n}", n, 2 * size_of(tiles[0]))
        return _table(
            spark,
            schema,
            f"mvp_in_se_pair{size}_v{RECIPE_VERSION}_{n}",
            lambda: client_dataframe(
                spark,
                [([tiles[i % 4], tiles[(i + 1) % 4]],) for i in range(n)],
                f"tiles array<{TILE_SQL}>",
                what=f"se_pair_{size} tile pairs",
            ).repartition(max(1, min(n, 8))),
        )

    return build


INPUTS.update({f"se_dem_{s}": _se_dems(s) for s in _SE_SIZES})
INPUTS["se_pair_512"] = _se_pairs(512)


# The cached frames of the replicated inputs by view name: a rung that has run releases them (`release_cached_inputs`).
_CACHED_INPUTS: Dict[str, object] = {}


def release_cached_inputs(spark) -> None:
    """Unpersist the cached frames of the replicated inputs and drop their views, so the next rung builds its own. A
    view left behind would be found by the next build and serve its rows uncached."""
    for name, df in list(_CACHED_INPUTS.items()):
        df.unpersist()
        spark.catalog.dropTempView(name)
    _CACHED_INPUTS.clear()


def _se_dems_replicated(size: int):
    """`size` px DEM tiles, n rows: four distinct tiles built once on the client and replicated on the executors, then
    cached (the rows are not rebuilt by each timed query, and no client cap applies to n). The cache lasts one rung:
    `release_cached_inputs` frees it.
    """

    def build(spark, n, corpus, schema):
        from pyspark.sql import functions as F

        from databricks.labs.gbx.manifest import TILE_SQL

        name = f"mvp_in_se_dem{size}_mem_v{RECIPE_VERSION}_{n}"
        if schema is None and spark.catalog.tableExists(name):
            return name
        four = client_dataframe(
            spark,
            [(i, _se_dem_tile(size, i)) for i in range(4)],
            f"idx int, tile {TILE_SQL}",
            what=f"se_dem_{size} tiles",
        )

        def rows():
            df = (
                spark.range(n)
                .select((F.col("id") % 4).cast("int").alias("idx"))
                .join(F.broadcast(four), "idx")
                .select("tile")
                .repartition(8)
            )
            if schema is None:
                df = df.cache()
                df.count()
                _CACHED_INPUTS[name] = df
            return df

        return _table(spark, schema, name, rows)

    return build


INPUTS["se_dem_1024_mem"] = _se_dems_replicated(1024)
BASE_ROWS["se_dem_1024_mem"] = 16


def _se_floor_function():
    """The floor's spec: `raster select --band=1` on slope's signature (a bench spec, not a SQL function)."""
    import dataclasses

    from databricks.labs.gbx import manifest

    return dataclasses.replace(
        manifest.load().functions["gbx_rst_slope"],
        sql_name="gbx_bench_engine_floor",
        gdal=manifest.parse_gdal_spec(
            {
                "alg": "raster select",
                "in": {"tile": "raster"},
                "out": {"kind": "raster", "dtype": "input"},
                "write": "tile",
                "args": [{"arg": "band", "const": 1}],
            }
        ),
    )


SE_FLOOR = _se_floor_function()
_SE_FLOOR_REF = "databricks.labs.gbx.bench.levels_forms:SE_FLOOR"


def _se_bench_section():
    """The manifest's allowlist plus the floor's `raster select`: a bench-only extension, never the product's."""
    import dataclasses
    from types import MappingProxyType

    from databricks.labs.gbx import manifest

    section = manifest.load().gdal
    return dataclasses.replace(
        section, allow=MappingProxyType({**section.allow, "raster select": ("band",)})
    )


SE_SECTION = _se_bench_section()
_SE_SECTION_REF = "databricks.labs.gbx.bench.levels_forms:SE_SECTION"


def setup_se_floor(spark):
    """The floor's row-wise and batch UDFs under their own names."""
    from databricks.labs.gbx.engine import l3 as engine_l3

    spark.udf.register(
        "gbx_bench_floor_c0",
        engine_l3.c0(
            "gbx_bench_engine_floor", (), _SE_FLOOR_REF, section_ref=_SE_SECTION_REF
        ),
    )
    spark.udf.register(
        "gbx_bench_floor_c1",
        engine_l3.c1(
            "gbx_bench_engine_floor", (), _SE_FLOOR_REF, section_ref=_SE_SECTION_REF
        ),
    )


def setup_se_slope_today(spark):
    """Today's slope: the Catalyst RST_Slope under its private name, the light numpy UDF under a bench name."""
    from databricks.labs.gbx.manager import jvm
    from databricks.labs.gbx.pyrx import functions as prx

    spark.udf.register("gbx_bench_slope_l3_today", prx._slope_udf)
    why = jvm.install_private_l1(
        spark,
        "com.databricks.labs.gbx.rasterx.expressions.dem.RST_Slope",
        "gbx__l1_rst_slope",
    )
    if why:
        raise RuntimeError(why)


def setup_se_slope_light_today(spark):
    from databricks.labs.gbx.pyrx import functions as prx

    spark.udf.register("gbx_bench_slope_l3_today", prx._slope_udf)


_SE_FP = (
    "SELECT count(*) AS n, sum(length(t.raster)) AS fp, count(t.raster) AS n_out "
    "FROM (SELECT {call} AS t FROM {{src}})"
)
_SE_SLOPE = _SE_FP.format(
    call="{fn}(tile, 'degrees', CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE))"
)
_SE_SLOPE_L1_TODAY = _SE_FP.format(call="gbx__l1_rst_slope(tile, 'degrees')")
_SE_SLOPE_L3_TODAY = _SE_FP.format(
    call="gbx_bench_slope_l3_today(tile, 'degrees', CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE))"
)
_SE_FLOOR_C0 = _SE_FP.format(call="gbx_bench_floor_c0(tile)")
_SE_FLOOR_C1 = (
    "SELECT count(*) AS n, sum(length(r.raster)) AS fp, count(r.raster) AS n_out FROM "
    "(SELECT gbx_bench_floor_c1(tile.raster, 'degrees', CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE)) AS r "
    "FROM {src})"
)
_SE_TCRS = _SE_FP.format(call="{fn}(tile, 'EPSG:3857')")
# The call is inserted as it is; the run formats the query with {src}, so the JSON braces are doubled once.
_SE_MA_TODAY = _SE_FP.format(call="""{fn}(tiles, '{{"calc": "(A - B) / (A + B)"}}')""")
_SE_MA_ENGINE = _SE_FP.format(
    call="""{fn}(tiles, '{{"calc": "norm_diff", "dialect": "builtin"}}')"""
)


def _se(fn, level, variant, inp, sql, *, setup="", register=True, **kw):
    return Form(
        "SE",
        fn,
        level,
        variant,
        inp,
        sql.replace("{fn}", fn),
        setup=("databricks.labs.gbx.bench.levels_forms:" + setup) if setup else "",
        register=register,
        **kw,
    )


_SL, _MALG = "gbx_rst_slope", "gbx_rst_mapalgebra"
_DED = ("dedicated",)
# Today's forms first: they live under private names that the engine registrations after them do not replace.
# A label `<variant>@<size>` registers `<variant>` (see levels._run_one) and names the tile size.
FORMS["SE"] = (
    [
        _se(
            _SL,
            "L1",
            f"today@{s}",
            f"se_dem_{s}",
            _SE_SLOPE_L1_TODAY,
            setup="setup_se_slope_today",
            register=False,
            computes=_DED,
        )
        for s in _SE_SIZES
    ]
    + [
        _se(
            _SL,
            "L3",
            f"today@{s}",
            f"se_dem_{s}",
            _SE_SLOPE_L3_TODAY,
            setup="setup_se_slope_light_today",
            register=False,
        )
        for s in _SE_SIZES
    ]
    + [
        _se(
            _SL,
            "L3",
            f"floor-c0@{s}",
            f"se_dem_{s}",
            _SE_FLOOR_C0,
            setup="setup_se_floor",
            register=False,
        )
        for s in _SE_SIZES
    ]
    + [
        _se(
            _SL,
            "L3",
            f"floor-c1@{s}",
            f"se_dem_{s}",
            _SE_FLOOR_C1,
            setup="setup_se_floor",
            register=False,
        )
        for s in _SE_SIZES
    ]
    + [
        _se(_SL, "L1", f"engine@{s}", f"se_dem_{s}", _SE_SLOPE, computes=_DED)
        for s in _SE_SIZES
    ]
    + [
        _se(_SL, "L3", f"{v}@{s}", f"se_dem_{s}", _SE_SLOPE)
        for v in ("engine-c0", "engine-c1")
        for s in _SE_SIZES
    ]
    + [
        _se(T, "L1", "-", "tiles_mat", _SE_TCRS, computes=_DED),
        _se(T, "L2", "-", "tiles_mat", _SE_TCRS, computes=("dedicated",)),
        _se(T, "L3", "-", "tiles_mat", _SE_TCRS),
        _se(T, "L1", "engine", "tiles_mat", _SE_TCRS, computes=_DED),
        _se(T, "L3", "engine-c0", "tiles_mat", _SE_TCRS),
        _se(T, "L3", "engine-c1", "tiles_mat", _SE_TCRS),
        _se(T, "L1", "engine", "tiles_virt", _SE_TCRS, computes=_DED),
        _se(T, "L3", "engine-c1", "tiles_virt", _SE_TCRS),
        _se(_MALG, "L1", "-", "se_pair_512", _SE_MA_TODAY, computes=_DED),
        _se(_MALG, "L3", "-", "se_pair_512", _SE_MA_TODAY),
        _se(_MALG, "L1", "engine", "se_pair_512", _SE_MA_ENGINE, computes=_DED),
        _se(_MALG, "L3", "engine-c0", "se_pair_512", _SE_MA_ENGINE),
        _se(_MALG, "L3", "engine-c1", "se_pair_512", _SE_MA_ENGINE),
    ]
)

# Slope over 1024 px tiles replicated on the executors (`se_dem_1024_mem`): the local timing pass needs a few hundred
# tiles of real work per size, so the per-tile cost shows above Spark's fixed query cost; tiles built on the client stop
# at the client cap (16 of this size) and the corpus tiles (4 bands, 16 MiB) make the pass a measure of disk reads.
FORMS["SE"] += [
    _se(_SL, "L1", "engine@mem", "se_dem_1024_mem", _SE_SLOPE, computes=_DED),
    _se(_SL, "L3", "engine-c0@mem", "se_dem_1024_mem", _SE_SLOPE),
    _se(_SL, "L3", "engine-c1@mem", "se_dem_1024_mem", _SE_SLOPE),
    _se(
        _SL,
        "L3",
        "floor-c0@mem",
        "se_dem_1024_mem",
        _SE_FLOOR_C0,
        setup="setup_se_floor",
        register=False,
    ),
]

# The query floor: one 256 px tile per variant, so what the form costs is Spark's query, the worker start and the first
# row, not the per-tile work. A fixed cost an engine form pays and today's light form does not shows here first.
BASE_ROWS["se_dem_256_one"] = 1
INPUTS["se_dem_256_one"] = _se_dems(256)
FORMS["SE"] += [
    _se(_SL, "L3", f"{v}@query-floor", "se_dem_256_one", _SE_SLOPE)
    for v in ("engine-c0", "engine-c1")
] + [
    _se(
        _SL,
        "L3",
        "today@query-floor",
        "se_dem_256_one",
        _SE_SLOPE_L3_TODAY,
        setup="setup_se_slope_light_today",
        register=False,
    )
]


# --- IO, the read paths: bytes-free reads with a Python decode against the reader with bytes ----------------------------
#
# The measured heavy read lead was the cost of moving decoded bytes into Python rows. Every read form reads the corpus
# tiles under `{CORPUS}/rows` end to end (read, decode, `sum(length(tile.raster))`) at 1000 / 4000 / 10000 tiles:
#   b      the light `raster_gbx` reader with bytes (the reader decodes in Python, the rows carry the bytes)
#   udf    virtual tiles, decoded row by row by the light UDF (`gbx_mvp_rst_materialize` at L3)
#   d-py   virtual tiles, decoded in one Python call per Arrow batch (`mapInArrow`)
#   floor  d-py's read and `mapInArrow` shell with the decode skipped (the tiles stay virtual): its median is the shell
#          cost, shown as `floor_median_s` beside d-py
# The writer form starts from the same read and writes GeoTIFFs to a scratch directory it deletes after timing:
#   W1 `gtiff_gbx` over virtual tiles.
# (The Scala `gdal` reader, the JVM decode, the Rust decode and the JVM pre-encode writer were measured against these and
# retired; see the decision record.)

IO_FN = "gbx_mvp_rst_materialize"
IO_SCALES = (1, 4, 10)  # times BASE_ROWS["io_rows"]: 1000 / 4000 / 10000 tiles
BASE_ROWS["io_rows"] = 1000
SCALES: Dict[str, tuple] = {
    "IO": IO_SCALES
}  # a set whose ladder is not 1 / 4 / 16 (run_mvp takes it for the default)
# The scratch directory of a run that is not on a Volume (a local probe): under the temp directory.
SCRATCH_PREFIX = "/geobrix/0.6.0/scratch/"


def _io_scratch(corpus: str) -> str:
    """The scratch directory of a run: `scratch` beside the corpus on a Volume, or `geobrix/0.6.0/scratch` under the temp
    directory when the corpus is not (a local probe). A writer form deletes only a strict child of it.
    """
    if corpus.startswith("/Volumes/"):
        return os.path.dirname(corpus.rstrip("/")) + "/scratch"
    import tempfile

    return tempfile.gettempdir() + SCRATCH_PREFIX.rstrip("/")


_IO_SPEC_CACHE: Dict[tuple, tuple] = (
    {}
)  # (rows dir, n, dir mtime) -> (tiles held, a/b file filter)


def _io_listing(rows: str, n: int):
    """`(tiles held, the filterRegex that keeps the first n)` of the corpus' rows folder. The folder is listed once per
    scale (the six read forms and the three writers all build the same input), and again only when the folder changed.
    """
    key = (rows, int(n), os.stat(rows).st_mtime_ns)
    if key not in _IO_SPEC_CACHE:
        files = _first_files(rows, None)
        if len(_IO_SPEC_CACHE) > 32:
            _IO_SPEC_CACHE.clear()
        _IO_SPEC_CACHE[key] = (len(files), _only_files_regex(files[: int(n)]))
    return _IO_SPEC_CACHE[key]


def _io_spec(spark, n, corpus, schema):
    """One row `(rows_dir, n, scratch, only)`: the forms read the corpus themselves (a table of tiles would leave the read
    out). `only` is the file filter of the `a` / `b` reads. The corpus is listed and the filter built here, with the
    light raster reader and writer registered, outside the timed region. A scale that asks for more tiles than `rows`
    holds is an error (the corpus size found is printed).
    """
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(spark)
    rows = corpus.rstrip("/") + "/rows"
    if not os.path.isdir(rows):
        return None
    held, only = _io_listing(rows, n)
    print(f"[LEVELS IO] corpus {rows} holds {held} tiles", flush=True)
    if int(n) > held:
        # A short corpus must not cap silently: every scale of the ladder would read the same tiles under its own label.
        raise ValueError(
            f"the IO ladder asks for {int(n)} tiles but {rows} holds only {held}. "
            f"Fix: gbx:bench:gen-data --on-databricks --rows-from {held} --rows-to {int(n) - 1} "
            "(generated on Databricks, not uploaded), or lower the scales"
        )
    return _table(
        spark,
        None,  # a view: one row naming the corpus of this run, never worth a cached table
        f"mvp_io_spec_v{RECIPE_VERSION}_{n}",
        lambda: client_dataframe(
            spark,
            [(rows, int(n), _io_scratch(corpus), only)],
            "rows_dir string, n int, scratch string, only string",
            what="the read-path spec row",
        ),
    )


INPUTS["io_rows"] = _io_spec


def _io_row(spark, src):
    return spark.table(src).first()


def remove_scratch(path: str, scratch: str) -> None:
    """Delete a writer form's output: only a STRICT CHILD of the run's scratch directory (`_io_scratch`), compared as
    resolved paths. The scratch directory itself, a sibling and a path that climbs out of it are refused.
    """
    root, target = os.path.realpath(scratch), os.path.realpath(path)
    if target == root or os.path.commonpath([root, target]) != root:
        raise ValueError(f"refusing to delete {path!r}: not inside {scratch!r}")
    from databricks.labs.gbx.ds._scratch import remove_scratch_dir

    remove_scratch_dir(target)


IO_ROWS_PER_PARTITION = 8  # at most this many tiles in a partition (`_virtual_tiles` guarantees it): a Python batch of ~13 MB tiles must fit a worker


def io_partitions(spark, n) -> int:
    """The partition count every IO form decodes or writes its `n` tiles with: the compute's slots, but never fewer
    than `ceil(n / IO_ROWS_PER_PARTITION)` (a handful of tiles each: 8 partitions for 400 tiles OOM-killed a local
    JVM) and never more than `n`. A function of `n` and the compute alone, so every form gets the same one.
    `bench.runner._bench_parallelism` is the Connect-safe slot count (no `sparkContext` on Serverless).
    """
    from databricks.labs.gbx.bench.runner import _bench_parallelism

    n = int(n)
    return max(
        1, min(n, max(_bench_parallelism(spark), -(-n // IO_ROWS_PER_PARTITION)))
    )


def _first_files(rows_dir, n) -> List[str]:
    """The corpus' first `n` row files (all of them for `n` None), sorted (the order the light reader lists them in), and
    without the hidden ones (`.x` / `_x`) the reader skips: a hidden file among the first `n` would leave `b` short.
    """
    from databricks.labs.gbx.ds._listing import list_files

    files = list_files(
        rows_dir,
        filter_regex=r".*\.tif$",
        recursive=False,
        raise_on_empty=False,
        skip_hidden=True,
    )
    return files if n is None else files[: int(n)]


def _trie_regex(words) -> str:
    """A regex for exactly `words`, factored by shared prefix (a flat alternation of 10000 names is tried name by name
    on every file; this one reads each file's name once)."""
    words = sorted(set(words))
    if words == [""]:
        return ""
    groups: Dict[str, List[str]] = {}
    for w in words:
        if w:
            groups.setdefault(w[0], []).append(w[1:])
    parts = [re.escape(c) + _trie_regex(rest) for c, rest in groups.items()]
    body = parts[0] if len(parts) == 1 else "(?:" + "|".join(parts) + ")"
    return f"(?:{body})?" if "" in words else body


def _only_files_regex(files) -> str:
    """The `filterRegex` that keeps exactly `files` (all in one folder: that folder, then the file name), for the light
    and the heavy reader alike (the light one anchors at the start of the path, the heavy one wraps the regex in `.*`).
    The folder is part of it: the heavy reader is recursive, so a bare name would also match a same-named file below.
    """
    files = list(files)
    if not files:
        return "(?!)"  # no file: nothing matches
    folder = os.path.dirname(files[0]).rstrip("/") + "/"
    return (
        ".*" + re.escape(folder) + _trie_regex(os.path.basename(f) for f in files) + "$"
    )


def _virtual_tiles(spark, rows_dir, n, only=None):
    """The virtual tiles of the corpus' first `n` files, as `n` rows spread over `io_partitions` partitions.

    The scan is given the first `n` files themselves (`only`: the `filterRegex` the input builder made for `b`;
    built here when not given), so it plans and reads `n` files, never the folder: a `limit` alone leaves a task per file
    of a 10,000-file corpus, whatever `n` is.

    `limit(n)` then a range exchange of P partitions on a row index: whatever reads or writes the tiles runs ABOVE
    the exchange, on exactly `n` tiles and `P` tasks. Without the exchange Spark pushes a row UDF (and a JVM decode) below
    the limit, onto every file of the corpus, while a `mapInArrow` stays above it in one task. The index is `0..n-1` (the
    limit is one partition) and the range exchange samples every row (it keeps up to 60 per partition, P >= n / 8), so
    the partitions hold `ceil(n / P)` tiles each, never more than `IO_ROWS_PER_PARTITION`: a hash of the paths would
    only average to it. By column and with an explicit count, so Serverless' AQE does not coalesce it back to one.
    """
    from pyspark.sql import functions as F

    only = only or _only_files_regex(_first_files(rows_dir, n))
    return (
        spark.read.format("raster_gbx")
        .option("virtualTiles", "true")
        .option("filterRegex", only)
        .load(rows_dir)
        .select("tile")
        .limit(n)
        .withColumn("_row", F.monotonically_increasing_id())
        .repartitionByRange(io_partitions(spark, n), "_row")
        .select("tile")
    )


def _io_read(spark, rows_dir, n, kind, only=None):
    """The read of `kind` over the corpus' first `n` tiles, one `tile` column, every kind on exactly `n` tiles.

    Every kind is given the first `n` files themselves (the readers' `filterRegex`, `only`: the input builder's, so a
    timed form lists nothing; built here when not given), so every scan has `n` tasks. `b` decodes inside the scan,
    where a limit cannot reach; the others decode above `_virtual_tiles`' exchange.
    """
    import functools

    from databricks.labs.gbx.ds.file_gbx import report_detected_cap
    from databricks.labs.gbx.pyrx import _mvp_forms, tile_batches

    only = only or _only_files_regex(_first_files(rows_dir, n))
    if kind == "b":
        return (
            spark.read.format("raster_gbx")
            .option("virtualTiles", "false")
            .option("filterRegex", only)
            .load(rows_dir)
        )
    virt = _virtual_tiles(spark, rows_dir, n, only=only)
    if kind == "udf":
        return virt.selectExpr(f"{IO_FN}(tile) AS tile")
    if kind in ("d-py", "floor"):
        # the floor is d-py's shell with the decode skipped (`keep_virtual`): same read, same Arrow round trip
        decode = functools.partial(
            tile_batches.materialize_batches,
            cap_bytes=report_detected_cap(spark),
            **({"decode": _mvp_forms.keep_virtual} if kind == "floor" else {}),
        )
        return virt.mapInArrow(decode, virt.schema)
    if kind == "virtual":
        return virt
    raise ValueError(kind)


def _io_form(kind):
    def run(spark, src):
        r = _io_row(spark, src)
        tiles = _io_read(spark, r["rows_dir"], int(r["n"]), kind, only=r["only"])
        return _stats(tiles.select(tiles["tile"]["raster"].alias("raster")), None)

    run.__name__ = f"io_{kind.replace('-', '_')}"
    return run


io_b, io_udf, io_d_py = (_io_form(k) for k in ("b", "udf", "d-py"))


def io_floor(spark, src):
    """The IO floor: the `d-py` read with the decode skipped, so its median is what the read and the `mapInArrow` shell
    cost without a decode (`floor_median_s` beside `d-py`, as the BNG floor sits beside its variants). The tiles stay
    virtual, so there are no raster bytes: it counts the tiles that came through (`path`), not decoded rasters.
    """
    from pyspark.sql import functions as F

    r = _io_row(spark, src)
    tiles = _io_read(spark, r["rows_dir"], int(r["n"]), "floor", only=r["only"])
    out = tiles.agg(
        F.count(F.lit(1)),
        F.coalesce(F.sum(F.length(tiles["tile"]["raster"])), F.lit(0)),
        F.count(tiles["tile"]["path"]),
    )
    r = out.collect()[0]
    return r[0], r[1], r[2], out


def _io_named(spark, rows_dir, n, kind, only=None):
    """What a writer form writes: the `kind` read as `(source, tile)`, the file name and the tile the raster writers take."""
    from pyspark.sql import functions as F

    return (
        _io_read(spark, rows_dir, n, kind, only=only)
        .select("tile")
        .withColumn(
            "source",
            F.concat(
                F.lit("t"),
                F.monotonically_increasing_id().cast("string"),
                F.lit(".tif"),
            ),
        )
        .select("source", "tile")
    )


def _io_write(fmt, kind):
    """A writer form: write the `kind` read as `fmt` into a fresh scratch directory, count what landed, delete it."""

    def run(spark, src):
        from databricks.labs.gbx.ds._scratch import new_scratch_dir

        r = _io_row(spark, src)
        rows_dir, n, scratch = r["rows_dir"], int(r["n"]), r["scratch"]
        out = new_scratch_dir(scratch, "io")
        os.makedirs(scratch, exist_ok=True)
        try:
            named = _io_named(spark, rows_dir, n, kind, only=r["only"])
            named.write.format(fmt).mode("overwrite").save(out)
            files = [
                os.path.join(d, f)
                for d, _, fs in os.walk(out)
                for f in fs
                if f.endswith(".tif")
            ]
            size = sum(os.path.getsize(f) for f in files)
            return len(files), size, len(files)
        finally:
            remove_scratch(out, scratch)

    run.__name__ = f"io_write_{fmt}_{kind}"
    return run


io_w1 = _io_write("gtiff_gbx", "virtual")

_IO = "databricks.labs.gbx.bench.levels_forms:"


def _io(fn, level, variant, run, *, setup="", **kw):
    return Form(
        "IO",
        fn,
        level,
        variant,
        "io_rows",
        run=_IO + run.__name__,
        setup=_IO + setup if setup else "",
        register=False,
        **kw,
    )


FORMS["IO"] = [
    _io("read", "DF", "b", io_b),
    _io(IO_FN, "L3", "udf", io_udf, setup="register_io_udf"),
    _io("read", "DF", "d-py", io_d_py),
    # the shell cost of d-py (Task 17's "decode share" floor): `summarize` puts its median beside it
    _io("read", "DF", "floor", io_floor),
    _io("write", "DF", "W1", io_w1),
]


def register_io_udf(spark):
    """Install `gbx_mvp_rst_materialize` at L3 (the row-wise light UDF) for the `udf` form."""
    from databricks.labs.gbx import register

    register(spark, only=[IO_FN], _levels={IO_FN: ("L3", "-")})


register_io_udf.installs = {IO_FN: ("L3", "-")}


# Module-level names the forms resolve by ``module:function``.
for _fn in (io_b, io_udf, io_d_py, io_floor, io_w1):
    globals()[_fn.__name__] = _fn


# A form that only sleeps, for proving the launcher's watchdog (``--inject-stall`` of the cluster launcher). It
# is not in any set: ``levels.run_mvp(inject_stall=...)`` adds it after the first form.
INPUTS["none"] = lambda spark, n, corpus, schema: "none"
BASE_ROWS["none"] = 1
STALL_SECONDS = 0.0


def stall_form(spark, src):
    """Sleep on the driver for STALL_SECONDS: a form with no Spark job, so a run that sits in it has no tasks."""
    import time

    time.sleep(STALL_SECONDS)
    return 1, 1, 1


STALL_FORM = Form(
    "S",
    "inject_stall",
    "L3",
    "sleep",
    "none",
    run="databricks.labs.gbx.bench.levels_forms:stall_form",
    register=False,
)


# The inputs a builder makes on the CLIENT (the rest are read or generated on the executors), as (tile pixels,
# tiles per row): the preflight refuses a ladder whose client-built bytes pass the cap before anything is submitted.
CLIENT_TILES = {
    "se_dem_256": (256, 1),
    "se_dem_256_one": (256, 1),
    "se_dem_512": (512, 1),
    "se_dem_1024": (1024, 1),
    "se_pair_512": (512, 2),
}


def client_bytes(input_key: str, rows: int) -> int:
    """The bytes the builder of `input_key` ships from the client for `rows` rows (0 for an executor-side input)."""
    if input_key not in CLIENT_TILES:
        return 0
    size, per_row = CLIENT_TILES[input_key]
    return rows * per_row * size_of(_se_dem_tile(size, 0))


# One set per function family of the manifest, beside the sets above (`bench.levels_family`)
from databricks.labs.gbx.bench import levels_family as _levels_family  # noqa: E402

_levels_family.install(__import__(__name__, fromlist=["x"]))


def form_id(f: Form) -> str:
    """The qualified name of a form, ``<function>:<variant>``: unique within a set, where a bare variant is not
    (``c0`` is a form of both ``gbx_rst_merge_agg`` and ``gbx_rst_bng_rasterize_agg``).
    """
    return f"{f.fn}:{f.variant}"


def form_named(f: Form, names) -> bool:
    """True when `names` (``--levels-forms`` entries) pick the form: its qualified name, or its bare variant."""
    return f.variant in names or form_id(f) in names


def ambiguous_form_names(mvp: str, names) -> Dict[str, List[str]]:
    """``{bare name: [qualified names]}`` for the bare entries of `names` that are the variant of forms of more than one
    function of the set `mvp`. A qualified entry is never ambiguous."""
    out: Dict[str, List[str]] = {}
    for n in names:
        if ":" in n or n in out:
            continue
        ids = sorted({form_id(f) for f in FORMS.get(mvp, ()) if f.variant == n})
        if len({i.split(":", 1)[0] for i in ids}) > 1:
            out[n] = ids
    return out


def select_forms(mvp: str, names=()) -> List[Form]:
    """The forms of `mvp` a run times: those `names` (the ``--levels-forms`` ids, probe twins included) pick by variant
    or by ``<function>:<variant>``, or, with no names, every form that is not opt-in."""
    return [f for f in FORMS[mvp] if (form_named(f, names) if names else not f.opt_in)]


# The one-worker readdir probe twins of the JVM forms (`bench.levels_probe`), opt-in
from databricks.labs.gbx.bench import levels_probe as _levels_probe  # noqa: E402

_levels_probe.install(__import__(__name__, fromlist=["x"]))
