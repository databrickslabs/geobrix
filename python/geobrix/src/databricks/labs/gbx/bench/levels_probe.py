"""The one-worker readdir probe twins of the JVM forms (`--levels <family> --levels-forms <ids>`).

A JVM form (L1, L2) of a tile family gets two twins, `<variant>@probe-readdir` and `<variant>@probe-readdir-empty`. Each
runs the form's own query over `PROBE_CALLS` corpus tiles on ONE task (one partition, so one worker): the tiles are read
from the Volume inside the timed query (a light-tier open of each file), then the JVM function is called on each. The
first twin leaves `GDAL_DISABLE_READDIR_ON_OPEN` as shipped (the light tier picks the sidecar probe mode per file);
the `-empty` twin sets it to `EMPTY_DIR` on the worker before the opens. The pair says whether the mode moves the
form's time on a Volume (a sidecar listing on FUSE is slow); the JVM itself opens no Volume path in these queries (it
is handed the tile bytes), so a gap between the twins is the Volume read, and no gap is the answer for the JVM call.

The twins are opt-in (`Form.opt_in`): a default run of the set skips them, and `--levels-forms` names them.
"""

from __future__ import annotations

import dataclasses
import os
from typing import List

from databricks.labs.gbx.bench.levels import Form

PROBE_CALLS = 20  # tiles (and JVM calls) of one probe
READDIR_ENV = "GDAL_DISABLE_READDIR_ON_OPEN"
_ME = "databricks.labs.gbx.bench.levels_probe:"


def _probe_paths(spark, n, corpus, schema):
    """A one-partition table of the corpus' first `n` tile paths: one task opens them all."""
    from databricks.labs.gbx.bench import levels_forms as lf

    rows = corpus.rstrip("/") + "/rows"
    held = lf._corpus_rows(corpus)
    if held is None:
        return None
    lf.corpus_guard("probe_paths", rows, len(held), n)
    pairs = [(p,) for p in held[:n]]
    return lf._table(
        spark,
        schema,
        f"mvp_in_probe_paths_v{lf.RECIPE_VERSION}_{n}",
        lambda: lf.client_dataframe(
            spark, pairs, "path string", what="probe tile paths"
        ).repartition(1),
    )


def _force_empty_dir(paths):
    """A pandas UDF body: set the readdir mode of this worker process to EMPTY_DIR, pass the paths through."""
    os.environ[READDIR_ENV] = "EMPTY_DIR"
    return paths


def _probe_run(sql: str, empty: bool, name: str):
    def run(spark, src):
        from pyspark.sql import functions as F

        from databricks.labs.gbx.rasterx import functions as rx

        path = F.col("path")
        if empty:
            path = F.pandas_udf(_force_empty_dir, "string")(path)
        tiles = spark.table(src).select(
            rx.rst_fromfile(path, "GTiff", materialize=True).alias("tile")
        )
        view = f"_probe_tiles_{name}"
        tiles.createOrReplaceTempView(view)
        df = spark.sql(sql.format(src=view, uc=""))
        r = df.collect()[0]
        return r[0], r[1], r[2], df

    run.__name__ = name
    return run


def probe_twins(forms: List[Form]) -> List[Form]:
    """The twins of the JVM forms of `forms` that read a tile table through SQL."""
    out: List[Form] = []
    taken = set()
    for f in sorted(
        forms, key=lambda f: f.level
    ):  # L1 before L2: one twin pair per (fn, variant)
        if f.level not in ("L1", "L2") or f.input != "tiles_n" or not f.sql or f.run:
            continue
        if (f.fn, f.variant) in taken:
            continue
        taken.add((f.fn, f.variant))
        for empty in (False, True):
            suffix = "probe-readdir-empty" if empty else "probe-readdir"
            fn_name = (
                f"probe_{f.fn}_{f.level}_{f.variant}_{suffix}".replace("@", "_")
                .replace("-", "_")
                .replace(".", "_")
            )
            globals()[fn_name] = _probe_run(f.sql, empty, fn_name)
            out.append(
                dataclasses.replace(
                    f,
                    variant=f"{f.variant}@{suffix}",
                    input="probe_paths",
                    run=_ME + fn_name,
                    sql="",
                    computes=("dedicated",),
                    opt_in=True,
                    allows_null=f.allows_null,
                )
            )
    return out


def install(lf) -> None:
    """Add the `probe_paths` input and the twins of every family set (called once, after the family sets exist)."""
    lf.INPUTS["probe_paths"] = _probe_paths
    lf.BASE_ROWS["probe_paths"] = PROBE_CALLS
    for fam in list(lf.FORMS):
        have = {f.variant for f in lf.FORMS[fam]}
        twins = [t for t in probe_twins(lf.FORMS[fam]) if t.variant not in have]
        lf.FORMS[fam] = lf.FORMS[fam] + twins
