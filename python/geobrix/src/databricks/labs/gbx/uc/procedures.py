"""Governed ETL recipes as Unity Catalog SQL procedures over the UC functions.

Importing this module pulls in no pyspark. Each recipe is a function of ``(catalog, schema)`` that returns its
``CREATE OR REPLACE PROCEDURE`` text: ``LANGUAGE SQL``, ``SQL SECURITY INVOKER``, every call 3-level-qualified.
A recipe adds ETL flow inside Unity Catalog (read a table, call the GeoBrix functions, MERGE the result); it computes
nothing itself and writes no file.

The tables a recipe reads and writes are STRING parameters read through ``IDENTIFIER(...)``. The columns it reads
are fixed and named in the procedure's comment. A recipe whose precondition does not hold raises :class:`Skipped`
at install time and :func:`build_all` reports it in one line.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Optional, Tuple

from databricks.labs.gbx import __version__, manifest
from databricks.labs.gbx.uc import ddl

# The tree procedures merge or combine at most this many tiles in one function call (an aggregate over tile structs
# runs out of memory past about ten 512 x 512 tiles); a key of any size is a tree of such calls.
TREE_FAN_IN = 8
# A tree of fan-in 8 has 10 levels for 1e9 tiles; a loop that has not ended by then is stopped.
TREE_MAX_LEVELS = 10
# The largest tile a Unity Catalog function returns.
TREE_MAX_BYTES = 64 * 1024 * 1024
# Tile names: 1-128 characters, as the Zarr manifest accepts.
_TAG_RE = r"^[A-Za-z0-9][A-Za-z0-9._:/@+=-]{0,127}$"


class Skipped(Exception):
    """A recipe whose gate does not hold: the message is the one line install prints."""


@dataclass(frozen=True)
class Param:
    name: str
    type: str
    comment: str
    default: Optional[str] = None


def _version() -> str:
    return __version__.split("+", 1)[0]


def _q(catalog: str, schema: str) -> str:
    return f"{ddl.quote_ident('catalog', catalog)}.{ddl.quote_ident('schema', schema)}"


def _ddl(
    catalog: str,
    schema: str,
    name: str,
    summary: str,
    params: List[Param],
    body: str,
) -> str:
    """One procedure: parameters with comments, the mandatory characteristics, the compound statement."""
    q = _q(catalog, schema)
    sig = ",\n".join(
        f"  IN {p.name} {p.type}"
        + (f" DEFAULT {p.default}" if p.default is not None else "")
        + f" COMMENT {ddl.quote_str(p.comment)}"
        for p in params
    )
    comment = ddl.quote_str(f"geobrix {_version()}: {summary}")
    return (
        f"CREATE OR REPLACE PROCEDURE {q}.{name}(\n{sig}\n)\n"
        f"LANGUAGE SQL\nSQL SECURITY INVOKER\nCOMMENT {comment}\n"
        f"AS BEGIN\n{body.strip()}\nEND"
    )


def _need(*functions: str) -> None:
    """The gate of a recipe over functions that must be on Unity Catalog."""
    fns = manifest.load().functions
    missing = [n for n in functions if n not in fns or "L4" not in fns[n].levels]
    if missing:
        raise Skipped(f"needs {', '.join(missing)} on Unity Catalog")


# what a MERGE-only procedure says in its COMMENT: it upserts, it never deletes, and its key must be unique
_UPSERTS = "It upserts: a target row the source no longer has stays."
_UNIQUE_ID = "The id must be unique in the source: a duplicate makes the MERGE raise."


SRC = Param("source_table", "STRING", "The table to read, as catalog.schema.table.")
TGT = Param(
    "target_table",
    "STRING",
    "The table to merge the result into; created when missing.",
)


def _create_table(
    table: str, columns: str, source: str = "IDENTIFIER(source_table)"
) -> str:
    """CREATE TABLE IF NOT EXISTS ``table`` with the columns ``columns`` selected from ``source`` and no row.

    The columns are a list, not a second copy of the select that fills the table: ``WHERE false`` does not stop a
    function in the select list from being planned, and a Python function costs a sandbox start on a warehouse.
    A column that the source has (its key) takes the source's type; the others are typed NULLs.
    """
    return (
        f"CREATE TABLE IF NOT EXISTS IDENTIFIER({table}) AS\n"
        f"SELECT {columns} FROM {source} WHERE false;"
    )


def _create_like(select: str) -> str:
    """CREATE TABLE IF NOT EXISTS target with the columns of ``select`` and no row."""
    return (
        "CREATE TABLE IF NOT EXISTS IDENTIFIER(target_table) AS\n"
        f"SELECT * FROM (\n{select}\n) WHERE false;"
    )


def _with_srid(sql: str) -> str:
    """The SQL expression for ``sql`` as a STRING, its ``@SRID@`` marks replaced by the local variable ``v_srid``.

    A UC function returns GEOMETRY(ANY), which no table column takes (DELTA_GEOSPATIAL_SRID_NOT_SUPPORTED) and no
    assignment casts to GEOMETRY(<srid>). ``st_geomfromwkb(st_asbinary(geom), <srid>)`` has the type GEOMETRY(<srid>)
    only when the SRID is a literal of the statement, so the statement is built from ``v_srid`` and run with
    EXECUTE IMMEDIATE (its table names and values go in as parameters, never into the text).
    """
    return (
        "concat("
        + ", CAST(v_srid AS STRING), ".join(
            ddl.quote_str(part) for part in sql.split("@SRID@")
        )
        + ")"
    )


# --- polygons to BNG cells -------------------------------------------------------------------------------------------
def polygons_to_bng(catalog: str, schema: str) -> str:
    q = _q(catalog, schema)
    sel = (
        f"SELECT s.id, t.cellid, t.core, t.chip\n"
        f"FROM IDENTIFIER(source_table) AS s,\n"
        f"LATERAL {q}.gbx_bng_tessellateexplode(CAST(s.geom AS GEOMETRY(ANY)), bng_resolution, keep_core_geom) AS t"
    )
    body = f"""
{_create_like(sel)}
MERGE INTO IDENTIFIER(target_table) AS t
USING (
{sel}
) AS s
ON t.id <=> s.id AND t.cellid = s.cellid
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;"""
    return _ddl(
        catalog,
        schema,
        "gbx_polygons_to_bng",
        "Tessellate the polygons of a table (columns id, geom in SRID 27700) into British National Grid "
        "cells and merge them into a table of (id, cellid, core, chip). "
        + _UPSERTS
        + " "
        + _UNIQUE_ID,
        [
            SRC,
            TGT,
            Param(
                "bng_resolution",
                "STRING",
                "The BNG resolution, for example '100m' or '3'.",
            ),
            Param(
                "keep_core_geom", "BOOLEAN", "Keep the geometry of core cells.", "false"
            ),
        ],
        body,
    )


# --- the scratch table of a procedure that stages rows ---------------------------------------------------------------
# A staged table sits beside the target (the caller can create tables there) and is named once per call. The handler
# drops it when any statement fails, whether a refusal of the procedure or an error of a function; the last
# statement drops it when the call succeeds.
_SCRATCH_DECLARE = "DECLARE v_scratch STRING DEFAULT concat(target_table, '_scratch_', replace(uuid(), '-', ''));"
_SCRATCH_HANDLER = (
    "DECLARE EXIT HANDLER FOR SQLEXCEPTION\n"
    "BEGIN\n"
    "  DROP TABLE IF EXISTS IDENTIFIER(v_scratch);\n"
    "  RESIGNAL;\n"
    "END;"
)
_SCRATCH_DROP = "DROP TABLE IF EXISTS IDENTIFIER(v_scratch);"


# --- clean geometries --------------------------------------------------------------------------------------------
def clean_geometries(catalog: str, schema: str) -> str:
    q = _q(catalog, schema)

    def fixed(src: str, level: str) -> str:
        return (
            f"SELECT id, {q}.gbx_st_makevalid(CAST(geom AS GEOMETRY(ANY)), {level}) AS geom, geom AS original\n"
            f"FROM {src} WHERE geom IS NOT NULL"
        )

    def good(src: str, level: str) -> str:
        return (
            f"SELECT id, st_geomfromwkb(st_asbinary(geom), @SRID@) AS geom FROM (\n{fixed(src, level)}\n) "
            "WHERE geom IS NOT NULL AND st_isvalid(geom)"
        )

    bad = (
        f"SELECT id, original AS geom, "
        f"coalesce({q}.gbx_st_explainvalidity(CAST(original AS GEOMETRY(ANY))), "
        f"'the geometry could not be repaired') AS reason\n"
        f"FROM (\n{fixed('IDENTIFIER(source_table)', 'repair_level')}\n) WHERE geom IS NULL OR NOT st_isvalid(geom)\n"
        f"UNION ALL\nSELECT id, geom, 'the geometry is NULL' AS reason\n"
        f"FROM IDENTIFIER(source_table) WHERE geom IS NULL"
    )
    good_ids = (
        f"SELECT id FROM (\n{fixed('IDENTIFIER(source_table)', 'repair_level')}\n) "
        "WHERE geom IS NOT NULL AND st_isvalid(geom)"
    )
    merge = (
        "MERGE INTO IDENTIFIER(:t) AS t USING (\n"
        f"{good('IDENTIFIER(:s)', ':r')}\n"
        ") AS s ON t.id <=> s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )
    fix = (
        "gbx_clean_geometries: the geometries of the source have more than one SRID. Fix: clean the rows of each "
        "SRID into a table of their own."
    )
    # The target keeps the type of the source column (GEOMETRY(<srid>)); the repaired geometry is given that SRID.
    body = f"""
DECLARE v_srid INT;
SET v_srid = coalesce((SELECT min(st_srid(geom)) FROM IDENTIFIER(source_table)), 4326);
IF coalesce((SELECT max(st_srid(geom)) FROM IDENTIFIER(source_table)), v_srid) <> v_srid THEN
  SIGNAL SQLSTATE '22023' SET MESSAGE_TEXT = {ddl.quote_str(fix)};
END IF;
CREATE TABLE IF NOT EXISTS IDENTIFIER(target_table) AS
SELECT id, geom FROM IDENTIFIER(source_table) WHERE false;
CREATE TABLE IF NOT EXISTS IDENTIFIER(rejects_table) AS
SELECT * FROM (
{bad}
) WHERE false;
EXECUTE IMMEDIATE {_with_srid(merge)} USING target_table AS t, source_table AS s, repair_level AS r;
DELETE FROM IDENTIFIER(target_table) WHERE id IN (SELECT id FROM (
{bad}
));
MERGE INTO IDENTIFIER(rejects_table) AS t USING (
{bad}
) AS s ON t.id <=> s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *;
DELETE FROM IDENTIFIER(rejects_table) WHERE id IN (SELECT id FROM (
{good_ids}
));"""
    return _ddl(
        catalog,
        schema,
        "gbx_clean_geometries",
        "Repair the geometries of a table (columns id, geom, one SRID), merge the valid ones into a target table and "
        "merge the rest, with the reason, into a rejects table. Each id ends up in one table only: a valid id leaves "
        "the rejects, an invalid one leaves the target. Other rows of both tables stay. "
        + _UNIQUE_ID,
        [
            SRC,
            TGT,
            Param(
                "rejects_table",
                "STRING",
                "The table that receives the rows that could not be made valid.",
            ),
            Param(
                "repair_level",
                "STRING",
                "The repair level of gbx_st_makevalid.",
                "'linework'",
            ),
        ],
        body,
    )


# --- migrate Mosaic geometry ---------------------------------------------------------------------------------------
def migrate_mosaic_geometry(catalog: str, schema: str) -> str:
    q = _q(catalog, schema)
    sel = (
        f"SELECT * EXCEPT (geom), st_geomfromwkb(st_asbinary({q}.gbx_st_legacyaswkb(geom)), @SRID@) AS geom\n"
        f"FROM IDENTIFIER(:s)"
    )
    # The legacy struct carries its SRID per row. The table is made with the one SRID of the source, read first.
    create = (
        "CREATE TABLE IF NOT EXISTS IDENTIFIER(:t) AS SELECT * EXCEPT (geom), "
        "CAST(NULL AS GEOMETRY(@SRID@)) AS geom FROM IDENTIFIER(:s) WHERE false"
    )
    merge = (
        "MERGE INTO IDENTIFIER(:t) AS t USING (\n"
        f"{sel}\n"
        ") AS s ON t.id <=> s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )
    fix = (
        "gbx_migrate_mosaic_geometry: the geometries of the source have more than one SRID. Fix: migrate the rows "
        "of each SRID into a table of their own."
    )
    body = f"""
DECLARE v_srid INT;
SET v_srid = coalesce((SELECT min(geom.srid) FROM IDENTIFIER(source_table)), 4326);
IF coalesce((SELECT max(geom.srid) FROM IDENTIFIER(source_table)), v_srid) <> v_srid THEN
  SIGNAL SQLSTATE '22023' SET MESSAGE_TEXT = {ddl.quote_str(fix)};
END IF;
EXECUTE IMMEDIATE {_with_srid(create)} USING target_table AS t, source_table AS s;
EXECUTE IMMEDIATE {_with_srid(merge)} USING target_table AS t, source_table AS s;"""
    return _ddl(
        catalog,
        schema,
        "gbx_migrate_mosaic_geometry",
        "Convert a table with a legacy Mosaic geometry column (columns id, geom, one SRID) to a table with a "
        "GEOMETRY column of that SRID. " + _UPSERTS + " " + _UNIQUE_ID,
        [SRC, TGT],
        body,
    )


# --- points to an H3 surface -------------------------------------------------------------------------------------
def points_to_h3_surface(catalog: str, schema: str) -> str:
    q = _q(catalog, schema)
    cells = (
        "SELECT h3_longlatash3(longitude, latitude, h3_resolution) AS cellid, avg(value) AS value\n"
        "FROM IDENTIFIER(source_table) GROUP BY 1"
    )
    # The fill only changes the cells it is given: a neighbour that no point fell in is not a cell of the set and
    # would never get a value. So every cell within fill_rings of a cell is added with no value (the product
    # h3_kring); max() keeps the value of a cell that has one, as the NULL of its own padding row never wins.
    padded = (
        "WITH c AS (\n" + cells + "\n)\n"
        "SELECT cellid, max(value) AS value FROM (\n"
        "  SELECT cellid, value FROM c\n"
        "  UNION ALL\n"
        "  SELECT explode(h3_kring(cellid, greatest(fill_rings, 0))) AS cellid, CAST(NULL AS DOUBLE) AS value FROM c\n"
        ") GROUP BY cellid"
    )
    sel = (
        f"SELECT surface_name AS surface,\n"
        f"  {q}.gbx_rst_h3_rasterize(\n"
        f"    {q}.gbx__h3_cellfill_cells(g.cells, fill_rings, 'mean', 2.0),\n"
        f"    4326, CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE),\n"
        f"    CAST(NULL AS DOUBLE), CAST(NULL AS INT), CAST(NULL AS INT), 'centroids', 0, CAST(NULL AS STRING)) AS tile\n"
        f"FROM (SELECT collect_list(struct(cellid, value)) AS cells FROM (\n{padded}\n)) AS g"
    )
    create = _create_like(sel)
    body = f"""
{create}
MERGE INTO IDENTIFIER(target_table) AS t
USING (
{sel}
) AS s
ON t.surface <=> s.surface
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;"""
    return _ddl(
        catalog,
        schema,
        "gbx_points_to_h3_surface",
        "Average the values of a table of points (columns longitude, latitude, value) per H3 cell, add the "
        "empty cells within fill_rings of a cell, fill them from their neighbours and merge the surface, one "
        "GeoTIFF, into a table of (surface, tile). " + _UPSERTS,
        [
            SRC,
            TGT,
            Param(
                "surface_name",
                "STRING",
                "The name of the surface: the key of the target row.",
            ),
            Param(
                "h3_resolution", "INT", "The H3 resolution of the cells, from 0 to 15."
            ),
            Param(
                "fill_rings",
                "INT",
                "The rings of neighbours around a cell that are filled from the cells that have a value.",
                "1",
            ),
        ],
        body,
    )


# --- features to vector tiles ---------------------------------------------------------------------------------------
def features_to_mvt(catalog: str, schema: str) -> str:
    q = _q(catalog, schema)
    sel = (
        f"SELECT p.z, p.x, p.y,\n"
        f"  {q}.gbx__st_asmvt_features(collect_list(p.mvt_bytes), mvt_layer, 4096) AS mvt\n"
        f"FROM IDENTIFIER(source_table) AS s,\n"
        f"LATERAL {q}.gbx_st_asmvt_pyramid(CAST(s.geom AS GEOMETRY(ANY)), to_json(s.attrs), min_zoom, max_zoom, "
        f"mvt_layer, 4096) AS p\n"
        f"GROUP BY p.z, p.x, p.y"
    )
    body = f"""
{_create_like(sel)}
MERGE INTO IDENTIFIER(target_table) AS t
USING (
{sel}
) AS s
ON t.z = s.z AND t.x = s.x AND t.y = s.y
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;"""
    return _ddl(
        catalog,
        schema,
        "gbx_features_to_mvt",
        "Cut the features of a table (columns geom in EPSG:4326, attrs a struct) into Mapbox Vector Tiles and merge "
        "them into a table of (z, x, y, mvt). "
        + _UPSERTS
        + " A .pmtiles archive is built outside Unity Catalog.",
        [
            SRC,
            TGT,
            Param("mvt_layer", "STRING", "The layer name written into each tile."),
            Param("min_zoom", "INT", "The first zoom level, from 0."),
            Param("max_zoom", "INT", "The last zoom level, at most 20."),
        ],
        body,
    )


# --- the tree: many tiles per key ----------------------------------------------------------------------------------
# One function call merges or combines at most TREE_FAN_IN tiles. A key of any size is a tree of such calls: each level
# writes one row per bucket of a scratch table, and the next level reads those rows, until a key has one row. The
# buckets of a key are contiguous runs of the same size (to within one), so no call gets a lone member: a combine
# function returns a single tile as it is, which would make a "count" of one tile the tile itself.
_BUCKET = f"((rn - 1) * ((n + {TREE_FAN_IN - 1}) DIV {TREE_FAN_IN})) DIV n"


def _ranked(columns: str, source: str, order: str) -> str:
    """The rows of ``source`` with the bucket of each in its key; only the keys with more than one row."""
    return (
        f"SELECT tile_key, {columns}, {_BUCKET} AS bucket\n"
        "FROM (\n"
        f"  SELECT tile_key, {columns},\n"
        f"    row_number() OVER (PARTITION BY tile_key ORDER BY {order}) AS rn,\n"
        "    count(*) OVER (PARTITION BY tile_key) AS n\n"
        f"  FROM {source}\n"
        ") WHERE n > 1"
    )


def _scratch_create(parts: int) -> str:
    """The scratch table: key, level, bucket and ``parts`` tile columns, with no row, typed by a column list."""
    tile = f"CAST(NULL AS {manifest.TILE_SQL})"
    cols = ", ".join(f"{tile} AS p{i}" for i in range(1, parts + 1))
    return (
        "CREATE TABLE IDENTIFIER(v_scratch) AS\n"
        f"SELECT tile_key, CAST(0 AS INT) AS lvl, CAST(0 AS INT) AS bucket, {cols}\n"
        "FROM IDENTIFIER(source_table) WHERE false;"
    )


def _size_check(level: str, fix: str) -> str:
    return (
        f"IF EXISTS (SELECT 1 FROM IDENTIFIER(v_scratch) WHERE lvl = {level} AND length(p1.raster) > {TREE_MAX_BYTES}) THEN\n"
        f"  SIGNAL SQLSTATE '54000' SET MESSAGE_TEXT = {ddl.quote_str(fix)};\n"
        "END IF;"
    )


def _tree_loop(next_level: str, fix: str, name: str) -> str:
    """The loop over the levels. ``next_level`` is the statement (or the chain of them) that writes level v_lvl + 1."""
    cap = (
        f"{name}: a key needs more than {TREE_MAX_LEVELS} levels of {TREE_FAN_IN} tiles. "
        "Fix: split the key."
    )
    return f"""WHILE EXISTS (SELECT 1 FROM IDENTIFIER(v_scratch) WHERE lvl = v_lvl GROUP BY tile_key HAVING count(*) > 1) DO
  IF v_lvl >= {TREE_MAX_LEVELS} THEN
    SIGNAL SQLSTATE '54000' SET MESSAGE_TEXT = {ddl.quote_str(cap)};
  END IF;
-- next level
{next_level}
  SET v_lvl = v_lvl + 1;
  {_size_check("v_lvl", fix).replace(chr(10), chr(10) + "  ")}
END WHILE;"""


def _final_merge(tile_expr: str, columns: str = "p1") -> str:
    """The one write to the target: the keys that never entered the tree as they are, the others from their last level."""
    return f"""MERGE INTO IDENTIFIER(target_table) AS t
USING (
  SELECT s.tile_key, s.tile FROM IDENTIFIER(source_table) AS s
  LEFT ANTI JOIN (SELECT DISTINCT tile_key FROM IDENTIFIER(v_scratch)) AS k ON s.tile_key <=> k.tile_key
  UNION ALL
  SELECT tile_key, {tile_expr} AS tile
  FROM (SELECT tile_key, lvl, {columns}, max(lvl) OVER (PARTITION BY tile_key) AS top FROM IDENTIFIER(v_scratch))
  WHERE lvl = top
) AS s
ON t.tile_key <=> s.tile_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;"""


# the order in which the members of a key are grouped: by content, so a re-run groups the same way
_MEMBER_ORDER = "xxhash64(tile.raster, tile.path)"


# --- mosaic by key ----------------------------------------------------------------------------------------------------
def mosaic_by_key(catalog: str, schema: str) -> str:
    _need("gbx_rst_merge")
    q = _q(catalog, schema)
    merge = f"{q}.gbx_rst_merge(collect_list(%s))"
    fix = (
        "gbx_mosaic_by_key: the merged tile of a key is over 64 MiB, more than a Unity Catalog function returns. "
        "Fix: mosaic that key as a virtual raster. Keep its tiles as files on a Volume and build the mosaic with "
        "mint_vrt (the VRT mosaic page), or split the key into smaller areas."
    )
    first = (
        "INSERT INTO IDENTIFIER(v_scratch) (tile_key, lvl, bucket, p1)\n"
        f"SELECT tile_key, 1, bucket, {merge % 'tile'}\n"
        "FROM (\n"
        f"{_ranked('tile', 'IDENTIFIER(source_table)', _MEMBER_ORDER)}\n"
        ") GROUP BY tile_key, bucket;"
    )
    nxt = (
        "  INSERT INTO IDENTIFIER(v_scratch) (tile_key, lvl, bucket, p1)\n"
        f"  SELECT tile_key, v_lvl + 1, bucket, {merge % 'p1'}\n"
        "  FROM (\n"
        f"{_ranked('p1', 'IDENTIFIER(v_scratch)', 'bucket')}\n"
        "  ) GROUP BY tile_key, bucket;"
    ).replace(
        "FROM IDENTIFIER(v_scratch)\n", "FROM IDENTIFIER(v_scratch) WHERE lvl = v_lvl\n"
    )
    create = _create_table(
        "target_table", f"tile_key, CAST(NULL AS {manifest.TILE_SQL}) AS tile"
    )
    body = f"""
DECLARE v_lvl INT DEFAULT 1;
{_SCRATCH_DECLARE}
{_SCRATCH_HANDLER}
{create}
{_scratch_create(1)}
-- level 1
{first}
{_size_check("1", fix)}
{_tree_loop(nxt, fix, "gbx_mosaic_by_key")}
-- result
{_final_merge("p1")}
{_SCRATCH_DROP}"""
    return _ddl(
        catalog,
        schema,
        "gbx_mosaic_by_key",
        f"Merge the tiles of a table (columns tile_key, tile) into one tile per key, in a tree of merges of at most "
        f"{TREE_FAN_IN} tiles each, so a key may have any number of tiles. A merged tile over 64 MiB is refused. "
        "The tiles of a key should not overlap: which of two overlapping tiles shows is not defined. "
        + _UPSERTS,
        [SRC, TGT],
        body,
    )


# --- combine by key ---------------------------------------------------------------------------------------------------
# op -> (the partial tile columns, the combine function of level 1 per column, of the levels above per column, the final
# expression over the partials or None when p1 is the result). The members of avg and stddev are cast to Float64 first
# (a sum in the member type wraps: eight uint8 tiles of 200). The partials of stddev are the count A, the sum B and the
# sum of squares C: the population standard deviation is sqrt(C / A - (B / A) ** 2), the combinestddev of the session.
_COMBINE_OPS = ("avg", "sum", "count", "min", "max", "stddev")
_EXPR = {"avg": "B / A", "stddev": "sqrt(abs(C / A - (B / A) ** 2))"}


def _combine_level1(q: str, op: str) -> str:
    cols = "tile_key, lvl, bucket, p1" + (", p2" if op in ("avg", "stddev") else "")
    cols += ", p3" if op == "stddev" else ""
    ranked = _ranked("tile", "IDENTIFIER(source_table)", _MEMBER_ORDER)
    if op in ("sum", "min", "max", "count"):
        fn = "combinecount" if op == "count" else f"combine{op}"
        sel = f"SELECT tile_key, 1, bucket, {q}.gbx_rst_{fn}(collect_list(tile)) AS p1\nFROM (\n{ranked}\n) GROUP BY tile_key, bucket"
    else:
        # u is the member as Float64, made once per member; for stddev sq is its square (Float32 from the map algebra)
        cast = f"{q}.gbx_rst_updatetype(tile, 'Float64') AS u"
        inner = f"SELECT tile_key, bucket, {cast}\nFROM (\n{ranked}\n)"
        if op == "avg":
            sel = (
                f"SELECT tile_key, 1, bucket, {q}.gbx_rst_combinecount(collect_list(u)) AS p1,\n"
                f"  {q}.gbx_rst_combinesum(collect_list(u)) AS p2\n"
                f"FROM (\n{inner}\n) GROUP BY tile_key, bucket"
            )
        else:
            sq = f"{q}.gbx_rst_mapalgebra(array(u), 'A * A') AS sq"
            sel = (
                f"SELECT tile_key, 1, bucket, {q}.gbx_rst_combinecount(collect_list(u)) AS p1,\n"
                f"  {q}.gbx_rst_combinesum(collect_list(u)) AS p2, {q}.gbx_rst_combinesum(collect_list(sq)) AS p3\n"
                f"FROM (SELECT tile_key, bucket, u, {sq}\nFROM (\n{inner}\n)) GROUP BY tile_key, bucket"
            )
    return f"INSERT INTO IDENTIFIER(v_scratch) ({cols})\n{sel};"


def _combine_next(q: str, op: str) -> str:
    parts = {"avg": 2, "stddev": 3}.get(op, 1)
    cols = "tile_key, lvl, bucket, " + ", ".join(f"p{i}" for i in range(1, parts + 1))
    fn = {
        "sum": "combinesum",
        "count": "combinesum",
        "min": "combinemin",
        "max": "combinemax",
    }.get(op, "combinesum")
    aggs = ", ".join(
        f"{q}.gbx_rst_{fn}(collect_list(p{i})) AS p{i}" for i in range(1, parts + 1)
    )
    ranked = _ranked(
        ", ".join(f"p{i}" for i in range(1, parts + 1)),
        "IDENTIFIER(v_scratch)",
        "bucket",
    )
    ranked = ranked.replace(
        "  FROM IDENTIFIER(v_scratch)\n",
        "  FROM IDENTIFIER(v_scratch) WHERE lvl = v_lvl\n",
    )
    return (
        f"INSERT INTO IDENTIFIER(v_scratch) ({cols})\n"
        f"SELECT tile_key, v_lvl + 1, bucket, {aggs}\nFROM (\n{ranked}\n) GROUP BY tile_key, bucket;"
    )


def _chain(statements: Dict[str, str]) -> str:
    """``IF v_op = 'a' THEN ... ELSEIF v_op = 'b' THEN ... END IF;`` over the ops, one statement each."""
    out = []
    for i, op in enumerate(_COMBINE_OPS):
        out.append(
            f"{'IF' if i == 0 else 'ELSEIF'} v_op = '{op}' THEN\n{statements[op]}"
        )
    return "\n".join(out) + "\nEND IF;"


def combine_by_key(catalog: str, schema: str) -> str:
    _need(
        "gbx_rst_combinesum", "gbx_rst_combinecount", "gbx_rst_combinemin", "gbx_rst_combinemax",
        "gbx_rst_updatetype", "gbx_rst_mapalgebra",
    )  # fmt: skip
    q = _q(catalog, schema)
    fix = (
        "gbx_combine_by_key: the combined tile of a key is over 64 MiB, more than a Unity Catalog function returns. "
        "Fix: combine that key in a notebook or job with the session aggregate, or split the key into smaller areas."
    )
    median = (
        "gbx_combine_by_key: median does not split into partial tiles, so it cannot be combined in a tree. "
        "Fix: combine at most 10 tiles per key with gbx_rst_combinemedian, or retile the key first."
    )
    other = (
        "gbx_combine_by_key: op must be one of avg, sum, count, min, max or stddev. "
        "Fix: pass one of them (median is refused: it does not split into partial tiles)."
    )
    first = _chain({op: _combine_level1(q, op) for op in _COMBINE_OPS})
    nxt = _chain({op: _combine_next(q, op) for op in _COMBINE_OPS})
    nxt = "\n".join("  " + line for line in nxt.split("\n"))

    def final(op: str) -> str:
        if op not in _EXPR:
            return _final_merge("p1")
        parts = 3 if op == "stddev" else 2
        cols = ", ".join(f"p{i}" for i in range(1, parts + 1))
        expr = f"{q}.gbx_rst_mapalgebra(array({cols}), {ddl.quote_str(_EXPR[op])})"
        return _final_merge(expr, cols)

    create = _create_table(
        "target_table", f"tile_key, CAST(NULL AS {manifest.TILE_SQL}) AS tile"
    )
    body = f"""
DECLARE v_op STRING DEFAULT lower(trim(op));
DECLARE v_lvl INT DEFAULT 1;
{_SCRATCH_DECLARE}
{_SCRATCH_HANDLER}
IF v_op = 'median' THEN
  SIGNAL SQLSTATE '22023' SET MESSAGE_TEXT = {ddl.quote_str(median)};
ELSEIF v_op IS NULL OR v_op NOT IN ({", ".join(ddl.quote_str(o) for o in _COMBINE_OPS)}) THEN
  SIGNAL SQLSTATE '22023' SET MESSAGE_TEXT = {ddl.quote_str(other)};
END IF;
{create}
{_scratch_create(3)}
-- level 1
{first}
{_size_check("1", fix)}
{_tree_loop(nxt, fix, "gbx_combine_by_key")}
-- result
{_chain({op: final(op) for op in _COMBINE_OPS})}
{_SCRATCH_DROP}"""
    return _ddl(
        catalog,
        schema,
        "gbx_combine_by_key",
        f"Combine the aligned tiles of a table (columns tile_key, tile) pixel by pixel into one tile per key with op "
        f"avg, sum, count, min, max or stddev (the population standard deviation), in a tree of partial tiles of at most "
        f"{TREE_FAN_IN} members each, so a key may have any number of tiles. Pixels with no data are left out, as in the "
        "session combine functions. avg and stddev return Float32 tiles; median is refused. A key with one tile keeps "
        "that tile. " + _UPSERTS,
        [
            SRC,
            TGT,
            Param(
                "op",
                "STRING",
                "The statistic: avg, sum, count, min, max or stddev.",
            ),
        ],
        body,
    )


# --- zonal statistics ------------------------------------------------------------------------------------------------
def zonal_stats(catalog: str, schema: str) -> str:
    _need("gbx_rst_clip", "gbx_rst_summary")
    q = _q(catalog, schema)
    sel = (
        f"SELECT z.zone_id, t.tile_id,\n"
        f"  {q}.gbx_rst_summary({q}.gbx_rst_clip(t.tile, CAST(z.geom AS GEOMETRY(ANY)), true, CAST(NULL AS STRING))) "
        f"AS summary\n"
        f"FROM IDENTIFIER(zones_table) AS z\n"
        f"JOIN IDENTIFIER(source_table) AS t ON st_intersects(z.geom, t.bbox)"
    )
    body = f"""
{_create_like(sel)}
MERGE INTO IDENTIFIER(target_table) AS t
USING (
{sel}
) AS s
ON t.zone_id <=> s.zone_id AND t.tile_id <=> s.tile_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;"""
    return _ddl(
        catalog,
        schema,
        "gbx_zonal_stats",
        "Clip the tiles of a table (columns tile_id, tile, bbox) to the zones of another (columns zone_id, geom) "
        "where their boxes meet, and merge each summary into a table of (zone_id, tile_id, summary). "
        + _UPSERTS
        + " The zone_id must be unique in the zones and the tile_id in the tiles: a duplicate makes the MERGE raise.",
        [
            Param("zones_table", "STRING", "The zones, as catalog.schema.table."),
            Param("source_table", "STRING", "The tiles, as catalog.schema.table."),
            TGT,
        ],
        body,
    )


# --- Zarr manifest: tag, untag, history -----------------------------------------------------------------------------
def _need_zarr_manifest() -> Tuple[str, ...]:
    """The gate of the Zarr recipes: the manifest table has the tag column and the commit rows they edit."""
    from databricks.labs.gbx.zarr import _manifest as M

    if "tag" not in M.MANIFEST_COLUMNS or "commit" not in M.KINDS:
        raise Skipped("the Zarr manifest table has no tag or commit rows")
    return tuple(M.MANIFEST_COLUMNS)


_MANIFEST = Param(
    "manifest_table",
    "STRING",
    "The Zarr store's manifest table, as catalog.schema.table.",
)
_TAG = Param(
    "tag_name", "STRING", "The tag: 1-128 characters of A-Z a-z 0-9 . _ : / @ + = - ."
)


def _tag_check() -> str:
    return (
        f"IF tag_name IS NULL OR NOT regexp_like(tag_name, {ddl.quote_str(_TAG_RE)}) THEN\n"
        "  SIGNAL SQLSTATE '22023' SET MESSAGE_TEXT = concat('tag name ', coalesce(tag_name, 'NULL'), "
        "' is not valid. Fix: use 1-128 characters of A-Z a-z 0-9 . _ : / @ + = - , starting with a letter or digit.');\n"
        "END IF;"
    )


def zarr_tag_version(catalog: str, schema: str) -> str:
    cols = _need_zarr_manifest()
    names = ", ".join(cols)
    values = ", ".join(f"s.{c}" for c in cols)
    pick = ", ".join(c for c in cols if c not in ("tag", "write_id", "meta"))
    # `from` is the version of the manifest the tag copies, as the Python tag records it (the latest commit)
    meta = "to_json(named_struct('mode', 'tag', 'tag', tag_name, 'write_id', v_write_id, 'from', v_from))"
    body = f"""
DECLARE v_write_id STRING DEFAULT replace(uuid(), '-', '');
DECLARE v_from BIGINT;
{_tag_check()}
IF EXISTS (SELECT 1 FROM IDENTIFIER(manifest_table) WHERE tag = tag_name) THEN
  SIGNAL SQLSTATE '22023' SET MESSAGE_TEXT = concat('tag ', tag_name, ' already exists. Fix: pick another name, or remove it with gbx_zarr_untag_version.');
END IF;
SET v_from = (SELECT max(_metadata.row_commit_version) FROM IDENTIFIER(manifest_table) WHERE tag IS NULL);
MERGE INTO IDENTIFIER(manifest_table) AS t
USING (
  SELECT {pick}, write_id, tag_name AS tag, meta
  FROM IDENTIFIER(manifest_table)
  WHERE tag IS NULL AND kind IN ('group', 'array', 'shard', 'chunk')
  UNION ALL
  SELECT 'commit' AS kind, CAST(NULL AS STRING) AS array, v_write_id AS key, CAST(NULL AS STRING) AS shard_key,
    NULL AS file, CAST(NULL AS BIGINT) AS offset, CAST(NULL AS BIGINT) AS length, CAST(NULL AS STRING) AS checksum,
    CAST(NULL AS STRING) AS origin, CAST(NULL AS STRING) AS identity, v_write_id AS write_id,
    CAST(NULL AS STRING) AS tag, {meta} AS meta
) AS s
ON t.kind = s.kind AND t.array <=> s.array AND t.key <=> s.key AND t.tag <=> s.tag
WHEN MATCHED AND s.kind <> 'commit' THEN UPDATE SET
  kind = raise_error(concat('tag ', tag_name, ' already exists. Fix: pick another name, or remove it with gbx_zarr_untag_version.'))
WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({values});"""
    return _ddl(
        catalog,
        schema,
        "gbx_zarr_tag_version",
        "Name the live version of a Zarr store so it outlives vacuum: copy its rows under the tag, in one MERGE. "
        "A tag that exists raises.",
        [_MANIFEST, _TAG],
        body,
    )


def zarr_untag_version(catalog: str, schema: str) -> str:
    _need_zarr_manifest()
    meta = "to_json(named_struct('mode', 'untag', 'tag', tag_name, 'write_id', v_write_id))"
    body = f"""
DECLARE v_write_id STRING DEFAULT replace(uuid(), '-', '');
{_tag_check()}
IF NOT EXISTS (SELECT 1 FROM IDENTIFIER(manifest_table) WHERE tag = tag_name) THEN
  SIGNAL SQLSTATE '22023' SET MESSAGE_TEXT = concat('tag ', tag_name, ' does not exist. Fix: pass an existing tag name.');
END IF;
MERGE INTO IDENTIFIER(manifest_table) AS t
USING (
  SELECT kind, array, key, tag FROM IDENTIFIER(manifest_table) WHERE tag = tag_name
  UNION ALL
  SELECT 'commit', CAST(NULL AS STRING), v_write_id, CAST(NULL AS STRING)
) AS s
ON t.kind = s.kind AND t.array <=> s.array AND t.key <=> s.key AND t.tag <=> s.tag
WHEN MATCHED AND s.kind <> 'commit' THEN DELETE
WHEN NOT MATCHED AND s.kind = 'commit' THEN INSERT (kind, key, write_id, meta)
  VALUES ('commit', v_write_id, v_write_id, {meta});"""
    return _ddl(
        catalog,
        schema,
        "gbx_zarr_untag_version",
        "Remove a tag of a Zarr store: delete the rows under it and record the commit, in one MERGE.",
        [_MANIFEST, _TAG],
        body,
    )


def zarr_version_history(catalog: str, schema: str) -> str:
    _need_zarr_manifest()
    body = """
SELECT _metadata.row_commit_version AS version, key AS write_id, get_json_object(meta, '$.mode') AS mode,
  from_json(get_json_object(meta, '$.variables'), 'array<string>') AS variables,
  from_json(get_json_object(meta, '$.region'), 'map<string,array<bigint>>') AS region
FROM IDENTIFIER(manifest_table)
WHERE kind = 'commit' AND tag IS NULL
ORDER BY version;"""
    return _ddl(
        catalog,
        schema,
        "gbx_zarr_version_history",
        "List the commits of a Zarr store, oldest first: version, write id, mode, arrays written and region.",
        [_MANIFEST],
        body,
    )


# recipe name -> the builder over (catalog, schema). The name is the public procedure name.
RECIPES: Dict[str, Callable[[str, str], str]] = {
    "gbx_polygons_to_bng": polygons_to_bng,
    "gbx_clean_geometries": clean_geometries,
    "gbx_migrate_mosaic_geometry": migrate_mosaic_geometry,
    "gbx_points_to_h3_surface": points_to_h3_surface,
    "gbx_features_to_mvt": features_to_mvt,
    "gbx_mosaic_by_key": mosaic_by_key,
    "gbx_combine_by_key": combine_by_key,
    "gbx_zonal_stats": zonal_stats,
    "gbx_zarr_tag_version": zarr_tag_version,
    "gbx_zarr_untag_version": zarr_untag_version,
    "gbx_zarr_version_history": zarr_version_history,
}

# the UC functions a recipe calls, for the install order and the collision gate
CALLS: Dict[str, Tuple[str, ...]] = {
    "gbx_polygons_to_bng": ("gbx_bng_tessellateexplode",),
    "gbx_clean_geometries": ("gbx_st_makevalid", "gbx_st_explainvalidity"),
    "gbx_migrate_mosaic_geometry": ("gbx_st_legacyaswkb",),
    "gbx_points_to_h3_surface": ("gbx_rst_h3_rasterize", "gbx__h3_cellfill_cells"),
    "gbx_features_to_mvt": ("gbx_st_asmvt_pyramid", "gbx__st_asmvt_features"),
    "gbx_mosaic_by_key": ("gbx_rst_merge",),
    "gbx_combine_by_key": (
        "gbx_rst_combinesum",
        "gbx_rst_combinecount",
        "gbx_rst_combinemin",
        "gbx_rst_combinemax",
        "gbx_rst_updatetype",
        "gbx_rst_mapalgebra",
    ),
    "gbx_zonal_stats": ("gbx_rst_clip", "gbx_rst_summary"),
}


def build_all(
    catalog: str,
    schema: str,
    only: Optional[List[str]] = None,
    available: Optional[Iterable[str]] = None,
) -> Tuple[List[Tuple[str, str]], Dict[str, str]]:
    """``([(name, DDL)], {skipped name: reason})`` for every recipe (or those in ``only``), in a fixed order.

    ``available``, when given, is the set of public functions being installed: a recipe that calls a public function
    outside it is skipped, with the reason, instead of being built over a function that will not exist.
    """
    ddl.quote_ident("catalog", catalog)
    ddl.quote_ident("schema", schema)
    built, skipped = [], {}
    for name, builder in RECIPES.items():
        if only is not None and name not in only:
            continue
        if available is not None:
            missing = [
                c
                for c in CALLS.get(name, ())
                if not c.startswith("gbx__") and c not in available
            ]
            if missing:
                skipped[name] = (
                    f"needs {', '.join(missing)}, which are not being installed"
                )
                continue
        try:
            built.append((name, builder(catalog, schema)))
        except Skipped as why:
            skipped[name] = str(why)
    return built, skipped
