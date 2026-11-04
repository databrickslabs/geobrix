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
from typing import Callable, Dict, List, Optional, Tuple

from databricks.labs.gbx import __version__, manifest
from databricks.labs.gbx.uc import ddl

# The UC ceiling of an aggregate over tile structs: ten tiles of 512 x 512 pass, fifty run out of memory.
MOSAIC_MAX_TILES = 10
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


SRC = Param("source_table", "STRING", "The table to read, as catalog.schema.table.")
TGT = Param(
    "target_table",
    "STRING",
    "The table to merge the result into; created when missing.",
)


def _create_like(select: str) -> str:
    """CREATE TABLE IF NOT EXISTS target with the columns of ``select`` and no row."""
    return (
        "CREATE TABLE IF NOT EXISTS IDENTIFIER(target_table) AS\n"
        f"SELECT * FROM (\n{select}\n) WHERE false;"
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
        "cells and merge them into a table of (id, cellid, core, chip).",
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


# --- clean geometries --------------------------------------------------------------------------------------------
def clean_geometries(catalog: str, schema: str) -> str:
    q = _q(catalog, schema)
    fixed = (
        f"SELECT id, {q}.gbx_st_makevalid(geom, repair_level) AS geom, geom AS original\n"
        f"FROM IDENTIFIER(source_table) WHERE geom IS NOT NULL"
    )
    good = f"SELECT id, geom FROM (\n{fixed}\n) WHERE geom IS NOT NULL AND st_isvalid(geom)"
    bad = (
        f"SELECT id, original AS geom, "
        f"coalesce({q}.gbx_st_explainvalidity(original), 'the geometry could not be repaired') AS reason\n"
        f"FROM (\n{fixed}\n) WHERE geom IS NULL OR NOT st_isvalid(geom)\n"
        f"UNION ALL\nSELECT id, geom, 'the geometry is NULL' AS reason\n"
        f"FROM IDENTIFIER(source_table) WHERE geom IS NULL"
    )
    # The target keeps the type of the source column: a UC function returns GEOMETRY(ANY), and Delta takes no column
    # of an unspecified SRID, so the table is not made from the type of the repaired expression.
    body = f"""
CREATE TABLE IF NOT EXISTS IDENTIFIER(target_table) AS
SELECT id, geom FROM IDENTIFIER(source_table) WHERE false;
CREATE TABLE IF NOT EXISTS IDENTIFIER(rejects_table) AS
SELECT * FROM (
{bad}
) WHERE false;
MERGE INTO IDENTIFIER(target_table) AS t
USING (
{good}
) AS s
ON t.id <=> s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
DELETE FROM IDENTIFIER(rejects_table);
INSERT INTO IDENTIFIER(rejects_table)
{bad};"""
    return _ddl(
        catalog,
        schema,
        "gbx_clean_geometries",
        "Repair the geometries of a table (columns id, geom), merge the valid ones into a target table and "
        "write the rest, with the reason, to a rejects table.",
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
        f"SELECT * EXCEPT (geom), {q}.gbx_st_legacyaswkb(geom) AS geom\n"
        f"FROM IDENTIFIER(source_table)"
    )
    # The legacy struct carries its SRID per row, the function returns GEOMETRY(ANY) and Delta takes no column of an
    # unspecified SRID: the table is made with the one SRID of the source, read first, by a statement built from it.
    create = (
        "EXECUTE IMMEDIATE concat('CREATE TABLE IF NOT EXISTS IDENTIFIER(:t) AS SELECT * EXCEPT (geom), "
        "CAST(NULL AS GEOMETRY(', CAST(v_srid AS STRING), ')) AS geom FROM IDENTIFIER(:s) WHERE false') "
        "USING target_table AS t, source_table AS s;"
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
{create}
MERGE INTO IDENTIFIER(target_table) AS t
USING (
{sel}
) AS s
ON t.id <=> s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;"""
    return _ddl(
        catalog,
        schema,
        "gbx_migrate_mosaic_geometry",
        "Convert a table with a legacy Mosaic geometry column (columns id, geom, one SRID) to a table with a "
        "GEOMETRY column of that SRID.",
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
    sel = (
        f"SELECT surface_name AS surface,\n"
        f"  {q}.gbx_rst_h3_rasterize(\n"
        f"    {q}.gbx__h3_cellfill_cells(g.cells, fill_rings, 'mean', 2.0),\n"
        f"    4326, CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE),\n"
        f"    CAST(NULL AS DOUBLE), CAST(NULL AS INT), CAST(NULL AS INT), 'centroids', 0, CAST(NULL AS STRING)) AS tile\n"
        f"FROM (SELECT collect_list(struct(cellid, value)) AS cells FROM ({cells})) AS g"
    )
    body = f"""
{_create_like(sel)}
MERGE INTO IDENTIFIER(target_table) AS t
USING (
{sel}
) AS s
ON t.surface = s.surface
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;"""
    return _ddl(
        catalog,
        schema,
        "gbx_points_to_h3_surface",
        "Average the values of a table of points (columns longitude, latitude, value) per H3 cell, fill the "
        "empty neighbours and merge the surface, one GeoTIFF, into a table of (surface, tile).",
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
                "The rings of neighbours a cell without a value is filled from.",
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
        "them into a table of (z, x, y, mvt). A .pmtiles archive is built outside Unity Catalog.",
        [
            SRC,
            TGT,
            Param("mvt_layer", "STRING", "The layer name written into each tile."),
            Param("min_zoom", "INT", "The first zoom level, from 0."),
            Param("max_zoom", "INT", "The last zoom level, at most 20."),
        ],
        body,
    )


# --- mosaic by key ----------------------------------------------------------------------------------------------------
def mosaic_by_key(catalog: str, schema: str) -> str:
    _need("gbx_rst_merge", "gbx_rst_frombands")
    q = _q(catalog, schema)
    sel = (
        f"SELECT tile_key, {q}.gbx_rst_merge(collect_list(tile)) AS tile\n"
        f"FROM IDENTIFIER(source_table) GROUP BY tile_key"
    )
    fix = (
        f"gbx_mosaic_by_key: a tile_key has more than {MOSAIC_MAX_TILES} tiles, over what a Unity Catalog "
        "aggregate holds. Fix: merge those keys in a notebook or job with the session aggregate "
        "gbx_rst_merge_agg, or split the key."
    )
    body = f"""
IF EXISTS (SELECT 1 FROM IDENTIFIER(source_table) GROUP BY tile_key HAVING count(*) > {MOSAIC_MAX_TILES}) THEN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = {ddl.quote_str(fix)};
END IF;
{_create_like(sel)}
MERGE INTO IDENTIFIER(target_table) AS t
USING (
{sel}
) AS s
ON t.tile_key = s.tile_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;"""
    return _ddl(
        catalog,
        schema,
        "gbx_mosaic_by_key",
        f"Merge the tiles of a table (columns tile_key, tile) into one tile per key, at most {MOSAIC_MAX_TILES} "
        "tiles per key.",
        [SRC, TGT],
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
        "where their boxes meet, and merge each summary into a table of (zone_id, tile_id, summary).",
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
        f"IF NOT regexp_like(tag_name, {ddl.quote_str(_TAG_RE)}) THEN\n"
        "  SIGNAL SQLSTATE '22023' SET MESSAGE_TEXT = concat('tag name ', coalesce(tag_name, 'NULL'), "
        "' is not valid. Fix: use 1-128 characters of A-Z a-z 0-9 . _ : / @ + = - , starting with a letter or digit.');\n"
        "END IF;"
    )


def zarr_tag_version(catalog: str, schema: str) -> str:
    cols = _need_zarr_manifest()
    names = ", ".join(cols)
    values = ", ".join(f"s.{c}" for c in cols)
    pick = ", ".join(c for c in cols if c not in ("tag", "write_id", "meta"))
    meta = (
        "to_json(named_struct('mode', 'tag', 'tag', tag_name, 'write_id', v_write_id))"
    )
    body = f"""
DECLARE v_write_id STRING DEFAULT replace(uuid(), '-', '');
{_tag_check()}
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
    "gbx_zonal_stats": ("gbx_rst_clip", "gbx_rst_summary"),
}


def build_all(
    catalog: str, schema: str, only: Optional[List[str]] = None
) -> Tuple[List[Tuple[str, str]], Dict[str, str]]:
    """``([(name, DDL)], {skipped name: reason})`` for every recipe (or those in ``only``), in a fixed order."""
    ddl.quote_ident("catalog", catalog)
    ddl.quote_ident("schema", schema)
    built, skipped = [], {}
    for name, builder in RECIPES.items():
        if only is not None and name not in only:
            continue
        try:
            built.append((name, builder(catalog, schema)))
        except Skipped as why:
            skipped[name] = str(why)
    return built, skipped
