"""Temporary SQL functions the manager composes over private level names.

The dispatch wrapper sends virtual tiles (a path, no raster) to light and materialized tiles to the
JVM. Each branch gets NULL for the other kind, so no level ships bytes it will not use, however
Spark extracts UDFs from the CASE. An error tile (no raster, no path, a diagnostic) comes out as it
went in, on every level.
"""

from databricks.labs.gbx.manifest import TILE_SQL, sql_type

_WINDOW = "struct<col_off:int,row_off:int,width:int,height:int>"


def _sig(fn) -> str:
    return ", ".join(
        f"{p.name} {sql_type(p.session[0])}"
        + (f" DEFAULT {p.default}" if p.default is not None else "")
        for p in fn.params
    )


def tile_dispatch(fn, jvm_call: str, l3_name: str) -> str:
    """CREATE FUNCTION text for a tile-first function.

    ``jvm_call`` is the JVM branch with ``{tile}`` (the tile argument, NULL unless it is
    materialized) and ``{rest}`` (the other arguments) as placeholders. A NULL required argument
    gives NULL. A tile with neither bytes nor a path is an error tile when it carries a diagnostic
    (it goes out unchanged, so the upstream reason survives) and NULL otherwise, as in the light tier.
    """
    t = fn.params[0].name
    others = [p.name for p in fn.params[1:]]
    rest = ", ".join(others)
    required = " OR ".join(f"{p.name} IS NULL" for p in fn.params[: fn.min_args])
    virt = f"(IF({t}.raster IS NULL AND {t}.path IS NOT NULL, {t}, NULL))"
    mat = f"(IF({t}.raster IS NOT NULL, {t}, NULL))"
    light = ", ".join([virt] + others)
    return (
        f"CREATE OR REPLACE TEMPORARY FUNCTION {fn.sql_name}({_sig(fn)}) "
        f"RETURNS {sql_type(fn.returns_session)} RETURN CASE WHEN {required} THEN NULL "
        f"WHEN {t}.raster IS NULL AND {t}.path IS NOT NULL THEN {l3_name}({light}) "
        f"WHEN {t}.raster IS NOT NULL THEN {jvm_call.format(tile=mat, rest=rest)} "
        f"WHEN map_contains_key({t}.metadata, 'last_error') "
        f"OR map_contains_key({t}.metadata, 'error_message') THEN {t} END"
    )


def tile_from_raster(raster_expr: str, tile: str) -> str:
    """A v2 tile around the bytes a Java UDF returned.

    NULL bytes mean the kernel failed on this tile: an error tile (no raster, no path) with the
    reason under ``last_error``, as on every other level. The bytes are computed once (the array
    lambda), not once per field.
    """
    reason = "the heavy kernel could not process this tile (see the executor log)"
    return (
        "transform(array({r}), gbx_r -> named_struct('cellid', {t}.cellid, 'raster', gbx_r, "
        "'path', CAST(NULL AS STRING), 'path_mode', CAST(NULL AS STRING), "
        "'window', CAST(NULL AS "
        + _WINDOW
        + "), 'clip_polygon', CAST(NULL AS BINARY), "
        "'clip_crs', CAST(NULL AS STRING), 'crs', CAST(NULL AS STRING), "
        "'metadata', IF(gbx_r IS NULL, map('last_error', '" + reason + "'), "
        "CAST(map() AS map<string,string>))))[0]"
    ).format(t=tile, r=raster_expr)


def tile_from_result(result_expr: str, tile: str) -> str:
    """A v2 tile around the engine's batch result (``struct<raster, error, command>``).

    All three NULL is a NULL result (a NULL argument). An error is an error tile (no raster, no path) with
    GDAL's reason under ``last_error`` and ``error_message``; a success carries the format and the command,
    the metadata every engine level writes. The result is computed once (the array lambda).
    """
    return (
        "transform(array({r}), gbx_r -> IF(gbx_r.raster IS NULL AND gbx_r.error IS NULL, "
        "CAST(NULL AS "
        + TILE_SQL
        + "), named_struct('cellid', {t}.cellid, 'raster', gbx_r.raster, "
        "'path', CAST(NULL AS STRING), 'path_mode', CAST(NULL AS STRING), "
        "'window', CAST(NULL AS "
        + _WINDOW
        + "), 'clip_polygon', CAST(NULL AS BINARY), "
        "'clip_crs', CAST(NULL AS STRING), 'crs', CAST(NULL AS STRING), "
        "'metadata', IF(gbx_r.error IS NULL, "
        "map('driver', 'GTiff', 'compression', 'ZSTD', 'last_command', gbx_r.command), "
        "map('last_error', gbx_r.error, 'error_message', gbx_r.error)))))[0]"
    ).format(t=tile, r=result_expr)


def tiles_dispatch(fn, plain_call: str, l3_name: str) -> str:
    """CREATE FUNCTION text for a function whose first argument is an array of tiles.

    ``plain_call`` (with ``{tiles}`` and ``{rest}`` placeholders) serves an array of materialized tiles; any
    other array (a virtual, NULL, empty or error tile in it) goes to the row-wise light UDF ``l3_name``, which
    materializes virtual tiles and gives an error tile out unchanged. As in ``tile_dispatch``, each branch gets
    NULL for the arrays the other serves. A NULL argument or an empty array gives NULL.
    """
    t = fn.params[0].name
    others = [p.name for p in fn.params[1:]]
    rest = ", ".join(others)
    required = " OR ".join(f"{p.name} IS NULL" for p in fn.params[: fn.min_args])
    odd = f"exists({t}, gbx_e -> gbx_e IS NULL OR gbx_e.raster IS NULL)"
    light = ", ".join([f"(IF({odd}, {t}, NULL))"] + others)
    plain = f"(IF({odd}, NULL, {t}))"
    return (
        f"CREATE OR REPLACE TEMPORARY FUNCTION {fn.sql_name}({_sig(fn)}) "
        f"RETURNS {sql_type(fn.returns_session)} RETURN CASE WHEN {required} OR size({t}) = 0 THEN NULL "
        f"WHEN {odd} THEN {l3_name}({light}) "
        f"ELSE {plain_call.format(tiles=plain, rest=rest)} END"
    )
