"""The `gdal` blocks of the manifest: a closed vocabulary, a closed allowlist, and the values the engine refuses.

Pure Python (no Spark, no GDAL): every row runs on the host.
"""

import re
from pathlib import Path

import pytest

from databricks.labs.gbx import manifest
from databricks.labs.gbx.engine import spec

MAN = manifest.load()
ENGINE_FNS = sorted(
    (f for f in MAN.functions.values() if f.gdal is not None), key=lambda f: f.sql_name
)
REPO = Path(__file__).resolve().parents[4]


def test_the_engine_rows_carry_a_gdal_block_and_the_pin_is_the_build_pin():
    assert {f.sql_name for f in ENGINE_FNS} >= {
        "gbx_rst_slope",
        "gbx_rst_transformcrs",
        "gbx_rst_mapalgebra",
    }
    pin = re.search(
        r"^GDAL_VERSION=(\S+)$", (REPO / "scripts/native/pins.conf").read_text(), re.M
    ).group(1)
    assert MAN.gdal.pin == pin
    # The production allowlist holds only the spec rows' algorithms (the bench's floor extends it for itself).
    assert set(MAN.gdal.allow) == {f.gdal.alg for f in ENGINE_FNS}
    # The virtual-tile policy (design section 4): the allowlist of the drivers the readers emit virtual tiles for, and
    # the refused drivers, pinned as sets: an over-broad list fails, a subset check would not.
    assert set(MAN.gdal.refused_drivers) == REFUSED_DRIVERS
    # the allowed class is every raster driver a reader can read, so the formats GeoBrix reads are allowed
    assert {"GTiff", "VRT", "PNG", "netCDF"} <= set(MAN.gdal.allowed_drivers)


REFUSED_DRIVERS = {
    "GDALG",  # a stored pipeline
    "GTI",
    "STACTA",  # tile indexes and catalogs: each opens other rasters it names
    "STACIT",
    "WMS",
    "WMTS",
    "WCS",
    "WFS",
    "HTTP",
    "CSW",
    "OGCAPI",  # network APIs
    "PLMOSAIC",
    "PLSCENES",
    "EEDAI",
    "EEDA",
    "DAAS",
    "NGW",
    "KMLSUPEROVERLAY",  # a NetworkLink or Icon href is fetched
    "ESRIC",  # an http(s) CRS in its conf.xml is fetched
    "MRF",  # opens a source its file names and writes the cache files its file names, on read
}


def test_the_classes_are_disjoint_and_the_refused_class_is_the_pinned_set():
    """The classification pins the refused class as a set, and no driver is both allowed and refused. The
    enumeration of the payload's registered rasters, which fails on an unclassified one, is in test_driver_policy.py.
    """
    assert not set(MAN.gdal.allowed_drivers) & set(MAN.gdal.refused_drivers)
    assert set(MAN.gdal.refused_drivers) == REFUSED_DRIVERS


_ENTRY = re.compile(r"^\s*#   (\w+): (none|path|fetch|write)\b(.*)$")
_CONTINUATION = re.compile(r"^\s*#     (.*)$")
_FETCH_WORD = re.compile(r"\bfetch(es|ed|ing)?\b", re.I)


def audit_entries(src: str) -> dict:
    """The audit comment's entries: driver -> (verdict, the entry's text, its first line and continuation lines)."""
    entries, cur = {}, None
    for line in src.splitlines():
        m = _ENTRY.match(line)
        if m:
            cur = m.group(1)
            entries[cur] = (m.group(2), m.group(3))
            continue
        c = _CONTINUATION.match(line)
        if c and cur is not None:
            verdict, text = entries[cur]
            entries[cur] = (verdict, f"{text} {c.group(1)}")
        else:
            cur = None
    return entries


def allowed_that_fetch(entries: dict) -> set:
    """The allowed drivers (none or path) whose entry says, anywhere in it, that the driver fetches."""
    return {
        name
        for name, (verdict, text) in entries.items()
        if verdict in ("none", "path") and _FETCH_WORD.search(text)
    }


def test_every_allowed_driver_has_a_remote_fetch_audit_line():
    """The audit of GDAL 3.13.3's source is one entry per allowed driver, in the classification's comment, and each
    driver refused for a fetch or a write names its evidence. A driver added to `allowed` without an entry fails
    here, and so does an allowed driver other than netCDF whose entry says it fetches.
    """
    entries = audit_entries(Path(manifest.__file__).read_text())
    allowed = sorted(n for n, (v, _) in entries.items() if v in ("none", "path"))
    assert allowed == sorted(MAN.gdal.allowed_drivers)
    refused_for = {n: v for n, (v, text) in entries.items() if v in ("fetch", "write")}
    assert refused_for == {"KMLSUPEROVERLAY": "fetch", "ESRIC": "fetch", "MRF": "write"}
    assert all("(refused)" in entries[n][1] for n in refused_for)
    assert set(refused_for) <= set(MAN.gdal.refused_drivers)
    assert allowed_that_fetch(entries) == {"netCDF"}


@pytest.mark.parametrize(
    "old,new",
    [
        ("#   ZMap: none.", "#   ZMap: none. It fetches the URL its header names."),
        (
            "#   VRT: path. a SourceFilename",
            "#   VRT: path. it fetches a SourceFilename",
        ),
    ],
    ids=["a none entry", "a path entry's first line"],
)
def test_the_fetch_detector_sees_the_word_anywhere_in_an_allowed_entry(old, new):
    src = Path(manifest.__file__).read_text()
    assert src.count(old) == 1
    driver = old.split()[1].rstrip(":")
    assert allowed_that_fetch(audit_entries(src.replace(old, new))) == {
        "netCDF",
        driver,
    }


def test_the_jvm_skips_exactly_the_refused_drivers():
    """Drift: GDALManager's GDAL_SKIP default (registered at GDAL init) is the manifest's refused set."""
    text = (
        REPO / "src/main/scala/com/databricks/labs/gbx/rasterx/gdal/GDALManager.scala"
    ).read_text()
    skipped = re.search(r'val DefaultSkippedDrivers\s*=\s*"([^"]*)"', text)
    assert skipped is not None
    assert set(skipped.group(1).split()) == set(MAN.gdal.refused_drivers)


@pytest.mark.parametrize(
    "raw,named",
    [
        ({"pin": "3.13.3", "deny": [], "allow": {}, "allowlist": {}}, "allowlist"),
        (
            {
                "pin": "3.13.3",
                "raster_drivers": {"allowed": ["GTiff"], "refused": ["GTiff"]},
            },
            "GTiff",
        ),
        (
            {
                "pin": "3.13.3",
                "raster_drivers": {
                    "allowed": ["GTiff"],
                    "refused": [],
                    "bytes": ["VRT"],
                },
            },
            "VRT",
        ),
    ],
    ids=[
        "a section key outside the vocabulary",
        "a driver on both lists",
        "a bytes driver that is not allowed",
    ],
)
def test_a_section_outside_the_vocabulary_is_refused_at_load(raw, named):
    with pytest.raises(ValueError, match=re.escape(named)):
        manifest._parse_gdal_section(raw)


def test_the_bytes_class_holds_no_driver_that_names_other_files():
    """The drivers the engine opens a tile's bytes with are self-contained: none whose dataset names another file, a
    dataset or a URL (a VRT's sources, a label's data file, a subdataset, a library's own reads).
    """
    named = {
        "VRT",
        "DERIVED",
        "GTI",
        "STACIT",
        "STACTA",
        "WMS",
        "WMTS",
        "DIMAP",
        "SAFE",
        "GPKG",
        "HDF5",
    }
    named |= {
        "HDF5Image",
        "BAG",
        "netCDF",
        "Zarr",
        "PDS",
        "PDS4",
        "ISIS2",
        "ISIS3",
        "VICAR",
        "ENVI",
        "EHdr",
    }
    named |= {
        "GenBin",
        "ERS",
        "PAux",
        "MFF",
        "MFF2",
        "HFA",
        "PCIDSK",
        "MEM",
        "NUMPY",
        "ECRGTOC",
        "RPFTOC",
        "TIL",
    }
    bytes_class = set(MAN.gdal.bytes_drivers)
    assert bytes_class and bytes_class <= set(MAN.gdal.allowed_drivers)
    assert not bytes_class & named, bytes_class & named


@pytest.mark.parametrize("fn", ENGINE_FNS, ids=lambda f: f.sql_name)
def test_every_gdal_block_validates(fn):
    spec.validate(fn, MAN.gdal)
    # the engine has no JVM level; an L1 entry that remains is the function's own Scala companion (until it is measured)
    assert "engine" in fn.levels["L3"] and "engine" not in fn.levels.get("L1", {})


def _row(**over):
    """The slope block as raw JSON, with `over` merged in (None deletes a key)."""
    raw = {
        "alg": "raster slope",
        "in": {"tile": "raster"},
        "out": {"kind": "raster", "dtype": "Float32"},
        "write": "tile",
        "args": [
            {
                "arg": "unit",
                "from": "unit",
                "type": "choice",
                "map": {"degrees": "degree"},
            },
            {
                "arg": "xscale",
                "from": "xscale",
                "type": "double",
                "null": "omit",
                "with": "yscale",
            },
            {
                "arg": "yscale",
                "from": "yscale",
                "type": "double",
                "null": "omit",
                "with": "xscale",
            },
            {"arg": "no-edges", "const": True},
        ],
    }
    raw.update(over)
    return {k: v for k, v in raw.items() if v is not None}


SLOPE = MAN.functions["gbx_rst_slope"]
REFUSED_BLOCKS = [
    ("unknown block key", _row(engine="fast"), "engine"),
    (
        "unknown arg key",
        _row(args=[{"arg": "unit", "from": "unit", "type": "choice", "pad": 1}]),
        "pad",
    ),
    (
        "unknown binder",
        _row(args=[{"arg": "unit", "from": "unit", "type": "path"}]),
        "path",
    ),
    ("unknown input kind", _row(**{"in": {"tile": "dataset"}}), "dataset"),
    (
        "unknown null rule",
        _row(args=[{"arg": "unit", "from": "unit", "type": "choice", "null": "zero"}]),
        "zero",
    ),
    (
        "const and from together",
        _row(args=[{"arg": "unit", "const": "degree", "from": "unit"}]),
        "unit",
    ),
    ("a VRT write profile", _row(write="vrt"), "vrt"),
    ("a GDALG write profile", _row(write="gdalg"), "gdalg"),
    (
        "an out key outside the vocabulary",
        _row(out={"kind": "raster", "dtyp": "Float32"}),
        "dtyp",
    ),
    (
        "a binder with no type",
        _row(args=[{"arg": "unit", "from": "unit"}]),
        "unit",
    ),
    (
        "a features output with no field",
        _row(out={"kind": "features"}, write=None),
        "at least one field",
    ),
    (
        "a features output with a write profile",
        _row(out={"kind": "features", "fields": [["value", "DOUBLE", "value"]]}),
        "write profile",
    ),
    (
        "a raster output with no write profile",
        _row(write=None),
        "write profile",
    ),
    (
        "a JSON output with a write profile",
        _row(out={"kind": "json"}),
        "write profile",
    ),
    (
        "a JSON output with a field",
        _row(
            out={"kind": "json", "fields": [["value", "DOUBLE", "value"]]}, write=None
        ),
        "at least one field",
    ),
    (
        "a streamed raster output",
        _row(out={"kind": "raster", "dtype": "Byte", "stream": True}),
        "only a features output can be streamed",
    ),
    ("an in_read outside the vocabulary", _row(in_read="rows"), "rows"),
    ("a same_crs outside the vocabulary", _row(same_crs="reproject"), "reproject"),
    (
        "a field type outside the vocabulary",
        _row(
            out={"kind": "features", "fields": [["value", "DECIMAL", "value"]]},
            write=None,
        ),
        "DECIMAL",
    ),
]


@pytest.mark.parametrize(
    "why,raw,named", REFUSED_BLOCKS, ids=[r[0] for r in REFUSED_BLOCKS]
)
def test_a_block_outside_the_vocabulary_is_refused_at_load(why, raw, named):
    with pytest.raises(ValueError, match=re.escape(named)):
        manifest.parse_gdal_spec(raw)


REFUSED_SPECS = [
    ("an algorithm off the allowlist", _row(alg="raster tile"), "raster tile"),
    (
        "an argument off the allowlist",
        _row(args=[{"arg": "zfactor", "const": 2}]),
        "zfactor",
    ),
    (
        "a param the function lacks",
        _row(args=[{"arg": "unit", "from": "units", "type": "choice"}]),
        "units",
    ),
    (
        "a one-sided with group",
        _row(
            args=[
                {
                    "arg": "xscale",
                    "from": "xscale",
                    "type": "double",
                    "null": "omit",
                    "with": "yscale",
                }
            ]
        ),
        "yscale",
    ),
    ("an input the function lacks", _row(**{"in": {"dem": "raster"}}), "dem"),
    (
        "a when_empty param the function lacks",
        _row(
            args=[
                {
                    "arg": "unit",
                    "from": "unit",
                    "type": "choice",
                    "map": {"degrees": "degree"},
                    "when_empty": "levels",
                }
            ]
        ),
        "levels",
    ),
    ("a per_band row with no band argument", _row(per_band=True), "per_band"),
    (
        "a per_band row with a band that is not 1",
        _row(per_band=True, args=[{"arg": "band", "const": 2}]),
        "per_band",
    ),
    (
        "a per_band row with a JSON output",
        _row(
            per_band=True,
            out={"kind": "json"},
            write=None,
            args=[{"arg": "band", "const": 1}],
        ),
        "per_band",
    ),
    (
        "a checked_name with no fixed word",
        _row(args=[{"arg": "unit", "from": "unit", "type": "checked_name"}]),
        "checked_name",
    ),
] + [
    (f"the deny-listed {a}", _row(args=[{"arg": a, "const": "x"}]), a)
    for a in sorted(MAN.gdal.deny)
]


@pytest.mark.parametrize(
    "why,raw,named", REFUSED_SPECS, ids=[r[0] for r in REFUSED_SPECS]
)
def test_a_spec_the_allowlist_does_not_cover_is_refused(why, raw, named):
    block = manifest.parse_gdal_spec(raw)
    with pytest.raises(spec.SpecError, match=re.escape(named)):
        spec.validate(SLOPE, MAN.gdal, block)


@pytest.mark.parametrize(
    "value",
    [
        "/etc/passwd",
        "\\\\host\\share",
        "/vsicurl/http://x/a.tif",
        "@file.txt",
        "http://x/crs.json",
        "a/../b",
    ],
)
def test_a_value_that_names_a_file_or_url_is_refused(value):
    with pytest.raises(spec.ParamError, match=re.escape(value)):
        spec.check_value("gbx_rst_transformcrs", "target_crs", value)


@pytest.mark.parametrize(
    "value", ["EPSG:3857", "3857", "+proj=merc +nadgrids=@null", "(A-B)/(A+B)"]
)
def test_an_ordinary_value_passes(value):
    spec.check_value("gbx_rst_transformcrs", "target_crs", value)


@pytest.mark.parametrize(
    "tokens,named",
    [
        (["-of=VRT"], "-of"),  # a short flag
        (["--output-format=VRT"], "output-format"),  # a deny-listed argument
        (["--zfactor=2"], "zfactor"),  # an argument off the allowlist
        (["--unit=degree", "!", "--unit=percent"], "!"),  # a step separator
    ],
)
def test_the_engine_core_refuses_tokens_the_spec_did_not_make(tokens, named):
    with pytest.raises(spec.SpecError, match=re.escape(named)):
        spec.guard_tokens(MAN.gdal, "raster slope", tokens)
    with pytest.raises(spec.SpecError, match="raster tile"):
        spec.guard_tokens(MAN.gdal, "raster tile", ["--unit=degree"])


def test_a_file_argument_is_allowed_only_as_a_file_the_engine_writes():
    """color-map is on the deny list (a path GDAL would read); a spec may bind it only through text_vsimem, and the
    engine's check lets it through only with the placeholder of a file the engine wrote.
    """
    import dataclasses

    fn = MAN.functions["gbx_rst_color_relief"]
    bad = dataclasses.replace(
        fn.gdal,
        args=tuple(
            dataclasses.replace(a, type="choice") if a.arg == "color-map" else a
            for a in fn.gdal.args
        ),
    )
    with pytest.raises(spec.SpecError, match="color-map"):
        spec.validate(fn, MAN.gdal, bad)
    spec.validate(fn, MAN.gdal)  # the shipped row binds it through text_vsimem
    files = frozenset({"color-map"})
    spec.guard_tokens(MAN.gdal, "raster color-map", ["--color-map=<text0>"], files)
    for token in ("--color-map=/Volumes/c/s/v/colors.txt", "--color-map=<text0>"):
        with pytest.raises(spec.SpecError, match="color-map"):
            spec.guard_tokens(MAN.gdal, "raster color-map", [token])
    with pytest.raises(spec.SpecError, match="color-map"):
        spec.guard_tokens(
            MAN.gdal, "raster color-map", ["--color-map=/etc/passwd"], files
        )


def test_the_read_and_crs_rules_are_the_vocabulary_and_the_warp_rows_use_them():
    """`in_read` is "factor" or "path" and `same_crs` is "keep", and nothing else: the shipped rows say so, and the block
    that says neither parses with both unset."""
    assert manifest.GDAL_IN_READS == {"factor", "path"}
    assert manifest.GDAL_SAME_CRS == {"keep"}
    assert MAN.functions["gbx_rst_resample"].gdal.in_read == "factor"
    assert {
        n for n, f in MAN.functions.items() if f.gdal and f.gdal.in_read == "path"
    } == {"gbx_rst_viewshed", "gbx_rst_gdal"}
    assert {
        n for n, f in MAN.functions.items() if f.gdal and f.gdal.same_crs == "keep"
    } == {"gbx_rst_transform", "gbx_rst_to_webmercator"}
    plain = manifest.parse_gdal_spec(_row())
    assert plain.in_read is None and plain.same_crs is None
    kept = manifest.parse_gdal_spec(_row(in_read="factor", same_crs="keep"))
    assert (kept.in_read, kept.same_crs) == ("factor", "keep")


def _with_l3(fn, **over):
    import dataclasses

    l3 = {**fn.levels["L3"], **over}
    return dataclasses.replace(fn, levels={**fn.levels, "L3": l3})


def test_the_engines_file_form_serves_one_tile_and_needs_the_engine_key():
    resample = MAN.functions["gbx_rst_resample"]
    spec.validate(resample, MAN.gdal)
    algebra = _with_l3(MAN.functions["gbx_rst_mapalgebra"], file_ref="engine")
    with pytest.raises(spec.SpecError, match="array of tiles"):
        spec.validate(algebra, MAN.gdal)
    no_engine = _with_l3(MAN.functions["gbx_rst_slope"], file_ref="engine")
    no_engine.levels["L3"].pop("engine")
    with pytest.raises(spec.SpecError, match="'engine' key"):
        spec.validate(no_engine, MAN.gdal)


def test_the_analysis_rows_use_the_json_output_the_whole_number_binder_and_the_band_step():
    """The summary is the one JSON row (`raster info`, with statistics); the fill steps through the bands and reads its two
    numbers as whole numbers. Neither has an output the engine could not write: JSON comes back as text.
    """
    summary, fill = (
        MAN.functions["gbx_rst_summary"],
        MAN.functions["gbx_rst_fillnodata"],
    )
    assert summary.gdal.out_kind == "json" and summary.gdal.write == ""
    assert [(a.arg, a.const) for a in summary.gdal.args] == [("stats", True)]
    assert fill.gdal.per_band and fill.gdal.out_dtype == "input"
    assert {a.arg: a.type for a in fill.gdal.args if a.source} == {
        "max-distance": "count",
        "smoothing-iterations": "count",
    }
    assert {f.sql_name for f in ENGINE_FNS if f.gdal.out_kind == "json"} == {
        "gbx_rst_summary"
    }
    assert {f.sql_name for f in ENGINE_FNS if f.gdal.per_band} == {"gbx_rst_fillnodata"}
    assert spec.OUTPUT_FORMAT_OF["json"] == "json"


# --- the command allowlist (gbx_rst_gdal) -----------------------------------------------------------------------------


def test_the_command_row_is_the_pipeline_and_its_allowlist_is_the_pipeline_argument_alone():
    fn = MAN.functions["gbx_rst_gdal"]
    assert fn.gdal.alg == "raster pipeline"
    assert MAN.gdal.allow["raster pipeline"] == ("pipeline",)
    assert [(a.arg, a.type) for a in fn.gdal.args] == [("pipeline", "command")]
    assert fn.gdal.write == "command"
    assert (
        fn.levels["L3"]["engine"] == "gdal"
        and "L1" not in fn.levels
        and "L4" not in fn.levels
    )


def test_the_command_allowlist_names_no_calc_dialect_and_no_deny_listed_argument_but_geometry():
    allow = MAN.gdal.command_allow
    assert "dialect" not in {a for kinds in allow.values() for a in kinds}
    assert "calc" not in allow
    held = {
        (step, arg)
        for step, kinds in allow.items()
        for arg in kinds
        if arg in MAN.gdal.deny or arg in spec.ENGINE_ARGS
    }
    assert held == {("clip", "geometry")}
    assert allow["clip"]["geometry"] == "wkt"
    # no argument of a dataset or a file: the kinds are values, never a name GDAL opens
    assert not {
        (s, a)
        for s, k in allow.items()
        for a in k
        if a.startswith("like") or a in ("mask", "sd-filename")
    }


def test_every_command_kind_is_one_the_grammar_knows():
    from databricks.labs.gbx.engine import command

    known = set(command._KIND) | {"flag", "crs", "wkt"}
    assert {
        k for kinds in MAN.gdal.command_allow.values() for k in kinds.values()
    } <= known
    assert manifest.GDAL_COMMAND_KINDS == known


def test_a_command_kind_outside_the_vocabulary_is_refused_at_load():
    raw = {
        "pin": "3.13.3",
        "command_allow": {"slope": {"unit": "path"}},
        "raster_drivers": {"allowed": ["GTiff"], "refused": []},
    }
    with pytest.raises(ValueError, match="path"):
        manifest._parse_gdal_section(raw)
