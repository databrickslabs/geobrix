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
    ],
    ids=["a section key outside the vocabulary", "a driver on both lists"],
)
def test_a_section_outside_the_vocabulary_is_refused_at_load(raw, named):
    with pytest.raises(ValueError, match=re.escape(named)):
        manifest._parse_gdal_section(raw)


@pytest.mark.parametrize("fn", ENGINE_FNS, ids=lambda f: f.sql_name)
def test_every_gdal_block_validates(fn):
    spec.validate(fn, MAN.gdal)
    assert "engine" in fn.levels["L1"] and "engine" in fn.levels["L3"]


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
