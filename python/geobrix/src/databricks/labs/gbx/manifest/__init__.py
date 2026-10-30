"""Per-surface function manifest: the source of truth for each function's names,
signatures and implementation levels on every surface.

Pure Python (no pyspark), so the UC DDL emitter, the docs generators and the tests
read it anywhere. JSON lives next to this module and ships as package data. The loaded
manifest is immutable (read-only mappings, tuples), so the cached copy cannot be edited
by a caller.
"""

from __future__ import annotations

import functools
import importlib
import json
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping, Optional, Tuple

SCHEMA_VERSION = 1

# The closed vocabulary of keys a function's per-level spec may carry. A mistyped key
# would be read with `.get` and silently drop a guard, so a contract test rejects any key
# outside this table; add a key here in the same change that first uses it.
# `engine` marks a level that runs the row's `gdal` block on the GDAL spec engine where that engine can serve,
# and its own implementation (the other keys) elsewhere.
LEVEL_KEYS = {
    "L1": frozenset({"companion", "needs_gdal", "session_returns", "engine"}),
    "L2": frozenset(
        {"class", "connect_class", "returns", "bytes_param", "needs_gdal", "wrapper"}
    ),
    "L3": frozenset(
        {
            "register",
            "variants",
            "factory",
            "factory_args",
            "callable",
            "udtf",
            "returns",
            "needs",
            "engine",
        }
    ),
    "L4": frozenset({"handler", "deps", "tvf_over", "engine"}),
}
# `connect_class` is the Java handler a standard access mode (Spark Connect) session registers instead of `class`: its
# sandbox loads only Java classes of a JAR added to the session, with concrete parameter types. A row without one has
# no L2 there.
# Keys every spec of that level must carry.
LEVEL_REQUIRED = {
    "L2": frozenset({"class", "returns"}),
    "L3": frozenset({"register"}),
}
# Levels whose spec must carry at least one of these keys.
LEVEL_REQUIRED_ANY = {
    "L1": frozenset({"companion", "engine"}),
    "L4": frozenset({"handler", "tvf_over"}),
}

# The closed vocabulary of a row's `gdal` block (the GDAL spec engine's spec): what `parse_gdal_spec` accepts.
GDAL_KEYS = frozenset({"alg", "in", "out", "write", "args", "needs_crs", "gdal_min"})
GDAL_ARG_KEYS = frozenset(
    {"arg", "const", "from", "type", "map", "null", "with", "bad"}
)
GDAL_IN_KINDS = frozenset({"raster", "rasters", "text"})
GDAL_OUT_KINDS = frozenset({"raster", "features", "json"})
GDAL_BINDERS = frozenset(
    {
        "double",
        "int",
        "bool",
        "choice",
        "crs",
        "geom_wkt",
        "double_list",
        "calc_envelope",
        "text_vsimem",
        "observer_xy",
        "size_from_factor",
    }
)
GDAL_NULL = frozenset({"omit", "null_result", "raise"})
GDAL_BAD = frozenset({"raise", "data"})
# Write profiles: `tile` is a GTiff with the engine's creation options. No profile writes VRT or GDALG.
GDAL_WRITE = frozenset({"tile"})

# The v2 tile struct as a SQL type (== V2_TILE_SCHEMA.simpleString(); a contract test pins it).
TILE_SQL = (
    "struct<cellid:bigint,raster:binary,path:string,path_mode:string,"
    "window:struct<col_off:int,row_off:int,width:int,height:int>,"
    "clip_polygon:binary,clip_crs:string,crs:string,metadata:map<string,string>>"
)


@dataclass(frozen=True)
class Param:
    name: str
    session: Tuple[str, ...]
    uc: str
    default: Optional[str] = None
    uc_crs: bool = False


@dataclass(frozen=True)
class GdalArg:
    """One argument of a spec's template: a fixed value (`const`; True is a bare flag) or a param bound
    through a binder (`source`, `type`)."""

    arg: str
    const: Any = None
    source: Optional[str] = None
    type: Optional[str] = None
    map: Mapping[str, str] = MappingProxyType({})
    null: str = "null_result"
    with_: Optional[str] = None
    bad: str = "raise"


@dataclass(frozen=True)
class GdalSpec:
    """A row's `gdal` block: the algorithm, its inputs and output, the write profile and the argument
    template. Both tiers turn it into the same argv."""

    alg: str
    inputs: Tuple[Tuple[str, str], ...]  # (param, input kind)
    out_kind: str
    out_dtype: str  # a GDAL type name, or "input" (the first input's band type)
    write: str
    args: Tuple[GdalArg, ...]
    needs_crs: bool = (
        False  # the input must carry a CRS (a tile without one is a data error)
    )
    gdal_min: Optional[str] = None


@dataclass(frozen=True)
class GdalSection:
    """The manifest's top-level `gdal` section: the GDAL release the specs are checked against, the
    arguments no spec or caller may set, and the algorithms (with their arguments) the engine runs.
    """

    pin: str
    deny: frozenset
    allow: Mapping[str, Tuple[str, ...]]
    # The driver classification, design section 4: EVERY raster driver the payload's GDAL registers is in `allowed` (a
    # GeoBrix reader can read it) or in `refused`. A test enumerates the payload and fails on an unclassified driver.
    # Opens are restricted to `allowed`; `refused` is skipped at GDAL's registration (deregistered where it is not).
    #
    # The remote-fetch audit of the classification, GDAL 3.13.3's source (frmts/<driver>, gcore, port, ogr, and
    # ogr/ogrsf_frmts for GPKG, OpenFileGDB and CAD). One line per driver, the verdict first:
    #   fetch  the driver turns a URL read from the file into a network request, with no path scheme in the data.
    #   write  the driver writes files the data names when it is read.
    #   path   the driver's source handles a remote name itself (a /vsicurl/ or http(s) name, a URL-valued href).
    #   none   none of the markers below is in its source.
    # The markers: /vsicurl, /vsicurl_streaming, CPLHTTP*, VSICurl*, http(s) literals in code, and SetFromUserInput on
    # a value read from the file without SET_FROM_USER_INPUT_LIMITATIONS (OGRSpatialReference fetches an http(s) CRS
    # definition: ogrspatialreference.cpp:4252-4257, 4543). Every driver that opens a file the data names (a VRT
    # source, a header's data file, a manifest's href) opens it through GDAL's file layer, so a /vsicurl/ name there is
    # fetched: that is the path-scheme question, which this classification does not decide, for "none" as for "path".
    #   KMLSUPEROVERLAY: fetch (refused). A NetworkLink or Icon href starting with http is opened as
    #     /vsicurl_streaming/<href> (kmlsuperoverlaydataset.cpp:1315-1317, 1771-1772, 1838-1840, 2701-2702).
    #   ESRIC: fetch (refused). The conf.xml SpatialReference.WKT goes to SetFromUserInput with no limitations
    #     (esric_dataset.cpp:315-317), so an http(s) value is fetched at open. No GeoBrix reader needs ESRIC.
    #   MRF: write (refused). A CachedSource.Source the file names (marfa_dataset.cpp:1519) is opened with
    #     GDALOpenShared (1669), and a read of a missing tile fetches it from that source (mrf_band.cpp:1061-1065) and
    #     writes it to the DataFile and IndexFile the file names (mrf_band.cpp:871; marfa_dataset.cpp:1033-1039,
    #     1104-1131, 1196, 1243-1275: opened for append or created, folders made). No GeoBrix reader needs MRF.
    #   AAIGrid: none.
    #   ACE2: none.
    #   ADRG: none.
    #   AIG: none.
    #   AirSAR: none.
    #   BAG: none.
    #   BIGGIF: none.
    #   BMP: none.
    #   BSB: none.
    #   BT: none.
    #   BYN: none.
    #   CAD: none.
    #   CALS: none.
    #   CEOS: none.
    #   COASP: none.
    #   COG: none.
    #   COSAR: none.
    #   CPG: none.
    #   CTG: none.
    #   DERIVED: none.
    #   DIMAP: path. DATA_FILE_PATH.href and COMPONENT_PATH.href name the files it opens (dimapdataset.cpp:624, 657,
    #     715, 825, 1197).
    #   DOQ1: none.
    #   DOQ2: none.
    #   DTED: none.
    #   E57: none.
    #   ECRGTOC: none.
    #   EHdr: none.
    #   EIR: none.
    #   ENVI: none.
    #   ERS: none.
    #   ESAT: none.
    #   FAST: none.
    #   GFF: none.
    #   GIF: none.
    #   GPKG: path. a GPKG:/vsicurl/ subdataset name is opened (ogrgeopackagedatasource.cpp:1339-1340); metadata
    #     URIs are compared only.
    #   GRASSASCIIGrid: none.
    #   GRIB: none.
    #   GS7BG: none.
    #   GSAG: none.
    #   GSBG: none.
    #   GSC: none.
    #   GTX: none.
    #   GTiff: none.
    #   GXF: none.
    #   GenBin: none.
    #   HDF5: path. a /vsicurl/ subdataset name is opened (hdf5drivercore.cpp:203-206).
    #   HDF5Image: none.
    #   HF2: none.
    #   HFA: none.
    #   ILWIS: none.
    #   IRIS: none.
    #   ISCE: none.
    #   ISG: none.
    #   ISIS2: none.
    #   ISIS3: none.
    #   JAXAPALSAR: none.
    #   JDEM: none.
    #   JP2OpenJPEG: none.
    #   JPEG: none.
    #   JPEGXL: none.
    #   KRO: none.
    #   L1B: none.
    #   LAN: none.
    #   LCP: none.
    #   LIBERTIFF: none.
    #   LOSLAS: none.
    #   Leveller: none.
    #   MAP: none.
    #   MBTiles: path. a /vsicurl/ name only switches a read cache on (mbtilesdataset.cpp:2769, 2918).
    #   MEM: none.
    #   MFF: none.
    #   MFF2: none.
    #   MSGN: none.
    #   MiraMonRaster: none.
    #   NDF: none.
    #   NGSGEOID: none.
    #   NITF: none.
    #   NOAA_B: none.
    #   NSIDCbin: none.
    #   NTv2: none.
    #   NUMPY: none.
    #   NWT_GRC: none.
    #   NWT_GRD: none.
    #   OpenFileGDB: path. a /vsicurl/ directory name is probed for its table files
    #     (ogropenfilegdbdrivercore.cpp:40-46).
    #   PAux: none.
    #   PCIDSK: none.
    #   PCRaster: none.
    #   PDF: none.
    #   PDS: none.
    #   PDS4: path. a PDS4:/vsicurl/ subdataset name is opened (pds4dataset.cpp:1742-1743); an http TEMPLATE is a
    #     creation option (4526-4529).
    #   PNG: none.
    #   PNM: none.
    #   PRF: none.
    #   RCM: none.
    #   RIK: none.
    #   RMF: none.
    #   ROI_PAC: none.
    #   RPFTOC: none.
    #   RRASTER: none.
    #   RS2: none.
    #   RST: none.
    #   S102: none.
    #   S104: none.
    #   S111: none.
    #   SAFE: path. byteStream.fileLocation.href names the files it opens (safedataset.cpp:1077, 1110, 1117).
    #   SAGA: none.
    #   SAR_CEOS: none.
    #   SENTINEL2: none.
    #   SIGDEM: none.
    #   SNAP_TIFF: none.
    #   SNODAS: none.
    #   SRP: none.
    #   SRTMHGT: none.
    #   TGA: none.
    #   TIL: none.
    #   TSX: none.
    #   Terragen: none.
    #   USGSDEM: none.
    #   VICAR: none.
    #   VRT: path. a SourceFilename names a file GDAL opens (vrtsources.cpp:206-207, 347-349).
    #   WEBP: none.
    #   XYZ: none.
    #   ZMap: none.
    #   Zarr: path. a plain http(s) name is refused by the driver (zarrdriver.cpp:373-377); a /vsicurl/ name is
    #     opened.
    #   netCDF: path. a plain http(s) or /vsicurl/ name opens through libnetcdf's DAP client
    #     (netcdfdrivercore.cpp:269-273, netcdfdataset.cpp:8187-8188). It also fetches: a variable's crs attribute
    #     that is an http(s) URL (and contains GEOGCS, PROJCS or EPSG:) goes to SetFromUserInput with no limitations
    #     (netcdfdataset.cpp:3530-3538), whose http branch fetches it (ogrspatialreference.cpp:4252-4257, 4543). Kept
    #     allowed: GeoBrix's netCDF readers need it.
    allowed_drivers: Tuple[str, ...] = ()
    refused_drivers: Tuple[str, ...] = ()


def _closed(raw: Mapping, keys: frozenset, what: str) -> None:
    extra = sorted(set(raw) - keys)
    if extra:
        raise ValueError(f"gdal spec: unknown {what} key(s) {extra}")


def _one_of(value, allowed: frozenset, what: str) -> str:
    if value not in allowed:
        raise ValueError(f"gdal spec: {what} {value!r} is not one of {sorted(allowed)}")
    return value


def _parse_gdal_arg(raw: Mapping) -> GdalArg:
    _closed(raw, GDAL_ARG_KEYS, "argument")
    name = raw.get("arg")
    if not isinstance(name, str) or not name:
        raise ValueError(f"gdal spec: an argument needs a name ({dict(raw)})")
    if ("const" in raw) == ("from" in raw):
        raise ValueError(
            f"gdal spec: argument {name!r} needs exactly one of const and from"
        )
    if "from" in raw:
        _one_of(raw.get("type"), GDAL_BINDERS, f"binder of {name!r}")
    return GdalArg(
        arg=name,
        const=raw.get("const"),
        source=raw.get("from"),
        type=raw.get("type"),
        map=MappingProxyType(dict(raw.get("map", {}))),
        null=_one_of(
            raw.get("null", "null_result"), GDAL_NULL, f"null rule of {name!r}"
        ),
        with_=raw.get("with"),
        bad=_one_of(raw.get("bad", "raise"), GDAL_BAD, f"bad rule of {name!r}"),
    )


def parse_gdal_spec(raw: Mapping) -> GdalSpec:
    """A row's `gdal` block -> GdalSpec. Refuses any key or word outside the closed vocabulary, so a
    mistyped key can never be read as absent."""
    _closed(raw, GDAL_KEYS, "block")
    out = raw.get("out") or {}
    _closed(out, frozenset({"kind", "dtype"}), "out")
    inputs = tuple(
        (p, _one_of(k, GDAL_IN_KINDS, f"input kind of {p!r}"))
        for p, k in (raw.get("in") or {}).items()
    )
    return GdalSpec(
        alg=str(raw.get("alg", "")),
        inputs=inputs,
        out_kind=_one_of(out.get("kind"), GDAL_OUT_KINDS, "output kind"),
        out_dtype=str(out.get("dtype", "input")),
        write=_one_of(raw.get("write"), GDAL_WRITE, "write profile"),
        args=tuple(_parse_gdal_arg(a) for a in raw.get("args", ())),
        needs_crs=bool(raw.get("needs_crs", False)),
        gdal_min=raw.get("gdal_min"),
    )


def _parse_gdal_section(raw: Optional[Mapping]) -> Optional[GdalSection]:
    if raw is None:
        return None
    _closed(raw, frozenset({"pin", "deny", "allow", "raster_drivers"}), "section")
    drivers = raw.get("raster_drivers", {})
    _closed(drivers, frozenset({"allowed", "refused"}), "raster_drivers")
    allowed = tuple(drivers.get("allowed", ()))
    refused = tuple(drivers.get("refused", ()))
    both = sorted(set(allowed) & set(refused))
    if both or len(set(allowed)) != len(allowed) or len(set(refused)) != len(refused):
        raise ValueError(
            f"gdal spec: the raster driver lists must be disjoint and free of repeats (both: {both})"
        )
    return GdalSection(
        pin=str(raw["pin"]),
        deny=frozenset(raw.get("deny", ())),
        allow=MappingProxyType(
            {alg: tuple(args) for alg, args in raw.get("allow", {}).items()}
        ),
        allowed_drivers=allowed,
        refused_drivers=refused,
    )


@dataclass(frozen=True)
class Helper:
    """A top-level DataFrame helper (not a SQL function): where it lives, and how settled it is."""

    name: str
    module: str
    status: str


# eq=False keeps identity hashing: the level specs are mappings, which do not hash.
@dataclass(frozen=True, eq=False)
class Function:
    sql_name: str
    family: str
    kind: str  # scalar | generator | aggregate
    params: Tuple[Param, ...]
    min_args: int
    returns_session: str
    returns_uc: Optional[str]
    accepts_virtual_tile: bool
    levels: Mapping[str, Mapping[str, Any]]
    oracle: str
    # A generator's output columns as (name, SQL type): what its UC table function RETURNS.
    columns: Tuple[Tuple[str, str], ...] = ()
    # The GDAL spec engine's spec for this row (its `gdal` block), when the row has one.
    gdal: Optional[GdalSpec] = None

    @property
    def max_args(self) -> int:
        return len(self.params)


@dataclass(frozen=True, eq=False)
class Manifest:
    functions: Mapping[str, Function]
    uc_dependencies: Mapping[str, str]
    level_table_version: str
    helpers: Tuple[Helper, ...] = ()
    gdal: Optional[GdalSection] = None


def sql_type(t: str) -> str:
    """Symbolic manifest type -> SQL type text."""
    if t == "ARRAY<TILE>":
        return f"array<{TILE_SQL}>"
    return TILE_SQL if t == "TILE" else t


def _freeze(obj: Any) -> Any:
    """Deep read-only copy: dict -> mappingproxy, list -> tuple."""
    if isinstance(obj, dict):
        return MappingProxyType({k: _freeze(v) for k, v in obj.items()})
    if isinstance(obj, list):
        return tuple(_freeze(v) for v in obj)
    return obj


@functools.lru_cache(maxsize=1)
def load() -> Manifest:
    raw = json.loads((Path(__file__).with_name("functions.json")).read_text())
    if raw.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(
            f"functions.json schema_version {raw.get('schema_version')!r}, expected {SCHEMA_VERSION}"
        )
    fns = {}
    for r in raw["functions"]:
        params = tuple(
            Param(
                p["name"],
                tuple(p["session"]),
                p.get("uc", ""),
                p.get("default"),
                bool(p.get("uc_crs")),
            )
            for p in r["params"]
        )
        fns[r["sql_name"]] = Function(
            r["sql_name"],
            r["family"],
            r["kind"],
            params,
            int(r["min_args"]),
            r["returns"]["session"],
            r["returns"].get("uc"),
            bool(r.get("accepts_virtual_tile")),
            _freeze(r["levels"]),
            r["oracle"],
            tuple((c[0], c[1]) for c in r.get("columns", [])),
            parse_gdal_spec(r["gdal"]) if "gdal" in r else None,
        )
    return Manifest(
        MappingProxyType(fns),
        MappingProxyType(dict(raw["uc_dependencies"])),
        raw["level_table_version"],
        tuple(
            Helper(h["name"], h["module"], h["status"]) for h in raw.get("helpers", [])
        ),
        _parse_gdal_section(raw.get("gdal")),
    )


def resolve_ref(ref: str) -> Any:
    """'pkg.module:attr' -> the object (imports the module)."""
    mod, _, attr = ref.partition(":")
    return getattr(importlib.import_module(mod), attr)
