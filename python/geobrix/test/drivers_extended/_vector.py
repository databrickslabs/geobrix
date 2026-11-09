"""Vector cases of the extended driver tests: a few features written with GDAL (a point, a line and a polygon with
attributes and a CRS), and the plain-pyogrio reading that GeoBrix's rows are held to."""

import os
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

# (WKT, id, name, val)
FEATURES = [
    ("POINT (10 50)", 1, "alpha", 1.5),
    ("LINESTRING (10 50, 11 51, 12 50)", 2, "beta", 2.5),
    ("POLYGON ((10 50, 11 50, 11 51, 10 51, 10 50))", 3, "gamma", 3.5),
]
POINT, LINE, POLY = 0, 1, 2


@dataclass(frozen=True)
class VectorCase:
    """One sample dataset.

    ``kinds``: the indexes of FEATURES it holds (a format with one geometry type per layer holds one).
    ``fields``: whether GDAL writes the attributes. ``reader``: the named GeoBrix reader, if any (every case also
    reads through ``vector_gbx`` with ``driverName``). ``layers``: the layers read, one read each (None: the dataset's only or first layer).
    ``write``: the GeoBrix writer entry the features are written with, None when the driver is not written here.
    ``build``: a builder of the sample when the generic one does not fit (a name in ``BUILDERS``).
    """

    driver: str
    name: str
    kinds: Tuple[int, ...] = (POINT, LINE, POLY)
    fields: bool = True
    epsg: Optional[int] = 4326
    layers: Tuple[Optional[str], ...] = (None,)
    reader: Optional[str] = None
    lco: Tuple[str, ...] = ()
    dsco: Tuple[str, ...] = ()
    write: Optional[str] = None
    write_cols: Tuple[str, ...] = (
        "id",
        "name",
        "val",
    )  # the attribute columns sent to the writer
    write_kinds: Optional[Tuple[int, ...]] = (
        None  # the geometries sent, if not ``kinds``
    )
    write_opts: Tuple[Tuple[str, str], ...] = ()  # writer options beyond the driver
    build: Optional[str] = None
    keeps_attrs: bool = True  # the format keeps id, name and val as written
    counts: bool = (
        True  # False: the tests run, but the driver is not counted as tested (a ruling, not a failure)
    )
    keeps_crs: bool = True  # the format stores the CRS in the dataset (a CSV does not)

    @property
    def id(self) -> str:
        return self.driver.replace(" ", "")


def _srs(gb, epsg):
    srs = gb.osr.SpatialReference()
    srs.ImportFromEPSG(epsg)
    srs.SetAxisMappingStrategy(gb.osr.OAMS_TRADITIONAL_GIS_ORDER)
    return srs


def write_generic(gb, case: VectorCase, path: str) -> None:
    ogr = gb.ogr
    ds = ogr.GetDriverByName(case.driver).CreateDataSource(path, list(case.dsco))
    gtype = ogr.wkbUnknown
    if len(case.kinds) == 1:
        gtype = (ogr.wkbPoint, ogr.wkbLineString, ogr.wkbPolygon)[case.kinds[0]]
    lyr = ds.CreateLayer(
        case.layers[0] or "lyr",
        _srs(gb, case.epsg) if case.epsg else None,
        gtype,
        list(case.lco),
    )
    if case.fields:
        lyr.CreateField(ogr.FieldDefn("id", ogr.OFTInteger))
        lyr.CreateField(ogr.FieldDefn("name", ogr.OFTString))
        lyr.CreateField(ogr.FieldDefn("val", ogr.OFTReal))
    for i in case.kinds:
        wkt, idv, nm, val = FEATURES[i]
        f = ogr.Feature(lyr.GetLayerDefn())
        if case.fields:
            f.SetField("id", idv)
            f.SetField("name", nm)
            f.SetField("val", val)
        f.SetGeometry(ogr.CreateGeometryFromWkt(wkt))
        lyr.CreateFeature(f)
    lyr = None
    ds = None


def build_gpx(gb, case, path):
    ogr = gb.ogr
    ds = ogr.GetDriverByName("GPX").CreateDataSource(path)
    srs = _srs(gb, 4326)
    wp = ds.CreateLayer("waypoints", srs, ogr.wkbPoint)
    wp.CreateField(ogr.FieldDefn("name", ogr.OFTString))
    wp.CreateField(ogr.FieldDefn("ele", ogr.OFTReal))
    f = ogr.Feature(wp.GetLayerDefn())
    f.SetField("name", "w1")
    f.SetField("ele", 12.5)
    f.SetGeometry(ogr.CreateGeometryFromWkt("POINT (10 50)"))
    wp.CreateFeature(f)
    tr = ds.CreateLayer("tracks", srs, ogr.wkbMultiLineString)
    tr.CreateField(ogr.FieldDefn("name", ogr.OFTString))
    f = ogr.Feature(tr.GetLayerDefn())
    f.SetField("name", "t1")
    f.SetGeometry(ogr.CreateGeometryFromWkt("MULTILINESTRING ((10 50, 11 51, 12 50))"))
    tr.CreateFeature(f)
    wp = tr = ds = None


_OSM = """<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6" generator="geobrix-tests">
 <node id="1" lat="50.0" lon="10.0" version="1"><tag k="name" v="cafe"/><tag k="amenity" v="cafe"/></node>
 <node id="2" lat="50.0" lon="10.1" version="1"/>
 <node id="3" lat="50.1" lon="10.1" version="1"/>
 <node id="4" lat="50.1" lon="10.0" version="1"/>
 <way id="10" version="1"><nd ref="1"/><nd ref="2"/><nd ref="3"/><tag k="highway" v="residential"/><tag k="name" v="Main St"/></way>
 <way id="11" version="1"><nd ref="1"/><nd ref="2"/><nd ref="3"/><nd ref="4"/><nd ref="1"/><tag k="building" v="yes"/></way>
</osm>
"""


def build_osm(gb, case, path):
    with open(path, "w") as fh:
        fh.write(_OSM)


def build_mbtiles(gb, case, path):
    """Vector tiles: the features, cut to zoom 0-2 by GDAL's MBTiles writer."""
    gdal = gb.gdal
    src = path + ".src.geojson"
    write_generic(gb, VectorCase("GeoJSON", "x"), src)
    gdal.VectorTranslate(
        path,
        src,
        options=gdal.VectorTranslateOptions(
            format="MBTiles", datasetCreationOptions=["MINZOOM=0", "MAXZOOM=2"]
        ),
    ).FlushCache()
    os.remove(src)


def build_mvt_folder(gb, case, path):
    """A folder of vector tiles (z/x/y.pbf and metadata.json), cut to zoom 0-1 by GDAL's MVT writer."""
    gdal = gb.gdal
    src = path + ".src.geojson"
    write_generic(gb, VectorCase("GeoJSON", "x"), src)
    gdal.VectorTranslate(
        path,
        src,
        options=gdal.VectorTranslateOptions(
            format="MVT", datasetCreationOptions=["MINZOOM=0", "MAXZOOM=1"]
        ),
    ).FlushCache()
    os.remove(src)


def build_mvt_tile(gb, case, path):
    """One vector tile: the 0/0/0.pbf of a folder, copied to ``path``."""
    import shutil

    build_mvt_folder(gb, case, path + ".d")
    shutil.copy(os.path.join(path + ".d", "0", "0", "0.pbf"), path)


def build_ogr_vrt(gb, case, path):
    """A user-written OGR VRT that names a local GeoPackage of the features."""
    gpkg = path + ".gpkg"
    write_generic(gb, VectorCase("GPKG", "x"), gpkg)
    with open(path, "w") as fh:
        fh.write(
            '<OGRVRTDataSource><OGRVRTLayer name="v">'
            f"<SrcDataSource>{gpkg}</SrcDataSource><SrcLayer>lyr</SrcLayer>"
            "</OGRVRTLayer></OGRVRTDataSource>"
        )


BUILDERS: Dict[str, Callable] = {
    "gpx": build_gpx,
    "osm": build_osm,
    "mbtiles": build_mbtiles,
    "mvt": build_mvt_tile,
    "ogr_vrt": build_ogr_vrt,
}


def write_sample(gb, case: VectorCase, directory: str) -> str:
    """The sample dataset of ``case`` under ``directory``; its path."""
    path = os.path.join(directory, case.name)
    if case.build:
        BUILDERS[case.build](gb, case, path)
    else:
        write_generic(gb, case, path)
    return path


def reference(path: str, layer: Optional[str] = None):
    """Plain pyogrio's reading of ``path``: a GeoDataFrame."""
    import pyogrio

    from databricks.labs.gbx import _gdal_policy

    _gdal_policy.pyogrio()  # the driver policy is on before pyogrio reads
    return pyogrio.read_dataframe(path, layer=layer)


VECTOR_CASES: List[VectorCase] = [
    VectorCase("GPKG", "a.gpkg", write="gpkg_gbx", reader="gpkg_gbx"),
    VectorCase("GeoJSON", "a.geojson", write="geojson_gbx", reader="geojson_gbx"),
    VectorCase("GeoJSONSeq", "a.geojsonl", write="geojsonl_gbx", reader="geojsonl_gbx"),
    VectorCase(
        "ESRI Shapefile",
        "a.shp",
        (POLY,),
        write="shapefile_gbx",
        reader="shapefile_gbx",
    ),
    VectorCase(
        "OpenFileGDB",
        "a.gdb",
        (POINT,),
        layers=("pts",),
        write="file_gdb_gbx",
        reader="file_gdb_gbx",
    ),
    VectorCase("FlatGeobuf", "a.fgb", (POLY,), write="vector_gbx"),
    VectorCase("KML", "a.kml", write="vector_gbx", keeps_attrs=False),
    VectorCase("GML", "a.gml", write="vector_gbx"),
    VectorCase("MapInfo File", "a.tab", write="vector_gbx"),
    VectorCase(
        "DXF",
        "a.dxf",
        fields=False,
        epsg=None,
        keeps_attrs=False,
        write="vector_gbx",
        write_cols=(),
    ),
    VectorCase(
        "CSV",
        "a.csv",
        lco=("GEOMETRY=AS_WKT",),
        keeps_attrs=False,
        keeps_crs=False,
        counts=False,  # Databricks' own CSV reader and the ST functions are the recommended route
        write="vector_gbx",
        write_opts=(("layerName", "out"),),
    ),
    VectorCase("SQLite", "a.sqlite", write="vector_gbx"),
    VectorCase(
        "GPX",
        "a.gpx",
        layers=("waypoints", "tracks"),
        build="gpx",
        keeps_attrs=False,
        write="vector_gbx",
        write_cols=("name",),
        write_kinds=(POINT,),
        write_opts=(("layerName", "waypoints"),),
    ),
    VectorCase(
        "OSM",
        "a.osm",
        layers=("points", "lines", "multipolygons"),
        build="osm",
        keeps_attrs=False,
    ),
    VectorCase("MVT", "t.pbf", build="mvt", epsg=3857, keeps_attrs=False),
    VectorCase("OGR_VRT", "o.vrt", build="ogr_vrt", layers=("v",), keeps_attrs=False),
    VectorCase(
        "MBTiles",
        "v.mbtiles",
        layers=("lyr",),
        build="mbtiles",
        epsg=3857,
        keeps_attrs=False,
    ),
]

BY_DRIVER: Dict[str, VectorCase] = {c.driver: c for c in VECTOR_CASES}

# (path, case id) -> why the call fails. See RASTER_FINDINGS in _raster.py.
VECTOR_FINDINGS: Dict[Tuple[str, str], str] = {
    (
        "folder",
        "MVT",
    ): "vector_gbx lists a tile folder's files and opens metadata.json, which is no vector dataset; GDAL opens the folder whole",
    (
        "write1",
        "GPX",
    ): "raises ValueError with a Fix: a column named like a GPX built-in field (name) cannot be created again",
    (
        "write2",
        "GPX",
    ): "raises ValueError with a Fix: a column named like a GPX built-in field (name) cannot be created again",
}


def read_paths(case: VectorCase) -> List[str]:
    """The GeoBrix paths the read tests run for a case: ``vector_gbx`` with ``driverName`` and the named reader."""
    return [
        f"reader:{r}"
        for r in dict.fromkeys(
            ["vector_gbx", case.reader] if case.reader else ["vector_gbx"]
        )
    ]


def param(case: VectorCase, path: str, *prefix):
    import pytest

    why = VECTOR_FINDINGS.get((path, case.id))
    marks = [pytest.mark.xfail(strict=True, reason=why)] if why else []
    return pytest.param(*prefix, case, id=f"{case.id}-{path}", marks=marks)
