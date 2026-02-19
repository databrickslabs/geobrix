#!/usr/bin/env python3
"""
Generate minimal doc-test bundle under sample-data/Volumes/main/default/test-data/geobrix-examples/.

Extracts small subsets from the full bundle (geobrix_samples/geobrix-examples) using bounding boxes
around configurable NYC (default: center of Manhattan) and London (default: center of London) so
joins and spatial operations have matching geometry. Vector data is limited by a default row count;
raster data is clipped to the bbox (all pixels within bounds).

NYC data is assumed WGS84 (EPSG:4326). London data may be BNG (EPSG:27700) or WGS84; the script
passes London center in WGS84 and transforms the bbox to the source CRS when clipping (so BNG
rasters are clipped correctly). Vector output is written in WGS84; raster output keeps the
source CRS (e.g. London rasters stay BNG if source is BNG).

Usage:
  python3 sample-data/generate-minimal-bundle.py [OPTIONS]

Requires full bundle at sample-data/Volumes/main/default/geobrix_samples/geobrix-examples/
(or set --source). Uses geopandas for vector clipping and GDAL for raster clipping.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import warnings
import zipfile
from pathlib import Path

# Suppress pyogrio DateTime->String field warning when writing shapefiles (e.g. parks)
warnings.filterwarnings(
    "ignore",
    message=".*Field.*created as String.*though DateTime requested.*",
    category=RuntimeWarning,
)

SCRIPT_DIR = Path(__file__).resolve().parent
OUT_ROOT = SCRIPT_DIR / "Volumes" / "main" / "default" / "test-data" / "geobrix-examples"

# Suppress GDAL 4.0 FutureWarning about UseExceptions/DontUseExceptions (set before any GDAL use)
try:
    from osgeo import gdal
    gdal.DontUseExceptions()
except ImportError:
    pass

# Default centers (lon, lat): Manhattan, London
DEFAULT_NYC_LON, DEFAULT_NYC_LAT = -73.9857, 40.7484   # Manhattan (Empire State area)
DEFAULT_LONDON_LON, DEFAULT_LONDON_LAT = -0.1276, 51.5074
DEFAULT_BBOX_SIZE = 0.02   # half-width and half-height in degrees
DEFAULT_MAX_ROWS = 10


def bbox(center_lon: float, center_lat: float, size: float) -> tuple[float, float, float, float]:
    """Return (minx, miny, maxx, maxy) for a square bbox around center."""
    h = size / 2.0
    return (center_lon - h, center_lat - h, center_lon + h, center_lat + h)


def _run(cmd: list[str], cwd: Path | None = None) -> bool:
    rc = subprocess.run(cmd, cwd=cwd or SCRIPT_DIR, capture_output=True, text=True)
    if rc.returncode != 0 and rc.stderr:
        print(rc.stderr, file=sys.stderr)
    return rc.returncode == 0


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def clip_vector_to_bbox(
    src: Path,
    out: Path,
    minx: float, miny: float, maxx: float, maxy: float,
    max_rows: int,
    driver: str | None = None,
) -> bool:
    """Clip vector file to bbox (WGS84), limit to max_rows, write to out in WGS84. Uses geopandas.
    Source may be in any CRS (e.g. London BNG 27700); we transform to 4326 for clipping and output."""
    try:
        import geopandas as gpd
        from shapely.geometry import box
    except ImportError:
        print("Skipping vector (geopandas not installed).", file=sys.stderr)
        return False
    if not src.exists():
        print(f"Source not found: {src}", file=sys.stderr)
        return False
    try:
        gdf = gpd.read_file(src)
        if gdf.crs is None:
            gdf.set_crs("EPSG:4326", inplace=True)
        # Clip bbox is always WGS84; transform data to 4326 for clipping (handles London BNG etc.)
        gdf = gdf.to_crs("EPSG:4326")
        clip = box(minx, miny, maxx, maxy)
        gdf = gdf[gdf.intersects(clip)].head(max_rows)
        if gdf.empty:
            # Fallback: take first max_rows (centroid.within in geographic CRS is discouraged)
            gdf = gpd.read_file(src).to_crs("EPSG:4326").head(max_rows)
        out.parent.mkdir(parents=True, exist_ok=True)
        gdf.to_file(out, driver=driver)
        return True
    except Exception as e:
        print(f"Vector clip error {src}: {e}", file=sys.stderr)
        return False


def write_geojsonl_from_geojson(src: Path, out: Path, minx: float, miny: float, maxx: float, maxy: float, max_rows: int) -> bool:
    """Write GeoJSONSeq (one feature per line) from GeoJSON, clipped to WGS84 bbox. Source may be BNG."""
    if not src.exists():
        return False
    try:
        import geopandas as gpd
        from shapely.geometry import box
        gdf = gpd.read_file(src)
        if gdf.crs is None:
            gdf.set_crs("EPSG:4326", inplace=True)
        gdf = gdf.to_crs("EPSG:4326")
        clip = box(minx, miny, maxx, maxy)
        gdf = gdf[gdf.intersects(clip)].head(max_rows)
        if gdf.empty:
            gdf = gpd.read_file(src).to_crs("EPSG:4326").head(max_rows)
        out.parent.mkdir(parents=True, exist_ok=True)
        # Use GeoDataFrame __geo_interface__ (FeatureCollection); iterrows() gives Series without __geo_interface__
        fc = gdf.__geo_interface__
        with open(out, "w") as f:
            for feature in fc.get("features", [])[:max_rows]:
                f.write(json.dumps(feature) + "\n")
        return True
    except Exception as e:
        print(f"GeoJSONL error {src}: {e}", file=sys.stderr)
        return False


def _wgs84_bbox_to_crs(minx: float, miny: float, maxx: float, maxy: float, epsg: int) -> tuple[float, float, float, float]:
    """Transform WGS84 bbox (minx, miny, maxx, maxy) to target CRS (e.g. 27700 for BNG). Returns (minx, miny, maxx, maxy) in target CRS."""
    if epsg == 4326:
        return (minx, miny, maxx, maxy)
    try:
        from osgeo import osr
        src = osr.SpatialReference()
        src.ImportFromEPSG(4326)
        dst = osr.SpatialReference()
        dst.ImportFromEPSG(epsg)
        ct = osr.CoordinateTransformation(src, dst)
        # Transform corners and compute bbox in target CRS
        (x1, y1, _) = ct.TransformPoint(minx, miny)
        (x2, y2, _) = ct.TransformPoint(maxx, maxy)
        (x3, y3, _) = ct.TransformPoint(minx, maxy)
        (x4, y4, _) = ct.TransformPoint(maxx, miny)
        xs = [x1, x2, x3, x4]
        ys = [y1, y2, y3, y4]
        return (min(xs), min(ys), max(xs), max(ys))
    except Exception as e:
        print(f"Bbox transform to EPSG:{epsg} failed: {e}", file=sys.stderr)
        return (minx, miny, maxx, maxy)


def clip_raster_to_bbox(src: Path, out: Path, minx: float, miny: float, maxx: float, maxy: float) -> bool:
    """Clip raster to bbox (all data within bounds). Bbox is in WGS84; if raster is in another CRS
    (e.g. London BNG 27700), the bbox is transformed to the raster CRS for -projwin. Output keeps source CRS."""
    if not src.exists():
        return False
    if not shutil.which("gdal_translate"):
        return False
    try:
        from osgeo import gdal, osr
        ds = gdal.Open(str(src))
        if ds is None:
            return False
        prj = ds.GetProjection()
        ds = None
        if not prj:
            projwin = (minx, maxy, maxx, miny)
        else:
            srs = osr.SpatialReference()
            srs.ImportFromWkt(prj)
            # Get EPSG (e.g. 27700 for BNG); may be on PROJCS or GEOGCS
            epsg = 4326
            try:
                if srs.AutoIdentifyEPSG() == 0:
                    auth = srs.GetAuthorityCode(None)
                    if auth:
                        epsg = int(auth)
                else:
                    auth = srs.GetAttrValue("AUTHORITY", 1)
                    if auth and auth.isdigit():
                        epsg = int(auth)
            except Exception:
                pass
            tminx, tminy, tmaxx, tmaxy = _wgs84_bbox_to_crs(minx, miny, maxx, maxy, epsg)
            projwin = (tminx, tmaxy, tmaxx, tminy)  # ulx uly lrx lry
        if _run([
            "gdal_translate", "-q", "-projwin", str(projwin[0]), str(projwin[1]), str(projwin[2]), str(projwin[3]),
            str(src), str(out)
        ]):
            return True
    except Exception as e:
        print(f"Raster clip error {src}: {e}", file=sys.stderr)
    return False


def create_synthetic_raster(out: Path, minx: float, miny: float, maxx: float, maxy: float) -> bool:
    """Create a small synthetic GeoTIFF covering the bbox with a viewable gradient (not all zeros)."""
    try:
        import numpy as np
        from osgeo import gdal, osr
    except ImportError:
        return False
    w, h = 64, 64
    res_x = (maxx - minx) / w
    res_y = (maxy - miny) / h
    out.parent.mkdir(parents=True, exist_ok=True)
    drv = gdal.GetDriverByName("GTiff")
    ds = drv.Create(str(out), w, h, 1, gdal.GDT_Byte)
    ds.SetGeoTransform([minx, res_x, 0, maxy, 0, -res_y])
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    ds.SetProjection(srs.ExportToWkt())
    # Gradient 0–255 so the raster is human-viewable (not solid black)
    x_ramp = np.linspace(0, 255, w, dtype=np.uint8)
    y_ramp = np.linspace(0, 255, h, dtype=np.uint8)
    arr = (np.add.outer(y_ramp, x_ramp) // 2).astype(np.uint8)
    band = ds.GetRasterBand(1)
    band.WriteArray(arr)
    band.FlushCache()
    ds = None
    return True


def _scale_raster_to_viewable_byte(raster_path: Path) -> bool:
    """Write a separate Byte 0-255 GeoTIFF (stem_byte.tif) for human viewing; keep the original .tif for tests.
    Uses -scale with no src range so GDAL stretches actual min/max to 0-255 (viewable in QGIS etc.)."""
    if not shutil.which("gdal_translate") or not raster_path.exists():
        return False
    out_path = raster_path.parent / (raster_path.stem + "_byte.tif")
    try:
        # -scale with no args: compute src min/max from raster, map to 0-255 (avoids black clips)
        rc = subprocess.run(
            [
                "gdal_translate", "-q", "-ot", "Byte", "-scale",
                str(raster_path), str(out_path),
            ],
            capture_output=True,
            text=True,
        )
        return rc.returncode == 0 and out_path.exists()
    except Exception:
        pass
    return False


def shapefile_to_zip(shp_path: Path, zip_path: Path) -> bool:
    """Zip shapefile components into .shp.zip. shp_path is the .shp file."""
    base = shp_path.stem
    out_dir = shp_path.parent
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for ext in [".shp", ".shx", ".dbf", ".prj", ".cpg", ".sbn", ".sbx"]:
            f = out_dir / f"{base}{ext}"
            if f.exists():
                zf.write(f, f.name)
                f.unlink()
    return True


def run(
    source_root: Path,
    out_root: Path,
    nyc_lon: float, nyc_lat: float,
    london_lon: float, london_lat: float,
    bbox_size: float,
    max_rows: int,
) -> bool:
    nyc_bbox = bbox(nyc_lon, nyc_lat, bbox_size)
    london_bbox = bbox(london_lon, london_lat, bbox_size)
    nyc_minx, nyc_miny, nyc_maxx, nyc_maxy = nyc_bbox
    lon_minx, lon_miny, lon_maxx, lon_maxy = london_bbox
    ok = True

    # NYC vector
    nyc_boroughs_src = source_root / "nyc" / "boroughs" / "nyc_boroughs.geojson"
    out_b = out_root / "nyc" / "boroughs"
    ensure_dir(out_b)
    if nyc_boroughs_src.exists():
        ok &= clip_vector_to_bbox(nyc_boroughs_src, out_b / "nyc_boroughs.geojson", *nyc_bbox, max_rows)
        ok &= write_geojsonl_from_geojson(nyc_boroughs_src, out_b / "nyc_boroughs.geojsonl", *nyc_bbox, max_rows)
    else:
        print("Skipping nyc/boroughs (source missing).")

    for rel, src_name, out_name in [
        ("nyc/taxi-zones", "nyc_taxi_zones.geojson", "nyc_taxi_zones.geojson"),
        ("nyc/neighborhoods", "nyc_nta.geojson", "nyc_nta.geojson"),
    ]:
        src = source_root / rel / src_name
        out_dir = out_root / rel
        if src.exists():
            ensure_dir(out_dir)
            ok &= clip_vector_to_bbox(src, out_dir / out_name, *nyc_bbox, max_rows)
        else:
            print(f"Skipping {rel} (source missing).")

    # NYC subway: shapefile zip from source or from clipped points
    nyc_subway_src = source_root / "nyc" / "subway" / "nyc_subway.shp.zip"
    out_subway = out_root / "nyc" / "subway"
    ensure_dir(out_subway)
    if nyc_subway_src.exists():
        try:
            import geopandas as gpd
            from shapely.geometry import box
            import zipfile as zf
            with zf.ZipFile(nyc_subway_src) as z:
                # read .shp from zip (need to extract to temp or use fiona/gdal)
                import tempfile
                with tempfile.TemporaryDirectory() as tmp:
                    z.extractall(tmp)
                    shps = list(Path(tmp).glob("*.shp"))
                    if shps:
                        gdf = gpd.read_file(shps[0]).to_crs("EPSG:4326")
                        clip = box(*nyc_bbox)
                        gdf = gdf[gdf.intersects(clip)].head(max_rows)
                        if gdf.empty:
                            gdf = gpd.read_file(shps[0]).to_crs("EPSG:4326").head(max_rows)
                        shp_out = out_subway / "nyc_subway.shp"
                        gdf.to_file(shp_out)
                        shapefile_to_zip(out_subway / "nyc_subway.shp", out_subway / "nyc_subway.shp.zip")
                        (out_subway / "nyc_subway.shp").unlink(missing_ok=True)
                        for ext in [".shx", ".dbf", ".prj", ".cpg", ".sbn", ".sbx"]:
                            (out_subway / f"nyc_subway{ext}").unlink(missing_ok=True)
        except Exception as e:
            print(f"Subway clip error: {e}", file=sys.stderr)
            ok = False
    else:
        # Fallback: use existing minimal points if present
        pts = out_root / "nyc" / "subway" / "nyc_subway_points.geojson"
        if pts.exists():
            try:
                import geopandas as gpd
                gdf = gpd.read_file(pts)
                if gdf.crs is None:
                    gdf.set_crs("EPSG:4326", inplace=True)
                gdf.to_file(out_subway / "nyc_subway.shp")
                shapefile_to_zip(out_subway / "nyc_subway.shp", out_subway / "nyc_subway.shp.zip")
                (out_subway / "nyc_subway.shp").unlink(missing_ok=True)
                for ext in [".shx", ".dbf", ".prj", ".cpg", ".sbn", ".sbx"]:
                    (out_subway / f"nyc_subway{ext}").unlink(missing_ok=True)
            except Exception as e:
                print(f"Subway from points: {e}", file=sys.stderr)
        else:
            print("Skipping nyc/subway (no source or points).")

    # NYC parks shapefile (clip like subway)
    nyc_parks_src = source_root / "nyc" / "parks"
    parks_zips = list(nyc_parks_src.glob("*.shp.zip")) if nyc_parks_src.exists() else []
    if not parks_zips and nyc_parks_src.exists():
        parks_zips = list(nyc_parks_src.glob("*.zip"))
    out_parks = out_root / "nyc" / "parks"
    ensure_dir(out_parks)
    if parks_zips:
        try:
            import geopandas as gpd
            from shapely.geometry import box
            import zipfile as zf
            with zf.ZipFile(parks_zips[0]) as z:
                import tempfile
                with tempfile.TemporaryDirectory() as tmp:
                    z.extractall(tmp)
                    shps = list(Path(tmp).glob("*.shp"))
                    if shps:
                        gdf = gpd.read_file(shps[0]).to_crs("EPSG:4326")
                        clip = box(*nyc_bbox)
                        gdf = gdf[gdf.intersects(clip)].head(max_rows)
                        if gdf.empty:
                            gdf = gpd.read_file(shps[0]).to_crs("EPSG:4326").head(max_rows)
                        base = "nyc_parks"
                        shp_out = out_parks / f"{base}.shp"
                        gdf.to_file(shp_out)
                        shapefile_to_zip(shp_out, out_parks / f"{base}.shp.zip")
                        shp_out.unlink(missing_ok=True)
                        for ext in [".shx", ".dbf", ".prj", ".cpg", ".sbn", ".sbx"]:
                            (out_parks / f"{base}{ext}").unlink(missing_ok=True)
        except Exception as e:
            print(f"Parks clip error: {e}", file=sys.stderr)
            ok = False

    # NYC geopackage: from clipped boroughs
    boroughs_geojson = out_root / "nyc" / "boroughs" / "nyc_boroughs.geojson"
    out_gpkg = out_root / "nyc" / "geopackage" / "nyc_complete.gpkg"
    ensure_dir(out_gpkg.parent)
    if boroughs_geojson.exists():
        if shutil.which("ogr2ogr"):
            _run(["ogr2ogr", "-f", "GPKG", str(out_gpkg), str(boroughs_geojson), "-nln", "boroughs"])
        else:
            try:
                import geopandas as gpd
                gdf = gpd.read_file(boroughs_geojson)
                gdf.to_file(out_gpkg, layer="boroughs", driver="GPKG")
            except Exception as e:
                print(f"GeoPackage: {e}", file=sys.stderr)
                ok = False

    # NYC raster: sentinel2 (scale to Byte 0-255 when clipped so viewable)
    sentinel_src = source_root / "nyc" / "sentinel2" / "nyc_sentinel2_red.tif"
    out_sentinel = out_root / "nyc" / "sentinel2" / "nyc_sentinel2_red.tif"
    ensure_dir(out_sentinel.parent)
    if sentinel_src.exists():
        ok &= clip_raster_to_bbox(sentinel_src, out_sentinel, *nyc_bbox)
        if out_sentinel.exists():
            _scale_raster_to_viewable_byte(out_sentinel)
    if not out_sentinel.exists():
        ok &= create_synthetic_raster(out_sentinel, *nyc_bbox)

    # NYC elevation: GeoTIFF only (GDAL supports GTiff everywhere; .hgt/SRTMHGT is optional)
    for tile in ["srtm_n40w073.tif", "srtm_n40w074.tif"]:
        src_tif = source_root / "nyc" / "elevation" / tile
        out_path = out_root / "nyc" / "elevation" / tile
        ensure_dir(out_path.parent)
        if src_tif.exists():
            if not clip_raster_to_bbox(src_tif, out_path, *nyc_bbox):
                ok &= create_synthetic_raster(out_path, *nyc_bbox)
        else:
            ok &= create_synthetic_raster(out_path, *nyc_bbox)

    # London vector
    for rel, fname in [
        ("london/postcodes", "london_postcodes.geojson"),
        ("london/boroughs", "london_boroughs.geojson"),
    ]:
        src = source_root / rel / fname
        out_dir = out_root / rel
        if src.exists():
            ensure_dir(out_dir)
            ok &= clip_vector_to_bbox(src, out_dir / fname, *london_bbox, max_rows)
        else:
            print(f"Skipping {rel} (source missing).")

    # London raster: sentinel2 (scale to Byte 0-255 when clipped so viewable)
    lon_sentinel_src = source_root / "london" / "sentinel2" / "london_sentinel2_red.tif"
    out_lon_sentinel = out_root / "london" / "sentinel2" / "london_sentinel2_red.tif"
    ensure_dir(out_lon_sentinel.parent)
    if lon_sentinel_src.exists():
        ok &= clip_raster_to_bbox(lon_sentinel_src, out_lon_sentinel, *london_bbox)
        if out_lon_sentinel.exists():
            _scale_raster_to_viewable_byte(out_lon_sentinel)
    if not out_lon_sentinel.exists():
        ok &= create_synthetic_raster(out_lon_sentinel, *london_bbox)

    lon_elev_src = source_root / "london" / "elevation" / "srtm_n51w001.tif"
    out_lon_elev = out_root / "london" / "elevation" / "srtm_n51w001.tif"
    ensure_dir(out_lon_elev.parent)
    if lon_elev_src.exists():
        if not clip_raster_to_bbox(lon_elev_src, out_lon_elev, *london_bbox):
            ok &= create_synthetic_raster(out_lon_elev, *london_bbox)
    else:
        ok &= create_synthetic_raster(out_lon_elev, *london_bbox)

    # FileGDB: copy one file if present (no clip for now)
    fgdb_src = source_root / "nyc" / "filegdb" / "NYC_Sample.gdb.zip"
    if fgdb_src.exists():
        ensure_dir(out_root / "nyc" / "filegdb")
        shutil.copy2(fgdb_src, out_root / "nyc" / "filegdb" / "NYC_Sample.gdb.zip")
    # HRRR: subset one GRIB2 to NYC bbox so minimal bundle has a small sample (~few MB instead of ~130MB)
    hrrr_dir = source_root / "nyc" / "hrrr-weather"
    hrrr_out = out_root / "nyc" / "hrrr-weather"
    if hrrr_dir.exists():
        gribs = list(hrrr_dir.glob("*.grib2"))
        if gribs:
            ensure_dir(hrrr_out)
            src_grib = gribs[0]
            out_grib = hrrr_out / src_grib.name
            # -projwin ulx uly lrx lry (WGS84: minx, maxy, maxx, miny for NYC)
            projwin = (nyc_minx, nyc_maxy, nyc_maxx, nyc_miny)
            if _run([
                "gdal_translate", "-q", "-projwin",
                str(projwin[0]), str(projwin[1]), str(projwin[2]), str(projwin[3]),
                str(src_grib), str(out_grib),
            ]):
                pass  # subset written
            else:
                # Subset failed (e.g. GRIB2 driver); remove any stale big file from previous run
                if out_grib.exists() and out_grib.stat().st_size > 50 * 1024 * 1024:
                    out_grib.unlink()
                if hrrr_out.exists() and not any(hrrr_out.iterdir()):
                    hrrr_out.rmdir()

    return ok


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Generate minimal doc-test bundle from full sample-data (bbox-based extraction)."
    )
    ap.add_argument(
        "--source",
        type=Path,
        default=SCRIPT_DIR / "Volumes" / "main" / "default" / "geobrix_samples" / "geobrix-examples",
        help="Source root (full bundle)",
    )
    ap.add_argument("--out", type=Path, default=OUT_ROOT, help="Output root (default: test-data/geobrix-examples)")
    ap.add_argument("--nyc-lon", type=float, default=DEFAULT_NYC_LON, help="NYC center longitude (default: Manhattan)")
    ap.add_argument("--nyc-lat", type=float, default=DEFAULT_NYC_LAT, help="NYC center latitude")
    ap.add_argument("--london-lon", type=float, default=DEFAULT_LONDON_LON, help="London center longitude")
    ap.add_argument("--london-lat", type=float, default=DEFAULT_LONDON_LAT, help="London center latitude")
    ap.add_argument("--bbox-size", type=float, default=DEFAULT_BBOX_SIZE,
                    help="Half-width/height of bbox in degrees (default: 0.02)")
    ap.add_argument("--max-rows", type=int, default=DEFAULT_MAX_ROWS,
                    help="Max vector features per layer (default: 10)")
    args = ap.parse_args()

    if not args.source.exists():
        print(f"Source not found: {args.source}", file=sys.stderr)
        print("Run essential/complete bundle download first, or set --source.", file=sys.stderr)
        return 2

    args.out.mkdir(parents=True, exist_ok=True)
    print(f"Generating minimal bundle at {args.out.relative_to(SCRIPT_DIR)}")
    print(f"  NYC center: ({args.nyc_lon}, {args.nyc_lat}), London: ({args.london_lon}, {args.london_lat})")
    print(f"  bbox_size={args.bbox_size}, max_rows={args.max_rows}")
    ok = run(
        args.source, args.out,
        args.nyc_lon, args.nyc_lat,
        args.london_lon, args.london_lat,
        args.bbox_size, args.max_rows,
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
