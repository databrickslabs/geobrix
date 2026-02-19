"""
Internal implementation for GeoBrix sample-data bundles (Essential and Complete).

This module is shipped in the GeoBrix WHL. It downloads geospatial sample data
to a Unity Catalog Volumes path: uses a temp directory for interim work
(downloads, zips, conversions) and copies only final files to the Volume.
Skips datasets that already exist at the destination.

Requires Python 3.10+. Optional runtime deps: requests,
pystac-client, planetary-computer, geopandas (see function docstrings).
"""

from pathlib import Path
from typing import List, Optional, Tuple
import errno
import gzip
import io
import os
import shutil
import tempfile

try:
    import requests
except ImportError:
    requests = None


def get_volumes_path(catalog: str, schema: str, volume: str) -> str:
    """Return the base path for sample data under Unity Catalog Volumes.

    Args:
        catalog: Unity Catalog name.
        schema: Schema name.
        volume: Volume name.

    Returns:
        Path string of the form /Volumes/{catalog}/{schema}/{volume}/geobrix-examples.
    """
    return f"/Volumes/{catalog}/{schema}/{volume}/geobrix-examples"


def _unity_catalog_volume_root(path: Path) -> Optional[Path]:
    """Return the Unity Catalog volume root if path is under /Volumes/, else None.

    In UC, the volume root is /Volumes/{catalog}/{schema}/{volume_name}; only that
    path cannot be created by code (Volume must already exist). Paths under it
    (e.g. .../volume_name/sample_data/geobrix-examples) can be created.
    """
    p = Path(path)
    try:
        resolved = p.resolve()
    except OSError:
        resolved = p
    parts = resolved.parts
    # /Volumes/catalog/schema/volume_name -> first 5 parts: /, Volumes, catalog, schema, volume_name
    if len(parts) >= 5 and parts[0] in ("/", "") and parts[1].lower() == "volumes":
        return Path(*parts[:5])
    return None


def get_temp_dir(temp_dir: Optional[str] = None) -> Path:
    """Return a temp directory for interim downloads and conversions.

    If temp_dir is given, returns that path (creating it if needed). Otherwise
    returns a path under the system temp dir (e.g. .../geobrix_bundle_build).
    Caller should create the directory if using for writes.

    Args:
        temp_dir: Optional directory path to use; if None, uses system temp.

    Returns:
        Path to the temp directory.
    """
    if temp_dir:
        p = Path(temp_dir)
        try:
            p.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass  # Volume path must already exist (e.g. Databricks)
        return p
    p = Path(tempfile.gettempdir()) / "geobrix_bundle_build"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _ensure_dir(path: Path | str, volume_root: Optional[Path | str] = None) -> None:
    """Create directory and parents if they do not exist.

    On a Databricks cluster, /Volumes/... is FUSE-mounted; Path.mkdir(..., exist_ok=True)
    does not throw for the volume root (it is idempotent). The Volume itself must pre-exist;
    paths under it (e.g. .../volume_name/geobrix-examples/...) are created with Path.mkdir.
    """
    path = Path(path)
    if volume_root is not None:
        volume_root = Path(volume_root)
        if path == volume_root:
            return  # Volume root must already exist; do not try to create it
    path.mkdir(parents=True, exist_ok=True)


def _is_volume_path(path: Path) -> bool:
    """True if path looks like a Unity Catalog Volume path (/Volumes/catalog/schema/volume/...)."""
    parts = Path(path).parts
    return (
        len(parts) >= 5
        and parts[0] in ("/", "")
        and parts[1].lower() == "volumes"
    )


def _path_exists_for_skip(path: Path | str) -> tuple[bool, Optional[float]]:
    """Return (exists, size_mb). On cluster, /Volumes/... is FUSE-mounted; use path.exists() and path.stat().
    Avoid random access (seek) on volume paths; sequential read/write and temp-file-then-copy are fine.
    """
    path = Path(path)
    _bundle_debug("_path_exists_for_skip(%s)" % path)
    try:
        if path.exists():
            size_mb = path.stat().st_size / (1024 * 1024)
            _bundle_debug("_path_exists_for_skip: exists=True size_mb=%s" % size_mb)
            return True, size_mb
        return False, None
    except OSError as e:
        if e.errno == getattr(errno, "ENOENT", 2):
            return False, None
        if e.errno == getattr(errno, "EOPNOTSUPP", 95) and _is_volume_path(path):
            # Fallback: check by listing parent (FUSE-safe)
            try:
                parent, name = path.parent, path.name
                if name in os.listdir(parent):
                    return True, None
            except OSError:
                pass
            return False, None
        raise


def _copy_final_to_volumes(
    temp_file: Path,
    volumes_subpath: Path,
    description: str,
    *,
    volume_root: Optional[Path] = None,
) -> Optional[Path]:
    """Copy a file from temp to the Volumes path; skip if destination already exists.

    Args:
        temp_file: Source file (e.g. in a temp directory).
        volumes_subpath: Destination path under the Volume.
        description: Short label used in log messages.
        volume_root: If set (e.g. Unity Catalog Volume root), directory creation
            will not try to create the volume itself (must already exist).

    Returns:
        The destination path (whether copied or skipped).
    """
    dest = Path(volumes_subpath)
    exists_skip, size_mb = _path_exists_for_skip(dest)
    if exists_skip and size_mb is not None:
        print(f"⏭️  {description}: {size_mb:.1f} MB (already exists)")
        return dest
    # On cluster, /Volumes/... is FUSE-mounted; use pathlib + shutil (no SDK).
    _ensure_dir(dest.parent, volume_root=volume_root)
    shutil.copy2(Path(temp_file), dest)
    size_mb = dest.stat().st_size / (1024 * 1024)
    print(f"✅ {description}: {size_mb:.1f} MB")
    return dest


def download_to_path(
    url: str,
    dest_path: Path,
    description: str,
    *,
    skip_if_exists: bool = True,
    quiet: bool = False,
) -> Optional[Path]:
    """Download a URL to a local file.

    Args:
        url: HTTP(S) URL to fetch.
        dest_path: Local path to write the file.
        description: Short label for log messages.
        skip_if_exists: If True and dest_path exists, skip download and return path.
        quiet: If True, do not print download/success messages (caller will report).

    Returns:
        The destination path (after download or when skipped).

    Raises:
        RuntimeError: If the requests library is not installed.
        requests.HTTPError: On HTTP errors (e.g. 404).
    """
    if skip_if_exists and dest_path.exists():
        if not quiet:
            size_mb = dest_path.stat().st_size / (1024 * 1024)
            print(f"⏭️  {description}: {size_mb:.1f} MB (already exists)")
        return dest_path
    if not requests:
        raise RuntimeError("requests is required; pip install requests")
    if not quiet:
        print(f"⬇️  Downloading {description}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    _ensure_dir(dest_path.parent)
    with open(dest_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    if not quiet:
        size_mb = dest_path.stat().st_size / (1024 * 1024)
        print(f"✅ {description}: {size_mb:.1f} MB")
    return dest_path


def download_srtm_to_path(
    tile: str,
    dest_path: Path,
    description: str,
    *,
    skip_if_exists: bool = True,
    quiet: bool = False,
) -> Optional[Path]:
    """Download and decompress an SRTM elevation tile from AWS to a local file.

    Tile names follow the pattern N40W074 (latitude band + longitude). The file
    is fetched as .hgt.gz and written as decompressed .hgt.

    Args:
        tile: SRTM tile id (e.g. N40W074).
        dest_path: Local path for the decompressed .hgt file.
        description: Short label for log messages.
        skip_if_exists: If True and dest_path exists, skip download and return path.
        quiet: If True, do not print download/success messages (caller will report).

    Returns:
        The destination path (after download or when skipped).

    Raises:
        RuntimeError: If the requests library is not installed.
    """
    if skip_if_exists and dest_path.exists():
        if not quiet:
            size_mb = dest_path.stat().st_size / (1024 * 1024)
            print(f"⏭️  {description}: {size_mb:.1f} MB (already exists)")
        return dest_path
    if not requests:
        raise RuntimeError("requests is required")
    lat_dir = tile[:3]
    url = f"https://elevation-tiles-prod.s3.amazonaws.com/skadi/{lat_dir}/{tile}.hgt.gz"
    if not quiet:
        print(f"⬇️  Downloading {description}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    _ensure_dir(dest_path.parent)
    with gzip.open(io.BytesIO(response.content), "rb") as f_in:
        with open(dest_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    if not quiet:
        size_mb = dest_path.stat().st_size / (1024 * 1024)
        print(f"✅ {description}: {size_mb:.1f} MB")
    return dest_path


def _bundle_debug(msg: str) -> None:
    if os.environ.get("GBX_BUNDLE_DEBUG"):
        print(f"[bundle] {msg}", flush=True)


def run_essential_bundle(
    volumes_path: str,
    temp_dir: Optional[str] = None,
) -> dict:
    """Download the Essential sample-data bundle (~355 MB) to a Volumes path.

    Fetches NYC and London vector data (GeoJSON), Sentinel-2 raster (red band),
    and SRTM elevation for both regions. All interim work (downloads, decompression)
    is done in a temp directory; only final files are copied to volumes_path.
    Existing files at the destination are skipped.

    Args:
        volumes_path: Base path (e.g. from get_volumes_path) where data is written.
        temp_dir: Optional directory for temp files; if None, uses system temp.

    Returns:
        Dict with keys:
        - "errors": list of (dataset_name, error_message) for failed datasets.
        - "file_count": total number of files under volumes_path.
        - "total_size_mb": total size in MB of those files.

    Requires:
        requests. For Sentinel-2: pystac-client, planetary-computer.
    """
    _bundle_debug("run_essential_bundle start")
    base = Path(volumes_path)
    # Only the UC volume root (/Volumes/catalog/schema/volume_name) cannot be created; paths under it can.
    volume_root = _unity_catalog_volume_root(base)
    _bundle_debug("volume_root=%s" % volume_root)
    tmp = get_temp_dir(temp_dir)
    errors: List[Tuple[str, str]] = []

    def vol(subpath: str) -> Path:
        return base / subpath

    def tmp_path(*parts: str) -> Path:
        return tmp / Path(*parts)

    def vol_dir_resolve(subpath: str) -> Path:
        p = base

        for s in subpath.split("/"):
            p = p / s
            p.mkdir(parents=False, exist_ok=True)
        return p.resolve()

    def tmp_dir_resolve(*parts: str) -> Path:
        p = tmp
        for s in parts:
            p = p / s
            p.mkdir(parents=False, exist_ok=True)
        return p.resolve()

    try:
        import pystac_client
        import planetary_computer
    except ImportError:
        pystac_client = planetary_computer = None

    def download_sentinel2(bbox, subfolder: str, filename: str, region_name: str) -> Optional[Path]:
        out = vol(f"{subfolder}/{filename}")
        exists_skip, size_mb = _path_exists_for_skip(out)
        if exists_skip and size_mb is not None:
            print(f"⏭️  Sentinel-2 {region_name}: {size_mb:.1f} MB (already exists)")
            return out
        if not pystac_client or not planetary_computer:
            raise RuntimeError(
                "pystac-client and planetary-computer required for Sentinel-2; "
                "pip install pystac-client planetary-computer"
            )
        catalog = pystac_client.Client.open(
            "https://planetarycomputer.microsoft.com/api/stac/v1",
            modifier=planetary_computer.sign_inplace,
        )
        search = catalog.search(
            collections=["sentinel-2-l2a"],
            bbox=bbox,
            datetime="2023-06-01/2023-08-31",
            query={"eo:cloud_cover": {"lt": 30}},
            limit=10,
        )
        items = list(search.items())
        if not items:
            print(f"⚠️  No Sentinel-2 scenes found for {region_name}")
            return None
        best = min(items, key=lambda x: x.properties.get("eo:cloud_cover", 100))
        red_url = best.assets["B04"].href
        print(f"⬇️  Downloading Sentinel-2 {region_name} (scene: {best.id})...")
        tmp_file = tmp_path("sentinel2", filename)
        tmp_file.parent.mkdir(parents=True, exist_ok=True)
        response = requests.get(red_url, stream=True)
        response.raise_for_status()
        with open(tmp_file, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        _copy_final_to_volumes(tmp_file, out, f"Sentinel-2 {region_name}", volume_root=volume_root)
        return out

    _bundle_debug("Starting NYC Vector Data")
    print("\n📍 NYC Vector Data")
    try:
        dest = vol_dir_resolve("nyc/taxi-zones") / "nyc_taxi_zones.geojson"
        exists_skip, size_mb = _path_exists_for_skip(dest)
        if exists_skip and size_mb is not None:
            print(f"⏭️  NYC Taxi Zones: {size_mb:.1f} MB (already exists)")
        else:
            print(f"⬇️  Downloading NYC Taxi Zones...")
            t = tmp_dir_resolve("nyc", "taxi-zones") / "nyc_taxi_zones.geojson"
            download_to_path(
                "https://data.cityofnewyork.us/resource/8meu-9t5y.geojson?$limit=300",
                t,
                "NYC Taxi Zones",
                skip_if_exists=False,
                quiet=True,
            )
            if t.exists():
                _copy_final_to_volumes(t, dest, "NYC Taxi Zones", volume_root=volume_root)
    except Exception as e:
        errors.append(("NYC Taxi Zones", str(e)))
        print(f"⚠️  NYC Taxi Zones failed: {e}")

    try:
        dest = vol("nyc/boroughs/nyc_boroughs.geojson")
        exists_skip, size_mb = _path_exists_for_skip(dest)
        if exists_skip and size_mb is not None:
            print(f"⏭️  NYC Boroughs: {size_mb:.1f} MB (already exists)")
        else:
            print(f"⬇️  Downloading NYC Boroughs...")
            t = tmp_path("nyc", "boroughs", "nyc_boroughs.geojson")
            download_to_path(
                "https://data.cityofnewyork.us/resource/gthc-hcne.geojson?$limit=10",
                t,
                "NYC Boroughs",
                skip_if_exists=False,
                quiet=True,
            )
            if t.exists():
                _copy_final_to_volumes(t, dest, "NYC Boroughs", volume_root=volume_root)
    except Exception as e:
        errors.append(("NYC Boroughs", str(e)))
        print(f"⚠️  NYC Boroughs failed: {e}")

    print("\n📍 London Vector Data")
    try:
        dest = vol("london/postcodes/london_postcodes.geojson")
        exists_skip, size_mb = _path_exists_for_skip(dest)
        if exists_skip and size_mb is not None:
            print(f"⏭️  London Postcodes: {size_mb:.1f} MB (already exists)")
        else:
            print(f"⬇️  Downloading London Postcodes...")
            t = tmp_path("london", "postcodes", "london_postcodes.geojson")
            download_to_path(
                "https://raw.githubusercontent.com/sjwhitworth/london_geojson/master/london_postcodes.json",
                t,
                "London Postcodes",
                skip_if_exists=False,
                quiet=True,
            )
            if t.exists():
                _copy_final_to_volumes(t, dest, "London Postcodes", volume_root=volume_root)
    except Exception as e:
        errors.append(("London Postcodes", str(e)))
        print(f"⚠️  London Postcodes failed: {e}")

    print("\n🛰️  NYC Raster Data")
    try:
        download_sentinel2(
            [-74.25, 40.50, -73.70, 40.92],
            "nyc/sentinel2",
            "nyc_sentinel2_red.tif",
            "NYC",
        )
    except Exception as e:
        errors.append(("NYC Sentinel-2", str(e)))
        print(f"⚠️  NYC Sentinel-2 failed: {e}")

    print("\n⛰️  NYC Elevation")
    try:
        dest = vol("nyc/elevation/srtm_n40w074.hgt")
        exists_skip, size_mb = _path_exists_for_skip(dest)
        if exists_skip and size_mb is not None:
            print(f"⏭️  SRTM NYC: {size_mb:.1f} MB (already exists)")
        else:
            print(f"⬇️  Downloading SRTM NYC...")
            t = tmp_path("nyc", "elevation", "srtm_n40w074.hgt")
            download_srtm_to_path("N40W074", t, "SRTM NYC", skip_if_exists=False, quiet=True)
            if t.exists():
                _copy_final_to_volumes(t, dest, "SRTM NYC", volume_root=volume_root)
    except Exception as e:
        errors.append(("SRTM NYC", str(e)))
        print(f"⚠️  SRTM NYC failed: {e}")

    print("\n⛰️  London Elevation")
    try:
        dest = vol("london/elevation/srtm_n51w001.hgt")
        exists_skip, size_mb = _path_exists_for_skip(dest)
        if exists_skip and size_mb is not None:
            print(f"⏭️  SRTM London: {size_mb:.1f} MB (already exists)")
        else:
            print(f"⬇️  Downloading SRTM London...")
            t = tmp_path("london", "elevation", "srtm_n51w001.hgt")
            download_srtm_to_path("N51W001", t, "SRTM London", skip_if_exists=False, quiet=True)
            if t.exists():
                _copy_final_to_volumes(t, dest, "SRTM London", volume_root=volume_root)
    except Exception as e:
        errors.append(("SRTM London", str(e)))
        print(f"⚠️  SRTM London failed: {e}")

    print("\n🛰️  London Raster Data")
    try:
        download_sentinel2(
            [-0.51, 51.28, 0.33, 51.70],
            "london/sentinel2",
            "london_sentinel2_red.tif",
            "London",
        )
    except Exception as e:
        errors.append(("London Sentinel-2", str(e)))
        print(f"⚠️  London Sentinel-2 failed: {e}")

    try:
        file_count = sum(1 for _ in base.rglob("*") if _.is_file())
        total_size = sum(f.stat().st_size for f in base.rglob("*") if f.is_file()) / (1024 * 1024)
    except OSError:
        file_count = 0
        total_size = 0.0
    return {"errors": errors, "file_count": file_count, "total_size_mb": total_size}


def run_complete_bundle(
    volumes_path: str,
    temp_dir: Optional[str] = None,
) -> dict:
    """Download the Complete sample-data bundle additions (~440 MB more).

    Run after run_essential_bundle. Adds NYC neighborhoods, extra SRTM tile,
    London boroughs (GeoJSON), HRRR weather (GRIB2), NYC parks and subway
    shapefiles (delivered as single zips per dataset), and a multi-layer
    GeoPackage. All interim work (unzipping, GPKG build) uses temp_dir;
    only final assets are written to volumes_path. Skips if already present.

    Args:
        volumes_path: Base path (same as used for Essential bundle).
        temp_dir: Optional directory for temp files; if None, uses system temp.

    Returns:
        Dict with keys:
        - "errors": list of (dataset_name, error_message) for failed datasets.
        - "file_count": total number of files under volumes_path.
        - "total_size_mb": total size in MB of those files.

    Requires:
        requests. For London boroughs and GeoPackage: geopandas.
    """
    import zipfile
    from datetime import datetime, timedelta, timezone

    # datetime.UTC exists in 3.11+; use timezone.utc on 3.10
    _utc = getattr(datetime, "UTC", timezone.utc)

    base = Path(volumes_path)
    volume_root = _unity_catalog_volume_root(base)
    tmp = get_temp_dir(temp_dir)
    tmp.mkdir(parents=True, exist_ok=True)
    errors: List[Tuple[str, str]] = []

    def vol(subpath: str) -> Path:
        return base / subpath

    def tmp_path(*parts: str) -> Path:
        return tmp / Path(*parts)

    print("\n📍 Additional Geographic Data")
    try:
        dest = vol("nyc/neighborhoods/nyc_nta.geojson")
        exists_skip, size_mb = _path_exists_for_skip(dest)
        if exists_skip and size_mb is not None:
            print(f"⏭️  NYC Neighborhoods: {size_mb:.1f} MB (already exists)")
        else:
            print(f"⬇️  Downloading NYC Neighborhoods...")
            t = tmp_path("nyc", "neighborhoods", "nyc_nta.geojson")
            download_to_path(
                "https://data.cityofnewyork.us/resource/9nt8-h7nd.geojson?$limit=250",
                t,
                "NYC Neighborhoods",
                skip_if_exists=False,
                quiet=True,
            )
            if t.exists():
                _copy_final_to_volumes(t, dest, "NYC Neighborhoods", volume_root=volume_root)
    except Exception as e:
        errors.append(("NYC Neighborhoods", str(e)))

    try:
        dest = vol("nyc/elevation/srtm_n40w073.hgt")
        exists_skip, size_mb = _path_exists_for_skip(dest)
        if exists_skip and size_mb is not None:
            print(f"⏭️  SRTM NYC East: {size_mb:.1f} MB (already exists)")
        else:
            print(f"⬇️  Downloading SRTM NYC East...")
            t = tmp_path("nyc", "elevation", "srtm_n40w073.hgt")
            download_srtm_to_path("N40W073", t, "SRTM NYC East", skip_if_exists=False, quiet=True)
            if t.exists():
                _copy_final_to_volumes(t, dest, "SRTM NYC East", volume_root=volume_root)
    except Exception as e:
        errors.append(("SRTM NYC East", str(e)))

    try:
        out_geojson = vol("london/boroughs/london_boroughs.geojson")
        exists_skip, size_mb = _path_exists_for_skip(out_geojson)
        if exists_skip and size_mb is not None:
            print(f"⏭️  London Boroughs: {size_mb:.1f} MB (already exists)")
        else:
            print(f"⬇️  Downloading London Boroughs...")
            url = "https://data.london.gov.uk/download/statistical-gis-boundary-files-london/9ba8c833-6370-4b11-abdc-314aa020d5e0/statistical-gis-boundaries-london.zip"
            zpath = tmp_path("london_boroughs.zip")
            download_to_path(url, zpath, "London Boroughs (zip)", skip_if_exists=False, quiet=True)
            if zpath.exists():
                tmp_extract = tmp_path("london_boroughs_extract")
                tmp_extract.mkdir(parents=True, exist_ok=True)
                with zipfile.ZipFile(zpath) as z:
                    for name in z.namelist():
                        if "London_Borough" in name:
                            z.extract(name, tmp_extract)
                shp_files = list(tmp_extract.glob("**/London_Borough*.shp"))
                if shp_files:
                    try:
                        import geopandas as gpd
                        gdf = gpd.read_file(str(shp_files[0]))
                        _ensure_dir(out_geojson.parent, volume_root=volume_root)
                        gdf.to_file(str(out_geojson), driver="GeoJSON")
                        print(f"✅ London Boroughs: {out_geojson.stat().st_size / (1024*1024):.1f} MB")
                    except ImportError:
                        errors.append(("London Boroughs", "geopandas required"))
                shutil.rmtree(tmp_extract, ignore_errors=True)
    except Exception as e:
        errors.append(("London Boroughs", str(e)))
        print(f"⚠️  London Boroughs failed: {e}")

    print("\n📦 Format Examples (shapefiles zipped; interim work in temp)")
    try:
        out_hrrr = vol("nyc/hrrr-weather")
        _ensure_dir(out_hrrr, volume_root=volume_root)
        try:
            existing_hrrr = list(out_hrrr.glob("*.grib2"))
        except OSError as e:
            if e.errno == getattr(errno, "EOPNOTSUPP", 95):
                existing_hrrr = []
            else:
                raise
        if existing_hrrr:
            try:
                size_mb = sum(f.stat().st_size for f in existing_hrrr) / (1024 * 1024)
            except OSError as e:
                if e.errno == getattr(errno, "EOPNOTSUPP", 95):
                    size_mb = 0.0
                else:
                    raise
            print(f"⏭️  HRRR Weather: {size_mb:.1f} MB (already exists)")
        else:
            print(f"⬇️  Downloading HRRR Weather...")
            yesterday = datetime.now(_utc) - timedelta(days=1)
            date_str = yesterday.strftime("%Y%m%d")
            hrrr_url = f"https://noaa-hrrr-bdp-pds.s3.amazonaws.com/hrrr.{date_str}/conus/hrrr.t12z.wrfsfcf00.grib2"
            fname = f"hrrr_nyc_{date_str}_12z.grib2"
            dest = out_hrrr / fname
            t = tmp_path("hrrr.grib2")
            download_to_path(hrrr_url, t, "HRRR Weather", skip_if_exists=False, quiet=True)
            if t.exists():
                _copy_final_to_volumes(t, dest, "HRRR Weather", volume_root=volume_root)
    except Exception as e:
        errors.append(("HRRR", str(e)))

    try:
        parks_zip_dest = vol("nyc/parks/nyc_parks.shp.zip")
        exists_skip, size_mb = _path_exists_for_skip(parks_zip_dest)
        if exists_skip and size_mb is not None:
            print(f"⏭️  NYC Parks: {size_mb:.1f} MB (already exists)")
        else:
            print(f"⬇️  Downloading NYC Parks...")
            url = "https://data.cityofnewyork.us/api/geospatial/enfh-gkve?method=export&format=Shapefile"
            tzip = tmp_path("nyc_parks_download.zip")
            download_to_path(url, tzip, "NYC Parks (zip)", skip_if_exists=False, quiet=True)
            if tzip.exists():
                extract_dir = tmp_path("nyc_parks_extract")
                extract_dir.mkdir(parents=True, exist_ok=True)
                with zipfile.ZipFile(tzip) as z:
                    z.extractall(extract_dir)
                out_zip_tmp = tmp_path("nyc_parks.shp.zip")
                with zipfile.ZipFile(out_zip_tmp, "w", zipfile.ZIP_DEFLATED) as zout:
                    for f in extract_dir.rglob("*"):
                        if f.is_file():
                            zout.write(f, f.relative_to(extract_dir))
                shutil.rmtree(extract_dir, ignore_errors=True)
                _ensure_dir(parks_zip_dest.parent, volume_root=volume_root)
                _copy_final_to_volumes(out_zip_tmp, parks_zip_dest, "NYC Parks", volume_root=volume_root)
                for old in parks_zip_dest.parent.glob("*"):
                    if old != parks_zip_dest and old.suffix.lower() in (".shp", ".dbf", ".prj", ".shx", ".cpg"):
                        try:
                            old.unlink()
                        except OSError:
                            pass
    except Exception as e:
        errors.append(("NYC Parks", str(e)))

    try:
        subway_zip_dest = vol("nyc/subway/nyc_subway.shp.zip")
        exists_skip, size_mb = _path_exists_for_skip(subway_zip_dest)
        if exists_skip and size_mb is not None:
            print(f"⏭️  NYC Subway: {size_mb:.1f} MB (already exists)")
        else:
            print(f"⬇️  Downloading NYC Subway...")
            url = "https://data.ny.gov/api/geospatial/i9wp-a4ja?method=export&format=Shapefile"
            tzip = tmp_path("nyc_subway_download.zip")
            download_to_path(url, tzip, "NYC Subway (zip)", skip_if_exists=False, quiet=True)
            if tzip.exists():
                extract_dir = tmp_path("nyc_subway_extract")
                extract_dir.mkdir(parents=True, exist_ok=True)
                with zipfile.ZipFile(tzip) as z:
                    z.extractall(extract_dir)
                out_zip_tmp = tmp_path("nyc_subway.shp.zip")
                with zipfile.ZipFile(out_zip_tmp, "w", zipfile.ZIP_DEFLATED) as zout:
                    for f in extract_dir.rglob("*"):
                        if f.is_file():
                            zout.write(f, f.relative_to(extract_dir))
                shutil.rmtree(extract_dir, ignore_errors=True)
                _ensure_dir(subway_zip_dest.parent, volume_root=volume_root)
                _copy_final_to_volumes(out_zip_tmp, subway_zip_dest, "NYC Subway", volume_root=volume_root)
                for old in subway_zip_dest.parent.glob("*"):
                    if old.suffix.lower() in (".shp", ".dbf", ".prj", ".shx", ".cpg") and old != subway_zip_dest:
                        try:
                            old.unlink()
                        except OSError:
                            pass
    except Exception as e:
        errors.append(("NYC Subway", str(e)))

    try:
        gpkg_dest = vol("nyc/geopackage/nyc_complete.gpkg")
        exists_skip, size_mb = _path_exists_for_skip(gpkg_dest)
        if exists_skip and size_mb is not None:
            print(f"⏭️  Multi-Layer GeoPackage: {size_mb:.1f} MB (already exists)")
        else:
            print(f"⬇️  Building Multi-Layer GeoPackage...")
            import geopandas as gpd
            gpkg_tmp = tmp_path("nyc_complete.gpkg")
            boroughs_file = vol("nyc/boroughs/nyc_boroughs.geojson")
            if _path_exists_for_skip(boroughs_file)[0]:
                gpd.read_file(str(boroughs_file)).to_file(str(gpkg_tmp), layer="boroughs", driver="GPKG")
            zones_file = vol("nyc/taxi-zones/nyc_taxi_zones.geojson")
            if _path_exists_for_skip(zones_file)[0]:
                gpd.read_file(str(zones_file)).to_file(str(gpkg_tmp), layer="taxi_zones", driver="GPKG", mode="a")
            parks_zip = vol("nyc/parks/nyc_parks.shp.zip")
            if _path_exists_for_skip(parks_zip)[0]:
                uri = "zip://" + parks_zip.resolve().as_posix()
                gpd.read_file(uri).to_file(str(gpkg_tmp), layer="parks", driver="GPKG", mode="a")
            subway_zip = vol("nyc/subway/nyc_subway.shp.zip")
            if _path_exists_for_skip(subway_zip)[0]:
                uri = "zip://" + subway_zip.resolve().as_posix()
                gpd.read_file(uri).to_file(str(gpkg_tmp), layer="subway_stations", driver="GPKG", mode="a")
            if gpkg_tmp.exists():
                _ensure_dir(gpkg_dest.parent, volume_root=volume_root)
                _copy_final_to_volumes(gpkg_tmp, gpkg_dest, "Multi-Layer GeoPackage", volume_root=volume_root)
    except ImportError:
        errors.append(("GeoPackage", "geopandas required"))
    except Exception as e:
        errors.append(("GeoPackage", str(e)))
        print(f"⚠️  GeoPackage failed: {e}")

    try:
        file_count = sum(1 for _ in base.rglob("*") if _.is_file())
        total_size = sum(f.stat().st_size for f in base.rglob("*") if f.is_file()) / (1024 * 1024)
    except OSError:
        file_count = 0
        total_size = 0.0
    return {"errors": errors, "file_count": file_count, "total_size_mb": total_size}
