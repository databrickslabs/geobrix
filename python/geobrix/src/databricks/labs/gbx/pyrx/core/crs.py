"""CRS string/int resolution — the one place the int-cast rule lives (light tier)."""

from typing import Optional, Union

from rasterio.crs import CRS


def _is_intlike(value) -> bool:
    if isinstance(value, int):
        return True
    if isinstance(value, str):
        try:
            int(value.strip())
            return True
        except (ValueError, AttributeError):
            return False
    return False


def resolve_crs(value: Union[int, str]) -> CRS:
    """int or int-castable string -> EPSG SRID; else a CRS string (EPSG:x/ESRI:x/WKT/PROJ4).

    Raises for garbage input — intended.
    """
    if _is_intlike(value):
        return CRS.from_epsg(int(str(value).strip()))
    return CRS.from_user_input(value)


def crs_to_canonical(crs: Optional[CRS]) -> Optional[str]:
    """Authority string ('EPSG:4326'/'ESRI:54008') when available, else WKT.

    None-safe: returns None when crs is None.
    """
    if crs is None:
        return None
    auth = crs.to_authority()  # (name, code) or None
    if auth:
        return f"{auth[0]}:{auth[1]}"
    return crs.to_wkt()
