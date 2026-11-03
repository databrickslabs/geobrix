"""NetCDF raster reader: the grid-variable filter of ``netcdf_gbx`` on the shared gridded fixture.

(This file held the heavy ``netcdf_gdal`` vs light ``netcdf_gbx`` parity gate; the Scala reader is retired, so the
reference set it anchored is the light reader's own contract now.)
"""

from pathlib import Path

import pytest

# Gridded fixture in the repo test resources. The coral granule is the canonical shared fixture: its data_vars are
# {bleaching_alert_area, mask}.
_HERE = Path(__file__).resolve()
_REPO = _HERE.parents[4]  # .../geobrix
_CORAL = (
    _REPO / "src/test/resources/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220101.nc"
)


def test_light_enumerates_coral_grid_variables():
    """Light ``readable_variables`` on the coral fixture is exactly the two CF grid variables."""
    if not _CORAL.exists():
        pytest.skip(f"coral fixture not present at {_CORAL}")
    from databricks.labs.gbx.ds import _netcdf

    with _netcdf.open_dataset(str(_CORAL), None) as ds:
        light_vars = sorted(_netcdf.readable_variables(ds, "raster"))
    assert light_vars == ["bleaching_alert_area", "mask"]
