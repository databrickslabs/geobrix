import numpy as np
import shapely.wkb
from shapely.geometry import box

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import features


def test_rasterize_burns_value_inside_nodata_outside():
    # extent x:[0,4] y:[0,4], 4x4 px (1 unit/px). Box covers x:[1,3] y:[1,3].
    geom = shapely.wkb.dumps(box(1.0, 1.0, 3.0, 3.0))
    out = features.rasterize_geom(geom, 5.0, 0.0, 0.0, 4.0, 4.0, 4, 4, 4326)
    with _serde.open_tile(out) as ds:
        assert ds.crs.to_epsg() == 4326
        assert (ds.width, ds.height) == (4, 4)
        assert ds.nodata == -9999.0
        arr = ds.read(1)
        # center pixels burned to 5.0, corners remain nodata
        assert 5.0 in np.unique(arr)
        assert -9999.0 in np.unique(arr)
