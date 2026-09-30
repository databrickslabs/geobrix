from databricks.labs.gbx.core.crs import enu_to_lonlat, lonlat_to_enu, utm_epsg_for


def test_utm_epsg_for_northern():
    assert utm_epsg_for(-122.4, 37.8) == 32610  # San Francisco, zone 10N
    assert utm_epsg_for(0.0, 51.5) == 32631  # London, zone 31N


def test_utm_epsg_for_southern():
    assert utm_epsg_for(151.2, -33.9) == 32756  # Sydney, zone 56S


def test_utm_epsg_for_boundaries():
    assert utm_epsg_for(-180.0, 0.0) == 32601  # zone 1N
    assert utm_epsg_for(179.9, -10.0) == 32760  # zone 60S


def test_enu_roundtrip():
    lon, lat, ref_lat, ref_lon = -122.39, 37.81, 37.80, -122.40
    e, n = lonlat_to_enu(lon, lat, ref_lat, ref_lon)
    lon2, lat2 = enu_to_lonlat(e, n, ref_lat, ref_lon)
    assert abs(lon2 - lon) < 1e-9 and abs(lat2 - lat) < 1e-9


def test_enu_lat_scale():
    # 1 degree north ~ 111320 m at any reference latitude.
    _e, n = lonlat_to_enu(0.0, 1.0, 0.0, 0.0)
    assert abs(n - 111320.0) < 1.0


def test_enu_lon_cos_scale():
    # East distance for 1 deg lon shrinks by cos(ref_lat): at 60 deg it is ~half.
    e0, _ = lonlat_to_enu(1.0, 0.0, 0.0, 0.0)
    e60, _ = lonlat_to_enu(1.0, 60.0, 60.0, 0.0)
    assert abs(e60 / e0 - 0.5) < 0.01
