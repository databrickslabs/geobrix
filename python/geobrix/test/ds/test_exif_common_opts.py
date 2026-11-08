"""EXIF profiling of a collection: ``build_exif_common_opts`` and ``eval_exif_opts``.

The public path is the table: canned per-image records go in through the file parser's seam, and the
(opts, report) a user gets out is checked row by row. The subset choice, the sensor-width lookup and the
aggregation are reached only through it. Pure Python, no Spark, no real JPEGs.
"""

import datetime

import pytest

import databricks.labs.gbx.ds.exif as exif_mod

_TS = datetime.datetime(2024, 6, 15, 10, 30, 0)


def rec(**over):
    """One image's record: every profiling field populated, a uniform known camera."""
    base = dict(
        camera_make="DJI", camera_model="FC220", focal_length_mm=2.92, image_width=4000, image_height=3000,
        latitude=48.1, longitude=11.4, altitude=150.0, timestamp=_TS,
    )  # fmt: skip
    return {**base, **over}


def profile(monkeypatch, tmp_path, records, **kw):
    """Run build_exif_common_opts over `records` (one image each); also return which images were parsed."""
    paths = [str(tmp_path / f"img{i:03d}.jpg") for i in range(len(records))]
    by_path = dict(zip(paths, records))
    parsed = []

    def parse(fp):
        parsed.append(fp)
        return dict(by_path[fp]) if by_path[fp] is not None else None

    monkeypatch.setattr(
        exif_mod, "list_local_files", lambda path, extensions=None: paths
    )
    monkeypatch.setattr(exif_mod, "_parse_exif_record", parse)
    opts, report = exif_mod.build_exif_common_opts(str(tmp_path), **kw)
    return opts, report, [paths.index(p) for p in parsed]


def same(n, **over):
    return [rec(**over) for _ in range(n)]


# name -> (records, build kwargs, opts, {report path: expected})
CASES = {
    "uniform_known_camera": (
        same(5), {"sensor_db": True}, {"focalLengthMm": "2.92", "sensorWidthMm": "6.17"},
        {"fields.focal_length_mm.is_uniform": True, "fields.focal_length_mm.value": 2.92,
         "fields.focal_length_mm.present_frac": 1.0, "fields.camera_model.value": "FC220",
         "sensor_width_mm.resolved": True, "sensor_width_mm.value": 6.17, "sensor_width_mm.source": "camera_db",
         "needs_supply": []},
    ),
    "uniform_unknown_camera": (
        same(5, camera_model="UnknownCam X9"), {}, {"focalLengthMm": "2.92"},
        {"sensor_width_mm.resolved": False, "sensor_width_mm.source": "none", "needs_supply": ["sensor_width_mm"]},
    ),
    "sensor_db_off": (
        same(5), {"sensor_db": False}, {"focalLengthMm": "2.92"},
        {"sensor_width_mm.resolved": False, "needs_supply": ["sensor_width_mm"]},
    ),
    "mixed_models": (
        same(3) + same(3, camera_model="FC330"), {}, {"focalLengthMm": "2.92"},
        {"fields.camera_model.is_uniform": False, "sensor_width_mm.resolved": False},
    ),
    "mixed_focal": (
        same(3) + same(3, focal_length_mm=3.0), {"sensor_db": False}, {},
        {"fields.focal_length_mm.is_uniform": False},
    ),
    "focal_missing_on_one_image": (
        [rec(), rec(focal_length_mm=None), rec()], {}, {"sensorWidthMm": "6.17"},
        {"fields.focal_length_mm.is_uniform": False, "fields.focal_length_mm.present_frac": pytest.approx(2 / 3)},
    ),
    "no_images": (
        [], {}, {},
        {"n_files": 0, "n_inspected": 0, "fields.focal_length_mm.present_frac": 0.0, "fields.focal_length_mm.is_uniform": False},
    ),
    # the camera database: case and space insensitive
    **{
        f"sensor_{i}_{model!r}": (same(3, camera_model=model), {}, opts, {})
        for i, (model, opts) in enumerate([
            ("HERO5 Black", {"focalLengthMm": "2.92", "sensorWidthMm": "6.17"}),
            ("  HERO5 BLACK  ", {"focalLengthMm": "2.92", "sensorWidthMm": "6.17"}),
            ("hero5 black", {"focalLengthMm": "2.92", "sensorWidthMm": "6.17"}),
            ("  fc6310 ", {"focalLengthMm": "2.92", "sensorWidthMm": "13.2"}),
            ("FC6310", {"focalLengthMm": "2.92", "sensorWidthMm": "13.2"}),
            ("fc220", {"focalLengthMm": "2.92", "sensorWidthMm": "6.17"}),
            ("L1D-20C", {"focalLengthMm": "2.92", "sensorWidthMm": "13.2"}),
            ("UnknownCam X1000", {"focalLengthMm": "2.92"}),
            (None, {"focalLengthMm": "2.92"}),
            ("", {"focalLengthMm": "2.92"}),
        ])
    },
    # classification and suggestions (4 images, unknown uniform camera, GPS varying, altitude on half)
    "classification_buckets": (
        [rec(camera_model="UnknownCamXYZ", latitude=48.1 + i / 10, longitude=11.1 + i / 10,
             altitude=[100.0, None, 200.0, None][i], timestamp=_TS.replace(second=i)) for i in range(4)],
        {}, {"focalLengthMm": "2.92"},
        {"classification.common.focal_length_mm": pytest.approx(2.92),
         "classification.variable.latitude.n_distinct": 4, "classification.variable.longitude.n_distinct": 4,
         "classification.missing.altitude": pytest.approx(0.5), "classification.missing.sensor_width_mm": 0.0},
    ),
    "resolved_uniform_collection_has_no_gaps": (
        same(4), {}, {"focalLengthMm": "2.92", "sensorWidthMm": "6.17"},
        {"suggestions": [], "classification.missing": {}, "classification.variable": {}},
    ),
}  # fmt: skip


def at(report, dotted):
    for k in dotted.split("."):
        report = report[k]
    return report


@pytest.mark.parametrize(
    "records,kw,opts,report_rows", CASES.values(), ids=CASES.keys()
)
def test_profile_of_a_collection(monkeypatch, tmp_path, records, kw, opts, report_rows):
    got_opts, report, _ = profile(monkeypatch, tmp_path, records, **kw)
    assert got_opts == opts
    for path, want in report_rows.items():
        assert at(report, path) == want, path


def test_the_report_has_every_field_and_the_sensor_block(monkeypatch, tmp_path):
    _, report, _ = profile(monkeypatch, tmp_path, same(2), sensor_db=False)
    assert set(report["fields"]) == {"focal_length_mm", "camera_make", "camera_model", "image_width", "image_height", "latitude", "longitude", "altitude", "timestamp"}  # fmt: skip
    assert set(report["sensor_width_mm"]) == {
        "resolved",
        "value",
        "source",
        "camera_model",
    }
    assert {"classification", "suggestions", "needs_supply"} <= set(report)


def test_suggestions_name_the_gaps(monkeypatch, tmp_path):
    unknown = [rec(camera_model="UnknownCamABC", latitude=48 + i / 100, longitude=11 + i / 100,
                   altitude=100.0 + i * 10 if i >= 2 else None, timestamp=_TS.replace(second=i)) for i in range(4)]  # fmt: skip
    _, report, _ = profile(monkeypatch, tmp_path, unknown)
    sug = report["suggestions"]
    assert any("sensorWidthMm" in s for s in sug)
    assert any("altitude" in s for s in sug)
    assert not any(
        "latitude present on" in s or "longitude present on" in s for s in sug
    ), "varying GPS is not a gap"
    two_cameras = [
        rec(camera_model="FC220" if i < 2 else "FC330", timestamp=_TS.replace(second=i))
        for i in range(4)
    ]
    _, report, _ = profile(monkeypatch, tmp_path, two_cameras)
    assert any("multiple camera models" in s for s in report["suggestions"])


# mode, limit, n files -> the indexes inspected
SUBSET = {
    "all": ("all", 24, 100, list(range(100))),
    "limit_first_n": ("limit", 24, 100, list(range(24))),
    "limit_short_collection": ("limit", 24, 5, list(range(5))),
    "sample_evenly_spaced": ("sample", 24, 100, list(range(0, 100, 4))[:24]),
    "sample_short_collection": ("sample", 24, 3, [0, 1, 2]),
    "sample_of_ten_takes_three": ("sample", 3, 10, [0, 3, 6]),
}


@pytest.mark.parametrize("mode,limit,n,want", SUBSET.values(), ids=SUBSET.keys())
def test_the_images_inspected_follow_the_mode(
    monkeypatch, tmp_path, mode, limit, n, want
):
    _, report, parsed = profile(monkeypatch, tmp_path, same(n), mode=mode, limit=limit)
    assert parsed == want
    assert (
        report["n_files"],
        report["n_inspected"],
        report["mode"],
        report["limit"],
    ) == (n, len(want), mode, limit)


def test_an_unknown_mode_is_refused(monkeypatch, tmp_path):
    with pytest.raises(ValueError, match="mode"):
        profile(monkeypatch, tmp_path, same(10), mode="random")


def test_a_missing_folder_is_an_empty_collection(monkeypatch, tmp_path):
    def gone(path, extensions=None):
        raise FileNotFoundError("no files")

    monkeypatch.setattr(exif_mod, "list_local_files", gone)
    opts, report = exif_mod.build_exif_common_opts(str(tmp_path))
    assert opts == {} and (report["n_files"], report["n_inspected"]) == (0, 0)
    assert report["needs_supply"]


def test_eval_returns_the_text_with_every_section_and_prints_nothing(
    monkeypatch, tmp_path, capsys
):
    records = [rec(camera_model="UnknownCam", latitude=48.1 + i / 10, altitude=100.0 if i < 2 else None,
                   timestamp=_TS.replace(second=i)) for i in range(4)]  # fmt: skip
    monkeypatch.setattr(
        exif_mod,
        "list_local_files",
        lambda path, extensions=None: [f"{tmp_path}/{i}.jpg" for i in range(5)],
    )
    monkeypatch.setattr(
        exif_mod,
        "_parse_exif_record",
        lambda fp: dict(records[int(fp.rsplit("/", 1)[1][0]) % 4]),
    )
    text = exif_mod.eval_exif_opts(str(tmp_path), mode="limit", limit=3)
    assert capsys.readouterr().out == ""
    assert text.startswith("3 of 5 files inspected")
    for header in ("COMMON", "PRESENT-BUT-VARIABLE", "MISSING", "SUGGESTIONS", "OPTS"):
        assert header in text
    assert "focal_length_mm" in text


# ---------------------------------------------------------------------------
# The record read from one file: GPS and time
# ---------------------------------------------------------------------------


class _Tag:
    def __init__(self, values, text=None):
        self.values = values
        self._s = (
            text
            if text is not None
            else str(values[0] if isinstance(values, list) and values else values)
        )

    def __str__(self):
        return self._s


class _Ratio:
    def __init__(self, v):
        self._v = v

    def __float__(self):
        return float(self._v)


def _dms(dd):
    dd = abs(dd)
    d = int(dd)
    m = int((dd - d) * 60)
    return [_Ratio(d), _Ratio(m), _Ratio(((dd - d) * 60 - m) * 60)]


def _tags(lat=None, lon=None, alt=None, ts=None, lat_ref="N", lon_ref="E"):
    tags = {
        "Image Make": _Tag([], "DJI"),
        "Image Model": _Tag([], "FC220"),
        "EXIF FocalLength": _Tag([_Ratio(2.92)]),
    }
    if lat is not None:
        tags["GPS GPSLatitude"], tags["GPS GPSLatitudeRef"] = _Tag(_dms(lat)), _Tag(
            [], lat_ref
        )
    if lon is not None:
        tags["GPS GPSLongitude"], tags["GPS GPSLongitudeRef"] = _Tag(_dms(lon)), _Tag(
            [], lon_ref
        )
    if alt is not None:
        tags["GPS GPSAltitude"] = _Tag([_Ratio(alt)])
    if ts is not None:
        tags["EXIF DateTimeOriginal"] = _Tag([], ts)
    return tags


# name -> (tags, expected latitude, longitude, altitude, timestamp)
RECORDS = {
    "gps_and_time": (
        _tags(48.123, 11.456, 150.0, "2024:06:15 10:30:00"),
        48.123,
        11.456,
        150.0,
        _TS,
    ),
    "altitude_value": (_tags(48.0, 11.0, 200.5), 48.0, 11.0, 200.5, None),
    "southern_hemisphere": (
        _tags(33.865, 151.0, 10.0, lat_ref="S"),
        -33.865,
        151.0,
        10.0,
        None,
    ),
    "western_hemisphere": (_tags(10.0, 70.5, 1.0, lon_ref="W"), 10.0, -70.5, 1.0, None),
    "no_gps_no_time": ({}, None, None, None, None),
}


@pytest.mark.parametrize("tags,lat,lon,alt,ts", RECORDS.values(), ids=RECORDS.keys())
def test_the_record_read_from_a_file_has_gps_and_time(
    monkeypatch, tmp_path, tags, lat, lon, alt, ts
):
    import exifread

    fp = tmp_path / "img.jpg"
    fp.write_bytes(b"fake")
    monkeypatch.setattr(exifread, "process_file", lambda fh, **kw: tags)
    monkeypatch.setattr(exif_mod, "_image_dimensions", lambda path, t: (4000, 3000))
    got = exif_mod._parse_exif_record(str(fp))
    assert got is not None
    for name, want in (("latitude", lat), ("longitude", lon), ("altitude", alt)):
        assert got[name] == (
            pytest.approx(want, abs=1e-3) if want is not None else None
        ), name
    assert got["timestamp"] == ts
