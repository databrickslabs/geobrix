"""TDD tests for build_exif_common_opts and its supporting helpers.

Tests are ordered to match the TDD implementation order:
  _select_subset → _lookup_sensor_width_mm → _aggregate_exif → build_exif_common_opts
"""

import pytest

# ---------------------------------------------------------------------------
# _select_subset
# ---------------------------------------------------------------------------


class TestSelectSubset:
    """_select_subset(seq, mode, limit) — three modes plus error path."""

    def _fn(self):
        from databricks.labs.gbx.ds.exif import _select_subset

        return _select_subset

    def test_all_returns_all(self):
        ss = self._fn()
        result = ss(list(range(100)), "all", 24)
        assert len(result) == 100
        assert result == list(range(100))

    def test_limit_returns_first_n(self):
        ss = self._fn()
        result = ss(list(range(100)), "limit", 24)
        assert len(result) == 24
        assert result == list(range(24))

    def test_limit_with_small_seq(self):
        ss = self._fn()
        result = ss(list(range(5)), "limit", 24)
        # seq shorter than limit — return all
        assert len(result) == 5

    def test_sample_returns_correct_count(self):
        ss = self._fn()
        result = ss(list(range(100)), "sample", 24)
        assert len(result) == 24

    def test_sample_spacing(self):
        """100 items, limit=24 → step=4 → [0,4,8,...,92]."""
        ss = self._fn()
        result = ss(list(range(100)), "sample", 24)
        # step = max(1, 100 // 24) = 4
        expected = list(range(100))[::4][:24]
        assert result == expected
        assert result[0] == 0
        assert result[-1] == 92

    def test_sample_returns_list(self):
        ss = self._fn()
        result = ss(tuple(range(10)), "sample", 3)
        assert isinstance(result, list)

    def test_invalid_mode_raises(self):
        ss = self._fn()
        with pytest.raises(ValueError, match="mode"):
            ss(list(range(10)), "random", 5)

    def test_all_empty_seq(self):
        ss = self._fn()
        assert ss([], "all", 24) == []

    def test_sample_step_1_when_short(self):
        """When seq shorter than limit, step=max(1,...)=1 → all items."""
        ss = self._fn()
        result = ss(list(range(3)), "sample", 24)
        assert len(result) == 3


# ---------------------------------------------------------------------------
# _lookup_sensor_width_mm
# ---------------------------------------------------------------------------


class TestLookupSensorWidthMm:
    """_lookup_sensor_width_mm(camera_model) — DB lookups, normalization."""

    def _fn(self):
        from databricks.labs.gbx.ds.exif import _lookup_sensor_width_mm

        return _lookup_sensor_width_mm

    def test_hero5_exact_title_case(self):
        lk = self._fn()
        assert lk("HERO5 Black") == pytest.approx(6.17)

    def test_hero5_normalized(self):
        """Case/space insensitive."""
        lk = self._fn()
        assert lk("  HERO5 BLACK  ") == pytest.approx(6.17)

    def test_hero5_lowercase(self):
        lk = self._fn()
        assert lk("hero5 black") == pytest.approx(6.17)

    def test_fc6310_leading_trailing_spaces(self):
        lk = self._fn()
        assert lk("  fc6310 ") == pytest.approx(13.2)

    def test_fc6310_uppercase(self):
        lk = self._fn()
        assert lk("FC6310") == pytest.approx(13.2)

    def test_unknown_model_returns_none(self):
        lk = self._fn()
        assert lk("UnknownCam X1000") is None

    def test_none_input_returns_none(self):
        lk = self._fn()
        assert lk(None) is None

    def test_empty_string_returns_none(self):
        lk = self._fn()
        assert lk("") is None

    def test_fc220_in_db(self):
        lk = self._fn()
        assert lk("fc220") == pytest.approx(6.17)

    def test_l1d_20c_in_db(self):
        lk = self._fn()
        assert lk("L1D-20C") == pytest.approx(13.2)


# ---------------------------------------------------------------------------
# _aggregate_exif
# ---------------------------------------------------------------------------


def _make_records(n, focal=2.92, make="DJI", model="FC220", w=4000, h=3000):
    return [
        {
            "camera_make": make,
            "camera_model": model,
            "focal_length_mm": focal,
            "image_width": w,
            "image_height": h,
        }
        for _ in range(n)
    ]


class TestAggregateExif:
    """_aggregate_exif(records, sensor_db=True) — all branch paths."""

    def _fn(self):
        from databricks.labs.gbx.ds.exif import _aggregate_exif

        return _aggregate_exif

    # ------------------------------------------------------------------
    # Case 1: uniform focal + uniform known model + sensor_db=True
    # ------------------------------------------------------------------

    def test_case1_opts_fully_resolved(self):
        """Uniform known model → opts has both focalLengthMm and sensorWidthMm."""
        agg = self._fn()
        records = _make_records(5, focal=2.92, model="FC220")
        opts, report = agg(records, sensor_db=True)
        assert opts == {"focalLengthMm": "2.92", "sensorWidthMm": "6.17"}

    def test_case1_focal_is_uniform(self):
        agg = self._fn()
        records = _make_records(5, focal=2.92, model="FC220")
        _, report = agg(records, sensor_db=True)
        f = report["fields"]["focal_length_mm"]
        assert f["is_uniform"] is True
        assert f["value"] == pytest.approx(2.92)
        assert f["present_frac"] == pytest.approx(1.0)

    def test_case1_model_is_uniform(self):
        agg = self._fn()
        records = _make_records(5, focal=2.92, model="FC220")
        _, report = agg(records, sensor_db=True)
        m = report["fields"]["camera_model"]
        assert m["is_uniform"] is True
        assert m["value"] == "FC220"

    def test_case1_sensor_resolved(self):
        agg = self._fn()
        records = _make_records(5, focal=2.92, model="FC220")
        _, report = agg(records, sensor_db=True)
        sw = report["sensor_width_mm"]
        assert sw["resolved"] is True
        assert sw["value"] == pytest.approx(6.17)
        assert sw["source"] == "camera_db"

    def test_case1_needs_supply_empty(self):
        agg = self._fn()
        records = _make_records(5, focal=2.92, model="FC220")
        _, report = agg(records, sensor_db=True)
        assert report["needs_supply"] == []

    # ------------------------------------------------------------------
    # Case 2: uniform focal + uniform UNKNOWN model
    # ------------------------------------------------------------------

    def test_case2_opts_has_focal_only(self):
        """Unknown model → only focalLengthMm in opts, no sensorWidthMm."""
        agg = self._fn()
        records = _make_records(5, focal=2.92, model="UnknownCam X9")
        opts, _ = agg(records, sensor_db=True)
        assert "focalLengthMm" in opts
        assert "sensorWidthMm" not in opts

    def test_case2_sensor_not_resolved(self):
        agg = self._fn()
        records = _make_records(5, focal=2.92, model="UnknownCam X9")
        _, report = agg(records, sensor_db=True)
        sw = report["sensor_width_mm"]
        assert sw["resolved"] is False
        assert sw["source"] == "none"

    def test_case2_needs_supply_has_sensor(self):
        agg = self._fn()
        records = _make_records(5, focal=2.92, model="UnknownCam X9")
        _, report = agg(records, sensor_db=True)
        assert "sensor_width_mm" in report["needs_supply"]

    # ------------------------------------------------------------------
    # Case 3: mixed camera_model
    # ------------------------------------------------------------------

    def test_case3_no_sensor_width_in_opts(self):
        """Mixed model → cannot auto-resolve sensor width."""
        agg = self._fn()
        records = _make_records(3, model="FC220") + _make_records(3, model="FC330")
        opts, _ = agg(records, sensor_db=True)
        assert "sensorWidthMm" not in opts

    def test_case3_model_not_uniform(self):
        agg = self._fn()
        records = _make_records(3, model="FC220") + _make_records(3, model="FC330")
        _, report = agg(records, sensor_db=True)
        m = report["fields"]["camera_model"]
        assert m["is_uniform"] is False
        assert len(m["distinct"]) == 2

    def test_case3_needs_supply_notes_mixed_model(self):
        """needs_supply should contain a note about the mixed model situation."""
        agg = self._fn()
        records = _make_records(3, model="FC220") + _make_records(3, model="FC330")
        _, report = agg(records, sensor_db=True)
        # needs_supply must contain at least one entry (about sensor_width_mm)
        ns = report["needs_supply"]
        assert len(ns) >= 1
        # At least one entry mentions sensor_width_mm
        assert any("sensor_width_mm" in s for s in ns)
        # At least one entry mentions the mixed / not uniform model
        assert any("model" in s or "uniform" in s or "mixed" in s for s in ns)

    # ------------------------------------------------------------------
    # Case 4: mixed focal
    # ------------------------------------------------------------------

    def test_case4_no_focal_in_opts(self):
        """Mixed focal → focalLengthMm not pinned."""
        agg = self._fn()
        records = _make_records(3, focal=2.92) + _make_records(3, focal=3.0)
        opts, _ = agg(records, sensor_db=False)
        assert "focalLengthMm" not in opts

    def test_case4_focal_not_uniform(self):
        agg = self._fn()
        records = _make_records(3, focal=2.92) + _make_records(3, focal=3.0)
        _, report = agg(records, sensor_db=False)
        f = report["fields"]["focal_length_mm"]
        assert f["is_uniform"] is False
        assert len(f["distinct"]) == 2

    # ------------------------------------------------------------------
    # Case 5: sensor_db=False with known uniform model
    # ------------------------------------------------------------------

    def test_case5_no_sensor_width_when_db_disabled(self):
        """Even a known model yields nothing when sensor_db=False."""
        agg = self._fn()
        records = _make_records(5, focal=2.92, model="FC220")
        opts, _ = agg(records, sensor_db=False)
        assert "sensorWidthMm" not in opts

    def test_case5_not_resolved(self):
        agg = self._fn()
        records = _make_records(5, focal=2.92, model="FC220")
        _, report = agg(records, sensor_db=False)
        sw = report["sensor_width_mm"]
        assert sw["resolved"] is False

    def test_case5_needs_supply_has_sensor(self):
        agg = self._fn()
        records = _make_records(5, focal=2.92, model="FC220")
        _, report = agg(records, sensor_db=False)
        assert "sensor_width_mm" in report["needs_supply"]

    # ------------------------------------------------------------------
    # Case 6: some records missing focal
    # ------------------------------------------------------------------

    def test_case6_focal_not_pinned_when_some_missing(self):
        """present_frac < 1 → is_uniform False → focal not in opts."""
        agg = self._fn()
        records = [
            {
                "camera_make": "DJI",
                "camera_model": "FC220",
                "focal_length_mm": 2.92,
                "image_width": 4000,
                "image_height": 3000,
            },
            {
                "camera_make": "DJI",
                "camera_model": "FC220",
                "focal_length_mm": None,
                "image_width": 4000,
                "image_height": 3000,
            },
            {
                "camera_make": "DJI",
                "camera_model": "FC220",
                "focal_length_mm": 2.92,
                "image_width": 4000,
                "image_height": 3000,
            },
        ]
        opts, report = agg(records, sensor_db=True)
        f = report["fields"]["focal_length_mm"]
        assert f["is_uniform"] is False
        assert f["present_frac"] == pytest.approx(2 / 3)
        assert "focalLengthMm" not in opts

    # ------------------------------------------------------------------
    # Edge cases
    # ------------------------------------------------------------------

    def test_empty_records(self):
        agg = self._fn()
        opts, report = agg([], sensor_db=True)
        assert opts == {}
        assert report["fields"]["focal_length_mm"]["present_frac"] == 0.0
        assert report["fields"]["focal_length_mm"]["is_uniform"] is False

    def test_fields_structure(self):
        """All five expected field keys are present."""
        agg = self._fn()
        records = _make_records(2)
        _, report = agg(records, sensor_db=False)
        for k in (
            "focal_length_mm",
            "camera_make",
            "camera_model",
            "image_width",
            "image_height",
        ):
            assert k in report["fields"], f"missing field: {k}"

    def test_sensor_width_info_keys(self):
        """sensor_width_mm block always has resolved/value/source/camera_model."""
        agg = self._fn()
        _, report = agg(_make_records(2), sensor_db=False)
        sw = report["sensor_width_mm"]
        for key in ("resolved", "value", "source", "camera_model"):
            assert key in sw, f"missing key in sensor_width_mm: {key}"

    def test_hero5_known_model_resolved(self):
        """HERO5 Black → sensorWidthMm = 6.17 via DB."""
        agg = self._fn()
        records = _make_records(3, model="HERO5 Black")
        opts, _ = agg(records, sensor_db=True)
        assert opts.get("sensorWidthMm") == "6.17"

    def test_fc6310_large_sensor(self):
        """FC6310 (1-inch) → sensorWidthMm = 13.2."""
        agg = self._fn()
        records = _make_records(3, model="FC6310")
        opts, _ = agg(records, sensor_db=True)
        assert opts.get("sensorWidthMm") == "13.2"


# ---------------------------------------------------------------------------
# build_exif_common_opts — wiring test (no real EXIF files needed)
# ---------------------------------------------------------------------------


class TestBuildExifCommonOptsWiring:
    """Monkeypatched wiring test: no JPEG synthesis required."""

    def test_wiring_flow(self, tmp_path, monkeypatch):
        """list_local_files + _parse_exif_record are monkeypatched; full (opts,report) flow."""
        import databricks.labs.gbx.ds.exif as exif_mod

        fake_paths = [str(tmp_path / f"img{i}.jpg") for i in range(3)]
        canned = {
            "camera_make": "DJI",
            "camera_model": "FC220",
            "focal_length_mm": 2.92,
            "image_width": 4000,
            "image_height": 3000,
        }

        monkeypatch.setattr(
            exif_mod, "list_local_files", lambda path, extensions=None: fake_paths
        )
        monkeypatch.setattr(exif_mod, "_parse_exif_record", lambda fp: dict(canned))

        opts, report = exif_mod.build_exif_common_opts(str(tmp_path), mode="all")

        # Top-level report keys
        assert report["n_files"] == 3
        assert report["n_inspected"] == 3
        assert report["mode"] == "all"

        # Opts: uniform focal + FC220 in DB → both keys present
        assert opts.get("focalLengthMm") == "2.92"
        assert opts.get("sensorWidthMm") == "6.17"

        # needs_supply should be empty
        assert report["needs_supply"] == []

    def test_wiring_n_inspected_respects_mode(self, tmp_path, monkeypatch):
        """sample mode with limit=2 on 10 files → n_inspected=2."""
        import databricks.labs.gbx.ds.exif as exif_mod

        fake_paths = [str(tmp_path / f"img{i}.jpg") for i in range(10)]
        canned = {
            "camera_make": "GoPro",
            "camera_model": "HERO5 Black",
            "focal_length_mm": 2.92,
            "image_width": 4000,
            "image_height": 3000,
        }
        monkeypatch.setattr(
            exif_mod, "list_local_files", lambda path, extensions=None: fake_paths
        )
        monkeypatch.setattr(exif_mod, "_parse_exif_record", lambda fp: dict(canned))

        opts, report = exif_mod.build_exif_common_opts(
            str(tmp_path), mode="sample", limit=2
        )
        assert report["n_files"] == 10
        assert report["n_inspected"] == 2
        assert report["limit"] == 2

    def test_wiring_empty_dir(self, tmp_path, monkeypatch):
        """Empty collection → empty opts, needs_supply has sensor_width_mm note."""
        import databricks.labs.gbx.ds.exif as exif_mod

        monkeypatch.setattr(
            exif_mod,
            "list_local_files",
            lambda path, extensions=None: (_ for _ in ()).throw(
                FileNotFoundError("no files")
            ),
        )

        opts, report = exif_mod.build_exif_common_opts(str(tmp_path))
        assert opts == {}
        assert report["n_files"] == 0
        assert report["n_inspected"] == 0
        assert len(report["needs_supply"]) >= 1


# ---------------------------------------------------------------------------
# Helpers shared by the new test classes
# ---------------------------------------------------------------------------


def _fake_ifd_tag(values, str_val=None):
    """Minimal exifread IfdTag stand-in."""

    class _Tag:
        def __init__(self, v, s):
            self.values = v
            self._s = (
                s
                if s is not None
                else (str(v[0]) if isinstance(v, list) and v else str(v))
            )

        def __str__(self):
            return self._s

        def __int__(self):
            v = self.values[0] if isinstance(self.values, list) else self.values
            return int(v)

    return _Tag(values, str_val)


def _fake_ratio(val):
    """Minimal exifread Ratio stand-in: supports float()."""

    class _Ratio:
        def __init__(self, v):
            self._v = v

        def __float__(self):
            return float(self._v)

    return _Ratio(val)


def _dd_to_dms_ratios(dd):
    """Convert decimal degrees to [deg, min, sec] _Ratio list."""
    dd = abs(dd)
    d = int(dd)
    m = int((dd - d) * 60)
    s = ((dd - d) * 60 - m) * 60
    return [_fake_ratio(d), _fake_ratio(m), _fake_ratio(s)]


def _build_fake_tags(lat=48.123, lon=11.456, alt=150.0, ts="2024:06:15 10:30:00"):
    """Return a minimal exifread tags dict with GPS + timestamp + intrinsics."""
    tags = {}
    tags["Image Make"] = _fake_ifd_tag([], "DJI")
    tags["Image Model"] = _fake_ifd_tag([], "FC220")
    tags["EXIF FocalLength"] = _fake_ifd_tag([_fake_ratio(2.92)])
    if lat is not None:
        tags["GPS GPSLatitude"] = _fake_ifd_tag(_dd_to_dms_ratios(lat))
        tags["GPS GPSLatitudeRef"] = _fake_ifd_tag([], "N" if lat >= 0 else "S")
    if lon is not None:
        tags["GPS GPSLongitude"] = _fake_ifd_tag(_dd_to_dms_ratios(lon))
        tags["GPS GPSLongitudeRef"] = _fake_ifd_tag([], "E" if lon >= 0 else "W")
    if alt is not None:
        tags["GPS GPSAltitude"] = _fake_ifd_tag([_fake_ratio(alt)])
    if ts is not None:
        tags["EXIF DateTimeOriginal"] = _fake_ifd_tag([], ts)
    return tags


# ---------------------------------------------------------------------------
# _parse_exif_record — GPS/timestamp keys (Change 1)
# ---------------------------------------------------------------------------


class TestParseExifRecordGps:
    """_parse_exif_record now returns latitude/longitude/altitude/timestamp."""

    def _patch(self, monkeypatch, tmp_path, tags):
        """Write a dummy file, patch exifread.process_file + _image_dimensions."""
        import exifread

        import databricks.labs.gbx.ds.exif as exif_mod

        fp = str(tmp_path / "img.jpg")
        with open(fp, "wb") as fh:
            fh.write(b"fake")

        monkeypatch.setattr(exifread, "process_file", lambda fh, **kw: tags)
        monkeypatch.setattr(exif_mod, "_image_dimensions", lambda path, t: (4000, 3000))
        return fp, exif_mod

    def test_latitude_extracted(self, tmp_path, monkeypatch):
        tags = _build_fake_tags(lat=48.123, lon=11.456, alt=150.0)
        fp, exif_mod = self._patch(monkeypatch, tmp_path, tags)
        rec = exif_mod._parse_exif_record(fp)
        assert rec is not None
        assert "latitude" in rec
        assert rec["latitude"] == pytest.approx(48.123, abs=1e-3)

    def test_longitude_extracted(self, tmp_path, monkeypatch):
        tags = _build_fake_tags(lat=48.123, lon=11.456, alt=150.0)
        fp, exif_mod = self._patch(monkeypatch, tmp_path, tags)
        rec = exif_mod._parse_exif_record(fp)
        assert rec is not None
        assert "longitude" in rec
        assert rec["longitude"] == pytest.approx(11.456, abs=1e-3)

    def test_altitude_extracted(self, tmp_path, monkeypatch):
        tags = _build_fake_tags(lat=48.0, lon=11.0, alt=200.5)
        fp, exif_mod = self._patch(monkeypatch, tmp_path, tags)
        rec = exif_mod._parse_exif_record(fp)
        assert rec is not None
        assert "altitude" in rec
        assert rec["altitude"] == pytest.approx(200.5)

    def test_timestamp_extracted(self, tmp_path, monkeypatch):
        import datetime

        tags = _build_fake_tags(ts="2024:06:15 10:30:00")
        fp, exif_mod = self._patch(monkeypatch, tmp_path, tags)
        rec = exif_mod._parse_exif_record(fp)
        assert rec is not None
        assert "timestamp" in rec
        assert rec["timestamp"] == datetime.datetime(2024, 6, 15, 10, 30, 0)

    def test_missing_gps_yields_none_values(self, tmp_path, monkeypatch):
        """Tags without GPS → lat/lon/altitude/timestamp are None in record."""
        tags = {}  # no GPS, no timestamp, no make/model/focal
        fp, exif_mod = self._patch(monkeypatch, tmp_path, tags)
        rec = exif_mod._parse_exif_record(fp)
        assert rec is not None
        assert rec["latitude"] is None
        assert rec["longitude"] is None
        assert rec["altitude"] is None
        assert rec["timestamp"] is None

    def test_southern_hemisphere_negative_lat(self, tmp_path, monkeypatch):
        """S hemisphere → negative latitude."""
        import exifread

        import databricks.labs.gbx.ds.exif as exif_mod

        lat = -33.865
        tags = _build_fake_tags(lat=lat, lon=151.0, alt=10.0)
        # Override the LatitudeRef to S
        tags["GPS GPSLatitudeRef"] = _fake_ifd_tag([], "S")

        fp = str(tmp_path / "img.jpg")
        with open(fp, "wb") as fh:
            fh.write(b"fake")
        monkeypatch.setattr(exifread, "process_file", lambda fh, **kw: tags)
        monkeypatch.setattr(exif_mod, "_image_dimensions", lambda path, t: (4000, 3000))

        rec = exif_mod._parse_exif_record(fp)
        assert rec is not None
        assert rec["latitude"] is not None
        assert rec["latitude"] < 0


# ---------------------------------------------------------------------------
# _aggregate_exif — classification buckets (Change 2)
# ---------------------------------------------------------------------------


def _make_records_full(
    n,
    focal=2.92,
    make="DJI",
    model="FC220",
    w=4000,
    h=3000,
    lat=48.1,
    lon=11.4,
    alt=150.0,
    ts=None,
):
    """Records with all nine profiling fields populated."""
    import datetime

    ts_val = ts if ts is not None else datetime.datetime(2024, 6, 15, 10, 30, 0)
    return [
        {
            "camera_make": make,
            "camera_model": model,
            "focal_length_mm": focal,
            "image_width": w,
            "image_height": h,
            "latitude": lat,
            "longitude": lon,
            "altitude": alt,
            "timestamp": ts_val,
        }
        for _ in range(n)
    ]


class TestAggregateExifClassification:
    """_aggregate_exif classification: common / variable / missing buckets."""

    def _fn(self):
        from databricks.labs.gbx.ds.exif import _aggregate_exif

        return _aggregate_exif

    def _mixed_gps_records(self):
        """4 records: uniform focal+model(unknown), varying lat/lon, altitude on 2/4."""
        import datetime

        records = []
        lats = [48.1, 48.2, 48.3, 48.4]
        lons = [11.1, 11.2, 11.3, 11.4]
        alts = [100.0, None, 200.0, None]
        for i in range(4):
            records.append(
                {
                    "camera_make": "DJI",
                    "camera_model": "UnknownCamXYZ",
                    "focal_length_mm": 2.92,
                    "image_width": 4000,
                    "image_height": 3000,
                    "latitude": lats[i],
                    "longitude": lons[i],
                    "altitude": alts[i],
                    "timestamp": datetime.datetime(2024, 6, 15, 10, 30, i),
                }
            )
        return records

    def test_classification_keys_present(self):
        agg = self._fn()
        _, report = agg(self._mixed_gps_records(), sensor_db=True)
        assert "classification" in report
        assert "common" in report["classification"]
        assert "variable" in report["classification"]
        assert "missing" in report["classification"]

    def test_uniform_focal_in_common(self):
        agg = self._fn()
        _, report = agg(self._mixed_gps_records(), sensor_db=True)
        common = report["classification"]["common"]
        assert "focal_length_mm" in common
        assert common["focal_length_mm"] == pytest.approx(2.92)

    def test_varying_lat_lon_in_variable(self):
        agg = self._fn()
        _, report = agg(self._mixed_gps_records(), sensor_db=True)
        variable = report["classification"]["variable"]
        assert "latitude" in variable
        assert "longitude" in variable
        assert variable["latitude"]["n_distinct"] == 4
        assert variable["longitude"]["n_distinct"] == 4

    def test_varying_lat_lon_examples_populated(self):
        agg = self._fn()
        _, report = agg(self._mixed_gps_records(), sensor_db=True)
        variable = report["classification"]["variable"]
        assert len(variable["latitude"]["examples"]) == 4

    def test_partial_altitude_in_missing_with_frac(self):
        agg = self._fn()
        _, report = agg(self._mixed_gps_records(), sensor_db=True)
        missing = report["classification"]["missing"]
        assert "altitude" in missing
        assert missing["altitude"] == pytest.approx(0.5)

    def test_unresolved_sensor_in_missing(self):
        """Unknown model → sensor_width_mm appended to missing with value 0.0."""
        agg = self._fn()
        _, report = agg(self._mixed_gps_records(), sensor_db=True)
        missing = report["classification"]["missing"]
        assert "sensor_width_mm" in missing
        assert missing["sensor_width_mm"] == 0.0

    def test_resolved_sensor_not_in_missing(self):
        """Known model (FC220) + sensor_db=True → sensor_width_mm absent from missing."""
        agg = self._fn()
        records = _make_records_full(4, model="FC220")
        _, report = agg(records, sensor_db=True)
        missing = report["classification"]["missing"]
        assert "sensor_width_mm" not in missing

    def test_uniform_field_not_in_variable(self):
        agg = self._fn()
        records = _make_records_full(4, model="FC220")
        _, report = agg(records, sensor_db=True)
        variable = report["classification"]["variable"]
        # focal_length_mm is uniform → must NOT be in variable
        assert "focal_length_mm" not in variable


# ---------------------------------------------------------------------------
# _aggregate_exif — suggestions (Change 2)
# ---------------------------------------------------------------------------


class TestAggregateExifSuggestions:
    """suggestions: actionable lines for gaps and intrinsic concerns."""

    def _fn(self):
        from databricks.labs.gbx.ds.exif import _aggregate_exif

        return _aggregate_exif

    def _all_gps_present(self, n=4):
        """Full GPS present and varying, unknown model → sensor suggestion."""
        import datetime

        return [
            {
                "camera_make": "DJI",
                "camera_model": "UnknownCamABC",
                "focal_length_mm": 2.92,
                "image_width": 4000,
                "image_height": 3000,
                "latitude": 48.0 + i * 0.01,
                "longitude": 11.0 + i * 0.01,
                "altitude": 100.0 + i * 10,
                "timestamp": datetime.datetime(2024, 6, 15, 10, 30, i),
            }
            for i in range(n)
        ]

    def test_suggestions_key_present(self):
        agg = self._fn()
        _, report = agg(self._all_gps_present(), sensor_db=True)
        assert "suggestions" in report
        assert isinstance(report["suggestions"], list)

    def test_unknown_sensor_suggestion_present(self):
        """Unknown uniform model → sensor_width_mm suggestion present."""
        agg = self._fn()
        _, report = agg(self._all_gps_present(), sensor_db=True)
        sug = report["suggestions"]
        assert any("sensorWidthMm" in s or "sensor_width_mm" in s for s in sug)

    def test_no_suggestion_for_varying_lat_lon(self):
        """lat/lon fully present but varying → no georef suggestion for them."""
        agg = self._fn()
        _, report = agg(self._all_gps_present(), sensor_db=True)
        sug = report["suggestions"]
        assert not any("latitude present on" in s for s in sug)
        assert not any("longitude present on" in s for s in sug)

    def test_altitude_partial_suggestion(self):
        """altitude absent from half of images → altitude suggestion emitted."""
        import datetime

        agg = self._fn()
        records = [
            {
                "camera_make": "DJI",
                "camera_model": "FC220",
                "focal_length_mm": 2.92,
                "image_width": 4000,
                "image_height": 3000,
                "latitude": 48.0 + i * 0.01,
                "longitude": 11.0 + i * 0.01,
                "altitude": 100.0 if i < 2 else None,
                "timestamp": datetime.datetime(2024, 6, 15, 10, 30, i),
            }
            for i in range(4)
        ]
        _, report = agg(records, sensor_db=True)
        sug = report["suggestions"]
        assert any("altitude" in s for s in sug)

    def test_sensor_suggestion_multi_model(self):
        """Mixed models → suggestion mentions 'multiple camera models'."""
        import datetime

        agg = self._fn()
        records = []
        for i in range(4):
            records.append(
                {
                    "camera_make": "DJI",
                    "camera_model": "FC220" if i < 2 else "FC330",
                    "focal_length_mm": 2.92,
                    "image_width": 4000,
                    "image_height": 3000,
                    "latitude": 48.0 + i * 0.01,
                    "longitude": 11.0 + i * 0.01,
                    "altitude": 100.0,
                    "timestamp": datetime.datetime(2024, 6, 15, 10, 30, i),
                }
            )
        _, report = agg(records, sensor_db=True)
        sug = report["suggestions"]
        assert any("multiple camera models" in s for s in sug)

    def test_no_suggestions_for_fully_resolved_uniform_collection(self):
        """Known model, uniform focal, all GPS present → no suggestions at all."""
        agg = self._fn()
        # All images identical GPS too — would be in 'common'; no issues
        records = _make_records_full(4, model="FC220", lat=48.1, lon=11.4, alt=150.0)
        # Timestamps also uniform (same ts)
        _, report = agg(records, sensor_db=True)
        sug = report["suggestions"]
        assert sug == []


# ---------------------------------------------------------------------------
# eval_exif_opts (Change 3)
# ---------------------------------------------------------------------------


class TestEvalExifOpts:
    """eval_exif_opts formats build_exif_common_opts output as a human-readable string."""

    def _canned_report(self):
        """Minimal plausible report for monkeypatching build_exif_common_opts."""
        import datetime

        return {
            "n_files": 5,
            "n_inspected": 3,
            "mode": "sample",
            "limit": 24,
            "fields": {
                "focal_length_mm": {
                    "present_frac": 1.0,
                    "distinct": [2.92],
                    "is_uniform": True,
                    "value": 2.92,
                },
                "camera_make": {
                    "present_frac": 1.0,
                    "distinct": ["DJI"],
                    "is_uniform": True,
                    "value": "DJI",
                },
                "camera_model": {
                    "present_frac": 1.0,
                    "distinct": ["FC220"],
                    "is_uniform": True,
                    "value": "FC220",
                },
                "image_width": {
                    "present_frac": 1.0,
                    "distinct": [4000],
                    "is_uniform": True,
                    "value": 4000,
                },
                "image_height": {
                    "present_frac": 1.0,
                    "distinct": [3000],
                    "is_uniform": True,
                    "value": 3000,
                },
                "latitude": {
                    "present_frac": 1.0,
                    "distinct": [48.1, 48.2, 48.3],
                    "is_uniform": False,
                    "value": None,
                },
                "longitude": {
                    "present_frac": 1.0,
                    "distinct": [11.1, 11.2, 11.3],
                    "is_uniform": False,
                    "value": None,
                },
                "altitude": {
                    "present_frac": 0.5,
                    "distinct": [100.0, 200.0],
                    "is_uniform": False,
                    "value": None,
                },
                "timestamp": {
                    "present_frac": 1.0,
                    "distinct": [
                        datetime.datetime(2024, 6, 15, 10, 30, i) for i in range(3)
                    ],
                    "is_uniform": False,
                    "value": None,
                },
            },
            "sensor_width_mm": {
                "resolved": True,
                "value": 6.17,
                "source": "camera_db",
                "camera_model": "FC220",
            },
            "needs_supply": [],
            "classification": {
                "common": {
                    "focal_length_mm": 2.92,
                    "camera_make": "DJI",
                    "camera_model": "FC220",
                    "image_width": 4000,
                    "image_height": 3000,
                },
                "variable": {
                    "latitude": {"n_distinct": 3, "examples": [48.1, 48.2, 48.3]},
                    "longitude": {"n_distinct": 3, "examples": [11.1, 11.2, 11.3]},
                },
                "missing": {"altitude": 0.5},
            },
            "suggestions": [
                "altitude present on 50% of images — missing altitude degrades "
                "Sim(3) scale/height; supply altitudes or use a DEM"
            ],
        }

    def test_returns_str(self, tmp_path, monkeypatch):
        import databricks.labs.gbx.ds.exif as exif_mod

        canned_opts = {"focalLengthMm": "2.92", "sensorWidthMm": "6.17"}
        monkeypatch.setattr(
            exif_mod,
            "build_exif_common_opts",
            lambda path, **kw: (canned_opts, self._canned_report()),
        )
        result = exif_mod.eval_exif_opts(str(tmp_path))
        assert isinstance(result, str)

    def test_contains_all_section_headers(self, tmp_path, monkeypatch):
        import databricks.labs.gbx.ds.exif as exif_mod

        canned_opts = {"focalLengthMm": "2.92", "sensorWidthMm": "6.17"}
        monkeypatch.setattr(
            exif_mod,
            "build_exif_common_opts",
            lambda path, **kw: (canned_opts, self._canned_report()),
        )
        result = exif_mod.eval_exif_opts(str(tmp_path))
        for header in ("COMMON", "PRESENT-BUT-VARIABLE", "MISSING", "SUGGESTIONS"):
            assert header in result, f"section header {header!r} missing from output"

    def test_contains_inspected_header(self, tmp_path, monkeypatch):
        import databricks.labs.gbx.ds.exif as exif_mod

        canned_opts = {}
        monkeypatch.setattr(
            exif_mod,
            "build_exif_common_opts",
            lambda path, **kw: (canned_opts, self._canned_report()),
        )
        result = exif_mod.eval_exif_opts(str(tmp_path))
        assert "3 of 5 files inspected" in result

    def test_common_field_appears_in_output(self, tmp_path, monkeypatch):
        import databricks.labs.gbx.ds.exif as exif_mod

        canned_opts = {"focalLengthMm": "2.92", "sensorWidthMm": "6.17"}
        monkeypatch.setattr(
            exif_mod,
            "build_exif_common_opts",
            lambda path, **kw: (canned_opts, self._canned_report()),
        )
        result = exif_mod.eval_exif_opts(str(tmp_path))
        # At least one intrinsic field appears in the output
        assert "focal_length_mm" in result or "camera_model" in result

    def test_opts_section_present(self, tmp_path, monkeypatch):
        import databricks.labs.gbx.ds.exif as exif_mod

        canned_opts = {"focalLengthMm": "2.92", "sensorWidthMm": "6.17"}
        monkeypatch.setattr(
            exif_mod,
            "build_exif_common_opts",
            lambda path, **kw: (canned_opts, self._canned_report()),
        )
        result = exif_mod.eval_exif_opts(str(tmp_path))
        assert "OPTS" in result

    def test_does_not_print(self, tmp_path, monkeypatch, capsys):
        """eval_exif_opts must return the string, not print it."""
        import databricks.labs.gbx.ds.exif as exif_mod

        canned_opts = {}
        monkeypatch.setattr(
            exif_mod,
            "build_exif_common_opts",
            lambda path, **kw: (canned_opts, self._canned_report()),
        )
        exif_mod.eval_exif_opts(str(tmp_path))
        captured = capsys.readouterr()
        assert captured.out == ""
