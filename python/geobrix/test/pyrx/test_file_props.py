from databricks.labs.gbx.pyrx import file_props as fp


def test_build_props_managed_cluster():
    p = fp.build_props(
        file_mode="managed",
        layout="cluster",
        filespace="/Volumes/c/s/v",
        library_version="0.5.0",
    )
    assert p[fp.WRITER_VERSION_KEY] == fp.CURRENT_WRITER_VERSION == "1"
    assert p[fp.WRITE_STRATEGY_KEY] == "managed:cluster"
    assert p[fp.FILESPACE_KEY] == "/Volumes/c/s/v"
    assert p[fp.LIBRARY_VERSION_KEY] == "0.5.0"


def test_build_props_external_omits_filespace():
    p = fp.build_props(
        file_mode="external", layout="order", filespace=None, library_version="0.5.0"
    )
    assert fp.FILESPACE_KEY not in p
    assert p[fp.WRITE_STRATEGY_KEY] == "external:order"


def test_build_props_rejects_bad_enum():
    import pytest

    with pytest.raises(ValueError):
        fp.build_props(
            file_mode="bogus", layout="order", filespace=None, library_version="0.5.0"
        )


def test_parse_props_geobrix_and_foreign():
    parsed = fp.parse_props(
        {fp.WRITER_VERSION_KEY: "1", fp.WRITE_STRATEGY_KEY: "managed:cluster"}
    )
    assert parsed == {
        "writer_version": "1",
        "file_mode": "managed",
        "layout": "cluster",
        "is_geobrix": True,
    }
    foreign = fp.parse_props({"some.other": "x"})
    assert foreign["is_geobrix"] is False and foreign["file_mode"] is None
