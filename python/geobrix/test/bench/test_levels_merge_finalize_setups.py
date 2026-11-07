"""The merge aggregate's finalize forms time the level they are labelled with.

The JVM finalize and the light ones share the SQL name `gbx_rst_merge`; the JVM setup replaces the light function in the
session. A setup that is judged only by having run once left the JVM merge under the light forms that followed it: they
timed it, and a virtual tile raised VirtualTileException.
"""

from types import SimpleNamespace

import pytest

from databricks.labs.gbx.bench import levels
from databricks.labs.gbx.bench import levels_forms as lf

NAME = "gbx_rst_merge"


@pytest.fixture
def session(monkeypatch):
    """A session stand-in that keeps which level answers `gbx_rst_merge`: the light registration or the JVM install."""
    import databricks.labs.gbx as gbx
    from databricks.labs.gbx import manager
    from databricks.labs.gbx.pyrx import _mvp_forms

    state = {NAME: None}

    def jvm_install(_spark_session, name, _fqn):
        state[name] = "L1"
        return "ok"

    monkeypatch.setattr(
        manager,
        "register_family",
        lambda spark, prefixes, only=(), level=None: state.update(
            {n: level for n in only}
        ),
    )
    monkeypatch.setattr(_mvp_forms, "register_probe_forms", lambda spark: None)
    monkeypatch.setattr(gbx, "register", lambda spark, only=None, _levels=None: None)
    installer = SimpleNamespace(install=jvm_install)
    spark = SimpleNamespace(
        _jsparkSession=None,
        _jvm=SimpleNamespace(
            com=SimpleNamespace(
                databricks=SimpleNamespace(
                    labs=SimpleNamespace(
                        gbx=SimpleNamespace(
                            kernels=SimpleNamespace(L1Installer=installer)
                        )
                    )
                )
            )
        ),
    )
    return spark, state


def _finalizes():
    return {
        (f.level, f.variant): f
        for f in lf.FORMS["A"]
        if f.fn == "gbx_rst_merge_agg" and f.variant.startswith("finalize")
    }


def test_each_finalize_form_finds_its_own_level_under_the_merge_name(session):
    spark, state = session
    forms = _finalizes()
    # the order a run takes: the light forms of the bytes input, the JVM one, then the forms that follow it
    order = [
        ("L3", "finalize"),
        ("L1", "finalize"),
        ("L3", "finalize-skew"),
        ("L1", "finalize-skew"),
        ("L3", "finalize-virt"),
        ("L3", "finalize-virt-skew"),
    ]
    ctx = levels._Ctx(spark, 0, 1, "", None, None, {})
    for key in order:
        form = forms[key]
        levels._ensure_setup(ctx, form)
        assert state[NAME] == form.level, (key, state[NAME])


def test_virtual_inputs_carry_the_readers_sidecar_stamp(tmp_path, monkeypatch):
    """The virtual tile inputs of the by-reference forms are stamped as the reader stamps its own tiles: a folder past
    GDAL's listing limit, the files with nothing beside them. Unstamped, every worker lists the corpus folder.
    """
    from databricks.labs.gbx.pyrx._env import SIDECAR_FREE_KEY

    rows = tmp_path / "rows"
    rows.mkdir()
    for i in range(1100):
        (rows / f"t{i:04d}.tif").write_bytes(b"")
    (rows / "t0005.tif.aux.xml").write_text("<PAMDataset/>")
    small = tmp_path / "small"
    small.mkdir()
    (small / "a.tif").write_bytes(b"")
    monkeypatch.setattr(lf, "_FOLDER_NAMES", {})
    seen = {}
    monkeypatch.setattr(
        lf,
        "client_dataframe",
        lambda spark, data, schema, what="": seen.setdefault("rows", data),
    )
    pairs = [
        ("k", str(rows / "t0005.tif")),
        ("k", str(rows / "t0006.tif")),
        ("k", str(small / "a.tif")),
    ]
    lf._tile_column(None, pairs, virtual=True, keyed=True)
    stamped = {
        r[1]["path"].rsplit("/", 1)[-1]: r[1]["metadata"].get(SIDECAR_FREE_KEY)
        for r in seen["rows"]
    }
    assert stamped == {"t0005.tif": None, "t0006.tif": "1", "a.tif": None}
