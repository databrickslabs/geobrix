"""Tests for pyrx.core.compression authority."""

import pytest

from databricks.labs.gbx.pyrx.core import compression as C


def test_predictor_for_dtype():
    assert C.predictor_for("float32") == 3
    assert C.predictor_for("float64") == 3
    assert C.predictor_for("int16") == 2
    assert C.predictor_for("uint16") == 2
    assert C.predictor_for("uint8") == 1
    assert C.predictor_for("int8") == 1


def test_auto_level_scales_down_with_size():
    # New ladder: ≤128 MiB → L6, ≤1 GiB → L3, >1 GiB → L1
    # Grounded by a ZSTD level sweep across 11 tile types (see the ladder in compression.py):
    # L6 is the universal knee; L9/L12 add nothing; L16 often regresses.
    small = C.auto_level(1 * 1024**2)  # 1 MiB -> L6 (<=128 MiB)
    mid = C.auto_level(200 * 1024**2)  # 200 MiB -> L3 (<=1 GiB)
    big = C.auto_level(2 * 1024**3)  # 2 GiB -> L1 (>1 GiB)
    assert small > mid > big  # monotonic non-increasing (strict across bands)
    assert big == 1  # >1 GiB OOM guard: L1 (was L6 before spike revision)


def test_creation_opts_auto_zstd_with_predictor():
    o = C.creation_opts(
        "float32", decoded_bytes=1 * 1024**2, compress="auto", driver="GTiff"
    )
    assert o["compress"] == "zstd"
    assert o["predictor"] == "3"
    assert int(o["zstd_level"]) == C.auto_level(1 * 1024**2)


def test_creation_opts_explicit_codec_and_level():
    o = C.creation_opts("int16", compress="deflate", level=9)
    assert o["compress"] == "deflate"
    assert o["zlevel"] == "9"
    assert o["predictor"] == "2"


def test_creation_opts_explicit_predictor_override():
    o = C.creation_opts("float32", compress="zstd", level=9, predictor=1)
    assert o["predictor"] == "1"


def test_creation_opts_none():
    o = C.creation_opts("uint8", compress="none")
    assert o.get("compress") in (None, "none")  # no compression
    # a 'none' profile must not carry zstd_level/zlevel/predictor
    assert "zstd_level" not in o and "zlevel" not in o


@pytest.mark.parametrize("driver", ["GTiff", "COG"])
@pytest.mark.parametrize("name", ["raw", "none", "RAW", "NONE"])
def test_creation_opts_no_codec_names_say_none_explicitly(name, driver):
    """'raw' and 'none' mean no compression for every driver. The key is emitted, not omitted:
    the COG driver compresses with LZW when COMPRESS is absent, and a profile copied from a
    compressed source would keep its codec."""
    assert C.creation_opts("uint16", compress=name, driver=driver) == {
        "compress": "none"
    }


def test_auto_plus_explicit_level_warns():
    with pytest.warns(UserWarning, match="auto"):
        C.creation_opts("int16", decoded_bytes=1024, compress="auto", level=22)


def test_auto_without_decoded_bytes_uses_balanced_default():
    o = C.creation_opts("int16", decoded_bytes=None, compress="auto")
    assert o["compress"] == "zstd"
    assert int(o["zstd_level"]) == C._AUTO_DEFAULT_LEVEL


def test_creation_opts_cog_driver_emits_level_not_zstd_level():
    """FIX 1: COG driver uses LEVEL (not zstd_level) for ZSTD level."""
    o = C.creation_opts(
        "float32", decoded_bytes=1 * 1024**2, compress="auto", driver="COG"
    )
    assert o["compress"] == "zstd"
    assert "LEVEL" in o  # COG driver option
    assert "zstd_level" not in o  # GTiff-only option
    assert o["LEVEL"] == str(C.auto_level(1 * 1024**2))


def test_cog_profile_drops_the_layout_and_codec_keys_of_a_source():
    # the COG driver lays the file out itself and takes its codec from creation_opts(driver="COG")
    source = dict(
        driver="GTiff",
        width=4,
        count=3,
        dtype="uint8",
        nodata=0,
        interleave="pixel",
        tiled=True,
        blockxsize=256,
        blockysize=256,
        photometric="rgb",
        compress="deflate",
        zlevel=6,
        predictor=2,
    )
    assert C.cog_profile(source) == dict(
        driver="GTiff", width=4, count=3, dtype="uint8", nodata=0, interleave="pixel"
    )
    assert "tiled" in source  # the source is left as it was


def test_creation_opts_gtiff_driver_emits_zstd_level():
    """FIX 1: GTiff driver uses zstd_level (as before)."""
    o = C.creation_opts(
        "float32", decoded_bytes=1 * 1024**2, compress="auto", driver="GTiff"
    )
    assert o["compress"] == "zstd"
    assert "zstd_level" in o  # GTiff option
    assert "LEVEL" not in o  # COG-only option
    assert o["zstd_level"] == str(C.auto_level(1 * 1024**2))


def test_creation_opts_cog_explicit_zstd_with_level():
    """FIX 1: COG with explicit ZSTD uses LEVEL."""
    o = C.creation_opts("int16", compress="zstd", level=18, driver="COG")
    assert o["compress"] == "zstd"
    assert o["LEVEL"] == "18"
    assert "zstd_level" not in o


def test_creation_opts_cog_deflate_uses_level():
    """FIX 1: COG DEFLATE also uses LEVEL."""
    o = C.creation_opts("int16", compress="deflate", level=9, driver="COG")
    assert o["compress"] == "deflate"
    assert "LEVEL" in o  # COG deflate uses LEVEL
    assert "zlevel" not in o  # GTiff-only option
    assert o["LEVEL"] == "9"


# ---------------------------------------------------------------------------
# New ladder exact values (L6 knee — grounded by compression-matrix spike)
# ---------------------------------------------------------------------------


def test_auto_default_level_is_6():
    """_AUTO_DEFAULT_LEVEL must be 6 (was 9; L6 is the universal knee)."""
    assert C._AUTO_DEFAULT_LEVEL == 6


def test_auto_level_new_ladder_boundaries():
    """Exact boundary values for the new _AUTO_LADDER."""
    # ≤128 MiB → L6
    assert C.auto_level(128 * 1024**2) == 6
    # just over 128 MiB → L3
    assert C.auto_level(128 * 1024**2 + 1) == 3
    # exactly at 1 GiB → L3
    assert C.auto_level(1 * 1024**3) == 3
    # just over 1 GiB → L1
    assert C.auto_level(1 * 1024**3 + 1) == 1
    # representative values
    assert C.auto_level(1 * 1024**2) == 6  # 1 MiB
    assert C.auto_level(200 * 1024**2) == 3  # 200 MiB
    assert C.auto_level(2 * 1024**3) == 1  # 2 GiB


# ---------------------------------------------------------------------------
# GBX_ZSTD_LEVEL knob
# ---------------------------------------------------------------------------


def test_gbx_zstd_level_fast(monkeypatch):
    """GBX_ZSTD_LEVEL=fast → L1 for auto path, overriding ladder."""
    monkeypatch.setenv("GBX_ZSTD_LEVEL", "fast")
    assert C.auto_level(1 * 1024**2) == 1
    assert C.auto_level(2 * 1024**3) == 1  # size doesn't matter when knob set


def test_gbx_zstd_level_max(monkeypatch):
    """GBX_ZSTD_LEVEL=max → L9 for auto path, overriding ladder."""
    monkeypatch.setenv("GBX_ZSTD_LEVEL", "max")
    assert C.auto_level(1 * 1024**2) == 9
    assert C.auto_level(2 * 1024**3) == 9


def test_gbx_zstd_level_explicit_integer(monkeypatch):
    """GBX_ZSTD_LEVEL=3 → fixed L3 regardless of decoded size."""
    monkeypatch.setenv("GBX_ZSTD_LEVEL", "3")
    assert C.auto_level(1 * 1024**2) == 3
    assert C.auto_level(2 * 1024**3) == 3  # size doesn't matter when knob set


def test_gbx_zstd_level_default_keeps_ladder(monkeypatch):
    """GBX_ZSTD_LEVEL=default → size-adaptive ladder (same as unset)."""
    monkeypatch.setenv("GBX_ZSTD_LEVEL", "default")
    assert C.auto_level(1 * 1024**2) == 6  # ≤128 MiB → L6
    assert C.auto_level(200 * 1024**2) == 3  # ≤1 GiB → L3
    assert C.auto_level(2 * 1024**3) == 1  # >1 GiB → L1


def test_gbx_zstd_level_unset_keeps_ladder(monkeypatch):
    """GBX_ZSTD_LEVEL unset → size-adaptive ladder."""
    monkeypatch.delenv("GBX_ZSTD_LEVEL", raising=False)
    assert C.auto_level(1 * 1024**2) == 6  # ≤128 MiB → L6
    assert C.auto_level(200 * 1024**2) == 3  # ≤1 GiB → L3
    assert C.auto_level(2 * 1024**3) == 1  # >1 GiB → L1


def test_gbx_zstd_level_none_returns_default_level(monkeypatch):
    """GBX_ZSTD_LEVEL unset, decoded_bytes=None → _AUTO_DEFAULT_LEVEL."""
    monkeypatch.delenv("GBX_ZSTD_LEVEL", raising=False)
    assert C.auto_level(None) == C._AUTO_DEFAULT_LEVEL


def test_gbx_zstd_level_invalid_out_of_range_warns_and_falls_back(monkeypatch):
    """GBX_ZSTD_LEVEL with out-of-range integer → warn + ladder fallback."""
    monkeypatch.setenv("GBX_ZSTD_LEVEL", "99")
    with pytest.warns(UserWarning, match="GBX_ZSTD_LEVEL"):
        result = C.auto_level(1 * 1024**2)
    assert result == 6  # ladder: 1 MiB → L6


def test_gbx_zstd_level_invalid_non_integer_warns_and_falls_back(monkeypatch):
    """GBX_ZSTD_LEVEL with unrecognised string → warn + ladder fallback."""
    monkeypatch.setenv("GBX_ZSTD_LEVEL", "foo")
    with pytest.warns(UserWarning, match="GBX_ZSTD_LEVEL"):
        result = C.auto_level(1 * 1024**2)
    assert result == 6  # ladder: 1 MiB → L6


def test_gbx_zstd_level_knob_case_insensitive(monkeypatch):
    """GBX_ZSTD_LEVEL is parsed case-insensitively."""
    monkeypatch.setenv("GBX_ZSTD_LEVEL", "FAST")
    assert C.auto_level(1 * 1024**2) == 1

    monkeypatch.setenv("GBX_ZSTD_LEVEL", "MAX")
    assert C.auto_level(1 * 1024**2) == 9

    monkeypatch.setenv("GBX_ZSTD_LEVEL", "DEFAULT")
    assert C.auto_level(1 * 1024**2) == 6  # ladder for 1 MiB


def test_gbx_zstd_level_knob_does_not_affect_explicit_compress(monkeypatch):
    """creation_opts(compress='zstd', level=9) is unaffected by GBX_ZSTD_LEVEL.

    The knob overrides ONLY the compress='auto' path; explicit codec call sites
    already carry an explicit level and must honour it.
    """
    monkeypatch.setenv("GBX_ZSTD_LEVEL", "fast")
    o = C.creation_opts("int16", compress="zstd", level=9)
    assert o["zstd_level"] == "9"  # explicit path ignores the env knob


def test_creation_opts_gtiff_deflate_uses_zlevel():
    """FIX 1: GTiff DEFLATE uses zlevel (as before)."""
    o = C.creation_opts("int16", compress="deflate", level=9, driver="GTiff")
    assert o["compress"] == "deflate"
    assert o["zlevel"] == "9"
    assert "LEVEL" not in o


# ---------------------------------------------------------------------------
# Missing matrix: explicit ZSTD predictor for float32/uint8; full LZW matrix
# (completeness additions — these test canonical which is already correct;
# they are regression guards, not red→green drivers)
# ---------------------------------------------------------------------------


def test_creation_opts_explicit_zstd_float32_predictor_3():
    """Explicit compress='zstd' + float32 must derive predictor=3 from dtype.

    test_creation_opts_auto_zstd_with_predictor covers the auto path.  This covers
    the explicit codec path (different code branch in creation_opts).
    """
    o = C.creation_opts("float32", compress="zstd", level=9)
    assert o["compress"] == "zstd"
    assert o["predictor"] == "3"
    assert o["zstd_level"] == "9"


def test_creation_opts_explicit_zstd_uint8_predictor_1():
    """Explicit compress='zstd' + uint8 must derive predictor=1 (no predictor for byte data)."""
    o = C.creation_opts("uint8", compress="zstd", level=6)
    assert o["compress"] == "zstd"
    assert o["predictor"] == "1"


def test_creation_opts_explicit_deflate_uint8_predictor_1():
    """Explicit compress='deflate' + uint8 must derive predictor=1.

    Regression guard: pre-fold _write.py used '3 if _is_float else 2', returning 2 for uint8.
    Canonical creation_opts must return predictor=1 (matching heavy OperatorOptions).
    """
    o = C.creation_opts("uint8", compress="deflate", level=6)
    assert o["compress"] == "deflate"
    assert o["predictor"] == "1"
    assert o["zlevel"] == "6"


def test_creation_opts_lzw_float32():
    """LZW + float32 must carry predictor=3 (floating-point horizontal differencing)."""
    o = C.creation_opts("float32", compress="lzw")
    assert o["compress"] == "lzw"
    assert o["predictor"] == "3"


def test_creation_opts_lzw_int16():
    """LZW + int16 must carry predictor=2 (integer horizontal differencing)."""
    o = C.creation_opts("int16", compress="lzw")
    assert o["compress"] == "lzw"
    assert o["predictor"] == "2"


def test_creation_opts_lzw_uint8_predictor_1():
    """LZW + uint8 must carry predictor=1 (no predictor for byte data)."""
    o = C.creation_opts("uint8", compress="lzw")
    assert o["compress"] == "lzw"
    assert o["predictor"] == "1"


# ---------------------------------------------------------------------------
# COG predictor hygiene (GDAL 3.12 COG driver — predictor=1 is the default
# "no prediction"; passing it explicitly is redundant and may trigger a
# CPLE_NotSupported warning on future GDAL versions.  Omit it for COG only.
# ---------------------------------------------------------------------------


def test_cog_predictor_hygiene_uint8_auto_omits_predictor():
    """COG + uint8 auto path: predictor key must be absent (pred=1 is COG default).

    GDAL 3.12's COG driver accepts numeric predictor=1 without warning but
    silently drops it (it does not appear in gdalinfo output).  Omitting the
    key avoids passing a redundant option and is behaviour-equivalent.

    This is the canonical red->green TDD test for the COG predictor hygiene fix.
    """
    o = C.creation_opts(
        "uint8", compress="auto", driver="COG", decoded_bytes=1 * 1024**2
    )
    assert o["compress"] == "zstd"
    assert (
        "predictor" not in o
    ), f"COG driver: predictor=1 must be omitted, got predictor={o.get('predictor')!r}"


def test_cog_predictor_hygiene_uint8_explicit_zstd_omits_predictor():
    """COG + uint8 explicit zstd: predictor key must be absent."""
    o = C.creation_opts("uint8", compress="zstd", driver="COG")
    assert o["compress"] == "zstd"
    assert "predictor" not in o


def test_cog_predictor_hygiene_uint8_explicit_deflate_omits_predictor():
    """COG + uint8 explicit deflate: predictor key must be absent."""
    o = C.creation_opts("uint8", compress="deflate", driver="COG")
    assert o["compress"] == "deflate"
    assert "predictor" not in o


def test_cog_predictor_hygiene_int8_auto_omits_predictor():
    """COG + int8 auto path: predictor key must be absent (same pred=1 as uint8)."""
    o = C.creation_opts(
        "int8", compress="auto", driver="COG", decoded_bytes=1 * 1024**2
    )
    assert "predictor" not in o


def test_cog_predictor_hygiene_float32_preserves_predictor_3():
    """Regression guard: COG + float32 must still emit predictor=3.

    The hygiene fix must not affect
    the floating-point predictor case.
    """
    o = C.creation_opts(
        "float32", compress="auto", driver="COG", decoded_bytes=1 * 1024**2
    )
    assert o["compress"] == "zstd"
    assert (
        o.get("predictor") == "3"
    ), f"COG float32 must have predictor='3', got {o.get('predictor')!r}"


def test_cog_predictor_hygiene_int16_preserves_predictor_2():
    """COG + int16 must still emit predictor=2 (integer delta encoding)."""
    o = C.creation_opts(
        "int16", compress="auto", driver="COG", decoded_bytes=1 * 1024**2
    )
    assert o.get("predictor") == "2"


def test_cog_predictor_hygiene_gtiff_uint8_unaffected():
    """Regression guard: GTiff + uint8 must still emit predictor=1 (unchanged).

    The COG hygiene fix must not affect the GTiff driver path.
    """
    o = C.creation_opts(
        "uint8", compress="auto", driver="GTiff", decoded_bytes=1 * 1024**2
    )
    assert o.get("predictor") == "1"


@pytest.mark.parametrize("compress", ["auto", "zstd", "deflate", "lzw"])
def test_creation_opts_promotes_odd_nbits_under_predictor_2(compress):
    """PREDICTOR=2 needs 8/16/32/64-bit samples: a 15-bit source (Sentinel-2 style
    uint16 with NBITS=15) is promoted to the dtype's full width, losslessly."""
    opts = C.creation_opts("uint16", compress=compress, driver="COG", nbits=15)
    assert opts["predictor"] == "2" and opts["nbits"] == "16"


def test_creation_opts_leaves_nbits_alone_when_not_needed():
    """No promotion for byte-aligned depths, unknown depths, or no predictor 2."""
    for nbits in (None, 16):
        assert "nbits" not in C.creation_opts("uint16", compress="deflate", nbits=nbits)
    assert "nbits" not in C.creation_opts("uint8", compress="deflate", nbits=4)
    assert "nbits" not in C.creation_opts("uint16", compress="none", nbits=15)


def test_drop_stripe_blocks_drops_only_the_strip_width_of_an_untiled_profile():
    striped = {"tiled": False, "blockxsize": 64, "blockysize": 8, "compress": "deflate"}
    assert C.drop_stripe_blocks(striped) == {
        "tiled": False,
        "blockysize": 8,
        "compress": "deflate",
    }
    assert striped["blockxsize"] == 64  # the argument is not changed
    tiled = {"tiled": True, "blockxsize": 256, "blockysize": 256}
    assert C.drop_stripe_blocks(tiled) == tiled
    assert C.drop_stripe_blocks({"width": 4}) == {"width": 4}  # no block keys at all


def test_source_profile_is_drop_ycbcr_and_drop_stripe_blocks_in_one():
    prof = {
        "photometric": "ycbcr",
        "compress": "jpeg",
        "tiled": False,
        "blockxsize": 64,
        "blockysize": 8,
    }
    assert C.source_profile(prof) == {
        "compress": "jpeg",
        "tiled": False,
        "blockysize": 8,
    }
    assert C.source_profile(prof, unless_jpeg=True)["photometric"] == "ycbcr"


def test_drop_ycbcr_is_called_only_inside_the_compression_module():
    """A writer starts from a source's profile through ``source_profile`` (``drop_ycbcr`` plus
    ``drop_stripe_blocks``), never by calling ``drop_ycbcr`` itself: one call site per rule, so a site
    cannot pick up the YCbCr photometric fix and miss the strip-width one."""
    import ast
    import pathlib

    src = pathlib.Path(C.__file__).parents[2]  # .../databricks/labs/gbx
    offenders = []
    for path in sorted(src.rglob("*.py")):
        if path.name == "compression.py":
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Call):
                fn = node.func
                name = (
                    fn.attr
                    if isinstance(fn, ast.Attribute)
                    else getattr(fn, "id", None)
                )
                if name == "drop_ycbcr":
                    offenders.append(f"{path.relative_to(src)}:{node.lineno}")
    assert offenders == [], offenders


# A function that copies a source dataset's profile must also clean it (``source_profile``,
# ``cog_profile``) or hand it to ``write_gtiff``, which does. These copy a profile for another use.
_PROFILE_CLEANERS = {"source_profile", "cog_profile", "write_gtiff"}
_PROFILE_COPY_EXCEPTIONS = {
    (
        "pyrx/core/open_tile.py",
        "profile",
    ): "a read-only view of a window; nothing is written",
    (
        "pyrx/core/open_tile.py",
        "materialize_array",
    ): "returns the profile to its caller",
    (
        "pyrx/core/focal.py",
        "_out_profile",
    ): "drops tiled, blockxsize, blockysize and interleave itself",
}


def _copies_of_a_dataset_profile(tree):
    """``(line, enclosing function)`` of ``x.profile.copy()``, ``dict(x.profile)`` and ``{**x.profile}``."""
    import ast

    def is_profile(node):
        return isinstance(node, ast.Attribute) and node.attr == "profile"

    found = []

    def visit(node, func):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            func = node
        hit = (
            isinstance(node, ast.Call)
            and (
                (
                    isinstance(node.func, ast.Attribute)
                    and node.func.attr == "copy"
                    and is_profile(node.func.value)
                )
                or (
                    isinstance(node.func, ast.Name)
                    and node.func.id == "dict"
                    and any(is_profile(a) for a in node.args)
                )
            )
        ) or (
            isinstance(node, ast.Dict)
            and any(k is None and is_profile(v) for k, v in zip(node.keys, node.values))
        )
        if hit:
            found.append((node.lineno, func))
        for child in ast.iter_child_nodes(node):
            visit(child, func)

    visit(tree, None)
    return found


def _called_names(func):
    import ast

    return {
        (
            n.func.attr
            if isinstance(n.func, ast.Attribute)
            else getattr(n.func, "id", None)
        )
        for n in ast.walk(func)
        if isinstance(n, ast.Call)
    }


def test_a_profile_copied_from_a_source_is_cleaned_before_it_is_written():
    """A writer that starts from ``ds.profile.copy()`` and never cleans it logs GDAL's BLOCKXSIZE
    warning for a striped source and fails on a JPEG/YCbCr one (15 sites did before ``source_profile``
    was applied to them). New writers go through ``source_profile`` / ``cog_profile`` /
    ``write_gtiff``; the few that copy a profile for another use are listed with their reason.
    """
    import ast
    import pathlib

    src = pathlib.Path(C.__file__).parents[2]  # .../databricks/labs/gbx
    offenders, used = [], set()
    for path in sorted(src.rglob("*.py")):
        rel = path.relative_to(src).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for lineno, func in _copies_of_a_dataset_profile(tree):
            name = func.name if func is not None else "<module>"
            if (rel, name) in _PROFILE_COPY_EXCEPTIONS:
                used.add((rel, name))
            elif func is None or not (_called_names(func) & _PROFILE_CLEANERS):
                offenders.append(f"{rel}:{lineno} in {name}")
    assert offenders == [], offenders
    assert used == set(
        _PROFILE_COPY_EXCEPTIONS
    ), "a listed exception no longer copies a profile"
