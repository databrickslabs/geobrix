"""The SE forms of the L1 viability pass: slope, transformcrs and map algebra, each at the engine's JVM level, its light
row-wise and batch UDFs, today's light implementation and a floor, on one input that scales past the client cap.
"""

import pytest

from databricks.labs.gbx.bench import levels_forms as lf
from databricks.labs.gbx.bench import preflight

# function -> the input its forms of the pass read
PASS = {
    "gbx_rst_slope": "se_dem_1024_mem",
    "gbx_rst_transformcrs": "tiles_mat",
    "gbx_rst_mapalgebra": "se_pair_512_mem",
}
NEEDED = (
    {  # (level, variant label without the input tag) every function has on its input
        ("L1", "engine"),
        ("L3", "engine-c0"),
        ("L3", "engine-c1"),
        ("L3", "today"),
        ("L3", "floor-c0"),
    }
)


def _on(fn, inp):
    return [
        f for f in lf.FORMS["SE"] if f.fn == fn and f.input == inp and "@" in f.variant
    ]


@pytest.mark.parametrize("fn,inp", PASS.items())
def test_each_function_has_every_form_of_the_pass_on_one_input(fn, inp):
    got = {(f.level, f.variant.split("@")[0]) for f in _on(fn, inp)}
    assert NEEDED <= got


@pytest.mark.parametrize("fn,inp", PASS.items())
def test_the_forms_of_the_pass_are_found_by_their_names_and_by_no_other_form(fn, inp):
    names = [f.variant for f in _on(fn, inp)]
    assert len(names) == len(set(names))
    for f in _on(fn, inp):
        assert [g for g in lf.FORMS["SE"] if g.fn == fn and g.variant == f.variant] == [
            f
        ]


@pytest.mark.parametrize("fn,inp", PASS.items())
def test_the_levels_are_compared_by_decoded_pixels_not_encoding_length(fn, inp):
    for f in _on(fn, inp):
        if f.variant.startswith("floor"):
            assert not f.vsql  # the floor returns another raster by design
        else:
            assert "gbx_bench_vfp_raster" in f.vsql, f.variant
            assert f.vsql.format(src="t", uc="")  # braces balanced, only {src} left


def test_the_l1_forms_run_on_dedicated_compute_only():
    for fn, inp in PASS.items():
        for f in _on(fn, inp):
            assert (f.computes == ("dedicated",)) == (f.level == "L1")


def test_no_input_of_the_pass_is_built_on_the_client_at_16x():
    assert (
        preflight.client_cap_problems("SE", (4, 16), forms=("engine@mem", "engine@mat"))
        == []
    )
    assert (
        preflight.client_cap_problems(
            "SE",
            (4, 16),
            forms=tuple(f.variant for fn, inp in PASS.items() for f in _on(fn, inp)),
        )
        == []
    )


def test_the_fingerprint_query_of_the_corpus_tiles_does_not_hash_every_tile():
    """It runs after the timed iterations over 4000 tiles of 16 MiB: it may pick its 64 by path, not by their bytes."""
    for f in _on("gbx_rst_transformcrs", "tiles_mat"):
        if f.vsql:
            assert "ORDER BY tile.path LIMIT 64" in f.vsql and "xxhash" not in f.vsql
            assert (
                f.allows_null
            )  # a tile the corpus holds that a level cannot warp is a note, not a lost row
