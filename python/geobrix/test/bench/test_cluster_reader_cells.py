"""The reader and writer bench cells of build_bench_notebook (NetCDF reader and writer, vector formats, PMTiles write).

Each ``benchmark_<x>`` flag adds its cell beside the function sections, each ``<x>_only`` flag drops the function
sections, the preamble carries the pair of flags, and the cell reports into the shared sink and light accumulator.
"""

import pytest

from databricks.labs.gbx.bench import cluster as cl


def _cfg(**kw):
    base = dict(
        wheel="/Volumes/c/s/v/geobrix-0.4.3-py3-none-any.whl",
        corpus="/Volumes/c/s/v/bench-corpus",
        out_dir="/Volumes/c/s/v/bench-out/run1",
        table="main.default.bench_results",
        run_id="run1",
        functions="rst_width,rst_slope",
        modes="both",
        row_counts="1000",
        warmup=1,
        measured=3,
        heavyweight=True,
        lightweight=True,
    )
    base.update(kw)
    return base


def _src(nb):
    return "\n".join("".join(c.get("source", [])) for c in nb["cells"])


def _section_cells(nb):
    return [
        "".join(c["source"])
        for c in nb["cells"]
        if "".join(c["source"]).lstrip().startswith("# (")
    ]


# flag -> (its only flag, the preamble variables, the leg it calls, text only its cell holds, the sink variable)
_CELLS = {
    "benchmark_netcdf": (
        "netcdf_only",
        ("BENCHMARK_NETCDF", "NETCDF_ONLY"),
        "run_format_read",
        "NETCDF BENCH SKIPPED",
        "_r",
    ),
    "benchmark_netcdf_writer": (
        "netcdf_writer_only",
        ("BENCHMARK_NETCDF_WRITER", "NETCDF_WRITER_ONLY"),
        "run_format_write",
        "NETCDF WRITER RASTER BENCH SKIPPED",
        "_wr",
    ),
    "benchmark_vector": (
        "vector_only",
        ("BENCHMARK_VECTOR", "VECTOR_ONLY"),
        "run_format_read",
        "shapefile_gbx",
        "_r",
    ),
    "benchmark_pmtiles": (
        "pmtiles_only",
        ("BENCHMARK_PMTILES", "PMTILES_ONLY"),
        "run_pmtiles_write",
        'write_fmt="pmtiles_gbx"',
        "_r",
    ),
}


@pytest.mark.parametrize("flag", _CELLS)
def test_flag_adds_its_cell_and_only_drops_the_function_sections(flag):
    only, (on_var, only_var), leg, marker, sink = _CELLS[flag]
    on = cl.build_bench_notebook(_cfg(**{flag: True}))
    src = _src(on)
    assert leg in src and marker in src
    assert f"{on_var} = True" in src and f"{only_var} = False" in src
    assert len(_section_cells(on)) == 4, "the function sections stay"
    assert f"_sink([{sink}])" in src and f"lw.append({sink})" in src

    only_nb = cl.build_bench_notebook(_cfg(**{only: True}))
    assert leg in _src(only_nb) and _section_cells(only_nb) == []
    assert f"{only_var} = True" in _src(only_nb) and f"{on_var} = False" in _src(
        only_nb
    )
    # a reader-only run stages no function corpus, so the corpus.json read must not be unconditional
    assert 'corpus = _m.Corpus.read(f"{CORPUS}/corpus.json")\nfnspecs' not in _src(
        only_nb
    )
    assert "_m.Corpus.read" in _src(cl.build_bench_notebook(_cfg()))

    off = _src(cl.build_bench_notebook(_cfg(**{flag: False, only: False})))
    assert marker not in off and f"{on_var} = False" in off


def test_netcdf_reader_has_a_raster_leg_and_a_light_only_swath_leg():
    src = _src(cl.build_bench_notebook(_cfg(benchmark_netcdf=True)))
    assert (
        "netcdf_gbx" in src and "netcdf_gdal" not in src
    ), "the Scala reader is retired"
    assert r".*\.nc$" in src
    assert 'f"{CORPUS}/netcdf"' in src and "size_mib=-1" in src  # one tile per variable
    assert 'f"{CORPUS}/netcdf-swath"' in src and "NETCDF SWATH BENCH SKIPPED" in src
    assert '"mode": "vector"' in src or '"mode":"vector"' in src
    assert "list_corpus_files" in src, "an empty pool is a clear skip, not a failure"
    assert "if LIGHTWEIGHT:" in _src(
        cl.build_bench_notebook(_cfg(netcdf_only=True, heavyweight=False))
    )


def test_netcdf_writer_legs_plain_single_file_and_merge_over_distinct_inputs():
    src = _src(cl.build_bench_notebook(_cfg(benchmark_netcdf_writer=True)))
    assert 'read_fmt="netcdf_gbx"' in src and 'write_fmt="netcdf_gbx"' in src
    for pool in ("netcdf-distinct", "netcdf-out", "netcdf-swath", "netcdf-swath-out"):
        assert f'f"{{CORPUS}}/{pool}"' in src, pool
    assert '"singleFile": "true"' in src or '"singleFile":"true"' in src
    assert '"merge": "true"' in src or '"merge":"true"' in src
    assert src.count('label="singleFile"') == 2 and src.count('label="merge"') == 2
    for target in ("netcdf-out-single", "netcdf-out-merge", "netcdf-swath-out-merge"):
        assert f'f"{{CORPUS}}/{target}"' in src, target
    # single and merge fail by design on a pool whose members share variables
    assert 'f"{CORPUS}/netcdf"' not in src
    assert "NETCDF WRITER SWATH BENCH SKIPPED" in src
    assert (
        "light-only"
        in _src(cl.build_bench_notebook(_cfg(netcdf_writer_only=True))).lower()
    )


def test_vector_cell_times_the_gbx_formats_and_none_of_the_retired_readers():
    src = _src(cl.build_bench_notebook(_cfg(benchmark_vector=True)))
    for fmt in ("geojson_gbx", "shapefile_gbx", "gpkg_gbx", "file_gdb_gbx"):
        assert fmt in src
    assert "_ogr" not in src and "VECTOR PARITY" not in src
    off = _src(cl.build_bench_notebook(_cfg()))
    assert not any(f in off for f in ("shapefile_gbx", "gpkg_gbx", "file_gdb_gbx"))


def test_pmtiles_cell_writes_only_pmtiles_gbx():
    src = _src(cl.build_bench_notebook(_cfg(benchmark_pmtiles=True)))
    assert 'write_fmt="pmtiles_gbx"' in src and 'write_fmt="pmtiles"' not in src
    assert "PMTILES PARITY" not in src
