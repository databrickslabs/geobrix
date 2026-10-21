"""The heavy Python ``rst_cog_convert`` shim defaults to ZSTD, like the SQL builder and the docs.

JAR-free: ``f.lit`` / ``f.call_function`` are captured so no Spark session or JAR is needed.
"""

from databricks.labs.gbx.rasterx import functions as rx


def _capture(monkeypatch):
    calls = []
    monkeypatch.setattr(rx.f, "lit", lambda v: ("lit", v))
    monkeypatch.setattr(
        rx.f, "call_function", lambda name, *args: calls.append((name, args))
    )
    return calls


def test_rst_cog_convert_default_compression_is_zstd(monkeypatch):
    calls = _capture(monkeypatch)
    rx.rst_cog_convert("tile")
    ((name, args),) = calls
    assert name == "gbx_rst_cog_convert"
    assert args == ("tile", ("lit", "ZSTD"), ("lit", 512), ("lit", "AVERAGE"))


def test_rst_cog_convert_explicit_compression_wins(monkeypatch):
    calls = _capture(monkeypatch)
    rx.rst_cog_convert("tile", "DEFLATE")
    ((name, args),) = calls
    assert args[1] == ("lit", "DEFLATE")
