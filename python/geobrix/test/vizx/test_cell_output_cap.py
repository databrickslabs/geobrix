"""Serverless cell-output-cap auto-raise (set_cell_max_output).

vizx interactive embeds base64 into one notebook cell; Databricks Serverless caps
cell output at 10 MB by default (20 MB max). When set_cell_max_output is on (default),
plot_interactive raises the cap to its max via %set_cell_max_output_size_in_mb so a
larger map isn't truncated -- ONLY on the interactive-embed path, and a graceful
no-op off Serverless.
"""

import databricks.labs.gbx.vizx._interactive as IV
import databricks.labs.gbx.vizx._maplibre as M
import databricks.labs.gbx.vizx._static_map as S
from databricks.labs.gbx.vizx._layers import pmtiles_layer
from databricks.labs.gbx.vizx._maplibre import (
    DEFAULT_MAX_EMBED_MB,
    MAX_EMBED_MB_CAP_RAISED,
    _resolve_embed_budget,
)

# prepare_layers is mocked in the integration tests, so the layer content is
# irrelevant -- as_layers just needs a non-empty list of real Layer objects.
_LYRS = [pmtiles_layer(b"x")]


def test_embed_budget_tracks_the_cap_and_stays_under_the_measured_safe_ceiling():
    assert _resolve_embed_budget(None, True) == MAX_EMBED_MB_CAP_RAISED
    assert _resolve_embed_budget(None, False) == DEFAULT_MAX_EMBED_MB
    # Explicit budget always wins (including 0 = force static).
    assert _resolve_embed_budget(12, True) == 12
    assert _resolve_embed_budget(0, False) == 0
    # displayHTML inflates the cell payload ~2-3.3x; live Serverless renders embedded 6 MB
    # and truncated 13 MB even at the raised 20 MB cap. Do not raise without a fresh live render.
    assert MAX_EMBED_MB_CAP_RAISED <= 6 and DEFAULT_MAX_EMBED_MB <= 3


class _FakeIP:
    def __init__(self, has_magic=True):
        self._has = has_magic
        self.calls = []

    def find_line_magic(self, name):
        return (lambda *a, **k: None) if self._has else None

    def run_line_magic(self, name, arg):
        self.calls.append((name, arg))


def test_raise_cap_fires_only_when_the_magic_exists_and_never_raises(monkeypatch):
    ip = _FakeIP(has_magic=True)
    monkeypatch.setattr(IV, "get_ipython", lambda: ip)
    assert IV._raise_cell_output_cap() is True
    assert ip.calls == [("set_cell_max_output_size_in_mb", "20")]

    def boom():
        raise RuntimeError("kernel gone")

    no_magic = _FakeIP(has_magic=False)
    for getter in (lambda: no_magic, lambda: None, boom):
        monkeypatch.setattr(IV, "get_ipython", getter)
        assert IV._raise_cell_output_cap() is False
    assert no_magic.calls == []


def _result(mode):
    return {"mode": mode, "prepared": [], "warnings": [], "audit": {}}


def test_plot_interactive_raises_the_cap_only_on_the_interactive_path_when_enabled(
    monkeypatch,
):
    import builtins

    calls = []
    monkeypatch.setattr(IV, "_raise_cell_output_cap", lambda: (calls.append(1) or True))
    monkeypatch.setattr(M, "build_html", lambda *a, **k: "<div>map</div>")
    monkeypatch.setattr(IV, "_notebook_display_html", lambda: None)
    monkeypatch.setattr(S, "plot_static", lambda *a, **k: "STATIC")
    # Without IPython.display, plot_interactive returns the html str, so it can be asserted on
    # (IPython is present in the light CI env, where the display path would return None).
    _real_import = builtins.__import__

    def _no_ipython(name, *a, **k):
        if name == "IPython.display":
            raise ImportError("disabled for test")
        return _real_import(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", _no_ipython)

    monkeypatch.setattr(M, "prepare_layers", lambda *a, **k: _result("interactive"))
    assert IV.plot_interactive(_LYRS, set_cell_max_output=True) == "<div>map</div>"
    assert calls == [1]
    IV.plot_interactive(_LYRS, set_cell_max_output=False)
    assert calls == [1]  # disabled: untouched
    monkeypatch.setattr(M, "prepare_layers", lambda *a, **k: _result("static"))
    assert IV.plot_interactive(_LYRS, set_cell_max_output=True) == "STATIC"
    assert calls == [1]  # static fallback never raises the cap


def _rich_result(mode):
    return {
        "mode": mode,
        "prepared": [],
        "warnings": [],
        "audit": {
            "layers": [{"label": "L", "kind": "pmtiles", "embed_bytes": 1_048_576}],
            "total_embed_bytes": 1_048_576,
            "fits": True,
            "verdict": "embed",
        },
    }


def test_debug_mode_levels_are_silent_then_concise_then_verbose(monkeypatch, capsys):
    monkeypatch.setattr(
        M, "prepare_layers", lambda *a, **k: _rich_result("interactive")
    )
    monkeypatch.setattr(M, "build_html", lambda *a, **k: "<div>map</div>")
    monkeypatch.setattr(IV, "_notebook_display_html", lambda: None)
    monkeypatch.setattr(IV, "_raise_cell_output_cap", lambda: True)

    def out_at(level):
        IV.plot_interactive(_LYRS, set_cell_max_output=True, debug_mode=level)
        return capsys.readouterr().out

    silent, concise, verbose = out_at(0), out_at(1), out_at(2)
    assert "[vizx]" not in silent
    assert "[vizx]" in concise
    assert len(verbose) > len(concise)
