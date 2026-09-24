import threading

import pytest

from databricks.labs.gbx.pyrx.checkpoint import (
    Manifest,
    checkpoint_skip,
    input_signature,
)


def test_input_signature_stable_and_order_sensitive():
    a = input_signature({"cid": 1}, {"max_image_size": 800})
    b = input_signature({"cid": 1}, {"max_image_size": 800})
    c = input_signature({"cid": 1}, {"max_image_size": 1600})
    assert a == b                    # deterministic
    assert len(a) == 16 and all(ch in "0123456789abcdef" for ch in a)
    assert a != c                    # a knob change flips the signature
    assert input_signature(1, 2) != input_signature(2, 1)  # order matters


def test_manifest_roundtrip_and_reload(tmp_path):
    out = tmp_path / "fused.ply"
    out.write_text("x")
    mpath = tmp_path / "_checkpoint.json"
    m = Manifest(str(mpath))
    assert m.is_done("dense", "g::0", "sig1") is False
    m.mark_done("dense", "g::0", "sig1", str(out))
    # a fresh Manifest reloads from disk and sees the entry
    assert Manifest(str(mpath)).is_done("dense", "g::0", "sig1") is True
    assert Manifest(str(mpath)).get_sig("dense", "g::0") == "sig1"


def test_is_done_false_when_output_missing(tmp_path):
    out = tmp_path / "fused.ply"
    out.write_text("x")
    m = Manifest(str(tmp_path / "_checkpoint.json"))
    m.mark_done("dense", "g::0", "sig1", str(out))
    out.unlink()
    assert m.is_done("dense", "g::0", "sig1") is False  # output gone → not done


def test_is_done_false_on_sig_change(tmp_path):
    out = tmp_path / "o.tif"
    out.write_text("x")
    m = Manifest(str(tmp_path / "_checkpoint.json"))
    m.mark_done("mosaic", "g", "sigA", str(out))
    assert m.is_done("mosaic", "g", "sigB") is False  # knobs changed


def test_checkpoint_skip_honors_force(tmp_path):
    out = tmp_path / "o.tif"
    out.write_text("x")
    m = Manifest(str(tmp_path / "_checkpoint.json"))
    m.mark_done("cog", "g", "s", str(out))
    assert checkpoint_skip(m, "cog", "g", "s") is True
    assert checkpoint_skip(m, "cog", "g", "s", force=True) is False


def test_manifest_preserves_other_stages(tmp_path):
    o1 = tmp_path / "a.tif"; o1.write_text("a")
    o2 = tmp_path / "b.tif"; o2.write_text("b")
    mpath = str(tmp_path / "_checkpoint.json")
    Manifest(mpath).mark_done("mosaic", "g", "s1", str(o1))
    Manifest(mpath).mark_done("cog", "g", "s2", str(o2))  # separate reload
    m = Manifest(mpath)
    assert m.is_done("mosaic", "g", "s1") and m.is_done("cog", "g", "s2")


def test_missing_manifest_loads_empty(tmp_path):
    m = Manifest(str(tmp_path / "does_not_exist.json"))
    assert m.is_done("dense", "g::0", "s") is False  # no crash


def test_corrupt_manifest_loads_empty(tmp_path):
    p = tmp_path / "_checkpoint.json"
    p.write_text("{ not json")
    m = Manifest(str(p))
    assert m.is_done("dense", "g::0", "s") is False


def test_concurrent_mark_done_threadsafe(tmp_path):
    outs = []
    for i in range(20):
        o = tmp_path / f"c{i}.ply"; o.write_text("x"); outs.append(o)
    m = Manifest(str(tmp_path / "_checkpoint.json"))

    def _mark(i):
        m.mark_done("dense", f"g::{i}", f"sig{i}", str(outs[i]))

    ts = [threading.Thread(target=_mark, args=(i,)) for i in range(20)]
    [t.start() for t in ts]; [t.join() for t in ts]
    reloaded = Manifest(str(tmp_path / "_checkpoint.json"))
    for i in range(20):
        assert reloaded.is_done("dense", f"g::{i}", f"sig{i}") is True
