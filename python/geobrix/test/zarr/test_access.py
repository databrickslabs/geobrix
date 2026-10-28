"""Range readers (FUSE and obstore), direct-access resolution, and the credentials' secrecy.

``FuseRangeReader``: coalesced reads, drift, and the containment of every path it opens.
``resolve_access`` / ``ObstoreRangeReader`` / ``put_pack``: Unity Catalog credential
vending (a faked SDK; nothing here calls Databricks) and obstore against ``file://``
locations, which obstore's ``LocalStore`` serves, so the same bytes come back as from the
FUSE reader. Credentials are a secret: the sentinel in these tests must reach only the
task closure.
"""

import dataclasses
import errno
import os
import shutil
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

from databricks.labs.gbx.zarr import _access
from databricks.labs.gbx.zarr._access import (
    AccessConfig,
    FuseRangeReader,
    ObstoreRangeReader,
    reader_for,
    resolve_access,
)
from databricks.labs.gbx.zarr._shard import Region
from databricks.labs.gbx.zarr.errors import ZarrAccessUnavailable, ZarrStoreDrift

DATA = bytes(range(100))


@pytest.fixture
def packs(tmp_path):
    (tmp_path / "base").mkdir()
    (tmp_path / "over").mkdir()
    (tmp_path / "base" / "a.bin").write_bytes(DATA)
    (tmp_path / "over" / "b.bin").write_bytes(bytes(range(100, 200)))
    return tmp_path


@pytest.fixture
def vol(tmp_path):
    """A store root, a sibling whose name starts with the root's, a base root, and a
    directory that belongs to neither."""
    for name in ("store", "store2", "base", "outside"):
        (tmp_path / name).mkdir()
        (tmp_path / name / "x.bin").write_bytes(DATA)
    (tmp_path / "outside" / "secret.bin").write_bytes(b"secret-bytes")
    return tmp_path


def test_reader_resolves_relative_files_and_external_origin(packs):
    reader = FuseRangeReader(str(packs / "over"), base_root=str(packs / "base"))
    got = reader.read(
        [
            Region("b.bin", 2, 3),
            Region("a.bin", 10, 4, "external"),
            Region(str(packs / "base" / "a.bin"), 0, 2),  # absolute, under base_root
        ]
    )
    assert got == [bytes([102, 103, 104]), bytes([10, 11, 12, 13]), bytes([0, 1])]
    # without a base_root an external file resolves under the root
    assert FuseRangeReader(str(packs / "base")).read(
        [Region("a.bin", 5, 2, "external")]
    ) == [bytes([5, 6])]


def test_reader_coalesces_adjacent_ranges_and_keeps_the_order(packs, monkeypatch):
    preads = []
    real = os.pread

    def counting(fd, n, offset):
        preads.append((offset, n))
        return real(fd, n, offset)

    monkeypatch.setattr(os, "pread", counting)
    reader = FuseRangeReader(str(packs / "base"))
    regions = [
        Region("a.bin", 30, 10),  # adjacent to the next one in file order
        Region("a.bin", 10, 20),
        Region("a.bin", 50, 5),  # a gap: its own read
        Region("a.bin", 35, 10),  # overlaps the first
        Region("a.bin", 7, 0),  # empty
    ]
    got = reader.read(regions)
    assert got == [
        bytes(range(30, 40)),
        bytes(range(10, 30)),
        bytes(range(50, 55)),
        bytes(range(35, 45)),
        b"",
    ]
    assert preads == [(10, 35), (50, 5)]


def test_reader_reports_a_missing_or_short_file_as_drift(packs):
    reader = FuseRangeReader(str(packs / "base"), attempts=1)
    with pytest.raises(ZarrStoreDrift) as ei:
        reader.read([Region("gone.bin", 0, 4)])
    assert "gone.bin" in str(ei.value) and "gbx_zarr_sync" in ei.value.fix
    with pytest.raises(ZarrStoreDrift) as ei:
        reader.read([Region("a.bin", 90, 20)])  # the file has 100 bytes
    assert "a.bin" in str(ei.value) and "Fix:" in str(ei.value)


@pytest.mark.parametrize("bad", [(-1, 4), (0, -4), (-8, -8)])
def test_a_negative_offset_or_length_is_drift(packs, bad):
    with pytest.raises(ZarrStoreDrift) as ei:
        FuseRangeReader(str(packs / "base")).read([Region("a.bin", *bad)])
    assert "Fix:" in str(ei.value)


# --- containment ----------------------------------------------------------------


def _refused(reader, region):
    with pytest.raises(ZarrStoreDrift) as ei:
        reader.read([region])
    return ei.value


def test_a_dotdot_escape_is_refused_and_one_that_stays_inside_is_not(vol):
    reader = FuseRangeReader(str(vol / "store"), attempts=1)
    err = _refused(reader, Region("../outside/secret.bin", 0, 4))
    assert "'../outside/secret.bin' resolves outside the store root" in str(err)
    assert str(vol / "store") in str(err)
    assert "gbx_zarr_sync" in err.fix and "base root" in err.fix
    assert reader.read([Region("sub/../x.bin", 0, 3)]) == [bytes([0, 1, 2])]


def test_an_absolute_path_is_allowed_only_under_a_root(vol):
    reader = FuseRangeReader(
        str(vol / "store"), base_root=str(vol / "base"), attempts=1
    )
    for ok in ("store", "base"):
        assert reader.read([Region(str(vol / ok / "x.bin"), 1, 2)]) == [bytes([1, 2])]
    err = _refused(reader, Region(str(vol / "outside" / "secret.bin"), 0, 4))
    assert "resolves outside the store root" in str(err)
    assert str(vol / "store") in str(err) and str(vol / "base") in str(err)
    # without a base_root only the root is allowed
    plain = FuseRangeReader(str(vol / "store"), attempts=1)
    _refused(plain, Region(str(vol / "base" / "x.bin"), 0, 4))


def test_a_sibling_whose_name_starts_with_the_root_is_outside_it(vol):
    reader = FuseRangeReader(str(vol / "store"), attempts=1)
    _refused(reader, Region(str(vol / "store2" / "x.bin"), 0, 4))
    _refused(reader, Region("../store2/x.bin", 0, 4))
    # the same holds for a base_root
    ext = FuseRangeReader(
        str(vol / "outside"), base_root=str(vol / "store"), attempts=1
    )
    _refused(ext, Region("../store2/x.bin", 0, 4, "external"))


def test_a_relative_file_is_confined_to_the_root_of_its_origin(vol):
    reader = FuseRangeReader(
        str(vol / "store"), base_root=str(vol / "base"), attempts=1
    )
    # under the root a '..' reaching the base root is still outside the root
    _refused(reader, Region("../base/x.bin", 0, 4))
    # an external file may not reach the overlay root either
    _refused(reader, Region("../store/x.bin", 0, 4, "external"))


def test_a_symlink_may_not_leave_the_root(vol):
    store = vol / "store"
    (store / "link_dir").symlink_to(vol / "outside", target_is_directory=True)
    (store / "link.bin").symlink_to(vol / "outside" / "secret.bin")
    (store / "inside.bin").symlink_to(store / "x.bin")
    reader = FuseRangeReader(str(store), attempts=1)
    _refused(reader, Region("link_dir/secret.bin", 0, 4))
    _refused(reader, Region("link.bin", 0, 4))
    _refused(reader, Region(str(store / "link.bin"), 0, 4))
    # a link that stays inside is fine
    assert reader.read([Region("inside.bin", 0, 2)]) == [bytes([0, 1])]


@pytest.mark.parametrize("file", ["", None, "a\0b", 7])
def test_an_unusable_file_value_is_drift(vol, file):
    err = _refused(FuseRangeReader(str(vol / "store")), Region(file, 0, 4))
    assert "Fix:" in str(err)


def test_the_real_path_of_a_file_is_looked_up_once(vol, monkeypatch):
    calls = []
    real = os.path.realpath
    monkeypatch.setattr(
        os.path, "realpath", lambda p, **k: calls.append(p) or real(p, **k)
    )
    reader = FuseRangeReader(str(vol / "store"))
    reader.read([Region("x.bin", 0, 4)])
    first = len(calls)
    assert first > 0
    for _ in range(3):
        reader.read([Region("x.bin", 4, 4), Region("x.bin", 20, 4)])
    assert len(calls) == first


# --- the descriptor, not the name -----------------------------------------------

needs_proc = pytest.mark.skipif(
    not os.path.isdir("/proc/self/fd"), reason="needs /proc/self/fd (Linux)"
)


@needs_proc
def test_a_file_swapped_for_an_outside_link_after_the_check_is_refused(vol):
    """The real path of a file is cached, so a link put in its place later still looks
    inside the root by name. The opened descriptor says where the bytes really come from.
    """
    store = vol / "store"
    reader = FuseRangeReader(str(store), attempts=1)
    assert reader.read([Region("x.bin", 0, 3)]) == [bytes([0, 1, 2])]
    (store / "x.bin").unlink()
    (store / "x.bin").symlink_to(vol / "outside" / "secret.bin")
    err = _refused(reader, Region("x.bin", 0, 4))
    assert "resolves outside the store root" in str(err) and str(store) in str(err)
    assert "gbx_zarr_sync" in err.fix


@needs_proc
def test_a_descriptor_that_lands_outside_the_root_is_closed_and_refused(
    vol, monkeypatch
):
    """The race itself: the name checked is inside, the file opened is not."""
    opened, closed = [], []
    real_open, real_close = os.open, os.close

    def other_file(path, flags, *args, **kwargs):
        fd = real_open(str(vol / "outside" / "secret.bin"), flags)
        opened.append(fd)
        return fd

    def record_close(fd):
        closed.append(fd)
        return real_close(fd)

    monkeypatch.setattr(os, "open", other_file)
    monkeypatch.setattr(os, "close", record_close)
    reader = FuseRangeReader(str(vol / "store"), attempts=1)
    err = _refused(reader, Region("x.bin", 0, 4))
    assert "resolves outside the store root" in str(err)
    assert opened and closed == opened


@needs_proc
def test_the_descriptor_check_allows_each_root_a_file_was_checked_against(vol):
    (vol / "store" / "inside.bin").symlink_to(vol / "store" / "x.bin")
    reader = FuseRangeReader(
        str(vol / "store"), base_root=str(vol / "base"), attempts=1
    )
    assert reader.read(
        [
            Region("inside.bin", 1, 2),  # a link that stays in the root
            Region("x.bin", 3, 2, "external"),  # a base-root file
            Region(str(vol / "base" / "x.bin"), 5, 2),  # absolute, under the base root
        ]
    ) == [bytes([1, 2]), bytes([3, 4]), bytes([5, 6])]


# --- retries --------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc",
    [
        PermissionError(errno.EACCES, "denied"),
    ],
)
def test_permanent_errors_are_not_retried(packs, monkeypatch, exc):
    opens, sleeps = [], []

    def refuse(*args, **kwargs):
        opens.append(1)
        raise exc

    monkeypatch.setattr(os, "open", refuse)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    with pytest.raises(type(exc)):
        FuseRangeReader(str(packs / "base")).read([Region("a.bin", 0, 4)])
    assert opens == [1] and sleeps == []


def test_a_directory_named_as_a_file_is_drift_and_not_retried(vol, monkeypatch):
    """A row that names a directory is a manifest that does not match the store; it
    surfaces as ``ZarrStoreDrift`` (``os.open`` of a directory succeeds on Linux and the
    ``pread`` then fails with ``IsADirectoryError``), and waiting cannot fix it."""
    (vol / "store" / "adir").mkdir()
    sleeps = []
    monkeypatch.setattr(time, "sleep", sleeps.append)
    err = _refused(FuseRangeReader(str(vol / "store")), Region("adir", 0, 4))
    assert "adir" in str(err) and "directory" in str(err)
    assert "Fix:" in str(err) and "gbx_zarr_sync" in err.fix
    assert sleeps == []


@pytest.mark.parametrize(
    "exc, word",
    [
        (IsADirectoryError(errno.EISDIR, "a directory"), "directory"),
        (NotADirectoryError(errno.ENOTDIR, "not a directory"), "not a directory"),
    ],
)
def test_an_open_that_reports_a_directory_is_drift_and_not_retried(
    packs, monkeypatch, exc, word
):
    opens, sleeps = [], []

    def refuse(*args, **kwargs):
        opens.append(1)
        raise exc

    monkeypatch.setattr(os, "open", refuse)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    with pytest.raises(ZarrStoreDrift) as ei:
        FuseRangeReader(str(packs / "base")).read([Region("a.bin", 0, 4)])
    assert word in str(ei.value) and "Fix:" in str(ei.value)
    assert opens == [1] and sleeps == []


def test_a_path_through_a_file_is_drift(vol):
    """``x.bin/more``: a component of the name is a file, so the file is not there."""
    err = _refused(
        FuseRangeReader(str(vol / "store"), attempts=1), Region("x.bin/more", 0, 4)
    )
    assert "x.bin" in str(err) and "Fix:" in str(err)


@needs_proc
def test_a_descriptor_that_cannot_be_traced_is_refused_and_not_retried(
    packs, monkeypatch
):
    """The check fails closed, and an OS error from it is no transient miss to retry."""
    sleeps, tries = [], []

    def untraceable(fd):
        tries.append(fd)
        raise OSError(errno.EIO, "Input/output error")

    monkeypatch.setattr("databricks.labs.gbx.zarr._access._held_path", untraceable)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    with pytest.raises(ZarrStoreDrift) as ei:
        FuseRangeReader(str(packs / "base")).read([Region("a.bin", 0, 4)])
    assert "a.bin" in str(ei.value) and "Fix:" in str(ei.value)
    assert len(tries) == 1 and sleeps == []


def test_a_transient_error_is_retried(packs, monkeypatch):
    tries, sleeps = [], []
    real = os.open

    def flaky(*args, **kwargs):
        tries.append(1)
        if len(tries) < 3:
            raise OSError(errno.EIO, "Input/output error")
        return real(*args, **kwargs)

    monkeypatch.setattr(os, "open", flaky)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    got = FuseRangeReader(str(packs / "base")).read([Region("a.bin", 5, 2)])
    assert got == [bytes([5, 6])] and len(tries) == 3 and len(sleeps) == 2


# --- the light import -----------------------------------------------------------


def test_importing_and_using_the_reader_loads_no_raster_or_spark_stack():
    """The retry guard is lazy-free: a first read must not pull in the ds package
    (PySpark, rasterio, DataSource registration)."""
    code = (
        "import os, sys, tempfile\n"
        "from databricks.labs.gbx.zarr._access import FuseRangeReader\n"
        "from databricks.labs.gbx.zarr._shard import Region\n"
        "d = tempfile.mkdtemp()\n"
        "open(os.path.join(d, 'f'), 'wb').write(b'abcdef')\n"
        "assert FuseRangeReader(d).read([Region('f', 1, 3)]) == [b'bcd']\n"
        "bad = [m for m in ('rasterio', 'pyspark', 'databricks.labs.gbx.ds') "
        "if m in sys.modules]\n"
        "assert not bad, bad\n"
    )
    done = subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True
    )
    assert done.returncode == 0, done.stderr


# --- direct access: resolving it ---------------------------------------------------------

SENTINEL = "SENTINEL-sk-4f9c1a"  # any trace of this outside a task closure is a leak
FIX = (
    'use access="auto" or "fuse" on this compute; direct access needs dedicated '
    "compute where UC can vend volume credentials"
)
VOLUME = "/Volumes/main/geo/zarr"
CLOUDS = {
    "aws": (
        {
            "aws_temp_credentials": {
                "access_key_id": SENTINEL + "-ak",
                "secret_access_key": SENTINEL + "-sk",
                "session_token": SENTINEL + "-st",
            },
            "url": "s3://bkt/meta/volumes/vid-1",
        },
        {
            "access_key_id": SENTINEL + "-ak",
            "secret_access_key": SENTINEL + "-sk",
            "session_token": SENTINEL + "-st",
        },
        "S3Store",
    ),
    "azure": (
        {
            "azure_user_delegation_sas": {"sas_token": "?sv=1&sig=" + SENTINEL},
            "url": "abfss://c@acct.dfs.core.windows.net/vols/vid-1",
        },
        {"sas_key": "sv=1&sig=" + SENTINEL},
        "AzureStore",
    ),
    "gcp": (
        {
            "gcp_oauth_token": {"oauth_token": SENTINEL},
            "url": "gs://bkt/vols/vid-1",
        },
        {"bearer_token": SENTINEL},
        "GCSStore",
    ),
}


_REAL_CANARY = _access._canary
_REAL_DISCOVER = _access._discover_region


@pytest.fixture(autouse=True)
def _vending_has_no_network(monkeypatch):
    """Vending in these tests makes no request: no canary, a known region, no discovery."""
    monkeypatch.setattr(_access, "_canary", lambda cfg, path: None)
    monkeypatch.setattr(_access, "_discover_region", lambda bucket: None)
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)


@pytest.fixture(autouse=True)
def _forget_refusals():
    """A refusal is remembered for the life of the process: every test starts without one."""
    _access._REFUSED.clear()
    yield
    _access._REFUSED.clear()


class DatabricksError(Exception):
    """Stands in for ``databricks.sdk.errors.DatabricksError``."""


class _Client:
    """A ``WorkspaceClient`` that vends canned credentials, or fails, and records its calls."""

    def __init__(self, response=None, error=None):
        self.calls, self.response, self.error = [], response, error
        self.volumes = SimpleNamespace(read=self._read)
        self.api_client = SimpleNamespace(do=self._do)

    def _read(self, name):
        self.calls.append(("volumes.read", name))
        return SimpleNamespace(volume_id="vid-1")

    def _do(self, method, path, body=None):
        self.calls.append((method, path, body))
        if self.error is not None:
            raise self.error
        return self.response


def _install(monkeypatch, response=None, error=None):
    client = _Client(response, error)
    monkeypatch.setattr(_access, "_workspace_client", lambda: client)
    return client


def _serverless_403(mp):
    _install(
        mp,
        error=DatabricksError(
            "403 UC_SERVERLESS_UNTRUSTED_DOMAIN: Serverless python and scala notebooks "
            "are prohibited from retrieving temporary storage credentials"
        ),
    )
    return VOLUME, ()


def _no_permission(mp):
    _install(mp, error=DatabricksError("User does not have EXTERNAL USE SCHEMA"))
    return VOLUME, ()


def _no_network(mp):
    _install(mp, error=ConnectionError("unreachable"))
    return VOLUME, ()


def _nothing_usable_in_the_response(mp):
    _install(mp, response={"url": "s3://bkt/x"})
    return VOLUME, ()


def _unusable_url(mp):
    _install(mp, response={**CLOUDS["aws"][0], "url": None})
    return VOLUME, ()


def _url_that_does_not_fit_the_credentials(mp):
    _install(mp, response={**CLOUDS["aws"][0], "url": "https://example.com/vol"})
    return VOLUME, ()


def _first_request_fails(mp):
    """A wrong region, say: S3 answers the one request made with the credentials with a
    redirect, and its text happens to echo a credential."""
    from obstore import exceptions as ox

    def canary(cfg, path):
        raise ox.GenericError(f"301 PermanentRedirect for key {SENTINEL}-sk")

    _install(mp, response=CLOUDS["aws"][0])
    mp.setattr(_access, "_canary", canary)
    return VOLUME, ()


def _region_not_known(mp):
    _install(mp, response=CLOUDS["aws"][0])
    mp.delenv("AWS_REGION", raising=False)  # and nothing discovers one (fixture)
    return VOLUME, ()


def _store_that_cannot_be_read(mp):
    """The real first request, against a location that is not there."""
    url = "file:///nonexistent-gbx-volume"
    _install(mp, response={**CLOUDS["aws"][0], "url": url})
    mp.setattr(_access, "_canary", _REAL_CANARY)
    return VOLUME, ()


def _no_sdk(mp):
    mp.setitem(sys.modules, "databricks.sdk", None)  # the import fails
    return VOLUME, ()


def _no_obstore(mp):
    _install(mp, response=CLOUDS["aws"][0])
    mp.setitem(sys.modules, "obstore", None)
    return VOLUME, ()


def _not_on_a_volume(mp):
    _install(mp, response=CLOUDS["aws"][0])
    return "/tmp/store", ()


def _two_volumes(mp):
    _install(mp, response=CLOUDS["aws"][0])
    return VOLUME + "/a", ("/Volumes/main/geo/other/b",)


CAUSES = [
    _serverless_403,
    _no_permission,
    _no_network,
    _nothing_usable_in_the_response,
    _no_sdk,
    _no_obstore,
    _not_on_a_volume,
    _two_volumes,
    _unusable_url,
    _url_that_does_not_fit_the_credentials,
    _first_request_fails,
    _region_not_known,
    _store_that_cannot_be_read,
]


@pytest.mark.parametrize("cause", CAUSES, ids=lambda c: c.__name__.strip("_"))
def test_auto_falls_back_to_fuse_when_vending_fails(cause, monkeypatch):
    path, also = cause(monkeypatch)
    cfg = resolve_access(None, path, access="auto", operation="READ_VOLUME", also=also)
    assert cfg.mode == "fuse" and cfg.creds is None and cfg.root_url is None


@pytest.mark.parametrize("cause", CAUSES, ids=lambda c: c.__name__.strip("_"))
def test_direct_raises_when_unavailable(cause, monkeypatch):
    path, also = cause(monkeypatch)
    with pytest.raises(ZarrAccessUnavailable) as e:
        resolve_access(None, path, access="direct", operation="READ_VOLUME", also=also)
    assert e.value.fix == FIX and "direct" in e.value.message
    assert str(e.value).endswith("Fix: " + FIX) and SENTINEL not in str(e.value)


@pytest.mark.parametrize("cloud", CLOUDS)
def test_vending_asks_uc_for_the_volume_and_maps_the_credentials(cloud, monkeypatch):
    response, creds, store = CLOUDS[cloud]
    client = _install(monkeypatch, response=response)
    where = VOLUME + "/stores/era5"
    cfg = resolve_access(
        None,
        where,
        access="direct",
        operation="WRITE_VOLUME",
        also=[VOLUME + "/base"],
    )
    assert client.calls == [
        ("volumes.read", "main.geo.zarr"),
        (
            "POST",
            "/api/2.0/unity-catalog/temporary-volume-credentials",
            {"volume_id": "vid-1", "operation": "WRITE_VOLUME"},
        ),
    ]
    url = response["url"]
    assert (cfg.mode, cfg.cloud, cfg.creds.reveal()) == ("direct", cloud, creds)
    assert (cfg.volume_path, cfg.volume_url) == (VOLUME, url)
    assert cfg.root_url == url + "/stores/era5"
    assert cfg.url_of(VOLUME + "/base/x.bin") == url + "/base/x.bin"
    assert type(_access._make_store(cfg)).__name__ == store  # built, not connected
    # "auto" asks again and gets the same; the dbfs spelling of the path is the same path
    again = resolve_access(
        None, "dbfs:" + where, access="auto", operation="WRITE_VOLUME"
    )
    assert dataclasses.replace(again, fallback=False) == cfg and again.fallback
    assert not cfg.fallback and len(client.calls) == 4


def test_direct_is_chosen_after_one_request_and_names_its_region(monkeypatch, tmp_path):
    """The canary is ONE request with the vended credentials against the store root; the
    region is the response's, else AWS_REGION, else the bucket's own; never a default.
    """
    response, creds, _ = CLOUDS["aws"]
    _install(monkeypatch, response=response)
    seen = []
    monkeypatch.setattr(_access, "_canary", lambda cfg, path: seen.append((cfg, path)))

    def region(env=None, found=None, answer=None):
        for name in ("AWS_REGION", "AWS_DEFAULT_REGION"):
            monkeypatch.delenv(name, raising=False)
        if env:
            monkeypatch.setenv(*env)
        monkeypatch.setattr(_access, "_discover_region", lambda bucket: found)
        seen.clear()
        cfg = resolve_access(
            None, VOLUME + "/stores", access="direct", operation="READ_VOLUME"
        )
        assert len(seen) == 1 and seen[0][1] == VOLUME + "/stores"
        assert (
            seen[0][0].creds.reveal() == creds
        )  # the canary signs with what was vended
        return cfg.region, _access._make_store(cfg).config.get("region")

    assert region(("AWS_REGION", "ap-south-1")) == ("ap-south-1",) * 2
    assert region(("AWS_DEFAULT_REGION", "eu-west-2")) == ("eu-west-2",) * 2
    assert region(found="eu-west-1") == ("eu-west-1",) * 2  # asked of S3
    monkeypatch.setattr(_access, "_discover_region", lambda bucket: "never")
    monkeypatch.setitem(response, "region", "sa-east-1")  # the response says
    assert region(("AWS_REGION", "ap-south-1"))[0] == "sa-east-1"

    # the real request, against a location obstore can read (no region applies to file://)
    vol = tmp_path / "vol"
    vol.mkdir()
    _install(monkeypatch, response={**CLOUDS["aws"][0], "url": f"file://{vol}"})
    monkeypatch.setattr(_access, "_canary", _REAL_CANARY)
    cfg = resolve_access(
        None, VOLUME + "/new", access="direct", operation="WRITE_VOLUME"
    )
    assert cfg.is_direct and cfg.volume_url == f"file://{vol}"


def test_the_region_of_a_bucket_is_read_from_the_header_s3_sends(monkeypatch):
    """S3 names the bucket's region on a redirect, a refusal or a success alike."""
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    answers = {
        "moved": (301, {"x-amz-bucket-region": "eu-north-1"}),
        "refused": (403, {"x-amz-bucket-region": "ap-southeast-2"}),
        "open": (200, {"x-amz-bucket-region": "us-west-1"}),
        "nowhere": (404, {}),
    }

    class Handler(BaseHTTPRequestHandler):
        def do_HEAD(self):
            status, headers = answers[self.path.strip("/")]
            self.send_response(status)
            for k, v in headers.items():
                self.send_header(k, v)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        monkeypatch.setattr(
            _access, "_S3_ENDPOINT", f"http://127.0.0.1:{server.server_port}"
        )
        got = {b: _REAL_DISCOVER(b) for b in answers}
    finally:
        server.shutdown()
    assert got == {
        "moved": "eu-north-1",
        "refused": "ap-southeast-2",
        "open": "us-west-1",
        "nowhere": None,
    }


def test_the_sdk_may_not_log_the_vending_response(monkeypatch):
    """The SDK's round-trip log prints a response body at DEBUG: while vending, its loggers
    sit above DEBUG, and they come back as they were."""
    import logging

    records = []

    class Capture(logging.Handler):
        def emit(self, record):
            if record.name.startswith("databricks.sdk"):
                records.append(record.getMessage())

    log = logging.getLogger("databricks.sdk")
    child = logging.getLogger("databricks.sdk.round_trip")
    child.setLevel(logging.DEBUG)
    handler = Capture(logging.DEBUG)
    root = logging.getLogger()
    before = (root.level, log.level, child.level)
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    try:
        response = CLOUDS["aws"][0]
        client = _install(monkeypatch, response=response)
        real_do = client.api_client.do

        def chatty(method, path, body=None):
            out = real_do(method, path, body=body)
            child.debug("< 200 OK %s", out)  # what the SDK does
            return out

        client.api_client.do = chatty
        resolve_access(None, VOLUME, access="direct", operation="READ_VOLUME")
        assert child.level == logging.DEBUG and log.level == before[1]  # restored
        child.debug("after: %s", SENTINEL)
        assert records == [f"after: {SENTINEL}"]  # only the line from outside the call
    finally:
        root.removeHandler(handler)
        root.setLevel(before[0])
        child.setLevel(before[2])
    assert not any(SENTINEL + "-sk" in r for r in records)


def test_fuse_never_asks_and_a_bad_argument_is_refused(monkeypatch):
    client = _install(monkeypatch, response=CLOUDS["aws"][0])
    assert (
        resolve_access(None, VOLUME, access="fuse", operation="READ_VOLUME").mode
        == "fuse"
    )
    assert client.calls == []
    with pytest.raises(ValueError, match="access must be one of"):
        resolve_access(None, VOLUME, access="turbo", operation="READ_VOLUME")
    with pytest.raises(ValueError, match="operation must be one of"):
        resolve_access(None, VOLUME, access="auto", operation="DELETE_VOLUME")


# --- direct access: the obstore reader ---------------------------------------------------


@pytest.fixture
def cloud(vol):
    """The ``vol`` directories as a Volume whose cloud location (``file://``, which obstore
    serves from disk) and mount path are the same place."""
    pack = bytes((i * 31 + 7) % 256 for i in range(10_000))
    (vol / "store" / "pack.bin").write_bytes(pack)
    (vol / "store" / "other.bin").write_bytes(pack[::-1])
    (vol / "base" / "b.bin").write_bytes(pack[500:])
    url = f"file://{vol}"
    cfg = AccessConfig("direct", url, {"access_key_id": SENTINEL}, "aws", str(vol), url)
    return SimpleNamespace(vol=vol, cfg=cfg, pack=pack)


def _readers(c, attempts=1):
    root, base = str(c.vol / "store"), str(c.vol / "base")
    scoped = dataclasses.replace(c.cfg, root_url=c.cfg.url_of(root))
    return {
        "fuse": FuseRangeReader(root, base_root=base, attempts=attempts),
        "obstore": ObstoreRangeReader(
            scoped, base_url=c.cfg.url_of(base), attempts=attempts
        ),
    }


def test_obstore_reader_coalesces_and_matches_fuse(cloud, monkeypatch):
    import obstore

    asked, preads = [], []
    real_get, real_pread = obstore.get_ranges, os.pread
    monkeypatch.setattr(
        obstore,
        "get_ranges",
        lambda store, key, **kw: asked.append(
            (key, list(zip(kw["starts"], kw["ends"])))
        )
        or real_get(store, key, **kw),
    )
    monkeypatch.setattr(
        os, "pread", lambda fd, n, at: preads.append((at, n)) or real_pread(fd, n, at)
    )
    regions = (
        [Region("pack.bin", 100 * i, 100) for i in range(24)]  # touching: one span
        + [Region("pack.bin", 4000 + 700 * i, 200) for i in range(5)]  # apart: five
        + [Region("pack.bin", 4100, 50), Region("pack.bin", 4750, 100)]  # inside those
        + [Region("pack.bin", 9000, 0)]  # empty: no request
        + [Region("other.bin", 0, 64), Region("other.bin", 64, 64)]  # one span
        + [Region("b.bin", 10, 20, "external")]  # under the base root
        + [Region(str(cloud.vol / "base" / "b.bin"), 40, 8)]  # absolute, under it too
    )
    readers = _readers(cloud)
    over_the_mount = readers["fuse"].read(regions)
    asked.clear()
    direct = readers["obstore"].read(regions)
    want_file = {"pack.bin": cloud.pack, "other.bin": cloud.pack[::-1]}
    base = cloud.pack[500:]
    want = [
        (want_file.get(r.file) or base)[r.offset : r.offset + r.length] for r in regions
    ]
    assert direct == over_the_mount == want
    spans = sorted(span for _, ss in asked for span in ss)
    # 36 ranges (one empty) become 9 spans in 3 requests (one per file): the very reads
    # the FUSE reader makes
    assert (len(regions), len(asked), len(spans)) == (36, 3, 9)
    assert spans == sorted((at, at + n) for at, n in preads)


REFUSED = [
    (
        "a dot-dot escape",
        lambda c: Region("../outside/secret.bin", 0, 4),
        "resolves outside",
    ),
    (
        "an absolute path outside",
        lambda c: Region(str(c.vol / "outside" / "secret.bin"), 0, 4),
        "resolves outside",
    ),
    (
        "a sibling named like the root",
        lambda c: Region(str(c.vol / "store2" / "x.bin"), 0, 4),
        "resolves outside",
    ),
    (
        "a sibling by dot-dot",
        lambda c: Region("../store2/x.bin", 0, 4),
        "resolves outside",
    ),
    (
        "an external file out of the base",
        lambda c: Region("../store/x.bin", 0, 4, "external"),
        "resolves outside",
    ),
    ("a missing file", lambda c: Region("gone.bin", 0, 4), "missing"),
    ("a short file", lambda c: Region("x.bin", 90, 20), "ends before byte"),
    ("a negative offset", lambda c: Region("x.bin", -1, 4), "negative"),
    ("no file name", lambda c: Region("", 0, 4), "not a usable path"),
    (
        "an external file that changed",
        lambda c: Region("b.bin", 0, 8, "external", "1:1"),
        "changed outside GeoBrix",
    ),
]


@pytest.mark.parametrize("kind", ["fuse", "obstore"])
@pytest.mark.parametrize("why, region, text", REFUSED, ids=[r[0] for r in REFUSED])
def test_both_readers_refuse_what_the_manifest_must_not_say(
    cloud, monkeypatch, kind, why, region, text
):
    import obstore

    asked = []
    real_get = obstore.get_ranges
    monkeypatch.setattr(
        obstore,
        "get_ranges",
        lambda store, key, **kw: asked.append(key) or real_get(store, key, **kw),
    )
    with pytest.raises(ZarrStoreDrift) as e:
        _readers(cloud)[kind].read([region(cloud)])
    assert text in str(e.value) and "Fix:" in str(e.value)
    if text == "resolves outside":
        assert asked == []  # nothing outside the store was ever requested


def test_an_external_files_identity_is_checked_once_and_matches_the_mount(
    cloud, monkeypatch
):
    import obstore

    heads = []
    real_head = obstore.head
    monkeypatch.setattr(
        obstore, "head", lambda store, key: heads.append(key) or real_head(store, key)
    )
    st = (cloud.vol / "base" / "b.bin").stat()
    here = f"{st.st_mtime_ns // 1_000_000}:{st.st_size}"
    regions = [
        Region("b.bin", 0, 8, "external", here),
        Region("b.bin", 20, 8, "external", here),
    ]
    for kind, reader in _readers(cloud).items():
        assert reader.read(regions) == [cloud.pack[500:508], cloud.pack[520:528]], kind
        reader.read(regions)
    assert heads == ["base/b.bin"]  # one head, for one reader, for two reads


def test_request_failures_are_retried_and_never_echo_the_credentials(
    cloud, monkeypatch
):
    import obstore
    from obstore import exceptions as ox

    sleeps = []
    monkeypatch.setattr(time, "sleep", sleeps.append)
    real_get = obstore.get_ranges
    failures = [
        ox.GenericError(f"error sending https://a.blob/c/p?sv=1&sig={SENTINEL}&se=2"),
        ox.GenericError(f"again, with the key {SENTINEL}-ak"),
    ]

    def flaky(store, key, **kw):
        if failures:
            raise failures.pop(0)
        return real_get(store, key, **kw)

    monkeypatch.setattr(obstore, "get_ranges", flaky)
    reader = reader_for(cloud.cfg, str(cloud.vol / "store"))
    assert reader.read([Region("x.bin", 0, 3)]) == [bytes([0, 1, 2])]
    assert len(sleeps) == 2  # the helper the FUSE reader uses: 2 failures, 2 waits

    def always(store, key, **kw):
        raise ox.GenericError(f"sig={SENTINEL} {SENTINEL}-sk")

    monkeypatch.setattr(obstore, "get_ranges", always)
    with pytest.raises(OSError) as e:
        _readers(cloud, attempts=2)["obstore"].read([Region("x.bin", 0, 3)])
    assert SENTINEL not in str(e.value) and "***" in str(e.value)

    def denied(store, key, **kw):
        raise ox.PermissionDeniedError(f"403 {SENTINEL}")

    monkeypatch.setattr(obstore, "get_ranges", denied)
    before = len(sleeps)
    with pytest.raises(ZarrAccessUnavailable) as e:
        _readers(cloud)["obstore"].read([Region("x.bin", 0, 3)])
    assert SENTINEL not in str(e.value) and "credentials expired" in e.value.fix
    assert len(sleeps) == before  # a refusal is not a transient miss


def _fail_after(monkeypatch, n, make):
    """obstore's ``get_ranges``, ``put`` and ``head`` work ``n`` times, then fail with
    ``make()``: a store whose credentials expire. ``calls["n"]`` counts every request.
    """
    import obstore

    calls = {"n": 0}
    for name in ("get_ranges", "put", "head"):

        def wrapped(*args, _real=getattr(obstore, name), **kwargs):
            calls["n"] += 1
            if calls["n"] > n:
                raise make()
            return _real(*args, **kwargs)

        monkeypatch.setattr(obstore, name, wrapped)
    return calls


def _s3_expired():
    from obstore import exceptions as ox

    return ox.GenericError(
        "Generic S3 error: Client error with status 400 Bad Request: "
        "<Error><Code>ExpiredToken</Code><Message>The provided token has expired.</Message>"
    )


def _forbidden():
    from obstore import exceptions as ox

    return ox.PermissionDeniedError("403 Forbidden")


@pytest.mark.parametrize("mode", ["auto", "direct"])
@pytest.mark.parametrize(
    "op, refusal",
    [
        ("read", _s3_expired),
        ("read", _forbidden),
        ("put", _s3_expired),
        ("stat", _s3_expired),
    ],
    ids=["read-S3-400", "read-403", "put", "stat"],
)
def test_refused_credentials_are_not_retried_and_auto_carries_on_through_the_mount(
    cloud, monkeypatch, op, refusal, mode
):
    """The credentials expire after a request: nothing waits and retries; "auto" does the rest
    of the work through the mount (a pack is written there, a read comes from there), "direct"
    raises with a Fix that names the expiry."""
    sleeps = []
    monkeypatch.setattr(time, "sleep", sleeps.append)
    vol = cloud.vol
    mount = vol / "mnt"  # a pack written through the mount lands here, not in the cloud
    cfg = dataclasses.replace(
        cloud.cfg,
        fallback=mode == "auto",
        volume_path=str(mount) if op != "read" else str(vol),
    )
    root, base = str(vol / "store"), str(vol / "base")
    expired = "credentials expired mid-job; use access='auto' for long jobs"
    if op == "read":
        reader = reader_for(cfg, root, base)
        calls = _fail_after(monkeypatch, 1, refusal)
        assert reader.read([Region("pack.bin", 0, 10)]) == [
            cloud.pack[:10]
        ]  # still good
        if mode == "auto":
            assert reader.read([Region("pack.bin", 20, 10)]) == [cloud.pack[20:30]]
            assert reader.read([Region("pack.bin", 30, 10)]) == [cloud.pack[30:40]]
            assert calls["n"] == 2  # one good, one refused, and no request after that
            assert cfg.refused and isinstance(
                reader_for(cfg, root, base), FuseRangeReader
            )
        else:
            with pytest.raises(ZarrAccessUnavailable) as e:
                reader.read([Region("pack.bin", 20, 10)])
            assert e.value.fix == expired and SENTINEL not in str(e.value)
            assert calls["n"] == 2
    elif op == "put":
        from databricks.labs.gbx.zarr._pack import PackWriter

        calls = _fail_after(monkeypatch, 0, refusal)
        packs = mount / "store"
        writer = PackWriter(
            str(packs / "data" / "t2m" / "w1"), reader_root=str(packs), access=cfg
        )
        writer.add(b"x" * 100)
        if mode == "auto":
            rel = writer.close()  # rewritten through the mount
            assert (packs / rel).read_bytes() == b"x" * 100
            assert not (vol / "store" / "data").exists()  # and not in the cloud
        else:
            with pytest.raises(ZarrAccessUnavailable) as e:
                writer.close()
            assert e.value.fix == expired
            assert (
                not (packs / "data").exists() and not (vol / "store" / "data").exists()
            )
        assert calls["n"] == 1
    else:
        from databricks.labs.gbx.zarr._pack import require_packs

        packs = mount / "store" / "data" / "t2m" / "w1"
        packs.mkdir(parents=True)
        (packs / "p.zpack").write_bytes(b"x")
        calls = _fail_after(monkeypatch, 0, refusal)
        args = (str(mount / "store"), ["data/t2m/w1/p.zpack"])
        if mode == "auto":
            require_packs(*args, what="write", access=cfg)  # the mount has it
        else:
            with pytest.raises(ZarrAccessUnavailable) as e:
                require_packs(*args, what="write", access=cfg)
            assert e.value.fix == expired
        assert calls["n"] == 1
    assert sleeps == []


def test_a_direct_read_compares_the_object_time_at_whole_seconds(cloud, monkeypatch):
    """A registration took the identity from a listing that may carry milliseconds; an object
    store answers to the second. A direct read compares at the second both share (a rewrite
    of the same size within the second goes unseen there; the mount sees it)."""
    from datetime import datetime, timezone

    import obstore

    size = len(cloud.pack[500:])
    at = datetime(
        2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc
    )  # an HTTP header's second
    ms = int(at.timestamp()) * 1000
    real_head = obstore.head
    monkeypatch.setattr(
        obstore,
        "head",
        lambda store, key: {**real_head(store, key), "last_modified": at},
    )

    def read(identity):
        region = Region("b.bin", 0, 8, "external", identity)
        return _readers(cloud)["obstore"].read([region])

    for recorded in (ms, ms + 123, ms + 999):  # no false drift on a millisecond listing
        assert read(f"{recorded}:{size}") == [cloud.pack[500:508]]
    for stale in (
        f"{ms - 1000}:{size}",
        f"{ms + 1000}:{size}",
        f"{ms + 123}:{size + 1}",
    ):
        with pytest.raises(ZarrStoreDrift, match="changed outside GeoBrix"):
            read(stale)


def test_a_pack_is_one_put_and_the_commit_looks_for_the_object_not_the_mount(
    cloud, monkeypatch
):
    import obstore

    from databricks.labs.gbx.zarr._pack import PackWriter, require_packs
    from databricks.labs.gbx.zarr.errors import ZarrError

    monkeypatch.setattr(time, "sleep", lambda s: None)
    puts, real_put = [], obstore.put
    monkeypatch.setattr(
        obstore,
        "put",
        lambda store, key, f, **kw: puts.append(key) or real_put(store, key, f, **kw),
    )
    root = cloud.vol / "store"
    writer = PackWriter(
        str(root / "data" / "t2m" / "w1"), reader_root=str(root), access=cloud.cfg
    )
    for i in range(5):
        writer.add(bytes([i]) * 1000)
    rel = writer.close()
    assert len(puts) == 1 and rel == "/".join(
        ["data", "t2m", "w1", puts[0].rpartition("/")[2]]
    )
    assert (root / rel).read_bytes() == b"".join(bytes([i]) * 1000 for i in range(5))
    require_packs(str(root), [rel], what="write", access=cloud.cfg)
    with pytest.raises(ZarrError, match="1 pack file"):
        require_packs(
            str(root), [rel, "data/t2m/w1/gone.zpack"], what="write", access=cloud.cfg
        )
    # the mount is not consulted: a path the mount does not have, the object store does
    away = dataclasses.replace(
        cloud.cfg, volume_path=str(cloud.vol / "mnt"), volume_url=f"file://{cloud.vol}"
    )
    require_packs(
        str(cloud.vol / "mnt"),
        ["store/data/t2m/w1/" + rel.rpartition("/")[2]],
        what="write",
        access=away,
    )


# --- direct access: the credentials stay in the task closure ---------------------------------


def test_creds_not_in_plan_json(monkeypatch, era5_v3_sharded):
    import json
    from test.zarr import _helpers as H

    from pyspark import cloudpickle

    from databricks.labs.gbx.zarr import _read, _tiles

    store = era5_v3_sharded
    url = f"file://{store.parent}"
    direct = AccessConfig(
        "direct", url, {"access_key_id": SENTINEL}, "aws", str(store.parent), url
    )
    assert SENTINEL not in repr(direct) + str(direct) + f"{direct!r}"
    assert "access_key_id" in repr(direct)  # the names show, the values never do

    src = H.registered_source(monkeypatch, store, origin="gbx")
    _, names, units, cfg = H.plan(src, variables=["t2m"], access=direct)
    seen = [u.to_json() for u in units] + [repr(cfg), repr(units)]
    plan = _read._plan_tile_set(
        None, src, names, True, None, {"time": slice(0, 1)}, True, None, None, None
    )[0]
    seen.append(json.dumps(_tiles.tile_rows(plan, range(plan.n_tiles)), default=str))
    rows = H.read_blocks(src, variables=["t2m"], access=direct)
    seen.append(json.dumps([{k: v for k, v in r.items() if k != "data"} for r in rows]))
    assert all(SENTINEL not in text for text in seen)
    # the holder keeps them out of the ways a config is dumped: asdict, vars, json, a round trip
    unpickled = cloudpickle.loads(cloudpickle.dumps(direct))
    for dumped in (
        dataclasses.asdict(direct),
        vars(direct),
        [direct, unpickled],
        {"cfg": cfg},
    ):
        assert SENTINEL not in repr(dumped) + str(dumped)
    with pytest.raises(TypeError):
        json.dumps(dataclasses.asdict(direct))
    # the closure is the one place they travel: a pickled config still signs requests
    assert unpickled.creds == direct.creds and unpickled.creds.reveal() == {
        "access_key_id": SENTINEL
    }


@pytest.mark.parametrize("what", ["blocks", "table", "sample", "tiles"])
def test_every_reader_reads_the_same_through_obstore(
    monkeypatch, era5_v3_sharded, what
):
    """The readers of one family, each with a direct ``AccessConfig`` in its task config:
    the bytes obstore serves are the bytes the mount serves, and obstore was asked."""
    import struct
    from test.zarr import _helpers as H

    import obstore

    from databricks.labs.gbx.zarr import _read, _tiles

    asked, real_get = [], obstore.get_ranges
    monkeypatch.setattr(
        obstore,
        "get_ranges",
        lambda store, key, **kw: asked.append(key) or real_get(store, key, **kw),
    )
    store = era5_v3_sharded
    url = f"file://{store.parent}"
    direct = AccessConfig("direct", url, {}, "aws", str(store.parent), url)
    src = H.registered_source(monkeypatch, store, origin="gbx")
    isel = {"time": slice(0, 12)}
    if what == "blocks":
        got = H.read_blocks(src, isel=isel, access=direct)
        want = H.read_blocks(src, isel=isel)
    elif what == "table":
        got = H.read_table(src, isel=isel, access=direct)[1].to_pylist()
        want = H.read_table(src, isel=isel)[1].to_pylist()
    elif what == "sample":
        point = [struct.pack("<BIdd", 1, 1, 10.0, 50.0)]
        got = H.sample_table(src, point, access=direct)[1].to_pylist()
        want = H.sample_table(src, point)[1].to_pylist()
    else:
        from databricks.labs.gbx.pyrx.core.open_tile import materialize_to_bytes
        from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

        (plan,) = _read._plan_tile_set(
            None, src, ["t2m"], True, None, {"time": 0}, True, None, None, None
        )

        def materialized(access):
            with _tiles.tile_access(access):
                return [
                    materialize_to_bytes(VirtualTile.from_row(r)).to_row()["raster"]
                    for r in _tiles.tile_rows(plan, range(plan.n_tiles))
                ]

        want = materialized(None)
        asked.clear()  # the mount read above made no request; this is the count that matters
        got = materialized(direct)
    assert len(got) > 0 and got == want
    assert asked and all(key.startswith("era5_v3_sharded.zarr/") for key in asked)


# --- direct access: each function resolves it once and hands it to its tasks -------------------


class _Stop(Exception):
    """Ends a call at the point a test has looked at."""


def _recording(monkeypatch, module, cfg):
    calls = []

    def resolve(spark, path, *, access, operation, also=()):
        calls.append((path, access, operation, list(also)))
        return cfg

    monkeypatch.setattr(module, "resolve_access", resolve)
    return calls


@pytest.fixture
def direct_cfg():
    return AccessConfig(
        "direct", "file:///v", {"k": SENTINEL}, "aws", "/v", "file:///v"
    )


def test_read_resolves_access_for_a_table_once_and_gives_it_to_its_tasks(
    monkeypatch, tmp_path, era5_v3_sharded, direct_cfg
):
    from test.zarr import _helpers as H

    from databricks.labs.gbx.zarr import _read

    sent = []
    monkeypatch.setattr(
        _read,
        "_work_frame",
        lambda spark, units: SimpleNamespace(
            mapInArrow=lambda fn, schema: sent.append(fn)
        ),
    )
    monkeypatch.setattr(_read, "_task_function", lambda cfg: cfg)  # the config itself
    monkeypatch.setattr(
        _read,
        "_tiles_frame",
        lambda spark, plans, virtual, access: sent.append((virtual, access)),
    )
    calls = _recording(monkeypatch, _read, direct_cfg)
    for source, root, base in (
        (H.registered_source(monkeypatch, era5_v3_sharded, origin="gbx"), None, []),
        (
            H.registered_source(monkeypatch, era5_v3_sharded, overlay=tmp_path),
            str(tmp_path),
            [str(era5_v3_sharded)],
        ),
    ):
        monkeypatch.setattr(_read, "_open_source", lambda *a, src=source, **k: src)
        calls.clear(), sent.clear()
        _read.gbx_zarr_read(None, "db.t", variables=["t2m"], isel={"time": 0})
        assert calls == [(root or source.root, "auto", "READ_VOLUME", base)]
        assert sent[0].access is direct_cfg  # the task config carries it
        # materialized tiles read in their tasks, with it; virtual tiles read nothing now
        calls.clear(), sent.clear()
        _read.gbx_zarr_read(
            None,
            "db.t",
            output="tiles",
            variables=["t2m"],
            isel={"time": 0},
            virtual_tiles=False,
        )
        assert len(calls) == 1 and sent == [(False, direct_cfg)]
        calls.clear(), sent.clear()
        _read.gbx_zarr_read(
            None, "db.t", output="tiles", variables=["t2m"], isel={"time": 0}
        )
        assert calls == [] and sent == [(True, _access.FUSE)]
    with pytest.raises(ZarrAccessUnavailable, match="virtual tiles") as e:
        _read.gbx_zarr_read(None, "db.t", output="tiles", access="direct")
    assert "virtual_tiles=False" in e.value.fix


def test_sample_resolves_access_for_its_source_and_a_plain_store_has_none(
    monkeypatch, era5_v3_sharded, direct_cfg
):
    from test.zarr import _helpers as H

    from pyspark.sql import types as T

    from databricks.labs.gbx.zarr import _read, _sample

    src = H.registered_source(monkeypatch, era5_v3_sharded, origin="gbx")
    monkeypatch.setattr(_sample, "_open_source", lambda *a, **k: src)
    calls = _recording(monkeypatch, _read, direct_cfg)

    def stop(points_df, cfg):
        raise _Stop(cfg)

    monkeypatch.setattr(_sample, "_spark_schema", stop)
    points = SimpleNamespace(
        columns=["geom"], schema={"geom": SimpleNamespace(dataType=T.BinaryType())}
    )
    with pytest.raises(_Stop) as e:
        _sample.gbx_zarr_sample(None, points, "db.t", access="direct")
    assert calls == [(src.root, "direct", "READ_VOLUME", [])]
    assert e.value.args[0].read.access is direct_cfg
    with pytest.raises(ZarrAccessUnavailable, match="plain store"):
        _sample.gbx_zarr_sample(None, points, str(era5_v3_sharded), access="direct")
    assert len(calls) == 1  # a plain store never asks


# --- direct access: credentials ride only in a small closure ------------------------------------


def test_credentials_ride_only_in_a_small_closure():
    """A task function that pickles above Spark's broadcast threshold is written to local disk
    by PySpark. So with a closure over the threshold "auto" builds it again for the mount (no
    credentials in it) and "direct" raises; the threshold is Spark's own conf."""
    from pyspark import cloudpickle

    direct = AccessConfig(
        "direct", "file:///v", {"k": SENTINEL}, "aws", "/v", "file:///v", fallback=True
    )

    def build(pad):
        def make(access):
            blob = b"x" * pad

            def run(batches):
                return blob, access

            return run

        return make

    def spark(value):
        def get(key, default=None):
            if isinstance(value, Exception):
                raise value
            return value

        return SimpleNamespace(conf=SimpleNamespace(get=get))

    def carries(fn):
        return SENTINEL.encode() in cloudpickle.dumps(fn)

    fn, used = _access.bounded_closure(spark("2m"), direct, build(1 << 20))
    assert used is direct and carries(fn)  # 1 MiB under a 2 MiB threshold
    fn, used = _access.bounded_closure(spark("2m"), direct, build(3 << 20))
    assert used is _access.FUSE and not carries(fn)  # auto: the mount, no credentials
    for threshold in ("1048576", None, RuntimeError("a client that will not say")):
        fn, used = _access.bounded_closure(spark(threshold), direct, build(2 << 20))
        assert used is _access.FUSE and not carries(fn)  # the default is 1 MiB
    with pytest.raises(ZarrAccessUnavailable) as e:
        _access.bounded_closure(
            spark("2m"), dataclasses.replace(direct, fallback=False), build(3 << 20)
        )
    assert "smaller selection" in e.value.fix and SENTINEL not in str(e.value)
    # nothing is measured for the mount, which holds no credentials
    fn, used = _access.bounded_closure(spark("2m"), _access.FUSE, build(3 << 20))
    assert used is _access.FUSE and not carries(fn)


# --- direct access, end to end on Spark -------------------------------------------------------


def test_a_direct_write_and_read_round_trip_on_spark(
    spark_delta, tmp_path, monkeypatch
):
    """Spark-run tasks (worker processes) put the packs and read them back through obstore,
    against a ``file://`` "cloud" that is not the mount: the mount never sees a pack."""
    from test.zarr import _helpers as H

    import numpy as np
    import pytest as _pytest  # noqa: F401

    from databricks.labs.gbx.zarr import _read, _write
    from databricks.labs.gbx.zarr._testing import era5_like_dataset
    from databricks.labs.gbx.zarr._write import gbx_zarr_write

    ds = era5_like_dataset().isel(
        time=slice(0, 24), latitude=slice(0, 10), longitude=slice(0, 12)
    )
    ds["t2m"].encoding["_FillValue"] = np.float32("nan")
    chunks = {"time": 12, "latitude": 5, "longitude": 6}
    shards = {"time": 24, "latitude": 10, "longitude": 12}
    table = f"gbx_zarr_access_{uuid_hex()}"
    mount, cloud_dir = tmp_path / "mount", tmp_path / "cloud"
    cloud_dir.mkdir()
    url = f"file://{cloud_dir}"
    direct = AccessConfig(
        "direct", url, {"access_key_id": SENTINEL}, "aws", str(mount), url
    )
    try:
        # where direct access cannot be had, an explicit "direct" writes nothing at all
        with pytest.raises(ZarrAccessUnavailable):
            gbx_zarr_write(
                ds,
                table,
                root=str(tmp_path / "never"),
                spark=spark_delta,
                access="direct",
            )
        assert not (tmp_path / "never").exists()
        assert not spark_delta.catalog.tableExists(table)

        calls = _recording(monkeypatch, _write, direct)
        stages = {}
        for name in ("explode_function", "assemble_function"):
            real = getattr(_write, name)
            monkeypatch.setattr(
                _write,
                name,
                lambda cfg, _real=real, _name=name: (
                    stages.__setitem__(_name, cfg.access) or _real(cfg)
                ),
            )
        version = gbx_zarr_write(
            ds,
            table,
            root=str(mount / "store"),
            spark=spark_delta,
            chunks=chunks,
            shards=shards,
            access="direct",
            optimize=True,  # the fold reads and puts through obstore as well
        )
        assert calls == [(str(mount / "store"), "direct", "WRITE_VOLUME", [])]
        # the stage that touches no storage never holds the credentials
        assert stages == {"explode_function": None, "assemble_function": direct}
        assert version == 1  # the write's own; the fold it asked for is the next one
        packs = sorted((cloud_dir / "store" / "data").rglob("*.zpack"))
        assert packs and not (mount / "store" / "data").exists()  # the mount saw none
        assert (
            spark_delta.sql(
                f"SELECT count(*) FROM {table} WHERE kind = 'shard' AND tag IS NULL"
            ).first()[0]
            == 2
        )  # the fold made t2m's and u10's one shard each

        # The driver plans from the coordinate arrays, which it reads through the mount: let
        # the mount show those (and only those), so the tasks can only have read t2m by obstore.
        for name in ("time", "latitude", "longitude"):
            shutil.copytree(
                cloud_dir / "store" / "data" / name, mount / "store" / "data" / name
            )
        _recording(monkeypatch, _read, direct)
        df = _read.gbx_zarr_read(spark_delta, table, variables=["t2m"], access="direct")
        got, seen = H.assemble(df.collect(), "t2m", ds["t2m"].shape)
        assert seen.all()
        np.testing.assert_array_equal(got, ds["t2m"].values)
        assert SENTINEL not in df._jdf.queryExecution().toString()
    finally:
        spark_delta.sql(f"DROP TABLE IF EXISTS {table}")


def uuid_hex():
    import uuid

    return uuid.uuid4().hex[:8]


def test_a_large_selection_never_puts_credentials_on_local_disk(
    spark_delta, tmp_path, monkeypatch
):
    """A table read of a 150,000-step axis has a task closure of over 1 MiB (the labels ride
    in it). "auto" reads it through the mount and "direct" refuses; either way no file under
    ``spark.local.dir`` holds the token. A control closure of the same size WITH the config in
    it does land the token on disk, so the search can see it."""
    from pathlib import Path

    import numpy as np
    import pandas as pd
    import xarray as xr

    from databricks.labs.gbx.zarr import _read, _write

    n = 150_000
    ds = xr.Dataset(
        {"v": (("time", "y", "x"), np.arange(n * 4, dtype="float32").reshape(n, 2, 2))},
        coords={
            "time": pd.date_range("2000-01-01", periods=n, freq="h"),
            "y": [10.0, 11.0],
            "x": [20.0, 21.0],
        },
    )
    table = f"gbx_zarr_access_big_{uuid_hex()}"
    url = f"file://{tmp_path}"
    direct = AccessConfig(
        "direct", url, {"access_key_id": SENTINEL}, "aws", str(tmp_path), url
    )

    def resolve(spark, path, *, access, operation, also=()):
        return dataclasses.replace(direct, fallback=access == "auto")

    def holders():
        sc = spark_delta.sparkContext
        scratch = Path(sc._temp_dir).parent  # spark-<id>: the files PySpark writes
        roots = [scratch, *scratch.parent.glob("blockmgr-*")]
        found = []
        for root in roots:
            for f in root.rglob("*"):
                try:
                    if f.is_file() and SENTINEL.encode() in f.read_bytes():
                        found.append(str(f))
                except OSError:
                    pass
        return found

    try:
        gbx_write = _write.gbx_zarr_write
        gbx_write(
            ds,
            table,
            root=str(tmp_path / "store"),
            spark=spark_delta,
            chunks={"time": 15_000, "y": 2, "x": 2},
            access="fuse",
        )
        monkeypatch.setattr(_read, "resolve_access", resolve)
        built = []
        real_task = _read._task_function
        monkeypatch.setattr(
            _read,
            "_task_function",
            lambda cfg: built.append(cfg.access) or real_task(cfg),
        )
        df = _read.gbx_zarr_read(
            spark_delta, table, output="table", variables=["v"], access="auto"
        )
        assert df.count() == n * 4
        assert built[0].is_direct and built[-1].mode == "fuse"  # too big: built again
        with pytest.raises(ZarrAccessUnavailable, match="over the"):
            _read.gbx_zarr_read(
                spark_delta, table, output="table", variables=["v"], access="direct"
            )
        assert holders() == []

        blob = b"x" * (2 << 20)

        def control(
            batches,
        ):  # the same size, with the config in it: nothing guards this
            for batch in batches:
                yield batch.select(["id"])
            len(blob), direct

        spark_delta.range(1).mapInArrow(control, "id long").collect()
        assert len(holders()) >= 1
    finally:
        spark_delta.sql(f"DROP TABLE IF EXISTS {table}")
