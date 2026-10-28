"""Tests for examples/tests/stage_sample_to_volume.py (the gbx:data:stage-sample launcher).

The launcher deletes remote files, so the tests pin where it may and may not delete. No
workspace and no real SDK: a stub `databricks.sdk` module hands the launcher an in-memory
Files API (`FakeFiles`) and records every client it builds.
"""

import importlib
import posixpath
import sys
import types
from types import SimpleNamespace

import pytest

from conftest import REPO_ROOT

VOL = "/Volumes/c/s/v"
EXAMPLES = f"{VOL}/geobrix-examples"
DATASET = "global/era5-weather"


class NotFound(Exception):
    """Stands in for databricks.sdk.errors.NotFound."""


class Unauthenticated(Exception):
    """Stands in for databricks.sdk.errors.Unauthenticated (the launcher matches the name)."""


class PermissionDenied(Exception):
    """Stands in for databricks.sdk.errors.PermissionDenied."""


class FakeFiles:
    """The Files API calls the launcher makes, over a dict of path -> bytes.

    `normalize` models a server that resolves `..` in a path (the dangerous case);
    `truncate` drops the last byte of every upload; `upload_error` fails every upload;
    `list_prefix` makes the listing report paths in a form the launcher did not build.
    """

    def __init__(
        self,
        files=None,
        *,
        normalize=False,
        truncate=False,
        upload_error=None,
        list_prefix="",
    ):
        self.files = dict(files or {})
        self.normalize = normalize
        self.truncate = truncate
        self.upload_error = upload_error
        self.list_prefix = list_prefix
        self.uploaded = []
        self.deleted = []

    def _n(self, path):
        return posixpath.normpath(path) if self.normalize else path

    def create_directory(self, path):
        pass

    def upload(self, path, fh, overwrite=False):
        if self.upload_error:
            raise self.upload_error
        data = fh.read()
        self.files[self._n(path)] = data[:-1] if self.truncate else data
        self.uploaded.append(self._n(path))

    def get_metadata(self, path):
        return SimpleNamespace(content_length=len(self.files[self._n(path)]))

    def list_directory_contents(self, directory):
        prefix = self._n(directory).rstrip("/") + "/"
        children = {}
        for path in self.files:
            if path.startswith(prefix):
                name, _, rest = path[len(prefix) :].partition("/")
                children[name] = children.get(name, False) or bool(rest)
        if not children:
            raise NotFound(directory)
        for name, is_dir in sorted(children.items()):
            yield SimpleNamespace(
                name=name,
                path=self.list_prefix + prefix + name,
                is_directory=is_dir,
            )

    def delete(self, path):
        del self.files[self._n(path)]
        self.deleted.append(self._n(path))


@pytest.fixture
def sdk(monkeypatch):
    """A stub databricks.sdk: `state.files` is the remote, `state.clients` the profiles built."""
    state = SimpleNamespace(
        files=FakeFiles(), clients=[], ctor_error=None, me_error=None
    )

    class WorkspaceClient:
        def __init__(self, profile=None, **kwargs):
            if state.ctor_error:
                raise state.ctor_error
            state.clients.append(profile)
            self.files = state.files
            self.current_user = SimpleNamespace(me=self._me)

        @staticmethod
        def _me():
            if state.me_error:
                raise state.me_error

    top = types.ModuleType("databricks.sdk")
    top.WorkspaceClient = WorkspaceClient
    errors = types.ModuleType("databricks.sdk.errors")
    errors.NotFound = NotFound
    monkeypatch.setitem(sys.modules, "databricks.sdk", top)
    monkeypatch.setitem(sys.modules, "databricks.sdk.errors", errors)
    return state


@pytest.fixture
def launcher(monkeypatch):
    monkeypatch.syspath_prepend(str(REPO_ROOT / "examples" / "tests"))
    return importlib.import_module("stage_sample_to_volume")


def _write(root, files):
    for rel, data in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)


LOCAL = {
    f"{DATASET}/era5_like_v3.zarr/zarr.json": b"{v3}",
    f"{DATASET}/era5_like_v3.zarr/t2m/c/0/0/0": b"chunk-a",
    f"{DATASET}/era5_like_v2.zarr/.zgroup": b"{v2}",
    f"{DATASET}/era5_like_v2.zarr/t2m/.zarray": b"{arr}",
    f"{DATASET}/era5_like_v2.zarr/t2m/0.0.0": b"chunk-b",
    f"{DATASET}/era5_like_v2.zarr/.DS_Store": b"finder",
    "nyc/boroughs/nyc_boroughs.geojson": b"minimal clip",
}
# What else is on the Volume: another dataset, a full-size file the clip would replace, and
# the artifacts the wheel and JAR stagers keep beside the bundle.
ELSEWHERE = {
    f"{EXAMPLES}/nyc/boroughs/nyc_boroughs.geojson": b"FULL SIZE",
    f"{EXAMPLES}/nyc/sentinel2/nyc_sentinel2_red.tif": b"FULL SIZE",
    f"{EXAMPLES}/london/boroughs/london_boroughs.geojson": b"FULL SIZE",
    f"{VOL}/geobrix-0.6.0-py3-none-any.whl": b"wheel",
    f"{VOL}/geobrix-0.6.0-jar-with-dependencies.jar": b"jar",
    f"{VOL}/other-dataset/keep.txt": b"keep",
}


@pytest.fixture
def src_root(tmp_path):
    root = tmp_path / "geobrix-examples"
    _write(root, LOCAL)
    (tmp_path / "a").mkdir()
    return root


def run(launcher, src_root, *args):
    return launcher.main(["--src-root", str(src_root), "--volume-root", VOL, *args])


BAD_PATHS = [
    "..",
    "../",
    ".",
    "",
    "/",
    "/etc",
    "a/../..",
    "nyc/../..",
    "global/./era5-weather",
    "a//b",
]


@pytest.mark.parametrize("path", BAD_PATHS)
def test_a_bad_path_is_refused_before_any_client_or_delete(
    launcher, sdk, src_root, path
):
    sdk.files = FakeFiles(ELSEWHERE, normalize=True)

    rc = run(launcher, src_root, "--profile", "p", "--path", path)

    assert rc == 2
    assert sdk.clients == [] and sdk.files.uploaded == [] and sdk.files.deleted == []
    assert set(sdk.files.files) == set(ELSEWHERE)


def test_a_bad_path_is_refused_on_a_dry_run_too(launcher, sdk, src_root, capsys):
    assert run(launcher, src_root, "--dry-run", "--path", "..") == 2
    assert "would upload" not in capsys.readouterr().out


@pytest.mark.parametrize(
    "volume_root", ["", "/Volumes/c", "rel/v", "/Volumes/c/s/../x"]
)
def test_a_bad_volume_root_is_refused(launcher, sdk, src_root, volume_root):
    rc = launcher.main(
        ["--src-root", str(src_root), "--volume-root", volume_root, "--profile", "p"]
    )

    assert rc == 2 and sdk.clients == []


def test_the_prune_stays_inside_the_zarr_stores_it_uploads(launcher, sdk, src_root):
    store3 = f"{EXAMPLES}/{DATASET}/era5_like_v3.zarr"
    store2 = f"{EXAMPLES}/{DATASET}/era5_like_v2.zarr"
    stale = {
        f"{store3}/t2m/c/9/9/9": b"old",  # a chunk of an older layout
        f"{store2}/t2m/9.9.9": b"old",
    }
    survivors = {
        f"{EXAMPLES}/{DATASET}/notes.txt": b"not a store file",
        f"{EXAMPLES}/{DATASET}/older_store.zarr/zarr.json": b"a store not uploaded",
        **ELSEWHERE,
    }
    sdk.files = FakeFiles({**stale, **survivors})

    rc = run(launcher, src_root, "--profile", "p")

    assert rc == 0 and sdk.clients == ["p"]
    assert sorted(sdk.files.deleted) == sorted(stale)
    assert all(path in sdk.files.files for path in survivors)
    assert sdk.files.files[f"{store3}/t2m/c/0/0/0"] == b"chunk-a"
    assert f"{store2}/.DS_Store" not in sdk.files.files  # Finder litter is not data
    assert f"{store2}/.zgroup" in sdk.files.files  # dotfiles of a store are


def test_a_dataset_that_is_not_a_store_is_uploaded_and_never_pruned(
    launcher, sdk, src_root
):
    sdk.files = FakeFiles(ELSEWHERE)

    rc = run(launcher, src_root, "--profile", "p", "--path", "nyc")

    assert rc == 0 and sdk.files.deleted == []
    assert (
        sdk.files.files[f"{EXAMPLES}/nyc/boroughs/nyc_boroughs.geojson"]
        == b"minimal clip"
    )
    assert f"{EXAMPLES}/nyc/sentinel2/nyc_sentinel2_red.tif" in sdk.files.files


def test_dry_run_and_a_missing_profile_build_no_client(launcher, sdk, src_root, capsys):
    assert run(launcher, src_root, "--dry-run") == 0
    assert run(launcher, src_root) == 2

    assert sdk.clients == [] and sdk.files.uploaded == []
    assert "would upload era5_like_v3.zarr/zarr.json" in capsys.readouterr().out


@pytest.mark.parametrize(
    "broken", [{"truncate": True}, {"upload_error": OSError("disk full")}]
)
def test_a_failed_upload_stops_before_any_delete(
    launcher, sdk, src_root, capsys, broken
):
    stale = f"{EXAMPLES}/{DATASET}/era5_like_v3.zarr/t2m/c/9/9/9"
    sdk.files = FakeFiles({stale: b"old"}, **broken)

    rc = run(launcher, src_root, "--profile", "p")

    assert rc == 1 and sdk.files.deleted == [] and stale in sdk.files.files
    assert "Traceback" not in capsys.readouterr().err


def test_a_listing_that_misses_the_uploaded_files_stops_before_any_delete(
    launcher, sdk, src_root
):
    stale = f"{EXAMPLES}/{DATASET}/era5_like_v3.zarr/t2m/c/9/9/9"
    sdk.files = FakeFiles({stale: b"old"}, list_prefix="dbfs:")

    rc = run(launcher, src_root, "--profile", "p")

    assert rc == 1 and sdk.files.deleted == []


@pytest.mark.parametrize(
    "where, error, code",
    [
        ("ctor_error", ValueError("cannot configure default credentials"), 3),
        ("me_error", Unauthenticated("401"), 3),
        ("me_error", PermissionDenied("403"), 3),
        ("me_error", ConnectionError("no route"), 4),
    ],
)
def test_a_client_error_stops_with_its_own_exit_code(
    launcher, sdk, src_root, capsys, where, error, code
):
    setattr(sdk, where, error)

    assert run(launcher, src_root, "--profile", "p") == code
    assert sdk.files.uploaded == [] and sdk.files.deleted == []
    assert type(error).__name__ in capsys.readouterr().err
