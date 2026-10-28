"""Range readers: how the store gets the bytes of a ``Region``, and where its packs go.

``FuseRangeReader`` reads files through the local file system, which is how a
Unity Catalog Volume appears on a cluster (``/Volumes/...``). Blocking, so callers on
an event loop run it in a thread. It opens only files under the store's roots: a
manifest is data, and a row must not be able to name ``/etc/passwd``. The name is checked
before the file is opened and, on Linux, the opened descriptor is checked again.

``resolve_access`` decides, once per ``gbx_zarr_*`` call and on the driver, whether the
tasks read and write the Volume through the file system or straight against its cloud
storage. For direct access it asks Unity Catalog for temporary credentials for the Volume
(``temporary-volume-credentials``), which only dedicated compute is allowed to get. The
credentials ride in the task closure inside an ``AccessConfig`` and nowhere else: not in a
DataFrame, a ``Unit``, tile metadata, a log line or an error message. ``ObstoreRangeReader``
then reads byte ranges with obstore under the same containment rules as the FUSE reader,
and ``put_pack`` writes a pack with one ``obstore.put``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import posixpath
import re
import sys
import urllib.error
import urllib.request
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import Protocol

from databricks.labs.gbx.core._retry import _retry_transient
from databricks.labs.gbx.zarr._shard import Region
from databricks.labs.gbx.zarr.errors import ZarrAccessUnavailable, ZarrStoreDrift

__all__ = [
    "RangeReader",
    "FuseRangeReader",
    "ObstoreRangeReader",
    "AccessConfig",
    "FUSE",
    "resolve_access",
    "reader_for",
    "put_pack",
    "stat_for",
    "bounded_closure",
    "closure_size",
    "check_identity",
    "identities_match",
    "MTIME_RESOLUTION_MS",
]

_LOG = logging.getLogger(__name__)

# A transient FUSE miss is worth another try; these will not change by waiting.
_PERMANENT = (PermissionError, IsADirectoryError, NotADirectoryError)
_SYNC_FIX = "run gbx_zarr_sync if the store changed outside GeoBrix; otherwise "
_DRIFT_FIX = (
    "run gbx_zarr_sync(<table>) on the store's manifest table to record the change, "
    "then read again"
)
# How finely a file's modification time is compared with the one recorded when it was
# listed: whole milliseconds. A Unity Catalog Volume lists times through Hadoop and shows
# them through the FUSE mount; if they ever differ below a second, raise this to 1000
# (``same_identity(..., mtime_resolution_ms=1000)``) rather than reading drift into every file.
MTIME_RESOLUTION_MS = 1
_PROC_FD = "/proc/self/fd"
# Where an open descriptor can be traced back to the file it holds (Linux).
_CHECK_DESCRIPTORS = sys.platform.startswith("linux") and os.path.isdir(_PROC_FD)


def identities_match(a: str, b: str, mtime_resolution_ms: int | None = None) -> bool:
    """Whether two identities (``"<mtime_ms>:<size>"``) name the same file, at
    ``mtime_resolution_ms`` (default ``MTIME_RESOLUTION_MS``). Every place that compares
    identities (a read, a sync, a publish's drift check) goes through here, so one constant
    decides."""
    from databricks.labs.gbx.zarr._register import same_identity

    return same_identity(
        a,
        b,
        mtime_resolution_ms=(
            MTIME_RESOLUTION_MS if mtime_resolution_ms is None else mtime_resolution_ms
        ),
    )


def check_identity(
    row: dict, observed: str, mtime_resolution_ms: int | None = None
) -> None:
    """Raise ``ZarrStoreDrift`` unless the file ``observed`` is the one the manifest recorded.

    ``row`` holds the manifest ``file`` and ``identity`` (``"<mtime_ms>:<size>"``);
    ``observed`` is the identity of the file as it is now, in the same form. A row with
    no identity (a file GeoBrix wrote) is never drift.
    """
    expected = row.get("identity")
    if expected is None or identities_match(expected, observed, mtime_resolution_ms):
        return
    raise ZarrStoreDrift(
        f"{row.get('file')} changed outside GeoBrix: the manifest recorded "
        f"{expected} (modification time in ms : size) and the file is now {observed}.",
        fix=_DRIFT_FIX,
    )


class RangeReader(Protocol):
    def read(self, regions: list[Region]) -> list[bytes]:
        """The bytes of each region, in the order given."""
        ...


class _Permanent(Exception):
    """Carries a permanent OS error through ``_retry_transient`` without a retry."""


def _coalesce(
    regions: list[Region], members: list[int]
) -> list[tuple[int, int, list[int]]]:
    """Merge the regions at ``members`` (all of one file) into ``(start, stop, members)``
    spans: ranges that touch or overlap share one read."""
    spans: list[tuple[int, int, list[int]]] = []
    for i in sorted(members, key=lambda i: regions[i].offset):
        r = regions[i]
        if spans and r.offset <= spans[-1][1]:
            start, stop, held = spans[-1]
            spans[-1] = (start, max(stop, r.offset + r.length), held + [i])
        else:
            spans.append((r.offset, r.offset + r.length, [i]))
    return spans


def _held_path(fd: int) -> str:
    """The path of the file an open descriptor holds, as the kernel reports it.

    The link target is already fully resolved, so it is read with one ``readlink`` rather
    than walked with ``realpath`` (an ``lstat`` per path component, each a round trip on a
    FUSE mount).
    """
    return os.readlink(f"{_PROC_FD}/{fd}")


def _within(path: str, root: str) -> bool:
    """``path`` is ``root`` or below it (a whole-component test, not a prefix test)."""
    return os.path.commonpath([path, root]) == root


class _SpanReader:
    """What the range readers share: ``read`` turns regions into per-file spans and back.

    A subclass names where a region lives (``_resolve``, which also enforces that it lies
    under the store's roots) and fetches the spans of one file (``_read_spans``). Regions of
    one file that touch or overlap are fetched as one span; the bytes of each region come
    back in the order the regions were given.
    """

    def _resolve(self, region: Region) -> tuple[str, str]:
        raise NotImplementedError

    def _read_spans(self, path: str, spans, root: str, recorded) -> list[bytes]:
        raise NotImplementedError

    def read(self, regions: list[Region]) -> list[bytes]:
        out: list[bytes] = [b""] * len(regions)
        by_file: dict[str, list[int]] = {}
        root_of: dict[str, str] = (
            {}
        )  # path -> real path of the root it was checked under
        recorded: dict[str, set[str]] = {}  # path -> identities the manifest recorded
        for i, r in enumerate(regions):
            path, root_of[path] = self._resolve(r)
            if r.origin == "external" and r.identity is not None:
                recorded.setdefault(path, set()).add(r.identity)
            if r.offset < 0 or r.length < 0:
                raise ZarrStoreDrift(
                    f"manifest region {r.length} bytes at offset {r.offset} of "
                    f"{r.file!r} is negative",
                    fix="correct the row or run gbx_zarr_sync",
                )
            if r.length:
                by_file.setdefault(path, []).append(i)
        for path, members in by_file.items():
            spans = _coalesce(regions, members)
            for (start, _, held), data in zip(
                spans,
                self._read_spans(path, spans, root_of[path], recorded.get(path, ())),
            ):
                if len(held) == 1 and regions[held[0]].offset == start:
                    out[held[0]] = data
                    continue
                view = memoryview(data)
                for i in held:
                    r = regions[i]
                    out[i] = bytes(view[r.offset - start : r.offset - start + r.length])
        return out


class FuseRangeReader(_SpanReader):
    """Reads regions from files under a root with positional ``os.pread`` calls.

    A relative ``Region.file`` resolves under ``root``, or under ``base_root`` when the
    region's ``origin`` is ``"external"`` (a registered store's own files; ``root`` then
    holds only what GeoBrix wrote), and must stay under that directory. An absolute
    ``file`` must lie under ``root`` or ``base_root``. A ``..`` that leaves the root, a
    symlink that leads out of it, an empty or non-string ``file`` raise
    ``ZarrStoreDrift`` before anything is opened. Without a ``base_root`` external
    files resolve under ``root``.

    Regions of one file that touch or overlap are fetched with one read. A file that is
    missing, or shorter than the manifest says, raises ``ZarrStoreDrift``; other I/O
    errors propagate. UC Volume FUSE mounts can raise ``FileNotFoundError`` transiently
    on a healthy file, so the open and reads of a file are retried ``attempts`` times
    (the same guard the readers use) before a missing file counts as drift. Errors that
    waiting cannot fix are raised at once: a path that names a directory, or runs through
    a file, is drift, and a permission error propagates.

    A region of an external file carries the identity the manifest recorded for it. The
    first time a reader opens such a file it takes the file's modification time and size
    from the open descriptor (once per reader, which is once per task) and raises
    ``ZarrStoreDrift`` if they are not what was recorded: the file was changed, or replaced,
    outside GeoBrix (``check_identity``).

    The name check can lag the file system: the real path of a name is looked up once
    and kept, and a link can be put in the name's place between the check and the open.
    So on Linux the opened descriptor is traced (``/proc/self/fd``) to the file it really
    holds, which must lie under the same real root; if it does not the descriptor is
    closed and ``ZarrStoreDrift`` is raised before a byte is read.
    """

    def __init__(self, root: str, *, base_root: str | None = None, attempts: int = 10):
        self._root = root
        self._base_root = base_root
        self._attempts = attempts
        self._real: dict[str, str] = {}  # path -> os.path.realpath(path)
        self._observed: dict[str, str] = {}  # path -> its identity, taken once

    def _realpath(self, path: str) -> str:
        real = self._real.get(path)
        if real is None:
            real = self._real[path] = os.path.realpath(path)
        return real

    def _resolve(self, region: Region) -> tuple[str, str]:
        """``(path to open, real path of the root it lies under)`` for ``region``, or
        ``ZarrStoreDrift`` if it leaves the roots."""
        file = region.file
        if not isinstance(file, str) or not file or "\0" in file:
            raise ZarrStoreDrift(
                f"manifest file {file!r} is not a usable path",
                fix="correct the row or run gbx_zarr_sync",
            )
        external = region.origin == "external" and self._base_root is not None
        base = os.path.normpath(
            os.path.abspath(self._base_root if external else self._root)
        )
        if os.path.isabs(file):
            cand = os.path.normpath(file)
            roots = [
                os.path.normpath(os.path.abspath(r))
                for r in (self._root, self._base_root)
                if r
            ]
        else:
            cand = os.path.normpath(os.path.join(base, file))
            roots = [base]
        for root in roots:
            real_root = self._realpath(root)
            if _within(cand, root) and _within(self._realpath(cand), real_root):
                return cand, real_root
        raise ZarrStoreDrift(
            f"manifest file {file!r} resolves outside the store root {' or '.join(roots)}",
            fix="correct the row or run gbx_zarr_sync; a manifest may only point at "
            "files under the store root or, for a registered store, its base root",
        )

    def _read_spans(self, path: str, spans, real_root: str, recorded) -> list[bytes]:
        def attempt() -> list[bytes]:
            try:
                return self._pread_all(path, spans, real_root, recorded)
            except _PERMANENT as e:
                raise _Permanent(e) from None

        try:
            return _retry_transient(attempt, attempts=self._attempts)
        except _Permanent as p:
            if isinstance(p.args[0], IsADirectoryError):
                raise ZarrStoreDrift(
                    f"{path} is recorded in the manifest as a file but is a directory",
                    fix=_SYNC_FIX + "restore the file",
                ) from None
            if isinstance(p.args[0], NotADirectoryError):
                raise ZarrStoreDrift(
                    f"{path} is recorded in the manifest, but a part of its path "
                    "is not a directory",
                    fix=_SYNC_FIX + "restore the file",
                ) from None
            raise p.args[0] from None
        except FileNotFoundError:
            raise ZarrStoreDrift(
                f"{path} is recorded in the manifest but missing",
                fix=_SYNC_FIX + "restore the file",
            ) from None

    def _pread_all(self, path: str, spans, real_root: str, recorded) -> list[bytes]:
        fd = os.open(path, os.O_RDONLY)
        try:
            if _CHECK_DESCRIPTORS:
                try:
                    held = _held_path(fd)
                except (
                    OSError
                ) as e:  # fail closed, and do not retry as a transient miss
                    raise ZarrStoreDrift(
                        f"could not tell which file {path} opened ({e})",
                        fix="run gbx_zarr_sync if the store changed outside GeoBrix; "
                        "otherwise check that /proc is readable",
                    ) from None
                # an absolute path is the file the descriptor really holds
                if os.path.isabs(held) and not _within(held, real_root):
                    raise ZarrStoreDrift(
                        f"manifest file {path!r} resolves outside the store root "
                        f"{real_root} (it opened {held!r})",
                        fix="correct the row or run gbx_zarr_sync; a manifest may "
                        "only point at files under the store root or, for a "
                        "registered store, its base root",
                    )
            if recorded:
                observed = self._observed.get(path)
                if observed is None:
                    st = os.fstat(fd)
                    observed = self._observed[path] = (
                        f"{st.st_mtime_ns // 1_000_000}:{st.st_size}"
                    )
                for identity in recorded:
                    check_identity({"file": path, "identity": identity}, observed)
            out = []
            for start, stop, _ in spans:
                want, parts, at = stop - start, [], start
                while want:
                    chunk = os.pread(fd, want, at)
                    if not chunk:
                        raise ZarrStoreDrift(
                            f"{path} ends before byte {stop}, which the manifest "
                            "says it holds",
                            fix=_SYNC_FIX + "the file is truncated",
                        )
                    parts.append(chunk)
                    want -= len(chunk)
                    at += len(chunk)
                out.append(parts[0] if len(parts) == 1 else b"".join(parts))
            return out
        finally:
            os.close(fd)


# --- direct access -----------------------------------------------------------------------

_ACCESS_MODES = ("auto", "fuse", "direct")
_OPERATIONS = ("READ_VOLUME", "WRITE_VOLUME")
_S3_ENDPOINT = "https://s3.amazonaws.com"
_VENDING_PATH = "/api/2.0/unity-catalog/temporary-volume-credentials"
_UNAVAILABLE_FIX = (
    'use access="auto" or "fuse" on this compute; direct access needs dedicated '
    "compute where UC can vend volume credentials"
)
_VOLUME = re.compile(r"^/Volumes/([^/]+)/([^/]+)/([^/]+)(?:/|$)")
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_MS = timedelta(milliseconds=1)
# Errors a storage reports for credentials it will not accept, as text: S3 answers an expired
# session token with a 400 (obstore raises a generic error for it, not a permission error).
_STATUS = re.compile(r"non-2xx status code: (\d{3})")
_AUTH_ERROR = re.compile(
    r"ExpiredToken|InvalidAccessKeyId|InvalidToken|TokenRefreshRequired|"
    r"SignatureDoesNotMatch|AuthenticationFailed|InvalidAuthenticationInfo|"
    r"AuthorizationFailure|AuthorizationPermissionMismatch|UNAUTHENTICATED"
)
# An object store reports a modification time to the second (HTTP ``Last-Modified``) where the
# listing registration took the identity from may carry milliseconds, so a direct read compares
# the time at whole seconds, with the size. A rewrite of the same size within the same second
# goes unseen on a direct read (it is seen over the mount).
_OBJECT_MTIME_MS = 1000
_SAS_SIGNATURE = re.compile(r"(?i)\bsig=[^&\s\"']+")


class Credentials:
    """The keyword arguments of an obstore store (keys, tokens), held so that nothing prints,
    logs or serializes them by accident.

    ``repr``/``str`` show the names only, ``dataclasses.asdict`` and ``vars`` of the config
    that holds it show this object (a ``dict`` would show the values), and it is not
    JSON-serializable. A task closure pickles it whole: that is how a task signs its
    requests. ``reveal()`` is the one way to the values.
    """

    __slots__ = ("_items",)

    def __init__(self, items=None):
        self._items = dict(items or {})

    def reveal(self) -> dict:
        return dict(self._items)

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._items))

    def digest(self) -> str:
        """A short name for this set of credentials that does not reveal them."""
        text = json.dumps(sorted(self._items.items()), default=str)
        return hashlib.sha256(text.encode()).hexdigest()[:16]

    def __bool__(self) -> bool:
        return bool(self._items)

    def __repr__(self) -> str:
        return "<Credentials masked: " + ", ".join(self.names) + ">"

    __str__ = __repr__

    def __eq__(self, other) -> bool:
        return isinstance(other, Credentials) and self._items == other._items

    __hash__ = None  # type: ignore[assignment]

    def __reduce__(self):
        return (Credentials, (self._items,))


@dataclass(frozen=True)
class AccessConfig:
    """How the tasks of one ``gbx_zarr_*`` call reach the files of a store.

    ``mode`` is ``"fuse"`` (the Volume mount) or ``"direct"`` (the Volume's cloud
    storage, through obstore). A direct config holds the Volume's mount path
    (``volume_path``) and cloud location (``volume_url``), ``root_url`` (the cloud URL of
    the path it was resolved for), the ``cloud`` (``"aws"``, ``"azure"`` or ``"gcp"``), the
    S3 ``region`` and ``creds`` (``Credentials``): the keyword arguments of the obstore
    store, which Unity Catalog vended for an hour at most. ``fallback`` is true when the call
    asked for ``"auto"``: a task whose credentials the storage refuses (they expired) carries
    on through the mount instead of failing.

    ``creds`` are secrets. They travel in a (small) task closure and nowhere else, so
    ``repr`` and ``str`` show the names of the fields and never their values; nothing may put
    them in a DataFrame, a log line or an error message.
    """

    mode: str
    root_url: str | None = None
    creds: Credentials | None = field(default=None, repr=False)
    cloud: str | None = None
    volume_path: str | None = None
    volume_url: str | None = None
    fallback: bool = False
    region: str | None = None

    def __post_init__(self):
        if isinstance(self.creds, dict):
            object.__setattr__(self, "creds", Credentials(self.creds))

    def __repr__(self) -> str:
        return (
            f"AccessConfig(mode={self.mode!r}, root_url={self.root_url!r}, "
            f"creds={self.creds!r}, cloud={self.cloud!r}, region={self.region!r}, "
            f"volume_path={self.volume_path!r}, volume_url={self.volume_url!r}, "
            f"fallback={self.fallback!r})"
        )

    @property
    def is_direct(self) -> bool:
        return self.mode == "direct"

    @property
    def refused(self) -> bool:
        """Whether a storage has refused these credentials in this process."""
        return self.creds is not None and self.creds.digest() in _REFUSED

    def key_of(self, path: str) -> str:
        """The object key, relative to the Volume's cloud location, of the file ``path``
        (a mount path under ``volume_path``)."""
        vol = self.volume_path
        norm = os.path.normpath(path) if isinstance(path, str) else ""
        if vol is None or not os.path.isabs(norm) or not _within(norm, vol):
            raise _unavailable(
                f"{path!r} is not under the Volume {vol} that direct access was set up for"
            )
        rel = os.path.relpath(norm, vol).replace(os.sep, "/")
        return "" if rel == "." else rel

    def url_of(self, path: str) -> str:
        """The cloud URL of the file or directory ``path``."""
        key = self.key_of(path)
        return self.volume_url + ("/" + key if key else "")

    def key_of_url(self, url: str) -> str:
        """The object key of a cloud URL under ``volume_url``."""
        base = self.volume_url
        if url.rstrip("/") == base:
            return ""
        if not url.startswith(base + "/"):
            raise _unavailable(
                f"{url} is not under {base}, the cloud location of the Volume"
            )
        return url[len(base) + 1 :].strip("/")


FUSE = AccessConfig("fuse")

# Credentials a storage has refused in this process, by digest. A refusal (expired token, wrong
# key, no permission) does not pass with waiting, so once one is seen every later reader and
# writer of the process using those credentials goes straight to the fallback (or raises).
_REFUSED: set[str] = set()


def _mark_refused(cfg: "AccessConfig") -> None:
    if cfg.creds is not None:
        _REFUSED.add(cfg.creds.digest())


class _Refused(Exception):
    """The storage refused the credentials: expired, wrong, or not allowed. Not transient."""


def _expired(cfg: "AccessConfig") -> ZarrAccessUnavailable:
    return ZarrAccessUnavailable(
        f"the storage refused the credentials Unity Catalog vended for {cfg.volume_path}: "
        "they expired mid-job or were revoked",
        fix="credentials expired mid-job; use access='auto' for long jobs",
    )


def _unavailable(message: str) -> ZarrAccessUnavailable:
    return ZarrAccessUnavailable(message, fix=_UNAVAILABLE_FIX)


class _Unavailable(Exception):
    """Why credentials cannot be vended for a path; the text never holds a credential."""


def _volume_of(path) -> tuple[str, str, str]:
    """``("<catalog>.<schema>.<volume>", "/Volumes/<catalog>/<schema>/<volume>", path)`` of
    a path on a Unity Catalog Volume (``path`` bare and normalized)."""
    from databricks.labs.gbx.ds._listing import to_local_path

    local = (
        os.path.normpath(to_local_path(path)) if isinstance(path, str) and path else ""
    )
    m = _VOLUME.match(local)
    if m is None:
        raise _Unavailable(
            f"{path!r} is not on a Unity Catalog Volume "
            "(/Volumes/<catalog>/<schema>/<volume>/...)"
        )
    return ".".join(m.groups()), "/" + "/".join(("Volumes", *m.groups())), local


@contextmanager
def _sdk_logs_above_debug():
    """Keep the Databricks SDK's loggers above DEBUG while a request is made: its round-trip
    log prints the response body, which for credentials is the keys. The levels come back
    after (loggers the SDK makes during the call inherit the raised parent)."""
    names = ["databricks.sdk"] + [
        n
        for n in list(logging.root.manager.loggerDict)
        if n.startswith("databricks.sdk.")
    ]
    saved = []
    for name in names:
        log = logging.getLogger(name)
        saved.append((log, log.level))
        log.setLevel(max(log.level, logging.INFO))
    try:
        yield
    finally:
        for log, level in saved:
            log.setLevel(level)


def _workspace_client():
    """The Databricks SDK client of this compute. Imported here, so a cluster without the
    SDK (or a host test) still imports the package."""
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def _credentials(response) -> tuple[str, dict]:
    """``(cloud, store keyword arguments)`` of a ``temporary-volume-credentials`` response."""
    if "aws_temp_credentials" in response:
        c = response["aws_temp_credentials"]
        return "aws", {
            "access_key_id": c["access_key_id"],
            "secret_access_key": c["secret_access_key"],
            "session_token": c["session_token"],
        }
    if "azure_user_delegation_sas" in response:
        return "azure", {
            "sas_key": response["azure_user_delegation_sas"]["sas_token"].lstrip("?")
        }
    if "gcp_oauth_token" in response:
        return "gcp", {"bearer_token": response["gcp_oauth_token"]["oauth_token"]}
    raise _Unavailable(
        "Unity Catalog returned credentials of a kind GeoBrix cannot use"
    )


def _vend(path: str, operation: str, also) -> AccessConfig:
    """Credentials for the Volume ``path`` is on, from Unity Catalog. Driver only."""
    name, volume_path, local = _volume_of(path)
    for other in also:
        if other and _volume_of(other)[1] != volume_path:
            raise _Unavailable(
                f"{other!r} is in another Volume than {path!r}, and one set of "
                "credentials covers one Volume"
            )
    try:
        import obstore  # noqa: F401
    except ImportError:
        raise _Unavailable("obstore is not installed") from None
    try:
        client = _workspace_client()
    except ImportError:
        raise _Unavailable("databricks-sdk is not installed") from None
    with _sdk_logs_above_debug():  # the SDK logs response bodies at DEBUG
        volume_id = client.volumes.read(name).volume_id
        response = client.api_client.do(
            "POST",
            _VENDING_PATH,
            body={"volume_id": volume_id, "operation": operation},
        )
    cloud, creds = _credentials(response)
    url = response.get("url") if isinstance(response, dict) else None
    if not isinstance(url, str) or "://" not in url:
        raise _Unavailable("Unity Catalog returned no usable storage location")
    url = url.rstrip("/")
    below = os.path.relpath(local, volume_path).replace(os.sep, "/")
    cfg = AccessConfig(
        "direct",
        url if below == "." else f"{url}/{below}",
        creds,
        cloud,
        volume_path,
        url,
        region=_s3_region(response, url) if cloud == "aws" else None,
    )
    try:
        _make_store(cfg)  # a URL or credential kind obstore will not build fails here
        _canary(cfg, local)
    except Exception as e:
        cause = (
            "the storage refused the credentials"
            if isinstance(e, _Refused)
            else (f"{type(e).__name__}: {_scrub(cfg, str(e))[:200]}")
        )
        raise _Unavailable(
            f"the first request with the credentials failed ({cause})"
        ) from None
    return cfg


def _s3_region(response, url: str) -> str:
    """The region of the bucket the credentials are for: the response's, else
    ``AWS_REGION`` / ``AWS_DEFAULT_REGION``, else the bucket's own, asked of S3. Never a
    silent default (obstore's is us-east-1, which every other region answers with a redirect
    or a signature error)."""
    region = (
        response.get("region")
        or response["aws_temp_credentials"].get("region")
        or os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
    )
    if region:
        return str(region)
    bucket = url.partition("://")[2].partition("/")[0]
    # Asked once per bucket per process, a "no" included: on a network that blocks the
    # question it would otherwise cost its timeout on every vend.
    if bucket not in _REGIONS:
        _REGIONS[bucket] = _discover_region(bucket)
    region = _REGIONS[bucket]
    if not region:
        raise _Unavailable(
            f"the region of the bucket {bucket!r} is not known: the response has none, "
            "AWS_REGION is not set and the bucket did not say"
        )
    return region


# bucket -> its region, or None when S3 did not say
_REGIONS: dict[str, str | None] = {}


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def _discover_region(bucket: str) -> str | None:
    """The region of an S3 bucket from the ``x-amz-bucket-region`` header of an unsigned HEAD
    (S3 sends it on a redirect or a refusal alike), or ``None``."""
    request = urllib.request.Request(f"{_S3_ENDPOINT}/{bucket}", method="HEAD")
    opener = urllib.request.build_opener(_NoRedirect)
    try:
        with opener.open(request, timeout=5) as r:
            return r.headers.get("x-amz-bucket-region")
    except urllib.error.HTTPError as e:  # 301 / 403 carry the header
        return e.headers.get("x-amz-bucket-region")
    except Exception:
        return None


def _canary(cfg: AccessConfig, path: str) -> None:
    """ONE small request with the vended credentials, against the store root: it fails for
    a wrong region or URL, credentials that are not allowed there, or no route to the storage,
    which a vend that merely succeeded cannot tell. The tasks would otherwise all fail where
    the mount works."""
    import obstore

    store = _make_store(cfg)
    with _obstore_errors(cfg):
        next(iter(obstore.list(store, cfg.key_of(path) or None, chunk_size=1)), None)


def resolve_access(
    spark, volume_path: str, *, access: str, operation: str, also=()
) -> AccessConfig:
    """How the tasks of this call reach the files under ``volume_path``.

    ``access`` is ``"fuse"`` (the Volume mount), ``"direct"`` (the Volume's cloud storage
    with credentials Unity Catalog vends) or ``"auto"``: direct when the credentials can be
    had, otherwise the mount, silently, whatever the reason (a Serverless
    ``UC_SERVERLESS_UNTRUSTED_DOMAIN``, no SDK, no permission, no network, a path that is
    not on a Volume). An explicit ``"direct"`` that cannot be had raises
    ``ZarrAccessUnavailable``. ``operation`` is ``"READ_VOLUME"`` or ``"WRITE_VOLUME"``;
    ``also`` lists other paths the tasks open (a registered store's base), which must be on
    the same Volume. Runs on the driver; the credentials are valid for under an hour, so
    each call asks again. ``spark`` is accepted like the other driver functions take it;
    vending needs only the SDK.
    """
    if access not in _ACCESS_MODES:
        raise ValueError(
            f"access must be one of {_ACCESS_MODES}, got {access!r}. "
            "Fix: pass 'auto', 'fuse' or 'direct'."
        )
    if operation not in _OPERATIONS:
        raise ValueError(
            f"operation must be one of {_OPERATIONS}, got {operation!r}. "
            "Fix: pass 'READ_VOLUME' or 'WRITE_VOLUME'."
        )
    if access == "fuse":
        return FUSE
    try:
        cfg = _vend(volume_path, operation, also)
    except Exception as e:  # any failure to vend: the mount is the answer for "auto"
        reason = (
            str(e)
            if isinstance(e, _Unavailable)
            else f"{type(e).__name__}: {str(e)[:200]}"
        )
        _LOG.debug("direct access is not available for %s: %s", volume_path, reason)
        if access == "auto":
            return FUSE
        raise _unavailable(
            f"direct storage access is not available for {volume_path}: {reason}"
        ) from None
    _LOG.info("direct access to %s (%s)", cfg.volume_path, cfg.cloud)
    return replace(cfg, fallback=access == "auto")


def _make_store(cfg: AccessConfig):
    """The obstore store of the Volume's cloud location, signed with the vended credentials
    (the URL's scheme picks the store: ``s3``, ``abfss`` or ``gs``)."""
    from obstore.store import from_url

    return from_url(cfg.volume_url, **_store_args(cfg))


def _store_args(cfg: AccessConfig) -> dict:
    args = {} if cfg.creds is None else cfg.creds.reveal()
    if cfg.region is not None:
        args["region"] = cfg.region
    return args


def _scrub(cfg: AccessConfig, text: str) -> str:
    """``text`` without any credential: a failed request can echo its URL or headers."""
    for value in ({} if cfg.creds is None else cfg.creds.reveal()).values():
        if isinstance(value, str) and value:
            text = text.replace(value, "***")
    return _SAS_SIGNATURE.sub("sig=***", text)


def _is_refusal(text: str) -> bool:
    """Whether the error text of a request obstore calls generic is the storage refusing the
    credentials: a code S3 or Azure names, a 401 or 403, or a 400 with no body to tell
    (S3 answers a failed HEAD with a bare 400 or 403, "not possible to retrieve the exact
    exception", so an expired token seen on a HEAD has no ``ExpiredToken`` in it)."""
    if _AUTH_ERROR.search(text):
        return True
    m = _STATUS.search(text)
    if m is None:
        return False
    status = int(m.group(1))
    return status in (401, 403) or (status == 400 and "<Code>" not in text)


@contextmanager
def _obstore_errors(cfg: AccessConfig):
    """Failures of an obstore call, as the errors the readers handle: credentials the storage
    refuses are ``_Refused`` (not transient: the caller falls back or raises ``_expired``),
    a missing object ``FileNotFoundError``, any other request failure an ``OSError`` (what
    ``_retry_transient`` retries). None of them holds a credential."""
    from obstore import exceptions as ox

    try:
        yield
    except (ox.PermissionDeniedError, ox.UnauthenticatedError):
        raise _Refused() from None
    except ox.NotFoundError as e:
        raise FileNotFoundError(_scrub(cfg, str(e))) from None
    except ox.BaseError as e:
        text = str(e)
        if _is_refusal(text):
            raise _Refused() from None
        raise OSError(_scrub(cfg, text)) from None


class ObstoreRangeReader(_SpanReader):
    """Reads regions from the Volume's cloud storage with ``obstore.get_ranges``.

    The same protocol and the same rules as ``FuseRangeReader``: regions of one file that
    touch or overlap are one span, a relative ``Region.file`` resolves under the store's
    root (``cfg.root_url``) or, for an external-origin region, under ``base_url``, an
    absolute one must lie under one of them, and nothing outside them is ever requested (the
    credentials cover the whole Volume, so the manifest cannot be trusted to stay inside the
    store). A missing or short object is ``ZarrStoreDrift``; an external object whose
    modification time and size are not those recorded is drift as well (one ``head`` per
    object per reader, compared at whole seconds: see ``_OBJECT_MTIME_MS``). The spans of an
    object go out in one ``get_ranges`` call, which
    obstore sends in parallel (and merges when they are less than 1 MiB apart). Request
    failures are retried ``attempts`` times with the helper the FUSE reader uses. Credentials
    the storage refuses (expired, rejected) are not retried: with a ``fallback`` (the call
    asked for ``"auto"``) the reader reads the same regions through the mount from then on,
    without one it raises ``ZarrAccessUnavailable``.
    """

    def __init__(
        self,
        cfg: AccessConfig,
        *,
        base_url: str | None = None,
        attempts: int = 10,
        store=None,
        fallback=None,
    ):
        if not cfg.is_direct or cfg.root_url is None:
            raise ValueError(
                "ObstoreRangeReader needs a direct AccessConfig with a root_url. "
                "Fix: build the reader with reader_for(cfg, root, base_root)."
            )
        self._cfg = cfg
        self._attempts = attempts
        self._root = cfg.key_of_url(cfg.root_url)
        self._base = None if base_url is None else cfg.key_of_url(base_url)
        self._store = store if store is not None else _make_store(cfg)
        self._observed: dict[str, str] = {}  # key -> its identity, taken once
        self._fallback = fallback  # builds the mount reader to carry on with, or None
        self._carry_on = None

    def read(self, regions: list[Region]) -> list[bytes]:
        if self._carry_on is not None:
            return self._carry_on.read(regions)
        try:
            return super().read(regions)
        except _Refused:
            _mark_refused(self._cfg)
            if self._fallback is None:
                raise _expired(self._cfg) from None
            self._carry_on = (
                self._fallback()
            )  # the rest of this reader's work: the mount
            return self._carry_on.read(regions)

    def _show(self, key: str) -> str:
        return self._cfg.volume_url + ("/" + key if key else "")

    @staticmethod
    def _under(key: str, root: str) -> bool:
        if key == ".." or key.startswith("../") or key.startswith("/"):
            return False
        return not root or key == root or key.startswith(root + "/")

    def _resolve(self, region: Region) -> tuple[str, str]:
        """``(object key, key of the root it lies under)`` for ``region``, or
        ``ZarrStoreDrift`` if it leaves the roots."""
        file = region.file
        if not isinstance(file, str) or not file or "\0" in file:
            raise ZarrStoreDrift(
                f"manifest file {file!r} is not a usable path",
                fix="correct the row or run gbx_zarr_sync",
            )
        external = region.origin == "external" and self._base is not None
        base = self._base if external else self._root
        if os.path.isabs(file):
            roots = [self._root] + ([self._base] if self._base is not None else [])
            vol = self._cfg.volume_path
            norm = os.path.normpath(file)
            key = (
                os.path.relpath(norm, vol).replace(os.sep, "/")
                if _within(norm, vol)
                else ".."
            )
        else:
            roots = [base]
            key = posixpath.normpath(posixpath.join(base, file))
        for root in roots:
            if self._under(key, root):
                return key, root
        raise ZarrStoreDrift(
            f"manifest file {file!r} resolves outside the store root "
            f"{' or '.join(self._show(r) for r in roots)}",
            fix="correct the row or run gbx_zarr_sync; a manifest may only point at "
            "files under the store root or, for a registered store, its base root",
        )

    def _read_spans(self, key: str, spans, _root: str, recorded) -> list[bytes]:
        try:
            return _retry_transient(
                lambda: self._fetch(key, spans, recorded), attempts=self._attempts
            )
        except FileNotFoundError:
            raise ZarrStoreDrift(
                f"{self._show(key)} is recorded in the manifest but missing",
                fix=_SYNC_FIX + "restore the file",
            ) from None

    def _fetch(self, key: str, spans, recorded) -> list[bytes]:
        import obstore

        with _obstore_errors(self._cfg):
            if recorded:
                observed = self._observed.get(key)
                if observed is None:
                    meta = obstore.head(self._store, key)
                    mtime_ms = (meta["last_modified"] - _EPOCH) // _MS
                    observed = self._observed[key] = f"{mtime_ms}:{meta['size']}"
                for identity in recorded:
                    check_identity(
                        {"file": self._show(key), "identity": identity},
                        observed,
                        _OBJECT_MTIME_MS,
                    )
            got = obstore.get_ranges(
                self._store,
                key,
                starts=[a for a, _, _ in spans],
                ends=[b for _, b, _ in spans],
            )
            out = [bytes(part) for part in got]
        for (start, stop, _), data in zip(spans, out):
            if len(data) != stop - start:
                raise ZarrStoreDrift(
                    f"{self._show(key)} ends before byte {stop}, which the manifest "
                    "says it holds",
                    fix=_SYNC_FIX + "the file is truncated",
                )
        return out


def reader_for(cfg: AccessConfig | None, root: str, base_root: str | None = None):
    """The range reader of a store whose files are under ``root`` (and a registered
    store's own under ``base_root``): the FUSE reader, or for a direct ``cfg`` the obstore
    reader of the same two directories. With ``cfg.fallback`` (``"auto"``) the obstore
    reader carries on through the mount once the storage refuses the credentials, and a
    config already refused in this process gets the FUSE reader at once."""
    if cfg is None or not cfg.is_direct or (cfg.fallback and cfg.refused):
        return FuseRangeReader(root, base_root=base_root)
    return ObstoreRangeReader(
        replace(cfg, root_url=cfg.url_of(root)),
        base_url=None if base_root is None else cfg.url_of(base_root),
        fallback=(
            (lambda: FuseRangeReader(root, base_root=base_root))
            if cfg.fallback
            else None
        ),
    )


def put_pack(cfg: AccessConfig, local_path: str, dest_path: str) -> bool:
    """Upload the file ``local_path`` to the Volume path ``dest_path`` with one
    ``obstore.put`` (the object appears whole or not at all). ``True`` once it is there.

    If the storage refuses the credentials, ``False`` when the call asked for ``"auto"``
    (the caller writes the file through the mount instead) and ``ZarrAccessUnavailable``
    when it asked for ``"direct"``."""
    import obstore

    if cfg.refused:
        if cfg.fallback:
            return False
        raise _expired(cfg)
    key = cfg.key_of(dest_path)
    try:
        with _obstore_errors(cfg), open(local_path, "rb") as f:
            obstore.put(_make_store(cfg), key, f)
    except _Refused:
        _mark_refused(cfg)
        if cfg.fallback:
            return False
        raise _expired(cfg) from None
    return True


def stat_for(cfg: AccessConfig | None):
    """A function that raises ``FileNotFoundError`` for a Volume path with no file: ``os.stat``
    for the mount, a ``head`` of the object for direct access (a file just put is seen by
    the store at once, by the mount maybe a moment later). If the storage refuses the
    credentials the ``"auto"`` function asks the mount from then on; a ``"direct"`` one
    raises ``ZarrAccessUnavailable``."""
    if cfg is None or not cfg.is_direct:
        return os.stat
    import obstore

    store = _make_store(cfg)

    def head(path: str):
        if cfg.refused:
            if cfg.fallback:
                return os.stat(path)
            raise _expired(cfg)
        try:
            with _obstore_errors(cfg):
                return obstore.head(store, cfg.key_of(path))
        except _Refused:
            _mark_refused(cfg)
            if cfg.fallback:
                return os.stat(path)
            raise _expired(cfg) from None

    return head


# --- credentials only in a small closure -------------------------------------------------------

_UDF_COMPRESSION_THRESHOLD = (
    1 << 20
)  # spark.broadcast.UDFCompressionThreshold's default
_CLOSURE_MARGIN = 64 * 1024  # what PySpark adds around the pickled function
_SIZE = re.compile(r"^\s*(\d+)\s*([kmg]?)b?\s*$", re.IGNORECASE)


def closure_size(fn) -> int:
    """The bytes ``fn`` pickles to, as PySpark pickles a task function."""
    from pyspark import cloudpickle

    return len(cloudpickle.dumps(fn))


def _closure_limit(spark) -> int:
    """The largest task closure that stays below Spark's UDF broadcast threshold
    (``spark.broadcast.UDFCompressionThreshold``, default 1 MiB), less a margin. Above it
    PySpark broadcasts the pickled function, which writes it to ``spark.local.dir``."""
    threshold = _UDF_COMPRESSION_THRESHOLD
    try:
        m = _SIZE.match(
            str(spark.conf.get("spark.broadcast.UDFCompressionThreshold", None))
        )
        if m:
            threshold = (
                int(m.group(1))
                << {"": 0, "k": 10, "m": 20, "g": 30}[m.group(2).lower()]
            )
    except Exception:  # no session (a test), or a client that will not say
        pass
    return max(threshold - _CLOSURE_MARGIN, 0)


def bounded_closure(spark, access: AccessConfig | None, build):
    """``(fn, access)``: the function a task runs, with the access it carries.

    ``build(a)`` makes the function with ``a`` as its access. Credentials may only ride in a
    SMALL closure: one that pickles above Spark's broadcast threshold is written to disk
    (``spark.local.dir``) by PySpark, and the credentials with it. So when the function built
    with a direct ``access`` is too big (a long time axis, a large tile plan), an ``"auto"``
    call builds it again for the mount (and returns ``FUSE``, which the caller adopts for the
    driver's own phases); a ``"direct"`` call raises ``ZarrAccessUnavailable``.
    """
    fn = build(access)
    if access is None or not access.is_direct:
        return fn, access
    size, limit = closure_size(fn), _closure_limit(spark)
    if size <= limit:
        return fn, access
    if access.fallback:
        _LOG.info(
            "the task closure is %d bytes, over the %d that may hold credentials: "
            "reading through the mount",
            size,
            limit,
        )
        return build(FUSE), FUSE
    raise ZarrAccessUnavailable(
        f"this call's plan is {size} bytes, over the {limit} a task closure that holds "
        "credentials may be (Spark would write it to local disk, credentials included)",
        fix='read a smaller selection, or use access="auto" (it reads a large plan '
        'through the mount) or "fuse"',
    )
