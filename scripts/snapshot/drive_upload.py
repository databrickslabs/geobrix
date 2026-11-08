"""Google Drive side of gbx:local:snapshot: upload one verified snapshot zip, prune the folder.

Stdlib only and Python 3.8+. A plain snapshot never imports this module; snapshot.py loads it
only for --upload / GBX_SNAPSHOT_UPLOAD=1 or the --drive options.

Every request this module can make, all to https://www.googleapis.com/ and nowhere else:

  files.get      the destination folder (name, kind, owner), or one snapshot (its bytes)
  permissions.list  who the destination folder is shared with (read before any upload or prune)
  files.list     the children of the destination folder only
  files.create   a resumable upload of ONE zip, into the destination folder
  files.update   move a snapshot (matched by the exact snapshot name pattern, owned by you) to the trash
  files.delete   remove a bad upload this same run just created (md5 mismatch), by the id Drive returned

There is no function that uploads an arbitrary path. The only way to a Drive upload is
Produced.after_scan(...), which snapshot.py calls for the zip it has just written and only when the
secret scan came back clean. Neither an upload nor a prune runs against a folder that anyone but you
can open, unless GBX_SNAPSHOT_DRIVE_ALLOW_SHARED=1 (see DriveClient.check_private).
"""

from __future__ import annotations

import hashlib
import http.client
import json
import os
import random
import re
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

API = "https://www.googleapis.com/drive/v3"
UPLOAD_API = "https://www.googleapis.com/upload/drive/v3/files"
ALLOWED_PREFIX = "https://www.googleapis.com/"
FOLDER_MIME = "application/vnd.google-apps.folder"
ZIP_MIME = "application/zip"

GRANULE = 256 * 1024  # Drive wants resumable chunks in multiples of 256 KiB
CHUNK_BYTES = 8 * 1024 * 1024
TOTAL_SECONDS = 30 * 60  # one budget for a whole upload, prune, or download
MAX_ATTEMPTS = 6  # per request, and per stalled chunk
BACKOFF_CAP_S = 60.0
RETRY_STATUS = frozenset((429, 500, 502, 503, 504))
RATE_LIMIT_REASONS = frozenset(("rateLimitExceeded", "userRateLimitExceeded"))
MAX_TRASHED_PER_RUN = 25  # blast radius: oldest first, the rest wait for the next run

# A Drive id is letters, digits, '-' and '_'. Checked before an id goes into a URL or a query.
DRIVE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{10,100}$")
# A GCP project id (lowercase, 6-30 chars) or a project number.
PROJECT_RE = re.compile(r"^(?:[a-z][a-z0-9-]{4,28}[a-z0-9]|[0-9]{6,20})$")
_TOKENISH = re.compile(
    r"ya29\.[A-Za-z0-9._-]+|Bearer\s+\S+|[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}"
)


# --- errors ----------------------------------------------------------------------------------------
class DriveError(Exception):
    """Anything that stops a Drive step. The message is safe to print (see redact)."""


class DriveTimeout(DriveError):
    pass


class UploadRefused(DriveError):
    """The zip is not eligible for upload (scan not clean, file changed, folder wrong)."""


class VerifyError(DriveError):
    """Drive's copy does not match the local zip."""


class PruneRefused(DriveError):
    """The folder listing does not look like a snapshot folder, so nothing is removed."""


class SharedFolderRefused(UploadRefused):
    """The Drive folder is open to someone besides its owner, and sharing was not accepted."""


# --- small helpers ---------------------------------------------------------------------------------
def redact(text: str, private: Iterable[str] = ()) -> str:
    """Strip anything token-shaped, and the configured folder id and quota project, from text."""
    out = _TOKENISH.sub("[redacted]", text)
    for secret in private:
        if secret:
            out = out.replace(secret, "[configured]")
    return out


def check_id(value: str, what: str = "id") -> str:
    if not DRIVE_ID_RE.match(value or ""):
        raise DriveError("the %s is not a Drive id (letters, digits, - and _)" % what)
    return value


def config_problems(folder_id: str, quota_project: str) -> List[str]:
    """What is wrong with the Drive settings, in words that never include the values."""
    problems = []
    if not folder_id:
        problems.append("GBX_SNAPSHOT_DRIVE_FOLDER_ID is not set")
    elif not DRIVE_ID_RE.match(folder_id):
        problems.append("GBX_SNAPSHOT_DRIVE_FOLDER_ID is not a Drive folder id")
    if quota_project and not PROJECT_RE.match(quota_project):
        problems.append(
            "GBX_SNAPSHOT_GCP_QUOTA_PROJECT is not a GCP project id or number"
        )
    return problems


def file_md5(path: Path, block: int = 1024 * 1024) -> str:
    """Drive reports md5 for integrity, not security; hashlib may flag it, hence the flag."""
    try:
        h = hashlib.md5(usedforsecurity=False)  # type: ignore[call-arg]
    except TypeError:  # Python < 3.9
        h = hashlib.md5()
    with open(str(path), "rb") as fh:
        for chunk in iter(lambda: fh.read(block), b""):
            h.update(chunk)
    return h.hexdigest()


def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return "%d B" % n if unit == "B" else "%.1f %s" % (n, unit)
        n /= 1024.0
    return "%.1f GB" % n


# --- the one gate to an upload ----------------------------------------------------------------------
class Produced:
    """A snapshot zip that run just wrote AND whose contents passed the secret scan.

    Build one only with after_scan(); DriveClient.upload() takes nothing else. The md5 and size are
    taken from the file at that moment, and the upload checks that Drive holds exactly those bytes.
    """

    def __init__(self, path: Path, size: int, md5: str, _token: object = None):
        if _token is not _GATE:
            raise UploadRefused("build a Produced with Produced.after_scan()")
        self.path, self.size, self.md5 = path, size, md5

    @classmethod
    def after_scan(
        cls,
        path: Path,
        findings: Sequence[object],
        zip_findings: Sequence[object],
        allowed_paths: Sequence[str],
        gitleaks_ran: bool,
        name_re: "re.Pattern[str]",
    ) -> "Produced":
        """Raise UploadRefused unless the scan was clean and complete.

        findings      from the pre-write scan of the files (must be empty)
        zip_findings  from a re-scan of the bytes inside the finished zip (must be empty)
        allowed_paths paths whose findings GBX_SNAPSHOT_SCAN_ALLOW waved through (must be empty:
                      that waiver is for the local copy; it does not extend to a shared folder)
        gitleaks_ran  the gitleaks pass completed (the built-in patterns alone are not enough to leave the machine)
        """
        why = []
        if findings:
            why.append("the secret scan reported %d finding(s)" % len(findings))
        if zip_findings:
            why.append(
                "the finished zip holds %d secret-shaped value(s)" % len(zip_findings)
            )
        if allowed_paths:
            why.append(
                "GBX_SNAPSHOT_SCAN_ALLOW waived %d path(s); that waiver is not honored for an upload"
                % len(allowed_paths)
            )
        if not gitleaks_ran:
            why.append(
                "gitleaks did not run cleanly (set GBX_SNAPSHOT_GITLEAKS or put gitleaks on PATH)"
            )
        if why:
            raise UploadRefused("not uploading: " + "; ".join(why))
        path = Path(path)
        if not path.is_file() or not name_re.fullmatch(path.name):
            raise UploadRefused("not uploading: %s is not a snapshot zip" % path.name)
        return cls(path, path.stat().st_size, file_md5(path), _GATE)


_GATE = object()


# --- transport and token ---------------------------------------------------------------------------
class Response:
    def __init__(self, status: int, headers: Dict[str, str], body: bytes):
        self.status, self.headers, self.body = status, headers, body

    def json(self) -> Dict:
        try:
            data = json.loads(self.body.decode("utf-8"))
        except ValueError:
            raise DriveError(
                "Drive returned a reply that is not JSON (HTTP %d)" % self.status
            )
        if not isinstance(data, dict):
            raise DriveError(
                "Drive returned an unexpected reply (HTTP %d)" % self.status
            )
        return data


Transport = Callable[[str, str, Dict[str, str], Optional[bytes]], Response]


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Never follow a redirect: urllib would resend the Authorization header to the new host."""

    def redirect_request(self, *args, **kwargs):  # type: ignore[override]
        return None


def urllib_transport(
    method: str,
    url: str,
    headers: Dict[str, str],
    body: Optional[bytes],
    timeout: float = 120.0,
) -> Response:
    """The real transport: HTTPS to www.googleapis.com only, no redirects."""
    if not url.startswith(ALLOWED_PREFIX):
        raise DriveError("refusing a request to anything but www.googleapis.com")
    req = urllib.request.Request(url, data=body, method=method, headers=headers)
    opener = urllib.request.build_opener(_NoRedirect)
    try:
        with opener.open(req, timeout=timeout) as resp:
            return Response(
                resp.status,
                {k.lower(): v for k, v in resp.headers.items()},
                resp.read(),
            )
    except urllib.error.HTTPError as exc:
        try:
            data = exc.read()
        finally:
            exc.close()
        return Response(exc.code, {k.lower(): v for k, v in exc.headers.items()}, data)


class GcloudToken:
    """Access token from gcloud application-default credentials. Held in memory only, never printed."""

    def __init__(self, run: Callable = subprocess.run):
        self._run = run
        self._token = ""

    def get(self, refresh: bool = False) -> str:
        if self._token and not refresh:
            return self._token
        env = dict(os.environ, CLOUDSDK_CORE_DISABLE_PROMPTS="1")
        try:
            res = self._run(
                ["gcloud", "auth", "application-default", "print-access-token"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                timeout=60,
                env=env,
            )
        except (OSError, subprocess.TimeoutExpired):
            raise DriveError(
                "gcloud did not run; install it and sign in with application-default credentials"
            )
        token = (res.stdout or "").strip()
        if res.returncode != 0 or not token:
            raise DriveError(
                "gcloud has no usable application-default credentials; sign in yourself "
                "(gcloud auth application-default login) and run again"
            )
        self._token = token
        return token


# --- the client ------------------------------------------------------------------------------------
class DriveClient:
    def __init__(
        self,
        token: GcloudToken,
        transport: Transport = urllib_transport,
        quota_project: str = "",
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        total_seconds: float = TOTAL_SECONDS,
        chunk_bytes: int = CHUNK_BYTES,
        say: Callable[[str], None] = lambda msg: None,
        private: Sequence[str] = (),
        allow_shared: bool = False,
    ):
        if chunk_bytes <= 0 or chunk_bytes % GRANULE:
            raise ValueError("chunk_bytes must be a positive multiple of 256 KiB")
        self.token, self.transport, self.quota_project = token, transport, quota_project
        self.sleep, self.clock, self.chunk, self.say = sleep, clock, chunk_bytes, say
        self.private = [p for p in list(private) + [quota_project] if p]
        self.allow_shared = allow_shared
        # folder ids this client has already confirmed private
        self._private_ok: set = set()
        self._deadline = clock() + total_seconds

    # -- plumbing --
    def safe(self, text: str) -> str:
        return redact(text, self.private)

    def _check_time(self) -> None:
        if self.clock() > self._deadline:
            raise DriveTimeout("the time budget for this Drive step ran out")

    def _headers(self, extra: Optional[Dict[str, str]], auth: bool) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        if auth:
            headers["Authorization"] = "Bearer " + self.token.get()
            if self.quota_project:
                headers["x-goog-user-project"] = self.quota_project
        headers.update(extra or {})
        return headers

    @staticmethod
    def _reason(resp: Response) -> str:
        try:
            return str(
                json.loads(resp.body.decode("utf-8"))["error"]["errors"][0]["reason"]
            )
        except (ValueError, KeyError, IndexError, TypeError):
            return ""

    def _error(self, resp: Response) -> DriveError:
        msg = ""
        try:
            err = json.loads(resp.body.decode("utf-8")).get("error", {})
            msg = err.get("message", "") if isinstance(err, dict) else str(err)
        except (ValueError, AttributeError):
            pass
        return DriveError(
            self.safe("Drive answered HTTP %d %s" % (resp.status, msg[:200])).strip()
        )

    def _retryable(self, resp: Response) -> bool:
        return resp.status in RETRY_STATUS or (
            resp.status == 403 and self._reason(resp) in RATE_LIMIT_REASONS
        )

    def _pause(self, attempt: int, resp: Optional[Response]) -> None:
        wait = min(BACKOFF_CAP_S, 2.0**attempt + random.random())
        if resp is not None and resp.headers.get("retry-after", "").isdigit():
            wait = min(BACKOFF_CAP_S, max(wait, float(resp.headers["retry-after"])))
        self._check_time()
        self.sleep(wait)

    def _send(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        body: Optional[bytes],
        auth: bool,
    ) -> Optional[Response]:
        """One attempt. None means the network failed (no HTTP reply)."""
        try:
            return self.transport(method, url, self._headers(headers, auth), body)
        except (OSError, http.client.HTTPException):
            return None

    def _request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[bytes] = None,
        ok: Tuple[int, ...] = (200,),
    ) -> Response:
        """An authenticated call: retried with backoff on 5xx/429/network errors, and once with a
        fresh token on 401. Returns a reply whose status is in ok, else raises DriveError.
        """
        refreshed = False
        attempt = 0
        while True:
            self._check_time()
            resp = self._send(method, url, headers or {}, body, auth=True)
            if resp is not None:
                if resp.status in ok:
                    return resp
                if resp.status == 401 and not refreshed:
                    refreshed = True
                    self.token.get(refresh=True)
                    continue
                if not self._retryable(resp):
                    raise self._error(resp)
            attempt += 1
            if attempt >= MAX_ATTEMPTS:
                raise (
                    self._error(resp)
                    if resp is not None
                    else DriveError(
                        "no reply from Drive after %d attempts" % MAX_ATTEMPTS
                    )
                )
            self._pause(attempt, resp)

    # -- reads --
    def get_folder(self, folder_id: str) -> Dict:
        """The destination folder; refuses anything that is not a folder you can add files to."""
        check_id(folder_id, "folder id")
        query = urllib.parse.urlencode(
            {
                "fields": "id,name,mimeType,ownedByMe,shared,capabilities/canAddChildren",
                "supportsAllDrives": "true",
            }
        )
        folder = self._request("GET", "%s/files/%s?%s" % (API, folder_id, query)).json()
        if folder.get("mimeType") != FOLDER_MIME:
            raise UploadRefused("the configured Drive id is not a folder")
        return folder

    def list_permissions(self, folder_id: str) -> List[Dict]:
        """Every permission on the folder (permissions.list), by type, role, address and domain."""
        check_id(folder_id, "folder id")
        perms: List[Dict] = []
        page = ""
        for _ in range(50):
            params = {
                "fields": "nextPageToken,permissions(type,role,emailAddress,domain)",
                "pageSize": "100",
                "supportsAllDrives": "true",
            }
            if page:
                params["pageToken"] = page
            data = self._request(
                "GET",
                "%s/files/%s/permissions?%s"
                % (API, folder_id, urllib.parse.urlencode(params)),
            ).json()
            perms.extend(data.get("permissions", []))
            page = data.get("nextPageToken", "")
            if not page:
                return perms
        raise DriveError("the folder's permission list did not end after 50 pages")

    def check_private(self, folder_id: str, folder: Optional[Dict] = None) -> List[str]:
        """Refuse a folder that anyone but its owner (this account) can open.

        Called before any upload or prune. Returns who else has access (empty when private). With
        allow_shared the sharing is only reported, never refused, and an unreadable permission list
        is a note, not a stop. Without it, an unreadable list is a refusal: private is unconfirmed.
        """
        if folder_id in self._private_ok:
            return []
        folder = folder if folder is not None else self.get_folder(folder_id)
        try:
            others = other_access(folder, self.list_permissions(folder_id))
        except DriveTimeout:
            raise
        except DriveError as exc:
            if not self.allow_shared:
                raise SharedFolderRefused(
                    self.safe(
                        "refusing: the folder's sharing could not be read (%s), so it cannot be "
                        "confirmed private. %s" % (exc, SHARED_FIX)
                    )
                )
            self.say(
                "  drive: could not read the folder's sharing (%s); continuing" % exc
            )
            return []
        if others and not self.allow_shared:
            raise SharedFolderRefused(
                "refusing: the Drive folder is not private (%s). %s"
                % ("; ".join(others), SHARED_FIX)
            )
        if others:
            self.say(
                "  drive: the folder is shared (%s); accepted by GBX_SNAPSHOT_DRIVE_ALLOW_SHARED"
                % "; ".join(others)
            )
        else:
            self._private_ok.add(folder_id)
        return others

    def list_folder(self, folder_id: str) -> List[Dict]:
        """Every non-trashed child of the folder. Any error raises; a partial listing is never returned."""
        check_id(folder_id, "folder id")
        files: List[Dict] = []
        page = ""
        for _ in range(200):
            params = {
                "q": "'%s' in parents and trashed=false" % folder_id,
                "fields": "nextPageToken,files(id,name,size,md5Checksum,createdTime,mimeType,ownedByMe)",
                "pageSize": "1000",
                "supportsAllDrives": "true",
                "includeItemsFromAllDrives": "true",
            }
            if page:
                params["pageToken"] = page
            data = self._request(
                "GET", "%s/files?%s" % (API, urllib.parse.urlencode(params))
            ).json()
            files.extend(data.get("files", []))
            page = data.get("nextPageToken", "")
            if not page:
                return files
        raise DriveError("the folder listing did not end after 200 pages")

    # -- upload --
    def upload(
        self, produced: Produced, folder_id: str, name_re: "re.Pattern[str]"
    ) -> Dict:
        """Upload the zip, then check Drive's md5 and size against the local file. On a mismatch the
        upload is removed (by the id Drive just returned) and VerifyError is raised."""
        if not isinstance(produced, Produced):
            raise UploadRefused("only a Produced zip can be uploaded")
        if not name_re.fullmatch(produced.path.name):
            raise UploadRefused("not a snapshot name")
        folder = self.get_folder(folder_id)
        self.check_private(folder_id, folder)  # before any byte leaves
        if not folder.get("capabilities", {}).get("canAddChildren", False):
            raise UploadRefused("this account cannot add files to the folder")
        info: Optional[Dict] = None
        for _ in range(2):  # once more if Drive drops the upload session
            session = self._start_session(produced, folder_id)
            info = self._send_chunks(session, produced)
            if info is not None:
                break
        if info is None:
            raise DriveError("the upload session was dropped twice")
        file_id = str(info.get("id", ""))
        check_id(file_id, "uploaded file id")
        remote_md5, remote_size = self._remote_checksum(file_id, info)
        if remote_md5 != produced.md5 or remote_size != produced.size:
            self._discard(file_id)
            raise VerifyError(
                "Drive's copy does not match the local zip; the upload was removed"
            )
        self.say("  drive: md5 and size match (%s)" % human(produced.size))
        return info

    def _start_session(self, produced: Produced, folder_id: str) -> str:
        meta = {
            "name": produced.path.name,
            "parents": [folder_id],
            "mimeType": ZIP_MIME,
        }
        query = urllib.parse.urlencode(
            {
                "uploadType": "resumable",
                "supportsAllDrives": "true",
                "fields": "id,name,size,md5Checksum",
            }
        )
        resp = self._request(
            "POST",
            "%s?%s" % (UPLOAD_API, query),
            {
                "Content-Type": "application/json; charset=UTF-8",
                "X-Upload-Content-Type": ZIP_MIME,
                "X-Upload-Content-Length": str(produced.size),
            },
            json.dumps(meta).encode("utf-8"),
        )
        session = resp.headers.get("location", "")
        if not session.startswith(ALLOWED_PREFIX):
            raise DriveError("Drive did not return an upload session")
        return session

    @staticmethod
    def _received(resp: Response) -> int:
        """Bytes Drive holds, from a 308 reply's Range header (no header: none)."""
        m = re.match(r"^bytes=0-(\d+)$", resp.headers.get("range", ""))
        return int(m.group(1)) + 1 if m else 0

    def _send_chunks(self, session: str, produced: Produced) -> Optional[Dict]:
        """PUT the file in chunks, resuming from what Drive reports after a failure.
        None means the session is gone (404/410) and the caller starts a new one."""
        total = produced.size
        offset = 0
        stalls = 0
        next_report = 10
        with open(str(produced.path), "rb") as fh:
            while True:
                self._check_time()
                fh.seek(offset)
                data = fh.read(min(self.chunk, total - offset))
                last = offset + len(data) - 1
                resp = self._send(
                    "PUT",
                    session,
                    {
                        "Content-Length": str(len(data)),
                        "Content-Range": "bytes %d-%d/%d" % (offset, last, total),
                        "Content-Type": ZIP_MIME,
                    },
                    data,
                    auth=False,
                )
                if resp is not None and resp.status in (200, 201):
                    return resp.json()
                if resp is not None and resp.status == 308:
                    offset, stalls = self._received(resp), 0
                    pct = 100 * offset // max(total, 1)
                    if pct >= next_report:
                        self.say("  drive: uploaded %d%%" % pct)
                        next_report = (pct // 10 + 1) * 10
                    continue
                if resp is not None and resp.status in (404, 410):
                    return None
                if resp is not None and not self._retryable(resp):
                    raise self._error(resp)
                stalls += 1
                if stalls >= MAX_ATTEMPTS:
                    raise DriveError(
                        "the upload stalled after %d attempts" % MAX_ATTEMPTS
                    )
                self._pause(stalls, resp)
                status = self._send(
                    "PUT",
                    session,
                    {
                        "Content-Length": "0",
                        "Content-Range": "bytes */%d" % total,
                        "Content-Type": ZIP_MIME,
                    },
                    b"",
                    auth=False,
                )
                if status is None or status.status in RETRY_STATUS:
                    continue  # counted as another stall on the next pass
                if status.status in (200, 201):
                    return status.json()
                if status.status == 308:
                    offset = self._received(status)
                    continue
                if status.status in (404, 410):
                    return None
                raise self._error(status)

    def _remote_checksum(self, file_id: str, created: Dict) -> Tuple[str, int]:
        """(md5, size) as Drive reports them. The md5 can lag the create reply, so ask again a few times."""
        md5, size = str(created.get("md5Checksum", "")), created.get("size")
        for attempt in range(5):
            if md5 and size is not None:
                return md5, int(size)
            self._pause(attempt + 1, None)
            query = urllib.parse.urlencode(
                {"fields": "md5Checksum,size", "supportsAllDrives": "true"}
            )
            got = self._request("GET", "%s/files/%s?%s" % (API, file_id, query)).json()
            md5, size = str(got.get("md5Checksum", "")), got.get("size")
        raise VerifyError(
            "Drive did not report a checksum for the upload, so it cannot be verified"
        )

    # -- removing --
    def trash(self, file_id: str) -> None:
        check_id(file_id, "file id")
        query = urllib.parse.urlencode(
            {"supportsAllDrives": "true", "fields": "id,trashed"}
        )
        self._request(
            "PATCH",
            "%s/files/%s?%s" % (API, file_id, query),
            {"Content-Type": "application/json; charset=UTF-8"},
            b'{"trashed": true}',
        )

    def delete(self, file_id: str) -> None:
        check_id(file_id, "file id")
        self._request(
            "DELETE",
            "%s/files/%s?supportsAllDrives=true" % (API, file_id),
            ok=(200, 204),
        )

    def _discard(self, file_id: str) -> None:
        """Remove the upload this run just made and found corrupt. Never raises: the caller is
        already reporting the verify failure, and a corrupt file must not stay the newest snapshot.
        """
        try:
            self.delete(file_id)
        except DriveError as exc:
            self.say(
                "  drive: could not remove the bad upload; delete it by hand (%s)" % exc
            )

    # -- prune --
    def prune(
        self,
        folder_id: str,
        keep: int,
        name_re: "re.Pattern[str]",
        dry_run: bool = False,
        protect_ids: Sequence[str] = (),
    ) -> "PruneResult":
        """Keep the newest `keep` snapshots (by the timestamp in the name); trash the rest.
        Only files named like a snapshot and owned by this account are ever candidates.
        """
        # a dry run is refused too: it shows what a real run would do
        self.check_private(folder_id)
        listing = self.list_folder(
            folder_id
        )  # raises on any error: no listing, no deletes
        plan = plan_prune(listing, keep, name_re, protect_ids)
        # plan.stale is newest first, so its tail is the oldest files; at most MAX_TRASHED_PER_RUN go.
        cut = max(0, len(plan.stale) - MAX_TRASHED_PER_RUN)
        deferred, victims = plan.stale[:cut], plan.stale[cut:]
        for f in victims:
            if dry_run:
                self.say("  would trash %s" % f["name"])
            else:
                self.trash(str(f["id"]))
                self.say("  trashed %s" % f["name"])
        return PruneResult(plan.kept, victims, deferred, plan.foreign, dry_run)

    # -- download --
    def download(self, f: Dict, dest: Path) -> None:
        """Fetch one snapshot to dest in ranged pieces, checking size and md5 before it appears."""
        file_id = check_id(str(f.get("id", "")), "file id")
        size, want = int(f.get("size", -1)), str(f.get("md5Checksum", ""))
        if size < 0 or not want:
            raise DriveError("Drive did not give a size and checksum for that file")
        part = dest.with_name(dest.name + ".part")
        try:
            with open(str(part), "wb") as out:
                offset = 0
                while offset < size:
                    end = min(offset + self.chunk, size) - 1
                    resp = self._request(
                        "GET",
                        "%s/files/%s?alt=media&supportsAllDrives=true" % (API, file_id),
                        {"Range": "bytes=%d-%d" % (offset, end)},
                        ok=(200, 206),
                    )
                    if resp.status == 200:  # Range ignored: the whole file came back
                        if len(resp.body) != size:
                            raise DriveError("Drive sent an unexpected number of bytes")
                        out.write(resp.body)
                        break
                    if len(resp.body) != end - offset + 1:
                        raise DriveError("Drive sent an unexpected number of bytes")
                    out.write(resp.body)
                    offset = end + 1
            if part.stat().st_size != size or file_md5(part) != want:
                raise VerifyError("the download does not match Drive's checksum")
            os.replace(str(part), str(dest))
        finally:
            if part.exists():
                part.unlink()


# --- choosing what to keep (pure functions: easy to read, easy to test) ----------------------------
class PrunePlan:
    def __init__(self, kept: List[Dict], stale: List[Dict], foreign: List[Dict]):
        self.kept, self.stale, self.foreign = kept, stale, foreign


class PruneResult:
    def __init__(self, kept, trashed, deferred, foreign, dry_run):
        self.kept, self.trashed, self.deferred = kept, trashed, deferred
        self.foreign, self.dry_run = foreign, dry_run


def snapshot_files(
    listing: Iterable[Dict], name_re: "re.Pattern[str]"
) -> Tuple[List[Dict], List[Dict]]:
    """(owned, foreign): the listing's files whose whole name matches the snapshot pattern, split by
    owner, newest first by the UTC stamp in the name. Drive's own dates are only a tiebreak for two
    files with the same name. Folders and non-matching names are dropped here and never reach a delete.
    """
    owned, foreign = [], []
    for f in listing:
        m = name_re.fullmatch(str(f.get("name", "")))
        if not m or f.get("mimeType") == FOLDER_MIME:
            continue
        (owned if f.get("ownedByMe") is True else foreign).append((m.group(1), f))

    def order(pair):
        stamp, f = pair
        return (stamp, str(f.get("createdTime", "")), str(f.get("id", "")))

    owned.sort(key=order, reverse=True)
    foreign.sort(key=order, reverse=True)
    return [f for _, f in owned], [f for _, f in foreign]


def plan_prune(
    listing: List[Dict],
    keep: int,
    name_re: "re.Pattern[str]",
    protect_ids: Sequence[str] = (),
) -> PrunePlan:
    if keep < 1:
        raise PruneRefused("keep must be at least 1")
    owned, foreign = snapshot_files(listing, name_re)
    if listing and not owned:
        raise PruneRefused(
            "the folder holds %d item(s) and none is a snapshot you own; "
            "is the folder id the right one? nothing was removed" % len(listing)
        )
    kept, stale = owned[:keep], owned[keep:]
    if protect_ids and not set(protect_ids) <= {f.get("id") for f in kept}:
        raise PruneRefused(
            "the file just uploaded is not among the newest %d by name; nothing was removed"
            % keep
        )
    return PrunePlan(kept, stale, foreign)


def mask_email(addr: str) -> str:
    """First letter and the domain only: enough to recognise a person, not to collect addresses."""
    local, _, domain = str(addr).partition("@")
    return "%s***@%s" % (local[:1], domain) if domain else "***"


def describe_permission(p: Dict) -> str:
    """One permission as kind, who and role: a domain by name, a person or group by masked address."""
    kind, role = str(p.get("type", "?")), str(p.get("role", "?"))
    if kind == "anyone":
        who = "ANYONE WITH THE LINK"
    elif kind == "domain":
        who = "everyone at domain %s" % (p.get("domain") or "?")
    elif p.get("emailAddress"):
        who = "%s %s" % (kind, mask_email(p["emailAddress"]))
    else:
        who = "%s (address hidden)" % kind
    return "%s (%s)" % (who, role)


def other_access(folder: Dict, perms: Sequence[Dict]) -> List[str]:
    """Who besides this account can open the folder; empty means private.

    The folder must be owned by this account (ownedByMe). The only permission allowed is the owner's
    own (a user with the owner role). Anything else is access: a person, a group, a domain, a link.
    """
    out = []
    if folder.get("ownedByMe") is not True:
        out.append("the folder is owned by someone else, or is in a shared drive")
    for p in perms:
        if p.get("type") == "user" and p.get("role") == "owner":
            continue
        out.append(describe_permission(p))
    if not out and folder.get("shared") is True:
        out.append("Drive marks the folder as shared but lists no one")
    return sorted(out)


SHARED_FIX = (
    "Fix: make the folder private in Drive (remove its sharing), or set "
    "GBX_SNAPSHOT_DRIVE_ALLOW_SHARED=1 to accept it."
)
