"""Stage GeoBrix artifacts on a Unity Catalog Volume.

``stage_jar`` stages the JAR this wheel carries, for a dedicated cluster (or a standard cluster whose metastore admin
allowlists the path); the Python level never needs it. ``python -m databricks.labs.gbx.stage deps`` and ``stage_deps``
stage the wheels of an install without PyPI access.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import os
import re
import shutil
import sys
import zipfile

from databricks.labs.gbx.uc.ddl import check_volume_dir

_IN_WHEEL = re.compile(
    r"databricks/labs/gbx/_jar/geobrix-[^/]+-jar-with-dependencies\.jar"
)
INSTRUCTION = (
    "geobrix: staged {dst} (sha256 {sha}).\n"
    "Next: add it as a cluster library on a dedicated cluster (Compute > your cluster > Libraries > "
    "Install new > Volumes > {dst}) and restart the cluster; a running cluster cannot load a JAR. "
    "On standard access mode a metastore admin must first add {dst} to the artifact allowlist. "
    "Then install the same env wheel on the cluster and call databricks.labs.gbx.register(spark): "
    "GeoBrix checks that the JAR and the wheel are the same version."
)


_WHEEL_NAME = re.compile(r"geobrix-([^-+]+)(?:\+[^-]+)?-py3-none-any\.whl")


def _recorded_sha256(z: zipfile.ZipFile, member: str):
    """The hex sha256 the wheel's RECORD lists for ``member`` (``sha256=<urlsafe base64>``), or None."""
    records = [n for n in z.namelist() if re.fullmatch(r"[^/]+\.dist-info/RECORD", n)]
    if len(records) != 1:
        return None
    for row in csv.reader(io.StringIO(z.read(records[0]).decode("utf-8"))):
        if row and row[0] == member:
            algo, _, digest = (row[1] if len(row) > 1 else "").partition("=")
            if algo == "sha256" and digest:
                try:
                    return base64.urlsafe_b64decode(
                        digest + "=" * (-len(digest) % 4)
                    ).hex()
                except ValueError:
                    return None
    return None


def jar_from_wheel(whl_path: str) -> bytes:
    """The bytes of the JAR an env wheel carries, after two checks: the JAR is named for the wheel's
    version, and its sha256 is the one the wheel's RECORD lists."""
    base = os.path.basename(whl_path)
    with zipfile.ZipFile(whl_path) as z:
        hits = [n for n in z.namelist() if _IN_WHEEL.fullmatch(n)]
        if len(hits) != 1:
            raise ValueError(
                f"{base} carries {len(hits)} geobrix JARs, expected 1. "
                "Fix: use an env wheel (geobrix-<version>+env5 or +env6)."
            )
        member = hits[0]
        named = _WHEEL_NAME.fullmatch(base)
        if named and os.path.basename(member) != (
            f"geobrix-{named.group(1)}-jar-with-dependencies.jar"
        ):
            raise ValueError(
                f"{base} carries {os.path.basename(member)}, not the JAR of version "
                f"{named.group(1)}. Fix: rebuild the wheel (gbx:data:stage-wheel) so it bundles its own JAR."
            )
        data = z.read(member)
        want = _recorded_sha256(z, member)
        if want is None or hashlib.sha256(data).hexdigest() != want:
            raise ValueError(
                f"the JAR in {base} does not match the sha256 its RECORD lists"
                + (" (RECORD lists none)" if want is None else "")
                + ". Fix: rebuild the wheel and stage the new one."
            )
        return data


def _already_staged(src: str, dst: str, sha: str) -> bool:
    """The staged JAR exists with the source's size and hashes to ``sha``, and its sidecar says so: the sidecar alone
    is not proof (an interrupted copy or a deleted JAR leaves it behind)."""
    if not (os.path.exists(dst) and os.path.exists(dst + ".sha256")):
        return False
    if os.path.getsize(dst) != os.path.getsize(src):
        return False
    with open(dst + ".sha256") as fh:
        if fh.read().split()[:1] != [sha]:
            return False
    with open(dst, "rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest() == sha


def stage_jar(vol_dir: str, *, overwrite: bool = False) -> str:
    """Copy the JAR this wheel carries into ``vol_dir`` with a ``.sha256`` sidecar and print the cluster
    step. One geobrix JAR per folder (one per cluster). Returns the staged path."""
    from databricks.labs.gbx import _native

    src = _native.jar_path()
    if src is None:
        raise RuntimeError(
            "this wheel carries no JAR. Fix: install the env wheel (geobrix-<version>+env5 or +env6)."
        )
    vol_dir = check_volume_dir(vol_dir.rstrip("/"))
    os.makedirs(vol_dir, exist_ok=True)
    name = os.path.basename(src)
    others = sorted(
        f
        for f in os.listdir(vol_dir)
        if f.endswith("-jar-with-dependencies.jar") and f != name
    )
    if others:
        raise RuntimeError(
            f"{vol_dir} already holds {others[0]}. Fix: keep one geobrix JAR per folder (one geobrix JAR per "
            "cluster): remove it, or stage into another folder."
        )
    with open(src, "rb") as fh:
        sha = hashlib.sha256(fh.read()).hexdigest()
    dst = os.path.join(vol_dir, name)
    if overwrite or not _already_staged(src, dst, sha):
        shutil.copyfile(src, dst)  # a Volume (FUSE) takes no rename: copy in place
        with open(dst + ".sha256", "w") as fh:
            fh.write(f"{sha}  {name}\n")
    print(INSTRUCTION.format(dst=dst, sha=sha))
    return dst


# --- the wheels of an install without PyPI access ------------------------------------------------------------------
#
# `python -m databricks.labs.gbx.stage deps --out DIR` downloads, on any machine with PyPI access and the env wheel
# installed, the wheels the env wheel needs (the pinned, hashed lists it carries in _native/deps/), for each target
# architecture, and proves offline that they are complete. `stage_deps(vol_dir, src=DIR)` puts DIR on a Volume.

WHEELS_TXT = "wheels.txt"
_ENV_WHEEL = re.compile(r"geobrix-[^-/]+\+env([0-9]+)-py3-none-any\.whl")


def _deps_module():
    from databricks.labs.gbx._native import _deps

    return _deps


def _installed_env() -> int:
    """The environment of the installed env wheel (``<version>+env<N>``)."""
    from databricks.labs.gbx import __version__

    m = re.search(r"\+env([0-9]+)$", __version__)
    if m is None:
        raise RuntimeError(
            f"geobrix {__version__} is not an env wheel and carries no dependency lists. "
            "Fix: install the env wheel (geobrix-<version>+env5 or +env6) and run this again."
        )
    return int(m.group(1))


def _read_list(name: str) -> str:
    from databricks.labs.gbx import _native

    d = _native.deps_dir()
    path = os.path.join(d, name) if d else None
    if path is None or not os.path.isfile(path):
        raise RuntimeError(
            f"this wheel carries no dependency list {name}. Fix: install an env wheel of 0.6.0 or later "
            "(geobrix-<version>+env5 or +env6), which carries them in _native/deps/."
        )
    with open(path) as fh:
        return fh.read()


def dep_pins(arch: str, extras=(), env: int = None):
    """canonical name -> (version, sha256) of every wheel an install of the env wheel (with ``extras``) needs on ``arch``,
    beyond what the runtime preinstalls: the core list with each extra's additions. Two extras that pin one package to
    different versions are refused."""
    d = _deps_module()
    env = env or _installed_env()
    pins = d.parse_list(_read_list(d.list_name(env, arch)), d.list_name(env, arch))
    core = dict(pins)
    for extra in extras:
        name = d.list_name(env, arch, extra)
        try:
            text = _read_list(name)
        except RuntimeError:
            raise ValueError(
                f"the env{env} wheel has no extra [{extra}]. Fix: pass extras from {sorted(_known_extras(env, arch))}."
            ) from None
        for n, p in d.parse_list(text, name).items():
            if n in pins and pins[n] != p and n not in core:
                raise ValueError(
                    f"extras {sorted(extras)} pin {n} to two versions. Fix: stage them in two folders."
                )
            pins[n] = p
    return pins


def _known_extras(env: int, arch: str):
    from databricks.labs.gbx import _native

    d, root = _deps_module(), _native.deps_dir() or ""
    head = f"env{env}-{arch}-"
    return {
        f[len(head) : -len(".txt")]
        for f in (os.listdir(root) if root else [])
        if f.startswith(head)
        and f.endswith(".txt")
        and f != d.list_name(env, arch, d.UC)
    }


def uc_pins():
    """name -> version of the Unity Catalog ENVIRONMENT pins (the manifest's ``uc_dependencies``)."""
    from databricks.labs.gbx import manifest

    return dict(manifest.load().uc_dependencies)


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _pip(python: str, *args: str, run=None):
    import subprocess

    run = run or subprocess.run
    res = run(
        [python, "-m", "pip", "--disable-pip-version-check", *args],
        capture_output=True,
        text=True,
    )
    if res.returncode != 0:
        tail = (res.stdout + res.stderr).strip().splitlines()[-15:]
        raise RuntimeError(
            f"pip {args[0]} exited {res.returncode}:\n  " + "\n  ".join(tail)
        )
    return res


def _stub_env(tmp: str, base, python: str, run=None) -> str:
    """A venv with no packages but stand-ins (a METADATA each) for what the runtime preinstalls (``base``, name ->
    version), so pip's dry run counts them as installed. Returns the venv's interpreter.
    """
    import subprocess

    run = run or subprocess.run
    root = os.path.join(tmp, "base-env")
    res = run(
        [python, "-m", "venv", "--without-pip", root], capture_output=True, text=True
    )
    if res.returncode != 0:
        raise RuntimeError(
            f"{python} -m venv exited {res.returncode}: {(res.stdout + res.stderr).strip()[-500:]}"
        )
    py = os.path.join(root, "Scripts" if os.name == "nt" else "bin", "python")
    res = run(
        [py, "-c", "import sysconfig; print(sysconfig.get_path('purelib'))"],
        capture_output=True,
        text=True,
    )
    site = res.stdout.strip()
    if res.returncode != 0 or not os.path.isdir(site):
        raise RuntimeError(f"the venv {root} has no site-packages ({site!r})")
    for name, ver in base.items():
        info = os.path.join(site, f"{name.replace('-', '_')}-{ver}.dist-info")
        os.makedirs(info, exist_ok=True)
        with open(os.path.join(info, "METADATA"), "w") as fh:
            fh.write(f"Metadata-Version: 2.1\nName: {name}\nVersion: {ver}\n")
        with open(os.path.join(info, "INSTALLER"), "w") as fh:
            fh.write("geobrix-stage\n")
    return py


def _requirement_lines(extras) -> list:
    """The installed env wheel's own requirements (``Requires-Dist``): the unmarked ones and those of ``extras``."""
    from importlib.metadata import requires

    out = []
    for line in requires("geobrix") or []:
        req, _, marker = line.partition(";")
        m = (
            re.fullmatch(r"""\s*extra\s*==\s*["']([^"']+)["']\s*""", marker)
            if marker
            else None
        )
        if not marker or (m and m.group(1) in extras):
            out.append(req.strip())
    return out


def check_offline(
    folder: str, arch: str, targets, base, python: str = None, run=None
) -> list:
    """Prove that ``targets`` (requirement strings or wheel paths) install from ``folder`` alone, for ``arch``: pip's dry
    run with ``--no-index``, the target platform's options, and ``base`` counted as installed. Returns the
    ``name==version`` lines pip would install; raises with pip's answer otherwise."""
    import json
    import tempfile

    d = _deps_module()
    python = python or sys.executable
    with tempfile.TemporaryDirectory(prefix="gbx-deps-") as tmp:
        stub = _stub_env(tmp, base, python, run)
        report = os.path.join(tmp, "report.json")
        try:
            _pip(
                python, "--python", stub, "install", "--dry-run", "--quiet", "--no-index",
                "--find-links", folder, "--report", report, *d.pip_target_args(arch), *targets,
                run=run,
            )  # fmt: skip
        except RuntimeError as exc:
            raise RuntimeError(
                f"the wheels in {folder} do not cover the install on {arch}: {exc}\n"
                "Fix: run `python -m databricks.labs.gbx.stage deps` again into an empty folder, with the same extras."
            ) from None
        with open(report) as fh:
            got = json.load(fh).get("install", [])
    return sorted(f"{i['metadata']['name']}=={i['metadata']['version']}" for i in got)


def download_deps(
    out: str,
    *,
    arches=None,
    extras=(),
    uc: bool = False,
    wheel: str = None,
    python: str = None,
    run=None,
) -> str:
    """Download the wheels the env wheel needs for each of ``arches`` (default both) into ``out/<arch>/`` (binary wheels
    only, each checked against the sha256 its list pins), check offline that they are complete, copy ``wheel`` (the env
    wheel file) into ``out``, and write ``out/wheels.txt``. Returns the path of wheels.txt.

    ``extras`` adds what those extras need; ``uc`` also checks the Unity Catalog pins. Runs on any machine with PyPI
    access and the env wheel installed (no checkout); ``python`` is the interpreter whose pip runs (pip 22.3 or later).
    """
    import tempfile

    from databricks.labs.gbx import __version__

    d = _deps_module()
    env = _installed_env()
    arches = tuple(arches or d.ARCHES)
    for a in arches:
        d.platforms(a)  # the arch check
    extras = tuple(sorted(set(extras)))
    python = python or sys.executable
    os.makedirs(out, exist_ok=True)
    rows = []
    targets = []
    if wheel is not None:
        name = os.path.basename(wheel)
        if name != f"geobrix-{__version__}-py3-none-any.whl":
            raise ValueError(
                f"--wheel {name} is not the installed env wheel (geobrix-{__version__}-py3-none-any.whl). "
                "Fix: pass the wheel file you installed, or install that wheel first."
            )
        dst = os.path.join(out, name)
        if os.path.abspath(wheel) != os.path.abspath(dst):
            shutil.copyfile(wheel, dst)
        rows.append((_sha256_file(dst), name))
        targets = [dst + (f"[{','.join(extras)}]" if extras else "")]
    base = d.parse_pins(_read_list(d.base_name(env)), d.base_name(env))
    with tempfile.TemporaryDirectory(prefix="gbx-deps-") as tmp:
        if not targets:
            req = os.path.join(tmp, "requires.txt")
            with open(req, "w") as fh:
                fh.write("\n".join(_requirement_lines(extras)) + "\n")
            targets = ["-r", req]
        for a in arches:
            pins = dep_pins(a, extras, env)
            if uc:
                for n, v in uc_pins().items():
                    if pins.get(d.canon(n), ("",))[0] != v:
                        raise RuntimeError(
                            f"the {a} list has no {n}=={v} (the UC pin). Fix: rebuild the env wheel."
                        )
            folder = os.path.join(out, a)
            os.makedirs(folder, exist_ok=True)
            req = os.path.join(tmp, f"{a}.txt")
            with open(req, "w") as fh:
                fh.write(d.format_list(f"geobrix {__version__} {a}", pins))
            print(
                f"geobrix: downloading {len(pins)} wheels for {a} into {folder}",
                flush=True,
            )
            _pip(
                python, "download", "--quiet", "--no-deps", "--require-hashes", *d.pip_target_args(a),
                "-r", req, "-d", folder, run=run,
            )  # fmt: skip
            have = {}
            for f in sorted(os.listdir(folder)):
                if f.endswith(".whl"):
                    have[d.wheel_name_version(f)] = f
            want = {(n, v): sha for n, (v, sha) in pins.items()}
            stray = sorted(f for k, f in have.items() if k not in want)
            gone = sorted(f"{n}=={v}" for (n, v) in want if (n, v) not in have)
            if stray or gone:
                raise RuntimeError(
                    f"{folder} holds {stray or 'no stray wheel'} beside the list and lacks {gone or 'nothing'}. "
                    "Fix: download into an empty --out folder."
                )
            for k, f in sorted(have.items()):
                sha = _sha256_file(os.path.join(folder, f))
                if sha != want[k]:
                    raise RuntimeError(
                        f"{a}/{f} has sha256 {sha}, its list pins {want[k]}. Fix: delete it and run again."
                    )
                rows.append((sha, f"{a}/{f}"))
            extra_targets = [f"{n}=={v}" for n, v in uc_pins().items()] if uc else []
            got = check_offline(folder, a, targets + extra_targets, base, python, run)
            print(
                f"geobrix: {a}: the offline install check passed ({len(got)} packages from {folder})",
                flush=True,
            )
    path = os.path.join(out, WHEELS_TXT)
    with open(path, "w") as fh:
        fh.write(
            f"# geobrix {__version__}: the wheels of an install without PyPI access "
            f"(arches {' '.join(arches)}; extras {','.join(extras) or '-'}; uc {'yes' if uc else 'no'})\n"
        )
        fh.writelines(f"{sha}  {rel}\n" for sha, rel in rows)
    if wheel is None:
        print(
            f"geobrix: {out} holds no env wheel. Fix: copy geobrix-{__version__}-py3-none-any.whl there, "
            "or run again with --wheel <path>, before gbx.stage_deps."
        )
    print(
        f"geobrix: wrote {path} ({len(rows)} files). Next: gbx.stage_deps('/Volumes/<catalog>/<schema>/<volume>/<dir>', src='{out}')"
    )
    return path


def read_wheels_txt(src: str):
    """[(sha256, relative path)] of ``src/wheels.txt``."""
    path = os.path.join(src, WHEELS_TXT)
    if not os.path.isfile(path):
        raise RuntimeError(
            f"{src} has no {WHEELS_TXT}. Fix: run `python -m databricks.labs.gbx.stage deps --out {src}` first."
        )
    rows = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith("#"):
                sha, _, rel = line.partition("  ")
                if (
                    not re.fullmatch(r"[0-9a-f]{64}", sha)
                    or not rel
                    or ".." in rel.split("/")
                    or rel.startswith("/")
                ):
                    raise RuntimeError(
                        f"{path} has a bad line {line!r}. Fix: run the deps download again."
                    )
                rows.append((sha, rel))
    return rows


class _Fuse:
    """A Volume folder through its FUSE mount (on Databricks compute)."""

    def __init__(self, root):
        self.root = root

    def same(self, rel, size, sha):
        p = os.path.join(self.root, rel)
        return (
            os.path.isfile(p) and os.path.getsize(p) == size and _sha256_file(p) == sha
        )

    def put(self, rel, local):
        p = os.path.join(self.root, rel)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        shutil.copyfile(local, p)  # a Volume (FUSE) takes no rename: copy in place

    def sha(self, rel):
        return _sha256_file(os.path.join(self.root, rel))

    def names(self):
        return os.listdir(self.root) if os.path.isdir(self.root) else []


class _FilesApi:
    """A Volume folder through the Databricks SDK Files API (off Databricks)."""

    def __init__(self, root, client):
        self.root, self.w = root, client

    def _path(self, rel):
        return f"{self.root}/{rel}"

    def same(self, rel, size, sha):
        try:
            meta = self.w.files.get_metadata(self._path(rel))
        except Exception:  # noqa: BLE001 - absent (NotFound) or unreadable: copy it
            return False
        return int(meta.content_length or -1) == size and self.sha(rel) == sha

    def put(self, rel, local):
        parent = os.path.dirname(self._path(rel))
        self.w.files.create_directory(parent)
        with open(local, "rb") as fh:
            self.w.files.upload(self._path(rel), fh, overwrite=True)

    def sha(self, rel):
        resp = self.w.files.download(self._path(rel))
        h = hashlib.sha256()
        for block in iter(lambda: resp.contents.read(1 << 20), b""):
            h.update(block)
        return h.hexdigest()

    def names(self):
        try:
            return [
                os.path.basename(e.path.rstrip("/"))
                for e in self.w.files.list_directory_contents(self.root)
            ]
        except Exception:  # noqa: BLE001 - a folder not there yet holds nothing
            return []


def _on_databricks() -> bool:
    return bool(os.environ.get("DATABRICKS_RUNTIME_VERSION")) and os.path.isdir(
        "/Volumes"
    )


def install_lines(vol_dir: str, wheel: str, arches, extras=()) -> str:
    """The install lines for a Volume folder that ``stage_deps`` filled."""
    whl = f"{vol_dir}/{wheel}" + (f"[{','.join(extras)}]" if extras else "")
    lines = [
        "geobrix: install without PyPI access (use the folder of your compute's architecture):"
    ]
    for a in arches:
        links = f"{vol_dir}/{a}"
        lines += [
            f"  {a}:",
            f"    notebook:    %pip install --no-index --find-links {links} {whl}",
            f"    Serverless:  Environment > Dependencies, one line each: --no-index | --find-links={links} | {whl}",
            f"    cluster:     Libraries > Install new > Volumes > {vol_dir}/requirements-{a}.txt",
        ]
    lines.append(
        f"  UC functions: gbx.uc.install(catalog, schema, '{vol_dir}', deps='volume')"
        + (
            ""
            if len(arches) > 1
            else f"  (the sandbox must be {arches[0]}: pass arch='{arches[0]}')"
        )
    )
    return "\n".join(lines)


def stage_deps(vol_dir: str, src: str, *, overwrite: bool = False, client=None) -> str:
    """Copy a folder that ``python -m databricks.labs.gbx.stage deps`` filled (the env wheel, ``<arch>/`` wheel folders
    and ``wheels.txt``) to ``vol_dir``, checking each file's sha256 before and after the copy, and print the install
    lines. On Databricks compute it copies through the Volume's FUSE mount; elsewhere through the Databricks SDK Files
    API (``client``, default ``WorkspaceClient()``). A file already there with the same sha256 is kept (``overwrite``
    copies it anyway). Returns ``vol_dir``."""
    import tempfile

    vol_dir = check_volume_dir(vol_dir.rstrip("/"))
    rows = read_wheels_txt(src)
    wheels = [rel for _, rel in rows if _ENV_WHEEL.fullmatch(rel)]
    if len(wheels) != 1:
        raise RuntimeError(
            f"{src}/{WHEELS_TXT} lists {len(wheels)} env wheels, expected 1. "
            "Fix: run the deps download again with --wheel <the env wheel file>."
        )
    for sha, rel in rows:
        local = os.path.join(src, rel)
        if not os.path.isfile(local) or _sha256_file(local) != sha:
            raise RuntimeError(
                f"{local} is missing or does not match the sha256 {WHEELS_TXT} lists. "
                "Fix: run the deps download again into an empty folder."
            )
    if client is None and not _on_databricks():
        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient()
    target = _Fuse(vol_dir) if client is None else _FilesApi(vol_dir, client)
    others = sorted(
        n
        for n in target.names()
        if re.fullmatch(r"geobrix-.+\.whl", n) and n != wheels[0]
    )
    if others:
        raise RuntimeError(
            f"{vol_dir} already holds {others[0]}. Fix: stage each version and environment into its own folder."
        )
    arches = sorted({rel.split("/", 1)[0] for _, rel in rows if "/" in rel})
    copied = 0
    for sha, rel in rows:
        local = os.path.join(src, rel)
        if overwrite or not target.same(rel, os.path.getsize(local), sha):
            target.put(rel, local)
            got = target.sha(rel)
            if got != sha:
                raise RuntimeError(
                    f"{vol_dir}/{rel} has sha256 {got} after the copy, not {sha}. Fix: run gbx.stage_deps again."
                )
            copied += 1
    with tempfile.TemporaryDirectory(prefix="gbx-deps-") as tmp:
        for a in arches:
            req = os.path.join(tmp, f"requirements-{a}.txt")
            with open(req, "w") as fh:
                fh.write(
                    f"--no-index\n--find-links {vol_dir}/{a}\n{vol_dir}/{wheels[0]}\n"
                )
            target.put(f"requirements-{a}.txt", req)
        target.put(WHEELS_TXT, os.path.join(src, WHEELS_TXT))
    print(
        f"geobrix: staged {len(rows)} files in {vol_dir} ({copied} copied, {len(rows) - copied} already there), "
        "each sha256-checked."
    )
    print(install_lines(vol_dir, wheels[0], arches))
    return vol_dir


def main(argv=None) -> int:
    import argparse

    p = argparse.ArgumentParser(
        prog="python -m databricks.labs.gbx.stage",
        description=__doc__.split("\n", 1)[0],
    )
    sub = p.add_subparsers(dest="cmd", required=True)
    dp = sub.add_parser(
        "deps", help="download and check the wheels of an install without PyPI access"
    )
    dp.add_argument("--arch", choices=("x86_64", "aarch64", "both"), default="both")
    dp.add_argument(
        "--extras", default="", help="comma-separated extras, e.g. zarr,vizx"
    )
    dp.add_argument(
        "--uc", action="store_true", help="also check the Unity Catalog pins"
    )
    dp.add_argument("--wheel", help="the env wheel file to stage beside the wheels")
    dp.add_argument("--out", required=True, help="the folder to fill")
    dp.add_argument(
        "--python", help="the interpreter whose pip runs (default: this one)"
    )
    a = p.parse_args(argv)
    arches = ("x86_64", "aarch64") if a.arch == "both" else (a.arch,)
    extras = tuple(e.strip() for e in a.extras.split(",") if e.strip())
    try:
        download_deps(
            a.out, arches=arches, extras=extras, uc=a.uc, wheel=a.wheel, python=a.python
        )
    except (RuntimeError, ValueError) as exc:
        print(f"geobrix: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
