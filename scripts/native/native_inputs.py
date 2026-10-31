#!/usr/bin/env python3
"""The native inputs: what CI's cached x86_64 native build and the two-architecture gate stamp are keyed on.

    native_inputs.py files --set build|gate|sources [--rev REV] [--root DIR]   the files of a set, one per line
    native_inputs.py hash --set build|gate|sources [--rev REV] [--root DIR]    the set's sha256
    native_inputs.py check-stamp [--root DIR]
        CI (build_main.yml, job arch-gate): exit 1 when the gate set's hash is not scripts/native/arch-gate.json's,
        or the stamp records a failed check; the message names the changed files and the command to run
    native_inputs.py provenance [--native-out DIR] [--dist DIR] [--root DIR]
        gbx:native:verify-arches, before it verifies: the stage-native trees and the env wheels it is about to check
        are one build, made from HEAD's native inputs (the last line is BUILT_FROM=<the wheels' git sha>)
    native_inputs.py write-stamp --verify-native ARCH=LOG ... --env-wheel N:ARCH=REPORT ... --built-from SHA
                                 [--rev REV] [--root DIR]
        gbx:native:verify-arches, after it verified: count each check's `ok:` and failure lines and write the stamp;
        a failed check (or a log without its last line) writes nothing and exits 1

The sets (only files under git count; --rev reads that commit's tree, without it the checkout's files):
  build  what the native build's output depends on, CI's cache key: scripts/native/pins.conf as the image build sees
         it (its KEY=VALUE lines: a comment edit moves nothing), scripts/native/build.Dockerfile, every recipe it
         copies or mounts from scripts/native (both architectures' spelling of ${GBX_ARCH}), rust-toolchain.toml.
         The sources, wheels and tools it copies are fixed by the sha256s in pins.conf and the hashed requirement files.
  sources  pins.conf's KEY=VALUE lines only: the key of CI's cache of the pinned source downloads.
  gate   the native inputs and the env wheel's assembly inputs, what differs by architecture and is what
         gbx:native:verify-arches exists for: build, plus the env wheel's inputs as its builder lists them (GIT_INPUTS
         of scripts/native/build_env_wheel.py, read from the same tree: the envs files with pyproject.env.toml,
         pyproject.toml, requirements-build.txt and scripts/native, the checks among them), with the package source
         narrowed to its native part (python/geobrix/src/databricks/labs/gbx/_native/**, _gbx_native_path.py) and
         without README.md, less the stamp itself. The rest of the package is pure Python, the same on both
         architectures, and tested by x86_64 CI; GDALManager.scala is not run by any gate check.

Stdlib only: CI runs it with the runner's python3.
"""

import argparse
import ast
import datetime
import hashlib
import json
import re
import subprocess
import sys
import zipfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from pins import canonical_text  # noqa: E402

DEFAULT_ROOT = HERE.parent.parent
ARCHES = ("aarch64", "x86_64")
ENVS = (5, 6)
PINS = "scripts/native/pins.conf"
DOCKERFILE = "scripts/native/build.Dockerfile"
STAMP = "scripts/native/arch-gate.json"
GATE_COMMAND = "bash scripts/commands/gbx-native-verify-arches.sh"
# The env wheel's inputs come from one list, its builder's GIT_INPUTS (what its dirty check refuses), with two explicit
# exclusions. README.md: no gate check reads it. The pure-Python package tree (PACKAGE_SOURCE): it is the same on both
# architectures and x86_64 CI tests it, while the two-architecture gate is for what differs by architecture, so only
# its native part (NATIVE_PYTHON: the payload's activation) stays in. Cost, accepted: an aarch64-only regression from a
# pure-Python change is not stamp-gated (the per-release Graviton run covers it).
WHEEL_BUILDER = "scripts/native/build_env_wheel.py"
NOT_EXERCISED = ("python/geobrix/README.md",)
PACKAGE_SOURCE = "python/geobrix/src"
NATIVE_PYTHON = ("python/geobrix/src/databricks/labs/gbx/_native", "python/geobrix/src/_gbx_native_path.py")
# Gate inputs the checks read from the checkout when they run, not from the env wheel (verify-native mounts
# scripts/native with JniSmoke.java; --verify mounts scripts/native for check-env-wheel.sh and its base_held.py): a
# change to them needs the gate re-run, not a wheel rebuild. Everything else in the set is in the wheel, or built into it.
READ_FROM_THE_CHECKOUT = (
    "scripts/native/verify-native.sh",
    "scripts/native/JniSmoke.java",
    "scripts/native/check-env-wheel.sh",
    "scripts/native/base_held.py",
)
# Build-context directories the Dockerfile copies or mounts whose content pins.conf (sha256) or the hashed requirement
# files fix (prefetch.sh checks every sum): not hashed again here.
PINNED_CONTEXT = ("src", "bin", "wheelhouse", "wheelhouse-bindings", "sdists")
CHECKS = ("verify_native",) + tuple(f"env{e}" for e in ENVS)


class Fail(Exception):
    pass


class Tree:
    """The files under git: the checkout's (rev None) or a commit's."""

    def __init__(self, root, rev=None):
        self.root, self.rev = Path(root), rev

    def _git(self, *args):
        res = subprocess.run(["git", "-C", str(self.root), *args], capture_output=True)
        if res.returncode != 0:
            raise Fail(f"git {' '.join(args)}: {res.stderr.decode().strip()}")
        return res.stdout

    def ls(self, specs):
        if self.rev:
            out = self._git("ls-tree", "-r", "-z", "--name-only", self.rev, "--", *specs)
        else:
            out = self._git("ls-files", "-z", "--", *specs)
        return sorted(p for p in out.decode().split("\0") if p)

    def read(self, rel):
        if self.rev:
            return self._git("cat-file", "blob", f"{self.rev}:{rel}")
        return (self.root / rel).read_bytes()

    def read_all(self, rels):
        """Every file's bytes; a commit's in one `git cat-file --batch`."""
        if not self.rev:
            return [(self.root / rel).read_bytes() for rel in rels]
        res = subprocess.run(
            ["git", "-C", str(self.root), "cat-file", "--batch"],
            input="".join(f"{self.rev}:{rel}\n" for rel in rels).encode(), capture_output=True,
        )
        out, pos, data = res.stdout, 0, []
        if res.returncode != 0:
            raise Fail(f"git cat-file --batch {self.rev}: {res.stderr.decode().strip()}")
        for rel in rels:
            end = out.find(b"\n", pos)
            if end < 0:
                raise Fail(f"git cat-file --batch {self.rev}: no header for {rel} (output ended early)")
            head = out[pos:end].split()
            if len(head) != 3 or head[1] != b"blob":
                raise Fail(f"git cat-file {self.rev}:{rel}: {out[pos:end].decode()}")
            start, size = end + 1, int(head[2])
            data.append(out[start:start + size])
            pos = start + size + 1
        return data


def dockerfile_inputs(text):
    """The repository files build.Dockerfile copies or bind-mounts into an image, for both architectures."""
    out = set()
    for line in re.sub(r"\\\n", " ", text).splitlines():
        toks = line.split()
        if not toks or toks[0] not in ("COPY", "RUN"):
            continue
        if toks[0] == "COPY":
            if any(t.startswith("--from=") for t in toks):
                continue
            srcs = [t for t in toks[1:] if not t.startswith("--")][:-1]
        else:
            srcs = []
            for t in toks[1:]:
                if t.startswith("--mount="):
                    kv = dict(p.split("=", 1) for p in t.removeprefix("--mount=").split(",") if "=" in p)
                    if kv.get("type") == "bind" and "source" in kv:
                        srcs.append(kv["source"])
        for src in srcs:
            for arch in ARCHES:
                s = src.replace("${GBX_ARCH}", arch).rstrip("/")
                top = s.split("/")[0]
                if top == "recipes" and "/" in s:
                    out.add("scripts/native/" + s.split("/", 1)[1])
                elif s == "rust-toolchain.toml":
                    out.add(s)
                elif top not in PINNED_CONTEXT:
                    raise Fail(
                        f"{DOCKERFILE} copies {src}, which {Path(__file__).name} does not know: hash it (the build"
                        f" set) or name its pin (PINNED_CONTEXT)"
                    )
    return out


def files(tree, which):
    build = {PINS, DOCKERFILE} | dockerfile_inputs(tree.read(DOCKERFILE).decode())
    have = set(tree.ls(sorted(build)))
    missing = sorted(build - have)
    if missing:
        raise Fail(f"{', '.join(missing)} (from {DOCKERFILE}) not under git")
    if which == "sources":
        return [PINS]
    if which == "build":
        return sorted(build)
    extra = set()
    for spec in gate_specs(tree):
        found = tree.ls([spec])
        if not found:
            raise Fail(f"no file under git matches {spec} (a gate input)")
        extra.update(found)
    return sorted((build | extra) - set(NOT_EXERCISED) - {STAMP})


def wheel_inputs(tree):
    """GIT_INPUTS of build_env_wheel.py at this tree (the env wheel's inputs), less README.md, with the package
    source narrowed to its native part (see PACKAGE_SOURCE)."""
    for node in ast.parse(tree.read(WHEEL_BUILDER).decode()).body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", "") == "GIT_INPUTS" for t in node.targets):
            specs = [s for s in ast.literal_eval(node.value) if s not in NOT_EXERCISED]
            if PACKAGE_SOURCE in specs:
                specs = [s for s in specs if s != PACKAGE_SOURCE] + list(NATIVE_PYTHON)
            return specs
    raise Fail(f"{WHEEL_BUILDER} has no GIT_INPUTS")


def gate_specs(tree):
    """The pathspecs the gate set adds to the build set."""
    return sorted(set(wheel_inputs(tree)) | {WHEEL_BUILDER} | set(READ_FROM_THE_CHECKOUT))


def file_hashes(tree, which):
    out = {}
    paths = files(tree, which)
    for rel, data in zip(paths, tree.read_all(paths)):
        if rel == PINS:
            data = canonical_text(data.decode()).encode()
        out[rel] = hashlib.sha256(data).hexdigest()
    return out


def digest(hashes):
    h = hashlib.sha256()
    for rel in sorted(hashes):
        h.update(f"{rel}\0{hashes[rel]}\n".encode())
    return h.hexdigest()


def changed(old, new):
    return sorted(k for k in set(old) | set(new) if old.get(k) != new.get(k))


def result_problems(results):
    out = []
    for arch in ARCHES:
        for check in CHECKS:
            r = (results.get(arch) or {}).get(check)
            if not r:
                out.append(f"{arch} {check}: not run")
            elif r.get("failed") != 0 or r.get("passed", 0) < 1:
                out.append(f"{arch} {check}: {r.get('passed')} passed, {r.get('failed')} failed")
    return out


def count(path, done, fail_marker):
    """`ok:` lines passed; failure lines plus a missing last line (`done`, a regex) failed."""
    p = Path(path)
    if not p.is_file():
        return {"passed": 0, "failed": 1}
    lines = p.read_text(errors="replace").splitlines()
    passed = sum(1 for ln in lines if ln.startswith("ok:"))
    failed = sum(1 for ln in lines if fail_marker in ln)
    if not any(re.match(done, ln) for ln in lines):
        failed += 1
    return {"passed": passed, "failed": failed}


def check_stamp(root):
    p = Path(root) / STAMP
    fix = (
        f"Fix: run {GATE_COMMAND} on a machine that runs both architectures (an Apple Silicon Mac with Docker: aarch64"
        f" natively, x86_64 under emulation); its provenance check names what to rebuild first, if anything"
        f" (gbx:build:native, gbx:build:wheel). Commit {STAMP}."
    )
    if not p.is_file():
        print(f"::error::ARCH GATE MISSING: no {STAMP}.")
        print(fix)
        return 1
    stamp = json.loads(p.read_text())
    now = file_hashes(Tree(root), "gate")
    h = digest(now)
    problems = []
    if stamp.get("native_inputs_sha256") != h:
        diff = changed(stamp.get("files", {}), now)
        problems.append(
            f"the native inputs changed since the two-architecture gate ran ({STAMP} names"
            f" {str(stamp.get('native_inputs_sha256'))[:12]}, this checkout is {h[:12]}); changed: {', '.join(diff)}"
        )
    problems += result_problems(stamp.get("results", {}))
    if problems:
        for pr in problems:
            print(f"::error::ARCH GATE STALE: {pr}")
        print(fix)
        return 1
    res = stamp["results"]
    print(
        f"arch gate ok: native inputs {h[:12]}, gated {stamp.get('date')} on GDAL {stamp.get('gdal_version')}; "
        + "; ".join(f"{a} " + ", ".join(f"{c} {res[a][c]['passed']}/0" for c in CHECKS) for a in ARCHES)
    )
    return 0


def uncommitted(root):
    """The native inputs (gate set) with uncommitted changes in the checkout: a file under git changed, staged or
    deleted. Not the stamp (a gate run writes it, and no set hashes it), and not an untracked file (the sets are
    git's files: none hashes one)."""
    head = Tree(root, "HEAD")
    specs = files(head, "build") + gate_specs(head) + [f":(exclude){STAMP}"]
    out = Tree(root)._git("status", "--porcelain", "--untracked-files=no", "--", *specs).decode()
    return sorted(ln[3:] for ln in out.splitlines() if ln.strip())


def provenance(root, native_out, dist):
    """The trees and wheels the gate is about to verify are one build, made from HEAD's native inputs."""
    dirty = uncommitted(root)
    if dirty:
        print(
            f"PRECONDITION MISSING: committed native inputs ({', '.join(dirty)} changed, uncommitted): the checks read"
            f" the checkout and the stamp names HEAD; commit or stash them first"
        )
        return 1
    head = Tree(root, "HEAD")
    now = file_hashes(head, "gate")
    problems, shas = [], {}
    for env in ENVS:
        wheels = sorted((Path(dist) / f"env{env}").glob("*.whl"))
        if len(wheels) != 1:
            print(
                f"PRECONDITION MISSING: one env{env} wheel in {dist}/env{env} (gbx:build:wheel --env {env});"
                f" found {len(wheels)}"
            )
            return 1
        with zipfile.ZipFile(wheels[0]) as zf:
            info = zf.read("databricks/labs/gbx/_build_info.py").decode()
            sha = re.search(r'^GIT_SHA = "([0-9a-f]{40})"', info, re.M)
            dirty = re.search(r"^GIT_DIRTY = (True|False)", info, re.M)
            if not sha or not dirty:
                problems.append(f"{wheels[0].name} records no GIT_SHA / GIT_DIRTY")
                continue
            sha = sha.group(1)
            shas[env] = sha
            if dirty.group(1) == "True":
                problems.append(f"{wheels[0].name} was built from a dirty tree (--allow-dirty): rebuild it from a commit")
            try:
                then = file_hashes(Tree(root, sha), "gate")
            except Fail as e:
                problems.append(f"{wheels[0].name} was built at {sha[:12]}, which this repository cannot read ({e})")
                then = None
            if then is not None:
                diff = [f for f in changed(then, now) if f not in READ_FROM_THE_CHECKOUT]
                if diff:
                    problems.append(
                        f"{wheels[0].name} was built at {sha[:12]}, whose native inputs differ from HEAD's ({', '.join(diff)}):"
                        f" rebuild it (gbx:build:native first when a build input changed, then gbx:build:wheel --env {env})"
                    )
            for arch in ARCHES:
                m = Path(native_out) / arch / "stage-native" / "gdal-native" / "MANIFEST.json"
                if not m.is_file():
                    print(f"PRECONDITION MISSING: {m} (gbx:build:native --arch {arch} --target stage-native)")
                    return 1
                if m.read_bytes() != zf.read(f"databricks/labs/gbx/_native/{arch}/MANIFEST.json"):
                    problems.append(
                        f"{m.parent.parent} is not the {arch} build in {wheels[0].name}: rebuild the wheel"
                        f" (gbx:build:wheel --env {env}) from this native build"
                    )
    if problems:
        for pr in problems:
            print(f"GATE FAILED: {pr}")
        return 1
    built_from = shas[5] if shas[5] == shas[6] else f"env5:{shas[5]},env6:{shas[6]}"
    print(
        "provenance ok: the stage-native trees are the wheels' native builds, made from HEAD's native inputs"
        f" ({digest(now)[:12]})"
    )
    print(f"BUILT_FROM={built_from}")
    return 0


def write_stamp(root, rev, verify_native, env_wheel, built_from):
    results = {a: {} for a in ARCHES}
    for arch in ARCHES:
        results[arch]["verify_native"] = count(
            verify_native.get(arch, ""), rf"VERIFY OK arch={arch}\b", "VERIFY FAILED"
        )
        for env in ENVS:
            results[arch][f"env{env}"] = count(
                env_wheel.get((env, arch), ""), rf"ENV_WHEEL_VERIFIED env={env} arch={arch} failures=0$", "CHECK FAILED"
            )
    problems = result_problems(results)
    if problems:
        for pr in problems:
            print(f"ARCH GATE FAILED: {pr}")
        print(f"{STAMP} not written.")
        return 1
    if rev == "HEAD":
        dirty = uncommitted(root)
        if dirty:
            print(f"ARCH GATE FAILED: {', '.join(dirty)} changed during the run (uncommitted); {STAMP} not written.")
            return 1
    tree = Tree(root, rev)
    hashes = file_hashes(tree, "gate")
    pins = dict(
        ln.split("=", 1) for ln in canonical_text(tree.read(PINS).decode()).splitlines()
    )
    stamp = {
        "written_by": GATE_COMMAND,
        "checked_by": "python3 scripts/native/native_inputs.py check-stamp (CI: build_main.yml, job arch-gate)",
        "native_inputs_sha256": digest(hashes),
        "date": datetime.datetime.now(datetime.timezone.utc).date().isoformat(),
        "gdal_version": pins["GDAL_VERSION"],
        "native_revision": pins["GBX_NATIVE_REVISION"],
        "built_from": built_from,
        "results": results,
        "files": hashes,
    }
    (Path(root) / STAMP).write_text(json.dumps(stamp, indent=2) + "\n")
    print(f"ARCH_GATE_STAMP {STAMP} native_inputs_sha256={stamp['native_inputs_sha256']} date={stamp['date']}")
    return 0


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("files", "hash"):
        s = sub.add_parser(name)
        s.add_argument("--set", dest="which", choices=("build", "gate", "sources"), required=True)
        s.add_argument("--rev")
    sub.add_parser("check-stamp")
    pv = sub.add_parser("provenance")
    pv.add_argument("--native-out", type=Path)
    pv.add_argument("--dist", type=Path)
    ws = sub.add_parser("write-stamp")
    ws.add_argument("--verify-native", action="append", default=[], metavar="ARCH=LOG")
    ws.add_argument("--env-wheel", action="append", default=[], metavar="N:ARCH=REPORT")
    ws.add_argument("--built-from", required=True)
    ws.add_argument("--rev")
    for s in sub.choices.values():
        s.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    args = ap.parse_args(argv)
    try:
        if args.cmd == "files":
            print("\n".join(files(Tree(args.root, args.rev), args.which)))
            return 0
        if args.cmd == "hash":
            print(digest(file_hashes(Tree(args.root, args.rev), args.which)))
            return 0
        if args.cmd == "check-stamp":
            return check_stamp(args.root)
        if args.cmd == "provenance":
            return provenance(
                args.root,
                args.native_out or args.root / "build" / "native" / "out",
                args.dist or args.root / "dist",
            )
        vn = dict(v.split("=", 1) for v in args.verify_native)
        ew = {}
        for v in args.env_wheel:
            key, path = v.split("=", 1)
            env, arch = key.split(":", 1)
            ew[(int(env), arch)] = path
        return write_stamp(args.root, args.rev, vn, ew, args.built_from)
    except Fail as e:
        print(f"native_inputs.py: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
