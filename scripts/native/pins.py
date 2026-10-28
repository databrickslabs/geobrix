#!/usr/bin/env python3
"""The pin files of the native stack and the audit that fails on drift.

scripts/native/pins.conf pins GDAL, PROJ, GEOS, every library they are built from and the build toolchain; its
KEY=VALUE lines are the cache key of the GDAL build. scripts/native/rust-pins.conf pins what feeds no GDAL build
(PyO3, the georust crates, the rust-native stage's libclang), so a crate bump never rebuilds GDAL.

    pins.py get KEY                       print one value (from either file)
    pins.py build-args                    one `--build-arg KEY=VALUE` per line of pins.conf (no licences or URLs)
    pins.py sources                       one tab-separated `FILE SHA256 URL URL2` line per source tarball (not tools or sdists)
    pins.py validate                      the pin files alone: no placeholder, checksums present and well formed
    pins.py canonical [--root DIR]        pins.conf as the image build sees it: its KEY=VALUE lines, no comment or blank line
    pins.py tag-hash ARCH [--root DIR]    the native image tag: 12 hex of a sha256 over the canonical pins.conf, the
                                          toolchain stage of build.Dockerfile and the files that stage copies
    pins.py audit [--check] [--root DIR]  every consumer against the pins; --check exits 1 on drift
    pins.py env-check [--check] LABEL=REPORT.json ...
                                          environments (JSON from scripts/ci/venv_env_report.py collect)
                                          against the pins: Python patch version, bound packages, natives

Stdlib only (the host python3 may be 3.10). The pin file is KEY=VALUE, readable by bash `source` too.
"""

import argparse
import hashlib
import importlib.util
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT_ROOT = HERE.parent.parent
BOUND = ("RASTERIO", "PYOGRIO", "PYPROJ", "SHAPELY", "NETCDF4")
# X_VERSION keys with no X_SHA256: tools fetched per architecture or from a registry, and versions only compared.
# Their URLs (RUSTUP_URL, CARGO_DENY_URL) are still checked for the version. The CRATE_<name>_VERSION pins are
# crates too: their checksums are in each Cargo.lock, which the audit checks against the pin. CLANG_LIBS is a
# distribution package (rpm checks its signature).
NO_CHECKSUM = {
    "RUSTUP",
    "CARGO_DENY",
    "MATURIN",
    "PYO3",
    "NUMPY_BUILD",
    "PYTHON",
    "CLANG_LIBS",
}
NATIVE_PINS = "scripts/native/pins.conf"
RUST_PINS = "scripts/native/rust-pins.conf"
CRATE_PIN = re.compile(r"CRATE_([A-Z0-9_]+)_VERSION")
# keys that belong in rust-pins.conf: in pins.conf they would rebuild GDAL on every bump
RUST_ONLY = re.compile(r"CRATE_[A-Z0-9_]+_VERSION|PYO3_VERSION|CLANG_LIBS_VERSION")
# what the toolchain stage of build.Dockerfile copies besides the pins (the image tag hashes them)
TOOLCHAIN_FILES = (
    "scripts/native/build-requirements-{arch}.txt",
    "scripts/native/cargo-offline.toml",
    "rust-toolchain.toml",
)
LOCKED = re.compile(r'(?m)^\[\[package\]\]\nname = "([^"]+)"\nversion = "([^"]+)"')
PLACEHOLDER = re.compile(r"^$|[<>]|\bTODO\b|\bTBD\b|CHANGEME|\bXXX+\b", re.I)
SKIP_DIRS = {
    ".git",
    "target",
    "node_modules",
    "build",
    "vendor",
    ".venv-pyrx",
    ".venv-connectors",
    "m2",
}


def load_pins(path: Path) -> dict:
    pins = {}
    for n, raw in enumerate(path.read_text().splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if not sep or not re.fullmatch(r"[A-Z][A-Z0-9_]*", key):
            raise SystemExit(f"{path}: line {n} is not KEY=VALUE")
        if len(value) > 1 and value[0] == value[-1] == '"':
            value = value[1:-1]
        pins[key] = value
    return pins


def load_all(root: Path) -> dict:
    """pins.conf and rust-pins.conf as one mapping; a key set in both is an error."""
    native = load_pins(root / NATIVE_PINS)
    rust_path = root / RUST_PINS
    rust = load_pins(rust_path) if rust_path.exists() else {}
    both = sorted(set(native) & set(rust))
    if both:
        raise SystemExit(f"{NATIVE_PINS} and {RUST_PINS} both set {', '.join(both)}")
    return {**native, **rust}


def canonical(path: Path) -> str:
    """The KEY=VALUE lines of a pin file, stripped, in file order: what the image build copies and its tag hashes,
    so a comment or blank-line edit changes neither."""
    lines = [ln.strip() for ln in path.read_text().splitlines()]
    return "".join(f"{ln}\n" for ln in lines if ln and not ln.startswith("#"))


def toolchain_stage(dockerfile: str) -> str:
    """The toolchain stage of build.Dockerfile: the lines after `FROM ... AS toolchain`, up to the next FROM."""
    out, inside = [], False
    for line in dockerfile.splitlines():
        if re.match(r"FROM .* AS toolchain$", line):
            inside = True
        elif inside and line.startswith("FROM "):
            break
        elif inside:
            out.append(line + "\n")
    return "".join(out)


def tag_hash(root: Path, arch: str) -> str:
    """12 hex of a sha256 over the canonical pins.conf, the toolchain stage and the files it copies: the tag of
    every native image (geobrix-native-build:<target>-<hash>-<arch>)."""
    h = hashlib.sha256(canonical(root / NATIVE_PINS).encode())
    h.update(toolchain_stage((root / "scripts/native/build.Dockerfile").read_text()).encode())
    for rel in TOOLCHAIN_FILES:
        h.update((root / rel.format(arch=arch)).read_bytes())
    return h.hexdigest()[:12]


def version_forms(version: str) -> list:
    """The spellings a version takes in a URL or file name: 1.2.3, 1_2_3, and SQLite's 3540100 for 3.54.1."""
    forms = [re.escape(version), re.escape(version.replace(".", "_"))]
    parts = version.split(".")
    if len(parts) == 3 and all(p.isdigit() for p in parts):
        forms.append(f"{parts[0]}{int(parts[1]):02d}{int(parts[2]):02d}00")
    return forms


def validate(pins: dict, name: str = "scripts/native/pins.conf") -> list:
    """Findings for the pin file alone."""
    out = []
    for key, value in sorted(pins.items()):
        if PLACEHOLDER.search(value):
            out.append(f"{name}: {key} is empty or a placeholder")
        if "SHA256" in key and not re.fullmatch(r"[0-9a-f]{64}", value):
            out.append(f"{name}: {key} is not a sha256")
        if "SHA512" in key and not re.fullmatch(r"[0-9a-f]{128}", value):
            out.append(f"{name}: {key} is not a sha512")
        if key.endswith("_VERSION"):
            dep = key[: -len("_VERSION")]
            if (
                dep not in NO_CHECKSUM
                and not CRATE_PIN.fullmatch(key)
                and f"{dep}_SHA256" not in pins
                and f"{dep}_SDIST_SHA256" not in pins
            ):
                out.append(f"{name}: {key} has no {dep}_SHA256")
            # a bump moves the version, the URLs, the file name and the checksum together
            spelled = re.compile(
                r"(?<![\d.])(?:%s)(?![\d])" % "|".join(version_forms(value))
            )
            for suffix in ("URL", "URL2", "FILE", "SDIST_FILE"):
                other = pins.get(f"{dep}_{suffix}")
                if other not in (None, "single-source") and not spelled.search(other):
                    out.append(
                        f"{name}: {key}={value} does not appear in {dep}_{suffix}; move the version, URL, file name and checksum together"
                    )
    return out


def rust_toolchain(root: Path, pins: dict) -> list:
    path = root / "rust-toolchain.toml"
    if not path.exists():
        return ["rust-toolchain.toml: missing"]
    m = re.search(r'(?m)^\s*channel\s*=\s*"([^"]+)"', path.read_text())
    if not m or m.group(1) != pins["RUST_TOOLCHAIN"]:
        return [
            f"rust-toolchain.toml: channel is {m.group(1) if m else 'missing'}, pins.conf says RUST_TOOLCHAIN={pins['RUST_TOOLCHAIN']}"
        ]
    return []


def walk(root: Path, names):
    for p in sorted(root.rglob("*")):
        if p.name in names and not (set(p.relative_to(root).parts[:-1]) & SKIP_DIRS):
            yield p


DEPENDENCY_SECTION = re.compile(r"(^|\.)(dev-|build-)?dependencies$")
PYO3_TABLE = re.compile(r"(^|\.)(dev-|build-)?dependencies\.pyo3$")


def pyo3_declarations(text: str) -> list:
    """(kind, version, in_workspace) for each pyo3 declaration in a Cargo.toml: kind is 'version' or
    'inherited'. Covers `pyo3 = "x"`, `pyo3 = { version = "x" }`, `pyo3.version`, `pyo3.workspace = true`,
    `pyo3 = { workspace = true }` and the `[dependencies.pyo3]` table form."""
    out, section, table = [], "", None
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        head = re.fullmatch(r"\[([^\[\]]+)\]", line)
        if head:
            if table is not None and not table["seen"]:
                out.append(("version", None, table["ws"]))
            section = head.group(1).strip()
            ws = section.startswith("workspace.")
            table = (
                {"seen": False, "ws": ws}
                if PYO3_TABLE.search(section.removeprefix("workspace."))
                else None
            )
            continue
        ws = section.startswith("workspace.")
        bare = section.removeprefix("workspace.")
        if table is not None:
            m = re.match(r'version\s*=\s*"([^"]*)"', line)
            if m:
                out.append(("version", m.group(1), table["ws"]))
                table["seen"] = True
            elif re.match(r"workspace\s*=\s*true", line):
                out.append(("inherited", None, table["ws"]))
                table["seen"] = True
            continue
        if not DEPENDENCY_SECTION.search(bare):
            continue
        if re.match(r"pyo3\.workspace\s*=\s*true", line):
            out.append(("inherited", None, ws))
            continue
        m = re.match(r'pyo3\.version\s*=\s*"([^"]*)"', line)
        if m:
            out.append(("version", m.group(1), ws))
            continue
        m = re.match(r"pyo3\s*=\s*(.*)", line)
        if m:
            value = m.group(1)
            if value.startswith('"'):
                out.append(("version", value.split('"')[1], ws))
            elif re.search(r"\bworkspace\s*=\s*true", value):
                out.append(("inherited", None, ws))
            else:
                v = re.search(r'\bversion\s*=\s*"([^"]*)"', value)
                out.append(("version", v.group(1) if v else None, ws))
    if table is not None and not table["seen"]:
        out.append(("version", None, table["ws"]))
    return out


def rust_crates(root: Path, pins: dict) -> list:
    out, inheriting, workspace_pinned = [], [], False
    want = "=" + pins["PYO3_VERSION"]
    for path in walk(root, {"Cargo.toml"}):
        rel = path.relative_to(root)
        for kind, version, in_workspace in pyo3_declarations(path.read_text()):
            if kind == "inherited":
                inheriting.append(rel)
            elif version != want:
                out.append(
                    f"{rel}: pyo3 is {version!r}, rust-pins.conf says {want!r} (=PYO3_VERSION)"
                )
            elif in_workspace:
                workspace_pinned = True
    if inheriting and not workspace_pinned:
        out += [
            f"{rel}: pyo3 is inherited from the workspace, but no [workspace.dependencies] entry pins {want!r} (=PYO3_VERSION)"
            for rel in inheriting
        ]
    for path in walk(root, {"pyproject.toml"}):
        text = path.read_text()
        if not re.search(r"(?m)^\s*build-backend\s*=\s*[\"']maturin[\"']", text):
            continue
        requires = re.search(r"(?ms)^\s*requires\s*=\s*\[(.*?)\]", text)
        m = (
            re.search(r"[\"']maturin\s*([^\"']*)[\"']", requires.group(1))
            if requires
            else None
        )
        wanted = "==" + pins["MATURIN_VERSION"]
        if not m or m.group(1).replace(" ", "") != wanted:
            out.append(
                f"{path.relative_to(root)}: maturin requirement is {('maturin' + m.group(1)) if m else 'missing'}, pins.conf says maturin{wanted}"
            )
    return out


def crate_locks(root: Path, pins: dict) -> list:
    """Every Cargo.lock against the CRATE_<name>_VERSION pins: a locked crate of a pinned name at another version,
    and a pin that no Cargo.lock locks (a stale pin)."""
    want = {
        m.group(1).lower().replace("_", "-"): (key, value)
        for key, value in pins.items()
        if (m := CRATE_PIN.fullmatch(key))
    }
    out, seen = [], set()
    for path in walk(root, {"Cargo.lock"}):
        for name, version in LOCKED.findall(path.read_text()):
            key, pinned = want.get(name.replace("_", "-"), (None, version))
            seen.add(name.replace("_", "-"))
            if version != pinned:
                out.append(
                    f"{path.relative_to(root)}: locks {name} {version}, rust-pins.conf says {key}={pinned}"
                )
    for name in sorted(set(want) - seen):
        out.append(
            f"{RUST_PINS}: {want[name][0]} pins {name}, which no Cargo.lock locks (a stale pin: drop it)"
        )
    return out


def build_requirements(root: Path, pins: dict) -> list:
    path = root / "scripts/native/build-requirements.in"
    if not path.exists():
        return []
    text = path.read_text()
    out = []
    for name, key in (("maturin", "MATURIN_VERSION"), ("numpy", "NUMPY_BUILD_VERSION")):
        m = re.search(r"(?mi)^\s*%s\s*==\s*([^\s#;]+)" % name, text)
        if not m or m.group(1) != pins[key]:
            out.append(
                f"scripts/native/build-requirements.in: {name} is {m.group(1) if m else 'not pinned'}, pins.conf says {key}={pins[key]}"
            )
    return out


def pom_property(root: Path, pins: dict) -> list:
    path = root / "pom.xml"
    if not path.exists():
        return []
    m = re.search(
        r"<gdal\.java\.version>([^<]*)</gdal\.java\.version>", path.read_text()
    )
    want = f"{pins['GDAL_VERSION']}-gbx{pins['GBX_NATIVE_REVISION']}"
    if m and m.group(1) != want:
        return [f"pom.xml: gdal.java.version is {m.group(1)}, pins.conf says {want}"]
    return []


def legacy(root: Path) -> list:
    path = root / "scripts/native/legacy-consumers.txt"
    out = []
    for n, raw in enumerate(path.read_text().splitlines(), 1):
        if not raw.strip() or raw.startswith("#"):
            continue
        parts = raw.split("|")
        if len(parts) != 4:
            out.append(
                f"scripts/native/legacy-consumers.txt: line {n} needs path|regex|legacy value|retired by (the regex must not contain '|')"
            )
            continue
        rel, regex, want, _ = parts
        target = root / rel
        if not target.exists():
            out.append(
                f"{rel}: listed in legacy-consumers.txt but missing; delete its line"
            )
            continue
        found = re.findall(regex, target.read_text())
        if not found:
            out.append(
                f"{rel}: no longer matches its legacy line (consumer retired?); delete line {n} of scripts/native/legacy-consumers.txt"
            )
        elif len(found) > 1:
            out.append(
                f"{rel}: the legacy regex matches {len(found)} times, it must match once"
            )
        elif found[0] != want:
            out.append(
                f"{rel}: pins {found[0]}, legacy-consumers.txt says {want} (a consumer drifted; move it to pins.conf or update the line)"
            )
    return out


def build_dockerfile(root: Path) -> list:
    path = root / "scripts/native/build.Dockerfile"
    if not path.exists():
        return []
    out = []
    for n, line in enumerate(path.read_text().splitlines(), 1):
        m = re.match(r"\s*ARG\s+\w+=(\S+)", line)
        if m and (
            re.search(r"\d+\.\d+", m.group(1))
            or re.fullmatch(r"[0-9a-f]{40,}", m.group(1))
        ):
            out.append(
                f"scripts/native/build.Dockerfile: line {n} gives an ARG a literal version ({line.strip()}); it must come from pins.conf"
            )
    return out


def audit(root: Path) -> list:
    pins_path = root / NATIVE_PINS
    if not pins_path.exists():
        return [f"{NATIVE_PINS}: missing under {root}"]
    native = load_pins(pins_path)
    rust = load_pins(root / RUST_PINS) if (root / RUST_PINS).exists() else {}
    pins = load_all(root)
    findings = validate(native) + validate(rust, RUST_PINS)
    findings += [
        f"{NATIVE_PINS}: {key} belongs in {RUST_PINS} (here every bump would rebuild GDAL)"
        for key in sorted(native)
        if RUST_ONLY.fullmatch(key)
    ]
    for check in (
        rust_toolchain,
        rust_crates,
        crate_locks,
        build_requirements,
        pom_property,
    ):
        findings += check(root, pins)
    findings += legacy(root)
    findings += build_dockerfile(root)
    return findings


def env_report_module():
    """venv_env_report.py (scripts/ci), the comparison gbx:venv:check uses."""
    spec = importlib.util.spec_from_file_location(
        "venv_env_report", DEFAULT_ROOT / "scripts/ci/venv_env_report.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def env_check(labels: list) -> int:
    """Each environment against the pins, through venv_env_report's compare (the pins play the container).

    Returns the number of drifting environments.
    """
    import json

    report = env_report_module()
    pins = load_pins(DEFAULT_ROOT / "scripts/native/pins.conf")
    pinned = {
        "python": pins["PYTHON_VERSION"],
        "packages": {b.lower(): pins[f"{b}_VERSION"] for b in BOUND},
        "natives": {k: pins[f"{k.upper()}_VERSION"] for k in ("gdal", "proj", "geos")},
    }
    drifting = 0
    for item in labels:
        label, _, path = item.partition("=")
        env = json.loads(Path(path).read_text())
        rows, notes = report.compare(env, pinned)
        rows = [
            r
            for r in rows
            if r[3] == "DRIFT" or r[0] in ("python", "gdal", "proj", "geos")
        ]
        for name in sorted(pinned["packages"]):
            if name not in env["packages"]:
                rows.append((name, "-", pinned["packages"][name], "DRIFT"))
        text = report.render(rows, [])
        head, _, rest = text.partition("\n")
        print(f"{label} against scripts/native/pins.conf:")
        # the same table gbx:venv:check prints, with the pins in the container's column
        print(head.replace("host", "env ", 1).replace("container", "pinned   ", 1))
        print(rest.rstrip())
        print()
        drifting += any(r[3] == "DRIFT" for r in rows)
    return drifting


def main(argv: list) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("get")
    g.add_argument("key")
    sub.add_parser("build-args")
    sub.add_parser("sources")
    sub.add_parser("validate")
    c = sub.add_parser("canonical")
    c.add_argument("--root", default=str(DEFAULT_ROOT))
    t = sub.add_parser("tag-hash")
    t.add_argument("arch", choices=("x86_64", "aarch64"))
    t.add_argument("--root", default=str(DEFAULT_ROOT))
    a = sub.add_parser("audit")
    a.add_argument("--check", action="store_true")
    a.add_argument("--root", default=str(DEFAULT_ROOT))
    e = sub.add_parser("env-check")
    e.add_argument("--check", action="store_true")
    e.add_argument("environments", nargs="+", metavar="LABEL=REPORT.json")
    args = ap.parse_args(argv)

    if args.cmd == "canonical":
        sys.stdout.write(canonical(Path(args.root) / NATIVE_PINS))
        return 0
    if args.cmd == "tag-hash":
        print(tag_hash(Path(args.root), args.arch))
        return 0
    if args.cmd in ("build-args", "sources"):
        pins = load_pins(DEFAULT_ROOT / NATIVE_PINS)
    if args.cmd in ("get", "validate"):
        pins = load_all(DEFAULT_ROOT)
    if args.cmd == "get":
        if args.key not in pins:
            print(f"no such pin: {args.key}", file=sys.stderr)
            return 2
        print(pins[args.key])
        return 0
    if args.cmd == "build-args":
        for k, v in pins.items():
            if not k.endswith(("_LICENSE", "_URL", "_URL2")):
                print(f"--build-arg {k}={v}")
        return 0
    if args.cmd == "sources":
        for k in sorted(pins):
            dep = k[: -len("_FILE")] if k.endswith("_FILE") else None
            if dep and f"{dep}_URL" in pins and f"{dep}_SHA256" in pins:
                print(
                    "\t".join(
                        [
                            pins[k],
                            pins[f"{dep}_SHA256"],
                            pins[f"{dep}_URL"],
                            pins.get(f"{dep}_URL2", "single-source"),
                        ]
                    )
                )
        return 0
    if args.cmd == "validate":
        findings = validate(load_pins(DEFAULT_ROOT / NATIVE_PINS)) + validate(
            load_pins(DEFAULT_ROOT / RUST_PINS), RUST_PINS
        )
        print("\n".join(findings) or "pins.conf, rust-pins.conf: ok")
        return 1 if findings else 0
    if args.cmd == "env-check":
        drifting = env_check(args.environments)
        return 1 if drifting and args.check else 0
    findings = audit(Path(args.root))
    print(
        "\n".join(f"DRIFT {f}" for f in findings)
        if findings
        else "native pins: no drift"
    )
    return 1 if findings and args.check else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
