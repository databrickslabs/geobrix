#!/usr/bin/env python3
"""The one pin file for the native stack (scripts/native/pins.conf) and the audit that fails on drift.

    pins.py get KEY                       print one value
    pins.py build-args                    one `--build-arg KEY=VALUE` per line (no licences or URLs)
    pins.py validate                      the pin file alone: no placeholder, checksums present and well formed
    pins.py audit [--check] [--root DIR]  every consumer against the pins; --check exits 1 on drift
    pins.py env-check [--check] LABEL=REPORT.json ...
                                          environments (JSON from scripts/ci/venv_env_report.py collect)
                                          against the pins: Python patch version, bound packages, natives

Stdlib only (the host python3 may be 3.10). The pin file is KEY=VALUE, readable by bash `source` too.
"""

import argparse
import importlib.util
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT_ROOT = HERE.parent.parent
BOUND = ("RASTERIO", "PYOGRIO", "PYPROJ", "SHAPELY", "NETCDF4")
# X_VERSION keys with no X_SHA256: tools fetched per architecture or from a registry, and versions only compared.
NO_CHECKSUM = {"RUSTUP", "CARGO_DENY", "MATURIN", "PYO3", "NUMPY_BUILD", "PYTHON"}
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
            if dep in NO_CHECKSUM:
                continue
            if f"{dep}_SHA256" not in pins and f"{dep}_SDIST_SHA256" not in pins:
                out.append(f"{name}: {key} has no {dep}_SHA256")
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


def rust_crates(root: Path, pins: dict) -> list:
    out = []
    for path in walk(root, {"Cargo.toml"}):
        text = path.read_text()
        for m in re.finditer(
            r'(?m)^\s*pyo3\s*=\s*(?:"([^"]*)"|\{[^}]*?\bversion\s*=\s*"([^"]*)")', text
        ):
            got = m.group(1) or m.group(2)
            if got != "=" + pins["PYO3_VERSION"]:
                out.append(
                    f"{path.relative_to(root)}: pyo3 is {got!r}, pins.conf says =PYO3_VERSION (={pins['PYO3_VERSION']})"
                )
    for path in walk(root, {"pyproject.toml"}):
        text = path.read_text()
        if not re.search(r'(?m)^\s*build-backend\s*=\s*"maturin"', text):
            continue
        m = re.search(r'"maturin\s*([^"]*)"', text)
        want = "==" + pins["MATURIN_VERSION"]
        if not m or m.group(1).replace(" ", "") != want:
            out.append(
                f"{path.relative_to(root)}: maturin requirement is {('maturin' + m.group(1)) if m else 'missing'}, pins.conf says maturin{want}"
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
    pins_path = root / "scripts/native/pins.conf"
    if not pins_path.exists():
        return [f"scripts/native/pins.conf: missing under {root}"]
    pins = load_pins(pins_path)
    findings = validate(pins)
    for check in (rust_toolchain, rust_crates, build_requirements, pom_property):
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
    sub.add_parser("validate")
    a = sub.add_parser("audit")
    a.add_argument("--check", action="store_true")
    a.add_argument("--root", default=str(DEFAULT_ROOT))
    e = sub.add_parser("env-check")
    e.add_argument("--check", action="store_true")
    e.add_argument("environments", nargs="+", metavar="LABEL=REPORT.json")
    args = ap.parse_args(argv)

    if args.cmd in ("get", "build-args", "validate"):
        pins = load_pins(DEFAULT_ROOT / "scripts/native/pins.conf")
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
    if args.cmd == "validate":
        findings = validate(pins)
        print("\n".join(findings) or "pins.conf: ok")
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
