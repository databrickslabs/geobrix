"""Static checks of scripts/docker/Dockerfile (the geobrix-dev image) for the invariants of the
native arm64 image: nothing is hard-wired to amd64, each architecture gets its own pip lock,
Hadoop native libraries and library path, and the package index can only arrive as a BuildKit
secret and is never echoed.

The checks are functions of the Dockerfile text. The real file must have no problems, and each
regression the checks exist for is reproduced on a copy of the text and must be reported, so a
check that stops detecting its regression fails here.
"""

import json
import os
import platform
import re
import stat
import subprocess
from pathlib import Path

import pytest
from conftest import REPO_ROOT

DOCKERFILE = REPO_ROOT / "scripts" / "docker" / "Dockerfile"
PROBE = REPO_ROOT / "scripts" / "docker" / "gbx-native-probe.sh"
OVERLAY = REPO_ROOT / "scripts" / "docker" / "Dockerfile.gbxnative"
LOCK_X86 = "requirements-dev-container.txt"
LOCK_ARM = "requirements-dev-container-aarch64.txt"


# Official SHA-512 of apache-maven-<version>-bin.tar.gz, per version, from the Apache .sha512 files
# (archive.apache.org, downloads.apache.org and dlcdn.apache.org agree for each). A bump of MAVEN_VERSION
# in the Dockerfile needs its row here, so the version and the pinned hash cannot move apart.
MAVEN_SHA512_BY_VERSION = {
    "3.9.9": "a555254d6b53d267965a3404ecb14e53c3827c09c3b94b5678835887ab404556bfaf78dcfe03ba76fa2508649dca8531c74bca4d5846513522404d48e8c4ac8b",
    "3.9.16": "831a8591fe20c8243b1dbe7d71e3244f31d1665b0804b2e825e38cbbe5ce0cafb8338851f90780735568773e0a6cd07bbec107cda0b896b008b861075358b6f6",
}


class Instr:
    def __init__(self, stage, keyword, rest):
        self.stage, self.keyword, self.rest = stage, keyword, rest

    @property
    def text(self):
        return f"{self.keyword} {self.rest}"


def parse(text: str) -> list:
    """The Dockerfile's instructions (continuation lines joined, comments dropped), each with
    the stage it belongs to."""
    out, stage, buf = [], "(global)", ""
    for raw in text.splitlines():
        line = raw.rstrip()
        if line.lstrip().startswith("#") or (not buf and not line.strip()):
            continue
        if line.endswith("\\"):
            buf += line[:-1] + " "
            continue
        buf += line
        m = re.match(r"\s*(\w+)\s+(.*)", buf)
        if m:
            keyword, rest = m.group(1).upper(), m.group(2)
            if keyword == "FROM":
                named = re.search(r"\bAS\s+(\S+)\s*$", rest, re.I)
                stage = named.group(1) if named else "(unnamed)"
            out.append(Instr(stage, keyword, rest))
        buf = ""
    return out


def in_stage(instrs, stage):
    return [i for i in instrs if i.stage == stage]


def case_branches(command: str, arch_var: str) -> dict:
    """{'amd64': 'branch text', ...} of the `case "$<arch_var>" in ... esac` in a RUN."""
    m = re.search(r'case "\$%s" in(.*?)esac' % re.escape(arch_var), command, re.S)
    if not m:
        return {}
    branches = {}
    for b in re.finditer(r"([\w*]+)\)(.*?);;", m.group(1), re.S):
        branches[b.group(1)] = b.group(2)
    return branches


def problems(text: str) -> list:
    instrs = parse(text)
    out = []

    # --- no platform default; the build's --platform decides
    if "BUILDPLATFORM" in text.replace("BuildKit", ""):
        out.append(
            "BUILDPLATFORM is referenced (no platform default belongs in the Dockerfile)"
        )
    for i in instrs:
        if i.keyword == "FROM" and "--platform" in i.rest:
            out.append(f"FROM pins a platform: {i.text}")

    # --- the package index is a secret, never an ARG/ENV, and never echoed
    for i in instrs:
        if i.keyword in ("ARG", "ENV") and re.search(
            r"INDEX_URL|\bPIP_INDEX\b|\bUV_INDEX\b|UV_DEFAULT_INDEX|PIP_EXTRA_INDEX",
            i.rest,
            re.I,
        ):
            out.append(
                f"the package index is carried by {i.keyword}: {i.text.split('=')[0]}"
            )
    secret_runs = [
        i
        for i in instrs
        if i.keyword == "RUN" and "/run/secrets/pip_index_url" in i.rest
    ]
    if not any("--mount=type=secret,id=pip_index_url" in i.rest for i in secret_runs):
        out.append("no RUN mounts the BuildKit secret pip_index_url")
    for i in secret_runs:
        for m in re.finditer(r"/run/secrets/pip_index_url", i.rest):
            around = i.rest[max(0, m.start() - 8) : m.end() + 2]
            if not (
                "-s " in around
                or 'idx="$(cat' in i.rest[max(0, m.start() - 12) : m.start()]
            ):
                out.append(
                    f"the secret is read other than into a variable: ...{around}..."
                )
        for cmd in re.split(r";|&&", i.rest):
            if not re.search(r"\$\{?idx\b|\$\(cat /run/secrets", cmd):
                continue
            c = cmd.strip()
            if c.startswith("echo"):
                out.append(f"the secret is echoed: {c[:60]}")
            if c.startswith("printf") and not re.search(
                r"tee /etc/pip\.conf|> /etc/uv/uv\.toml", c
            ):
                out.append(
                    f"the secret is printed other than into a config file: {c[:60]}"
                )

    # --- the JDK path follows the image's architecture
    for i in instrs:
        if "zulu17-ca-amd64" in i.rest or "zulu17-ca-arm64" in i.rest:
            out.append(f"hard-coded JDK path: {i.text[:70]}")
    jdk = [i for i in instrs if i.keyword == "RUN" and "/usr/lib/jvm/zulu-17" in i.rest]
    if not jdk or "dpkg --print-architecture" not in jdk[0].rest:
        out.append("the JDK symlink does not use dpkg --print-architecture")

    # --- each architecture installs its own lock
    locks = [i for i in instrs if i.keyword == "RUN" and "lock=" in i.rest]
    if not locks:
        out.append("no RUN selects the pip lock by architecture")
    else:
        b = case_branches(locks[0].rest, "arch")
        want = {"amd64": LOCK_X86, "arm64": LOCK_ARM}
        for arch, lock in want.items():
            m = re.search(r"lock=(\S+?)\s*$", b.get(arch, "").strip())
            if not m or not m.group(1).endswith(f"/{lock}"):
                out.append(
                    f"{arch} does not install {lock} (got {m.group(1) if m else None})"
                )
        if "*" not in b or "exit 1" not in b["*"]:
            out.append("an unsupported architecture does not fail the lock selection")
    copies = " ".join(i.rest for i in instrs if i.keyword == "COPY")
    for lock in (LOCK_X86, LOCK_ARM):
        if lock not in copies:
            out.append(f"{lock} is not COPYed into the image")

    # --- each architecture installs its own Hadoop release, with its own pinned SHA-512
    envs = {
        i.rest.split("=", 1)[0]: i.rest.split("=", 1)[1]
        for i in instrs
        if i.keyword == "ENV" and "=" in i.rest
    }
    sums = {a: envs.get(f"HADOOP_SHA512_{a}", "") for a in ("AMD64", "ARM64")}
    for a, v in sums.items():
        if not re.fullmatch(r"[0-9a-f]{128}", v):
            out.append(f"HADOOP_SHA512_{a} is not a pinned SHA-512")
    if sums["AMD64"] and sums["AMD64"] == sums["ARM64"]:
        out.append("both architectures pin the same Hadoop SHA-512")
    hadoop = [i for i in instrs if i.keyword == "RUN" and "hadoop_tgz" in i.rest]
    if not hadoop:
        out.append("no RUN selects the Hadoop release by TARGETARCH")
    else:
        b = case_branches(hadoop[0].rest, "TARGETARCH")
        if not (
            "aarch64.tar.gz" in b.get("arm64", "")
            and "HADOOP_SHA512_ARM64" in b.get("arm64", "")
        ):
            out.append(
                "arm64 does not select the aarch64 Hadoop release with its own SHA-512"
            )
        if "aarch64" in b.get("amd64", "") or "HADOOP_SHA512_AMD64" not in b.get(
            "amd64", ""
        ):
            out.append(
                "amd64 does not select the x86_64 Hadoop release with its own SHA-512"
            )
        if "*" not in b or "exit 1" not in b["*"]:
            out.append("an unsupported TARGETARCH does not fail the Hadoop selection")
        if "sha512sum -c" not in hadoop[0].rest:
            out.append("the Hadoop tarball is not verified with sha512sum -c")
        if "ARG TARGETARCH" not in [i.text for i in in_stage(instrs, "hadoop-builder")]:
            out.append("hadoop-builder does not declare ARG TARGETARCH")

    # --- the library path and the final stage follow TARGETARCH
    for arch, triplet, other in (
        ("amd64", "x86_64-linux-gnu", "aarch64-linux-gnu"),
        ("arm64", "aarch64-linux-gnu", "x86_64-linux-gnu"),
    ):
        body = " ".join(
            i.text for i in in_stage(instrs, f"final-{arch}") if i.keyword == "ENV"
        )
        if f"/usr/lib/{triplet}" not in body or other in body:
            out.append(f"final-{arch} does not set LD_LIBRARY_PATH for {triplet} only")
    final = [i for i in instrs if i.keyword == "FROM" and i.stage == "final"]
    if not final or "final-${TARGETARCH}" not in final[0].rest:
        out.append("the final stage is not FROM final-${TARGETARCH}")

    # --- Maven is pinned by version AND the official SHA-512 of that version, and verified
    maven = {
        k: v
        for k, v in key_values(instrs, ("ARG",)).items()
        if k in ("MAVEN_VERSION", "MAVEN_SHA512")
    }
    version, digest = maven.get("MAVEN_VERSION"), maven.get("MAVEN_SHA512")
    if not version or not digest:
        out.append("MAVEN_VERSION and MAVEN_SHA512 must both be pinned")
    elif version not in MAVEN_SHA512_BY_VERSION:
        out.append(
            f"MAVEN_VERSION {version} has no recorded official SHA-512 in this test "
            "(add its row from the Apache .sha512 file)"
        )
    elif digest != MAVEN_SHA512_BY_VERSION[version]:
        out.append(
            f"MAVEN_SHA512 does not match the recorded official hash for Maven {version}"
        )
    maven_runs = [i for i in instrs if i.keyword == "RUN" and "apache-maven" in i.rest]
    if not maven_runs:
        out.append("no RUN installs Maven")
    else:
        run = maven_runs[0].rest
        if (
            "https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/"
            "apache-maven-${MAVEN_VERSION}-bin.tar.gz" not in run
        ):
            out.append(
                "Maven is not downloaded from archive.apache.org by MAVEN_VERSION"
            )
        if '"${MAVEN_SHA512}  /tmp/apache-maven.tar.gz" | sha512sum -c -' not in run:
            out.append("the Maven tarball is not verified with the pinned MAVEN_SHA512")

    # --- the pip stage carries the index fingerprint that keys its cache
    fpr = [
        i
        for i in instrs
        if i.keyword == "ARG" and i.rest.startswith("GBX_PIP_INDEX_FPR")
    ]
    if not fpr or not any("GBX_PIP_INDEX_FPR" in i.rest for i in secret_runs):
        out.append(
            "GBX_PIP_INDEX_FPR is not declared and used by the secret-reading RUN"
        )
    # It keys the cache of the stage that holds the pip steps: declared once, in the stage of the
    # secret-reading RUN (a copy in another stage would key the wrong layers, e.g. rebuild GDAL).
    mount_runs = [
        i for i in secret_runs if "--mount=type=secret,id=pip_index_url" in i.rest
    ]
    if mount_runs and fpr:
        stage = mount_runs[0].stage
        if [i.stage for i in fpr] != [stage]:
            out.append(
                "GBX_PIP_INDEX_FPR must be declared once, in the stage of the secret-reading RUN "
                f"({stage}); declared in {[i.stage for i in fpr]}"
            )

    # --- no x86/amd64 literal outside the places that select by architecture
    def exempt(i):
        return (
            i.stage == "final-amd64"
            or i.keyword == "FROM"
            or (i.keyword == "ENV" and i.rest.startswith("HADOOP_SHA512_AMD64"))
            or (i.keyword == "RUN" and ("case " in i.rest))
        )

    for i in instrs:
        if not exempt(i) and re.search(r"amd64|x86[_-]64", i.rest, re.I):
            out.append(f"an architecture literal outside a selection: {i.text[:70]}")
    return out


REAL = DOCKERFILE.read_text()


def test_the_dockerfile_has_no_problems():
    assert problems(REAL) == []


def mutate(old: str, new: str, count: int = 1) -> str:
    assert REAL.count(old) >= 1, f"fixture drifted: {old!r}"
    return REAL.replace(old, new, count)


SECRET_RUN = "RUN --mount=type=secret,id=pip_index_url"

MUTATIONS = {
    "index as an ARG": (
        mutate(
            "ARG GBX_PIP_INDEX_FPR=none",
            "ARG PIP_INDEX_URL=https://pypi.org/simple/\nARG GBX_PIP_INDEX_FPR=none",
        ),
        "carried by ARG",
    ),
    "index as an ENV": (
        mutate(
            "ARG GBX_PIP_INDEX_FPR=none",
            "ENV PIP_INDEX_URL=${GBX_PIP_INDEX_FPR}\nARG GBX_PIP_INDEX_FPR=none",
        ),
        "carried by ENV",
    ),
    "uv index as an ENV": (
        mutate(
            "ARG GBX_PIP_INDEX_FPR=none",
            "ENV UV_INDEX_URL=x\nARG GBX_PIP_INDEX_FPR=none",
        ),
        "carried by ENV",
    ),
    "secret mount removed": (
        mutate("--mount=type=secret,id=pip_index_url", ""),
        "no RUN mounts the BuildKit secret",
    ),
    "secret echoed": (
        mutate(
            "mkdir -p /etc/pip /root/.pip",
            'echo "$idx" && mkdir -p /etc/pip /root/.pip',
        ),
        "the secret is echoed",
    ),
    "secret read straight to the log": (
        mutate(
            'echo "package index fingerprint',
            'cat /run/secrets/pip_index_url; echo "package index fingerprint',
        ),
        "read other than into a variable",
    ),
    "secret printed to stdout": (
        mutate("| tee /etc/pip.conf", "; printf '%s' \"$idx\" | tee /etc/pip.conf"),
        "printed other than into a config file",
    ),
    "hard-coded amd64 JDK": (
        mutate("zulu17-ca-$(dpkg --print-architecture)", "zulu17-ca-amd64"),
        "hard-coded JDK path",
    ),
    "hard-coded arm64 JDK": (
        mutate("zulu17-ca-$(dpkg --print-architecture)", "zulu17-ca-arm64"),
        "hard-coded JDK path",
    ),
    "BUILDPLATFORM default": (
        "ARG BUILDPLATFORM=linux/amd64\n" + REAL,
        "BUILDPLATFORM",
    ),
    "FROM pins a platform": (
        mutate("FROM ubuntu@", "FROM --platform=linux/amd64 ubuntu@"),
        "FROM pins a platform",
    ),
    "locks swapped": (
        mutate(
            "amd64) lock=/tmp/requirements-dev-container.txt ;;",
            "amd64) lock=/tmp/requirements-dev-container-aarch64.txt ;;",
        ).replace(
            "arm64) lock=/tmp/requirements-dev-container-aarch64.txt ;;",
            "arm64) lock=/tmp/requirements-dev-container.txt ;;",
        ),
        "does not install",
    ),
    "arm64 mapped to the x86 lock": (
        mutate(
            "arm64) lock=/tmp/requirements-dev-container-aarch64.txt ;;",
            "arm64) lock=/tmp/requirements-dev-container.txt ;;",
        ),
        "arm64 does not install requirements-dev-container-aarch64.txt",
    ),
    "amd64 mapped to the arm lock": (
        mutate(
            "amd64) lock=/tmp/requirements-dev-container.txt ;;",
            "amd64) lock=/tmp/requirements-dev-container-aarch64.txt ;;",
        ),
        "amd64 does not install requirements-dev-container.txt",
    ),
    "unsupported arch not refused (lock)": (
        mutate(
            '*) echo "no dev-container lock for architecture $arch" >&2; exit 1 ;;',
            "*) lock=/tmp/requirements-dev-container.txt ;;",
        ),
        "unsupported architecture does not fail",
    ),
    "aarch64 lock not copied": (
        mutate(
            " python/geobrix/requirements-dev-container-aarch64.txt /tmp/", " /tmp/"
        ),
        "is not COPYed",
    ),
    "aarch64 Hadoop selection missing": (
        mutate(
            '"hadoop-${HADOOP_VERSION}-aarch64.tar.gz"',
            '"hadoop-${HADOOP_VERSION}.tar.gz"',
        ),
        "arm64 does not select the aarch64 Hadoop release",
    ),
    "arm64 uses the x86 Hadoop SHA": (
        mutate(
            'hadoop_sha="$HADOOP_SHA512_ARM64"', 'hadoop_sha="$HADOOP_SHA512_AMD64"'
        ),
        "arm64 does not select the aarch64 Hadoop release",
    ),
    "Hadoop arm64 branch removed": (
        re.sub(r"      arm64\) hadoop_tgz=.*?\n", "", REAL),
        "arm64 does not select the aarch64 Hadoop release",
    ),
    "Hadoop SHAs identical": (
        re.sub(
            r"(ENV HADOOP_SHA512_ARM64=)[0-9a-f]{128}",
            lambda m: m.group(1)
            + re.search(r"HADOOP_SHA512_AMD64=([0-9a-f]{128})", REAL).group(1),
            REAL,
        ),
        "same Hadoop SHA-512",
    ),
    "Hadoop not verified": (
        mutate("sha512sum -c", "sha512sum"),
        "not verified with sha512sum -c",
    ),
    "unsupported TARGETARCH not refused (Hadoop)": (
        re.sub(
            r'(\*\) echo "unsupported TARGETARCH.*?)>&2; exit 1 ;;',
            r"\1;;",
            REAL,
            flags=re.S,
        ),
        "unsupported TARGETARCH does not fail",
    ),
    "library path swapped": (
        mutate("/usr/lib/aarch64-linux-gnu", "/usr/lib/x86_64-linux-gnu"),
        "final-arm64 does not set LD_LIBRARY_PATH",
    ),
    "final stage not by TARGETARCH": (
        mutate("FROM final-${TARGETARCH} AS final", "FROM final-amd64 AS final"),
        "not FROM final-${TARGETARCH}",
    ),
    "fingerprint dropped": (
        mutate("ARG GBX_PIP_INDEX_FPR=none\n", ""),
        "GBX_PIP_INDEX_FPR is not declared",
    ),
    "fingerprint moved to the GDAL stage": (
        mutate("ARG GBX_PIP_INDEX_FPR=none\n", "").replace(
            "FROM hadoop-builder AS gdal-builder\n",
            "FROM hadoop-builder AS gdal-builder\nARG GBX_PIP_INDEX_FPR=none\n",
        ),
        "GBX_PIP_INDEX_FPR must be declared once, in the stage of the secret-reading RUN",
    ),
    "fingerprint declared in two stages": (
        mutate(
            "FROM hadoop-builder AS gdal-builder\n",
            "FROM hadoop-builder AS gdal-builder\nARG GBX_PIP_INDEX_FPR=none\n",
        ),
        "GBX_PIP_INDEX_FPR must be declared once, in the stage of the secret-reading RUN",
    ),
    "Maven version bumped without the hash": (
        mutate("ARG MAVEN_VERSION=3.9.16", "ARG MAVEN_VERSION=3.9.17"),
        "MAVEN_VERSION 3.9.17 has no recorded official SHA-512",
    ),
    "Maven version moved back without the hash": (
        mutate("ARG MAVEN_VERSION=3.9.16", "ARG MAVEN_VERSION=3.9.9"),
        "MAVEN_SHA512 does not match the recorded official hash for Maven 3.9.9",
    ),
    "Maven hash changed without the version": (
        mutate("ARG MAVEN_SHA512=831a8591", "ARG MAVEN_SHA512=a555254d"),
        "MAVEN_SHA512 does not match the recorded official hash for Maven 3.9.16",
    ),
    "Maven hash dropped": (
        re.sub(r"ARG MAVEN_SHA512=[0-9a-f]{128}", "ARG MAVEN_SHA512=", REAL),
        "MAVEN_VERSION and MAVEN_SHA512 must both be pinned",
    ),
    "Maven download not verified": (
        mutate('"${MAVEN_SHA512}  /tmp/apache-maven.tar.gz" | sha512sum -c -', "true"),
        "Maven tarball is not verified",
    ),
    "Maven from another host": (
        mutate(
            "https://archive.apache.org/dist/maven/", "https://dlcdn.apache.org/maven/"
        ),
        "Maven is not downloaded from archive.apache.org",
    ),
    "stray amd64 path": (
        mutate(
            "ARG GBX_PIP_INDEX_FPR=none",
            "ENV EXTRA_LIB=/usr/lib/x86_64-linux-gnu\nARG GBX_PIP_INDEX_FPR=none",
        ),
        "an architecture literal outside a selection",
    ),
}


@pytest.mark.parametrize("name", sorted(MUTATIONS))
def test_each_regression_is_reported(name):
    text, expected = MUTATIONS[name]
    assert text != REAL, f"the mutation {name!r} changed nothing"
    found = problems(text)
    assert any(expected in p for p in found), (expected, found)


def key_values(instrs, keywords=("ARG", "ENV")) -> dict:
    out = {}
    for i in instrs:
        if i.keyword in keywords and "=" in i.rest:
            k, v = i.rest.split("=", 1)
            out.setdefault(k.strip(), v.strip())
    return out


# --- the native-stack probe (scripts/docker/gbx-native-probe.sh) -------------------------------------


def fake_tools(bin_dir: Path, ldd_lines: str, needed: str, **versions) -> None:
    """Stand-ins for gdalinfo, pkg-config, geos-config, readelf and ldd, first on PATH."""
    tools = {
        "gdalinfo": f"echo 'GDAL {versions.get('gdal', '3.11.4')}, released 2025/01/01'",
        "pkg-config": f"echo {versions.get('proj', '9.4.0')}",
        "geos-config": f"echo {versions.get('geos', '3.12.1')}",
        # `readelf -d <file>` lists what the library needs
        "readelf": (
            "echo ' 0x0000000000000001 (NEEDED)             Shared library: [libc.so.6]'; "
            f"echo ' 0x0000000000000001 (NEEDED)             Shared library: [{needed}]'; "
            f"echo ' 0x000000000000000e (SONAME)             Library soname: [libgdalalljni.so]'"
        ),
        "ldd": f"cat <<'EOF'\n{ldd_lines}\nEOF",
    }
    for name, body in tools.items():
        path = bin_dir / name
        path.write_text(f"#!/bin/sh\n{body}\n")
        path.chmod(path.stat().st_mode | stat.S_IEXEC)


def run_probe(tmp_path: Path, *args: str) -> dict:
    env = dict(os.environ, PATH=f"{tmp_path / 'bin'}{os.pathsep}{os.environ['PATH']}")
    res = subprocess.run(
        ["bash", str(PROBE), *args], capture_output=True, text=True, env=env
    )
    assert res.returncode == 0, res.stdout + res.stderr
    return dict(line.split("=", 1) for line in res.stdout.splitlines() if "=" in line)


def test_the_native_probe_reports_the_stack_it_finds(tmp_path):
    """Legacy mode reads the system GDAL/PROJ/GEOS and where the JNI resolves its libgdal; --payload reads the
    payload's. Both derive the soname from the JNI file (never a constant) and count what ldd cannot find."""
    (tmp_path / "bin").mkdir()
    # --- the base image: one library resolved, one not found
    fake_tools(
        tmp_path / "bin",
        "\tlinux-vdso.so.1 (0x0000ffff)\n"
        "\tlibgdal.so.37 => /usr/local/lib/libgdal.so.37 (0x0000ffff)\n"
        "\tlibmissing.so.1 => not found",
        "libgdal.so.37",
    )
    legacy = run_probe(tmp_path)
    assert legacy["gdal"] == "3.11.4"
    assert (legacy["proj"], legacy["geos"]) == ("9.4.0", "3.12.1")
    assert legacy["jni_soname"] == "libgdal.so.37"
    assert legacy["jni_libgdal"] == "/usr/local/lib/libgdal.so.37"
    assert legacy["jni_unresolved"] == "1"

    # --- the native image: the payload's MANIFEST says what it was built from; its JNI needs a hashed libgdal
    root = tmp_path / "payload"
    native = root / "_native" / platform.machine()
    native.mkdir(parents=True)
    (native / "MANIFEST.json").write_text(
        json.dumps({"versions": {"gdal": "3.13.3", "proj": "9.9.0", "geos": "3.15.0"}})
    )
    fake_tools(
        tmp_path / "bin",
        "\tlibgdal-abc12345.so.39 => " + str(native / "lib/libgdal-abc12345.so.39") + " (0x1)",
        "libgdal-abc12345.so.39",
    )
    payload = run_probe(tmp_path, "--payload", str(root))
    assert (payload["gdal"], payload["proj"], payload["geos"]) == (
        "3.13.3",
        "9.9.0",
        "3.15.0",
    )
    assert payload["jni_soname"] == "libgdal-abc12345.so.39"
    assert payload["jni_libgdal"] == str(native / "lib/libgdal-abc12345.so.39")
    assert payload["jni_unresolved"] == "0"
    # the HDF5/netCDF-C default plugin directory is reported in payload mode only
    assert "hdf5_plugin_dir" in payload and "hdf5_plugin_dir" not in legacy
    # nothing of the payload is importable here: the bindings are reported missing, not guessed
    assert payload.get("rasterio_error") or payload.get("rasterio_from_payload") == "false"


def test_the_native_probe_refuses_an_unknown_argument(tmp_path):
    res = subprocess.run(
        ["bash", str(PROBE), "--bogus"], capture_output=True, text=True
    )
    assert res.returncode == 2 and "--bogus" in res.stderr


# --- the native overlay (scripts/docker/Dockerfile.gbxnative) -----------------------------------------


def overlay_problems(text: str) -> list:
    """The overlay installs from files in its build context only: no package index, no secret, no network
    except rustup's toolchain download, every pip step offline and binary-only, rustup-init checked."""
    instrs = parse(text)
    out = []
    if "# syntax=" in text:
        out.append("a # syntax= line would make the build pull a frontend image")
    if not any(i.keyword == "FROM" and i.rest.strip() == "${BASE_IMAGE}" for i in instrs):
        out.append("the overlay is not FROM ${BASE_IMAGE}")
    if any(i.keyword == "ARG" and i.rest.startswith("BASE_IMAGE") and "=" in i.rest for i in instrs):
        out.append("BASE_IMAGE has a default (it names an architecture)")
    for i in instrs:
        if i.keyword in ("ARG", "ENV") and re.search(r"INDEX_URL|UV_INDEX|PIP_INDEX", i.rest, re.I):
            out.append(f"the package index is carried by {i.keyword}")
        if i.keyword == "RUN" and "pip_index_url" in i.rest:
            out.append("a RUN reads the package index secret")
    for i in instrs:
        if i.keyword != "RUN":
            continue
        if "pip3 install" in i.rest:
            for flag in ("--no-index", "--only-binary :all:", "--no-deps"):
                if flag not in i.rest:
                    out.append(f"a pip step lacks {flag}: {i.rest[:60]}")
        # the one step with network is rustup's toolchain download; every other RUN has none
        if "rustup toolchain install" not in i.rest and "--network=none" not in i.rest:
            out.append(f"a RUN has network: {i.rest[:60]}")
        if "rustup toolchain install" in i.rest and "--network=none" in i.rest:
            out.append("the rustup toolchain download has no network")
    maturin = [i for i in instrs if i.keyword == "RUN" and "maturin" in i.rest and "pip3" in i.rest]
    if not maturin or "--require-hashes" not in maturin[0].rest:
        out.append("maturin is not installed from its hashed requirement")
    rustup = [i for i in instrs if i.keyword == "RUN" and "rustup-init" in i.rest]
    if not rustup or 'echo "${RUSTUP_INIT_SHA256}  /tmp/rust/rustup-init" | sha256sum -c -' not in rustup[0].rest:
        out.append("rustup-init is not checked against RUSTUP_INIT_SHA256")
    if not any("rustc --version | grep -q" in i.rest for i in rustup):
        out.append("the rustc version is not checked against RUST_TOOLCHAIN")
    link = [i for i in instrs if i.keyword == "RUN" and "libgdalalljni.so" in i.rest]
    if (
        not link
        or not re.search(r"ln -sfn \S+ /usr/lib/libgdalalljni\.so", link[0].rest)
        or "ldconfig" not in link[0].rest
    ):
        out.append("the JNI link or ldconfig registration is missing")
    if (
        not link
        or not re.search(
            r'ln -sfn "\$native/hdf5-plugins" /usr/local/hdf5/lib/plugin', link[0].rest
        )
    ):
        out.append("HDF5's default plugin directory is not linked to the payload's plugins")
    envs = " ".join(i.rest for i in instrs if i.keyword == "ENV")
    if "GBX_NATIVE_ROOT=/opt/gbx-native" not in envs:
        out.append("GBX_NATIVE_ROOT is not the payload's root")
    return out


OVERLAY_REAL = OVERLAY.read_text()


def test_the_native_overlay_dockerfile_has_no_problems():
    assert overlay_problems(OVERLAY_REAL) == []


OVERLAY_MUTATIONS = {
    "syntax line": ("# syntax=docker/dockerfile:1\n" + OVERLAY_REAL, "# syntax="),
    "base default": (
        OVERLAY_REAL.replace("ARG BASE_IMAGE\n", "ARG BASE_IMAGE=geobrix-dev:x\n", 1),
        "BASE_IMAGE has a default",
    ),
    "pip with an index": (
        OVERLAY_REAL.replace("pip3 install --no-index --no-deps", "pip3 install --no-deps", 1),
        "lacks --no-index",
    ),
    "pip may build from source": (
        OVERLAY_REAL.replace(" --only-binary :all:", "", 1),
        "lacks --only-binary :all:",
    ),
    "a RUN with network": (
        OVERLAY_REAL.replace("RUN --network=none set -e;", "RUN set -e;", 1),
        "a RUN has network",
    ),
    "the toolchain step with no network": (
        OVERLAY_REAL.replace(
            "RUN --mount=type=bind,source=rust,target=/tmp/rust \\\n    echo",
            "RUN --network=none --mount=type=bind,source=rust,target=/tmp/rust \\\n    echo",
            1,
        ),
        "the rustup toolchain download has no network",
    ),
    "maturin without hashes": (
        OVERLAY_REAL.replace(" --require-hashes", "", 1),
        "maturin is not installed from its hashed requirement",
    ),
    "rustup-init unchecked": (
        OVERLAY_REAL.replace('echo "${RUSTUP_INIT_SHA256}  /tmp/rust/rustup-init" | sha256sum -c - \\', "true \\", 1),
        "rustup-init is not checked",
    ),
    "rustc unchecked": (
        OVERLAY_REAL.replace('rustc --version | grep -q " ${RUST_TOOLCHAIN} "', "rustc --version", 1),
        "the rustc version is not checked",
    ),
    "no JNI link": (
        OVERLAY_REAL.replace("ln -sfn \"$native/lib/libgdalalljni.so\" /usr/lib/libgdalalljni.so;", "", 1),
        "the JNI link or ldconfig registration is missing",
    ),
    "no HDF5 plugin link": (
        OVERLAY_REAL.replace(
            'ln -sfn "$native/hdf5-plugins" /usr/local/hdf5/lib/plugin;',
            "true;",
            1,
        ),
        "HDF5's default plugin directory is not linked",
    ),
    "HDF5 plugin link to the wrong place": (
        OVERLAY_REAL.replace(
            'ln -sfn "$native/hdf5-plugins" /usr/local/hdf5/lib/plugin',
            'ln -sfn "$native/lib" /usr/local/hdf5/lib/plugin',
            1,
        ),
        "HDF5's default plugin directory is not linked",
    ),
    "an index as an ENV": (
        OVERLAY_REAL.replace("ENV RUSTUP_HOME", "ENV PIP_INDEX_URL=x RUSTUP_HOME", 1),
        "the package index is carried by ENV",
    ),
    "payload root moved": (
        OVERLAY_REAL.replace("GBX_NATIVE_ROOT=/opt/gbx-native", "GBX_NATIVE_ROOT=/opt/other", 1),
        "GBX_NATIVE_ROOT is not the payload's root",
    ),
}


@pytest.mark.parametrize("name", sorted(OVERLAY_MUTATIONS))
def test_each_overlay_regression_is_reported(name):
    text, expected = OVERLAY_MUTATIONS[name]
    assert text != OVERLAY_REAL, f"the mutation {name!r} changed nothing"
    assert any(expected in p for p in overlay_problems(text)), (
        expected,
        overlay_problems(text),
    )


def test_the_salt_files_are_ignored_by_both_names():
    """The default is ~/.cache/geobrix/pip-index-fpr-salt; GBX_PIP_INDEX_SALT_FILE may point into the checkout."""
    lines = (REPO_ROOT / ".gitignore").read_text().splitlines()
    assert ".pip-index-fpr-salt" in lines and "pip-index-fpr-salt" in lines
