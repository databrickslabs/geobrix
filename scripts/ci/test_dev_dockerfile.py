"""Static checks of scripts/docker/Dockerfile (the geobrix-dev image) for the invariants of the
native arm64 image: nothing is hard-wired to amd64, each architecture gets its own pip lock,
Hadoop native libraries and library path, and the package index can only arrive as a BuildKit
secret and is never echoed.

The checks are functions of the Dockerfile text. The real file must have no problems, and each
regression the checks exist for is reproduced on a copy of the text and must be reported, so a
check that stops detecting its regression fails here.
"""

import re
from pathlib import Path

import pytest
from conftest import REPO_ROOT

DOCKERFILE = REPO_ROOT / "scripts" / "docker" / "Dockerfile"
LOCK_X86 = "requirements-dev-container.txt"
LOCK_ARM = "requirements-dev-container-aarch64.txt"


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
                stage = re.search(r"\bAS\s+(\S+)\s*$", rest, re.I).group(1)
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


# --- the dev image's GDAL build stays in lockstep with the DCS image's, and with what the JAR loads --------
#
# The dev image (scripts/docker/Dockerfile) and the Databricks Container Service image
# (scripts/docker/dcs/Dockerfile) build the same GDAL from source with the Java bindings. Until now only a
# comment said to keep them in step. The heavyweight JAR loads the JNI library by the absolute path in
# GDALManager.scala, so the image must provide exactly that path.

DCS_DOCKERFILE = REPO_ROOT / "scripts" / "docker" / "dcs" / "Dockerfile"
GDAL_MANAGER = (
    REPO_ROOT / "src/main/scala/com/databricks/labs/gbx/rasterx/gdal/GDALManager.scala"
)
DCS_REAL = DCS_DOCKERFILE.read_text()
SCALA_REAL = GDAL_MANAGER.read_text()

# The only cmake flag the DCS build has that the dev build does not: it skips building GDAL's own test
# programs. It does not change the libraries, and adding it to the dev build would invalidate the
# cached (slow) GDAL stage for nothing.
DCS_ONLY_FLAGS = {"-DBUILD_TESTING=OFF"}
JAVA_FLAGS = (
    "-DGDAL_USE_JAVA=ON",
    "-DBUILD_JAVA_BINDINGS=ON",
    "-DBUILD_PYTHON_BINDINGS=OFF",
)
JAVA_FLAG_NAMES = ("-DJAVA_HOME=", "-DJNI_INCLUDE_DIRS=")
JNI = "libgdalalljni.so"


def key_values(instrs, keywords=("ARG", "ENV")) -> dict:
    out = {}
    for i in instrs:
        if i.keyword in keywords and "=" in i.rest:
            k, v = i.rest.split("=", 1)
            out.setdefault(k.strip(), v.strip())
    return out


def cmake_flags(instrs, stage=None) -> set:
    runs = [
        i
        for i in instrs
        if i.keyword == "RUN"
        and "cmake -G Ninja" in i.rest
        and "GDAL_USE_JAVA" in i.rest
        and (stage is None or i.stage == stage)
    ]
    if not runs:
        return set()
    return set(re.findall(r'-D[A-Z_]+=(?:"[^"]*"|\S+)', runs[0].rest))


def jni_links(instrs, env: dict) -> set:
    """{(target, link)} of every `ln -s[f] <target> <link>` for the JNI library."""
    out = set()
    for i in instrs:
        if i.keyword != "RUN":
            continue
        for m in re.finditer(r"ln -sf?\s+(\S*%s)\s+(\S+)" % re.escape(JNI), i.rest):
            target, link = (
                re.sub(
                    r"\$\{?(\w+)\}?",
                    lambda v: env.get(v.group(1), v.group(0)),
                    p,
                )
                for p in m.groups()
            )
            out.add((target, link))
    return out


def lockstep_problems(dev=REAL, dcs=DCS_REAL, scala=SCALA_REAL) -> list:
    out = []
    dev_i, dcs_i = parse(dev), parse(dcs)
    dev_kv, dcs_kv = key_values(dev_i), key_values(dcs_i)

    for name in ("GDAL_VERSION", "GDAL_SHA256"):
        a, b = dev_kv.get(name), dcs_kv.get(name)
        if not a or not b:
            out.append(f"{name} is missing from the dev or the DCS Dockerfile")
        elif a != b:
            out.append(f"{name} differs: dev {a[:16]} vs DCS {b[:16]}")
    if not re.fullmatch(r"[0-9a-f]{64}", dev_kv.get("GDAL_SHA256", "")):
        out.append("the dev Dockerfile does not pin GDAL_SHA256")

    dev_flags = cmake_flags(dev_i, "gdal-builder")
    dcs_flags = cmake_flags(dcs_i)
    for label, flags in (("dev", dev_flags), ("DCS", dcs_flags)):
        if not flags:
            out.append(f"the {label} Dockerfile has no GDAL cmake build")
            continue
        for flag in JAVA_FLAGS:
            if flag not in flags:
                out.append(f"the {label} GDAL build lacks the Java-binding flag {flag}")
        for name in JAVA_FLAG_NAMES:
            if not any(f.startswith(name) for f in flags):
                out.append(f"the {label} GDAL build lacks {name}...")
    if dev_flags and dcs_flags:
        if dev_flags - dcs_flags:
            out.append(f"dev-only cmake flags: {sorted(dev_flags - dcs_flags)}")
        if (dcs_flags - dev_flags) - DCS_ONLY_FLAGS:
            out.append(
                f"cmake flags the DCS build has beyond {sorted(DCS_ONLY_FLAGS)}: "
                f"{sorted((dcs_flags - dev_flags) - DCS_ONLY_FLAGS)}"
            )

    # the path GDALManager.loadSharedObjects loads first
    m = re.search(r'loadOrNoop\("([^"]*%s)"\)' % re.escape(JNI), scala)
    if not m:
        out.append(f"GDALManager.scala no longer loads an absolute path to {JNI}")
    else:
        path = m.group(1)
        for label, instrs, kv in (("dev", dev_i, dev_kv), ("DCS", dcs_i, dcs_kv)):
            links = jni_links(instrs, kv)
            if not any(
                link == path and target.endswith(f"/lib/jni/{JNI}")
                for target, link in links
            ):
                out.append(
                    f"the {label} image does not link {path} to the built {JNI} "
                    f"(links: {sorted(links)})"
                )
    return out


def test_the_dev_and_dcs_gdal_builds_are_in_lockstep():
    assert lockstep_problems() == []


def test_the_one_allowed_flag_difference_is_the_documented_one():
    dev = cmake_flags(parse(REAL), "gdal-builder")
    dcs = cmake_flags(parse(DCS_REAL))
    assert dcs - dev == DCS_ONLY_FLAGS and dev - dcs == set()


def mutate_in(text: str, old: str, new: str) -> str:
    assert old in text, f"fixture drifted: {old!r}"
    return text.replace(old, new, 1)


LOCKSTEP_MUTATIONS = {
    "dev GDAL version bumped": (
        dict(dev=mutate_in(REAL, "ARG GDAL_VERSION=3.11.4", "ARG GDAL_VERSION=3.11.5")),
        "GDAL_VERSION differs",
    ),
    "DCS GDAL version bumped": (
        dict(
            dcs=mutate_in(
                DCS_REAL, "ARG GDAL_VERSION=3.11.4", "ARG GDAL_VERSION=3.12.0"
            )
        ),
        "GDAL_VERSION differs",
    ),
    "dev GDAL checksum changed": (
        dict(
            dev=mutate_in(REAL, "ENV GDAL_SHA256=0fa36ee3", "ENV GDAL_SHA256=1fa36ee3")
        ),
        "GDAL_SHA256 differs",
    ),
    "DCS GDAL checksum changed": (
        dict(dcs=mutate_in(DCS_REAL, "GDAL_SHA256=0fa36ee3", "GDAL_SHA256=2fa36ee3")),
        "GDAL_SHA256 differs",
    ),
    "dev GDAL checksum unpinned": (
        dict(
            dev=re.sub(r"ENV GDAL_SHA256=[0-9a-f]{64}", "ENV GDAL_SHA256=", REAL),
            dcs=re.sub(r"ARG GDAL_SHA256=[0-9a-f]{64}", "ARG GDAL_SHA256=", DCS_REAL),
        ),
        "does not pin GDAL_SHA256",
    ),
    "dev loses the Java bindings flag": (
        dict(dev=mutate_in(REAL, "-DBUILD_JAVA_BINDINGS=ON", "")),
        "dev GDAL build lacks the Java-binding flag -DBUILD_JAVA_BINDINGS=ON",
    ),
    "dev loses GDAL_USE_JAVA": (
        dict(dev=mutate_in(REAL, "-DGDAL_USE_JAVA=ON", "-DGDAL_USE_JAVA=OFF")),
        "dev GDAL build lacks the Java-binding flag -DGDAL_USE_JAVA=ON",
    ),
    "dev loses the JNI include dirs": (
        dict(dev=mutate_in(REAL, "-DJNI_INCLUDE_DIRS=", "-DJNI_INCLUDE_X=")),
        "dev GDAL build lacks -DJNI_INCLUDE_DIRS=",
    ),
    "DCS loses the Java bindings flag": (
        dict(dcs=mutate_in(DCS_REAL, "-DBUILD_JAVA_BINDINGS=ON", "")),
        "DCS GDAL build lacks the Java-binding flag -DBUILD_JAVA_BINDINGS=ON",
    ),
    "dev gains a cmake flag": (
        dict(
            dev=mutate_in(
                REAL,
                "-DBUILD_PYTHON_BINDINGS=OFF",
                "-DBUILD_PYTHON_BINDINGS=OFF -DGDAL_USE_PARQUET=ON",
            )
        ),
        "dev-only cmake flags",
    ),
    "DCS gains a flag beyond the allowlist": (
        dict(
            dcs=mutate_in(
                DCS_REAL,
                "-DBUILD_TESTING=OFF",
                "-DBUILD_TESTING=OFF -DGDAL_BUILD_OPTIONAL_DRIVERS=OFF",
            )
        ),
        "cmake flags the DCS build has beyond",
    ),
    "dev JNI link removed": (
        dict(
            dev=mutate_in(
                REAL,
                "ln -s $ROOTDIR/lib/jni/libgdalalljni.so /usr/lib/libgdalalljni.so && \\\n    ",
                "",
            )
        ),
        "dev image does not link /usr/lib/libgdalalljni.so",
    ),
    "dev JNI link points at libgdal": (
        dict(
            dev=mutate_in(
                REAL,
                "ln -s $ROOTDIR/lib/jni/libgdalalljni.so /usr/lib/libgdalalljni.so",
                "ln -s $ROOTDIR/lib/libgdal.so /usr/lib/libgdalalljni.so",
            )
        ),
        "dev image does not link /usr/lib/libgdalalljni.so",
    ),
    "dev JNI link moved to /usr/lib/jni only": (
        dict(
            dev=mutate_in(
                REAL,
                "ln -s $ROOTDIR/lib/jni/libgdalalljni.so /usr/lib/libgdalalljni.so",
                "ln -s $ROOTDIR/lib/jni/libgdalalljni.so /usr/lib/gdal/libgdalalljni.so",
            )
        ),
        "dev image does not link /usr/lib/libgdalalljni.so",
    ),
    "DCS JNI link removed": (
        dict(
            dcs=mutate_in(
                DCS_REAL,
                "ln -sf /usr/local/lib/jni/libgdalalljni.so /usr/lib/libgdalalljni.so && \\\n    ",
                "",
            )
        ),
        "DCS image does not link /usr/lib/libgdalalljni.so",
    ),
    "the JAR loads another path": (
        dict(
            scala=mutate_in(
                SCALA_REAL,
                'loadOrNoop("/usr/lib/libgdalalljni.so")',
                'loadOrNoop("/opt/gdal/libgdalalljni.so")',
            )
        ),
        "dev image does not link /opt/gdal/libgdalalljni.so",
    ),
    "the JAR stops loading the JNI by path": (
        dict(
            scala=mutate_in(
                SCALA_REAL,
                'loadOrNoop("/usr/lib/libgdalalljni.so")',
                "loadOrNoop(jniPath)",
            )
        ),
        "GDALManager.scala no longer loads an absolute path",
    ),
}


@pytest.mark.parametrize("name", sorted(LOCKSTEP_MUTATIONS))
def test_each_lockstep_regression_is_reported(name):
    kwargs, expected = LOCKSTEP_MUTATIONS[name]
    found = lockstep_problems(**kwargs)
    assert any(expected in p for p in found), (expected, found)
