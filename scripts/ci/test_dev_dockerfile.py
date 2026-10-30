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
