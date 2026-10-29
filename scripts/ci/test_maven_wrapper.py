"""One Maven for CI and dev: the Maven wrapper pins it, everything else must agree.

.mvn/wrapper/maven-wrapper.properties pins Maven (version, URL, SHA-256). CI runs every Maven call
through ./mvnw, the dev image and the local-CI runner image install the same version themselves
(the wrapper cannot download inside the dev container), and pom.xml's enforcer refuses a Maven below
that version. Nothing but these checks keeps the four in step, so they are functions of the file
texts: the real repo must have no problems, and each way of drifting is reproduced on a copy of the
text and must be reported.
"""

import os
import re
import shutil
import stat
import subprocess

import pytest
from conftest import REPO_ROOT

PROPS = ".mvn/wrapper/maven-wrapper.properties"
DEV_DOCKERFILE = "scripts/docker/Dockerfile"
RUNNER_DOCKERFILE = "scripts/ci-local/Dockerfile.gha-runner"
POM = "pom.xml"
# Files that run Maven in CI, scanned for bare `mvn`. jfrog-auth only detects whether `mvn` is on
# PATH (to write the Maven settings), it does not run it.
WRAPPER_ACTION = "./.github/actions/maven-wrapper"
# A step that uses one of these has installed the pinned Maven before any later step runs.
PROVIDERS = (f"uses: {WRAPPER_ACTION}", "uses: ./.github/actions/scala_build")
# What needs the pinned Maven installed first: a ./mvnw call, or a script that runs it by default.
NEEDS_WRAPPER = re.compile(
    r"\./mvnw\b|scripts/security/maven-pgp-verify|scripts/ci/fetch-delta-test-jars\.sh"
)
BARE_MVN = re.compile(r"(?<![\w./-])mvn(?![\w-])")
EXEMPT_FROM_BARE_MVN = {".github/actions/jfrog-auth/action.yml"}
INSTALLER = ".github/actions/maven-wrapper/action.yml"
PGP_VERIFY = "scripts/security/maven-pgp-verify"
PYRX_ACTION = ".github/actions/pyrx_build/action.yml"
SCRIPTS = (PGP_VERIFY, "scripts/security/maven-pgp-bootstrap")


def ci_files() -> list:
    out = sorted((REPO_ROOT / ".github" / "workflows").glob("*.yml"))
    out += sorted((REPO_ROOT / ".github" / "actions").glob("*/action.yml"))
    return [p.relative_to(REPO_ROOT).as_posix() for p in out]


def read_all() -> dict:
    names = [PROPS, DEV_DOCKERFILE, RUNNER_DOCKERFILE, POM, *SCRIPTS, *ci_files()]
    return {n: (REPO_ROOT / n).read_text() for n in names}


def code_lines(text: str) -> list:
    """The lines that are not whole-line comments."""
    return [ln for ln in text.splitlines() if not ln.lstrip().startswith("#")]


def props_of(text: str) -> dict:
    out = {}
    for ln in code_lines(text):
        if "=" in ln:
            k, v = ln.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def arg(text: str, name: str):
    m = re.search(rf"^ARG {name}=(\S*)", text, re.M)
    return m.group(1) if m else None


def problems(files: dict) -> list:
    out = []

    # --- the wrapper pins version + SHA-256 of a Maven Central zip, and ships no jar
    props = props_of(files[PROPS])
    url = props.get("distributionUrl", "")
    m = re.fullmatch(
        r"https://repo\.maven\.apache\.org/maven2/org/apache/maven/apache-maven/"
        r"(\d+\.\d+\.\d+)/apache-maven-\1-bin\.zip",
        url,
    )
    version = m.group(1) if m else None
    if not m:
        out.append(
            f"distributionUrl is not the Maven Central bin.zip of one version: {url!r}"
        )
    if not re.fullmatch(r"[0-9a-f]{64}", props.get("distributionSha256Sum", "")):
        out.append("distributionSha256Sum is missing or not a SHA-256")
    if props.get("distributionType") != "only-script":
        out.append(
            "distributionType must be only-script (no unpinned maven-wrapper.jar)"
        )
    if "wrapperUrl" in props:
        out.append("wrapperUrl is set: a wrapper jar would be an unpinned binary")

    # --- the images install the same version, with a SHA-512 that matches each other
    for name in (DEV_DOCKERFILE, RUNNER_DOCKERFILE):
        got = arg(files[name], "MAVEN_VERSION")
        if got != version:
            out.append(f"{name} installs Maven {got}, the wrapper pins {version}")
    dev512, runner512 = (
        arg(files[n], "MAVEN_SHA512") for n in (DEV_DOCKERFILE, RUNNER_DOCKERFILE)
    )
    if not dev512 or dev512 != runner512:
        out.append("the dev and local-CI images pin different (or no) MAVEN_SHA512")

    # --- the enforcer's floor is exactly that version, and its message names the fix
    m = re.search(
        r"<requireMavenVersion>\s*<version>\[([\d.]+),\)</version>(.*?)</requireMavenVersion>",
        files[POM],
        re.S,
    )
    if not m:
        out.append("pom.xml has no requireMavenVersion with an open-ended range")
    else:
        if m.group(1) != version:
            out.append(
                f"pom.xml requires Maven >= {m.group(1)}, the wrapper pins {version}"
            )
        if "./mvnw" not in m.group(2):
            out.append("the requireMavenVersion message does not name ./mvnw")
        if "gbx:docker:rebuild" not in m.group(2):
            out.append(
                "the requireMavenVersion message does not name gbx:docker:rebuild "
                "(inside the dev container ./mvnw cannot work; the image must be rebuilt)"
            )

    # --- CI: no bare mvn, and the pinned Maven is installed before anything needs it
    for name in [*SCRIPTS, *ci_files_in(files)]:
        if name in EXEMPT_FROM_BARE_MVN:
            continue
        for ln in code_lines(files[name]):
            if BARE_MVN.search(ln):
                out.append(f"{name}: bare mvn (use ./mvnw): {ln.strip()[:80]}")
    for name in SCRIPTS:
        if not re.search(r"GBX_MVN:-\$\{root\}/mvnw\}", files[name]):
            out.append(
                f"{name}: the default Maven is not the wrapper (${{GBX_MVN:-${{root}}/mvnw}})"
            )
    for name in ci_files_in(files):
        code = "\n".join(code_lines(files[name]))
        if "fetch-delta-test-jars.sh" in code and not re.search(
            r"GBX_MVN:\s*\./mvnw", code
        ):
            out.append(
                f"{name}: fetch-delta-test-jars.sh runs without GBX_MVN: ./mvnw "
                "(the script's default is the runner's bare mvn)"
            )
    action = files[INSTALLER]
    if "command -v unzip" not in action:
        out.append(
            f"{INSTALLER}: no unzip guard (the wrapper then fails with a misleading checksum error)"
        )
    if "jfrog-central" not in action:
        out.append(f"{INSTALLER}: no check for the JFrog mirror in ~/.m2/settings.xml")
    for name in ci_files_in(files):
        if name == INSTALLER:
            continue
        text = "\n".join(code_lines(files[name]))
        need = NEEDS_WRAPPER.search(text)
        if not need:
            continue
        have = [text.find(p) for p in PROVIDERS if p in text]
        if not have or min(have) > need.start():
            out.append(
                f"{name}: {need.group(0)} runs before the pinned Maven is installed"
            )
    return out


def ci_files_in(files: dict) -> list:
    return [n for n in files if n.startswith(".github/")]


REAL = read_all()


def test_the_repo_has_one_maven():
    assert problems(REAL) == []


def test_the_wrapper_script_is_in_place():
    mvnw = REPO_ROOT / "mvnw"
    assert mvnw.read_text().startswith("#!/bin/sh\n")
    assert mvnw.stat().st_mode & stat.S_IXUSR, "mvnw must be executable"
    assert "/mvnw text eol=lf" in (REPO_ROOT / ".gitattributes").read_text()
    # core.fileMode=false hides a missing exec bit locally, but a CI checkout uses the index mode.
    staged = subprocess.run(
        ["git", "ls-files", "-s", "mvnw"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    ).stdout
    assert staged.startswith(
        "100755 "
    ), f"mvnw is not executable in the index: {staged!r}"
    assert not list((REPO_ROOT / ".mvn" / "wrapper").glob("*.jar"))


# --- mutations: every anchor comes from the live files, so a consistent bump needs no edit here -------


def live_version(files: dict = REAL) -> str:
    m = re.search(r"apache-maven-(\d+\.\d+\.\d+)-bin\.zip", files[PROPS])
    assert m, "the wrapper properties have no apache-maven-<version>-bin.zip URL"
    return m.group(1)


def bumped(version: str, part: str = "patch") -> str:
    major, minor, patch = version.split(".")
    if (
        part == "minor"
    ):  # 3.9.16 -> 3.10.0: a two-digit minor must still pass the checks
        return f"{major}.{int(minor) + 1}.0"
    return f"{major}.{minor}.{int(patch) + 1}"


STALE = "0.0.1"  # a version no repo ever pins


def sub(name: str, pattern: str, repl: str, flags: int = 0) -> dict:
    """REAL with the first match of a structural pattern replaced in one file."""
    new, n = re.subn(pattern, repl, REAL[name], count=1, flags=flags)
    assert n == 1, f"fixture drifted: {pattern!r} not in {name}"
    return {**REAL, name: new}


def set_maven_arg(text: str, version: str) -> str:
    return re.sub(r"(?m)^(ARG MAVEN_VERSION=)\S+$", rf"\g<1>{version}", text)


def set_floor(text: str, version: str) -> str:
    return re.sub(
        r"(<requireMavenVersion>\s*<version>\[)[\d.]+(,\)</version>)",
        rf"\g<1>{version}\g<2>",
        text,
    )


def set_wrapper(text: str, version: str) -> str:
    return re.sub(
        r"(apache-maven/)[\d.]+(/apache-maven-)[\d.]+(-bin\.zip)",
        rf"\g<1>{version}\g<2>{version}\g<3>",
        text,
    )


def consistent_bump(version: str) -> dict:
    """What the documented bump edits: the wrapper URL, both images, the enforcer floor."""
    return {
        **REAL,
        PROPS: set_wrapper(REAL[PROPS], version),
        DEV_DOCKERFILE: set_maven_arg(REAL[DEV_DOCKERFILE], version),
        RUNNER_DOCKERFILE: set_maven_arg(REAL[RUNNER_DOCKERFILE], version),
        POM: set_floor(REAL[POM], version),
    }


@pytest.mark.parametrize("part", ["patch", "minor"])
def test_a_consistent_bump_needs_no_edit_here(part):
    version = bumped(live_version(), part)
    files = consistent_bump(version)
    assert files != REAL
    assert problems(files) == []


def m_wrapper_alone():
    v2 = bumped(live_version())
    dev = arg(REAL[DEV_DOCKERFILE], "MAVEN_VERSION")
    return (
        {**REAL, PROPS: set_wrapper(REAL[PROPS], v2)},
        f"{DEV_DOCKERFILE} installs Maven {dev}, the wrapper pins {v2}",
    )


def m_image_alone(name):
    return lambda: (
        {**REAL, name: set_maven_arg(REAL[name], STALE)},
        f"{name} installs Maven {STALE}, the wrapper pins {live_version()}",
    )


def m_runner_hash():
    old8 = arg(REAL[RUNNER_DOCKERFILE], "MAVEN_SHA512")[:8]
    new8 = "0" * 8 if old8 != "0" * 8 else "f" * 8
    return (
        sub(RUNNER_DOCKERFILE, rf"(ARG MAVEN_SHA512=){old8}", rf"\g<1>{new8}"),
        "pin different (or no) MAVEN_SHA512",
    )


def m_floor_alone():
    return (
        {**REAL, POM: set_floor(REAL[POM], STALE)},
        f"pom.xml requires Maven >= {STALE}, the wrapper pins {live_version()}",
    )


def m_message(text, expected):
    return lambda: (
        sub(POM, r"<message>.*?</message>", f"<message>{text}</message>", re.S),
        expected,
    )


def m_sub(name, pattern, repl, expected, flags=0):
    return lambda: (sub(name, pattern, repl, flags), expected)


SCALA_BUILD = ".github/actions/scala_build/action.yml"
BUILD_MAIN = ".github/workflows/build_main.yml"
VERIFY_WORKFLOW = ".github/workflows/verify-maven-pgp.yml"
NO_WRAPPER_STEP = (r"uses: \./\.github/actions/maven-wrapper", "run: true")

MUTATIONS = {
    "wrapper version moved alone": m_wrapper_alone,
    "dev image moved alone": m_image_alone(DEV_DOCKERFILE),
    "local-CI image moved alone": m_image_alone(RUNNER_DOCKERFILE),
    "local-CI image hash differs": m_runner_hash,
    "enforcer floor moved alone": m_floor_alone,
    "enforcer message loses ./mvnw": m_message(
        "Rebuild with gbx:docker:rebuild", "message does not name ./mvnw"
    ),
    "enforcer message loses the container fix": m_message(
        "Run ./mvnw", "message does not name gbx:docker:rebuild"
    ),
    "SHA-256 dropped": m_sub(
        PROPS,
        r"(?m)^distributionSha256Sum=\S+\n",
        "",
        "distributionSha256Sum is missing",
    ),
    "wrapper jar type": m_sub(
        PROPS, r"distributionType=\S+", "distributionType=source", "must be only-script"
    ),
    "download from another host": m_sub(
        PROPS,
        r"https://repo\.maven\.apache\.org/maven2",
        "https://example.org/maven2",
        "distributionUrl is not the Maven Central bin.zip",
    ),
    "bare mvn in the Scala build": m_sub(
        SCALA_BUILD, r"\./mvnw (?=-)", "mvn ", "scala_build/action.yml: bare mvn"
    ),
    "bare mvn in the PGP script": m_sub(
        PGP_VERIFY, r'exec "\$\{mvn_cmd\}"', "exec mvn", "maven-pgp-verify: bare mvn"
    ),
    "PGP script defaults to the runner's mvn": m_sub(
        PGP_VERIFY,
        r"\$\{GBX_MVN:-\$\{root\}/mvnw\}",
        "${GBX_MVN:-mvn}",
        "maven-pgp-verify: the default Maven is not the wrapper",
    ),
    "bare mvn behind sudo": m_sub(
        BUILD_MAIN, r"sudo -E \./mvnw", "sudo -E mvn", "build_main.yml: bare mvn"
    ),
    "Maven not installed before the PGP gate": m_sub(
        VERIFY_WORKFLOW,
        *NO_WRAPPER_STEP,
        "verify-maven-pgp.yml: scripts/security/maven-pgp-verify runs before the pinned Maven is installed",
    ),
    "Maven not installed before the Delta fetch": m_sub(
        PYRX_ACTION,
        *NO_WRAPPER_STEP,
        "pyrx_build/action.yml: ./mvnw runs before the pinned Maven is installed",
    ),
    "Delta fetch without GBX_MVN": m_sub(
        PYRX_ACTION,
        r"      env:\n        GBX_MVN: \./mvnw\n",
        "",
        "pyrx_build/action.yml: fetch-delta-test-jars.sh runs without GBX_MVN: ./mvnw",
    ),
    "installer without the unzip guard": m_sub(
        INSTALLER, r"command -v unzip", "command -v true", "no unzip guard"
    ),
    "installer without the mirror guard": m_sub(
        INSTALLER, r"jfrog-central", "central", "no check for the JFrog mirror"
    ),
}


@pytest.mark.parametrize("name", sorted(MUTATIONS))
def test_each_drift_is_reported(name):
    files, expected = MUTATIONS[name]()
    assert files != REAL, f"the mutation {name!r} changed nothing"
    found = problems(files)
    assert any(expected in p for p in found), (expected, found)


# ------------------------------------------- fetch-delta-test-jars.sh runs the Maven GBX_MVN names


def _fetch_delta_jars(tmp_path, **env):
    """Run a copy of the script on a tree with no Maven repository holding the jars: its own
    repo root, an empty home and an empty GBX_M2_REPO, so it has to fetch."""
    script = tmp_path / "scripts" / "ci" / "fetch-delta-test-jars.sh"
    script.parent.mkdir(parents=True)
    shutil.copy(REPO_ROOT / "scripts" / "ci" / "fetch-delta-test-jars.sh", script)
    (tmp_path / "home").mkdir()
    (tmp_path / "m2").mkdir()
    full = dict(os.environ, HOME=str(tmp_path / "home"), GBX_M2_REPO=str(tmp_path / "m2"))
    full.pop("GBX_MVN", None)
    full.update(env)
    return subprocess.run(
        ["bash", str(script)], env=full, capture_output=True, text=True, timeout=60
    )


def test_the_delta_fetch_runs_the_maven_gbx_mvn_names(tmp_path):
    argv_log = tmp_path / "argv.log"
    mvn = tmp_path / "stub-mvn"
    mvn.write_text(f'#!/bin/sh\nfor a in "$@"; do echo "$a"; done > "{argv_log}"\nexit 0\n')
    mvn.chmod(0o755)

    res = _fetch_delta_jars(tmp_path, GBX_MVN=str(mvn))

    argv = argv_log.read_text().split()
    assert "-C" in argv and f"-Dmaven.repo.local={tmp_path / 'm2'}" in argv
    assert "org.apache.maven.plugins:maven-dependency-plugin:3.7.0:get" in argv
    assert any(a.startswith("-Dartifact=io.delta:delta-spark_2.13:") for a in argv)
    # the stub put no jars in the repository: the script says so and fails
    assert res.returncode == 1 and "mvn succeeded but the jars are not in any repo" in res.stderr


def test_the_delta_fetch_names_a_missing_maven_and_fails(tmp_path):
    res = _fetch_delta_jars(tmp_path, GBX_MVN=str(tmp_path / "no-such-maven"))

    assert res.returncode == 1
    assert "no-such-maven is not available" in res.stderr
