"""One Maven for CI and dev: the Maven wrapper pins it, everything else must agree.

.mvn/wrapper/maven-wrapper.properties pins Maven (version, URL, SHA-256). CI runs every Maven call
through ./mvnw, the dev image and the local-CI runner image install the same version themselves
(the wrapper cannot download inside the dev container), and pom.xml's enforcer refuses a Maven below
that version. Nothing but these checks keeps the four in step, so they are functions of the file
texts: the real repo must have no problems, and each way of drifting is reproduced on a copy of the
text and must be reported.
"""

import re
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
SCRIPTS = ("scripts/security/maven-pgp-verify", "scripts/security/maven-pgp-bootstrap")
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

    # --- CI: no bare mvn, and the pinned Maven is installed before anything needs it
    for name in [*SCRIPTS, *ci_files_in(files)]:
        if name in EXEMPT_FROM_BARE_MVN:
            continue
        for ln in code_lines(files[name]):
            if BARE_MVN.search(ln):
                out.append(f"{name}: bare mvn (use ./mvnw): {ln.strip()[:80]}")
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
    assert staged.startswith("100755 "), f"mvnw is not executable in the index: {staged!r}"
    assert not list((REPO_ROOT / ".mvn" / "wrapper").glob("*.jar"))


def with_change(name: str, old: str, new: str) -> dict:
    assert old in REAL[name], f"fixture drifted: {old!r} not in {name}"
    return {**REAL, name: REAL[name].replace(old, new, 1)}


def dropped(name: str, pattern: str) -> dict:
    new, n = re.subn(pattern, "", REAL[name], count=1, flags=re.S)
    assert n == 1, f"fixture drifted: {pattern!r} not in {name}"
    return {**REAL, name: new}


MUTATIONS = {
    "wrapper version moved alone": (
        with_change(
            PROPS,
            "apache-maven/3.9.16/apache-maven-3.9.16",
            "apache-maven/3.9.17/apache-maven-3.9.17",
        ),
        f"{DEV_DOCKERFILE} installs Maven 3.9.16, the wrapper pins 3.9.17",
    ),
    "dev image moved alone": (
        with_change(
            DEV_DOCKERFILE, "ARG MAVEN_VERSION=3.9.16", "ARG MAVEN_VERSION=3.9.9"
        ),
        f"{DEV_DOCKERFILE} installs Maven 3.9.9, the wrapper pins 3.9.16",
    ),
    "local-CI image moved alone": (
        with_change(
            RUNNER_DOCKERFILE, "ARG MAVEN_VERSION=3.9.16", "ARG MAVEN_VERSION=3.9.9"
        ),
        f"{RUNNER_DOCKERFILE} installs Maven 3.9.9",
    ),
    "local-CI image hash differs": (
        with_change(
            RUNNER_DOCKERFILE, "ARG MAVEN_SHA512=831a8591", "ARG MAVEN_SHA512=a555254d"
        ),
        "pin different (or no) MAVEN_SHA512",
    ),
    "enforcer floor moved alone": (
        with_change(POM, "<version>[3.9.16,)</version>", "<version>[3.8.0,)</version>"),
        "pom.xml requires Maven >= 3.8.0, the wrapper pins 3.9.16",
    ),
    "enforcer message loses the fix": (
        with_change(POM, "Run ./mvnw instead of mvn", "Upgrade Maven"),
        "message does not name ./mvnw",
    ),
    "SHA-256 dropped": (
        dropped(PROPS, r"distributionSha256Sum=\S+\n"),
        "distributionSha256Sum is missing",
    ),
    "wrapper jar type": (
        with_change(PROPS, "distributionType=only-script", "distributionType=source"),
        "distributionType must be only-script",
    ),
    "download from another host": (
        with_change(
            PROPS, "https://repo.maven.apache.org/maven2", "https://example.org/maven2"
        ),
        "distributionUrl is not the Maven Central bin.zip",
    ),
    "bare mvn in the Scala build": (
        with_change(
            ".github/actions/scala_build/action.yml",
            "./mvnw -C -q clean install -DskipTests",
            "mvn -C -q clean install -DskipTests",
        ),
        "scala_build/action.yml: bare mvn",
    ),
    "bare mvn in the PGP script": (
        with_change(
            "scripts/security/maven-pgp-verify", 'exec "${mvn_cmd}" -B', "exec mvn -B"
        ),
        "maven-pgp-verify: bare mvn",
    ),
    "bare mvn behind sudo": (
        with_change(
            ".github/workflows/build_main.yml", "sudo -E ./mvnw", "sudo -E mvn"
        ),
        "build_main.yml: bare mvn",
    ),
    "Maven not installed before the PGP gate": (
        dropped(
            ".github/workflows/verify-maven-pgp.yml",
            r"      # The gate runs on the same pinned.*?jfrog-access-token: \$\{\{ steps\.jfrog\.outputs\.jfrog-access-token \}\}\n\n",
        ),
        "verify-maven-pgp.yml: scripts/security/maven-pgp-verify runs before the pinned Maven is installed",
    ),
    "Maven not installed before the Delta fetch": (
        dropped(
            ".github/actions/pyrx_build/action.yml",
            r"    # The Delta jar fetch below runs Maven.*?jfrog-access-token: \$\{\{ steps\.jfrog\.outputs\.jfrog-access-token \}\}\n",
        ),
        "pyrx_build/action.yml: ./mvnw runs before the pinned Maven is installed",
    ),
}


@pytest.mark.parametrize("name", sorted(MUTATIONS))
def test_each_drift_is_reported(name):
    files, expected = MUTATIONS[name]
    assert files != REAL, f"the mutation {name!r} changed nothing"
    found = problems(files)
    assert any(expected in p for p in found), (expected, found)
