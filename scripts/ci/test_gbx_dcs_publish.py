"""Tests for scripts/commands/gbx-dcs-publish.sh: the gates before any tag or push.

The command runs with a fake `docker` (see conftest.py): nothing reaches a daemon or a
registry, and the repository is always gbx-test/none.
"""

import pytest

TEST_REPO = (
    "gbx-test/none"  # conftest.py runs every command with GBX_DCS_REPO set to this
)
VER = "0.0.2"
RC = f"{TEST_REPO}:{VER}-env6-arm-rc"
DIGEST = "sha256:" + "a" * 64
OTHER = "sha256:" + "b" * 64


def _pushed_rc(cmd, digest=DIGEST):
    """A local -rc image that was pushed to the repository as the candidate."""
    cmd.state["images"][RC] = {"digests": [f"{TEST_REPO}@{digest}"]}
    cmd.state["push_digest"] = digest


def _tags_and_pushes(cmd):
    return [c for c in cmd.calls if c[0] in ("tag", "push")]


def test_release_without_confirm_is_refused(dcs_publish):
    _pushed_rc(dcs_publish)
    res = dcs_publish.run("--release", "--version", VER)
    assert res.returncode == 1
    assert "REFUSED" in res.output and f"--confirm {VER}" in res.output
    assert dcs_publish.calls == []  # refused before any docker call


def test_release_confirm_must_equal_the_version(dcs_publish):
    _pushed_rc(dcs_publish)
    res = dcs_publish.run("--release", "--confirm", "0.0.1", "--version", VER)
    assert res.returncode == 1
    assert "REFUSED" in res.output and "does not match" in res.output
    assert dcs_publish.calls == []


def test_confirm_applies_to_release_only(dcs_publish):
    res = dcs_publish.run("--candidate", "--confirm", VER, "--version", VER)
    assert res.returncode == 1 and "--release only" in res.output
    assert dcs_publish.calls == []


@pytest.mark.parametrize(
    "digests",
    [[], [f"{TEST_REPO}-dev@{DIGEST}"]],
    ids=["never-pushed", "pushed-to-another-repo"],
)
def test_release_refuses_an_rc_that_was_not_pushed(dcs_publish, digests):
    """A rebuilt -rc has no <repo>@ digest; one pushed elsewhere (a repository whose
    name starts with this one's) does not count either."""
    dcs_publish.state["images"][RC] = {"digests": digests}
    res = dcs_publish.run("--release", "--confirm", VER, "--version", VER)
    assert res.returncode == 1
    assert "not a pushed candidate" in res.output
    assert _tags_and_pushes(dcs_publish) == []


def test_missing_rc_image_is_a_precondition(dcs_publish):
    res = dcs_publish.run("--candidate", "--version", VER)
    assert res.returncode == 1
    assert f"PRECONDITION MISSING: local image {RC}" in res.output
    assert _tags_and_pushes(dcs_publish) == []


@pytest.mark.parametrize("mode", [["--candidate"], ["--release", "--confirm", VER]])
def test_expect_digest_refuses_another_digest(dcs_publish, mode):
    _pushed_rc(dcs_publish, OTHER)
    res = dcs_publish.run(*mode, "--version", VER, "--expect-digest", DIGEST)
    assert res.returncode == 1
    assert f"does not have the expected digest {DIGEST}" in res.output
    assert OTHER.split(":")[1] in res.output  # names the digest it has
    assert _tags_and_pushes(dcs_publish) == []


def test_expect_digest_must_be_a_sha256(dcs_publish):
    res = dcs_publish.run("--candidate", "--version", VER, "--expect-digest", "abc")
    assert res.returncode == 1 and "sha256:<64 hex" in res.output
    assert dcs_publish.calls == []


def test_candidate_pushes_only_the_rc_and_prints_its_digest(dcs_publish):
    dcs_publish.state["images"][RC] = {"digests": []}
    dcs_publish.state["push_digest"] = DIGEST
    res = dcs_publish.run("--candidate", "--version", VER)
    assert res.returncode == 0, res.output
    assert _tags_and_pushes(dcs_publish) == [["push", RC]]
    assert f"DIGEST={TEST_REPO}@{DIGEST}" in res.output


def test_release_of_the_verified_digest_tags_then_pushes_both(dcs_publish):
    _pushed_rc(dcs_publish)
    res = dcs_publish.run(
        "--release", "--confirm", VER, "--version", VER, "--expect-digest", DIGEST
    )
    assert res.returncode == 0, res.output
    assert _tags_and_pushes(dcs_publish) == [
        ["tag", RC, f"{TEST_REPO}:{VER}-env6-arm"],
        ["tag", RC, f"{TEST_REPO}:env6-arm"],
        ["push", f"{TEST_REPO}:{VER}-env6-arm"],
        ["push", f"{TEST_REPO}:env6-arm"],
    ]
    assert f"RELEASING={TEST_REPO}@{DIGEST}" in res.output
    assert f"DIGEST={TEST_REPO}@{DIGEST}" in res.output


def test_version_needs_exactly_one_jar(dcs_publish):
    dcs_publish.jar("0.0.3")
    dcs_publish.jar("0.0.4")
    res = dcs_publish.run("--candidate")
    assert res.returncode == 1
    assert "exactly one target/geobrix-*-jar-with-dependencies.jar" in res.output
    assert "found 2" in res.output and "--version" in res.output
    assert dcs_publish.calls == []


def test_version_is_read_from_the_one_jar(dcs_publish):
    dcs_publish.jar("0.0.3")
    rc = f"{TEST_REPO}:0.0.3-env6-arm-rc"
    dcs_publish.state["images"][rc] = {"digests": []}
    res = dcs_publish.run("--candidate", "--arch", "arm64")
    assert res.returncode == 0, res.output
    assert _tags_and_pushes(dcs_publish) == [["push", rc]]
