"""Tests for scripts/commands/gbx-dcs-build.sh: the refusals before a build, and (with
--target base) the image-history check for a package index and tag-after-verify.

The command runs with a fake `docker` (see conftest.py): nothing reaches a daemon or a
registry, and the repository is always gbx-test/none. The package index used here is a
made-up value; the tests assert the command never prints it.
"""

import pytest

TEST_REPO = (
    "gbx-test/none"  # conftest.py runs every command with GBX_DCS_REPO set to this
)
BASE_TAG = f"{TEST_REPO}:base-env6-arm"
INDEX = "https://ci-user@idx.internal:8443/simple"
INDEX_HOST = "idx.internal"
SECRET_READ = (
    'RUN /bin/sh -c export PIP_INDEX_URL="$(cat /run/secrets/pip_index_url)" '
    "&& pip install /tmp/geobrix.whl"
)
# The public base image's own history names its build-time index host.
BASE_HISTORY = [
    f"ENV UV_DEFAULT_INDEX=https://{INDEX_HOST}/base",
    "RUN /bin/sh -c apt-get install -y python3",
    "ADD file:base in /",
]


def _base_build(cmd, own, env=None):
    cmd.state.update(history_own=own, history_base=BASE_HISTORY)
    return cmd.run("--target", "base", env=env)


def _tagged(cmd):
    return [c for c in cmd.calls if c[0] == "tag"]


# --- refusals before any build (default --target release) ---------------------------


def test_two_jars_are_refused(dcs_build):
    dcs_build.jar("0.0.2")
    dcs_build.jar("0.0.3")
    dcs_build.wheel("0.0.2")
    res = dcs_build.run()
    assert res.returncode == 1
    assert (
        "exactly one target/geobrix-*-jar-with-dependencies.jar, found 2" in res.output
    )
    assert dcs_build.commands() == ["info"]


def test_jar_and_wheel_versions_must_match(dcs_build):
    dcs_build.jar("0.0.2")
    dcs_build.wheel("0.0.3")
    res = dcs_build.run()
    assert res.returncode == 1
    assert "JAR version 0.0.2 != wheel version 0.0.3" in res.output
    assert dcs_build.commands() == ["info"]


def test_a_jar_bundling_the_scala_library_is_refused(dcs_build):
    dcs_build.jar("0.0.2", ["com/databricks/labs/gbx/A.class", "scala/Predef.class"])
    dcs_build.wheel("0.0.2")
    res = dcs_build.run()
    assert res.returncode == 1
    assert "scala/Predef.class" in res.output and "rebuild the JAR" in res.output
    assert dcs_build.commands() == ["info"]


# --- --target base: history check, then the tag ---------------------------------------


def test_clean_history_is_tagged_after_every_check(dcs_build):
    """The base image's own index host is not this build's; the Dockerfile's literal
    secret read holds no value. The final tag comes after both history reads."""
    res = _base_build(
        dcs_build, [SECRET_READ, "COPY gdal /usr"], env={"PIP_INDEX_URL": INDEX}
    )
    assert res.returncode == 0, res.output
    assert f"IMAGE={BASE_TAG}" in res.output
    cmds = dcs_build.commands()
    assert cmds.index("tag") > max(i for i, c in enumerate(cmds) if c == "history")
    ((_, tmp, final),) = _tagged(dcs_build)
    assert tmp.startswith("gbx-dcs-build-tmp:") and final == BASE_TAG
    build = next(c for c in dcs_build.calls if c[0] == "build")
    assert any(a.startswith("id=pip_index_url,src=") for a in build)
    assert INDEX not in res.output and INDEX_HOST not in res.output
    assert not any(INDEX in a for c in dcs_build.calls for a in c)


@pytest.mark.parametrize(
    "leak, counts",
    [
        (f"ENV PIP_INDEX_URL={INDEX}", "PIP_INDEX_URL values: 1"),
        (f"RUN /bin/sh -c pip install -i https://{INDEX_HOST}/x pkg", "index host: 1"),
    ],
    ids=["index-value", "index-host"],
)
def test_a_package_index_in_this_builds_layers_is_not_tagged(dcs_build, leak, counts):
    res = _base_build(dcs_build, [leak, "COPY gdal /usr"], env={"PIP_INDEX_URL": INDEX})
    assert res.returncode == 1
    assert "VERIFY FAILED: package index found in the history" in res.output
    assert counts in res.output and f"not tagged as {BASE_TAG}" in res.output
    assert _tagged(dcs_build) == []
    assert INDEX not in res.output and INDEX_HOST not in res.output


def test_layers_that_cannot_be_told_from_the_base_are_not_tagged(dcs_build):
    """No history line of its own: the check would inspect nothing, so it fails."""
    res = _base_build(dcs_build, [])
    assert res.returncode == 1
    assert "could not isolate this build's layers" in res.output
    assert _tagged(dcs_build) == []


def test_a_failed_image_check_is_not_tagged(dcs_build):
    dcs_build.state["run_rc"] = 1
    res = _base_build(dcs_build, ["COPY gdal /usr"])
    assert res.returncode == 1
    assert "VERIFY FAILED: GDAL 3.11.4" in res.output
    assert _tagged(dcs_build) == [] and "history" not in dcs_build.commands()
    assert any(
        c[0] == "rmi" and c[1].startswith("gbx-dcs-build-tmp:") for c in dcs_build.calls
    )


def test_a_failed_build_is_not_tagged(dcs_build):
    dcs_build.state["build_rc"] = 3
    res = _base_build(dcs_build, ["COPY gdal /usr"])
    assert res.returncode == 3
    assert "BUILD FAILED: docker build exited 3" in res.output
    assert _tagged(dcs_build) == []
