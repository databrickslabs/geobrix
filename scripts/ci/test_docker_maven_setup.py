"""scripts/docker/extras/docker_maven_setup.sh: the container's Maven settings, in the main checkout and in a
new linked worktree (whose gitignored scripts/docker/m2/ has no settings.xml).

No Docker, no Maven: the script runs under `sh` (as gbx:docker:start runs it) against a scratch checkout
and a scratch Maven conf dir, with a fake `mvn` first on PATH.
"""

import os
import shutil
import subprocess

from conftest import REPO_ROOT

SETUP = REPO_ROOT / "scripts" / "docker" / "extras" / "docker_maven_setup.sh"
DEFAULT = REPO_ROOT / "scripts" / "docker" / "extras" / "maven-settings.xml"
IMAGE_SETTINGS = "<settings><!-- the image's own --></settings>\n"


def checkout(tmp_path, with_default=True, m2_settings=None):
    root = tmp_path / "root"
    (root / "scripts" / "docker" / "extras").mkdir(parents=True)
    if with_default:
        shutil.copy(DEFAULT, root / "scripts" / "docker" / "extras" / DEFAULT.name)
    if m2_settings is not None:
        (root / "scripts" / "docker" / "m2").mkdir(parents=True)
        (root / "scripts" / "docker" / "m2" / "settings.xml").write_text(m2_settings)
    conf = tmp_path / "conf"
    conf.mkdir()
    (conf / "settings.xml").write_text(IMAGE_SETTINGS)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "calls.log"
    mvn = bin_dir / "mvn"
    mvn.write_text(
        f'#!/bin/sh\necho "mvn $*" >> "{calls}"\necho "The following profiles are active:"\necho " - skipScoverage"\n'
    )
    mvn.chmod(0o755)
    # the GDAL Java classes installer (scripts/native/java_jar.py): logged, exit code from FAKE_INSTALL_RC
    py = bin_dir / "python3"
    py.write_text(
        f'#!/bin/sh\necho "python3 $*" >> "{calls}"\nexit "${{FAKE_INSTALL_RC:-0}}"\n'
    )
    py.chmod(0o755)
    return root, conf, bin_dir


def run(root, conf, bin_dir, **extra):
    env = dict(
        os.environ,
        GBX_CONTAINER_ROOT=str(root),
        GBX_MAVEN_CONF_DIR=str(conf),
        PATH=f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        **extra,
    )
    return subprocess.run(["sh", str(SETUP)], env=env, capture_output=True, text=True)


def test_a_worktree_without_settings_gets_the_tracked_default(tmp_path):
    root, conf, bin_dir = checkout(tmp_path)
    res = run(root, conf, bin_dir)
    assert res.returncode == 0, res.stdout + res.stderr
    written = (root / "scripts" / "docker" / "m2" / "settings.xml").read_text()
    assert written == DEFAULT.read_text()
    assert (conf / "settings.xml").read_text() == written
    assert (conf / "settings.xml.BAK").read_text() == IMAGE_SETTINGS
    assert "skipScoverage" in res.stdout


def test_the_tracked_default_names_the_mirror_and_the_project_repository():
    text = DEFAULT.read_text()
    assert "<mirrorOf>central</mirrorOf>" in text and "db-maven-proxy" in text
    assert "<localRepository>/root/geobrix/scripts/docker/m2</localRepository>" in text
    assert "<activeProfile>skipScoverage</activeProfile>" in text


def test_a_settings_file_already_in_the_checkout_is_kept(tmp_path):
    own = "<settings><!-- the checkout's own --></settings>\n"
    root, conf, bin_dir = checkout(tmp_path, m2_settings=own)
    res = run(root, conf, bin_dir)
    assert res.returncode == 0, res.stdout + res.stderr
    assert (root / "scripts" / "docker" / "m2" / "settings.xml").read_text() == own
    assert (conf / "settings.xml").read_text() == own


def test_without_any_settings_the_image_settings_stay_in_place(tmp_path):
    # The old order moved the image's settings aside and then failed to copy a replacement, which left
    # Maven with no mirror at all.
    root, conf, bin_dir = checkout(tmp_path, with_default=False)
    res = run(root, conf, bin_dir)
    assert res.returncode == 1
    assert (conf / "settings.xml").read_text() == IMAGE_SETTINGS
    assert not (conf / "settings.xml.BAK").exists()


def test_the_gdal_java_classes_are_installed_before_maven_resolves_anything(tmp_path):
    # pom.xml's org.gdal:gdal is the native build's jar, in no repository: java_jar.py puts it into the project's
    # local repository (the settings' localRepository) before the first mvn call
    root, conf, bin_dir = checkout(tmp_path)
    res = run(root, conf, bin_dir)
    assert res.returncode == 0, res.stdout + res.stderr
    calls = (tmp_path / "calls.log").read_text().splitlines()
    install = f"python3 {root}/scripts/native/java_jar.py install --repo {root}/scripts/docker/m2"
    assert calls[0] == install, calls
    assert calls[1].startswith("mvn ") and len(calls) == 2


def test_a_refused_install_stops_the_setup_before_maven(tmp_path):
    root, conf, bin_dir = checkout(tmp_path)
    res = run(root, conf, bin_dir, FAKE_INSTALL_RC="1")
    assert res.returncode == 1
    assert [
        c.split()[0] for c in (tmp_path / "calls.log").read_text().splitlines()
    ] == ["python3"]
