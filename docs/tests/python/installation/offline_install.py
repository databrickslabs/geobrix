"""
Install without PyPI access: the snippets of docs/docs/installation.mdx (Install without PyPI access).

The download runs on a machine with PyPI access; the staging and the installs run against a Volume. Tested by
test_offline_install.py: the download command's options, the staging copy (to a local folder), and the
Unity Catalog install as a dry run.
"""

DOWNLOAD_SHELL = """# On a machine with PyPI access and Python 3.12
pip install geobrix-0.6.0+env6-py3-none-any.whl

# The wheels for both architectures, the env wheel beside them, and the Unity Catalog pins
python -m databricks.labs.gbx.stage deps --uc \\
    --wheel geobrix-0.6.0+env6-py3-none-any.whl --out gbx-wheels"""

STAGE_DEPS_PYTHON = """from databricks.labs import gbx

# Copy the folder to a Volume; each file's sha256 is checked before and after the copy
gbx.stage_deps("/Volumes/<catalog>/<schema>/<volume>/geobrix/0.6.0/env6", src="gbx-wheels")"""

NOTEBOOK_PIP = """%pip install --no-index \\
    --find-links /Volumes/<catalog>/<schema>/<volume>/geobrix/0.6.0/env6/x86_64 \\
    /Volumes/<catalog>/<schema>/<volume>/geobrix/0.6.0/env6/geobrix-0.6.0+env6-py3-none-any.whl"""

SERVERLESS_DEPENDENCIES = """--no-index
--find-links=/Volumes/<catalog>/<schema>/<volume>/geobrix/0.6.0/env6/x86_64
/Volumes/<catalog>/<schema>/<volume>/geobrix/0.6.0/env6/geobrix-0.6.0+env6-py3-none-any.whl"""

UC_INSTALL_FROM_VOLUME = """from databricks.labs import gbx

# The folder holds the env 6 wheel and the x86_64/ and aarch64/ folders gbx.stage_deps made
gbx.uc.install(
    "<catalog>",
    "<schema>",
    "/Volumes/<catalog>/<schema>/<volume>/geobrix/0.6.0/env6",
    deps="volume",
)"""
