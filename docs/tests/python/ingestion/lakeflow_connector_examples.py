"""Lakeflow connector examples -- single source of truth for
docs/docs/ingestion/lakeflow-connector.mdx.

The pipeline source file template is shown straight from the connector
(``connectors/lakeflow/sources/geobrix/pipeline/ingest.py``). The commands and
dependency lines here are checked against the connector's own build and its
spec: the framework commit is the one the connector tests run against, and the
wheel names are the packages these commands build.
"""

# The commit of the Lakeflow community-connectors framework the GeoBrix connector
# is tested against (scripts/commands/gbx-test-connectors.sh).
FRAMEWORK_COMMIT = "70d16606fb2a7c32e9965af0c9c9452a0172008e"

BUILD_WHEELS = r"""# The framework and the connector are not on PyPI: build both wheels once
git clone https://github.com/databrickslabs/geobrix.git
pip wheel --no-deps -w wheels ./geobrix/connectors/lakeflow
pip wheel --no-deps -w wheels \
  "lakeflow-community-connectors @ git+https://github.com/databrickslabs/lakeflow-community-connectors@70d16606fb2a7c32e9965af0c9c9452a0172008e"
# then upload wheels/*.whl and the GeoBrix wheel to one Volume folder"""

PIPELINE_DEPENDENCIES = """geobrix@file:///Volumes/<catalog>/<schema>/<volume>/<dir>/geobrix-<version>+env5-py3-none-any.whl
/Volumes/<catalog>/<schema>/<volume>/<dir>/lakeflow_community_connectors-<version>-py3-none-any.whl
/Volumes/<catalog>/<schema>/<volume>/<dir>/geobrix_lakeflow_connectors-<version>-py3-none-any.whl"""
