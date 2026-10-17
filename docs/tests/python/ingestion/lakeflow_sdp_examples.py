"""Lakeflow SDP examples -- single source of truth for docs/docs/ingestion/lakeflow-sdp.mdx.

The dependency lines and the bundle are checked against the wheel's extras and
against the example bundles that run in production; the sink helper runs against
a real table.
"""

# One line per pipeline dependency, for a wheel staged on a Volume (the channel's
# runtime picks the extra; see the page's channel table).
DEPENDENCY_VOLUME = """geobrix[light_dbr18]@file:///Volumes/<catalog>/<schema>/<volume>/<dir>/geobrix-<version>-py3-none-any.whl"""

# The same requirement straight from the GitHub release.
DEPENDENCY_GITHUB = """geobrix[light_dbr18]@https://github.com/databrickslabs/geobrix/releases/download/v<version>/geobrix-<version>-py3-none-any.whl"""

# Pipeline channel -> the Databricks Runtime major version it ran when this page was
# written -> the GeoBrix extra for that runtime. The page's table shows exactly this.
CHANNEL_EXTRAS = {
    "CURRENT": ("18", "light_dbr18"),
    "PREVIEW": ("19", "light_dbr19"),
}

BUNDLE_PIPELINE = """resources:
  pipelines:
    landing_ingest:
      name: landing_ingest
      serverless: true
      channel: CURRENT          # match the GeoBrix extra below to this channel's runtime
      catalog: main
      schema: geo
      root_path: ./transformations
      libraries:
        - glob:
            include: transformations/**
      environment:
        dependencies:
          # the Volume wheel path followed by its extras, all on one line
          - /Volumes/main/geo/wheels/geobrix-<version>-py3-none-any.whl[light_dbr18]"""

# Starts with a comment, not ``def``: the docs component shows a snippet that starts
# with ``def`` as its body only.
TABLE_EXISTS_IN_SINK = """# spark.catalog.tableExists is not allowed inside a pipeline sink:
# look the table up and treat "not found" as False instead.
def table_exists(spark, name):
    try:
        spark.table(name).columns
        return True
    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" not in str(e):
            raise
        return False"""
