"""Lakeflow SDP examples -- single source of truth for docs/docs/ingestion/lakeflow-sdp.mdx.

The dependency lines and the bundle are checked against the env wheels' extras and
against the example bundles that run in production; the sink helper runs against
a real table.
"""

# One line per pipeline dependency, for a wheel staged on a Volume (the channel's
# runtime picks the wheel; see the page's channel table), with the feature extras it needs.
DEPENDENCY_VOLUME = """geobrix[vizx]@file:///Volumes/<catalog>/<schema>/<volume>/<dir>/geobrix-<version>+env5-py3-none-any.whl"""

# The same requirement straight from the GitHub release.
DEPENDENCY_GITHUB = """geobrix[vizx]@https://github.com/databrickslabs/geobrix/releases/download/v<version>/geobrix-<version>+env5-py3-none-any.whl"""

# Pipeline channel -> the Databricks Runtime major version it ran when this page was
# written -> the GeoBrix env wheel for that runtime. The page's table shows exactly this.
CHANNEL_WHEELS = {
    "CURRENT": ("18", "+env5"),
    "PREVIEW": ("19", "+env6"),
}

BUNDLE_PIPELINE = """resources:
  pipelines:
    landing_ingest:
      name: landing_ingest
      serverless: true
      channel: CURRENT          # match the GeoBrix wheel below to this channel's runtime
      catalog: main
      schema: geo
      root_path: ./transformations
      libraries:
        - glob:
            include: transformations/**
      environment:
        dependencies:
          # the Volume wheel path followed by its extras, all on one line
          - /Volumes/main/geo/wheels/geobrix-<version>+env5-py3-none-any.whl[vizx]"""

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
