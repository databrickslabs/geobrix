"""NASA Earthdata (LP DAAC / CMR) client — the earthaccess analog of the STAC client.

`EarthdataClient` owns Earthdata-Login auth, CMR search, and a validating download
loop with the shared result contract + repair. Per-dataset downloaders in
`databricks.labs.gbx.sample` (e.g. `EmitDownloader`) are thin wrappers over it,
mirroring how the STAC downloaders sit on `StacClient`.
"""

from databricks.labs.gbx.earthdata.client import EarthdataClient

__all__ = ["EarthdataClient"]
