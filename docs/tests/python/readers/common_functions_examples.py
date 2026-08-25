"""Doc-test source for the GBX Common Functions page (single source of truth).

Code shown in docs/docs/common-functions.mdx is imported from here.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from path_config import SAMPLE_DATA_BASE  # noqa: E402

SAMPLE_RASTER_DIR = f"{SAMPLE_DATA_BASE}/nyc/sentinel2"


def gbx_file_read_then_decode(spark, path=None):
    """gbx_file_read -> rst_fromfile: the canonical compose pattern.

    gbx_file_read returns [path, size, file] — a FILE reference (MANAGED or
    EXTERNAL) when the runtime supports FILE, else null.  Compose with
    rst_fromfile to decode the raster at each path into a tile struct.
    """
    from databricks.labs.gbx.ds.file_gbx import gbx_file_read
    from databricks.labs.gbx.pyrx.functions import rst_fromfile

    files = gbx_file_read(spark, path or SAMPLE_RASTER_DIR, extensions=(".tif",))
    # files has columns: path (STRING), size (BIGINT), file (FILE ref or null)
    tiles = files.select("path", rst_fromfile(files["path"]).alias("tile"))
    n = tiles.count()
    assert n > 0, "expected at least one decoded tile"
    return n


def list_local_files_example(path):
    """Session-free enumeration used by every DataSource reader."""
    from databricks.labs.gbx.ds.file_gbx import list_local_files

    paths = list_local_files(path, extensions=(".tif",))
    assert paths == sorted(paths)
    return paths
