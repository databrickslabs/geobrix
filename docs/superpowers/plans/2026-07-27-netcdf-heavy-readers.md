# NetCDF Heavy Readers + Light Auto-Enumeration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add heavy-tier NetCDF readers (`netcdf_gdal` raster, `netcdf_ogr` vector) following the existing named-reader pattern, and unify the variable-selection contract so the shipped light `netcdf_gbx` reader auto-enumerates all readable variables (variable option becomes an optional filter, not a mandatory selector).

**Architecture:** Three readers share one contract — a bare `load` returns *every* readable variable (raster: every georeferenced grid variable, one tile row each; vector: every DSG/curvilinear feature), and `variable`/`variables` narrows that set. `netcdf_gdal extends GDAL_DataSource` but overrides plan-time partitioning to enumerate GDAL subdatasets (one partition per `(file, variable)`) instead of assuming one raster per file. `netcdf_ogr extends OGR_DataSource` unchanged except `driverName=netCDF`. The light reader moves variable resolution from `__init__` (where it raised on absence) into the open-dataset path (`read()`/`schema()`), enumerating via the existing `classify()`.

**Tech Stack:** Scala 2.13.16 / Spark 4.0.0 / Java 17 (heavy, GDAL Java bindings); Python 3.12+ / PySpark DataSource V2 / xarray+netcdf4 (light). Tests: ScalaTest (`PlanTest with SilentSparkSession`) in Docker; pytest (local Spark) for light.

## Global Constraints

- **No aliases** — one canonical name per reader (`netcdf_gdal`, `netcdf_ogr`, `netcdf_gbx`). Beta breaks API to stabilize.
- **Named-reader pattern:** a heavy named reader `extends <Engine>_DataSource with DataSourceExtras`, overrides `shortName()`, `dsExtraMap(checkMap)`, `inferSchema`, `getTable`, and registers a fully-qualified class line in `src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`. Pattern is `<format>_<engine>`.
- **GDAL/OGR registration only via the synchronized `GDALManager` guards** — `GDALManager.init(config)` for GDAL, `GDALManager.initOgr()` for OGR. Never raw `gdal.AllRegister()`/`ogr.RegisterAll()` per task.
- **Heavy raster schema is fixed:** `struct<source: string, tile: struct<cellid: bigint, raster: binary, metadata: map<string,string>>>` via `RST_ExpressionUtil.tileDataType(BinaryType)`. Do not change it.
- **Credential-aware listing/staging:** on `/Volumes`, a raw driver-thread Hadoop FS listing lacks the UC credential. Enumerate via `HadoopUtils.listDataFilesSpark(spark, path)` and stage per-executor via `NodeFileManager.readRemote(path)` (the OGR/GDAL batch pattern), never a raw driver FS call.
- **Serverless-safe light tier:** the Python reader may only `spark.dataSource.register` + build Column output — no `spark.conf.set`, `_jvm`, `.rdd`.
- **Swath→points is light-only** (non-goal for heavy): `netcdf_ogr` surfaces only native CF-DSG features; do not reimplement the per-pixel flatten on the JVM.
- **Heavy work runs in the `geobrix-dev` Docker container** via `gbx:*` commands. Dispatch long suites (Maven/Scala tests) to a Task subagent; never inline.
- **Binding parity** is enforced for *registered SQL functions*, not for readers — these readers add no `gbx_*` function, so `registered_functions.txt` is untouched. (Do not add reader shortNames to it.)

---

### Task 1: Light `netcdf_gbx` auto-enumeration (establishes the shared contract)

Move variable resolution from `__init__` (which raised when the option was absent) to the open-dataset path, so a bare `load` returns all readable variables. This is a **behavior change to the shipped v0.4.1 reader** — strictly more permissive; explicit-`variable` calls are unaffected.

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/_netcdf.py` (add `readable_variables` + `select_variables` helpers)
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/netcdf.py` (`NetcdfRasterReader`: enumerate + one tile per grid var; drop mandatory `_requested_variables`)
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/_netcdf_vector.py` (`NetcdfVectorReader`: enumerate at `schema()`/`read()`; optional filter)
- Test: `python/geobrix/test/ds/test_netcdf_helpers.py` (helper unit tests)
- Test: `python/geobrix/test/ds/test_netcdf_datasource.py` (auto-enumerate + back-compat)

**Interfaces:**
- Produces (consumed by heavy parity test in Task 4 and by the readers here):
  - `_netcdf.readable_variables(ds, mode: str) -> List[str]` — for `mode="raster"` returns data variables that `classify()` as `GRID`; for `mode="vector"` returns those classifying as `POINTS` or `CURVILINEAR`. Iterates `ds.data_vars` only (xarray excludes coordinate/grid-mapping variables, so `lat`/`lon`/`time_bnds`/`crs` are never returned).
  - `_netcdf.select_variables(ds, options: Dict[str, str], mode: str) -> List[str]` — `readable_variables(ds, mode)` when no `variable`/`variables` option; otherwise the option's names intersected with the readable set (order follows the option).
  - Light raster `source` column value = `f'NETCDF:"{path}":{var}"'` (the GDAL subdataset selector form) so multiple variables are disambiguable and the string matches the heavy `netcdf_gdal` source.

- [ ] **Step 1: Write failing helper tests**

Add to `python/geobrix/test/ds/test_netcdf_helpers.py`. Reuse the writer helpers already in `test_netcdf_datasource.py` — import them or inline equivalents; here we build datasets directly with `netCDF4` and open with `_netcdf.open_dataset`.

```python
import numpy as np
from netCDF4 import Dataset
from databricks.labs.gbx.ds import _netcdf


def _grid_two_vars(path):
    with Dataset(path, "w") as ds:
        ds.createDimension("lat", 3)
        ds.createDimension("lon", 4)
        lat = ds.createVariable("lat", "f8", ("lat",)); lat.standard_name = "latitude"
        lon = ds.createVariable("lon", "f8", ("lon",)); lon.standard_name = "longitude"
        lat[:] = [50.0, 49.5, 49.0]
        lon[:] = [10.0, 10.5, 11.0, 11.5]
        for name in ("ch4", "co"):
            v = ds.createVariable(name, "f4", ("lat", "lon"), fill_value=-9999.0)
            v[:] = np.arange(12, dtype="float32").reshape(3, 4)


def test_readable_variables_raster_enumerates_all_grids(tmp_path):
    f = tmp_path / "g.nc"; _grid_two_vars(str(f))
    with _netcdf.open_dataset(str(f), None) as ds:
        assert sorted(_netcdf.readable_variables(ds, "raster")) == ["ch4", "co"]
        # coordinate variables are never returned
        assert "lat" not in _netcdf.readable_variables(ds, "raster")


def test_select_variables_absent_option_returns_all(tmp_path):
    f = tmp_path / "g.nc"; _grid_two_vars(str(f))
    with _netcdf.open_dataset(str(f), None) as ds:
        assert sorted(_netcdf.select_variables(ds, {}, "raster")) == ["ch4", "co"]


def test_select_variables_filters_to_named(tmp_path):
    f = tmp_path / "g.nc"; _grid_two_vars(str(f))
    with _netcdf.open_dataset(str(f), None) as ds:
        assert _netcdf.select_variables(ds, {"variable": "co"}, "raster") == ["co"]
```

- [ ] **Step 2: Run to verify failure**

Run: `gbx:test:python --path python/geobrix/test/ds/test_netcdf_helpers.py -k "readable_variables or select_variables"`
Expected: FAIL — `AttributeError: module ... has no attribute 'readable_variables'`.

- [ ] **Step 3: Implement the helpers in `_netcdf.py`**

Add after `classify()`:

```python
def readable_variables(ds, mode: str) -> List[str]:
    """Data variables readable in `mode` ('raster' -> GRID; 'vector' -> POINTS/CURVILINEAR).

    Iterates ds.data_vars only: with open_dataset's decode_coords="all", lat/lon,
    grid-mapping, and bounds coordinate variables are xarray coords, not data_vars,
    so they are never surfaced as readable fields.
    """
    keep = {GRID} if mode == "raster" else {POINTS, CURVILINEAR}
    return [name for name in list(ds.data_vars) if classify(ds, name) in keep]


def select_variables(ds, options: Dict[str, str], mode: str) -> List[str]:
    """Auto-enumerate all readable variables, narrowed by an optional variable filter."""
    readable = readable_variables(ds, mode)
    raw = options.get("variables") or options.get("variable")
    if not raw:
        return readable
    requested = [v.strip() for v in str(raw).split(",") if v.strip()]
    readable_set = set(readable)
    return [v for v in requested if v in readable_set]
```

- [ ] **Step 4: Run helper tests to green**

Run: `gbx:test:python --path python/geobrix/test/ds/test_netcdf_helpers.py -k "readable_variables or select_variables"`
Expected: PASS.

- [ ] **Step 5: Write failing raster auto-enumerate test**

Add to `python/geobrix/test/ds/test_netcdf_datasource.py`. Add a two-variable grid writer alongside the existing `_write_regular_grid`:

```python
def _write_regular_grid_two(path):
    with Dataset(path, "w") as ds:
        ds.createDimension("lat", 3)
        ds.createDimension("lon", 4)
        lat = ds.createVariable("lat", "f8", ("lat",)); lat.standard_name = "latitude"
        lon = ds.createVariable("lon", "f8", ("lon",)); lon.standard_name = "longitude"
        lat[:] = [50.0, 49.5, 49.0]
        lon[:] = [10.0, 10.5, 11.0, 11.5]
        for name in ("ch4", "co"):
            v = ds.createVariable(name, "f4", ("lat", "lon"), fill_value=-9999.0)
            v[:] = np.arange(12, dtype="float32").reshape(3, 4)


def test_raster_bare_load_returns_all_grid_variables(spark, tmp_path):
    f = tmp_path / "grid2.nc"
    _write_regular_grid_two(str(f))
    spark.dataSource.register(NetcdfGbxDataSource)
    df = spark.read.format("netcdf_gbx").load(str(f))  # NO variable option
    rows = df.collect()
    assert len(rows) == 2  # one tile per grid variable
    sources = sorted(r["source"] for r in rows)
    assert sources[0].endswith(":ch4") and sources[1].endswith(":co")


def test_raster_variable_option_filters(spark, tmp_path):
    f = tmp_path / "grid2.nc"
    _write_regular_grid_two(str(f))
    spark.dataSource.register(NetcdfGbxDataSource)
    df = spark.read.format("netcdf_gbx").option("variable", "co").load(str(f))
    rows = df.collect()
    assert len(rows) == 1 and rows[0]["source"].endswith(":co")
```

- [ ] **Step 6: Run to verify failure**

Run: `gbx:test:python --path python/geobrix/test/ds/test_netcdf_datasource.py -k "bare_load_returns_all or variable_option_filters"`
Expected: FAIL — bare load raises `ValueError` (mandatory variable) today; filtered load returns 1 row but `source` lacks `:co` suffix.

- [ ] **Step 7: Rewrite `NetcdfRasterReader` for enumeration**

In `netcdf.py`, delete `_requested_variables` and rewrite the reader so selection happens inside `read()` (dataset is open there), emitting one tile per kept variable:

```python
class NetcdfRasterReader(RasterGbxReader):
    """Raster mode: transcode each CF grid variable to a GeoTIFF tile (one row per variable)."""

    def __init__(self, options: Dict[str, str]):
        super().__init__(options)  # path/sizeInMB/filterRegex/bbox/bboxCrs
        self.options = dict(options)
        self.group = options.get("group")

    def read(self, partition: "_FilePartition") -> Iterator[Tuple]:
        from rasterio.io import MemoryFile

        with _netcdf.open_dataset(partition.file_path, self.group) as ds:
            variables = _netcdf.select_variables(ds, self.options, "raster")
            for var in variables:
                transform, crs = _netcdf.grid_transform_crs(ds, var)
                arr = _netcdf.array_2d(ds, var)
                nodata = _netcdf.nodata_of(ds, var)
                source = f'NETCDF:"{partition.file_path}":{var}'
                h, w = arr.shape[-2], arr.shape[-1]
                profile = dict(driver="GTiff", width=w, height=h, count=1,
                               dtype=str(arr.dtype), crs=crs, transform=transform)
                if nodata is not None:
                    profile["nodata"] = nodata
                with MemoryFile() as mf:
                    with mf.open(**profile) as out:
                        out.write(arr.astype(profile["dtype"]), 1)
                    with mf.open() as rds:
                        cellid, raster_bytes, meta = _encode.encode_tile(
                            rds, window=(0, 0, w, h),
                            source_path=partition.file_path, all_parents="")
                yield (source, (cellid, raster_bytes, meta))
```

Drop the now-unused `_listing` import if nothing else uses it (leave `_encode`, `_netcdf`). Keep the curvilinear-in-raster-mode guard behavior via `select_variables` — a curvilinear variable simply isn't in the raster readable set, so a bare load skips it silently (matches "filter, not selector"). If the user *explicitly* names a curvilinear variable it is filtered out (returns no row for it); this is the intended optional-filter semantics.

- [ ] **Step 8: Update the pre-existing raster tests that assumed mandatory variable**

`test_raster_read_round_trip` still passes an explicit `variable` and expects 1 row — verify it stays green (source now ends `:ch4`; the test asserts `cellid == -1` and metadata, unaffected). `test_raster_mode_rejects_curvilinear` constructs `NetcdfRasterReader({...,"variable":"ch4"})` and expects a `ValueError` matching "vector" — this behavior is **removed** (curvilinear is now silently filtered). Replace that test with:

```python
def test_raster_mode_skips_curvilinear_variable(spark, tmp_path):
    f = tmp_path / "curv.nc"
    _write_curvilinear(str(f))
    spark.dataSource.register(NetcdfGbxDataSource)
    # bare load: curvilinear var is not a readable GRID -> zero rows, no error
    df = spark.read.format("netcdf_gbx").load(str(f))
    assert df.count() == 0
```

- [ ] **Step 9: Run raster suite to green**

Run: `gbx:test:python --path python/geobrix/test/ds/test_netcdf_datasource.py -k "raster"`
Expected: PASS (round-trip, bare-load-all, filter, skips-curvilinear).

- [ ] **Step 10: Write failing vector auto-enumerate test**

```python
def test_vector_bare_load_returns_all_dsg_variables(spark, tmp_path):
    f = tmp_path / "pts.nc"
    _write_points(str(f))
    spark.dataSource.register(NetcdfGbxDataSource)
    df = spark.read.format("netcdf_gbx").option("mode", "vector").load(str(f))  # no variables
    assert {"ch4", "qa_value"}.issubset(set(df.columns))
    assert df.count() == 5
```

- [ ] **Step 11: Run to verify failure**

Run: `gbx:test:python --path python/geobrix/test/ds/test_netcdf_datasource.py -k "vector_bare_load"`
Expected: FAIL — `ValueError` "vector mode requires a 'variables' option".

- [ ] **Step 12: Rewrite `NetcdfVectorReader` for enumeration**

In `_netcdf_vector.py`, stop raising in `__init__`; resolve variables from the open head file in `schema()` and per-file in `read()`:

```python
    def __init__(self, options: Dict[str, str]):
        self.path = options.get("path")
        if not self.path:
            raise ValueError("netcdf_gbx requires a 'path' (e.g. .load(path)).")
        self.options = dict(options)
        self.group: Optional[str] = options.get("group")
        self.filter_regex = options.get("filterRegex", ".*")

    def _variables(self, ds) -> List[str]:
        return _netcdf.select_variables(ds, self.options, "vector")

    def schema(self) -> StructType:
        members = self._members()
        if not members:
            raise ValueError(
                f"netcdf_gbx: no files matched filterRegex {self.filter_regex!r} "
                f"under {self.path!r} — nothing to infer a schema from.")
        fields: List[StructField] = []
        with _netcdf.open_dataset(members[0], self.group) as ds:
            for name in self._variables(ds):
                fields.append(StructField(name, _netcdf.np_to_spark(ds[name].values.dtype), True))
        fields.append(StructField("geom_0", BinaryType(), True))
        fields.append(StructField("geom_0_srid", StringType(), True))
        fields.append(StructField("geom_0_srid_proj", StringType(), True))
        return StructType(fields)
```

And in `read()`, replace `self.variables` with a per-file resolution and drop the UNSUPPORTED-raises-on-first-var assumption by iterating the resolved set:

```python
    def read(self, partition: "_NcFilePartition") -> Iterator[Tuple]:
        import shapely
        with _netcdf.open_dataset(partition.file_path, self.group) as ds:
            variables = self._variables(ds)
            if not variables:
                return
            lon, lat, attrs, srid = _netcdf.point_arrays(ds, variables)
        wkb = shapely.to_wkb(shapely.points(lon, lat))
        proj = f"EPSG:{srid}"
        for i in range(len(lon)):
            row = tuple(attrs[name][i].item() for name in variables)
            yield row + (bytes(wkb[i]), srid, proj)
```

Keep the existing explicit-`variables` tests (`test_vector_schema_columns`, `test_vector_read_dsg_points`, `test_vector_read_curvilinear_to_points`) green — they pass `variables` so `select_variables` returns exactly the named set.

- [ ] **Step 13: Run full light netcdf suite to green**

Run: `gbx:test:python --path python/geobrix/test/ds/`
Expected: PASS (all helper + datasource tests, old and new).

- [ ] **Step 14: Lint + commit**

Run: `gbx:lint:python --fix` then `gbx:lint:python --check` (verify with Docker `--check` per host/Docker black mismatch).

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/_netcdf.py \
        python/geobrix/src/databricks/labs/gbx/ds/netcdf.py \
        python/geobrix/src/databricks/labs/gbx/ds/_netcdf_vector.py \
        python/geobrix/test/ds/test_netcdf_helpers.py \
        python/geobrix/test/ds/test_netcdf_datasource.py
git commit -m "feat(netcdf): light netcdf_gbx auto-enumerates readable variables

variable/variables option becomes an optional filter, not a mandatory
selector. A bare load returns every readable variable (raster: one tile
row per grid variable, source=NETCDF:\"file\":var; vector: all DSG/
curvilinear features). Establishes the shared cross-tier contract for
the new heavy netcdf_gdal/netcdf_ogr readers.

Co-authored-by: Isaac"
```

---

### Task 2: `netcdf_gdal` heavy raster reader (subdataset enumeration)

The one genuinely new mechanism: a NetCDF file has no top-level bands, only subdatasets, so plan-time partitioning enumerates subdatasets (one partition per `(file, variable)`) rather than one-per-file.

**Files:**
- Create: `src/main/scala/com/databricks/labs/gbx/rasterx/ds/netcdf/NetCDF_DataSource.scala`
- Create: `src/main/scala/com/databricks/labs/gbx/rasterx/ds/netcdf/NetCDF_Table.scala`
- Create: `src/main/scala/com/databricks/labs/gbx/rasterx/ds/netcdf/NetCDF_Batch.scala`
- Create: `src/main/scala/com/databricks/labs/gbx/rasterx/ds/netcdf/NetCDF_Partition.scala`
- Create: `src/main/scala/com/databricks/labs/gbx/rasterx/ds/netcdf/NetCDF_Reader.scala`
- Modify: `src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister` (+1 line)
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/ds/NetCDF_DataSourceTest.scala`

**Interfaces:**
- Consumes: `RasterAccessors.subdatasetsMap(ds: Dataset): Map[String, String]` (SUBDATASETS metadata, `SUBDATASET_N_NAME -> "NETCDF:\"file\":var"`), `RasterDriver.read(path, options)` / `releaseDataset`, `BalancedSubdivision.splitRasterIter(ds, Map.empty, sizeInMB)`, `RasterSerializationUtil.tileToRow`, `NodeFileManager.readRemote/releaseRemote`, `HadoopUtils.listDataFilesSpark`, `GDALManager.init`.
- Produces: reader shortName `"netcdf_gdal"`; `dsExtraMap` = `Map("driver" -> "netCDF")`; `NetCDF_Partition(filePath: String, subdatasetName: String, sizeInMB: Int, expressionConfig: ExpressionConfig)`; `source` column value = the subdataset selector `NETCDF:"file":var`.

- [ ] **Step 1: Write failing unit test (shortName + dsExtraMap + inferSchema + is-a)**

Create `src/test/scala/com/databricks/labs/gbx/rasterx/ds/NetCDF_DataSourceTest.scala` mirroring `GTiff_DataSourceTest`:

```scala
package com.databricks.labs.gbx.rasterx.ds

import com.databricks.labs.gbx.rasterx.ds.netcdf.NetCDF_DataSource
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._

class NetCDF_DataSourceTest extends PlanTest with SilentSparkSession {

    test("NetCDF_DataSource short name is netcdf_gdal") {
        new NetCDF_DataSource().shortName() shouldBe "netcdf_gdal"
    }

    test("NetCDF_DataSource injects driver netCDF in dsExtraMap") {
        new NetCDF_DataSource().dsExtraMap() shouldBe Map("driver" -> "netCDF")
    }

    test("NetCDF_DataSource infers (source, tile) schema") {
        val ds = new NetCDF_DataSource()
        val schema = ds.inferSchema(new CaseInsensitiveStringMap(Map.empty[String, String].asJava))
        schema.fields.length shouldBe 2
        schema.fields(0).name shouldBe "source"
        schema.fields(0).dataType shouldBe StringType
        schema.fields(1).name shouldBe "tile"
    }

    test("NetCDF_DataSource is a TableProvider and DataSourceRegister") {
        val ds = new NetCDF_DataSource()
        ds shouldBe a[org.apache.spark.sql.connector.catalog.TableProvider]
        ds shouldBe a[org.apache.spark.sql.sources.DataSourceRegister]
    }
}
```

- [ ] **Step 2: Run to verify failure (compile error — class absent)**

Dispatch a Task subagent (Docker/Maven is long-running):
Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.ds.NetCDF_DataSourceTest' --log netcdf-gdal-unit.log`
Expected: FAIL — compilation error, `NetCDF_DataSource` not found.

- [ ] **Step 3: Create `NetCDF_Partition`**

```scala
package com.databricks.labs.gbx.rasterx.ds.netcdf

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.apache.spark.sql.connector.read.InputPartition

/** One partition of a netcdf_gdal scan: one (file, subdataset variable) pair.
  * Opened by NetCDF_Reader as the GDAL subdataset selector NETCDF:"file":var. */
case class NetCDF_Partition(
    filePath: String,
    subdatasetName: String,
    sizeInMB: Int,
    expressionConfig: ExpressionConfig
) extends InputPartition
      with Serializable
```

- [ ] **Step 4: Create `NetCDF_Reader`**

Opens the subdataset selector (staging the file locally first if remote), tiles via the shared `BalancedSubdivision` path, and yields `(selector, tile)` rows — `source` = the selector so the variable is recoverable.

```scala
package com.databricks.labs.gbx.rasterx.ds.netcdf

import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.BalancedSubdivision
import com.databricks.labs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import com.databricks.labs.gbx.util.NodeFileManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.unsafe.types.UTF8String

/** Reads one netcdf_gdal partition: opens its subdataset selector, splits into tiles, yields (source, tile). */
class NetCDF_Reader(partition: NetCDF_Partition) extends PartitionReader[InternalRow] {

    RST_ExpressionUtil.init(partition.expressionConfig)

    // Stage the .nc locally if remote (subdataset selectors are not plain paths, so
    // RasterDriver's own copyToLocal cannot recognize/stage them — do it explicitly).
    private val isLocal = partition.filePath.startsWith("/") &&
        !partition.filePath.startsWith("/Volumes/") && !partition.filePath.startsWith("/dbfs/")
    private val localPath = if (isLocal) partition.filePath else NodeFileManager.readRemote(partition.filePath)
    private val selector = s"""NETCDF:"$localPath":${partition.subdatasetName}"""

    private val ds = RasterDriver.read(selector, Map("isSubdataset" -> "true"))
    private val tilesIter = BalancedSubdivision.splitRasterIter(ds, Map.empty, partition.sizeInMB)
    RST_ExpressionUtil.addCleanupListener(tilesIter)
    private val hconf = partition.expressionConfig.hConf
    // The result-facing source keeps the ORIGINAL (remote) path, not the local staging copy.
    private val srcSelector = s"""NETCDF:"${partition.filePath}":${partition.subdatasetName}"""

    override def next(): Boolean = tilesIter.hasNext

    override def get(): InternalRow = {
        val tile = tilesIter.next()
        val tileRow = RasterSerializationUtil.tileToRow((-1L, tile._1, tile._2), BinaryType, hconf)
        RasterDriver.releaseDataset(tile._1)
        InternalRow.fromSeq(Seq(UTF8String.fromString(srcSelector), tileRow))
    }

    override def close(): Unit = {
        if (!isLocal) NodeFileManager.releaseRemote(partition.filePath)
    }
}
```

- [ ] **Step 5: Create `NetCDF_Batch` (subdataset enumeration at plan time)**

Enumerate subdatasets executor-side via a UDF (credential-aware on `/Volumes`, mirroring `OGR_Batch`). Filter to real grid variables, apply the optional `variable`/`variables` filter, emit one partition per `(file, variable)`.

```scala
package com.databricks.labs.gbx.rasterx.ds.netcdf

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.operations.RasterAccessors
import com.databricks.labs.gbx.util.{HadoopUtils, NodeFileManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.functions.{col, explode, udf}
import org.apache.spark.sql.types.StructType

/** Scan/Batch for netcdf_gdal: one partition per (file, grid-variable subdataset). */
class NetCDF_Batch(schema: StructType, options: Map[String, String]) extends Scan with Batch {

    override def readSchema(): StructType = schema
    override def toBatch: Batch = this

    override def planInputPartitions(): Array[InputPartition] = {
        val inPath = options("path")
        val sizeInMB = options.getOrElse("sizeInMB", "-1").toInt
        val filterRegex = options.getOrElse("filterRegex", ".*\\.nc$")
        // Optional variable filter (empty => keep all). Names, comma-separated.
        val wanted = (options.get("variables").orElse(options.get("variable")))
            .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSet).getOrElse(Set.empty[String])

        val spark = SparkSession.builder.getOrCreate
        val exprConfig = ExpressionConfig(spark)
        import spark.implicits._

        val files = HadoopUtils.listDataFilesSpark(spark, inPath)
            .filter(_.matches(filterRegex))
        NodeFileManager.init(exprConfig.hConf)

        // Executor-side enumeration: open each file, list SUBDATASETS, keep grid variables.
        val enumUDF = udf { (path: String) =>
            try {
                GDALManager.init(exprConfig)
                val localPath = NodeFileManager.readRemote(path)
                val ds = RasterDriver.read(localPath, Map.empty)
                val subs = RasterAccessors.subdatasetsMap(ds)
                // SUBDATASET_i_NAME -> "NETCDF:\"file\":var"; take NAME entries only.
                val vars = subs.toSeq.filter(_._1.endsWith("_NAME")).map { case (_, sel) =>
                    sel.reverse.takeWhile(_ != ':').reverse   // trailing :var
                }.filter(v => !v.endsWith("_bnds") && !v.endsWith("_bounds"))
                // Keep only subdatasets GDAL opens as a >1x1 raster (drops degenerate/coordinate arrays).
                val grids = vars.filter { v =>
                    try {
                        val sub = RasterDriver.read(s"""NETCDF:"$localPath":$v""", Map("isSubdataset" -> "true"))
                        val ok = sub.GetRasterXSize > 1 && sub.GetRasterYSize > 1 && sub.GetRasterCount >= 1
                        RasterDriver.releaseDataset(sub); ok
                    } catch { case _: Throwable => false }
                }
                RasterDriver.releaseDataset(ds)
                NodeFileManager.releaseRemote(path)
                grids.map(v => (path, v)).toArray
            } catch { case _: Throwable => Array.empty[(String, String)] }
        }

        val pairs = files.toDF("path")
            .select(explode(enumUDF(col("path"))).as("p"))
            .select("p._1", "p._2").as[(String, String)].collect()

        pairs
            .filter { case (_, v) => wanted.isEmpty || wanted.contains(v) }
            .map { case (file, v) => NetCDF_Partition(file, v, sizeInMB, exprConfig) }
            .toArray[InputPartition]
    }

    override def createReaderFactory(): PartitionReaderFactory =
        (partition: InputPartition) => new NetCDF_Reader(partition.asInstanceOf[NetCDF_Partition])
}
```

- [ ] **Step 6: Create `NetCDF_Table`**

Read-only raster table wiring `NetCDF_Batch` (mirror `GDAL_Table` but read-only — no write path for this reader).

```scala
package com.databricks.labs.gbx.rasterx.ds.netcdf

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._

/** Read-only Table for netcdf_gdal: batch read via NetCDF_Batch. */
class NetCDF_Table(schema: StructType, properties: Map[String, String]) extends Table with SupportsRead {

    override def name(): String = "netcdf_gdal"
    // noinspection ScalaDeprecation
    override def schema(): StructType = schema
    override def columns(): Array[Column] = schema.fields.map(f => Column.create(f.name, f.dataType, f.nullable))
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = { () =>
        new NetCDF_Batch(schema, properties ++ options.asScala)
    }
    override def capabilities(): java.util.Set[TableCapability] =
        Set(TableCapability.BATCH_READ).asJava
}
```

- [ ] **Step 7: Create `NetCDF_DataSource`**

Extends `GDAL_DataSource` (inherits the fixed `(source, tile)` `inferSchema`) with `DataSourceExtras`; overrides `getTable` to return the subdataset-aware `NetCDF_Table`.

```scala
package com.databricks.labs.gbx.rasterx.ds.netcdf

import com.databricks.labs.gbx.ds.DataSourceExtras
import com.databricks.labs.gbx.rasterx.ds.gdal.GDAL_DataSource
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._

/** GDAL TableProvider restricted to netCDF (driver = netCDF). Reads CF grid variables as
  * one (source, tile) row per variable; source = the NETCDF:"file":var subdataset selector.
  * Use format "netcdf_gdal". Read-only. */
//noinspection ScalaUnusedSymbol
class NetCDF_DataSource extends GDAL_DataSource with DataSourceExtras {

    override def dsExtraMap(checkMap: Map[String, String] = Map.empty): Map[String, String] =
        Map("driver" -> "netCDF")

    override def shortName(): String = "netcdf_gdal"

    override def inferSchema(options: CaseInsensitiveStringMap): StructType =
        super.inferSchema(extraCaseInsensitiveStringMap(options))

    override def getTable(schema: StructType, partitions: Array[Transform], properties: java.util.Map[String, String]): Table =
        new NetCDF_Table(schema, extraJavaUtilMap(properties).asScala.toMap)
}
```

- [ ] **Step 8: Register in META-INF/services**

Add one line to `src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`:

```
com.databricks.labs.gbx.rasterx.ds.netcdf.NetCDF_DataSource
```

- [ ] **Step 9: Run unit test to green**

Run (Task subagent): `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.ds.NetCDF_DataSourceTest' --log netcdf-gdal-unit.log`
Expected: PASS (all 4 unit tests).

- [ ] **Step 10: Write failing integration test (real fixture enumeration + filter)**

Append to `NetCDF_DataSourceTest.scala`. Use the coral fixtures (`/binary/netcdf-coral/`). First confirm the variable name(s) — a quick probe test that prints subdatasets is fine, but write the assertion against the known CF variable. (If the coral file is single-variable, assert `>= 1` grid var and that a bogus filter yields 0 rows; if multi-variable, assert the enumerated count.)

```scala
test("netcdf_gdal bare load enumerates grid variables into (source, tile) rows") {
    import com.databricks.labs.gbx.rasterx.functions._
    rasterx.functions.register(spark)
    val ncDir = this.getClass.getResource("/binary/netcdf-coral/").toString
    val df = spark.read.format("netcdf_gdal").option("sizeInMB", "1")
        .option("filterRegex", ".*20220101\\.nc$").load(ncDir)
    val rows = df.select("source").collect()
    rows.length should be >= 1
    all(rows.map(_.getString(0))) should startWith("NETCDF:")
}

test("netcdf_gdal variable filter naming an absent variable yields no rows") {
    import com.databricks.labs.gbx.rasterx.functions._
    rasterx.functions.register(spark)
    val ncDir = this.getClass.getResource("/binary/netcdf-coral/").toString
    val df = spark.read.format("netcdf_gdal")
        .option("filterRegex", ".*20220101\\.nc$")
        .option("variable", "no_such_variable_xyz").load(ncDir)
    df.count() shouldBe 0L
}
```

- [ ] **Step 11: Run integration test to verify then green**

Run (Task subagent): `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.ds.NetCDF_DataSourceTest' --log netcdf-gdal-int.log`
Expected: first PASS after implementation; if the grid filter over/under-includes, tighten the `_bnds` skip / `>1x1` check in `NetCDF_Batch` (Step 5) until the enumerated set matches the file's real grid variables. This test is the filter's spec.

- [ ] **Step 12: Bump the BenchDispatch count assert if tripped**

Per the `scala-benchdispatch-count-assert` gotcha, adding RasterX surfaces can trip a hardcoded `BenchDispatch.all.size == N`. These readers add no bench function, so likely untouched — but verify:

Run: `grep -rn "\.size shouldBe\|\.size ==" src/test/scala/**/BenchDispatch* 2>/dev/null; grep -rn "BenchDispatch.all.size" src/`
Expected: no change needed (no new bench dispatch). If a suite references reader counts, bump it.

- [ ] **Step 13: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/ds/netcdf/ \
        src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister \
        src/test/scala/com/databricks/labs/gbx/rasterx/ds/NetCDF_DataSourceTest.scala
git commit -m "feat(rasterx): netcdf_gdal heavy raster reader via subdataset enumeration

Reads CF grid variables from a .nc file as one (source, tile) row per
variable. Plans one partition per (file, subdataset), enumerating
SUBDATASETS executor-side (credential-aware for /Volumes) and filtering
to real >1x1 grid variables. source = the NETCDF:\"file\":var selector,
matching the light netcdf_gbx raster contract. variable/variables is an
optional filter. Read-only.

Co-authored-by: Isaac"
```

---

### Task 3: `netcdf_ogr` heavy vector reader (CF-DSG features)

A thin OGR reader — the OGR netCDF driver surfaces native CF Discrete Sampling Geometry features into the shared vector schema. No new plan-time mechanism.

**Files:**
- Create: `src/main/scala/com/databricks/labs/gbx/vectorx/ds/netcdf/NetCDF_OGR_DataSource.scala`
- Modify: `src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister` (+1 line)
- Test: `src/test/scala/com/databricks/labs/gbx/vectorx/ds/NetCDF_OGR_DataSourceTest.scala`
- Possibly create: a small CF-DSG `.nc` fixture under `src/test/resources/binary/netcdf-dsg/` (existing fixtures are grids)

**Interfaces:**
- Consumes: `OGR_DataSource` (inherited `inferSchema`, `getTable`, `supportsExternalMetadata`, write-guard), `DataSourceExtras`.
- Produces: shortName `"netcdf_ogr"`; `dsExtraMap` = `Map("driverName" -> "netCDF")`; emits the shared vector schema (attributes + `geom_0` WKB + `geom_0_srid` / `geom_0_srid_proj`).

- [ ] **Step 1: Write failing unit test**

Create `src/test/scala/com/databricks/labs/gbx/vectorx/ds/NetCDF_OGR_DataSourceTest.scala`:

```scala
package com.databricks.labs.gbx.vectorx.ds

import com.databricks.labs.gbx.vectorx.ds.netcdf.NetCDF_OGR_DataSource
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

class NetCDF_OGR_DataSourceTest extends PlanTest with SilentSparkSession {

    test("netcdf_ogr short name is netcdf_ogr") {
        new NetCDF_OGR_DataSource().shortName() shouldBe "netcdf_ogr"
    }

    test("netcdf_ogr injects driverName netCDF") {
        new NetCDF_OGR_DataSource().dsExtraMap() shouldBe Map("driverName" -> "netCDF")
    }

    test("netcdf_ogr is a TableProvider and DataSourceRegister") {
        val ds = new NetCDF_OGR_DataSource()
        ds shouldBe a[org.apache.spark.sql.connector.catalog.TableProvider]
        ds shouldBe a[org.apache.spark.sql.sources.DataSourceRegister]
    }
}
```

- [ ] **Step 2: Run to verify failure (compile error)**

Run (Task subagent): `gbx:test:scala --suite 'com.databricks.labs.gbx.vectorx.ds.NetCDF_OGR_DataSourceTest' --log netcdf-ogr-unit.log`
Expected: FAIL — class not found.

- [ ] **Step 3: Create `NetCDF_OGR_DataSource`**

Mirror `GeoJSON_DataSource` exactly, swapping the driver and messages:

```scala
package com.databricks.labs.gbx.vectorx.ds.netcdf

import com.databricks.labs.gbx.ds.DataSourceExtras
import com.databricks.labs.gbx.vectorx.ds.ogr.OGR_DataSource
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** OGR-based TableProvider for CF Discrete Sampling Geometry features in netCDF (driverName = netCDF).
  * Surfaces native DSG point/profile/trajectory features into the shared vector schema. Read-only.
  * Does NOT flatten swaths to per-cell points — that is the light netcdf_gbx vector mode only. */
//noinspection ScalaUnusedSymbol
class NetCDF_OGR_DataSource extends OGR_DataSource with DataSourceExtras {

    override def dsExtraMap(checkMap: Map[String, String] = Map.empty): Map[String, String] =
        Map("driverName" -> "netCDF")

    override def shortName(): String = "netcdf_ogr"

    override protected def writeGuardMessage(path: String): String =
        "'netcdf_ogr' is a read-only reader; write vector data with the light geojson_gbx writer " +
        "(or another _gbx vector writer)."

    override def inferSchema(options: CaseInsensitiveStringMap): StructType =
        super.inferSchema(extraCaseInsensitiveStringMap(options))

    override def getTable(schema: StructType, partitions: Array[Transform], properties: java.util.Map[String, String]): Table =
        super.getTable(schema, partitions, extraJavaUtilMap(properties))
}
```

- [ ] **Step 4: Register in META-INF/services**

Add:
```
com.databricks.labs.gbx.vectorx.ds.netcdf.NetCDF_OGR_DataSource
```

- [ ] **Step 5: Run unit test to green**

Run (Task subagent): `gbx:test:scala --suite 'com.databricks.labs.gbx.vectorx.ds.NetCDF_OGR_DataSourceTest' --log netcdf-ogr-unit.log`
Expected: PASS.

- [ ] **Step 6: Stage a CF-DSG fixture and write the integration test**

The existing fixtures (CMIP5/coral/ECMWF) are grids — the OGR netCDF driver reads DSG features, not grids, so a grid `.nc` yields zero features. Create a tiny CF-DSG point file. Add a generator (committed as a helper script + its output `.nc`) or write it in-test via a temp file. Prefer an in-test temp `.nc` built with the JVM's netCDF write path if available; otherwise stage a small committed fixture at `src/test/resources/binary/netcdf-dsg/points.nc` produced by this Python snippet (run once, commit the `.nc`):

```python
# scripts/testdata/make_netcdf_dsg.py  (run once; commit the .nc, not required at test time)
from netCDF4 import Dataset
import numpy as np
with Dataset("src/test/resources/binary/netcdf-dsg/points.nc", "w") as ds:
    ds.featureType = "point"          # CF-DSG marker the OGR driver keys on
    ds.createDimension("obs", 5)
    lat = ds.createVariable("latitude", "f8", ("obs",)); lat.standard_name = "latitude"; lat.units = "degrees_north"
    lon = ds.createVariable("longitude", "f8", ("obs",)); lon.standard_name = "longitude"; lon.units = "degrees_east"
    val = ds.createVariable("ch4", "f4", ("obs",)); val.coordinates = "latitude longitude"
    lat[:] = [50.0, 50.1, 50.2, 50.3, 50.4]
    lon[:] = [10.0, 10.1, 10.2, 10.3, 10.4]
    val[:] = np.arange(5, dtype="float32")
```

Then the integration test:

```scala
test("netcdf_ogr reads CF-DSG point features into the shared vector schema") {
    val dsgDir = this.getClass.getResource("/binary/netcdf-dsg/").toString
    val df = spark.read.format("netcdf_ogr").load(dsgDir)
    df.columns should contain allOf ("geom_0", "geom_0_srid", "geom_0_srid_proj")
    df.count() shouldBe 5L
}

test("netcdf_ogr on a grid file yields no features (empty, non-erroring)") {
    val gridDir = this.getClass.getResource("/binary/netcdf-coral/").toString
    val df = spark.read.format("netcdf_ogr").option("filterRegex", ".*20220101\\.nc$").load(gridDir)
    df.count() shouldBe 0L
}
```

If the OGR netCDF driver in the container does not expose the DSG file as features (driver build variance), fall back to asserting only the schema-infer + empty-on-grid behavior and note the DSG-read case as environment-dependent in the test comment. Verify against the actual container GDAL/OGR build first.

- [ ] **Step 7: Run integration test to green**

Run (Task subagent): `gbx:test:scala --suite 'com.databricks.labs.gbx.vectorx.ds.NetCDF_OGR_DataSourceTest' --log netcdf-ogr-int.log`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/vectorx/ds/netcdf/ \
        src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister \
        src/test/scala/com/databricks/labs/gbx/vectorx/ds/NetCDF_OGR_DataSourceTest.scala \
        src/test/resources/binary/netcdf-dsg/points.nc scripts/testdata/make_netcdf_dsg.py
git commit -m "feat(vectorx): netcdf_ogr heavy DSG vector reader

Thin OGR reader (driverName=netCDF) surfacing native CF Discrete
Sampling Geometry features into the shared vector schema. Read-only;
grid-only files yield no features. Does not flatten swaths to points
(light netcdf_gbx vector mode only). Adds a small CF-DSG test fixture.

Co-authored-by: Isaac"
```

---

### Task 4: Cross-tier raster parity test (the correctness gate for the grid filter)

On a shared gridded fixture, `netcdf_gdal` and light `netcdf_gbx` raster mode must enumerate the **same variable set** and produce the **same per-variable tile** (CRS + geotransform equal, cell values within tolerance). Byte parity is not expected (xarray/rasterio vs GDAL).

**Files:**
- Test: `docs/tests/python/...` OR a dedicated cross-tier test. Since heavy needs the JVM + Docker and light is Python, the practical parity harness is a **Python doc/integration test that reads both** through Spark (heavy JAR present in the Docker env). Place at `python/geobrix/test/ds/test_netcdf_cross_tier.py` guarded to run only where the JAR + GDAL are available (Docker). Follow the `docker-volumes-for-integration-tests` gating (skip cleanly when the heavy reader/format is unregistered).

**Interfaces:**
- Consumes: both registered formats (`netcdf_gdal`, `netcdf_gbx`); the coral/CMIP5 fixture copied to a readable path; `readable_variables` (light) as the reference variable set.

- [ ] **Step 1: Write the parity test (skipped outside Docker/heavy env)**

```python
import numpy as np
import pytest
from rasterio.io import MemoryFile


def _heavy_available(spark):
    try:
        spark.read.format("netcdf_gdal")
        return True
    except Exception:
        return False


@pytest.mark.integration
def test_netcdf_gdal_matches_light_raster(spark, netcdf_grid_fixture):
    if not _heavy_available(spark):
        pytest.skip("netcdf_gdal (heavy JAR) not available in this environment")
    from databricks.labs.gbx.ds.netcdf import NetcdfGbxDataSource
    spark.dataSource.register(NetcdfGbxDataSource)

    path = netcdf_grid_fixture  # a single gridded .nc with known variable(s)
    light = spark.read.format("netcdf_gbx").load(path).collect()
    heavy = spark.read.format("netcdf_gdal").load(path).collect()

    # same enumerated variable set (source ends in :var for both tiers)
    light_vars = sorted(r["source"].rsplit(":", 1)[-1] for r in light)
    heavy_vars = sorted(r["source"].rsplit(":", 1)[-1] for r in heavy)
    assert light_vars == heavy_vars

    # per-variable tile: CRS equal, values within tolerance
    def by_var(rows):
        out = {}
        for r in rows:
            v = r["source"].rsplit(":", 1)[-1]
            with MemoryFile(bytes(r["tile"]["raster"])) as mf, mf.open() as ds:
                out[v] = (ds.crs.to_epsg(), ds.read(1))
        return out

    lm, hm = by_var(light), by_var(heavy)
    for v in light_vars:
        assert lm[v][0] == hm[v][0]  # same EPSG
        np.testing.assert_allclose(lm[v][1], hm[v][1], rtol=1e-4, atol=1e-4,
                                   equal_nan=True)
```

Add a `netcdf_grid_fixture` fixture (module-scoped) that copies one coral/CMIP5 `.nc` into a temp path, or points at the mounted test-resources path in Docker.

- [ ] **Step 2: Run in Docker; refine the heavy grid filter if the variable sets differ**

Run (Task subagent, Docker with heavy JAR + volumes): `gbx:test:python --path python/geobrix/test/ds/test_netcdf_cross_tier.py --log netcdf-parity.log`
Expected: PASS. If `light_vars != heavy_vars`, the heavy `NetCDF_Batch` grid filter (Task 2 Step 5) is over/under-inclusive vs light `classify()` — adjust the `_bnds`/`>1x1` filter until they agree, re-run.

- [ ] **Step 3: Commit**

```bash
git add python/geobrix/test/ds/test_netcdf_cross_tier.py
git commit -m "test(netcdf): cross-tier raster parity netcdf_gdal vs netcdf_gbx

Same gridded fixture -> same enumerated variable set and same per-variable
tile (EPSG equal, cell values within tolerance). This is the correctness
gate for the heavy subdataset grid filter. Skips cleanly where the heavy
JAR is unavailable.

Co-authored-by: Isaac"
```

---

### Task 5: Documentation, release notes, reader tables

**Files:**
- Modify: `docs/docs/readers/netcdf.mdx` (document `netcdf_gdal` + `netcdf_ogr` alongside `netcdf_gbx`; the optional-filter contract; the light behavior change; the swath→points light-only asymmetry). Create if it does not exist.
- Modify: `docs/docs/beta-release-notes.mdx` (new heavy readers + light behavior change).
- Modify: `CLAUDE.md` (add `netcdf_gdal`, `netcdf_ogr` to the Readers named-reader lists).

**Interfaces:** none (docs). User-facing docs voice — no internal vocabulary (no "wave N", no subagent/dispatch references).

- [ ] **Step 1: Check whether `docs/docs/readers/netcdf.mdx` exists**

Run: `ls docs/docs/readers/netcdf.mdx 2>/dev/null && echo exists || echo missing`
If missing, create it modeled on a sibling reader page (e.g. `docs/docs/readers/geojson.mdx` or `gtiff.mdx`) — check `ls docs/docs/readers/`.

- [ ] **Step 2: Write/extend the readers page**

Document all three readers and the unified contract. Include runnable examples (per the doc-tests-are-source rule, code should come from `docs/tests/` — if the page uses executable imports, add the snippet to the corresponding doc-test module and import via raw-loader; otherwise keep examples minimal and accurate). Cover:
- Bare `load` returns all readable variables (raster: one tile row per grid variable; vector: all DSG features).
- `variable`/`variables` is an optional filter.
- `source` column is the `NETCDF:"file":var` selector.
- `netcdf_ogr` reads native CF-DSG features only; swath→points is `netcdf_gbx` vector-mode only.

- [ ] **Step 3: Add release notes entry**

In `docs/docs/beta-release-notes.mdx`, under the current unreleased/next section, add:
- New heavy readers `netcdf_gdal` (raster) and `netcdf_ogr` (DSG vector).
- **Behavior change:** light `netcdf_gbx` `variable`/`variables` option is now an optional filter; a bare load returns all readable variables (previously raised). Explicit-`variable` calls are unchanged.

- [ ] **Step 4: Update CLAUDE.md reader lists**

In `CLAUDE.md`, the "Readers are namespace-suffixed" section: add `netcdf_gdal` to the Raster (GDAL) list and `netcdf_ogr` to the Vector (OGR) list.

- [ ] **Step 5: Internals-leak + link checks**

Run: `grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/ 2>/dev/null` (expect nothing).
Verify doc links resolve (per the queued `docs-link-audit-pending` note, sanity-check the readers page links).

- [ ] **Step 6: Commit**

```bash
git add docs/docs/readers/netcdf.mdx docs/docs/beta-release-notes.mdx CLAUDE.md
git commit -m "docs(netcdf): document netcdf_gdal + netcdf_ogr and the light behavior change

Adds the readers page coverage for the two heavy readers and the unified
optional-filter contract; release-notes the light netcdf_gbx bare-load
behavior change; adds both readers to the CLAUDE.md reader tables.

Co-authored-by: Isaac"
```

---

### Task 6: Benchmarking — NetCDF corpus + format-parameterized reader-bench path

Add a same-corpus heavy-vs-light throughput bench over real S5P granules staged by the existing `TropomiDownloader`. The generic `readers.run_format_read(spark, dir, ..., fmt=...)` is already format-generic; the missing pieces are the corpus and the invocation cell.

**Files:**
- Modify: bench reader harness — `readers.py` (the `_list_tifs` glob is `.tif`-only) and/or `cluster.py` (the reader cell hard-codes `rows/` + `filterRegex: .*\.tif$`). Add a `.nc`-capable path + a NetCDF corpus stager.
- Modify: `docs/docs/api/benchmarking.mdx` (per the `bench-changes-update-docs` rule).

**Interfaces:**
- Consumes: `readers.run_format_read(spark, netcdf_dir, ..., fmt="netcdf_gdal" | "netcdf_gbx")`, `TropomiDownloader().download(bbox, out_dir, temporal=...)` (download-and-stop mode), the reader-bench corpus Volume (parallel `netcdf/` subdir alongside `rows/`).

- [ ] **Step 1: Locate the bench harness reader path**

Run: `grep -rn "_list_tifs\|run_format_read\|filterRegex.*tif\|rows/" scripts/ python/ docs/ 2>/dev/null | grep -i "bench\|reader\|cluster" | head -30`
Identify the exact files behind `readers.py` and `cluster.py` referenced in the spec (§6).

- [ ] **Step 2: Add a format+glob-parameterized listing**

Generalize the `.tif`-only glob so the reader bench can target `.nc`. Add a `filterRegex`/extension parameter (default `.*\.tif$`) rather than hard-coding, so `.*\.nc$` works. Keep the existing GeoTIFF path behavior identical (same default).

- [ ] **Step 3: Add the NetCDF corpus stager**

A helper (bench-only, not product) that calls `TropomiDownloader().download(bbox, out_dir=<CORPUS>/netcdf, temporal=...)` in download-and-stop mode to stage real S5P L2 CH4 granules as `{item_id}.nc`. Guard: if the corpus dir is empty (no Planetary Computer token / download unavailable), the bench **skips cleanly** rather than failing (spec §8).

- [ ] **Step 4: Add the NetCDF reader-bench invocation cell**

Add a cell/path that runs `run_format_read(spark, netcdf_dir, fmt="netcdf_gdal")` and `fmt="netcdf_gbx"` (raster mode) over the same staged `.nc` dir with `filterRegex: .*\.nc$` — a true same-corpus heavy-vs-light comparison. Follow the bench pre-flight discipline (scope from real run output, non-empty corpus, correct worker count, truthful stamp, guard dups). Use the standing bench defaults (spark-path 1000 tiles / pure-core 1; `--row-counts 1000`).

- [ ] **Step 5: Document the bench addition**

Update `docs/docs/api/benchmarking.mdx`: the reader-bench now covers NetCDF, and document the `TropomiDownloader`-staged S5P corpus recipe (real granules, PC token at stage time, corpus decoupled from `read()`), and the swath-vs-grid caveat (S5P = throughput bench; parity uses a gridded fixture).

- [ ] **Step 6: Commit**

```bash
git add <bench harness files> docs/docs/api/benchmarking.mdx
git commit -m "bench(netcdf): same-corpus netcdf_gdal vs netcdf_gbx reader bench

Adds a format-parameterized .nc reader-bench path and a TropomiDownloader-
staged real S5P L2 CH4 granule corpus (download-and-stop; skips cleanly
when unavailable). Documents the corpus recipe and swath-vs-grid caveat
in benchmarking.mdx.

Co-authored-by: Isaac"
```

---

## Sequencing note

Tasks 1 → 2 → 3 → 4 are the spec's implementation order (light contract first, then heavy raster, then heavy vector, then the parity gate). Tasks 5 (docs) and 6 (bench) come last and can be done in either order. The parity test (Task 4) is what refines the heavy grid filter (Task 2 Step 5) — expect to revisit the filter once after Task 4 runs in Docker.

## Loose ends to surface at "done" (per report-loose-ends-after-spec-execution)

- The light NetCDF **writer** is a separate later cycle on this branch — NOT in this plan.
- Wheel rebuild + Volume staging is required after the Task 1 Python change (per `whl-change-rebuild-and-stage`).
- The heavy readers ship in the JAR; cluster benching needs the JAR staged before cluster start (per `jar-stage-before-cluster-start`).
- Re-verify memory-based gates live at completion: `gh auth switch --user mjohns-databricks` before any push; `gbx:lint:python --check` (Docker black) and `gbx:lint:scalastyle`; run affected package tests.
