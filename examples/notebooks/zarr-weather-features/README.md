# Weather time-series features with Zarr

A self-contained notebook that takes an ERA5-like weather cube, stored as Zarr, through a
feature workflow with GeoBrix's **transactional Zarr stores**: register the cube in place as a
Unity Catalog table, read a box of it as N-dimensional blocks and as a table of cells, sample
the hourly series at 100 sites into a features table, derive wind speed and write it back to the
store as a new variable in one transaction, read the history and an earlier version, map the
new variable as GeoBrix raster tiles clipped with `rst_clip`, and tag the version the training
features need, so that it stays readable for training.

![Zarr weather features: register a cube in place, read and sample it, write a derived variable back, map it as tiles, tag the version](../../../resources/images/diagrams/zarr-weather-features/zarr-weather-features.png)

> **Lightweight tier only.** Zarr runs in the lightweight tier, so there is no JAR or init
> script: `geobrix[light_env6,zarr_env6,vizx]` on **Serverless environment 6** (the notebook's
> target), or `geobrix[light_dbr19,zarr_dbr19,vizx]` on a classic **Databricks Runtime 19**
> cluster. See [Zarr Stores](https://databrickslabs.github.io/geobrix/docs/formats/zarr#what-you-need).

> **Data: the GeoBrix sample cube.** Two days of made-up hourly weather on a one-degree global
> grid, `time` (48) by `latitude` (181) by `longitude` (360), with `t2m`, `u10`, `v10` and `msl`,
> as a sharded Zarr v3 store (about 3.6 MB). It is generated, not downloaded: run
> `write_era5_cube` from `databricks.labs.gbx.sample.zarr_era5` into a Volume of yours, and point
> `CUBE` at the `era5_like_v3.zarr` it writes.

---

## Notebooks at a glance

### 01\_zarr\_weather\_features.ipynb

The whole walk-through. It registers the sample cube as a writable store (7 arrays in 19 objects,
nothing copied), reads a box over Europe as six blocks and as 25,584 cells, samples 100 sites
into a 4,800-row features table, writes `wind_speed` back as version 2 (one pack file of about
1.4 MB in the overlay), reads version 1 to show the store before the write, maps one hour of wind
speed over the whole grid and clipped to Australia, and tags version 2 `training`; the features
reproduce from the tag with no row different. A last check confirms the cube's files are
unchanged. The committed outputs are from a run on Serverless environment 6.

---

## Files

| File | Purpose |
|---|---|
| `01_zarr_weather_features.ipynb` | The walk-through: register → read → sample → write back → history → tiles → tag. |

---

## Prerequisites

- **Serverless environment 6, or Databricks Runtime 19** (Python 3.12). Zarr stores need one of
  the two.
- **GeoBrix 0.6.0.** Point the `%pip install` cell at your staged
  `geobrix-0.6.0-py3-none-any.whl`, with the extras for your compute (above).
- **A schema and a Volume you can write to.** The notebook creates two Unity Catalog tables in
  `CATALOG.SCHEMA` (the store's manifest table and the features table) and writes the files of
  its writes to a folder under `WORK_DIR`. Each run gets its own `RUN_ID`, so runs never collide.
  The registered cube is only read; the notebook checks at the end that its files are unchanged.

---

## Run order

Run all cells top to bottom. The `%pip install` + `%restart_python` pair at the top restarts the
Python kernel.

1. **Install and restart.**
2. **Settings** — `CUBE`, `CATALOG`, `SCHEMA`, `WORK_DIR`; a new `RUN_ID` names this run's tables
   and folder.
3. **Register** — `gbx_zarr_register(..., writable=True, overlay_root=...)`.
4. **Read** — `gbx_zarr_read` as blocks, then `output="table"`.
5. **Sample** — `gbx_zarr_sample` at 100 sites, saved as the features table.
6. **Write back** — wind speed from the `u10` and `v10` blocks, `gbx_zarr_write(mode="update")`.
7. **History** — `gbx_zarr_history`, and `version_as_of` reads of the earlier version.
8. **Tiles** — `output="tiles"`, `rst_clip` to a polygon, `plot_raster`.
9. **Tag** — `gbx_zarr_tag(..., "training")`, then a reproduction of the features from the tag.
10. **Clean up** (optional) — `CLEAN_UP = True` drops the run's tables and deletes its folder.

---

## Data flow

```text
era5_like_v3.zarr  (time 48 x latitude 181 x longitude 360; t2m, u10, v10, msl)
        │
        ▼  gbx_zarr_register(writable=True, overlay_root=...)    nothing copied
Manifest table era5_weather_<run>  (Delta; one row per array document and shard)   [version 1]
        │
        ├─▶ gbx_zarr_read(sel=box)            blocks, then output="table"
        │
        ├─▶ gbx_zarr_sample(100 sites)  ──▶  features table era5_site_features_<run>
        │                                     (+ wind_speed = sqrt(u10² + v10²))
        │
        ▼  u10 + v10 blocks → wind_speed blocks (pandas UDF)
           gbx_zarr_write(mode="update", variables=["wind_speed"])  → overlay pack files
Manifest + wind_speed                                                              [version 2]
        │
        ├─▶ gbx_zarr_history; gbx_zarr_read(version_as_of=1)
        │
        ├─▶ gbx_zarr_read(output="tiles") → rst_clip(polygon) → plot_raster
        │
        ▼  gbx_zarr_tag("training", version_as_of=2)
Tagged version: outlives retain and vacuum; the features reproduce from it     [a tag commit]
```

---

## Key GeoBrix functions shown

- **Zarr stores** (`databricks.labs.gbx.zarr`): `gbx_zarr_register`, `gbx_zarr_read`
  (`output="blocks"`, `"table"`, `"tiles"`; `sel`, `isel`, `version_as_of`), `gbx_zarr_sample`,
  `gbx_zarr_write` (`mode="update"` with a new variable), `gbx_zarr_history`, `gbx_zarr_tag`,
  `block_to_numpy`.
- **GeoBrix RasterX** (`rx.*`): `rst_clip`, `rst_width`, `rst_height`, `rst_avg`, `rst_max` on
  tiles cut from the cube.
- **GeoBrix viz** (`gbx.vizx`): `plot_raster`.
- **Reference**: [Zarr Stores](https://databrickslabs.github.io/geobrix/docs/formats/zarr) ·
  [Zarr Reader](https://databrickslabs.github.io/geobrix/docs/readers/zarr) ·
  [Zarr Writer](https://databrickslabs.github.io/geobrix/docs/writers/zarr) ·
  [Zarr Function Reference](https://databrickslabs.github.io/geobrix/docs/api/zarr-functions).

---

## Gotchas

- **A registered store is never written.** Writes to a writable registration go to its
  `overlay_root` as new files, and the overlay rows win over the store's for the keys they touch.
  To make other Zarr readers see the writes, publish them (`gbx_zarr_export(method="publish")`),
  which needs write access to the store.
- **Name a new variable.** An `update` that creates a variable the store lacks needs
  `variables=[...]`; without it the writer knows only the store's arrays and refuses the block.
- **Longitudes 0 to 359.** The sample cube's longitude axis runs from 0 to 359, so its tiles are
  `EPSG:4326` with x from 0 to 360 and are not shifted into -180..180. Points and polygons use the
  same convention.
- **Versions last for `retain`.** Old versions stay readable for the store's `retain` period
  (7 days by default); a tag keeps one for as long as the tag exists.

---

## Related resources

- [Zarr Stores](https://databrickslabs.github.io/geobrix/docs/formats/zarr) — the transactional model: versions, tags, retention, file modes
- [Virtual Tiles](https://databrickslabs.github.io/geobrix/docs/api/virtual-tiles) — how a tile that holds no pixels is opened
- [GeoBrix RasterX API](https://databrickslabs.github.io/geobrix/docs/api/raster-functions)
- [GeoBrix Viz API](https://databrickslabs.github.io/geobrix/docs/api/vizx)
