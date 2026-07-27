"""netcdf_gbx vector mode: DSG points, or any 2-D field coerced to per-cell points.

Output schema mirrors the light vector reader convention: attribute columns (one
per requested variable, typed) then geom_0 (plain WKB) + geom_0_srid +
geom_0_srid_proj string columns. SRID travels in the string column, not as EWKB.
"""

from __future__ import annotations

from typing import Dict, Iterator, List, Optional, Sequence, Tuple

from pyspark.sql.datasource import DataSourceReader, InputPartition
from pyspark.sql.types import BinaryType, StringType, StructField, StructType

from databricks.labs.gbx.ds import _listing, _netcdf


class _NcFilePartition(InputPartition):
    def __init__(self, file_path: str):
        self.file_path = file_path


class NetcdfVectorReader(DataSourceReader):
    def __init__(self, options: Dict[str, str]):
        self.path = options.get("path")
        if not self.path:
            raise ValueError("netcdf_gbx requires a 'path' (e.g. .load(path)).")
        self.options = dict(options)
        self.group: Optional[str] = options.get("group")
        self.filter_regex = options.get("filterRegex", ".*")

    def _members(self) -> List[str]:
        return _listing.list_files(self.path, self.filter_regex)

    def _variables(self, ds) -> List[str]:
        return _netcdf.select_variables(ds, self.options, "vector")

    def schema(self) -> StructType:
        members = self._members()
        if not members:
            raise ValueError(
                f"netcdf_gbx: no files matched filterRegex {self.filter_regex!r} "
                f"under {self.path!r} — nothing to infer a schema from."
            )
        fields: List[StructField] = []
        with _netcdf.open_dataset(members[0], self.group) as ds:
            for name in self._variables(ds):
                fields.append(
                    StructField(name, _netcdf.np_to_spark(ds[name].values.dtype), True)
                )
        fields.append(StructField("geom_0", BinaryType(), True))
        fields.append(StructField("geom_0_srid", StringType(), True))
        fields.append(StructField("geom_0_srid_proj", StringType(), True))
        return StructType(fields)

    def partitions(self) -> Sequence[InputPartition]:
        return [_NcFilePartition(f) for f in self._members()]

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
