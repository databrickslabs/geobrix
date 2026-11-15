#!/usr/bin/env python3
"""What the GeoBrix Lakeflow connector should publish for a landing folder (gbx:test:lakeflow-connector).

Runs in the connector venv (.venv-connectors, made by gbx:test:connectors: PySpark >= 4.1, GeoBrix from
python/geobrix/src, the pinned framework wheel and the connector as a wheel). It reads a LOCAL copy of the
landing folder with the connector's own code (``GeoBrixLakeflowConnect``): the schema of each table, and
the rows of each of its files. The workspace run must publish the same.

Usage: python _lfc_expect.py --root <local landing dir> --tables '<json: [{"table": .., "config": {..}}]>'
Prints one JSON object: {table: {"columns": [[name, type], ...], "format": reader, "files": {relpath: rows}}}.
"""

import argparse
import json
import os
import sys


def expect(root: str, tables: list) -> dict:
    from databricks.labs.community_connector.sources.geobrix.lakeflow_connect import GeoBrixLakeflowConnect

    root = os.path.abspath(root)
    out = {}
    for entry in tables:
        name, cfg = entry["table"], dict(entry.get("config") or {})
        conn = GeoBrixLakeflowConnect({"root_path": root})
        schema = conn.get_table_schema(name, cfg)
        files = {}
        for part in conn.get_partitions(name, cfg):
            rows = sum(b.num_rows for b in conn.iter_member_batches(part["path"], part["mtime_ns"], cfg, schema, name))
            files[os.path.relpath(part["path"], root)] = rows
        out[name] = {
            "columns": [[f.name, f.dataType.simpleString()] for f in schema.fields],
            "format": conn._table_format(name, cfg),
            "files": files,
        }
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--root", required=True)
    ap.add_argument("--tables", required=True, help="JSON list of {table, config}")
    a = ap.parse_args(argv)
    print(json.dumps(expect(a.root, json.loads(a.tables)), sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
