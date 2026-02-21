#!/usr/bin/env python3
"""
Merge multiple scoverage.xml files from separate test runs (e.g. per-package runs)
into a single scoverage.xml by taking, for each statement, the max invocation-count
across all files. Use with granular CI: run scoverage:test per package in parallel,
then run this script and upload the merged result to Codecov.

Usage:
  python merge_scoverage.py -o merged/scoverage.xml path/to/scoverage-rasterx.xml path/to/scoverage-gridx.xml ...
  python merge_scoverage.py -o merged/scoverage.xml coverage-artifacts/
"""
import argparse
import xml.etree.ElementTree as ET
from pathlib import Path


def statement_key(stmt):
    """Unique key for a statement: (source, line, start, end)."""
    return (
        stmt.get("source", ""),
        stmt.get("line", ""),
        stmt.get("start", ""),
        stmt.get("end", ""),
    )


def get_invocation_count(stmt):
    try:
        return int(stmt.get("invocation-count", 0))
    except (TypeError, ValueError):
        return 0


def collect_statements(root):
    """Yield (key, invocation_count, element) for every statement under root."""
    for stmt in root.iter("statement"):
        key = statement_key(stmt)
        count = get_invocation_count(stmt)
        yield key, count, stmt


def merge_scoverage_files(input_paths, output_path):
    """Merge multiple scoverage XMLs into one; output to output_path.
    Takes union of all statements from all runs; for each statement key
    uses the max invocation-count so per-package runs combine correctly.
    """
    # Collect max invocation-count and one element per statement key from all files
    key_to_count = {}
    key_to_element = {}  # keep one element (we'll set count and use for output)

    for path in input_paths:
        path = Path(path)
        if not path.exists():
            continue
        tree = ET.parse(path)
        root = tree.getroot()
        for key, count, stmt in collect_statements(root):
            if key not in key_to_count or count > key_to_count[key]:
                key_to_count[key] = count
                key_to_element[key] = stmt

    if not key_to_count:
        raise SystemExit("No valid input scoverage XML files found.")

    # Build merged root: use first file's root as template for tag/attrs/structure
    first_path = Path(input_paths[0])
    base_tree = ET.parse(first_path)
    base_root = base_tree.getroot()

    # Find the element that contains statement children (e.g. <packages><package> or <statements>)
    stmt_parent = None
    for elem in base_root.iter():
        if any(c.tag.endswith("statement") for c in elem):
            stmt_parent = elem
            break
    if stmt_parent is None:
        stmt_parent = base_root

    # Clear existing statements from template and add merged ones (one element per key, max count)
    for child in list(stmt_parent):
        if child.tag.endswith("statement"):
            stmt_parent.remove(child)

    for key, count in key_to_count.items():
        stmt = key_to_element[key]
        stmt.set("invocation-count", str(count))
        stmt_parent.append(stmt)

    total = len(key_to_count)
    invoked = sum(1 for c in key_to_count.values() if c > 0)
    base_root.set("statement-count", str(total))
    base_root.set("statements-invoked", str(invoked))
    if total > 0:
        rate = round(100.0 * invoked / total, 2)
        base_root.set("statement-rate", f"{rate:.2f}")

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    base_tree.write(
        out,
        encoding="unicode",
        default_namespace="",
        xml_declaration=True,
        method="xml",
    )
    print(f"Wrote merged scoverage: {out} (statements={total}, invoked={invoked})")


def main():
    ap = argparse.ArgumentParser(description="Merge scoverage XML files from multiple runs.")
    ap.add_argument(
        "-o", "--output",
        required=True,
        help="Output path for merged scoverage.xml",
    )
    ap.add_argument(
        "inputs",
        nargs="+",
        help="Input scoverage.xml files or directories containing scoverage.xml",
    )
    args = ap.parse_args()

    input_files = []
    for x in args.inputs:
        p = Path(x)
        if p.is_file():
            input_files.append(p)
        elif p.is_dir():
            for f in p.glob("**/scoverage.xml"):
                input_files.append(f)
            if not any(p.glob("**/scoverage.xml")):
                # Single file named like the dir
                single = p / "scoverage.xml"
                if single.exists():
                    input_files.append(single)
        else:
            raise SystemExit(f"Not found: {p}")

    if not input_files:
        raise SystemExit("No scoverage.xml inputs found.")
    merge_scoverage_files(input_files, args.output)


if __name__ == "__main__":
    main()
