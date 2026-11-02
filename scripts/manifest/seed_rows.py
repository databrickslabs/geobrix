"""Draft manifest rows for whole function families from today's sources (a migration tool).

Trust order: the light registration (names, L3 refs, return types) > the Scala companions (L1 refs, arities)
> function-info.json (usage, since) > the light Column API (parameter names, defaults, docstrings,
str-as-literal parameters, force-output keywords) > the classification CSV (family, kind).
Every value the tool had to guess goes to the report; the implementer settles each before committing.
"""

from __future__ import annotations

import argparse
import ast
import csv
import importlib
import importlib.util
import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

REPO = Path(__file__).resolve().parents[2]
PKG = REPO / "python/geobrix/src/databricks/labs/gbx"
SCALA = REPO / "src/main/scala/com/databricks/labs/gbx"
FAMILY_OF_CLASS = {
    "F01": "grid-scalar",
    "F02": "grid-array",
    "F03": "grid-generator",
    "F04": "cell-aggregate",
    "F05": "vector-scalar",
    "F06": "vector-scalar",
    "F07": "vector-aggregate",
    "F08": "vector-generator",
    "F09": "raster-accessor",
    "F10": "raster-stat",
    "F11": "raster-tile-op",
    "F12": "raster-constructor",
    "F13": "raster-rasterize",
    "F14": "raster-array-op",
    "F15": "raster-aggregate",
    "F16": "raster-aggregate",
    "F17": "raster-vectorize",
    "F18": "raster-generator",
    "F19": "raster-generator",
    "F20": "raster-to-grid",
    "F21": "web-tiles",
    "F22": "raster-file-op",
}
ORACLE_OF_FAMILY = {
    f: f.upper().replace("-", "_") for f in set(FAMILY_OF_CLASS.values())
}
PAGE_OF_PREFIX = {
    "rst": "api/raster-functions",
    "bng": "api/gridx-functions",
    "quadbin": "api/gridx-functions",
    "custom": "api/gridx-functions",
    "h3": "api/gridx-functions",
    "st": "api/vectorx-functions",
    "pmtiles": "api/pmtiles-functions",
}
CELLID_TYPE = {
    "bng": ["STRING"],
    "quadbin": ["BIGINT"],
    "h3": ["BIGINT"],
    "custom": ["BIGINT"],
}
SESSION_TYPES = {
    "tile": ["TILE"],
    "tiles": ["ARRAY<TILE>"],
    "reference_tile": ["TILE"],
    "dsm_tile": ["TILE"],
    "dem_tile": ["TILE"],
    "geom": ["BINARY", "STRING"],
    "band": ["INT"],
    "srid": ["INT"],
    "out_srid": ["INT"],
    "crs": ["STRING"],
    "target_crs": ["STRING"],
    "out_crs": ["STRING"],
    "k": ["INT"],
    "driver": ["STRING"],
    "path": ["STRING"],
    "content": ["BINARY"],
    "pixel_size": ["DOUBLE"],
    "width": ["INT"],
    "height": ["INT"],
    "tile_width": ["INT"],
    "tile_height": ["INT"],
    "xmin": ["DOUBLE"],
    "ymin": ["DOUBLE"],
    "xmax": ["DOUBLE"],
    "ymax": ["DOUBLE"],
    "mode": ["STRING"],
    "coverage": ["STRING"],
    "assignment": ["STRING"],
    "kring_pad": ["INT"],
    "value": ["DOUBLE"],
    "easting": ["DOUBLE"],
    "northing": ["DOUBLE"],
}
_LIGHT = ("pyrx", "pygx", "pyvx")
# The names a light registrar passes to a factory, by the factory_args kind the manager fills (manager/l3.py).
_FACTORY_ARG_KIND = {
    "_grid_dirs": "proj_grid_dirs",
    "_read_cap": "read_cap",
    "read_cap": "read_cap",
}


def _session_types(name: str, prefix: str) -> Tuple[List[str], bool]:
    if name in ("cellid", "cellid1", "cellid2"):
        return CELLID_TYPE.get(prefix, ["?"]), prefix not in CELLID_TYPE
    if name == "resolution":
        return (["INT", "STRING"] if prefix == "bng" else ["INT"]), False
    t = SESSION_TYPES.get(name)
    return (t, False) if t else (["?"], True)


def _ref_of(
    modname: str, node, aliases: Optional[Dict[str, str]] = None
) -> Optional[str]:
    """`module:attr` for a registered object: a module-level name, or `<alias>.<attr>` where the alias is a
    module the registrar imports (`from . import _crs as _c`, then `_c.st_crs`)."""
    if isinstance(node, ast.Name):
        return f"{modname}:{node.id}"
    if (
        isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id in (aliases or {})
    ):
        return f"{aliases[node.value.id]}:{node.attr}"
    return None


def _module_aliases(tree, modname: str) -> Dict[str, str]:
    """Local name -> module, for every module import in the file (top level or inside a registrar)."""
    package = modname.rsplit(".", 1)[0]
    out = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                if a.asname:
                    out[a.asname] = a.name
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            if node.level:
                parts = modname.split(".")[: -node.level]
                base = ".".join(parts + ([node.module] if node.module else []))
            for a in node.names:
                out[a.asname or a.name] = f"{base}.{a.name}" if base else a.name
    out.pop("*", None)
    return {k: v for k, v in out.items() if v.startswith(package.split(".")[0])}


def _resolves(ref: Optional[str]) -> bool:
    """The ref names a module attribute (a registrar's local closure does not)."""
    if not ref or ":" not in ref:
        return False
    mod, attr = ref.split(":", 1)
    try:
        return hasattr(importlib.import_module(mod), attr)
    except ImportError:
        return False


def _factory_args(call: ast.Call) -> str:
    """The factory_args kind of a factory call: the registrar's grid dirs or read cap; '?' for anything else."""
    names = [a.id for a in call.args if isinstance(a, ast.Name)]
    names += [k.arg for k in call.keywords if k.arg]
    kinds = {_FACTORY_ARG_KIND[n] for n in names if n in _FACTORY_ARG_KIND}
    return kinds.pop() if len(kinds) == 1 else "?"


def light_registrations() -> Dict[str, dict]:
    """SQL name -> its L3 level block, from every light registrar: each module's SQL_REGISTRY table, its
    `.register("gbx_...", x[, returnType])` calls (plain, factory and UDTF forms), the `(name, class)` tuple lists
    a registrar loops over (pyrx's table functions), and the pmtiles package's own registration.
    """
    out: Dict[str, dict] = {}
    modules = [f"databricks.labs.gbx.{p}.functions" for p in _LIGHT]
    modules += [
        f"databricks.labs.gbx.pmtiles.{p.stem}"
        for p in sorted((PKG / "pmtiles").glob("*.py"))
        if p.stem not in ("functions", "__init__")
    ] + ["databricks.labs.gbx.pmtiles"]
    for modname in modules:
        mod = importlib.import_module(modname)
        by_id = {id(v): k for k, v in vars(mod).items() if not k.startswith("__")}
        for name, obj in getattr(mod, "SQL_REGISTRY", {}).items():
            out[name] = {
                "register": "udf",
                "variants": {"c0": f"{modname}:{by_id.get(id(obj), '?')}"},
            }
        tree = ast.parse(Path(mod.__file__).read_text())
        aliases = _module_aliases(tree, modname)
        for node in ast.walk(tree):
            # (name, class) tuples a registrar loops over: udtfs = [("gbx_rst_retile", _RstRetileUDTF), ...]
            if isinstance(node, (ast.List, ast.Tuple)):
                for el in node.elts:
                    if (
                        isinstance(el, ast.Tuple)
                        and len(el.elts) == 2
                        and isinstance(el.elts[0], ast.Constant)
                        and str(el.elts[0].value).startswith("gbx_")
                        and isinstance(el.elts[1], ast.Name)
                        and el.elts[1].id.endswith("UDTF")
                    ):
                        out.setdefault(
                            el.elts[0].value,
                            {"register": "udtf", "udtf": f"{modname}:{el.elts[1].id}"},
                        )
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "register"
                and len(node.args) >= 2
                and isinstance(node.args[0], ast.Constant)
                and str(node.args[0].value).startswith("gbx_")
            ):
                continue
            name, target = node.args[0].value, node.args[1]
            ref = _ref_of(modname, target, aliases)
            udtf = (
                isinstance(node.func.value, ast.Attribute)
                and node.func.value.attr == "udtf"
            )
            if udtf and ref:
                out.setdefault(name, {"register": "udtf", "udtf": ref})
            elif ref and len(node.args) > 2:
                rt = eval(  # noqa: S307 (a returnType expression of our own source)
                    compile(ast.Expression(node.args[2]), "<rt>", "eval"), vars(mod)
                )
                out.setdefault(
                    name,
                    {
                        "register": "callable",
                        "callable": ref,
                        "returns": rt.simpleString(),
                    },
                )
            elif ref:
                out.setdefault(name, {"register": "udf", "variants": {"c0": ref}})
            elif isinstance(target, ast.Call) and isinstance(target.func, ast.Name):
                # a registrar's factory build replaces the table entry of the same name (the last
                # registration wins at run time), so it overrides what SQL_REGISTRY gave
                out[name] = {
                    "register": "udf",
                    "factory": _ref_of(modname, target.func),
                    "factory_args": _factory_args(target),
                }
    for name, spec in out.items():
        if name.startswith("gbx_rst_"):
            spec.setdefault("needs", ["rasterio"])
    return out


def scala_companions() -> Dict[str, str]:
    """SQL name -> companion FQN, from `override def name: String = "gbx_..."` inside an object."""
    out = {}
    for path in SCALA.rglob("*.scala"):
        text = path.read_text()
        pkg = re.search(r"^package ([\w.]+)", text, re.M)
        for obj in re.finditer(r"^object (\w+)\b[^{]*\{(.*?)^\}", text, re.M | re.S):
            m = re.search(
                r'override def name: String = "(gbx_[a-z0-9_]+)"', obj.group(2)
            )
            if m and pkg:
                out.setdefault(m.group(1), f"{pkg.group(1)}.{obj.group(1)}")
    return out


def column_api(pkg: str, pyname: str) -> Optional[dict]:
    """Parameter names, defaults, str-as-literal parameters, force-output keywords and docs of the light def."""
    path = PKG / pkg / "functions.py"
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, ast.FunctionDef) and node.name == pyname:
            args = [a.arg for a in node.args.args]
            defaults = [None] * (len(args) - len(node.args.defaults)) + [
                ast.unparse(d) for d in node.args.defaults
            ]
            body = ast.unparse(node)
            lit = {
                a
                for a in args
                if re.search(rf"isinstance\({a}, str\)", body)
                or f"_crs_col({a})" in body
            }
            force = "virtualize_dir" in args
            v2 = re.search(r"(_\w+_v2_udf)\(", body) or re.search(
                r"(_build_\w+_v2_udf)\(", body
            )
            doc = ast.get_docstring(node) or ""
            summary = doc.split("\n\n", 1)[0].replace("\n", " ").strip()
            arg_docs = dict(re.findall(r"^\s{4,8}(\w+):\s*(.+)$", doc, re.M))
            return {
                "args": [
                    a
                    for a in args
                    if a not in ("virtualize_dir", "virtualize_prefix", "materialize")
                ],
                "defaults": defaults,
                "lit": lit,
                "force": force,
                "v2": (
                    f"databricks.labs.gbx.{pkg}.functions:{v2.group(1)}" if v2 else None
                ),
                "summary": summary,
                "arg_docs": arg_docs,
            }
    return None


def seed(classes: List[str], csv_path: Path) -> Tuple[List[dict], List[str]]:
    sys.path.insert(0, str(REPO / "docs/scripts"))
    spec = importlib.util.spec_from_file_location(
        "efm", REPO / "docs/scripts/extend-function-metadata.py"
    )
    efm = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(efm)
    usage = efm.scan_expressions_directory(str(SCALA))
    info = json.loads(
        (
            REPO / "src/main/resources/com/databricks/labs/gbx/function-info.json"
        ).read_text()
    )["functions"]
    since = dict(
        ln.split("\t")
        for ln in (REPO / "docs/tests-function-info/function-since.tsv")
        .read_text()
        .splitlines()[1:]
        if "\t" in ln
    )
    have = {
        r["sql_name"]
        for r in json.loads((PKG / "manifest/functions.json").read_text())["functions"]
    }
    light, scala = light_registrations(), scala_companions()
    rows, report = [], []
    for c in csv.DictReader(csv_path.open()):
        cls = c["family"][:3]
        name = c["name"]
        if cls not in classes or name in have:
            continue
        prefix = name.split("_")[1]
        pyname = name[len("gbx_") :]
        api = next(
            (column_api(p, pyname) for p in _LIGHT if column_api(p, pyname)), None
        )
        usage_args = (info.get(name) or {}).get("usage_args") or (
            usage.get(name) or {}
        ).get("usage_args", "")
        names = [a.strip().strip("[]") for a in usage_args.split(",") if a.strip()] or (
            api or {}
        ).get("args", [])
        optional = {
            a.strip().strip("[]")
            for a in usage_args.split(",")
            if a.strip().startswith("[")
        }
        params = []
        for i, pname in enumerate(names):
            types, guessed = _session_types(pname, prefix)
            if guessed:
                report.append(f"GUESS {name}: type of {pname} -> {types}")
            dflt = None
            if pname in optional:
                d = (api or {}).get("defaults", [None] * len(names))
                dflt = (
                    d[i]
                    if api and i < len(d) and d[i] not in (None, "None")
                    else "NULL"
                )
            p = {"name": pname, "session": types}
            if dflt is not None:
                p["default"] = dflt if dflt == "NULL" else dflt.replace('"', "'")
            if api and pname in api["lit"]:
                p["py"] = "lit"
            if api and pname in api["arg_docs"]:
                p["doc"] = api["arg_docs"][pname]
            params.append(p)
        levels = {}
        if name in scala:
            levels["L1"] = {"companion": scala[name]}
            if prefix == "rst":
                levels["L1"]["needs_gdal"] = True
        if name in light:
            levels["L3"] = light[name]
            if levels["L3"].get("factory_args") == "?":
                report.append(
                    f"GUESS {name}: factory_args of {levels['L3']['factory']}"
                )
            l3 = levels["L3"]
            for ref in [l3.get("udtf"), l3.get("callable"), l3.get("factory")] + list(
                (l3.get("variants") or {}).values()
            ):
                if ref and not _resolves(ref):
                    report.append(
                        f"GUESS {name}: L3 ref {ref} is not a module attribute (a registrar's local "
                        "function): name a module-level callable or factory"
                    )
        else:
            report.append(f"MISSING {name}: no light registration found")
        if api and len(api["args"]) != len(names):
            report.append(
                f"MISMATCH {name}: Python args {api['args']} vs SQL usage {names}"
            )
        row = {
            "sql_name": name,
            "family": FAMILY_OF_CLASS[cls],
            "kind": c["kind"],
            # a probe name keeps the named-install status; every other seeded row is stable
            "status": "mvp" if name.startswith("gbx_mvp_") else "stable",
            "params": params,
            "min_args": sum(1 for p in params if "default" not in p),
            "returns": {"session": "?"},
            "levels": levels,
            "oracle": ORACLE_OF_FAMILY[FAMILY_OF_CLASS[cls]],
            "example": None,
            "since": since.get(name, "?"),
            "doc": {
                "summary": (api or {}).get("summary") or "?",
                "page": PAGE_OF_PREFIX[prefix],
            },
        }
        if api and api["force"] and api["v2"]:
            row["python"] = {"extras": "force_output", "extra_impl": api["v2"]}
        report.append(
            f"GUESS {name}: returns.session (read it from the L3 UDF's returnType or the UDTF schema)"
        )
        rows.append(row)
    return rows, report


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--class", dest="classes", action="append", required=True)
    ap.add_argument("--classification", type=Path, required=True)
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--report", type=Path)
    a = ap.parse_args(argv)
    rows, report = seed(a.classes, a.classification)
    missing = [r for r in report if r.startswith("MISSING")]
    if (
        missing
    ):  # every registered function has a light registration today: a MISSING line is a seeder gap
        print("\n".join(missing))
        return 1
    if a.write:
        path = PKG / "manifest/functions.json"
        raw = json.loads(path.read_text())
        raw["functions"] += rows
        path.write_text(json.dumps(raw, indent=2, ensure_ascii=False) + "\n")
    text = "\n".join(report)
    print(f"{len(rows)} rows; {len(report)} report lines")
    if a.report:
        a.report.write_text(text + "\n")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
