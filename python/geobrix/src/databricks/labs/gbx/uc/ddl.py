"""CREATE FUNCTION text for the UC surface: env 6, 3-level names, DEFAULTs through a SQL wrapper."""

from __future__ import annotations

import json
from typing import Dict, List

from databricks.labs.gbx.manifest import sql_type


def function_ddl(
    fn, catalog: str, schema: str, wheel_uri: str, deps: Dict[str, str], version: str
) -> List[str]:
    """One Python function, plus a SQL wrapper that carries the DEFAULTs when a parameter has one.

    A UC Python function cannot declare a DEFAULT; the SQL wrapper takes the public name and calls the
    Python function, which is then named ``gbx__<name>``.
    """
    q = f"`{catalog}`.`{schema}`"
    l4 = fn.levels["L4"]
    mod, _, attr = l4["handler"].partition(":")
    has_default = any(p.default for p in fn.params)
    impl = f"gbx__{fn.sql_name[len('gbx_'):]}" if has_default else fn.sql_name
    params = ", ".join(f"{p.name} {sql_type(p.uc)}" for p in fn.params)
    env = json.dumps(
        [f"geobrix @ {wheel_uri}"] + [f"{n}=={deps[n]}" for n in l4["deps"]]
    )
    stmts = [
        f"CREATE OR REPLACE FUNCTION {q}.{impl}({params})\n"
        f"RETURNS {sql_type(fn.returns_uc)}\n"
        f"LANGUAGE PYTHON\nDETERMINISTIC\nCOMMENT 'geobrix {version}'\n"
        f"ENVIRONMENT (dependencies = '{env}', environment_version = '6')\n"
        f"HANDLER 'handler'\nAS $$\nfrom {mod} import {attr} as handler\n$$"
    ]
    if has_default:
        sig = ", ".join(
            f"{p.name} {sql_type(p.uc)}"
            + (f" DEFAULT {p.default}" if p.default else "")
            for p in fn.params
        )
        args = ", ".join(p.name for p in fn.params)
        stmts.append(
            f"CREATE OR REPLACE FUNCTION {q}.{fn.sql_name}({sig})\n"
            f"RETURNS {sql_type(fn.returns_uc)}\n"
            f"LANGUAGE SQL\nCOMMENT 'geobrix {version}'\nRETURN {q}.{impl}({args})"
        )
    return stmts
