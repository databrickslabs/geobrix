"""Statement hygiene of the governed procedures: a target made from a column list, ids matched null-safe, one repair of
each geometry, one read of the Zarr manifest, the incremental-install skip message and the private routines of a
full install.
"""

import re

from databricks.labs.gbx import uc
from databricks.labs.gbx.uc import procedures


def _text(name):
    return procedures.RECIPES[name]("cat", "sch")


def _statements(text):
    body = text.split("\nAS BEGIN\n", 1)[1].rsplit("\nEND", 1)[0]
    return [s.strip() for s in re.split(r";\n", body + "\n") if s.strip()]


# --- statement hygiene -----------------------------------------------------------------------------------------------


def _built():
    built, _skipped = procedures.build_all("cat", "sch")
    return dict(built)


def test_no_procedure_creates_a_table_by_running_a_function_over_the_source():
    """A CREATE TABLE ... AS SELECT with ``WHERE false`` still plans the select; a column list over the source does not."""
    built = _built()
    assert {"gbx_mosaic_by_key", "gbx_combine_by_key"} <= set(built)
    for name, text in built.items():
        for stmt in re.findall(
            r"CREATE TABLE (?:IF NOT EXISTS )?IDENTIFIER\([^;]*WHERE false;", text
        ):
            assert "gbx_" not in stmt.replace("IDENTIFIER", ""), (name, stmt)


def test_clean_geometries_matches_null_ids_and_repairs_each_geometry_once():
    text = _text("gbx_clean_geometries")
    deletes = [s for s in _statements(text) if s.startswith("DELETE FROM")]
    assert len(deletes) == 2
    for d in deletes:
        assert "EXISTS (SELECT 1 FROM" in d and "<=> t.id" in d, d
        assert " IN (SELECT" not in d, d  # an IN never matches a NULL id
    # the repair runs once, into a scratch table the four statements read
    assert text.count("gbx_st_makevalid(") == 1
    assert text.count("gbx_st_explainvalidity(") == 1
    # a NULL geometry is not repaired, and the reason is asked for the rejects only (a filter below the call)
    stage = next(
        s
        for s in _statements(text)
        if s.startswith("CREATE TABLE IDENTIFIER(v_scratch)")
    )
    assert "WHERE geom IS NOT NULL" in stage.split("UNION ALL")[0]
    assert "gbx_st_explainvalidity" not in stage
    reason = text[text.index("gbx_st_explainvalidity(") :]
    assert "FROM IDENTIFIER(v_scratch) WHERE NOT ok AND geom IS NOT NULL" in reason
    assert "DECLARE EXIT HANDLER FOR SQLEXCEPTION" in text
    assert text.rstrip().endswith("DROP TABLE IF EXISTS IDENTIFIER(v_scratch);\nEND")


def test_clean_geometries_says_its_statements_are_not_one_transaction():
    text = _text("gbx_clean_geometries")
    comment = re.search(r"\nCOMMENT '(.*)'\nAS BEGIN", text).group(1)
    assert "not one transaction" in comment and "run it again" in comment


def test_zarr_tag_version_reads_the_manifest_in_one_statement():
    text = _text("gbx_zarr_tag_version")
    assert "SET v_from" not in text and "DECLARE v_from" not in text
    assert "IF EXISTS (SELECT 1 FROM IDENTIFIER(manifest_table)" not in text
    merges = [s for s in _statements(text) if "IDENTIFIER(manifest_table)" in s]
    assert len(merges) == 1 and merges[0].startswith("MERGE INTO")
    # the version a tag copies is read in the same statement as the rows it copies: one snapshot
    assert "max(_metadata.row_commit_version)" in merges[0]
    assert (
        "already exists. Fix:" in merges[0]
    )  # the refusal of a tag that exists stays in the MERGE


def test_zarr_tag_version_refuses_an_existing_tag_whatever_its_keys_are():
    """A MERGE refuses only on matched keys; a tag whose rows no longer overlap the live keys matches nothing and would
    mix two versions under one name. The always-inserted commit row therefore carries the refusal in its meta.
    """
    text = _text("gbx_zarr_tag_version")
    merge = next(s for s in _statements(text) if s.startswith("MERGE INTO"))
    commit_row = merge[merge.index("SELECT 'commit' AS kind") :]
    guard = re.search(
        r"CASE WHEN EXISTS \(SELECT 1 FROM IDENTIFIER\(manifest_table\) WHERE tag = tag_name\) "
        r"THEN CAST\(raise_error\(concat\('tag ', tag_name, ' already exists\. Fix:[^;]*?\) AS STRING\) "
        r"ELSE to_json\(.*?\) END AS meta",
        commit_row,
        re.S,
    )
    assert guard, commit_row
    # the key-matched refusal stays as well, and the comment says the refusal does not depend on the keys
    assert "WHEN MATCHED AND s.kind <> 'commit' THEN UPDATE SET" in merge
    assert "whatever its keys are" in text


def test_the_incremental_install_skip_message_states_the_remedy(capsys):
    uc.install(
        "cat",
        "sch",
        "/Volumes/c/s/v",
        dry_run=True,
        procedures=True,
        only=["gbx_bng_tessellateexplode"],
    )
    out = capsys.readouterr().out
    line = next(ln for ln in out.splitlines() if "gbx_features_to_mvt skipped" in ln)
    assert "Fix: add" in line and "only=" in line and "install without only" in line


def test_the_full_install_creates_both_private_routines_before_their_procedures():
    stmts = uc.install("cat", "sch", "/Volumes/c/s/v", dry_run=True, procedures=True)
    heads = [s.split("(", 1)[0] for s in stmts]
    for private in ("gbx__h3_cellfill_cells", "gbx__st_asmvt_features"):
        made = [i for i, h in enumerate(heads) if h.endswith(f"`sch`.{private}")]
        assert len(made) == 1, private
    first_proc = min(i for i, h in enumerate(heads) if "PROCEDURE" in h)
    assert all(
        i < first_proc
        for p in ("gbx__h3_cellfill_cells", "gbx__st_asmvt_features")
        for i, h in enumerate(heads)
        if h.endswith(p)
    )
    # the new procedure is installed, and installed with the functions it calls
    assert any(h.endswith("gbx_combine_by_key") for h in heads)


# --- the scratch table is named in the target's catalog and schema, from a sanitized base ----------------------------


def _scratch_statements(name):
    text = _text(name)
    return text, next(s for s in _statements(text) if s.startswith("SET v_scratch"))


def test_every_staging_procedure_builds_a_quoted_three_level_scratch_name_and_drops_that_name():
    for name in ("gbx_clean_geometries", "gbx_mosaic_by_key", "gbx_combine_by_key"):
        text, name_stmt = _scratch_statements(name)
        # not the target's text plus a suffix (that breaks on a backtick-quoted target)
        assert "concat(target_table" not in text, name
        assert (
            "replace(target_table, '`', '')" in name_stmt
        ), name  # the backticks stripped
        assert "current_catalog()" in name_stmt and "current_schema()" in name_stmt
        # the table part is reduced to word characters, then every part is quoted
        assert (
            "regexp_replace(try_element_at(p, -1), '[^A-Za-z0-9_]', '_')" in name_stmt
        )
        assert name_stmt.count("concat('`'") == 3, name
        # one name, set before any statement uses it, and the exit handler drops that same variable
        assert text.count("SET v_scratch") == 1, name
        handler = text[text.index("DECLARE EXIT HANDLER") : text.index("SET v_scratch")]
        assert "DROP TABLE IF EXISTS IDENTIFIER(v_scratch)" in handler, name
        assert text.index("SET v_scratch") < text.index(
            "IDENTIFIER(v_scratch)", text.index("SET v_scratch") + 20
        )


def test_the_scratch_name_stays_in_the_target_schema_for_backticked_and_odd_targets(
    spark,
):
    """Evaluate the name expression on the local Spark: a backtick-quoted target, a plain one, a short one, a dashed table."""
    _, name_stmt = _scratch_statements("gbx_clean_geometries")

    inner = name_stmt.rstrip(";")
    query = inner[
        len("SET v_scratch = (") : -1
    ]  # the scalar subquery without its parentheses

    def scratch(target):
        return spark.sql(
            query.replace("target_table", "'" + target.replace("'", "''") + "'")
        ).first()[0]

    for target, head in (
        ("`my-cat`.`sch`.`tab-le`", "`my-cat`.`sch`.`tab_le_scratch_"),
        ("cat.sch.tab", "`cat`.`sch`.`tab_scratch_"),
    ):
        got = scratch(target)
        assert got.startswith(head) and got.endswith("`"), (target, got)
        assert "``" not in got.replace("`.`", ".")
    short = scratch("tab")
    assert (
        short.endswith(".`tab_scratch_" + short.split("_scratch_")[1])
        and short.count("`") == 6
    )
