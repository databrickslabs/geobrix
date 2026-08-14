# Plan: Light-Tier pip Extras — DBR-Runtime-Matched Variants

**For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development

**Date:** 2026-08-14
**Branch:** beta/0.5.0
**Status:** Plan (implementation)

---

## Goal

Restructure `python/geobrix/pyproject.toml`'s `[project.optional-dependencies]` from a single
`[light]` extra into Option B (shared `_light_base` + per-variant delta). Add the `light_dbr19`
variant for classic DBR 19 clusters. Rename the existing CI lock and add a DBR-19 variant lock.
Update `docs/docs/installation.mdx` to lead with Serverless-default positioning and include
the per-runtime compat matrix. Update the version-bump checklist. Zero change to `[light]`'s
resolved dependency set.

---

## Architecture

Option B: a private `_light_base` extra holds the 17 runtime-agnostic functional deps.
Each named variant (`light`, `light_dbr19`) depends on `_light_base` via self-referential
PEP 508 syntax (`"geobrix[_light_base]"`) plus a small runtime-sensitive delta (~4–5 lines).

The `[light]` variant's combined constraint for `mapbox-vector-tile` resolves to `>=2.1,<2.2`
(identical to today): `_light_base` carries the functional floor `>=2.1`; `[light]` re-adds
`>=2.1,<2.2`, tightening the union to `>=2.1,<2.2` — same as before.

The `[light_dbr19]` variant omits the `<2.2` cap, allowing `mapbox-vector-tile>=2.2`, which
requires `protobuf>=6.31.1` — matching classic DBR 19's `grpcio-status` expectation.

CI lock structure expands from one file (`requirements-pyrx-ci.*`) to four:
- `requirements-light-ci.*` — Serverless (env v5) + classic DBR ≤18 (renamed from `pyrx`)
- `requirements-light-dbr19-ci.*` — classic DBR 19 (new)
- `requirements-light-all-ci.*` — `[light_all]` umbrella, Serverless/DBR ≤18 (new)
- `requirements-light-dbr19-all-ci.*` — `[light_dbr19_all]` umbrella, DBR 19 (new)

All are hash-pinned via `uv pip compile --generate-hashes`.

**`_all` umbrella extras** — `light_all` and `light_dbr19_all` are feature-complete extras that
reference `[light]` / `[light_dbr19]` plus `[stac]` + `[vizx]` + `[overture]`, removing the
reasoning burden of hand-picking partial installs and preventing the `pmtiles`-omission class
of failure. `[databricks]` is excluded — it is not a geo feature; compute already provides its runtime.

---

## Tech Stack

- Python packaging: PEP 508 self-referential extras; setuptools >=61.2 (repo requirement, supports this)
- Lock tooling: `uv pip compile --generate-hashes --python-version 3.12`
- CI: `.github/actions/pyrx_build/action.yml`
- Docs: MDX (Docusaurus); `docs/docs/installation.mdx`

---

## Spec

`docs/superpowers/specs/2026-08-14-light-dbr-runtime-extras-design.md`

---

## Global Constraints

1. **Option B throughout** — shared `_light_base` + per-variant delta; no standalone duplication
   (Option A).
2. **`[light]`'s resolved dependency set must remain identical** — pip resolves `geobrix[light]`
   to exactly the same versions before and after. Verified by `uv pip compile` dry-run diff.
3. **pyspark is never pip-installed** by any `[light*]` extra. `pyspark` stays only in
   `[project.dependencies]` for off-cluster dev; the extras provide cluster-compatible non-pyspark
   deps only.
4. **Terminology discipline in all user-facing copy:**
   - *DBR* / *classic DBR* = classic compute (DBR 17.3 LTS, DBR 18 LTS, DBR 19 LTS, …)
   - *environment / env vN* = Serverless (env v5, env v6, …)
   - **Never** write "Serverless DBR" or "DBR Serverless" — these are distinct product surfaces;
     mixing the terms is wrong and confuses users.
5. **Per-variant hashed CI locks** — each variant gets its own `.in` source + `uv pip compile`-
   generated `.txt` with `--require-hashes`. The `_light_base` extra has no CI lock (it is a
   composition tool, not an installable target).
6. **Functional pins invariant** — the 17 functional deps from spec §3 reside only in `_light_base`
   and are never modified by variant deltas. Runtime-sensitive pins (`typing_extensions`, `rio-tiler`
   cap, `httpcore` floor, `idna` cap, `mapbox-vector-tile` cap) live only in variant deltas.
7. **Docs must lead with Serverless-default positioning** per spec §9 before the compat matrix —
   Serverless is the zero-config default; classic is an explicit per-DBR opt-in.
8. **`_light_base` must not appear in user-facing docs** — internal composition mechanism only.
9. **No Python/Scala source changes** — touch only `pyproject.toml`, lock files, CI action YAML,
   `docs/docs/installation.mdx`, and the version-bump checklist memory file.
10. **Do not push.** Commit after each task; accumulate on `beta/0.5.0`.

---

## Key Grounding Findings

*(Verified from source — not spec assumptions.)*

| Finding | Real value |
|---|---|
| Current light CI lock | `python/geobrix/requirements-pyrx-ci.in` / `requirements-pyrx-ci.txt` |
| Lock recompile command | `cd python/geobrix && uv pip compile --generate-hashes --python-version 3.12 --output-file <out>.txt <in>.in` (from `.in` header + `pyrx_build/action.yml` line 43) |
| CI action that installs the lock | `.github/actions/pyrx_build/action.yml` line 46 |
| Installation doc path | `docs/docs/installation.mdx` (already wired in `sidebars.js` — no sidebar change needed) |
| Extras self-reference support | `setuptools>=61.2` (repo requirement) handles PEP 508 self-referential extras (`"geobrix[_light_base]"`) cleanly |
| `mapbox-vector-tile` in current CI lock | `==2.2.0` — the `.in` file pins 2.2.0 as the pyvx test decode oracle; the CI environment has no Serverless `grpcio-status` constraint, so protobuf 6 is fine in CI |
| `protobuf` in current CI lock | `==6.33.6` — the CI lock already uses protobuf 6.x (consistent with `mapbox-vector-tile==2.2.0`) |
| `_LIGHT_TEST_DIRS` location | `python/geobrix/test/conftest.py` — not modified by this work (no new test dirs added) |
| `test` extra | Contains `mapbox-vector-tile>=2.1,<2.2` — **unchanged** per spec §1c (separate workstream) |

---

## File Structure

| File | Change type | Description |
|---|---|---|
| `python/geobrix/pyproject.toml` | Edit | Add `_light_base`; redefine `light`; add `light_dbr19`, `light_all`, `light_dbr19_all`; leave all other extras unchanged |
| `python/geobrix/requirements-pyrx-ci.in` | Rename → `requirements-light-ci.in` | Update header comment only; all package pins identical |
| `python/geobrix/requirements-pyrx-ci.txt` | Rename → `requirements-light-ci.txt` | Regenerate from renamed `.in`; hash set must be identical |
| `python/geobrix/requirements-light-dbr19-ci.in` | New file | Copy of `requirements-light-ci.in` with updated header + explicit `mapbox-vector-tile>=2.2.0` comment |
| `python/geobrix/requirements-light-dbr19-ci.txt` | New file (generated) | `uv pip compile --generate-hashes` output from `requirements-light-dbr19-ci.in` |
| `python/geobrix/requirements-light-all-ci.in` | New file | Lock source for `[light_all]` umbrella (Serverless/DBR ≤18) |
| `python/geobrix/requirements-light-all-ci.txt` | New file (generated) | `uv pip compile --generate-hashes` output for `[light_all]` |
| `python/geobrix/requirements-light-dbr19-all-ci.in` | New file | Lock source for `[light_dbr19_all]` umbrella (DBR 19) |
| `python/geobrix/requirements-light-dbr19-all-ci.txt` | New file (generated) | `uv pip compile --generate-hashes` output for `[light_dbr19_all]`; transitive-safety check for protobuf conflict |
| `.github/actions/pyrx_build/action.yml` | Edit | `requirements-pyrx-ci.txt` → `requirements-light-ci.txt` (comment + install line) |
| `.github/actions/python_build/action.yml` | Edit (comment only) | Update comment on line 120 from `requirements-pyrx-ci.txt` → `requirements-light-ci.txt` |
| `python/geobrix/requirements-dev-container.in` | Edit (comment only) | Update cross-reference on line 53 from `requirements-pyrx-ci.in` → `requirements-light-ci.in` |
| `python/geobrix/test/conftest.py` | Edit (docstring only) | Update reference on line 7 from `requirements-pyrx-ci.txt` → `requirements-light-ci.txt` |
| `docs/docs/security.mdx` | Edit | Update row link + prose from `requirements-pyrx-ci.txt` → `requirements-light-ci.txt` (user-facing link; would 404 after rename otherwise) |
| `docs/docs/installation.mdx` | Edit | Insert Serverless-default framing paragraph + compat matrix at top of lightweight tab section; include `_all` options in matrix |
| `~/.claude/projects/-Users-mjohns-IdeaProjects-geobrix/memory/geobrix-version-bump-checklist.md` | Edit | Add per-variant lock regeneration steps for all 4 lock files |

---

## Task 1 — pyproject restructure (Option B)

**Files:** `python/geobrix/pyproject.toml`

**Interfaces changed:**
- `[project.optional-dependencies]` gains `_light_base` (new) and `light_dbr19` (new)
- `light` extra is redefined to `["geobrix[_light_base]", <Serverless delta>]`
- All other extras (`test`, `dev`, `databricks`, `overture`, `stac`, `vizx`) left verbatim

**Steps:**

- [ ] Read `python/geobrix/pyproject.toml` to capture the current `[light]` entry verbatim before any edit.
- [ ] Insert `_light_base` directly above `light` in `[project.optional-dependencies]`. It contains exactly these 17 entries extracted from the current `[light]` list (all inline comments preserved):
  - `rasterio>=1.3.0`
  - `shapely>=2.0.0`
  - `numpy>=1.24.0`
  - `pandas>=2.0.0`
  - `pyarrow>=11.0.0`
  - `scipy>=1.11.0`
  - `numexpr>=2.8.0`
  - `rio-cogeo>=7.0,<8`
  - `h3>=4.0,<5`
  - `quadbin>=0.2,<0.3`
  - `pmtiles>=3.4,<4`
  - `pyogrio>=0.12,<1`
  - `pyproj>=3.6,<4`
  - `mapbox-vector-tile>=2.1` — **functional floor only; the `<2.2` cap moves to the `light` delta**
  - `scikit-image>=0.22,<1`
  - `xarray-spatial>=0.4,<1`
  - `netcdf4>=1.6,<2`

  Add a block comment above `_light_base`:
  ```
  # Internal composition extra — NOT for direct user installation.
  # Named with the _ prefix to signal private status. pip has no mechanism to mark
  # an extra as private; contributors must treat it as a build-time composition
  # tool. Install geobrix[light] (Serverless env v5 / classic DBR <=18) or
  # geobrix[light_dbr19] (classic DBR 19+) instead.
  ```

- [ ] Redefine `light` as:
  ```toml
  light = [
      "geobrix[_light_base]",
      # --- Serverless (env v5) and classic DBR <=18 runtime-sensitive pins ---
      # Combined with _light_base's mapbox-vector-tile>=2.1, this resolves to
      # >=2.1,<2.2 -- identical to the previous single [light] extra.
      "typing_extensions>=4.13",
      "rio-tiler>=9.0,<9.3",
      "httpcore>=1.0.9",
      "idna<3.8",
      "mapbox-vector-tile>=2.1,<2.2",
  ]
  ```
  Preserve the existing inline comments for each of those 5 pins (copy from the original `[light]` entry). The comment for `mapbox-vector-tile` should note: "Re-adds the floor from `_light_base` to form the combined constraint `>=2.1,<2.2`."

- [ ] Add `light_dbr19` immediately after `light`:
  ```toml
  # Classic DBR 19 variant: grpcio-status on DBR 19 requires protobuf>=6.31.1,
  # which conflicts with the <2.2 cap on mapbox-vector-tile (2.2.0 requires
  # protobuf>=6.31.1). Dropping that cap is the SOLE structurally necessary
  # change from [light]. The idna cap removal and rio-tiler relaxation are
  # OPTIONAL cleanliness changes; they are gated on on-cluster validation
  # (Task 6 — on-cluster smoke test) confirming they do not regress DBR 19 imports. Spec §7.
  light_dbr19 = [
      "geobrix[_light_base]",
      # --- Classic DBR 19 runtime-sensitive pins ---
      "typing_extensions>=4.13",  # DBR 19 certainly ships 4.13+; floor harmless
      # OPTIONAL: relaxed from <9.3 to <9.4; the 9.3.x typing regression is a
      # Serverless/Python 3.12 issue — unverified on DBR 19. Apply only if Task 6
      # confirms rio-tiler 9.3.x imports cleanly on DBR 19 Python 3.12; revert
      # to <9.3 here if validation fails (conservative fallback, OQ2).
      "rio-tiler>=9.0,<9.4",
      "httpcore>=1.0.9",          # DBR 19 ships >=1.0.9; floor harmless
      # OPTIONAL: idna cap dropped (cleanliness). DBR 19 manages its own idna;
      # the Serverless notebook-change warning does not apply on classic compute.
      # Gated on Task 6 confirming idna>=3.8 does not cause import errors on DBR 19.
      # REQUIRED: Drop <2.2 cap — protobuf>=6.31.1 is expected/required on DBR 19.
      # This is the SOLE structurally necessary delta from [light].
      "mapbox-vector-tile>=2.2",
  ]
  ```

- [ ] Confirm `test`, `dev`, `databricks`, `overture`, `stac`, `vizx` are byte-for-byte identical to their pre-edit state. Run `git diff python/geobrix/pyproject.toml` and inspect: only the `_light_base`, `light`, and `light_dbr19` blocks should appear in the diff.

- [ ] **Verify — `[light]` resolved set unchanged.** From the repo root (Docker container or local env with `uv` available):
  ```bash
  # Capture resolved set BEFORE the edit (use git stash or a pre-edit copy):
  git stash
  uv pip compile --generate-hashes --python-version 3.12 \
      --extra light \
      --output-file /tmp/light-before.txt \
      python/geobrix/pyproject.toml 2>/dev/null
  git stash pop

  # Resolve AFTER the edit:
  uv pip compile --generate-hashes --python-version 3.12 \
      --extra light \
      --output-file /tmp/light-after.txt \
      python/geobrix/pyproject.toml 2>/dev/null

  # Diff must be empty (ignore comment-only lines):
  diff <(grep -v '^#' /tmp/light-before.txt | sort) \
       <(grep -v '^#' /tmp/light-after.txt | sort)
  ```
  If any package version differs, the Option B rewrite is wrong — fix before proceeding.

- [ ] **Verify — `[light_dbr19]` resolves without conflict.** Run:
  ```bash
  uv pip compile --generate-hashes --python-version 3.12 \
      --extra light_dbr19 \
      --output-file /tmp/light-dbr19.txt \
      python/geobrix/pyproject.toml
  grep "mapbox-vector-tile" /tmp/light-dbr19.txt  # must be >=2.2.x (e.g., ==2.2.0)
  grep "^protobuf==" /tmp/light-dbr19.txt          # must be 6.x.y
  ```
  The compile must succeed with exit code 0 and no `ResolutionImpossible` or conflict output.

- [ ] **Note — CI-lock vs runtime:** The per-variant hashed locks (Tasks 2 and 3) are for **CI reproducibility**. CI resolves both variants against PyPI with no Serverless `grpcio-status` constraint, so both `requirements-light-ci.txt` and `requirements-light-dbr19-ci.txt` currently pin `protobuf 6.x` (because `mapbox-vector-tile 2.2.0` requires it and PyPI has no Serverless restriction). This is correct: the lock captures what pip selects in a clean CI environment, not what a Serverless cluster installs. The **real runtime protobuf** is set at cluster install time: `geobrix[light]` constrains `mapbox-vector-tile<2.2` → Serverless installs `protobuf 5.x`; `geobrix[light_dbr19]` allows `>=2.2` → DBR 19 installs `protobuf 6.x`. **The on-cluster gate (Task 6) is the true acceptance test for the runtime constraint; the CI lock is a reproducibility tool only.**

- [ ] Commit: `chore(packaging): Option B extras — _light_base + light_dbr19 variant`

  Body: explain that `[light]`'s resolved set is unchanged (verified by diff), `light_dbr19` drops the `<2.2` mapbox-vector-tile cap to allow protobuf 6 on DBR 19, `_light_base` is an internal composition tool not for direct install, and the idna/rio-tiler relaxations in `light_dbr19` are OPTIONAL pending on-cluster validation in Task 6.

---

## Task 2 — Lock files: rename Serverless lock + add DBR-19 lock + update CI refs

**Files:**
- `python/geobrix/requirements-pyrx-ci.in` → `requirements-light-ci.in`
- `python/geobrix/requirements-pyrx-ci.txt` → `requirements-light-ci.txt`
- `python/geobrix/requirements-light-dbr19-ci.in` (new)
- `python/geobrix/requirements-light-dbr19-ci.txt` (new)
- `.github/actions/pyrx_build/action.yml`

**Steps:**

- [ ] Read `python/geobrix/requirements-pyrx-ci.in` in full before making any changes.

- [ ] Rename via git to preserve history:
  ```bash
  git mv python/geobrix/requirements-pyrx-ci.in python/geobrix/requirements-light-ci.in
  git mv python/geobrix/requirements-pyrx-ci.txt python/geobrix/requirements-light-ci.txt
  ```

- [ ] Edit `requirements-light-ci.in` header comment:
  - Replace every occurrence of `requirements-pyrx-ci` with `requirements-light-ci`.
  - Change the description line from "Source of truth for the lightweight pyrx CI job's Python deps." to "Source of truth for the light-tier CI job's Python deps (Serverless env v5 / classic DBR 17–18)."
  - Update the `--output-file` path in the embedded regeneration command to `requirements-light-ci.txt`.
  - Replace the "Versions track DBR 17.3 LTS / the dev container" note with: "Versions track Serverless env v5 / DBR 17.3–18 LTS / the dev container where applicable."
  - All package pins remain **unchanged**.

- [ ] Regenerate `requirements-light-ci.txt` from the updated `.in` file:
  ```bash
  cd python/geobrix
  uv pip compile --generate-hashes --python-version 3.12 \
      --output-file requirements-light-ci.txt requirements-light-ci.in
  ```

- [ ] **Verify — Serverless lock hash set unchanged.** The regenerated `requirements-light-ci.txt` must be hash-for-hash identical to the pre-rename `requirements-pyrx-ci.txt` (modulo comment lines):
  ```bash
  # Compare package lines only (strip comments and blank lines):
  diff <(grep -v '^#' python/geobrix/requirements-light-ci.txt | grep -v '^$' | sort) \
       <(git show HEAD:python/geobrix/requirements-pyrx-ci.txt | grep -v '^#' | grep -v '^$' | sort)
  ```
  Diff must be empty. If any hash changed, investigate before continuing.

- [ ] Create `python/geobrix/requirements-light-dbr19-ci.in` as a copy of `requirements-light-ci.in` with these changes:
  - Header: "Source of truth for the **DBR-19 classic** light-tier CI job's Python deps."
  - Embedded `--output-file` → `requirements-light-dbr19-ci.txt`
  - Add a note: "Classic DBR 19 variant: mapbox-vector-tile>=2.2 + protobuf 6.x. The CI environment has no Serverless grpcio-status constraint, so both variants currently resolve to similar dep trees; the separate lock documents intent and guards future divergence."
  - The `mapbox-vector-tile` pin section comment: add "DBR 19 variant — no <2.2 cap; >=2.2.0 already selected by the existing test oracle pin."
  - All package versions (including `mapbox-vector-tile==2.2.0` and `protobuf==6.33.6`) are **identical** to `requirements-light-ci.in`. The DBR-19 CI environment also has no Serverless grpcio-status, so the resolved set is the same. Separate file preserves intent and enables future divergence if DBR-19 base transitive deps shift.

- [ ] Generate `requirements-light-dbr19-ci.txt`:
  ```bash
  cd python/geobrix
  uv pip compile --generate-hashes --python-version 3.12 \
      --output-file requirements-light-dbr19-ci.txt requirements-light-dbr19-ci.in
  ```

- [ ] **Verify — DBR-19 lock is complete and has protobuf 6.x:**
  ```bash
  grep -c "sha256:" python/geobrix/requirements-light-dbr19-ci.txt  # must be > 0
  grep "^protobuf==" python/geobrix/requirements-light-dbr19-ci.txt  # must be 6.x.y
  grep "mapbox-vector-tile" python/geobrix/requirements-light-dbr19-ci.txt  # must be 2.2.x
  ```

- [ ] Edit `.github/actions/pyrx_build/action.yml`:
  - Line ~43 comment: `requirements-pyrx-ci.in` → `requirements-light-ci.in`
  - Line ~46 install command: `requirements-pyrx-ci.txt` → `requirements-light-ci.txt`
  - No other changes to this file.

- [ ] Edit `.github/actions/python_build/action.yml`:
  - Line ~120 comment: `requirements-pyrx-ci.txt` → `requirements-light-ci.txt`
  - Comment only; no functional change.

- [ ] Edit `python/geobrix/requirements-dev-container.in`:
  - Line ~53 comment: `requirements-pyrx-ci.in` → `requirements-light-ci.in`
  - Comment only; all package pins and the rest of the file unchanged.

- [ ] Edit `python/geobrix/test/conftest.py`:
  - Line ~7 docstring: `requirements-pyrx-ci.txt` → `requirements-light-ci.txt`
  - Docstring only; no code change.

- [ ] Edit `docs/docs/security.mdx`:
  - Line ~60: update the table row's hyperlink target from `requirements-pyrx-ci.txt` to `requirements-light-ci.txt` (the hyperlink points to the file on GitHub; after the rename, the old URL 404s)
  - Line ~60: update the row label from "CI (lightweight pyrx build)" to "CI (lightweight build)"
  - Line ~69: update the prose reference from `requirements-pyrx-ci.txt` to `requirements-light-ci.txt`
  - Do not modify any other content in `security.mdx`.

- [ ] **Verify — no remaining stale references to the old lock name in committed files:**
  ```bash
  grep -rn "requirements-pyrx-ci" \
      /Users/mjohns/IdeaProjects/geobrix/.github \
      /Users/mjohns/IdeaProjects/geobrix/scripts \
      /Users/mjohns/IdeaProjects/geobrix/docs/docs \
      /Users/mjohns/IdeaProjects/geobrix/python/geobrix/test \
      /Users/mjohns/IdeaProjects/geobrix/python/geobrix/requirements-dev-container.in \
      2>/dev/null
  ```
  Must return nothing. (Old plan docs under `docs/superpowers/plans/` and `prompts/` reference the old name as historical prose — those are acceptable and not updated here.)

- [ ] Commit: `chore(ci): rename pyrx-ci lock → light-ci; add light-dbr19-ci lock`

  Body: note the rename preserves git history via `git mv`; the hash set of `requirements-light-ci.txt` is identical to the pre-rename `requirements-pyrx-ci.txt`; `requirements-light-dbr19-ci.{in,txt}` is the new DBR-19 variant lock.

---

## Task 3 — Umbrella `_all` extras + locks

**Files:**
- `python/geobrix/pyproject.toml`
- `python/geobrix/requirements-light-all-ci.in` (new)
- `python/geobrix/requirements-light-all-ci.txt` (new, generated)
- `python/geobrix/requirements-light-dbr19-all-ci.in` (new)
- `python/geobrix/requirements-light-dbr19-all-ci.txt` (new, generated)

**Interfaces changed:**
- `[project.optional-dependencies]` gains `light_all` and `light_dbr19_all` (both new)
- No existing extras modified; no Python source changes

**Steps:**

- [ ] Read `python/geobrix/pyproject.toml` to confirm the `stac`, `vizx`, and `overture` extras
  are still as-read in Task 1 (no changes since that task). Note their exact entry names.

- [ ] Add `light_all` immediately after `light_dbr19` in `[project.optional-dependencies]`:
  ```toml
  # Feature-complete umbrella for Serverless (env v5) and classic DBR <=18.
  # Installs [light] + all optional feature extras ([stac], [vizx], [overture]).
  # [databricks] is NOT included — it is a Databricks SDK integration, not a geo
  # feature, and Databricks compute already provides its runtime.
  # Use this for the "don't reason about deps" full install on Serverless/DBR <=18.
  light_all = [
      "geobrix[light]",
      "geobrix[stac]",
      "geobrix[vizx]",
      "geobrix[overture]",
  ]
  ```

- [ ] Add `light_dbr19_all` immediately after `light_all`:
  ```toml
  # Feature-complete umbrella for classic DBR 19. Same as [light_all] but uses
  # [light_dbr19] so the protobuf constraint is DBR-19-correct.
  # [databricks] excluded for the same reason as [light_all].
  light_dbr19_all = [
      "geobrix[light_dbr19]",
      "geobrix[stac]",
      "geobrix[vizx]",
      "geobrix[overture]",
  ]
  ```

- [ ] Confirm `test`, `dev`, `databricks`, `overture`, `stac`, `vizx`, `light`, `light_dbr19`,
  and `_light_base` are byte-for-byte identical to their pre-edit state.

- [ ] **Verify — `light_all` resolves cleanly:**
  ```bash
  uv pip compile --generate-hashes --python-version 3.12 \
      --extra light_all \
      --output-file /tmp/light-all.txt \
      python/geobrix/pyproject.toml
  echo "Exit: $?"  # must be 0
  grep "mapbox-vector-tile" /tmp/light-all.txt  # must be <2.2.x (e.g., ==2.1.x)
  grep "^protobuf==" /tmp/light-all.txt          # must be 5.x (Serverless-safe)
  ```

- [ ] **Verify — `light_dbr19_all` resolves without protobuf conflict (transitive-safety check):**
  ```bash
  uv pip compile --generate-hashes --python-version 3.12 \
      --extra light_dbr19_all \
      --output-file /tmp/light-dbr19-all.txt \
      python/geobrix/pyproject.toml
  echo "Exit: $?"  # must be 0; a non-zero exit indicates a stac/vizx/overture
                   # transitive dep conflicts with light_dbr19's protobuf>=6
  grep "mapbox-vector-tile" /tmp/light-dbr19-all.txt  # must be >=2.2.x
  grep "^protobuf==" /tmp/light-dbr19-all.txt          # must be 6.x
  ```
  If this compile fails with `ResolutionImpossible`, one of `stac` / `vizx` / `overture`
  carries a transitive protobuf-adjacent constraint. In that case: identify the conflicting
  dep, add a `_dbr19` variant for that feature extra on the same base-plus-delta pattern,
  and update `light_dbr19_all` to reference it. Do NOT merge the plan step; fix the issue
  first and document the finding.

- [ ] Create `python/geobrix/requirements-light-all-ci.in` with content modelled on
  `requirements-light-ci.in` but targeting `[light_all]`. Header comment:
  ```
  # Source of truth for the light_all umbrella CI job's Python deps
  # (Serverless env v5 / classic DBR 17–18, full feature set).
  # Consumed by CI via: pip install --require-hashes -r python/geobrix/requirements-light-all-ci.txt
  #
  # Regenerate:
  #   cd python/geobrix
  #   uv pip compile --generate-hashes --python-version 3.12 \
  #       --output-file requirements-light-all-ci.txt requirements-light-all-ci.in
  #
  # Note: CI resolves without Serverless grpcio-status constraints; both _all variants
  # will resolve protobuf 6.x in CI (stac/vizx/overture have no protobuf conflict).
  # The real runtime protobuf is set at cluster install time. See Task 1 CI-lock note.
  ```
  Dependency section: a single `geobrix[light_all]` entry pointing at the local pyproject
  (or list the full union of `[light]`+`[stac]`+`[vizx]`+`[overture]` pins explicitly —
  use the same style as `requirements-light-ci.in` for the individual packages; do NOT
  reference `geobrix` as a package name if the CI runner installs from the local wheel).
  Follow the exact style of the existing `.in` file (direct package pins matching the
  resolved set from the verify step above).

- [ ] Generate `requirements-light-all-ci.txt`:
  ```bash
  cd python/geobrix
  uv pip compile --generate-hashes --python-version 3.12 \
      --output-file requirements-light-all-ci.txt requirements-light-all-ci.in
  ```
  Verify: `grep -c "sha256:" requirements-light-all-ci.txt` must be > 0.

- [ ] Create `python/geobrix/requirements-light-dbr19-all-ci.in` with the same structure,
  targeting `[light_dbr19_all]`. Header comment mirrors `requirements-light-all-ci.in` but
  says "DBR 19, full feature set."

- [ ] Generate `requirements-light-dbr19-all-ci.txt`:
  ```bash
  cd python/geobrix
  uv pip compile --generate-hashes --python-version 3.12 \
      --output-file requirements-light-dbr19-all-ci.txt requirements-light-dbr19-all-ci.in
  ```
  Verify:
  ```bash
  grep -c "sha256:" python/geobrix/requirements-light-dbr19-all-ci.txt  # must be > 0
  grep "^protobuf==" python/geobrix/requirements-light-dbr19-all-ci.txt  # must be 6.x
  grep "mapbox-vector-tile" python/geobrix/requirements-light-dbr19-all-ci.txt  # must be >=2.2.x
  ```

- [ ] Commit: `chore(packaging): add light_all + light_dbr19_all umbrella extras + locks`

  Body: umbrella extras reference `[light]`/`[light_dbr19]` + `[stac]` + `[vizx]` + `[overture]`
  to give users a single no-reasoning-required install per runtime. `[databricks]` excluded
  by design (not a geo feature). Both `_all` locks resolve with no protobuf conflict —
  verified by `uv pip compile` exit code and grep for protobuf/mapbox-vector-tile pins.

---

## Task 4 — Installation docs update

**Files:** `docs/docs/installation.mdx`

**Steps:**

- [ ] Read `docs/docs/installation.mdx` in full. Locate the `<TabItem value="lightweight" label="Lightweight (pure-Python)">` opening tag. Note the exact first sentence following it: currently "The lightweight tier is a single Python wheel installed with the `light` extra — **no init script, no JAR, no native GDAL bundle**…"

- [ ] Insert the following block **immediately after** the `<TabItem value="lightweight" …>` opening tag, before the existing "The lightweight tier…" paragraph:

  ```mdx
  :::info GeoBrix Light and Databricks runtimes
  **GeoBrix Light defaults to Databricks Serverless environments (currently environment v5).**
  On Serverless, `%pip install geobrix[light]` installs cleanly — no further configuration needed.

  **On a classic DBR cluster, additional dependencies must be accounted for** — each classic DBR
  generation ships a different immutable base (`protobuf`/`grpcio-status`, `idna`,
  `typing_extensions`, …), so the default `[light]` extra can conflict there. On DBR 19 in
  particular, installing `geobrix[light]` triggers a protobuf conflict that causes hangs and
  kernel crashes. On classic compute, use the extra that matches your runtime from the table below.

  | Runtime | Install command | Full feature set |
  |---|---|---|
  | Serverless (env v5+) | `%pip install "geobrix[light] @ file:///Volumes/…"` | `geobrix[light_all]` |
  | Classic DBR 17.3–18.x | `%pip install "geobrix[light] @ file:///Volumes/…"` | `geobrix[light_all]` |
  | Classic DBR 19+ | `%pip install "geobrix[light_dbr19] @ file:///Volumes/…"` | `geobrix[light_dbr19_all]` |

  *(Full feature set = `[light/_dbr19]` + `[stac]` + `[vizx]` + `[overture]` in one install. `[databricks]` is added separately if needed.)*

  The `file:///Volumes/…` path is a placeholder for the wheel on your Unity Catalog Volume — see
  the install steps below for the exact pattern including the PEP 508 `package @ file://` quoting
  required on Serverless.
  :::
  ```

  Adapt to match the page's existing callout style (check whether it uses `:::info`, `:::note`, or a bare paragraph — use the same style). The `file:///Volumes/…` placeholder matches the pattern already in use on the page (the existing install snippet uses `file:///Volumes/<catalog>/...`).

- [ ] Do NOT expose `_light_base` anywhere in the page.
- [ ] Do NOT modify the existing `:::warning Serverless install requirements` callout — it must remain verbatim.
- [ ] Do NOT modify the heavyweight tier tabs or any other sections.

- [ ] **Verify — no wave numbers:**
  ```bash
  grep -rniE "wave [0-9]+|wave-[0-9]+" docs/docs/ 2>/dev/null
  ```
  Must return nothing.

- [ ] **Verify — terminology discipline:**
  ```bash
  grep -n "Serverless DBR\|DBR Serverless\|_light_base" docs/docs/installation.mdx
  ```
  Must return nothing.

- [ ] **Verify — compat matrix present and correct:**
  ```bash
  grep -n "light_dbr19\|DBR 19\|light_all\|light_dbr19_all" docs/docs/installation.mdx
  ```
  Must return at least the rows added in this task (both runtime variant rows and `_all` column entries).

- [ ] Commit: `docs(installation): lead with Serverless-default framing + DBR-19 compat matrix + _all column`

  Body: "No sidebar change needed — installation.mdx is already wired in sidebars.js. Adds the `_light_base`-invisible framing paragraph and per-runtime compat table including the `_all` umbrella column per spec §9 + §4d."

---

## Task 5 — Version-bump checklist + CI wiring notes

**Files:** `~/.claude/projects/-Users-mjohns-IdeaProjects-geobrix/memory/geobrix-version-bump-checklist.md`

**Steps:**

- [ ] Read the current version-bump checklist memory file.

- [ ] Locate the section that lists regeneration steps for wheels, JARs, and lock files. Add or update a "Light-tier CI lock regeneration" sub-section with these entries:

  ```
  - Regenerate Serverless lock:
      cd python/geobrix
      uv pip compile --generate-hashes --python-version 3.12 \
          --output-file requirements-light-ci.txt requirements-light-ci.in
  - Regenerate DBR-19 lock:
      cd python/geobrix
      uv pip compile --generate-hashes --python-version 3.12 \
          --output-file requirements-light-dbr19-ci.txt requirements-light-dbr19-ci.in
  - Regenerate light_all umbrella lock:
      cd python/geobrix
      uv pip compile --generate-hashes --python-version 3.12 \
          --output-file requirements-light-all-ci.txt requirements-light-all-ci.in
  - Regenerate light_dbr19_all umbrella lock:
      cd python/geobrix
      uv pip compile --generate-hashes --python-version 3.12 \
          --output-file requirements-light-dbr19-all-ci.txt requirements-light-dbr19-all-ci.in
  - For each future light_dbrN variant: add its .in/.txt pair AND its _all equivalent
    to this checklist; add both locks to CI (or a new CI action for lower-cadence DBR-N jobs).
  ```

- [ ] Also note the one-time rename: "`requirements-pyrx-ci.*` was renamed to `requirements-light-ci.*` in 0.5.0; update any external references."

- [ ] Note on secondary CI jobs: the spec §9 mentions that `requirements-light-dbr19-ci.txt` "can run on a lower cadence if DBR 19 clusters are expensive to provision." Wiring a dedicated DBR-19 CI job (e.g., a new `.github/actions/light_dbr19_build/action.yml`) and a `light_all`/`light_dbr19_all` CI job are follow-on tasks once DBR-19 cluster access is confirmed. All 4 lock files are ready; the job wiring is deferred.

- [ ] Commit: `chore(memory): version-bump checklist — per-variant light-tier lock regeneration`

---

## Task 6 — Manual on-cluster validation gate

**No files changed. This task is a manual gate executed by the lead agent or user.**

Requires: the updated wheel staged to the `oauth-fe` Volume via `gbx:data:push-wheel --profile oauth-fe`.

**Steps:**

- [ ] **MANUAL — Serverless regression check.** On a Serverless notebook (env v5+):
  ```python
  %pip install "geobrix[light] @ file:///Volumes/<catalog>/<schema>/<volume>/geobrix/geobrix-<version>-py3-none-any.whl"
  ```
  Acceptance criteria: pip output contains no `ERROR:` lines and no `WARNING: pip's dependency
  resolver does not currently take into account all the packages...` lines. Then:
  ```python
  import databricks.labs.gbx.rasterx.functions  # must succeed
  import google.protobuf; print(google.protobuf.__version__)  # must be 5.x
  ```

- [ ] **MANUAL — DBR-19 smoke test.** On a classic DBR 19 cluster:
  ```python
  %pip install "geobrix[light_dbr19] @ file:///Volumes/<catalog>/<schema>/<volume>/geobrix/geobrix-<version>-py3-none-any.whl"
  ```
  Acceptance criteria: pip output contains no `ERROR:` lines and no unsatisfied-constraint
  `WARNING:` lines. Then:
  ```python
  import google.protobuf; print(google.protobuf.__version__)  # must be >=6.31.1
  import databricks.labs.gbx.rasterx.functions                # must succeed
  import databricks.labs.gbx.pyvx.functions as pyvx
  pyvx.register(spark)
  result = spark.sql("SELECT gbx_st_asmvt(collect_list(named_struct('geom', ST_Point(0.5, 0.5))), 'test', 256)")
  result.show()  # must return a non-null BINARY row
  import idna; print(idna.__version__)  # must import cleanly (validates OPTIONAL idna cap drop)
  import rio_tiler; print(rio_tiler.__version__)  # must import cleanly (validates OPTIONAL <9.4 cap)
  ```

- [ ] **Gate on OPTIONAL deltas.** Based on DBR-19 smoke test results:
  - If `import idna` and `import rio_tiler` both succeed without errors → the OPTIONAL deltas
    (idna cap dropped, rio-tiler relaxed to `<9.4`) are confirmed safe. No pyproject change needed.
  - If either import fails → revert the failing OPTIONAL delta in `pyproject.toml` (restore
    `idna<3.8` or restore `rio-tiler<9.3` for `light_dbr19`), regenerate `requirements-light-dbr19-ci.txt`
    and `requirements-light-dbr19-all-ci.txt`, and commit the revert before shipping.
  - The `mapbox-vector-tile>=2.2` REQUIRED delta is not gated — it stays regardless.

- [ ] **Note on `_all` variants.** The `light_all` / `light_dbr19_all` umbrellas are validated
  indirectly: if `[light]` passes on Serverless and `[light_dbr19]` passes on DBR 19, and the
  Task 3 `uv pip compile` exits 0 for both `_all` variants, the umbrellas are safe. A dedicated
  `_all` on-cluster smoke (installing `[light_dbr19_all]` and verifying stac/vizx/overture imports)
  is optional but recommended for the first 0.5.x release that ships to DBR-19 users.

- [ ] Record pass/fail (and pip output if fail) in `prompts/testing/2026-08-14-light-dbr-runtime-extras-cluster-smoke.md`.
  Note which OPTIONAL deltas were confirmed or reverted.

---

## Self-Review

### Spec coverage

| Spec section | Plan coverage | Status |
|---|---|---|
| §3 pin taxonomy (21 deps classified) | Task 1: 17 in `_light_base`, 4+1 in `light` delta | ✓ |
| §4a additive-extras mechanic | Global Constraints §1 + Task 1 comment | ✓ |
| §4c Option B TOML sketch | Task 1 steps implement the exact TOML structure from spec | ✓ |
| §4c `[light]` resolved set unchanged | Task 1 verify: `uv pip compile` diff before/after | ✓ |
| §4d `_all` umbrella extras | Task 3: `light_all` + `light_dbr19_all` + 4 lock files + docs matrix `_all` column | ✓ |
| §4d `[databricks]` excluded from `_all` | Task 3 pyproject snippet omits it; comment explains rationale | ✓ |
| §4d transitive-safety check | Task 3 verify: `uv pip compile --extra light_dbr19_all` + grep for protobuf/mvt pins | ✓ |
| §5 pyspark not pip-installed | Global Constraint §3; Task 1 leaves `[project.dependencies]` unchanged | ✓ |
| §6 per-variant hashed locks | Tasks 2 + 3: four locks; `uv pip compile --generate-hashes` command exact | ✓ |
| §6 `_light_base` not locked | Task 2 does not create a `_light_base` lock | ✓ |
| §7 `light_dbr19` concrete delta | Task 1: mvt cap drop required; idna + rio-tiler marked OPTIONAL, gated on Task 6 | ✓ |
| §7 validation checklist | Task 6: 5-step on-cluster smoke maps to spec §7's list | ✓ |
| §9 docs lead framing | Task 4: framing paragraph + compat matrix + `_all` column per spec §9 + §4d | ✓ |
| §9 `_light_base` not in user docs | Task 4 step explicitly prohibits it; verify grep | ✓ |
| §9 version-bump checklist | Task 5: all 4 lock files covered | ✓ |
| §9 CI wiring | Task 2: `pyrx_build/action.yml` updated; secondary DBR-19 CI job deferred per spec | ✓ |
| §10 rollout order | Tasks 1→2→3→4→5→6 match spec §10 phases (a)(b)(c) | ✓ |

### Ground-check verdicts

| Claim | Verdict |
|---|---|
| Lock rename blast radius | DEFECT FIXED — 4 files missed by original plan; all added to Task 2: `python_build/action.yml`, `requirements-dev-container.in`, `test/conftest.py`, `docs/docs/security.mdx` (user-facing link would 404). Task 2 verify grep widened accordingly. |
| Recompile command `cd python/geobrix && uv pip compile --generate-hashes --python-version 3.12 ...` | CONFIRMED — `.in` header + `action.yml` line 43 both match exactly. |
| Extras self-reference: `setuptools>=61.2` in build-system | CONFIRMED — `python/geobrix/pyproject.toml` line 193: `requires = ["setuptools>=61.2", "wheel"]`. |
| `uv pip compile --extra light` diff proves resolved set unchanged | CONFIRMED — `uv pip compile` expands self-referential extras from the same pyproject; the before/after diff is a valid proof. |
| `docs/docs/installation.mdx` path + already in `sidebars.js` | CONFIRMED — file exists (19.5K); `sidebars.js` line 19 contains `'installation'`. No sidebar change needed. |

### Placeholder scan

No `TODO`, `TBD`, `FIXME`, or `???` in this plan. All open questions from the spec (OQ1–OQ5) are referenced by number and handled as:
- OQ1: Option B chosen and justified; `_` convention documented in the pyproject comment
- OQ2: `rio-tiler<9.4` marked OPTIONAL in Task 1; gated on Task 6 cluster validation
- OQ3: Future `light_dbr20` deferred; pattern is established
- OQ4: `test` extra unchanged; separate workstream noted
- OQ5: `pyproj<4` remains in `_light_base` (no-op today; monitored)

### Consistency check

- All 17 functional deps listed in Task 1 match the spec §3 "Functional" classification exactly.
- The 5 runtime-sensitive deps (`typing_extensions`, `rio-tiler` cap, `httpcore` floor, `idna` cap, `mapbox-vector-tile` cap) match spec §3 "Runtime-base-compat" exactly.
- Lock file names (`requirements-light-ci.*`, `requirements-light-dbr19-ci.*`, `requirements-light-all-ci.*`, `requirements-light-dbr19-all-ci.*`) match spec §6 + §4d and the grounding rename from `requirements-pyrx-ci.*`.
- Recompile command in Tasks 2, 3, and 5 checklist exactly matches the command in `requirements-pyrx-ci.in` header (confirmed by direct read).
- CI action file line numbers are grounding-verified (`.github/actions/pyrx_build/action.yml` line 43/46).
- Installation doc path `docs/docs/installation.mdx` confirmed by `ls`; already in `sidebars.js` — no sidebar edit needed.
- `_LIGHT_TEST_DIRS` in `test/conftest.py` is not touched as a code change (docstring-only update in Task 2; no new test dirs added).
- `test` extra unchanged (spec §1c: separate workstream, explicitly out of scope).
- No Python/Scala source changes in any task. Global Constraint §9 satisfied.
- Grounding surprise (current CI lock already has `mapbox-vector-tile==2.2.0` + `protobuf==6.33.6`) explained in Task 1 CI-lock-vs-runtime note and Task 2 — does not invalidate any task step; confirms the CI-lock is a reproducibility artifact, not a runtime constraint proof.
- `_all` extras exclude `[databricks]` per spec §4d ruling (confirmed).
