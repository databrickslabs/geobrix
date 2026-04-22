# scripts/security/

Tooling that implements the Databricks Labs [Repository Lockdown policy](https://docs.google.com/document/d/1J50oKQxG9WhGXWEl5zlbCq5pf9AGh57yDhZh9nxCQC0/edit)
for GeoBrix: every third-party GitHub Action reference must be pinned to a
full commit SHA taken from a release published **before the
`2026-03-10T00:00:00Z` cutoff**. The tag name is preserved as an inline
comment for human-readable cross-reference; the comment is **not**
authoritative — reviewers must verify the SHA against the referenced
release.

## Scripts

| Script | Requires | Purpose |
|---|---|---|
| `list-external-actions` | `yq` (Mike Farah) | Emit the set of external actions referenced by any workflow or composite action under `.github/`, one per line. |
| `resolve-action-ref` | `gh`, `jq` | For each `action[@ref]`, resolve the most recent pre-cutoff release tag to the commit SHA it points at. Marks already-pinned entries with `✓` and drift with `⚠`. |
| `pin-gh-actions` | `git` | Consume `resolve-action-ref` output, rewrite every `uses:` line under `.github/` to the new SHA form (skipping `databricks*`-owned actions), and stage the result with `git add`. Prints the staged diff for review — **does not commit**. |

## Typical flow

```sh
cd "$(git rev-parse --show-toplevel)"

# 1. Preview what would change
./scripts/security/list-external-actions \
  | xargs ./scripts/security/resolve-action-ref

# 2. Apply (stages under .github/)
./scripts/security/list-external-actions \
  | xargs ./scripts/security/resolve-action-ref \
  | ./scripts/security/pin-gh-actions

# 3. Review, then commit
git diff --cached -- .github
git commit -m "Re-pin GitHub Actions to commits from releases prior to 2026-03-10"
```

## Notes

- **`databricks*` / `databrickslabs*` actions are skipped.** They are
  considered first-party by the policy and do not require pinning; they
  remain on tag references.
- **Mono-repo tag prefixes.** `resolve-action-ref` handles actions under a
  mono-repo path (e.g. `databrickslabs/sandbox/acceptance` → tags like
  `acceptance/v0.4.4`). Review the `⚠` output before applying — the doc
  flags this as a known glitch.
- **`pin-gh-actions` does not switch branches.** Unlike the reference
  implementation at `databrickslabs/blueprint`, this script assumes the
  caller has already checked out the target branch.
- **Comment is informational only.** A reviewer verifying this PR must
  re-run `resolve-action-ref` (or an equivalent `gh api` lookup) to
  confirm every SHA corresponds to the claimed tag.

## Refresh cadence

The cutoff date is a constant inside `resolve-action-ref` and
`pin-gh-actions`. It will only change when the policy is updated by the
Databricks Labs team, at which point both scripts should be updated in
lockstep.
