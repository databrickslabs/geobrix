## Summary

<!-- What changes, and why. Two or three sentences. -->

## Checklist

- [ ] Tests run, pinpointed to what the change touches (name the suites or paths):
- [ ] Docs updated, and a release-notes line added when the change is user-visible
- [ ] `bash scripts/commands/gbx-lint-python.sh --check` and `bash scripts/commands/gbx-lint-scalastyle.sh` pass
- [ ] **UC integration on Databricks:** `bash scripts/commands/gbx-uc-tests.sh --profile fevm-geospatial` (the Geospatial Hub) passed at the sha below (`--scope full` when the change bumps the release version). Paste the summary line from the end of the run, or from `python3 scripts/ci/check_uc_integration_evidence.py`:

  ```
  gbx:uc:tests PASS at <sha12>: scope ..., slice ..., examples ...
  ```

- [ ] **Lakeflow connector on Databricks:** `bash scripts/commands/gbx-test-lakeflow-connector.sh --profile fevm-geospatial` (the Geospatial Hub) passed at the sha below. Paste the summary line from `python3 scripts/ci/check_lakeflow_connector_evidence.py`:

  ```
  gbx:test:lakeflow-connector PASS at <sha12>: checks ..., ... table(s), pipeline ..., schema ...
  ```

- [ ] No internal ids (planning labels, wave numbers, ticket keys, personal email addresses) in docs, comments or messages that users read
