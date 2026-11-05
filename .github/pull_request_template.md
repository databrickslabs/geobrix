## Summary

<!-- What changes, and why. Two or three sentences. -->

## Checklist

- [ ] Tests run, pinpointed to what the change touches (name the suites or paths):
- [ ] Docs updated, and a release-notes line added when the change is user-visible
- [ ] `bash scripts/commands/gbx-lint-python.sh --check` and `bash scripts/commands/gbx-lint-scalastyle.sh` pass
- [ ] **UC integration on Databricks:** `bash scripts/commands/gbx-test-uc-integration.sh --profile <name>` passed at the sha below. Paste the summary line from the end of the run, or from `python3 scripts/ci/check_uc_integration_evidence.py`:

  ```
  gbx:test:uc-integration PASS at <sha12>: slice ..., examples ...
  ```

- [ ] No internal ids (planning labels, wave numbers, ticket keys, personal email addresses) in docs, comments or messages that users read
