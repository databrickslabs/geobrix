# Setting up the UC integration gate in CI

This page is for a workspace and GitHub admin. Today the gate runs on a developer machine:
`gbx:test:uc-integration` writes `test-logs/uc-integration/evidence.json`, the QC check
`uc-integration-evidence` reads it on `gh pr create` and on a push to main, and the pull request
template asks for the summary line. These steps move the same run into GitHub Actions and make it
a required check on `main`. Nothing here is set up yet.

Items marked **(unverified)** were not checked against a live workspace or the repository
settings. The federation wording was read from the Databricks page linked in step 1.

## 1. Service principal and OIDC federation

Prefer GitHub OIDC workload identity federation: it needs no stored secret.

1. Create an account-level service principal for CI, for example `geobrix-ci`. Note its numeric ID and its application (client) ID.
2. Add it to the workspace (`e2-demo-field-eng` or a CI workspace) with no admin role.
3. Create a federation policy for the principal. The Databricks CLI command is
   `databricks account service-principal-federation-policy create <numeric-sp-id>` with a JSON body:

   ```json
   {
     "oidc_policy": {
       "issuer": "https://token.actions.githubusercontent.com",
       "audiences": ["<databricks account id>"],
       "subject": "repo:databrickslabs/geobrix:environment:uc-integration"
     }
   }
   ```

   - The subject names the GitHub environment of step 3. Databricks recommends the Environment entity type, which needs `environment:` in the workflow.
   - A repository created after the date named on the Databricks page has a subject with numeric IDs (`repo:<org>@<id>/<repo>@<id>:environment:<name>`). Check the format for this repository. **(unverified)**
   - Reference: <https://docs.databricks.com/aws/en/dev-tools/auth/provider-github>

If federation is not available, use an OAuth M2M client secret instead. Store `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` as secrets of the protected environment of step 3, never of the repository. Do not use a personal access token.

## 2. Grants for the principal

- A CI catalog, or a CI area in an existing catalog. The run creates the schema `geobrix_ci_pr<N>` and drops it. The principal needs `USE CATALOG` and `CREATE SCHEMA`. It owns the schema it creates.
- The staging Volume. The run writes `uc-it/<sha12>/` there. The principal needs `READ VOLUME` and `WRITE VOLUME` on it, and `USE SCHEMA` on the schema that holds it.
- The SQL warehouse: `CAN USE`.
- Serverless jobs: the right to create and run one-time jobs on Serverless compute, and a workspace folder `/Users/<principal>/GeoBrix` it can write. **(unverified: the permission names depend on the workspace settings)**
- Creating functions with `LANGUAGE PYTHON` or `LANGUAGE SCALA` in its own schema. **(unverified)**

## 3. Protected GitHub environment

1. Settings, Environments, New environment: `uc-integration`.
2. Add required reviewers: the maintainers who may spend workspace compute.
3. Limit deployment branches to pull-request refs and `main`.
4. Add the variables `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID` and `GBX_UC_WAREHOUSE_ID`. None of them is a secret.

A pull request from a fork gets no environment access, so the job does not run for it. A maintainer runs the gate by hand for a fork. **(unverified: confirm against the repository's fork policy)**

## 4. Workflow sketch

```yaml
name: uc-integration
on:
  pull_request:
    branches: [main]
permissions:
  id-token: write   # OIDC token for the federation policy
  contents: read
concurrency:
  group: uc-integration-${{ github.event.pull_request.number }}
  cancel-in-progress: true
jobs:
  uc-integration:
    runs-on: ubuntu-latest   # the wheel build needs Docker: use the heavy runner of the other workflows (unverified)
    environment: uc-integration
    env:
      DATABRICKS_AUTH_TYPE: github-oidc
      DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
      DATABRICKS_CLIENT_ID: ${{ vars.DATABRICKS_CLIENT_ID }}
      CI_CATALOG: geobrix_ci
      CI_SCHEMA: geobrix_ci_pr${{ github.event.pull_request.number }}
    steps:
      - uses: actions/checkout@v4
      - name: Run the gate into the per-PR schema
        run: |
          bash scripts/commands/gbx-test-uc-integration.sh \
            --profile ci --catalog "$CI_CATALOG" --schema "$CI_SCHEMA" \
            --warehouse-id "${{ vars.GBX_UC_WAREHOUSE_ID }}"
      - name: Drop the per-PR schema
        if: always()
        run: databricks schemas delete "$CI_CATALOG.$CI_SCHEMA" --force || true
      - uses: actions/upload-artifact@v4
        if: always()
        with:
          name: uc-integration-evidence
          path: test-logs/uc-integration/
```

Gaps to close before this works. None is built yet **(unverified)**:

- The command names its schema `geobrix_uc_it_<sha12>`. Add a `--schema` option to `gbx:test:uc-integration` so the run can use `geobrix_ci_pr<N>`.
- The command requires `--profile` and passes it to every sub-command. With OIDC the SDK reads `DATABRICKS_AUTH_TYPE`, `DATABRICKS_HOST` and `DATABRICKS_CLIENT_ID` from the environment. Allow a value that means "no profile, use the environment".
- The runner needs the Databricks CLI and `uv`, and Docker for the wheel build.
- The `concurrency` group cancels an older run of the same pull request. The `always()` step then drops the schema it left.

## 5. Required status check on `main`

1. Run the workflow once on a pull request, so GitHub lists the check `uc-integration / uc-integration`.
2. Settings, Branches (or Rulesets), the rule for `main`: require status checks to pass, and add that check.
3. Keep "Require branches to be up to date before merging" on, so the check covers the merged state.

After this, the local gate is a pre-flight convenience. The QC check can stay, or move to `warn`.
