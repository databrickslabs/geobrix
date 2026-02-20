# GeoBriX CI Management Scripts

**Comprehensive GitHub Actions CI integration for local and remote testing**

> **⭐ Recommended**: For organizations with auto-triggered CI workflows, use `push-and-watch.sh` to push code and automatically monitor the triggered workflow run.

---

## 🚀 Quick Start

### 1. Install GitHub CLI

```bash
./scripts/ci/setup-gh-cli.sh
```

This will:
- Install `gh` CLI (if not already installed)
- Authenticate with GitHub
- Verify setup

### 2. Check CI Status

```bash
./scripts/ci/check-ci-status.sh
```

View recent CI runs, status, and available actions.

### 3. Push and Watch CI (Recommended)

```bash
./scripts/ci/push-and-watch.sh
```

**For organizations with auto-triggered CI workflows** - Pushes your code and automatically monitors the triggered workflow run.

**Alternative** (manual trigger):
```bash
./scripts/ci/trigger-remote-tests.sh
```

---

## 📋 Available Scripts

### `run-doc-tests.sh` 📚
**Purpose**: Run documentation tests locally or in CI

**Usage**:
```bash
./scripts/ci/run-doc-tests.sh [command]

# Commands:
./scripts/ci/run-doc-tests.sh local      # Run tests locally in Docker
./scripts/ci/run-doc-tests.sh status     # Check CI status
./scripts/ci/run-doc-tests.sh trigger    # Trigger doc tests in CI
./scripts/ci/run-doc-tests.sh watch      # Watch latest run
./scripts/ci/run-doc-tests.sh logs       # Fetch CI logs
```

**What it does**:
- Runs Python documentation tests from `docs/tests/python/`
- Verifies all code examples shown in documentation work correctly
- Can run locally (Docker) or in CI (GitHub Actions)
- Ensures single source of truth between tests and docs

**Best for**: Testing documentation code before pushing, or checking CI status of doc tests.

**Output**:
- Test pass/fail counts
- Coverage information
- Failed test details
- Links to CI runs

---

### `setup-gh-cli.sh`
**Purpose**: Install and configure GitHub CLI

**Usage**:
```bash
./scripts/ci/setup-gh-cli.sh
```

**What it does**:
- Detects your OS (macOS/Linux)
- Installs `gh` CLI using appropriate package manager
- Guides you through authentication
- Verifies installation

---

### `check-ci-status.sh`
**Purpose**: Check status of GitHub Actions runs

**Usage**:
```bash
./scripts/ci/check-ci-status.sh [limit]

# Examples:
./scripts/ci/check-ci-status.sh        # Show last 10 runs
./scripts/ci/check-ci-status.sh 20     # Show last 20 runs
```

**Output**:
- Recent CI runs for current branch
- Detailed view of latest run
- Test pass/fail summary
- Quick action commands

---

### `push-and-watch.sh` ⭐
**Purpose**: Push code and automatically watch the auto-triggered workflow (Recommended)

**Usage**:
```bash
./scripts/ci/push-and-watch.sh
```

**What it does**:
1. Shows commits to be pushed
2. Confirms with user
3. Pushes to remote (triggers CI automatically)
4. Waits for GitHub to start workflow
5. Finds the workflow run for your specific commit
6. Watches it in real-time
7. Shows final results and next steps

**Best for**: Organizations with auto-triggered CI workflows (push events)

**Output**:
- Commit preview
- Push confirmation
- Workflow run detection
- Real-time progress updates
- Final status (success/failure)
- Next-step commands

**Smart Features**:
- Matches workflow to your exact commit SHA
- Handles concurrent runs
- Gracefully handles delays
- Interactive confirmations

---

### `trigger-remote-tests.sh`
**Purpose**: Push code and manually trigger CI workflow

**Usage**:
```bash
./scripts/ci/trigger-remote-tests.sh
```

**What it does**:
1. Shows current branch
2. Warns if uncommitted changes exist
3. Pushes to remote
4. Lists available workflows
5. Triggers selected workflow
6. Shows status

**Interactive**: Yes - prompts for confirmation

---

### `watch-ci.sh`
**Purpose**: Watch CI run in real-time

**Usage**:
```bash
./scripts/ci/watch-ci.sh [run_id]

# Examples:
./scripts/ci/watch-ci.sh              # Watch latest run
./scripts/ci/watch-ci.sh 1234567890   # Watch specific run
```

**Features**:
- Live updates as tests run
- Shows job progress
- Notifies when complete
- Colored output for pass/fail

---

### `fetch-ci-logs.sh`
**Purpose**: Download and analyze CI logs

**Usage**:
```bash
./scripts/ci/fetch-ci-logs.sh [run_id]

# Examples:
./scripts/ci/fetch-ci-logs.sh              # Fetch latest run logs
./scripts/ci/fetch-ci-logs.sh 1234567890   # Fetch specific run
```

**Output**:
- Full logs saved to `ci-logs/`
- Extracted errors (if any)
- Test summary (passed/failed counts)
- Scala and Python test results

**Saved files**:
- `ci-logs/ci-run-{RUN_ID}-{TIMESTAMP}.log` - Full logs
- `ci-logs/ci-run-{RUN_ID}-errors-{TIMESTAMP}.log` - Errors only

---

### `ci-manager.sh` (Master Script)
**Purpose**: All-in-one CI management interface

**Usage**:
```bash
./scripts/ci/ci-manager.sh [command]

# Commands:
./scripts/ci/ci-manager.sh status      # Check CI status
./scripts/ci/ci-manager.sh push        # Push and watch (recommended)
./scripts/ci/ci-manager.sh trigger     # Manually trigger workflow
./scripts/ci/ci-manager.sh watch       # Watch latest run
./scripts/ci/ci-manager.sh logs        # Fetch latest logs
./scripts/ci/ci-manager.sh help        # Show help
```

**Interactive menu**: Run without arguments for interactive mode.

---

## 🔧 Configuration

### GitHub Authentication

The scripts require GitHub authentication. Run:

```bash
gh auth login
```

Follow prompts to authenticate via:
- Browser
- Personal access token

### Permissions Required

Your GitHub token needs:
- `repo` - Repository access
- `workflow` - Trigger and view workflows
- `read:org` - Read organization membership (required by GitHub CLI)

---

## 📂 Directory Structure

```
scripts/ci/
├── README.md                    # This file
├── QUICKSTART.md                # Quick start guide
├── setup-gh-cli.sh             # Installation & setup
├── authenticate-gh.sh          # Authentication helper
├── check-ci-status.sh          # Status dashboard
├── push-and-watch.sh           # Push & auto-watch (recommended) ⭐
├── trigger-remote-tests.sh     # Manual trigger workflows
├── watch-ci.sh                 # Real-time monitoring
├── fetch-ci-logs.sh            # Log download & analysis
├── run-doc-tests.sh            # Documentation tests 📚
└── ci-manager.sh               # Master CLI

ci-logs/                         # Downloaded logs (created by fetch-ci-logs.sh)
├── ci-run-{ID}-{TIMESTAMP}.log
└── ci-run-{ID}-errors-{TIMESTAMP}.log
```

---

## 🎯 Common Workflows

### Workflow 1: Quick Status Check

```bash
# Check if CI is passing
./scripts/ci/check-ci-status.sh
```

**Use case**: Before starting work, verify CI is green.

---

### Workflow 2: Push and Watch (Recommended) ⭐

```bash
# Push and automatically watch CI (one command!)
./scripts/ci/push-and-watch.sh
```

**Use case**: After committing changes, push and monitor the auto-triggered workflow.

**Alternative** (manual trigger):
```bash
./scripts/ci/trigger-remote-tests.sh
./scripts/ci/watch-ci.sh
```

---

### Workflow 3: Debug Failures

```bash
# Fetch logs and analyze failures
./scripts/ci/fetch-ci-logs.sh
less ci-logs/ci-run-*.log
```

**Use case**: CI failed, need to see detailed error messages.

---

### Workflow 4: Documentation Tests 📚

```bash
# Run documentation tests locally
./scripts/ci/run-doc-tests.sh local

# If passing, check CI status
./scripts/ci/run-doc-tests.sh status

# Trigger doc tests in CI
./scripts/ci/run-doc-tests.sh trigger

# Watch the CI run
./scripts/ci/run-doc-tests.sh watch
```

**Use case**: Verify documentation code examples work before pushing changes.

---

### Workflow 5: Compare Local vs Remote

```bash
# Run tests locally
mvn test 2>&1 | tee test-logs/local-$(date +%Y%m%d-%H%M%S).log

# Push and watch remote tests
./scripts/ci/push-and-watch.sh

# Fetch remote logs (after run completes)
./scripts/ci/fetch-ci-logs.sh

# Compare
diff test-logs/local-*.log ci-logs/ci-run-*.log
```

**Use case**: Tests pass locally but fail on CI (environment differences).

---

## 🔍 Advanced Usage

### Filter Logs by Test Suite

```bash
# Fetch logs
./scripts/ci/fetch-ci-logs.sh

# Filter for specific test suite
grep "RST_AdvancedOperationsTest" ci-logs/ci-run-*.log

# Extract only GDALCalc test results
grep -A 10 "GDALCalcTest" ci-logs/ci-run-*.log
```

### Re-run Failed Jobs

```bash
# Get latest run ID
RUN_ID=$(gh run list --limit 1 --json databaseId --jq '.[0].databaseId')

# Re-run failed jobs only
gh run rerun $RUN_ID --failed
```

### View Specific Job

```bash
# List jobs in a run
gh run view $RUN_ID --json jobs --jq '.jobs[] | {id, name, status}'

# View specific job logs
gh run view $RUN_ID --job $JOB_ID --log
```

---

## 🐛 Troubleshooting

### Problem: `gh: command not found`

**Solution**: Run setup script
```bash
./scripts/ci/setup-gh-cli.sh
```

### Problem: "Not authenticated with GitHub"

**Solution**: Authenticate
```bash
gh auth login
```

### Problem: "No workflow runs found"

**Possible causes**:
1. Branch has no CI runs yet
2. Workflow not configured for this branch

**Solution**: Trigger a run
```bash
./scripts/ci/trigger-remote-tests.sh
```

### Problem: Workflow trigger fails

**Possible causes**:
1. Workflow name doesn't match
2. Workflow doesn't have `workflow_dispatch` trigger

**Solution**: Check workflow files
```bash
cat .github/workflows/*.yml | grep -A 5 "on:"
```

Add `workflow_dispatch:` to your workflow:
```yaml
on:
  push:
    branches: [ main, beta/* ]
  pull_request:
  workflow_dispatch:  # Add this
```

---

## 📊 Integration with Existing Tools

### Docker Integration

These scripts work seamlessly with the Docker development environment:

```bash
# Local tests in Docker
docker exec geobrix-dev /bin/bash -c "cd /root/geobrix && mvn test"

# Remote tests via GitHub Actions (recommended)
./scripts/ci/push-and-watch.sh

# Or manually trigger
./scripts/ci/trigger-remote-tests.sh
```

### Test Logging

CI logs are saved separately from local test logs:
- **Local**: `test-logs/` - Local test execution logs
- **Remote**: `ci-logs/` - GitHub Actions logs

### Git Integration

All scripts respect your current branch and git state:
- Show uncommitted changes warnings
- Auto-detect current branch
- Push before triggering CI

---

## 🎨 Color Coding

All scripts use consistent color coding:
- 🔵 **Blue**: Info messages
- 🟢 **Green**: Success / Passing tests
- 🔴 **Red**: Errors / Failing tests
- 🟡 **Yellow**: Warnings / In progress
- 🔷 **Cyan**: Headers / Section titles

---

## 📝 Examples

### Example 1: Daily Development Workflow

```bash
# Morning: Check CI status
./scripts/ci/check-ci-status.sh

# After changes: Run local tests
mvn test

# If local tests pass: Commit and push with auto-watch
git add .
git commit -m "feat: new feature"
./scripts/ci/push-and-watch.sh

# It automatically pushes and watches - done!
```

### Example 2: Investigating CI Failure

```bash
# Check what failed
./scripts/ci/check-ci-status.sh

# Fetch detailed logs
./scripts/ci/fetch-ci-logs.sh

# Analyze errors
less ci-logs/ci-run-*-errors-*.log

# View specific test failures
grep "FAILED" ci-logs/ci-run-*.log | grep "GDALCalcTest"

# Fix and re-run
git add .
git commit -m "fix: address CI failures"
./scripts/ci/push-and-watch.sh
```

---

## 🔗 Useful GitHub CLI Commands

These scripts are built on `gh` CLI. Here are additional useful commands:

```bash
# List all workflows
gh workflow list

# View workflow file
gh workflow view <workflow_name>

# Cancel a running workflow
gh run cancel <run_id>

# Delete old runs
gh run list --limit 100 --json databaseId --jq '.[].databaseId' | xargs -I {} gh api repos/{owner}/{repo}/actions/runs/{} -X DELETE

# View run in browser
gh run view <run_id> --web

# Download artifacts
gh run download <run_id>
```

---

## 📚 Additional Resources

- **GitHub CLI Docs**: https://cli.github.com/manual/
- **GitHub Actions Docs**: https://docs.github.com/actions
- **GeoBriX Test Docs**: `claude/tests/PHASE2_COMPLETION_SUMMARY.md`

---

## 🤝 Contributing

To add new CI scripts:
1. Place in `scripts/ci/`
2. Make executable: `chmod +x scripts/ci/your-script.sh`
3. Add documentation to this README
4. Test locally before committing

---

**Last Updated**: January 11, 2026  
**Maintainer**: GeoBriX Development Team
