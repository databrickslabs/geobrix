# GeoBriX CI Quick Start Guide

Get up and running with GitHub CLI-integrated CI management in 5 minutes!

---

## ⚡ Installation (One-Time Setup)

### Step 1: Run Setup Script

```bash
cd /Users/mjohns/IdeaProjects/geobrix
./scripts/ci/setup-gh-cli.sh
```

**This will**:
1. Install GitHub CLI (`gh`) if not present
2. Guide you through GitHub authentication
3. Verify everything is working

**Expected output**:
```
🔧 Setting up GitHub CLI for GeoBriX...
✅ GitHub CLI is already installed: gh version 2.x.x
✅ GitHub CLI is already authenticated
✅ GitHub CLI setup complete!
```

---

### Step 2: Test Your Setup

```bash
./scripts/ci/check-ci-status.sh
```

**Expected output**: Dashboard showing recent CI runs for your current branch.

---

## 🎯 Daily Usage

### Scenario 1: **Check If CI Is Passing**

```bash
./scripts/ci/ci-manager.sh status
```

**Use this**: Before starting work to verify the branch is green.

---

### Scenario 2: **Trigger CI After Making Changes**

```bash
# 1. Make your changes and commit
git add .
git commit -m "feat: your changes"

# 2. Push and trigger CI
./scripts/ci/ci-manager.sh trigger

# 3. Watch the run (optional)
./scripts/ci/ci-manager.sh watch
```

---

### Scenario 3: **CI Failed - Need to Debug**

```bash
# 1. Fetch the logs
./scripts/ci/ci-manager.sh logs

# 2. View the full log
less ci-logs/ci-run-*.log

# 3. Or just view errors
less ci-logs/ci-run-*-errors-*.log

# 4. Search for specific failures
grep "FAILED" ci-logs/ci-run-*.log
grep "GDALCalcTest" ci-logs/ci-run-*.log
```

---

## 🖥️ Interactive Mode

Don't remember the commands? Just run:

```bash
./scripts/ci/ci-manager.sh
```

You'll get a nice menu:

```
╔═══════════════════════════════════════════════════════╗
║  🧊 GeoBriX CI Manager                                ║
║  Manage GitHub Actions CI from the command line       ║
╚═══════════════════════════════════════════════════════╝

What would you like to do?

  1) Check CI status
  2) Trigger new CI run
  3) Watch latest CI run
  4) Fetch CI logs
  5) Setup GitHub CLI
  6) Exit

Enter choice [1-6]:
```

---

## 📚 Common Commands Reference

| Command | What It Does | When To Use |
|---------|--------------|-------------|
| `./scripts/ci/ci-manager.sh status` | Show CI dashboard | Check if tests are passing |
| `./scripts/ci/ci-manager.sh trigger` | Push & trigger CI | After committing changes |
| `./scripts/ci/ci-manager.sh watch` | Watch live | Monitor test progress |
| `./scripts/ci/ci-manager.sh logs` | Download logs | Debug failures |

---

## 🔧 Troubleshooting

### Problem: "gh: command not found"

**Solution**:
```bash
./scripts/ci/setup-gh-cli.sh
```

---

### Problem: "Not authenticated with GitHub"

**Solution**:
```bash
gh auth login
```

Follow the prompts to authenticate via browser or token.

---

### Problem: "No workflow runs found"

**Possible cause**: No CI has run for your branch yet.

**Solution**:
```bash
./scripts/ci/ci-manager.sh trigger
```

---

### Problem: Can't trigger workflow

**Possible cause**: Workflow file doesn't have `workflow_dispatch` trigger.

**Solution**: Add to your `.github/workflows/*.yml`:

```yaml
on:
  push:
    branches: [ main, beta/* ]
  workflow_dispatch:  # <-- Add this line
```

---

## 🎓 Example: Full Development Cycle

```bash
# Morning: Check CI status
./scripts/ci/ci-manager.sh status

# Work on your feature
# ... coding ...

# Run local tests first
mvn test

# If local tests pass, commit and push
git add .
git commit -m "feat: implement new feature"

# Trigger CI
./scripts/ci/ci-manager.sh trigger

# Watch it run (optional)
./scripts/ci/ci-manager.sh watch

# If it passes - celebrate! 🎉
# If it fails - fetch logs and debug
./scripts/ci/ci-manager.sh logs
```

---

## 💡 Pro Tips

### Tip 1: Alias for Quick Access

Add to your `~/.bashrc` or `~/.zshrc`:

```bash
alias ci='cd /Users/mjohns/IdeaProjects/geobrix && ./scripts/ci/ci-manager.sh'
```

Now you can run from anywhere:
```bash
ci status
ci trigger
ci logs
```

---

### Tip 2: Compare Local vs Remote Logs

```bash
# Local test
mvn test 2>&1 | tee test-logs/local-$(date +%Y%m%d-%H%M%S).log

# Remote test
./scripts/ci/ci-manager.sh trigger
./scripts/ci/ci-manager.sh logs

# Compare
diff test-logs/local-*.log ci-logs/ci-run-*.log
```

---

### Tip 3: Monitor Multiple Branches

```bash
# Switch branch
git checkout feature-branch

# Check its CI status
./scripts/ci/ci-manager.sh status

# Switch back
git checkout main
```

---

## 📖 Full Documentation

For complete documentation, see: `scripts/ci/README.md`

---

## 🆘 Need Help?

Run:
```bash
./scripts/ci/ci-manager.sh help
```

Or check:
- **Full README**: `scripts/ci/README.md`
- **GitHub CLI Docs**: https://cli.github.com/manual/
- **Phase 2 Test Docs**: `claude/tests/PHASE2_COMPLETION_SUMMARY.md`

---

**Last Updated**: January 11, 2026  
**Quick Start Time**: ~5 minutes ⏱️
