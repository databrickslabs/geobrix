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

### Scenario 2: **Push and Watch CI** (Recommended)

```bash
# 1. Make your changes and commit
git add .
git commit -m "feat: your changes"

# 2. Push and automatically watch the triggered CI run
./scripts/ci/push-and-watch.sh

# That's it! It pushes, finds your workflow run, and watches it.
```

**Alternative** (for manual workflow triggers):
```bash
./scripts/ci/ci-manager.sh trigger
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
  2) Push and watch (auto-triggered workflow)
  3) Trigger new CI run (manual)
  4) Watch latest CI run
  5) Fetch CI logs
  6) Setup GitHub CLI
  7) Exit

Enter choice [1-7]:
```

---

## 📚 Common Commands Reference

| Command | What It Does | When To Use |
|---------|--------------|-------------|
| `./scripts/ci/push-and-watch.sh` ⭐ | Push & auto-watch | **Most common - after commits** |
| `./scripts/ci/ci-manager.sh status` | Show CI dashboard | Check if tests are passing |
| `./scripts/ci/ci-manager.sh push` | Same as push-and-watch.sh | Alternative command |
| `./scripts/ci/ci-manager.sh watch` | Watch existing run | Monitor in-progress tests |
| `./scripts/ci/ci-manager.sh logs` | Download logs | Debug failures |

**⭐ Recommended**: Use `push-and-watch.sh` for organizations with auto-triggered CI workflows.

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

# If local tests pass, commit
git add .
git commit -m "feat: implement new feature"

# Push and watch CI (one command does it all!)
./scripts/ci/push-and-watch.sh

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
ci push      # Push and watch
ci logs
```

Or create a direct alias for push-and-watch:
```bash
alias cipush='cd /Users/mjohns/IdeaProjects/geobrix && ./scripts/ci/push-and-watch.sh'
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
