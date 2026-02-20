# CI Setup (GitHub CLI)

Installs and configures the GitHub CLI (`gh`) for CI management (trigger workflows, watch runs, fetch logs). Run this once per machine if `gh` is not installed or not authenticated.

## Usage

```bash
bash .cursor/commands/gbx-ci-setup.sh
```

## What it does

- Installs `gh` (Homebrew on macOS, apt/yum on Linux) if missing
- Prompts for `gh auth login` if not authenticated
- Ensures required scopes: `repo`, `workflow`, `read:org`
