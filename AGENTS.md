# Agent instructions

This repository's agent conventions live in plain markdown. Any coding agent working here reads them first:

1. **`CLAUDE.md`** (repo root): the always-on project guide. It covers the architecture, the `gbx:*` command palette (`scripts/commands/`), the development container, the subagent orientation, and the facts that are not findings. Most of it is tool-neutral. Where it names Claude Code features (subagents, hooks, skills), apply the intent with your own tool.
2. **`.claude/rules/*.md`**: path-scoped rules. Before you touch matching files, read the rule that covers them: functions authoring, GDAL resources, CRS resolution, compression, memory budgets, writer scaffolding, UC Volumes, docs and doc tests, notebooks, and `gbx:*` commands.
3. **The current handoff** (internal, gitignored): `.superpowers/prompts/handoff/` in the main checkout. Start from the newest file there; it names the decision record, the ledgers, and the open questions.

Standing basics:
- Use the `gbx:*` commands; if one is broken, fix the command.
- Run only the tests that exercise the code you changed.
- Never push, open a pull request, or publish without the maintainer's explicit go.
