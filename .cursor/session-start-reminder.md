# GeoBrix session start — paste this at the beginning of a session

**Read first:** `.cursor/rules/00-agent-context.mdc` (topic→subagent, topic→rules, commands vs skills).

**Operate:** Use `gbx:*` commands for tests, coverage, docs, Docker, data (see `.cursor/rules/cursor-commands.mdc`). If a command fails, fix the command (skill **add-or-fix-gbx-command**); don’t work around. Delegate to subagents (`.cursor/agents/*.md`) for Test, Coverage, Data, Docs, Function-Info, Docker, GDAL, RasterX, GridX, VectorX. When invoking another agent, pass this rule / 00-agent-context so they have context.

**Environment:** Container `geobrix-dev`; sample data in container at `/Volumes/main/default/geobrix_samples/` (and `geobrix-examples/` under it).

**Required:** Progress ~every 30s on long runs. Beta: no function aliases; one canonical name per function.
