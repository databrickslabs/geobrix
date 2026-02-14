# Prompt Session (Agent Context)

Outputs the contents of the agent-context rule so the agent can review it.

## Usage

```bash
bash .cursor/commands/gbx-prompt-session.sh
```

## Options

- `--help` — Display help message

## What it does

Prints the full contents of `.cursor/rules/00-agent-context.mdc` to stdout. Use this at the start of a session (or when switching context) so the agent has the rule in view: topic→subagent mapping, topic→rules, commands vs skills, and quick reference.

## Examples

```bash
# Paste agent context for the agent to review
bash .cursor/commands/gbx-prompt-session.sh
```
