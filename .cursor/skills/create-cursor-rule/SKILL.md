---
name: create-cursor-rule
description: Create or update a Cursor rule in the GeoBrix project. Use when the user wants to add a rule, document a convention, or add file-scoped guidance in .cursor/rules/.
---

# Create a Cursor Rule (GeoBrix)

Use this skill when adding or updating a **Cursor rule** in this project. For generic rule format and frontmatter, also refer to Cursor’s **create-rule** skill; this skill adds GeoBrix paths, naming, and integration with the rule system.

## Where rules live

- **Path**: `.cursor/rules/` at project root.
- **Format**: One rule per file, extension `.mdc`, with YAML frontmatter and markdown body.
- **Master rule**: `00-agent-context.mdc` is the read-first context (topic→subagent, topic→rules). Do not duplicate its role; new rules are **topic rules** or **file-scoped** rules that 00-agent-context or subagents can reference.

## Naming and role

- **Critical / read-first**: Use a numeric prefix so it sorts first, e.g. `00-agent-context.mdc`, `01-<name>.mdc`. Reserve `00-` for the single agent-context rule.
- **Topic rules**: Use a short, kebab-case name that matches the topic, e.g. `function-info.mdc`, `docs-test-single-source.mdc`, `subagent-protocol.mdc`.
- **One concern per rule**: Keep each rule focused; split broad topics into multiple rules if needed.

## Frontmatter (required)

Use YAML at the top of every rule:

```yaml
---
description: Brief description of what this rule does (one line)
alwaysApply: true | false
globs: "path/pattern"   # Optional; use when rule is file- or path-scoped
---
```

- **alwaysApply: true** — Loaded every session. Use sparingly (e.g. 00-agent-context, subagent-protocol, or a small set of global standards). Most rules should be `false`.
- **alwaysApply: false** — Rule is loaded when relevant (e.g. matching file open, or referenced by 00-agent-context). Use **globs** to scope by path, e.g. `scripts/**/*.py`, `docs/tests/python/**/*.py`.
- **description** — Short phrase for the rule; keep it clear and searchable.

## Content guidelines

- **Concise**: Prefer under 50 lines for a single rule; under 500 lines total. If longer, consider splitting.
- **Actionable**: Write as internal docs: what to do, when, and why. Include concrete examples (good vs bad) where it helps.
- **Link, don’t duplicate**: Point to 00-agent-context for topic→subagent and topic→rules. Point to subagent files (`.cursor/agents/*.md`) for domain detail. Avoid copying large blocks from other rules.

## After creating or changing a rule

1. **Topic → rules table**: If the rule defines or refines a **topic** (e.g. testing, docs, function-info, coverage), add or update that topic’s row in **`.cursor/rules/00-agent-context.mdc`** in the “Topic → Rule Files” table so agents know where to find it.
2. **Subagent**: If the rule belongs to a topic owned by a subagent (see 00-agent-context “Topic → Subagent” table), add a short note in that subagent’s `.cursor/agents/<name>.md` under a “Rule reference” or “Rules” section, e.g. “See `function-info.mdc` for generator and testing.”
3. **No duplicate topics**: If the rule replaces or narrows an existing one, update 00-agent-context (and any references) so the topic points to the right file(s).

## Example: New topic rule

Creating a rule for “reader naming”:

1. Add `.cursor/rules/reader-naming-convention.mdc` with frontmatter (`description`, `alwaysApply: false`, and `globs` if it’s path-scoped, e.g. `docs/**/*.md`, `src/**/*.scala`).
2. Write the body (convention, examples, links to code or other rules).
3. In 00-agent-context, under “Topic → Rule Files”, add a row or cell for the topic (e.g. “Naming” or “Readers”) and list `reader-naming-convention.mdc`.
4. If a subagent owns that topic (e.g. VectorX for vector readers), add a one-line reference in that subagent’s `.md`.

## Checklist

- [ ] File is `.mdc` in `.cursor/rules/` with a clear, kebab-case name.
- [ ] Frontmatter has `description` and `alwaysApply`; `globs` if path-scoped.
- [ ] Content is focused and under ~500 lines; includes examples where useful.
- [ ] 00-agent-context “Topic → Rule Files” updated if this is a topic rule.
- [ ] Owning subagent’s `.md` updated with a pointer to the rule, if applicable.

## Reference

- **Generic rule format**: Cursor skill **create-rule** (frontmatter, globs, examples).
- **GeoBrix rule layout**: `.cursor/rules/00-agent-context.mdc` (topic index), `.cursor/rules/subagent-protocol.mdc` (delegation), `.cursor/rules/function-info.mdc` (example of a topic rule with globs).
