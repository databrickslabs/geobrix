# Scala style guide (Scalaguide)

GeoBrix follows the **Scala style guide** for all Scala code. This document states our adoption and how we enforce it.

## Adopted style guide

We use the **official Scala Style Guide** as the primary reference:

- **Scala Style Guide** (Scala Documentation):  
  [https://docs.scala-lang.org/style/](https://docs.scala-lang.org/style/)

It covers indentation, naming, types, control structures, method invocation, Scaladoc, and related conventions. We treat it as the baseline; project-specific rules are documented below and in our linter config.

## Enforcement: Scalastyle

Style is enforced automatically with **Scalastyle**:

- **Config:** [scalastyle-config.xml](../scalastyle-config.xml) at the project root  
- **CI:** The main build runs Scalastyle; it can fail the build on violations (e.g. for PRs targeting `main`).  
- **Local:** Run `gbx:lint:scalastyle` (or `bash .cursor/commands/gbx-lint-scalastyle.sh`) to check before pushing.

The Scalastyle rules align with the official style guide (naming, formatting, braces, public method types, etc.) and a few extra rules (e.g. no `println` in committed code without an explicit opt-out). See `scalastyle-config.xml` for the full list.

## References

- [Scala Style Guide](https://docs.scala-lang.org/style/) (official)
- [Scalastyle rules](http://www.scalastyle.org/rules-0.7.0.html)
- [CONTRIBUTING.md](../CONTRIBUTING.md) for how to contribute
