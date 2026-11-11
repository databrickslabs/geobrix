#!/usr/bin/env python3
"""Remove orphan DESCRIBE FUNCTION overrides (description, usageArgs, examples, extendedUsageArgs, extendedDescription) from expression objects. Function info now comes from function-info.json only."""

import re
import sys
from pathlib import Path

# Script lives in docs/scripts/; repo root is two levels up.
SRC = Path(__file__).resolve().parent.parent.parent / "src" / "main" / "scala"

OVERRIDE_PATTERN = re.compile(
    r"^\s+override def (description|usageArgs|examples|extendedUsageArgs|extendedDescription)\b"
)
COMMENT_PATTERN = re.compile(
    r"^\s+/\* FOR `DESCRIBE FUNCTION EXTENDED|^\s+//TODO: ADD EXPRESSION INFO"
)


def end_of_override(lines: list, i: int) -> int:
    """Return the last line index (inclusive) of the override starting at line i."""
    if i >= len(lines):
        return i
    line = lines[i]
    stripped = line.strip()
    # override def X: String = "..."  (single line)
    if '= "' in line and (line.rstrip().endswith('"') or line.rstrip().endswith('"""')):
        return i
    # override def X: String = s"${...}" (single line)
    if '= s"' in line and line.rstrip().endswith('"'):
        return i
    # override def X: String =  (continuation on next line)
    if ": String =" in line and "{" not in line:
        j = i + 1
        while j < len(lines):
            next_strip = lines[j].strip()
            if not next_strip:
                j += 1
                continue
            if next_strip.startswith('"') or next_strip.startswith('s"""') or next_strip.startswith('"""'):
                # Find closing quote for multi-line string
                if '"""' in lines[j]:
                    close_count = lines[j].count('"""')
                    if close_count >= 2:
                        return j
                    k = j + 1
                    while k < len(lines):
                        if '"""' in lines[k]:
                            return k
                        k += 1
                    return k - 1 if k else j
                if next_strip.endswith('"'):
                    return j
                j += 1
                while j < len(lines):
                    if '"' in lines[j]:
                        return j
                    j += 1
                return j - 1
            if next_strip.startswith("override def"):
                return i
            j += 1
        return j - 1 if j > i + 1 else i
    # override def examples: String = {
    if "= {" in line:
        depth = 1
        j = i + 1
        while j < len(lines):
            l = lines[j]
            depth += l.count("{") - l.count("}")
            if depth <= 0:
                return j
            j += 1
        return j - 1 if j else i
    return i


def remove_block(content: str) -> str:
    lines = content.split("\n")
    start_i = None
    for i, line in enumerate(lines):
        if COMMENT_PATTERN.search(line):
            start_i = i
            break
    if start_i is None:
        # Find first override of the five
        for i, line in enumerate(lines):
            if OVERRIDE_PATTERN.search(line):
                start_i = i
                break
    if start_i is None:
        return content

    # If we started on a comment, skip to first override (next non-blank that is an override)
    if start_i is not None and COMMENT_PATTERN.search(lines[start_i]):
        i = start_i + 1
        while i < len(lines):
            if OVERRIDE_PATTERN.search(lines[i]):
                start_i = i
                break
            if lines[i].strip() == "}" and i > start_i:
                return content
            i += 1
        if not OVERRIDE_PATTERN.search(lines[start_i]):
            # Comment only, remove just the comment line
            new_lines = lines[:start_i] + lines[start_i + 1:]
            return "\n".join(new_lines)

    # Find end: last of the five overrides (with full body)
    end_i = start_i
    i = start_i
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()
        if stripped == "}" and i > start_i:
            break
        if OVERRIDE_PATTERN.search(line):
            end_i = end_of_override(lines, i)
            i = end_i
        i += 1

    # Remove optional blank line before block
    if start_i > 0 and lines[start_i - 1].strip() == "":
        start_i -= 1
    new_lines = lines[:start_i] + lines[end_i + 1:]
    return "\n".join(new_lines)


# The Scala packages that hold expressions. The tree is the list: a deleted or renamed file needs no edit here.
PACKAGES = [
    "com/databricks/labs/gbx/rasterx",
    "com/databricks/labs/gbx/gridx",
    "com/databricks/labs/gbx/vectorx",
]


def main():
    """Strip the orphan overrides from every expression file under PACKAGES that still has one.

    ``--check`` lists the files that would change, writes nothing, and exits 1 when there are any.
    """
    check = "--check" in sys.argv[1:]
    changed = []
    for base in PACKAGES:
        base_path = SRC / base
        if not base_path.exists():
            continue
        for path in sorted(base_path.rglob("*.scala")):
            text = path.read_text()
            if not OVERRIDE_PATTERN.search(text):
                continue
            new_text = remove_block(text)
            if new_text == text:
                continue
            rel = path.relative_to(SRC).as_posix()
            changed.append(rel)
            if not check:
                path.write_text(new_text)
            print(f"{'Would update' if check else 'Updated'}: {rel}")
    if check and changed:
        sys.exit(1)


if __name__ == "__main__":
    main()
