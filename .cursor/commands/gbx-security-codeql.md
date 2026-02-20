# Run CodeQL analysis locally (ad-hoc)

Runs **CodeQL** on the repo using the CodeQL CLI. No GitHub license required: the CLI is free to use locally; only uploading to GitHub Code Scanning requires Code Security to be enabled for the repo.

## Usage

```bash
bash .cursor/commands/gbx-security-codeql.sh [OPTIONS]
```

## Options

- `--output <path>` - SARIF output path (default: `test-logs/codeql-results.sarif`).
- `--help` - Display help.

## Prerequisites

- **CodeQL CLI** on your PATH. If missing, the command prints install instructions.
- Install: `gh extension install github/gh-codeql` (then ensure the CodeQL CLI is on your PATH).
- To overcome "CodeQL CLI not found on PATH.":
  ```bash
  gh codeql install-stub ~/bin
  echo 'export PATH="$HOME/bin:$PATH"' >> ~/.zshrc
  source ~/.zshrc
  ```

## Examples

```bash
# Run CodeQL for Python (creates DB, runs queries, writes SARIF)
gbx:security:codeql

# Custom output path
gbx:security:codeql --output ./codeql.sarif
```

## Viewing results

The command always writes:

- **`codeql-results.sarif`** – Standard format for tools and GitHub. Not meant to be read as plain text.

If **jq** is installed, it also generates **`codeql-results.csv`** from the SARIF (query, severity, message, file, line) so you get a non-empty, human-readable file. Without jq, use the SARIF Viewer extension to open the .sarif file.

**To read the output:**

1. **Easiest (if CSV exists):** Open **`test-logs/codeql-results.csv`** in a text editor or Excel.
2. **In Cursor/VS Code:** Install the **SARIF Viewer** extension, then open `codeql-results.sarif` to see results in the UI.
3. **On GitHub:** Use the repo’s Code scanning workflow and Security tab if you upload the SARIF (e.g. via the CodeQL action).

## Notes

- **Overwrite**: The command uses `--overwrite` when creating the CodeQL database, so re-runs replace the existing database at `.codeql/databases/python`.
- **Query pack**: The script runs `codeql pack download codeql/python-queries` before analysis so the Python query pack is available (avoids "Query pack codeql/python-queries cannot be found").
- **CI**: The workflow in `.github/workflows/codeql-analysis.yml` uses CodeQL Action v4. Upload to GitHub requires **Settings → Code security and analysis → Code scanning** to be enabled for the repo.
- **Local use**: Running this command does not upload anywhere; it only produces SARIF and CSV files. No special license is needed for local analysis.
