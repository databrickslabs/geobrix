# Stop Documentation Server

Stops running Docusaurus documentation server

## Usage

```bash
bash .cursor/commands/gbx-docs-stop.sh
```

## Options

- `--help` - Display help message

## Examples

```bash
# Stop docs server
bash .cursor/commands/gbx-docs-stop.sh
```

## Notes

- Stops servers on all ports (3000, 3001, etc.)
- Cleans up PID files and log files
- Safe to run even if no server running
- Uses multiple strategies to ensure complete shutdown
