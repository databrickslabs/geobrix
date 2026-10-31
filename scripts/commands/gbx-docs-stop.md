# Stop Documentation Server

Stops the Docusaurus documentation server on one port, or on every docs port.

## Usage

```bash
bash scripts/commands/gbx-docs-stop.sh [OPTIONS]
```

## Options

- `--port <number>` - Stop only the server listening on this port. Default: stop every docs server. A missing or non-numeric value exits 2 with the usage text.
- `--force` - With `--port`, also stop a server started from another checkout. Default: refuse.
- `--help` - Display help message.

## Examples

```bash
# Stop the agent preview on 3001 and leave the dev server on 3000 alone
bash scripts/commands/gbx-docs-stop.sh --port 3001

# Stop every docs server on this machine
bash scripts/commands/gbx-docs-stop.sh
```

## Behaviour

- With `--port`, it stops the listener and the `npm`/`node` parents that launched it, nothing else.
- With `--port`, it reads the listener's working directory. A server from another checkout is refused.
- The refusal names that checkout and prints the `--force` command and the other checkout's own command.
- Without `--port`, it stops ports 3000-3003 and prints each PID. It does not check the checkout.
- It removes the PID and log files of the stopped port. It is safe to run when nothing is running.
