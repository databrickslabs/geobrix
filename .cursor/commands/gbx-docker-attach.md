# Attach to Docker Container

Attach to running geobrix-dev container with interactive bash shell

## Usage

```bash
bash .cursor/commands/gbx-docker-attach.sh [OPTIONS]
```

## Options

- `--user <username>` - Attach as specific user (default: root)
- `--help` - Display help message

## Examples

```bash
# Attach as root
bash .cursor/commands/gbx-docker-attach.sh

# Attach as specific user
bash .cursor/commands/gbx-docker-attach.sh --user spark
```

## Shortcuts

- **Ctrl+D** or `exit` - Detach from container
- Container continues running after detach

## Notes

- Requires container to be running
- Opens interactive bash shell
- Use `gbx:docker:start` if container not running
- Multiple users can attach simultaneously
