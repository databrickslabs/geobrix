#!/bin/bash
# A shell in this checkout's dev container (geobrix-dev unless the worktree's .gbx-local.env names another).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
source "$SCRIPT_DIR/gbx-container.sh"
gbx_load_local_env "$(cd "$SCRIPT_DIR/../.." && pwd)" || exit 1
docker exec -it "$GBX_DEV_CONTAINER" /bin/bash -c "unset JAVA_TOOL_OPTIONS && cd /root/geobrix && /bin/bash"
