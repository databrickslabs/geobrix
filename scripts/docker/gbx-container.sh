#!/bin/bash
# gbx-container.sh: which dev container THIS checkout uses. SOURCED by common.sh, start_docker*.sh,
# build_smart.sh and the gbx-docker-* commands. Bash 3.2 compatible.
# Main checkout: geobrix-dev on 5005/8888/4040, <root>/sample-data/Volumes at /Volumes (as before).
# A linked git worktree must name its own container in an UNTRACKED <worktree>/.gbx-local.env
# (template: scripts/docker/gbx-local.env.example), or every container command refuses: one
# checkout must never stop, recreate or remount another's container. Values are never printed.
# A linked worktree's file must also name a container (never the main checkout's), and a container is
# only used or removed from a linked worktree when its /root/geobrix mount is that worktree.
GBX_MAIN_CONTAINER="geobrix-dev"
GBX_LOCAL_ENV_KEYS="GBX_DEV_CONTAINER GBX_DEV_PORT_JDWP GBX_DEV_PORT_JUPYTER GBX_DEV_PORT_SPARK_UI GBX_DEV_VOLUMES_DIR GBX_DEV_EXTRA_MOUNTS GBX_DEV_DOCKER_ENV GBX_DEV_IMAGE_TAG GBX_CARGO_REGISTRY"

gbx_is_linked_worktree() {
    local gd cd
    gd="$(git -C "$1" rev-parse --path-format=absolute --git-dir 2>/dev/null)" || return 1
    cd="$(git -C "$1" rev-parse --path-format=absolute --git-common-dir 2>/dev/null)" || return 1
    [ "$(cd "$gd" && pwd -P)" != "$(cd "$cd" && pwd -P)" ]
}

# A refused load leaves GBX_DEV_CONTAINER empty, so a command that forgot gbx_require_local_env
# fails on a blank container name instead of acting on the main checkout's geobrix-dev.
_gbx_local_env_refuse() {
    GBX_LOCAL_ENV_ERROR="$1"; GBX_DEV_CONTAINER=""
    echo "ERROR: $GBX_LOCAL_ENV_ERROR" >&2
    return 1
}

gbx_load_local_env() {
    local root="$1" file line key value p n=0 named=false
    GBX_LOCAL_ENV_ERROR=""; file="${GBX_LOCAL_ENV_FILE:-$root/.gbx-local.env}"
    GBX_LINKED_WORKTREE=false; gbx_is_linked_worktree "$root" && GBX_LINKED_WORKTREE=true
    GBX_DEV_CONTAINER="$GBX_MAIN_CONTAINER"; GBX_DEV_PORT_JDWP=5005; GBX_DEV_PORT_JUPYTER=8888; GBX_DEV_PORT_SPARK_UI=4040
    GBX_DEV_VOLUMES_DIR="$root/sample-data/Volumes"; GBX_DEV_EXTRA_MOUNTS=""; GBX_DEV_DOCKER_ENV=""
    GBX_DEV_IMAGE_TAG="ubuntu24-gdal311-spark"; GBX_CARGO_REGISTRY=""
    if [ ! -f "$file" ]; then
        if [ "$GBX_LINKED_WORKTREE" = true ]; then
            _gbx_local_env_refuse "$root is a linked git worktree with no $file: copy scripts/docker/gbx-local.env.example there and set GBX_DEV_CONTAINER and the GBX_DEV_PORT_* values"
            return 1
        fi
        export GBX_DEV_IMAGE_TAG; return 0
    fi
    while IFS= read -r line || [ -n "$line" ]; do
        n=$((n + 1))
        case "$line" in ''|'#'*) continue ;; esac
        case "$line" in *=*) ;; *)   # a line that is not KEY=VALUE is not echoed: it could be a value
            _gbx_local_env_refuse "line $n of $file is not KEY=VALUE"; return 1 ;; esac
        key="${line%%=*}"; value="${line#*=}"
        case " $GBX_LOCAL_ENV_KEYS " in *" $key "*) ;; *)
            _gbx_local_env_refuse "unknown key '$key' in $file"; return 1 ;; esac
        value="${value%\"}"; value="${value#\"}"
        eval "$key=\$value"   # key is allowlisted above
        [ "$key" = GBX_DEV_CONTAINER ] && named=true
    done < "$file"
    for p in "$GBX_DEV_PORT_JDWP" "$GBX_DEV_PORT_JUPYTER" "$GBX_DEV_PORT_SPARK_UI"; do
        case "$p" in ''|*[!0-9]*) _gbx_local_env_refuse "a GBX_DEV_PORT_* value in $file is not a port"; return 1 ;; esac
    done
    printf '%s' "$GBX_DEV_CONTAINER" | grep -Eq '^[A-Za-z0-9][A-Za-z0-9_.-]*$' || {
        _gbx_local_env_refuse "GBX_DEV_CONTAINER in $file is not a container name"; return 1; }
    if [ "$GBX_LINKED_WORKTREE" = true ]; then   # the default name is the main checkout's: never a worktree's
        if [ "$named" != true ]; then
            _gbx_local_env_refuse "$file does not set GBX_DEV_CONTAINER: a linked git worktree needs a container of its own (set GBX_DEV_CONTAINER and the GBX_DEV_PORT_* values)"; return 1
        elif [ "$GBX_DEV_CONTAINER" = "$GBX_MAIN_CONTAINER" ]; then
            _gbx_local_env_refuse "GBX_DEV_CONTAINER in $file is $GBX_MAIN_CONTAINER, the main checkout's container: a linked git worktree needs a container of its own"; return 1
        fi
    fi
    export GBX_DEV_IMAGE_TAG
}

gbx_require_local_env() { [ -z "${GBX_LOCAL_ENV_ERROR:-}" ] || { echo "ERROR: $GBX_LOCAL_ENV_ERROR" >&2; exit 1; }; }

# The host directory a container binds at /root/geobrix ("" when it has none).
gbx_container_mount_source() {
    docker inspect --format '{{ range .Mounts }}{{ if eq .Destination "/root/geobrix" }}{{ .Source }}{{ end }}{{ end }}' "$1" 2>/dev/null
}

# gbx_require_owned_container <root>: from a linked worktree, exits 1 when the container exists but its
# /root/geobrix is not <root> (another checkout's container, or one nobody can vouch for), so no command
# runs in it or stops, removes or remounts it. No container yet, or the main checkout: nothing to check.
gbx_require_owned_container() {
    local root="$1" src
    [ "${GBX_LINKED_WORKTREE:-false}" = true ] || return 0
    docker ps -a --format '{{.Names}}' 2>/dev/null | grep -Fxq -- "$GBX_DEV_CONTAINER" || return 0
    src="$(gbx_container_mount_source "$GBX_DEV_CONTAINER")"
    if [ -n "$src" ] && { [ "$src" = "$root" ] || [ "$(cd "$src" 2>/dev/null && pwd -P)" = "$(cd "$root" && pwd -P)" ]; }; then
        return 0
    fi
    echo "ERROR: the container $GBX_DEV_CONTAINER mounts ${src:-nothing} at /root/geobrix, not this worktree ($root); it was left untouched." >&2
    echo "  Give this worktree its own GBX_DEV_CONTAINER in .gbx-local.env, or, once you know the other checkout no longer uses it, remove it by hand: docker rm -f $GBX_DEV_CONTAINER" >&2
    exit 1
}

gbx_dev_run_args() {
    local m e
    GBX_DEV_RUN_ARGS=(--name "$GBX_DEV_CONTAINER" -p "$GBX_DEV_PORT_JDWP:5005" -p "$GBX_DEV_PORT_JUPYTER:8888"
        -p "$GBX_DEV_PORT_SPARK_UI:4040" -v "$1:/root/geobrix")
    if [ "${2:-}" != "--no-volumes" ]; then
        GBX_DEV_RUN_ARGS+=(-v "$GBX_DEV_VOLUMES_DIR:/Volumes")
        for m in $GBX_DEV_EXTRA_MOUNTS; do GBX_DEV_RUN_ARGS+=(-v "$m"); done
    fi
    for e in $GBX_DEV_DOCKER_ENV; do GBX_DEV_RUN_ARGS+=(-e "$e"); done
}
