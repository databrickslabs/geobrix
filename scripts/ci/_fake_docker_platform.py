"""A stand-in for the `docker` CLI in the scripts/ci tests of the geobrix-dev platform handling.

It never reaches a Docker daemon. Each call appends its argv (one JSON line) to $FAKE_DOCKER_LOG
and answers from the JSON state in $FAKE_DOCKER_STATE:

- images: {ref: {"arch": "arm64"|"amd64"}}; `image inspect` fails for a ref not in it, prints the
  architecture for `--format '{{.Architecture}}'` and a Config.Env document otherwise. `build -t`,
  `tag` and `rmi` keep it current.
- container: null, or {"name": ..., "image": ref-or-id, "running": bool, "arch": ..., "mount": path}:
  what `ps`, `inspect`, `stop`, `rm`, `start` and `run` see and change. The name defaults to
  geobrix-dev and is taken from `--name` on `run`; a call naming another container finds nothing.
- history: lines printed by `history`. history_rc / inspect_rc: a non-zero exit of `history` / of
  `image inspect` on a checkpoint image (the build could not be read back).
- container_image_gone: `image inspect` of the container's image fails (its architecture is unknown).
- probe / probe_rc: the lines a `run --rm` (the native-stack probe, no container is created) prints for
  the platform's architecture, and its exit code.
- A `build` with `--secret id=...,src=FILE` reads FILE (content and mode) and appends them to
  $FAKE_DOCKER_LOG.secrets before answering, so a test can see what the build was given.
"""

import json
import os
import stat
import sys

DEFAULT_NAME = "geobrix-dev"


def main(args):
    with open(os.environ["FAKE_DOCKER_LOG"], "a") as fh:
        fh.write(json.dumps(args) + "\n")
    path = os.environ["FAKE_DOCKER_STATE"]
    with open(path) as fh:
        state = json.load(fh)
    images = state.setdefault("images", {})

    def save():
        with open(path, "w") as fh:
            json.dump(state, fh)

    def flag(name):
        return args[args.index(name) + 1] if name in args else None

    cmd = args[0] if args else ""
    container = state.get("container")
    name = (container or {}).get("name", DEFAULT_NAME)
    if cmd == "info":
        return 0
    if cmd == "ps":
        if container and (container.get("running") or "-a" in args):
            print(name)
        return 0
    if cmd == "inspect":
        if not container or args[-1] != name:
            return 1
        fmt = flag("--format") or ""
        print(container.get("mount", "") if "Mounts" in fmt else container["image"])
        return 0
    if cmd == "image" and args[1:2] == ["inspect"]:
        ref = args[-1]
        if state.get("inspect_rc") and ref.startswith("geobrix-checkpoint:"):
            print("Error: cannot inspect", file=sys.stderr)
            return state["inspect_rc"]
        if ref == container.get("image") if container else False:
            if state.get("container_image_gone"):
                print(f"Error: No such image: {ref}", file=sys.stderr)
                return 1
            img = {"arch": container.get("arch", "amd64")}
        else:
            img = images.get(ref)
        if img is None:
            print(f"Error: No such image: {ref}", file=sys.stderr)
            return 1
        fmt = flag("--format")
        if fmt and "Architecture" in fmt:
            print(img["arch"])
        else:
            print(
                json.dumps([{"Config": {"Env": state.get("env", ["PATH=/usr/bin"])}}])
            )
        return 0
    if cmd == "history":
        for line in state.get("history", []):
            print(line)
        if state.get(
            "history_rc"
        ):  # fails AFTER printing: partial output, then an error
            print("Error: cannot read history", file=sys.stderr)
            return state["history_rc"]
        return 0
    if cmd == "build":
        secret = flag("--secret")
        if secret:
            src = dict(kv.split("=", 1) for kv in secret.split(","))["src"]
            with open(src) as fh:
                rec = {
                    "content": fh.read(),
                    "mode": stat.S_IMODE(os.stat(src).st_mode),
                    "src": src,
                }
            with open(os.environ["FAKE_DOCKER_LOG"] + ".secrets", "a") as fh:
                fh.write(json.dumps(rec) + "\n")
        platform = flag("--platform") or ""
        images[flag("-t")] = {"arch": platform.rsplit("/", 1)[-1]}
        save()
        return state.get("build_rc", 0)
    if cmd == "tag":
        images[args[2]] = images.get(args[1], {"arch": "amd64"})
        save()
        return 0
    if cmd == "rmi":
        for ref in args[1:]:
            images.pop(ref, None)
        save()
        return 0
    if cmd in ("stop", "rm") and args[-1] == name:
        if cmd == "stop" and container:
            container["running"] = False
        if cmd == "rm":
            state["container"] = None
        save()
        return 0
    if cmd == "start" and container and args[-1] == name:
        container["running"] = True
        save()
        return 0
    if cmd == "run" and "--rm" in args:
        # the native-stack probe: a throwaway container that prints state["probe"][arch] lines
        arch = (flag("--platform") or "").rsplit("/", 1)[-1]
        if state.get("probe_rc"):
            return state["probe_rc"]
        for line in state.get("probe", {}).get(arch, []):
            print(line)
        return 0
    if cmd == "run":
        platform = flag("--platform") or ""
        state["container"] = {
            "name": flag("--name") or DEFAULT_NAME,
            "image": args[-2],
            "running": True,
            "arch": platform.rsplit("/", 1)[-1],
        }
        save()
        return 0
    return 0  # pull, exec, ...


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
