"""A stand-in for the `docker` CLI in the scripts/ci tests of the gbx:dcs:* commands.

It never reaches a Docker daemon or a registry. Each call appends its argv (JSON) to
$FAKE_DOCKER_LOG and answers from the JSON state in $FAKE_DOCKER_STATE:

- images: {ref: {"digests": [...], "id": ...}}; `image inspect` fails for a ref not in it;
  `tag` copies an image to a new ref; `push` records <repo>@<push_digest> on the ref.
- history_own / history_base: `history` of a build's temporary tag prints history_own then
  history_base (newest first, like Docker); any other image prints history_base.
- info_rc, build_rc, pull_rc, run_rc, push_rc: exit codes (default 0). run_rcs: a list of exit
  codes for the first `run` calls, in order (each call takes the head of the list); then run_rc.
- build_hang: a file path. `build` creates it, then blocks until it is killed (a build in progress).
- push_no_digest: `push` succeeds but records no digest on the ref.
- push_fail_refs: refs whose `push` fails (exit 1) without recording a digest.

An argument naming the real repository (mjohns797/) exits 97 without doing anything.
"""

import json
import os
import sys
import time

TMP_TAG_PREFIX = "gbx-dcs-build-tmp:"


def main(args):
    with open(os.environ["FAKE_DOCKER_LOG"], "a") as fh:
        fh.write(json.dumps(args) + "\n")
    if any("mjohns797" in a for a in args):
        return 97
    path = os.environ["FAKE_DOCKER_STATE"]
    with open(path) as fh:
        state = json.load(fh)
    images = state.setdefault("images", {})

    def save():
        with open(path, "w") as fh:
            json.dump(state, fh)

    cmd = args[0] if args else ""
    if cmd == "build" and state.get("build_hang"):
        with open(state["build_hang"], "w") as fh:
            fh.write("building\n")
        time.sleep(120)
    if cmd == "run" and state.get("run_rcs"):
        rc = state["run_rcs"].pop(0)
        save()
        return rc
    if cmd in ("info", "build", "pull", "run"):
        return state.get(f"{cmd}_rc", 0)
    if cmd == "history":
        image = args[-1]
        lines = state.get("history_base", [])
        if image.startswith(TMP_TAG_PREFIX):
            lines = state.get("history_own", []) + lines
        for line in lines:
            print(line)
        return 0
    if cmd == "tag":
        src, dst = args[1], args[2]
        images[dst] = json.loads(json.dumps(images.get(src, {"digests": []})))
        save()
        return 0
    if cmd == "rmi":
        return 0
    if cmd == "push":
        ref = args[1]
        if ref in state.get("push_fail_refs", []):
            return 1
        repo = ref.rsplit(":", 1)[0]
        digest = f"{repo}@{state.get('push_digest', 'sha256:' + '1' * 64)}"
        img = images.setdefault(ref, {"digests": []})
        if digest not in img["digests"] and not state.get("push_no_digest"):
            img["digests"].append(digest)
        save()
        return state.get("push_rc", 0)
    if cmd == "image" and len(args) > 1 and args[1] == "inspect":
        img = images.get(args[-1])
        if img is None:
            print(f"Error: No such image: {args[-1]}", file=sys.stderr)
            return 1
        if "--format" in args:
            fmt = args[args.index("--format") + 1]
            if "RepoDigests" in fmt:
                for d in img.get("digests", []):
                    print(d)
            elif ".Id" in fmt:
                print(img.get("id", "sha256:" + "f" * 64))
        return 0
    print(f"fake docker: unsupported call {args}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
