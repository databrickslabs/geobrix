"""flock(1) for machines that lack it (macOS), in the scripts/ci tests of the gbx:test:* locks.

Supports the forms the lock helper in scripts/commands/common.sh uses:

    flock -n <fd>          fail (exit 1) at once if the lock is held
    flock -w <secs> <fd>   wait up to <secs>, then fail (exit 1)
    flock <fd>             wait for the lock

on a file descriptor the caller opened (`exec 9>>file`), exactly like util-linux flock.
"""

import fcntl
import sys
import time


def main(args):
    nonblock, timeout, fd = False, None, None
    i = 0
    while i < len(args):
        arg = args[i]
        if arg in ("-n", "--nonblock"):
            nonblock = True
        elif arg in ("-w", "--timeout"):
            timeout = float(args[i + 1])
            i += 1
        elif arg in ("-x", "--exclusive"):
            pass
        else:
            fd = int(arg)
        i += 1
    if fd is None:
        print("flock: a file descriptor is required", file=sys.stderr)
        return 64
    deadline = None if timeout is None else time.monotonic() + timeout
    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return 0
        except OSError:
            if nonblock or (deadline is not None and time.monotonic() >= deadline):
                return 1
            time.sleep(0.05)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
