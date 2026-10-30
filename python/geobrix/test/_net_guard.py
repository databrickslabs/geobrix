"""A test-side guard: no socket connection off loopback.

``install_loopback_only(monkeypatch)`` replaces ``socket.socket.connect`` / ``connect_ex`` for the test: a connect to
any address but a loopback one (or a Unix socket) raises before the real connect is called, so no packet leaves, and
is recorded; the caller fails the test on the record (code under test may catch the error). It sees Python sockets
only: GDAL's own HTTP client (C) is not covered, and the rows that drive it use 127.0.0.1 addresses.
"""

import errno
import ipaddress
import socket


def _loopback(family, address) -> bool:
    if family == getattr(socket, "AF_UNIX", object()):
        return True
    host = address[0] if isinstance(address, tuple) else address
    if not isinstance(host, str):
        return False
    if host == "localhost":
        return True
    try:
        ip = ipaddress.ip_address(host.split("%", 1)[0])
    except ValueError:
        return False  # a name: resolved by the C library, out of the test's hands
    mapped = getattr(ip, "ipv4_mapped", None)
    return (mapped or ip).is_loopback


def install_loopback_only(monkeypatch, connect=None) -> list:
    """Guard every socket connect of the test (see the module docstring); returns the refused addresses.
    ``connect`` is the connect the guard hands an allowed address to (default: the socket's own).
    """
    refused = []
    real = connect or socket.socket.connect
    real_ex = socket.socket.connect_ex

    def _check(sock, address):
        if not _loopback(sock.family, address):
            refused.append(address)
            raise OSError(
                errno.ENETUNREACH,
                f"test network guard: no connection off loopback ({address!r})",
            )

    def guarded(sock, address):
        _check(sock, address)
        return real(sock, address)

    def guarded_ex(sock, address):
        _check(sock, address)
        return real_ex(sock, address)

    monkeypatch.setattr(socket.socket, "connect", guarded)
    monkeypatch.setattr(socket.socket, "connect_ex", guarded_ex)
    return refused
