"""Fetching an href that comes from data (a STAC asset, an Overture asset): the destination checks.

A catalog, or a table passed to ``StacClient.download`` / ``repair``, names the hrefs, so an href can name any host,
and the fetch runs on an executor, with the node's network. Each fetch here:

- takes an ``http(s)`` URL only, and judges the host and port the HTTP client (requests, through urllib3) will
  connect to: a URL whose host or port the client reads differently from Python's ``urlsplit`` is refused;
- resolves that host once: a loopback, link-local (``169.254.0.0/16``, so the cloud metadata address, and
  ``fe80::/10``), cloud metadata (``fd00:ec2::254``) or unspecified address is refused always; a private one (RFC 1918
  and ``fc00::/7``) is refused unless the caller names the host in ``allowed_hosts``; and when ``allowed_hosts`` is
  set, any host off it is refused (``"example.com"`` is that host; ``".example.com"`` is it and every host under it);
- connects to an address it checked (the name is not resolved again, so it cannot change in between), with the
  host's name in the ``Host`` header and, for https, in TLS (SNI and the certificate check);
- follows redirects itself, at most ``MAX_REDIRECTS`` hops, and checks each hop as it checks the first;
- reads no ``~/.netrc``: the session does not trust the environment, and the proxies (``HTTP_PROXY``,
  ``HTTPS_PROXY``, ``NO_PROXY``) and the CA bundle (``REQUESTS_CA_BUNDLE``, ``CURL_CA_BUNDLE``) are passed from the
  environment explicitly, so a proxy keeps working.

Behind a proxy, the proxy connects (and resolves the host), so the addresses checked here are not the ones it uses;
a host that does not resolve here is then left to it, and only ``allowed_hosts`` bounds where it goes. Without a
proxy, a host that does not resolve here is refused (there is no checked address to connect to). ``RefusedHref`` is
the refusal; a fetch that is refused is not worth a retry.
"""

from __future__ import annotations

import ipaddress
import os
import socket
from dataclasses import dataclass
from typing import Iterable, Optional, Union
from urllib.parse import urljoin, urlsplit, urlunsplit

MAX_REDIRECTS = 5
_FIX = "Fix: an href must name a public host; check the catalog, or the table, that the href came from"
_DEFAULT_PORT = {"http": 80, "https": 443}
# Cloud metadata addresses outside link-local (AWS's IPv6 one is a ULA): refused like link-local, even when listed.
_METADATA = frozenset({ipaddress.ip_address("fd00:ec2::254")})


class RefusedHref(ValueError):
    """An href GeoBrix does not fetch: its scheme, its host's address, ``allowed_hosts``, or its redirects."""


@dataclass(frozen=True)
class _Target:
    """An href as the HTTP client sends it, judged: the prepared URL, its scheme, host and port, and the addresses
    the host resolved to (each one checked)."""

    url: str
    scheme: str
    host: str
    port: int
    addresses: tuple

    @property
    def netloc(self) -> str:
        """The host (bracketed when an IPv6 literal) and a port other than the scheme's own."""
        host = f"[{self.host}]" if ":" in self.host else self.host
        return (
            host if self.port == _DEFAULT_PORT[self.scheme] else f"{host}:{self.port}"
        )

    def at(self, netloc: str) -> str:
        """The prepared URL with ``netloc`` (an address and port, or the checked host) as its host: its user info
        (already percent-encoded by the client), path and query are kept."""
        parts = urlsplit(self.url)
        userinfo = parts.netloc.rpartition("@")[0]
        authority = f"{userinfo}@{netloc}" if userinfo else netloc
        return urlunsplit((self.scheme, authority, parts.path, parts.query, ""))

    @property
    def clean_url(self) -> str:
        """The URL rebuilt with the checked host and port as its host: what GDAL is handed, so its own URL parser
        reads the host that was checked."""
        return self.at(self.netloc)


def _addresses(host: str, port: int) -> list:
    """The addresses ``host`` resolves to here, in the resolver's order (none when it does not resolve)."""
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except OSError:
        return []
    return list(dict.fromkeys(info[4][0] for info in infos))


def _kind(addr: str) -> Optional[str]:
    """What makes an address one GeoBrix does not fetch from ('loopback' ...), else None."""
    ip = ipaddress.ip_address(addr.split("%", 1)[0])
    mapped = getattr(ip, "ipv4_mapped", None)
    if mapped is not None:
        ip = mapped
    if ip.is_unspecified:
        return "unspecified"
    if ip.is_loopback:
        return "loopback"
    if ip.is_link_local:
        return "link-local"
    if ip in _METADATA:
        return "cloud metadata"
    if ip.is_private:
        return "private"
    return None


def _allowed(host: str, allowed_hosts) -> bool:
    for entry in allowed_hosts:
        entry = entry.strip().lower()
        if entry.startswith("."):
            if host == entry[1:] or host.endswith(entry):
                return True
        elif host == entry:
            return True
    return False


def _hosts(allowed_hosts: Union[None, str, Iterable[str]]):
    if allowed_hosts is None:
        return None
    return (allowed_hosts,) if isinstance(allowed_hosts, str) else tuple(allowed_hosts)


def _client_view(url: str):
    """(scheme, host, port, prepared URL) as requests prepares ``url`` and urllib3 parses it to connect."""
    import requests
    from urllib3.util import parse_url

    try:
        prepared = requests.Request("GET", url).prepare().url
        parsed = parse_url(prepared)
    except (
        Exception
    ) as exc:  # noqa: BLE001 - a URL the client cannot prepare is one GeoBrix does not fetch
        raise RefusedHref(f"{url!r}: {exc}. {_FIX}") from None
    scheme = (parsed.scheme or "").lower()
    host = (parsed.host or "").lower().strip("[]")
    return scheme, host, parsed.port or _DEFAULT_PORT.get(scheme), prepared


def _idna(host: str) -> str:
    if host.isascii():
        return host
    try:
        return host.encode("idna").decode("ascii")
    except UnicodeError:
        return host


def _target(url: str, allowed_hosts) -> _Target:
    """``url`` judged (see the module docstring), or ``RefusedHref``."""
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    if scheme not in _DEFAULT_PORT or not parts.hostname:
        raise RefusedHref(
            f"{url!r}: GeoBrix fetches an href from data only as an http(s) URL with a host. {_FIX}"
        )
    try:
        port = parts.port or _DEFAULT_PORT[scheme]
    except ValueError as exc:
        raise RefusedHref(f"{url!r}: {exc}. {_FIX}") from None
    host = _idna(parts.hostname.lower())
    c_scheme, c_host, c_port, prepared = _client_view(url)
    if (c_scheme, c_host, c_port) != (scheme, host, port):
        # two parsers, two hosts (urllib3 ends the authority at a backslash, urlsplit does not): the one checked
        # would not be the one connected to
        raise RefusedHref(
            f"{url!r}: the HTTP client reads this URL's host as {c_host}:{c_port}, not {host}:{port}, so GeoBrix "
            f"does not fetch it. {_FIX}"
        )
    hosts = _hosts(allowed_hosts)
    listed = hosts is not None and _allowed(host, hosts)
    if hosts is not None and not listed:
        raise RefusedHref(
            f"{url!r}: the host {host} is not in allowed_hosts ({', '.join(hosts)}). Fix: add the host to "
            "allowed_hosts if you trust it, or check where the href came from"
        )
    addrs = _addresses(host, port)
    for addr in addrs:
        kind = _kind(addr)
        if kind == "private" and listed:
            continue  # a private host the caller named (an internal catalog); loopback, link-local never
        if kind is not None:
            fix = (
                f"Fix: if {host} is your own catalog's host, name it in allowed_hosts; else check the catalog, or "
                "the table, that the href came from"
                if kind == "private"
                else _FIX
            )
            raise RefusedHref(
                f"{url!r}: the host {host} resolves to {addr}, a {kind} address, which GeoBrix does not fetch for "
                f"an href that comes from data. {fix}"
            )
    return _Target(prepared, scheme, host, port, tuple(addrs))


def check_href(url: str, allowed_hosts: Union[None, str, Iterable[str]] = None) -> None:
    """``RefusedHref`` unless ``url`` is an http(s) URL whose host may be fetched (see the module docstring)."""
    _target(url, allowed_hosts)


def _session():
    import requests

    session = requests.Session()
    session.trust_env = False  # no ~/.netrc, no environment read behind our back
    return session


def _request_settings(url: str) -> dict:
    import requests

    verify = (
        os.environ.get("REQUESTS_CA_BUNDLE") or os.environ.get("CURL_CA_BUNDLE") or True
    )
    return {"proxies": requests.utils.get_environ_proxies(url), "verify": verify}


_PINNED = None


def _pinned_adapter(server_hostname: Optional[str]):
    """A requests transport whose connections name ``server_hostname`` in TLS (SNI, and the certificate check)
    while they connect to the address in the URL."""
    global _PINNED
    if _PINNED is None:
        from requests.adapters import HTTPAdapter

        class _Pinned(HTTPAdapter):
            def __init__(self, server_hostname):
                self._server_hostname = server_hostname
                super().__init__()

            def init_poolmanager(
                self, connections, maxsize, block=False, **pool_kwargs
            ):
                if self._server_hostname:
                    pool_kwargs["server_hostname"] = self._server_hostname
                super().init_poolmanager(
                    connections, maxsize, block=block, **pool_kwargs
                )

        _PINNED = _Pinned
    return _PINNED(server_hostname)


def _get(session, target: _Target, timeout, headers):
    """One GET of ``target``, connected to an address the check approved (or through the proxy the environment
    names for it); the caller closes the response."""
    import requests

    settings = _request_settings(target.url)
    kwargs = {"timeout": timeout, "stream": True, "allow_redirects": False}
    if requests.utils.select_proxy(target.url, settings["proxies"]) is not None:
        # the proxy connects, and resolves the name: allowed_hosts is what bounds it (security docs)
        return session.get(target.url, headers=headers, **kwargs, **settings)
    if not target.addresses:
        raise RefusedHref(
            f"{target.url!r}: the host {target.host} does not resolve here, so there is no checked address to "
            f"connect to. {_FIX}"
        )
    pinned_headers = {**(headers or {}), "Host": target.netloc}
    error = None
    for (
        addr
    ) in (
        target.addresses
    ):  # every one was checked; the next is tried when one does not connect
        netloc = f"[{addr}]:{target.port}" if ":" in addr else f"{addr}:{target.port}"
        session.mount(
            f"{target.scheme}://{netloc}/",
            _pinned_adapter(target.host if target.scheme == "https" else None),
        )
        url = target.at(netloc)
        try:
            return session.get(
                url,
                headers=pinned_headers,
                proxies={},
                verify=settings["verify"],
                **kwargs,
            )
        except requests.exceptions.SSLError:
            raise
        except requests.exceptions.ConnectionError as exc:
            error = exc
    raise error


def _follow(session, url: str, allowed_hosts, timeout, headers=None):
    """(the response at the end of ``url``'s redirects, its judged target), each hop checked first; the caller
    closes the response."""
    first = url
    for _hop in range(MAX_REDIRECTS + 1):
        target = _target(url, allowed_hosts)
        resp = _get(session, target, timeout, headers)
        if not resp.is_redirect:
            return resp, target
        location = session.get_redirect_target(resp)
        resp.close()
        url = urljoin(target.url, location)
    raise RefusedHref(f"{first!r}: more than {MAX_REDIRECTS} redirects. {_FIX}")


def download(
    url: str,
    outpath: str,
    allowed_hosts: Union[None, str, Iterable[str]] = None,
    timeout: float = 100,
) -> str:
    """Stream ``url`` to ``outpath`` under the checks above; an HTTP error status raises (``raise_for_status``), so a
    throttled or expired href is retried by the caller, not written as data."""
    session = _session()
    try:
        resp, _target_ = _follow(session, url, allowed_hosts, timeout)
        with resp:
            resp.raise_for_status()
            with open(outpath, "wb") as fh:
                for chunk in resp.iter_content(1024 * 1024):
                    if chunk:
                        fh.write(chunk)
    finally:
        session.close()
    return outpath


def final_url(
    url: str,
    allowed_hosts: Union[None, str, Iterable[str]] = None,
    timeout: float = 100,
) -> str:
    """The URL ``url`` redirects to, each hop checked (one byte is asked for, so nothing is downloaded): what a
    windowed read hands GDAL, whose own HTTP client would follow a redirect without the checks. Its authority is the
    checked host and port alone, so GDAL's URL parser reads the same host.
    """
    session = _session()
    try:
        resp, target = _follow(
            session, url, allowed_hosts, timeout, headers={"Range": "bytes=0-0"}
        )
        resp.close()
        return target.clean_url
    finally:
        session.close()
