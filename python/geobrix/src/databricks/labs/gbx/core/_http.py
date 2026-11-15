"""Fetching an href that comes from data (a STAC asset, an Overture asset): the destination checks.

A catalog, or a table passed to ``StacClient.download`` / ``repair``, names the hrefs, so an href can name any host,
and the fetch runs on an executor, with the node's network. Each fetch here:

- takes an ``http(s)`` URL only, and judges the host and port the HTTP client (requests, through urllib3) will
  connect to: a URL whose host or port the client reads differently from Python's ``urlsplit`` is refused;
- resolves that host once, and fetches only from public addresses: a cloud metadata address (``_METADATA``), a
  loopback, link-local or unspecified one, and any other address that is not public (``ipaddress``'s ``is_global``
  false, or multicast or reserved: the documentation and benchmark ranges ...) is refused always; a private one
  (RFC 1918, ``fc00::/7``) or a carrier-grade NAT one (``100.64.0.0/10``) is refused unless the caller names the host
  in ``allowed_hosts``; an IPv4-mapped IPv6 address is judged as the IPv4 address it maps, and a 6to4 one
  (``2002::/16``) is refused whatever it embeds; and when
  ``allowed_hosts`` is set, any host off it is refused (``"example.com"`` is that host; ``".example.com"`` is it and
  every host under it);
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
# The cloud metadata endpoints (AWS, Azure, GCP; AWS's IPv6 one; Alibaba's) and Azure's WireServer (a platform
# endpoint): refused always, even when listed.
_METADATA = frozenset(
    ipaddress.ip_address(a)
    for a in ("169.254.169.254", "fd00:ec2::254", "100.100.100.200", "168.63.129.16")
)
# 6to4 (deprecated, RFC 7526): refused outright, so the verdict does not depend on the Python patch level (whose
# ``is_global`` for it changed) or on what IPv4 address the prefix embeds.
_SIX_TO_FOUR = ipaddress.ip_network("2002::/16")
# The internal ranges a host the caller lists in allowed_hosts may resolve to (an internal catalog), by kind.
_LISTABLE = {
    "private": tuple(
        ipaddress.ip_network(n)
        for n in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "fc00::/7")
    ),
    "carrier-grade NAT": (ipaddress.ip_network("100.64.0.0/10"),),
}


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

    def at(self, netloc: str, userinfo: bool = True) -> str:
        """The prepared URL with ``netloc`` (an address and port, or the checked host) as its host; its path and
        query are kept, and its user info (already percent-encoded by the client) unless ``userinfo`` is False.
        """
        parts = urlsplit(self.url)
        info = parts.netloc.rpartition("@")[0] if userinfo else ""
        authority = f"{info}@{netloc}" if info else netloc
        return urlunsplit((self.scheme, authority, parts.path, parts.query, ""))

    @property
    def auth(self):
        """The href's user info as the request's credentials (user, password), or None."""
        import requests

        user, password = requests.utils.get_auth_from_url(self.url)
        return (user, password) if user or password else None

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
    if ip in _METADATA:
        return "cloud metadata"
    if ip in _SIX_TO_FOUR:
        return "6to4"
    if ip.is_unspecified:
        return "unspecified"
    if ip.is_loopback:
        return "loopback"
    if ip.is_link_local:
        return "link-local"
    for kind, networks in _LISTABLE.items():
        if any(ip in n for n in networks):
            return kind
    if not ip.is_global or ip.is_multicast or ip.is_reserved:
        return "non-public"
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
        if kind in _LISTABLE and listed:
            continue  # an internal host the caller named (an internal catalog); every other kind never
        if kind is not None:
            fix = (
                f"Fix: if {host} is your own catalog's host, name it in allowed_hosts; else check the catalog, or "
                "the table, that the href came from"
                if kind in _LISTABLE
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
        # the user info goes as the credentials, so the URL sent starts with the prefix the adapter is mounted on
        url = target.at(netloc, userinfo=False)
        try:
            return session.get(
                url,
                headers=pinned_headers,
                auth=target.auth,
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
