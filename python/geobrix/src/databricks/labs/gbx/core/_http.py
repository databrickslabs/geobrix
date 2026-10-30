"""Fetching an href that comes from data (a STAC asset, an Overture asset): the destination checks.

A catalog, or a table passed to ``StacClient.download`` / ``repair``, names the hrefs, so an href can name any host,
and the fetch runs on an executor, with the node's network. Each fetch here:

- takes an ``http(s)`` URL only, and resolves its host: a loopback, link-local (``169.254.0.0/16``, so the cloud
  metadata address, and ``fe80::/10``) or unspecified address is refused always; a private one (RFC 1918 and
  ``fc00::/7``) is refused unless the caller names the host in ``allowed_hosts``; and when ``allowed_hosts`` is set,
  any host off it is refused (``"example.com"`` is that host; ``".example.com"`` is it and every host under it);
- follows redirects itself, at most ``MAX_REDIRECTS`` hops, and checks each hop as it checks the first;
- reads no ``~/.netrc``: the session does not trust the environment, and the proxies (``HTTP_PROXY``,
  ``HTTPS_PROXY``, ``NO_PROXY``) and the CA bundle (``REQUESTS_CA_BUNDLE``, ``CURL_CA_BUNDLE``) are passed from the
  environment explicitly, so a proxy keeps working.

A host that does not resolve here is left to the request (behind a proxy, the proxy resolves it). ``RefusedHref`` is
the refusal; a fetch that is refused is not worth a retry.
"""

from __future__ import annotations

import ipaddress
import os
import socket
from typing import Iterable, Optional, Union
from urllib.parse import urljoin, urlsplit

MAX_REDIRECTS = 5
_FIX = "Fix: an href must name a public host; check the catalog, or the table, that the href came from"


class RefusedHref(ValueError):
    """An href GeoBrix does not fetch: its scheme, its host's address, ``allowed_hosts``, or its redirects."""


def _addresses(host: str, port: int) -> list:
    """The addresses ``host`` resolves to here (none when it does not resolve)."""
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except OSError:
        return []
    return sorted({info[4][0] for info in infos})


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


def check_href(url: str, allowed_hosts: Union[None, str, Iterable[str]] = None) -> None:
    """``RefusedHref`` unless ``url`` is an http(s) URL whose host may be fetched (see the module docstring)."""
    parts = urlsplit(url)
    if parts.scheme.lower() not in ("http", "https") or not parts.hostname:
        raise RefusedHref(
            f"{url!r}: GeoBrix fetches an href from data only as an http(s) URL with a host. {_FIX}"
        )
    host = parts.hostname.lower()
    hosts = _hosts(allowed_hosts)
    listed = hosts is not None and _allowed(host, hosts)
    if hosts is not None and not listed:
        raise RefusedHref(
            f"{url!r}: the host {host} is not in allowed_hosts ({', '.join(hosts)}). Fix: add the host to "
            "allowed_hosts if you trust it, or check where the href came from"
        )
    try:
        port = parts.port or (443 if parts.scheme.lower() == "https" else 80)
    except ValueError as exc:
        raise RefusedHref(f"{url!r}: {exc}. {_FIX}") from None
    for addr in _addresses(host, port):
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


def _follow(session, url: str, allowed_hosts, timeout, headers=None):
    """The response at the end of ``url``'s redirects, each hop checked first; the caller closes it."""
    first = url
    for _hop in range(MAX_REDIRECTS + 1):
        check_href(url, allowed_hosts)
        resp = session.get(
            url,
            timeout=timeout,
            stream=True,
            allow_redirects=False,
            headers=headers,
            **_request_settings(url),
        )
        if not resp.is_redirect:
            return resp
        target = session.get_redirect_target(resp)
        resp.close()
        url = urljoin(resp.url, target)
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
        resp = _follow(session, url, allowed_hosts, timeout)
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
    windowed read hands GDAL, whose own HTTP client would follow a redirect without the checks.
    """
    session = _session()
    try:
        with _follow(
            session, url, allowed_hosts, timeout, headers={"Range": "bytes=0-0"}
        ) as resp:
            return resp.url
    finally:
        session.close()
