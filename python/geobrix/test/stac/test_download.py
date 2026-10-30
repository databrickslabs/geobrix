import http.server
import os
import socket
import threading
import types

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.core import _http
from databricks.labs.gbx.stac._download import (
    download_href,
    fetch_validate_publish,
    read_validate,
    windowed_download,
)


def _write_gtiff(path):
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=8,
        width=8,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(0, 8, 1, 1),
    ) as dst:
        dst.write((np.arange(64, dtype="uint8")).reshape(1, 8, 8))


def test_read_validate_true_for_real_gtiff(tmp_path):
    p = tmp_path / "ok.tif"
    _write_gtiff(str(p))
    assert read_validate(str(p)) is True


def test_read_validate_false_for_garbage(tmp_path):
    p = tmp_path / "bad.tif"
    p.write_bytes(b"<Error>throttled</Error>" * 100)
    assert read_validate(str(p)) is False


def test_fetch_publishes_only_valid(tmp_path):
    src = tmp_path / "src.tif"
    _write_gtiff(str(src))
    out_dir = tmp_path / "out"

    def get(href, timeout=None, stream=None):
        class R:
            def raise_for_status(self):
                pass

            def iter_content(self, n):
                yield open(str(src), "rb").read()

        return R()

    res = fetch_validate_publish(
        lambda: "http://x/ok.tif", str(out_dir), "ok.tif", get=get
    )
    assert res == os.path.join(str(out_dir), "ok.tif")
    assert os.path.exists(res)


def test_fetch_retries_then_gives_up_on_bad(tmp_path):
    out_dir = tmp_path / "out"
    tries = {"n": 0}

    def get(href, timeout=None, stream=None):
        tries["n"] += 1

        class R:
            def raise_for_status(self):
                pass

            def iter_content(self, n):
                yield b"throttled-not-a-raster"

        return R()

    res = fetch_validate_publish(
        lambda: "http://x/bad.tif",
        str(out_dir),
        "bad.tif",
        get=get,
        max_tries=3,
        sleep=lambda s: None,
    )
    assert res is None
    assert tries["n"] == 3
    assert not os.path.exists(os.path.join(str(out_dir), "bad.tif"))


# ---------------------------------------------------------------------------
# Hrefs from data: the destination checks (loopback, private, link-local and unspecified addresses refused; every
# redirect hop checked; allowed_hosts; no ~/.netrc). A local server stands in for a host, and a patched resolver
# makes the name `public.test` a public address to the checks and the local server to the socket.
# ---------------------------------------------------------------------------


def _start(routes, tls=None):
    """A local HTTP(S) server on 127.0.0.1: `routes` maps a path to (status, headers, body); `hits` records
    (path, headers). `tls` is a server ssl context, for HTTPS."""
    hits = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            hits.append((self.path, dict(self.headers)))
            status, headers, body = routes.get(self.path, (404, {}, b""))
            self.send_response(status)
            for k, v in headers.items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    if tls is not None:
        srv.socket = tls.wrap_socket(srv.socket, server_side=True)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, types.SimpleNamespace(
        port=srv.server_address[1], hits=hits, routes=routes
    )


@pytest.fixture
def served_tif(tmp_path):
    tif = tmp_path / "served.tif"
    _write_gtiff(str(tif))
    return tif.read_bytes()


@pytest.fixture
def server(monkeypatch, served_tif):
    """A local HTTP server: `routes` maps a path to (status, headers, body); `hits` records (path, headers)."""
    for var in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"):
        monkeypatch.delenv(var, raising=False)
        monkeypatch.delenv(var.lower(), raising=False)
    srv, ns = _start({"/a.tif": (200, {}, served_tif)})
    yield ns
    srv.shutdown()
    srv.server_close()


def _network(monkeypatch, names, routes):
    """The rows' resolver and network: ``names`` maps a host name to the addresses the checks see (the
    ``_addresses`` seam); ``routes`` maps a name or an address to the local (host, port) a socket reaches for it
    (a route ignores the port the URL names)."""
    real_getaddrinfo = socket.getaddrinfo

    def getaddrinfo(host, port, *args, **kwargs):
        if host in routes:
            host, port = routes[host]
        return real_getaddrinfo(host, port, *args, **kwargs)

    def addresses(host, port):
        if host in names:
            return list(names[host])
        try:  # any other host resolves as it really does (a literal address to itself)
            infos = real_getaddrinfo(host, port, type=socket.SOCK_STREAM)
        except OSError:
            return []
        return list(dict.fromkeys(info[4][0] for info in infos))

    monkeypatch.setattr(socket, "getaddrinfo", getaddrinfo)
    monkeypatch.setattr(_http, "_addresses", addresses)


@pytest.fixture
def public_test(monkeypatch, server):
    """`public.test` is a public (documentation) address to the checks; the network routes that address, and the
    name itself, to the local server."""
    local = ("127.0.0.1", server.port)
    _network(
        monkeypatch,
        {"public.test": ["93.184.216.34"]},
        {"public.test": local, "93.184.216.34": local},
    )
    return f"http://public.test:{server.port}"


def test_a_loopback_href_is_refused_before_any_request_and_not_retried(
    server, tmp_path
):
    sleeps = []
    res = fetch_validate_publish(
        lambda: f"http://127.0.0.1:{server.port}/a.tif",
        str(tmp_path / "out"),
        "a.tif",
        max_tries=3,
        sleep=sleeps.append,
    )
    assert res is None and server.hits == [] and sleeps == []


@pytest.mark.parametrize(
    "href,kind",
    [
        ("http://169.254.169.254/latest/meta-data/", "cloud metadata"),
        ("http://[fd00:ec2::254]/x", "cloud metadata"),
        ("http://100.100.100.200/latest/meta-data/", "cloud metadata"),
        ("http://[::ffff:100.100.100.200]/x", "cloud metadata"),
        ("http://[fe80::1]/x", "link-local"),
        ("http://10.0.0.5/x", "private"),
        ("http://100.64.0.7/x", "carrier-grade NAT"),
        ("http://0.0.0.0/x", "unspecified"),
        ("http://[::ffff:127.0.0.1]/x", "loopback"),
        # any other address that is not public: documentation, benchmark, multicast, reserved
        ("http://192.0.2.10/x", "non-public"),
        ("http://[2001:db8::1]/x", "non-public"),
        ("http://198.18.0.1/x", "non-public"),
        ("http://224.0.0.1/x", "non-public"),
        ("http://240.0.0.1/x", "non-public"),
    ],
)
def test_an_href_to_a_private_destination_is_refused(tmp_path, href, kind):
    with pytest.raises(_http.RefusedHref, match=f"a {kind} address") as raised:
        download_href(href, str(tmp_path / "x"))
    assert "Fix:" in str(raised.value)


def test_a_redirect_from_a_public_host_to_a_private_one_is_refused(
    server, public_test, tmp_path
):
    secret = f"http://127.0.0.1:{server.port}/secret"
    server.routes["/a.tif"] = (302, {"Location": secret}, b"")
    server.routes["/secret"] = (200, {}, b"SECRET")
    out = tmp_path / "x"
    with pytest.raises(_http.RefusedHref, match="a loopback address"):
        download_href(f"{public_test}/a.tif", str(out))
    assert [p for p, _h in server.hits] == ["/a.tif"] and not out.exists()


def test_allowed_hosts_refuses_any_other_host_and_fetches_its_own(
    server, public_test, tmp_path
):
    out = tmp_path / "x.tif"
    with pytest.raises(_http.RefusedHref, match="not in allowed_hosts"):
        download_href(f"{public_test}/a.tif", str(out), allowed_hosts=["other.test"])
    assert server.hits == []
    download_href(f"{public_test}/a.tif", str(out), allowed_hosts=[".public.test"])
    assert read_validate(str(out)) and len(server.hits) == 1


def test_an_href_whose_host_is_in_netrc_gets_no_authorization(
    server, public_test, tmp_path, monkeypatch
):
    netrc = tmp_path / "netrc"
    netrc.write_text(
        " ".join(["machine", "public.test", "login", "u", "password", "p"])
    )
    netrc.chmod(0o600)
    monkeypatch.setenv("NETRC", str(netrc))
    download_href(f"{public_test}/a.tif", str(tmp_path / "x.tif"))
    assert len(server.hits) == 1
    assert "Authorization" not in server.hits[0][1], server.hits[0][1]


def test_a_windowed_read_of_a_loopback_href_is_refused_before_gdal_opens_it(
    server, tmp_path
):
    href = f"http://127.0.0.1:{server.port}/a.tif"
    with pytest.raises(_http.RefusedHref, match="a loopback address"):
        windowed_download(href, str(tmp_path / "w.tif"), (2, 2, 5, 6))
    assert server.hits == []


@pytest.fixture
def private_test(monkeypatch, server):
    """To the checks: `stac.corp` is a private (RFC 1918) address and `cgnat.corp` a carrier-grade NAT one (both
    hosts a caller may list), `meta.corp`, `meta6.corp` and `ali.corp` cloud metadata addresses, `ll.corp`
    link-local, `lo.corp` loopback and `doc.corp` a documentation address; the network routes each name and address
    to the local server.
    """
    names = {
        "stac.corp": "10.0.0.5",
        "cgnat.corp": "100.64.0.7",
        "meta.corp": "169.254.169.254",
        "meta6.corp": "fd00:ec2::254",
        "ali.corp": "100.100.100.200",
        "ll.corp": "fe80::1",
        "lo.corp": "127.0.0.1",
        "doc.corp": "192.0.2.10",
    }
    local = ("127.0.0.1", server.port)
    _network(
        monkeypatch,
        {n: [a] for n, a in names.items()},
        {k: local for k in [*names, *names.values()]},
    )
    return server.port


@pytest.mark.parametrize(
    "host,kind", [("stac.corp", "private"), ("cgnat.corp", "carrier-grade NAT")]
)
def test_a_private_host_the_caller_lists_is_fetched_and_an_unlisted_one_refused(
    server, private_test, tmp_path, host, kind
):
    href = f"http://{host}:{private_test}/a.tif"
    with pytest.raises(_http.RefusedHref, match=f"a {kind} address"):
        download_href(href, str(tmp_path / "x.tif"))
    with pytest.raises(_http.RefusedHref, match="not in allowed_hosts"):
        download_href(href, str(tmp_path / "x.tif"), allowed_hosts=["other.corp"])
    assert server.hits == []
    download_href(href, str(tmp_path / "x.tif"), allowed_hosts=[host])
    assert read_validate(str(tmp_path / "x.tif")) and len(server.hits) == 1


@pytest.mark.parametrize(
    "host,kind",
    [
        ("meta.corp", "cloud metadata"),
        ("meta6.corp", "cloud metadata"),
        ("ali.corp", "cloud metadata"),
        ("ll.corp", "link-local"),
        ("lo.corp", "loopback"),
        ("doc.corp", "non-public"),
    ],
)
def test_a_listed_loopback_or_metadata_host_is_still_refused(
    server, private_test, tmp_path, host, kind
):
    href = f"http://{host}:{private_test}/a.tif"
    with pytest.raises(_http.RefusedHref, match=f"a {kind} address"):
        download_href(href, str(tmp_path / "x.tif"), allowed_hosts=[host])
    with pytest.raises(_http.RefusedHref, match="a cloud metadata address"):
        download_href(
            "http://169.254.169.254/latest/meta-data/",
            str(tmp_path / "y"),
            allowed_hosts=["169.254.169.254"],
        )
    assert server.hits == []


# The host the check judges is the host the HTTP client connects to: a URL whose authority the two parsers split
# differently (urllib3 ends the authority at a backslash, urlsplit at the next "/") is refused, at the first hop
# and at a redirect hop.
@pytest.mark.parametrize("hop", ["first", "redirect"])
def test_an_href_the_http_client_reads_as_another_host_is_refused(
    server, public_test, tmp_path, hop
):
    split = f"http://127.0.0.1:{server.port}\\@public.test/secret"
    server.routes["/%5C@public.test/secret"] = (200, {}, b"SECRET")
    href = split
    if hop == "redirect":
        server.routes["/a.tif"] = (302, {"Location": split}, b"")
        href = f"{public_test}/a.tif"
    out = tmp_path / "x"
    with pytest.raises(_http.RefusedHref, match="reads this URL's host as 127.0.0.1"):
        download_href(href, str(out))
    first = ["/a.tif"] if hop == "redirect" else []
    assert [p for p, _h in server.hits] == first and not out.exists()


def test_a_name_with_a_private_address_among_public_ones_is_refused(
    monkeypatch, server, tmp_path
):
    local = ("127.0.0.1", server.port)
    _network(
        monkeypatch,
        {"multi.test": ["93.184.216.34", "10.0.0.5"]},
        {"multi.test": local, "93.184.216.34": local, "10.0.0.5": local},
    )
    with pytest.raises(_http.RefusedHref, match="10.0.0.5, a private address"):
        download_href(f"http://multi.test:{server.port}/a.tif", str(tmp_path / "x"))
    assert server.hits == []


@pytest.mark.parametrize("location", ["file:///etc/passwd", "ftp://127.0.0.1/x"])
def test_a_redirect_to_another_scheme_is_refused(
    server, public_test, tmp_path, location
):
    server.routes["/a.tif"] = (302, {"Location": location}, b"")
    with pytest.raises(_http.RefusedHref, match="only as an http"):
        download_href(f"{public_test}/a.tif", str(tmp_path / "x"))
    assert [p for p, _h in server.hits] == ["/a.tif"]


def test_a_redirect_chain_stops_at_the_hop_limit(server, public_test, tmp_path):
    server.routes["/a.tif"] = (302, {"Location": "/a.tif"}, b"")
    with pytest.raises(_http.RefusedHref, match=f"more than {_http.MAX_REDIRECTS}"):
        download_href(f"{public_test}/a.tif", str(tmp_path / "x"))
    assert len(server.hits) == _http.MAX_REDIRECTS + 1


def test_the_request_goes_to_the_address_the_check_approved(
    monkeypatch, server, tmp_path
):
    """A name that resolves again at connect time (DNS rebinding) to another host: the request goes to the
    checked address, with the name in its Host header, and never to the rebound one."""
    rebound_srv, rebound = _start({"/a.tif": (200, {}, b"SECRET")})
    try:
        _network(
            monkeypatch,
            {"public.test": ["93.184.216.34"]},
            {
                "public.test": ("127.0.0.1", rebound.port),
                "93.184.216.34": ("127.0.0.1", server.port),
            },
        )
        out = tmp_path / "x.tif"
        download_href(f"http://public.test:{server.port}/a.tif", str(out))
    finally:
        rebound_srv.shutdown()
        rebound_srv.server_close()
    assert rebound.hits == [] and read_validate(str(out))
    assert [h.get("Host") for _p, h in server.hits] == [f"public.test:{server.port}"]


def _certificates(tmp_path):
    """A test CA, and a server certificate and key it signed for each of public.test and other.test."""
    import shutil
    import subprocess

    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("PRECONDITION MISSING: no openssl to make the test certificates")

    def run(*args):
        subprocess.run([openssl, *map(str, args)], check=True, capture_output=True)

    ca_key, ca = tmp_path / "ca.key", tmp_path / "ca.pem"
    run("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", ca_key, "-out", ca, "-days", "1",
        "-subj", "/CN=GeoBrix test CA", "-addext", "basicConstraints=critical,CA:TRUE",
        "-addext", "keyUsage=critical,keyCertSign,cRLSign")  # fmt: skip
    leaves = {}
    for name in ("public.test", "other.test"):
        key, csr, crt = (tmp_path / f"{name}.{e}" for e in ("key", "csr", "pem"))
        ext = tmp_path / f"{name}.ext"
        ext.write_text(
            f"subjectAltName=DNS:{name}\nbasicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\n"
            "extendedKeyUsage=serverAuth\nauthorityKeyIdentifier=keyid,issuer\n"
        )
        run(
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            key,
            "-out",
            csr,
            "-subj",
            f"/CN={name}",
        )
        run("x509", "-req", "-in", csr, "-CA", ca, "-CAkey", ca_key, "-CAcreateserial", "-out", crt,
            "-days", "1", "-extfile", ext)  # fmt: skip
        leaves[name] = (crt, key)
    return ca, leaves


def test_a_pinned_https_fetch_checks_the_certificate_against_the_host_name(
    monkeypatch, server, served_tif, tmp_path
):
    """The connection goes to the checked address, and TLS still names and verifies the host: the server's
    certificate for public.test passes, one for another name fails."""
    import ssl

    import requests

    ca, leaves = _certificates(tmp_path)
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", str(ca))
    for name, ok in (("public.test", True), ("other.test", False)):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(*map(str, leaves[name]))
        srv, ns = _start({"/a.tif": (200, {}, served_tif)}, tls=ctx)
        try:
            # only the checked address reaches the server: the name itself resolves nowhere
            _network(
                monkeypatch,
                {"public.test": ["93.184.216.34"]},
                {"93.184.216.34": ("127.0.0.1", ns.port)},
            )
            out = tmp_path / f"{name}.tif"
            href = f"https://public.test:{ns.port}/a.tif"
            if ok:
                download_href(href, str(out))
                assert read_validate(str(out)) and len(ns.hits) == 1
            else:
                with pytest.raises(requests.exceptions.SSLError):
                    download_href(href, str(out))
                assert ns.hits == [] and not out.exists()
        finally:
            srv.shutdown()
            srv.server_close()


def test_a_windowed_read_of_an_href_without_slashes_is_refused_before_gdal_opens_it(
    server, tmp_path
):
    """rasterio reads ``https:host/path`` (no ``//``) as an https URL; the windowed read judges the href by the
    same parse, so it is refused, not handed to GDAL."""
    href = f"http:127.0.0.1:{server.port}/a.tif"
    with pytest.raises(_http.RefusedHref, match="only as an http"):
        windowed_download(href, str(tmp_path / "w.tif"), (2, 2, 5, 6))
    assert server.hits == []
