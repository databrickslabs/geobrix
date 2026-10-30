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


@pytest.fixture
def server(monkeypatch, tmp_path):
    """A local HTTP server: `routes` maps a path to (status, headers, body); `hits` records (path, headers)."""
    for var in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
        monkeypatch.delenv(var, raising=False)
        monkeypatch.delenv(var.lower(), raising=False)
    tif = tmp_path / "served.tif"
    _write_gtiff(str(tif))
    hits, routes = [], {"/a.tif": (200, {}, tif.read_bytes())}

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
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield types.SimpleNamespace(port=srv.server_address[1], hits=hits, routes=routes)
    srv.shutdown()
    srv.server_close()


@pytest.fixture
def public_test(monkeypatch, server):
    """`public.test` is a public (documentation) address to the checks, and the local server to the socket."""
    real_getaddrinfo, real_addresses = socket.getaddrinfo, _http._addresses

    def getaddrinfo(host, *args, **kwargs):
        host = "127.0.0.1" if host == "public.test" else host
        return real_getaddrinfo(host, *args, **kwargs)

    def addresses(host, port):
        if host == "public.test":
            return ["93.184.216.34"]
        return real_addresses(host, port)

    monkeypatch.setattr(socket, "getaddrinfo", getaddrinfo)
    monkeypatch.setattr(_http, "_addresses", addresses)
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
        ("http://169.254.169.254/latest/meta-data/", "link-local"),
        ("http://[fe80::1]/x", "link-local"),
        ("http://10.0.0.5/x", "private"),
        ("http://[fd00:ec2::254]/x", "private"),
        ("http://0.0.0.0/x", "unspecified"),
        ("http://[::ffff:127.0.0.1]/x", "loopback"),
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
    """`stac.corp` is a private (RFC 1918) address, `meta.corp` the metadata address and `lo.corp` loopback, to the
    checks; each is the local server to the socket."""
    names = {
        "stac.corp": "10.0.0.5",
        "meta.corp": "169.254.169.254",
        "lo.corp": "127.0.0.1",
    }
    real_getaddrinfo, real_addresses = socket.getaddrinfo, _http._addresses

    def getaddrinfo(host, *args, **kwargs):
        return real_getaddrinfo("127.0.0.1" if host in names else host, *args, **kwargs)

    def addresses(host, port):
        return [names[host]] if host in names else real_addresses(host, port)

    monkeypatch.setattr(socket, "getaddrinfo", getaddrinfo)
    monkeypatch.setattr(_http, "_addresses", addresses)
    return server.port


def test_a_private_host_the_caller_lists_is_fetched_and_an_unlisted_one_refused(
    server, private_test, tmp_path
):
    href = f"http://stac.corp:{private_test}/a.tif"
    with pytest.raises(_http.RefusedHref, match="a private address"):
        download_href(href, str(tmp_path / "x.tif"))
    with pytest.raises(_http.RefusedHref, match="not in allowed_hosts"):
        download_href(href, str(tmp_path / "x.tif"), allowed_hosts=["other.corp"])
    assert server.hits == []
    download_href(href, str(tmp_path / "x.tif"), allowed_hosts=["stac.corp"])
    assert read_validate(str(tmp_path / "x.tif")) and len(server.hits) == 1


@pytest.mark.parametrize(
    "host,kind", [("meta.corp", "link-local"), ("lo.corp", "loopback")]
)
def test_a_listed_loopback_or_metadata_host_is_still_refused(
    server, private_test, tmp_path, host, kind
):
    href = f"http://{host}:{private_test}/a.tif"
    with pytest.raises(_http.RefusedHref, match=f"a {kind} address"):
        download_href(href, str(tmp_path / "x.tif"), allowed_hosts=[host])
    with pytest.raises(_http.RefusedHref, match="a link-local address"):
        download_href(
            "http://169.254.169.254/latest/meta-data/",
            str(tmp_path / "y"),
            allowed_hosts=["169.254.169.254"],
        )
    assert server.hits == []
