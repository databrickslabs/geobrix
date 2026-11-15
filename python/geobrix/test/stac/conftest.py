"""The STAC and href-fetch rows never open a connection off loopback (``test/_net_guard.py``): a fetch the code
under test should have refused fails the row instead of dialling out, even when the fix it checks is reverted. The
rows marked ``integration`` (the real Planetary Computer) are left alone."""

from test._net_guard import install_loopback_only

import pytest


@pytest.fixture(autouse=True)
def _no_connection_off_loopback(request, monkeypatch):
    if request.node.get_closest_marker("integration"):
        yield None
        return
    refused = install_loopback_only(monkeypatch)
    yield refused
    assert refused == [], f"the row tried to connect off loopback: {refused}"
