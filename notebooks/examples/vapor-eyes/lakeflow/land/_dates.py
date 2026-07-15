"""Pure date/window helpers for the land task. No Spark, no side effects."""
import re
from datetime import date, timedelta

_DATE8 = re.compile(r"(\d{4})(\d{2})(\d{2})T\d{6}")


def parse_window(window: str) -> tuple[date, date]:
    start, end = window.split("/")
    return date.fromisoformat(start), date.fromisoformat(end)


def asof_window(asof: str, days: int = 1) -> str:
    end = date.fromisoformat(asof)
    start = end - timedelta(days=days)
    return f"{start.isoformat()}/{end.isoformat()}"


def observation_date_from_item(item_id: str, source: str) -> date | None:
    m = _DATE8.search(item_id)
    if not m:
        return None
    y, mo, d = (int(x) for x in m.groups())
    return date(y, mo, d)
