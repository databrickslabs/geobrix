"""Ensure the lakeflow/ directory (parent of tests/) is on sys.path so that
`from land._dates import ...` / `from land.land import ...` resolve when
pytest is invoked from within lakeflow/ (`python -m pytest tests/...`)."""
import sys
from pathlib import Path

_LAKEFLOW_DIR = Path(__file__).resolve().parent.parent
if str(_LAKEFLOW_DIR) not in sys.path:
    sys.path.insert(0, str(_LAKEFLOW_DIR))
