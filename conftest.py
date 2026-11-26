"""Pytest compatibility: ensure the repository root is importable.

The project’s documented runner is ``python -m unittest discover -s tests``;
this file only helps bare ``pytest`` invocations resolve ``strata`` and the
``support`` helper module.
"""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
_TESTS = _ROOT / "tests"
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))
