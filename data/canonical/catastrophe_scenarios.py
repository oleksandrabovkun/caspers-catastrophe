"""Compatibility import for notebooks that add data/canonical to sys.path.

The city registry ships with the app so the Databricks runtime can import it:
apps/catastrophe-command/app/catastrophe_scenarios.py

Loaded by file path so this module name does not import itself.
"""

import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

_APP_MODULE = (
    Path(__file__).resolve().parents[2]
    / "apps"
    / "catastrophe-command"
    / "app"
    / "catastrophe_scenarios.py"
)
_spec = spec_from_file_location("_caspers_catastrophe_scenarios", _APP_MODULE)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Could not load city registry from {_APP_MODULE}")
_mod = module_from_spec(_spec)
sys.modules[_spec.name] = _mod
_spec.loader.exec_module(_mod)

globals().update(
    {name: getattr(_mod, name) for name in dir(_mod) if not name.startswith("_")}
)
