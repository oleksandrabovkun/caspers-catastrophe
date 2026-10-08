# City registry

The city/kitchen/crossing registry lives with the app so the Databricks App
can import it at runtime:

`apps/catastrophe-command/app/catastrophe_scenarios.py`

Stages seed `metadata.catastrophe_kitchens` from that module. The live demo
picks a city in the app and triggers the crossing outage in the browser; it
does not replay a pre-baked incident table.

`data/canonical/catastrophe_scenarios.py` re-exports the same module for
notebooks that still put this directory on `sys.path`.
