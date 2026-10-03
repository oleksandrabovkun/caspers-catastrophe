# Catastrophe Command (standalone)

This repo is only the catastrophe command-center demo.

Deploy with DABs (`databricks.yml`). Runtime resources are created by the
**Catastrophe Command Initializer** job and tracked in `utils/uc_state`.

Talk track lives in `runbooks/DEMO.md`. SQL to paste is in `runbooks/`.

```bash
BUNDLE_VAR_catalog=<name> databricks bundle run cleanup
databricks bundle destroy
```

`--params CATALOG=...` does not apply to the cleanup script.

Stages are thin orchestrators under `stages/`. App code is
`apps/catastrophe-command/`. City registry is
`data/canonical/catastrophe_scenarios.py`.
