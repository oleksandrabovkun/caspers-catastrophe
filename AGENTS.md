# Catastrophe Command (standalone)

This repo is only the catastrophe command-center demo.

Deploy with DABs (`databricks.yml`). The UC catalog must already exist.
Runtime resources are created by the **Catastrophe Command Initializer** job
and tracked in `utils/uc_state`. Cleanup does not drop the catalog.

Talk track lives in `runbooks/DEMO.md`. SQL to paste is in `runbooks/`.

```bash
databricks bundle deploy --var catalog=<name>
databricks bundle run caspers
BUNDLE_VAR_catalog=<name> databricks bundle run cleanup
databricks bundle destroy
```

Deploy uses `--var catalog=`. The initializer job inherits that; do not pass
`--params CATALOG=`. Cleanup is a script: `bundle run` ignores `--var` and
`--params`, so set `BUNDLE_VAR_catalog=`.

Stages are thin orchestrators under `stages/`. App code is
`apps/catastrophe-command/`. City registry is
`apps/catastrophe-command/app/catastrophe_scenarios.py`.
