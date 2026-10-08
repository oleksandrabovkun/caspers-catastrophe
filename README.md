# Casper's Catastrophe Command

The bridge is closed. Frozen orders are melting. Complaints are stacking.

**You have minutes, not a ticket to data engineering.**

This is a Databricks app you deploy in your workspace: a live delivery map, Lakebase for the writes, a lakehouse that still answers questions while ops is on fire, and an agent that only runs SQL someone already vetted.

```
Open the app → pick a city → start the outage.
```

## Three scenes

1. **Survive the hour.** Paste the SQL in `runbooks/`. Reroute what can still arrive. Refund what cannot. Pull dead menu items. Size today vs a normal day.
2. **Don't get this lucky twice.** Same SQL, now governed functions. Type *"reroute the stuck cold orders, then refund the open complaints."* The model picks tools. It does not invent queries.
3. **Meet people where they work.** Ops stays in the app. Analysts get a dashboard. Execs ask Genie. Nobody waits for a screenshot.

Talk track: [`runbooks/DEMO.md`](runbooks/DEMO.md).

## Deploy it

Catalog must already exist. One dial: `--var catalog`.

```bash
databricks bundle deploy --var catalog=<name>
databricks bundle run caspers
```

Then open the app and start a city. Do not pass `--params CATALOG=` — the job already inherits the catalog.

```bash
BUNDLE_VAR_catalog=<name> databricks bundle run cleanup
databricks bundle destroy
```

Cleanup removes demo resources. It does **not** drop the catalog.

Workspace one-time: **LTAP Direct Writes**, **Reyden Lakehouse//RT**, **Lakebase Change Data Feed**, **Omnigent**, **Databricks Sandbox**. Omnigent policies live in `omnigent/catastrophe-coder/`.

## What's in the box

| You see | Under the hood |
| --- | --- |
| Live map | Databricks App |
| Reroutes / refunds / complaints | **Lakebase** (Postgres) |
| Revenue at risk, today vs normal | Warehouse SQL + Lakebase CDF |
| Agent that cannot freelance SQL | UC + Postgres functions |
| Cost / rate limits on stage | Unity Gateway `{catalog}.default.command-agent` |

Want another city? Ask your coding agent. Registry is `apps/catastrophe-command/app/catastrophe_scenarios.py`.
