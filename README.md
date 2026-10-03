# Casper's Catastrophe Command

A food-delivery network, a closed crossing, and a command center that has to
act *now* — not after a ticket to the data team.

This is a Databricks demo you can deploy in your own workspace. It is a live
map, a transactional ops database, a lakehouse that still answers questions
while ops is on fire, and an agent that only runs SQL someone already vetted.

## The story

Casper's Kitchens runs ghost kitchens across a city. Then the bridge (or
tunnel, or ferry) that the city actually depends on goes out.

Orders pile up on the map faster than anyone can click them. Frozen goods are
melting in the wrong borough. Complaints start stacking. Revenue is still
sitting in bags that might never arrive.

The old playbook is: Slack a data engineer, wait until tomorrow afternoon, hope
the evening rush is still there. This app is the other playbook.

1. **Survive the hour.** Reroute what can still be delivered. Refund what
   cannot. Pull sold-out items off the menu before you take another order for
   them. Size the damage against a normal day.
2. **Don't get this lucky twice.** Register that same SQL as governed functions
   and let an operator say *"reroute the stuck cold orders, then refund the
   open complaints"* — without giving the model a blank SQL editor.
3. **Meet people where they already work.** Ops stays in the app. Analysts get
   a dashboard. Execs ask a question in Genie. Nobody waits for a screenshot.

## What you get

| In the room | What it actually is |
| --- | --- |
| A live delivery map | A Databricks App. Start a city, watch the outage, drive the recovery. |
| Orders that stay consistent under load | **Lakebase** — Postgres for the writes (reroutes, refunds, complaints). |
| "How bad is today?" without blocking ops | **Lakehouse** SQL on the warehouse: live CDF from Lakebase plus 90 days of history. |
| One menu, one inventory, no half-updates | Managed Delta tables and multi-table transactions. |
| An agent that cannot freelance SQL | Unity Catalog functions + Postgres functions. The model picks tools; it does not invent queries. |
| Cost and rate limits you can show on stage | **Unity Gateway** model service `{catalog}.default.command-agent`. |
| A new action before the next incident | **Genie Code** to create a function, **Omnigent** to wire it into the app. |

Data moves both ways so nobody is looking at a stale copy:

- **Lakehouse → Lakebase** — historical orders and refunds sync into Postgres so
  ops SQL can join live tickets to "what a normal day looks like."
- **Lakebase → lakehouse** — Change Data Feed lands live order history in Delta
  so the warehouse can estimate revenue at risk without writing to Postgres.

## Why it matters

Operational systems and analytics platforms used to be different planets.
During an incident that split is the outage: the people who can *write* cannot
see history, and the people who can *see* cannot act.

If recovery depends on one engineer picking up Slack, you do not have a
playbook — you have a hope. Putting vetted SQL behind an agent, a dashboard,
and a warehouse means the same truth shows up for ops, analysts, and execs,
and the next collapse does not wait on a human to paste a query.

## Run it

One-time in the workspace: enable **LTAP Direct Writes**, **Reyden
Lakehouse//RT**, **Lakebase Change Data Feed**, **Omnigent**, and **Databricks
Sandbox**. Install Omnigent locally (`omnigent/catastrophe-coder/`). Add the
Genie Code rules from [`runbooks/DEMO.md`](runbooks/DEMO.md).

```bash
databricks bundle deploy --var catalog=<catalog_name>
databricks bundle run caspers --params "CATALOG=<catalog_name>"
```

Use the **same catalog name** for `--var` and `--params`. They are different
dials; if they disagree, the ops warehouse name lookup breaks.

Then open the app, pick a city, and start the simulation. The SQL you would
paste by hand is in `runbooks/`. Giving the talk? Follow
[`runbooks/DEMO.md`](runbooks/DEMO.md).

Cleanup is a script, not a job (`--params CATALOG=...` is ignored):

```bash
BUNDLE_VAR_catalog=<catalog_name> databricks bundle run cleanup
databricks bundle destroy
```

| Parameter | Where | Used by |
| --- | --- | --- |
| `--var catalog=<name>` | `bundle deploy` | Bundle resources (app, dashboard, warehouse name) |
| `--params "CATALOG=<name>"` | `bundle run caspers` | Initializer job |
| `--params "AI_GATEWAY_ENDPOINT_NAME=<fqn>"` | `bundle run caspers` | Optional. Defaults to `{catalog}.default.command-agent`; the job creates that Unity Gateway service if it is missing |
| `BUNDLE_VAR_catalog=<name>` | `bundle run cleanup` | Cleanup script |

The initializer creates the Unity Gateway service, grants the app `EXECUTE`,
and starts both CDF pipes. Do not start those feeds in the Lakebase UI.

## What's in the box

**Catastrophe Command Initializer** stages:

1. `Canonical_Data` — catalog + `metadata` schema
2. `Lakebase_Project` — Lakebase Autoscaling + `databricks_postgres`
3. `Catastrophe_History` — `{catalog}.orders.bronze_hist_orders` and `bronze_hist_refunds`
4. `Catastrophe_Hist_Lakebase` — hist tables into Postgres
5. `Catastrophe_Command` — kitchens, app, Lakebase CDF, UC functions, Unity Gateway
6. `Catalog_Commits` — catalog-managed commits on the demo Delta tables

```
apps/catastrophe-command/   Databricks App (FastAPI + map UI)
stages/                     initializer notebooks
runbooks/                   speaker notes + Demo 1 SQL
omnigent/                   coding-agent policies
data/canonical/             city/kitchen registry
resources/dashboards/       AI/BI command-center dashboard
utils/                      uc_state, Lakebase helpers, Unity Gateway, catalog commits
destroy.ipynb               catalog teardown for `bundle run cleanup`
```
