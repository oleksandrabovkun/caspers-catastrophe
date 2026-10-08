# Demo flow

Speaker notes for Casper's Catastrophe Command. SQL to paste is in this
folder. Architecture sketch: `Architecture.png` (if present).

Set the city in the app config screen. Ops warehouse: `{catalog}-ops`.

## Demo 1: Survive

1. Open the app and start the simulation. Everything collapses. Clicking
   individual orders is impossible. We need help.

2. Call the data engineer:

   > **You:** Heyyy, we have an emergency. Could you write SQL to reroute all orders with frozen goods that can still be delivered?
   >
   > **DE:** Sure, when do you need it? I can get it to you tomorrow afternoon.
   >
   > **You:** I need it NOW.

3. In Lakebase, run **`1-lakebase-reroute-orders.sql`** (once), let complaints stack up, then run **`2-lakebase-issue-fair-refund.sql`**. Explain Lakebase.

4. In the `{catalog}-ops` warehouse, run **`3-warehouse-estimate-revenue-at-risk.sql`**, then **`4-warehouse-compare-today-vs-normal.sql`**. Put your catalog in `USE CATALOG IDENTIFIER('your_catalog')` at the top of each file. These read Lakebase CDF (`lb_orders_history`) and `orders.bronze_hist_orders` through UC, read-only. Explain LTAP.

5. Kitchen supply is stuck because of the collapse. Update the menu from inventory with a transaction: run **`5-warehouse-transactions-remove-menu-items.sql`** in the same warehouse (blocks 5a → 5 → 5b). Explain managed tables and transactions.

**Features to cover:** Managed tables, Reyden / Lakehouse//RT, Lakebase (transactional write), LTAP

## Demo 2: Don't be this exposed again

We got lucky. Next time the DE won't pick up. Make the playbook run without the
DE in the loop, without giving up vetted SQL.

1. The SQL from Demo 1 is already registered as UC and Postgres functions (`{catalog}.ai` and Lakebase `public`), governed by UC.

2. In the app agent, type *"reroute the stuck cold orders, then refund the open complaints"*. It chains that SQL from plain English.

3. Add a new UC function with Genie Code. Select `{catalog}.ai` as the schema:

   ```
   Create UC function that return the current city choke-point information.
   ```

4. Show `apps/catastrophe-command/app/agent.py`. Don't edit it by hand — use Omnigent. Open `/omnigent` → **New session** → host **Sandbox**:

   ```
   Add <add function name> apps/catastrophe-command/app/agent.py the same way as other functions.
   ```

   Local policies: `omnigent/catastrophe-coder/`.

5. Optional: grab a coffee and continue from your phone. Share the sandbox session with a QR code.

6. Show Unity Gateway on the in-app `command-agent`: cost tracking, model routing, rate limits.

**Features to cover:** Genie Code, Omnigent, Unity Gateway (cost tracking)

### Genie Code and coding-agent rules

- Re-running the same short prompt must leave one good function, not duplicates. Prefer `CREATE OR REPLACE`. Use the same grants as sibling functions. AI functions go in the AI schema.
- Edit only the file named in the request. Copy existing `QUERY_CATALOG` entries. Invoke the deployed UC function by name; never embed SQL.
- Dashboards: reuse this project's tables, joins, and `demo_active_city` filter. Don't invent tables.

## Demo 3: Meet everyone where they are

1. **Analysts** — open the **Delivery Catastrophe Command Center** dashboard
   (`resources/dashboards/catastrophe_command_center.lvdash.json`), or build one live:

   ```
   Create a dark-themed "Delivery Catastrophe Command Center" dashboard with custom visualizations. Make it look cool.
   ```

   Point out the custom visualizations. You can also analyze dashboards with Genie Code.

2. **Execs** — ask Genie One:

   ```
   For the city we’re managing right now, how does today compare to a normal day — cancellations, disrupted orders, and average lateness? How much revenue is still at risk?
   ```

   Show Genie agents, and Slack or Teams if set up.

3. **Ops** — Databricks Apps with scale-to-zero.

**Features to cover:** Apps, AI/BI custom viz, Genie Agents (fka Spaces)
