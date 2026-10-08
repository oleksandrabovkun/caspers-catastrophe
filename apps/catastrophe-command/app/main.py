from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    Disposition,
    ExecuteStatementRequestOnWaitTimeout,
    Format,
)
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from . import db
from .agent import Agent
from .catastrophe_scenarios import (
    CITIES,
    city_config,
    city_geo_payload,
    generate_city_kitchens,
)

log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Open the Lakebase pool and ensure the orders/statuses/refunds/complaints
    # schema. Non-fatal: the sim runs client-side even if Lakebase is unavailable.
    db.init_db()
    cfg = db.get_config()
    if cfg and cfg.get("city"):
        _mirror_active_city_to_uc(str(cfg["city"]))
    yield


app = FastAPI(title="Catastrophe Command Center", version="4.0.0", lifespan=lifespan)

CATALOG = os.environ.get("DATABRICKS_CATALOG", "devconnect")
SIMULATOR_SCHEMA = os.environ.get("SIMULATOR_SCHEMA", "metadata")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
CATASTROPHE_SEED = os.environ.get("CATASTROPHE_SEED", "2026")
AI_GATEWAY_ENDPOINT_NAME = os.environ.get("AI_GATEWAY_ENDPOINT_NAME", "")
OPS_WAREHOUSE_NAME = os.environ.get("OPS_WAREHOUSE_NAME", "")

# Active DevConnect tour city. CITY_CONFIG is an optional JSON override from
# the initializer; geography otherwise comes from catastrophe_scenarios.py.
CITY = os.environ.get("CITY", "amsterdam").strip().lower()
CITY_NAME = os.environ.get("CITY_NAME", "Amsterdam, Netherlands")


def _city_config() -> dict[str, Any]:
    raw = os.environ.get("CITY_CONFIG", "").strip()
    if raw:
        try:
            return json.loads(raw)
        except (ValueError, TypeError):
            pass
    return city_config(CITY if CITY in CITIES else "amsterdam")


def _city_config_for(city_id: str) -> dict[str, Any]:
    if city_id not in CITIES:
        return _city_config()
    return city_config(city_id)


def _kitchen_seed() -> int:
    try:
        return int(CATASTROPHE_SEED)
    except (TypeError, ValueError):
        return 2026


def _generate_city_kitchens(city_id: str, *, count: int = 24) -> list[dict[str, Any]]:
    """Fallback kitchens when the catastrophe_kitchens table has no usable rows."""
    if city_id not in CITIES:
        return []
    return generate_city_kitchens(city_id, count=count, seed=_kitchen_seed())


class ActionRequest(BaseModel):
    action_type: str
    order_id: str = ""
    incident_id: str = ""
    notes: str = ""


# ── Simulation lifecycle events (written to Lakebase) ────────────────────────
# The map/simulation runs client-side; the frontend POSTs these discrete
# transitions (not per-second ticks) so Lakebase holds a real backend of every
# order, status change, refund and complaint.

class OrderEvent(BaseModel):
    order_id: str
    session_id: str = ""
    city: str = ""
    kitchen: str = ""
    vehicle: str = ""
    kind: str = ""
    cold: bool = False
    cross_river: bool = False
    status: str = "placed"
    late_min: int = 0
    max_delay_min: int = 0
    placed_at: float | None = None       # epoch seconds or millis
    promised_at: float | None = None


class StatusEvent(BaseModel):
    order_id: str
    status: str
    late_min: int = 0


class StatusBatch(BaseModel):
    events: list[StatusEvent]


class RefundEvent(BaseModel):
    order_id: str
    reason: str = ""
    amount: float | None = None
    session_id: str = ""
    city: str = ""


class ComplaintEvent(BaseModel):
    order_id: str
    quote: str = ""
    resolution: str | None = None
    session_id: str = ""
    city: str = ""


class ConfigBody(BaseModel):
    city: str = ""
    orders: int = 45
    speed: int = 4


class SessionBody(BaseModel):
    session: dict[str, Any]


class AgentChat(BaseModel):
    message: str
    history: list[dict[str, str]] = []


INDEX_HTML = Path(__file__).parent.parent / "index.html"
INDEX_HTML_TEXT = INDEX_HTML.read_text(encoding="utf-8") if INDEX_HTML.exists() else ""
_CITY_GEO_MARKER = "/*CITY_GEO*/{}/*END_CITY_GEO*/"
_CITY_GEO_JSON = json.dumps(city_geo_payload(), ensure_ascii=False)
ws = WorkspaceClient()


# Last-used demo config. Persisted to Lakebase with an in-process fallback so a
# refresh can skip the setup screen when localStorage is blocked/partitioned.
_LAST_CONFIG: dict[str, Any] | None = None


def _sql_str(value: str) -> str:
    return (value or "").replace("'", "''")


def _mirror_active_city_to_uc(city_id: str) -> None:
    """Best-effort: warehouse SQL reads demo_active_city for the current picker."""
    if not WAREHOUSE_ID or not city_id:
        return
    city = _sql_str(city_id.strip().lower())
    try:
        _query(
            f"""
            MERGE INTO `{CATALOG}`.`{SIMULATOR_SCHEMA}`.demo_active_city AS t
            USING (SELECT 1 AS id, '{city}' AS city_id) AS s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET
              city_id = s.city_id,
              updated_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (id, city_id, updated_at)
              VALUES (s.id, s.city_id, current_timestamp())
            """
        )
    except HTTPException as e:
        log.warning(f"demo_active_city UC mirror skipped: {e.detail}")
    except Exception as e:
        log.warning(f"demo_active_city UC mirror skipped: {e}")


ACTION_LABELS: dict[str, str] = {
    "reroute_driver": "Reroute driver",
    "issue_credit": "Issue customer credit",
    "cancel_order": "Cancel order",
    "acknowledge_incident": "Acknowledge incident",
}


def _await_statement(statement_id: str, timeout_s: int = 40):
    deadline = time.time() + timeout_s
    response = ws.statement_execution.get_statement(statement_id)
    while True:
        status = getattr(response, "status", None)
        state = getattr(getattr(status, "state", None), "value", "")
        if state in ("SUCCEEDED", "FAILED", "CANCELED"):
            return response
        if state == "CLOSED":
            # Result already materialized; treat as success if rows are present.
            return response
        if time.time() > deadline:
            raise HTTPException(status_code=504, detail="SQL statement timeout")
        time.sleep(1)
        response = ws.statement_execution.get_statement(statement_id)


def _column_names(response: Any) -> list[str]:
    """Extract result column names from a statement response (manifest location varies)."""
    for manifest in (
        getattr(response, "manifest", None),
        getattr(getattr(response, "result", None), "manifest", None),
    ):
        if not manifest:
            continue
        schema = getattr(manifest, "schema", None)
        columns = getattr(schema, "columns", None) if schema else None
        if columns:
            names = [getattr(c, "name", None) for c in columns]
            return [n for n in names if n]
    return []


def _statement_rows(response: Any) -> list[dict[str, Any]]:
    result = getattr(response, "result", None)
    data = getattr(result, "data_array", None) if result else None
    names = _column_names(response)
    if not data or not names:
        return []
    out: list[dict[str, Any]] = []
    for row in data:
        out.append({names[i]: row[i] for i in range(min(len(names), len(row)))})
    return out


def _query(statement: str) -> list[dict[str, Any]]:
    if not WAREHOUSE_ID:
        raise HTTPException(status_code=503, detail="DATABRICKS_WAREHOUSE_ID is not configured")

    initial = ws.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=statement,
        wait_timeout="30s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
    )
    statement_id = getattr(initial, "statement_id", None)
    response = _await_statement(statement_id) if statement_id else initial

    status = getattr(response, "status", None)
    state = getattr(getattr(status, "state", None), "value", "")
    if state not in ("SUCCEEDED", "CLOSED"):
        error_obj = getattr(status, "error", None)
        detail = str(error_obj) if error_obj else f"SQL statement ended in state={state}"
        raise HTTPException(status_code=500, detail=detail)

    return _statement_rows(response)


@app.get("/", include_in_schema=False)
async def index() -> HTMLResponse:
    if not INDEX_HTML_TEXT:
        raise HTTPException(status_code=404, detail="index.html not found")
    html = INDEX_HTML_TEXT.replace(_CITY_GEO_MARKER, _CITY_GEO_JSON, 1)
    return HTMLResponse(
        html,
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"},
    )


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "catalog": CATALOG}


@app.get("/api/cities")
def cities_api() -> list[dict[str, Any]]:
    """Selectable cities for the config screen (registry order, active first)."""
    active = CITY if CITY in CITIES else "amsterdam"
    ordered = [active] + [cid for cid in CITIES if cid != active]
    return [{"id": cid, "name": CITIES[cid].name, "flag": CITIES[cid].flag,
             "active": cid == active} for cid in ordered]


@app.get("/api/city")
def city_api(
    city: str = Query(default=""),
    id: str = Query(default=""),  # legacy alias; prefer `city=`
) -> dict[str, Any]:
    """City geography (bridge choke, reroute, banks, regions) for the map. With
    no city, returns the deploy-time active city; with a city id, computes that
    city. Prefer ?city= over ?id= — some gateways mishandle the bare `id` query."""
    cid = (city or id or "").strip().lower()
    if cid and cid in CITIES:
        return _city_config_for(cid)
    return _city_config()


@app.get("/api/kitchens")
def kitchens_api(city: str = Query(default="")) -> list[dict[str, Any]]:
    """Ghost-kitchen locations for a city from the catastrophe_kitchens table.
    With no `city`, uses the deploy-time active city. Falls back to a generated
    spread if the table has no rows for the requested city (e.g. the stage was
    last materialized for a different city)."""
    cid = city.strip().lower()
    # Lisbon and Seattle are fully curated in code (all on land). The
    # catastrophe_kitchens table may still hold an earlier random spread, so
    # always serve the curated list and skip the table for those cities.
    if cid in ("lisbon", "seattle"):
        return _generate_city_kitchens(cid)
    city_name = CITIES[cid].name if cid and cid in CITIES else CITY_NAME
    rows = _query(
        f"""
        SELECT kitchen_id, name, neighborhood, city, location_id, lat, lon, address
        FROM `{CATALOG}`.`{SIMULATOR_SCHEMA}`.catastrophe_kitchens
        WHERE city = '{_sql_str(city_name)}'
        ORDER BY kitchen_id
        """
    )
    if not rows and cid and cid in CITIES:
        return _generate_city_kitchens(cid)
    return rows


@app.post("/api/actions/execute")
def execute_action(body: ActionRequest) -> dict[str, Any]:
    if body.action_type not in ACTION_LABELS:
        raise HTTPException(status_code=400, detail=f"Unknown action_type: {body.action_type}")
    if not body.order_id and not body.incident_id:
        raise HTTPException(status_code=400, detail="order_id or incident_id required")

    action_id = str(uuid.uuid4())
    label = ACTION_LABELS[body.action_type]
    target = body.order_id or body.incident_id
    return {
        "ok": True,
        "action_id": action_id,
        "message": f"{label} recorded for {target}",
    }


@app.post("/api/sim/order")
def sim_order(body: OrderEvent) -> dict[str, Any]:
    db.upsert_order(body.model_dump())
    return {"ok": True, "persisted": db.enabled()}


@app.post("/api/sim/status-batch")
def sim_status_batch(body: StatusBatch) -> dict[str, Any]:
    db.add_status_batch([event.model_dump() for event in body.events])
    return {
        "ok": True,
        "persisted": db.enabled(),
        "count": len(body.events),
    }


@app.post("/api/sim/refund")
def sim_refund(body: RefundEvent) -> dict[str, Any]:
    db.add_refund(
        body.order_id, body.reason, body.amount, body.session_id, body.city,
    )
    return {"ok": True, "persisted": db.enabled()}


@app.post("/api/sim/complaint")
def sim_complaint(body: ComplaintEvent) -> dict[str, Any]:
    if body.resolution:
        db.resolve_complaint(
            body.order_id, body.resolution, body.session_id, body.city,
        )
    else:
        db.add_complaint(
            body.order_id, body.quote, session_id=body.session_id, city=body.city,
        )
    return {"ok": True, "persisted": db.enabled()}


@app.get("/api/sim/route-policy")
def sim_route_policy(city: str = Query("amsterdam")) -> dict[str, Any]:
    return {"enabled": db.enabled(), "policy": db.get_route_policy(city.strip().lower())}


@app.get("/api/sim/order-statuses")
def sim_order_statuses(limit: int = Query(1000, ge=1, le=5000)) -> dict[str, Any]:
    """Lightweight status feed the map polls so SQL run directly on Lakebase
    (e.g. flipping stuck orders to 'rerouted' or 'reordered') is reflected live
    on screen."""
    return {"enabled": db.enabled(), "statuses": db.order_statuses(limit)}


@app.get("/api/sim/refund-summary")
def refund_summary(
    session_id: str = Query(default=""),
    city: str = Query(default=""),
) -> dict[str, Any]:
    """Lightweight refund totals for the stats panel (no row list)."""
    sid = session_id.strip() or None
    city_id = city.strip() or None
    return {
        "enabled": db.enabled(),
        "summary": db.session_refund_summary(sid, city_id),
    }


@app.get("/api/sim/refunds")
def sim_refunds(
    limit: int = Query(500, ge=1, le=2000),
    session_id: str = Query(default=""),
    city: str = Query(default=""),
    include_summary: bool = Query(default=True),
) -> dict[str, Any]:
    """Session refunds for notification cards (+ optional summary).

    Pass ``include_summary=false`` when the client already polls
    ``/api/sim/refund-summary`` — saves a second Lakebase round-trip.
    """
    sid = session_id.strip() or None
    city_id = city.strip() or None
    session_rows, summary = db.session_refunds_with_summary(
        sid, city_id, limit,
    )
    out: dict[str, Any] = {
        "enabled": db.enabled(),
        "session_refunds": session_rows,
    }
    if include_summary:
        out["summary"] = summary
    return out


@app.get("/api/config")
def get_config() -> dict[str, Any]:
    global _LAST_CONFIG
    if _LAST_CONFIG is None:
        _LAST_CONFIG = db.get_config()
    return {"config": _LAST_CONFIG}


@app.post("/api/config")
def set_config(body: ConfigBody) -> dict[str, Any]:
    global _LAST_CONFIG
    city = body.city.strip().lower()
    _LAST_CONFIG = {"city": city, "orders": body.orders, "speed": body.speed}
    db.set_config(city, body.orders, body.speed)
    db.ensure_route_policy_open(city)
    _mirror_active_city_to_uc(city)
    return {"ok": True, "config": _LAST_CONFIG}


@app.get("/api/session")
def get_session() -> dict[str, Any]:
    return {"session": db.get_session()}


@app.post("/api/session")
def set_session(body: SessionBody) -> dict[str, Any]:
    db.set_session(body.session)
    return {"ok": True, "persisted": db.enabled()}


@app.delete("/api/session")
def delete_session() -> dict[str, Any]:
    db.clear_session()
    return {"ok": True}



# ── Catastrophe agent (Act 2) ────────────────────────────────────────────────
# The agent is a single, swappable file (app/agent.py). It owns the LLM and the
# vetted query catalog; the app injects the two SQL executors so the agent stays
# decoupled from FastAPI/psycopg: `_query` for the UC SQL warehouse (Q3–Q5b) and
# `db.run_script` for Lakebase (Q1–Q2).
_AGENT: Agent | None = None


def _agent_gateway_endpoint() -> str:
    return (AI_GATEWAY_ENDPOINT_NAME or f"{CATALOG}.default.command-agent").strip()


def _agent() -> Agent:
    global _AGENT
    if _AGENT is None:
        _AGENT = Agent(
            warehouse_exec=_query,
            lakebase_exec=db.run_script,
            catalog=CATALOG,
            gateway_endpoint=_agent_gateway_endpoint(),
        )
    return _AGENT


@app.get("/api/agent/info")
def agent_info() -> dict[str, Any]:
    agent = _agent()
    return {
        "available": agent.available(),
        "model": agent.model,
        "via_gateway": agent.via_gateway,
    }


@app.post("/api/agent/chat")
async def agent_chat(body: AgentChat) -> dict[str, Any]:
    message = (body.message or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="message required")
    if not _agent().available():
        raise HTTPException(
            status_code=503,
            detail="Agent unavailable — workspace auth could not be resolved.",
        )
    # Warehouse actions (revenue_at_risk / compare_orders_today_vs_baseline) read Lakebase CDF
    # via lb_orders_history and filter on demo_active_city — keep that in sync
    # with the picker before the agent runs.
    try:
        cfg = _LAST_CONFIG or db.get_config()
        if cfg and cfg.get("city"):
            _mirror_active_city_to_uc(str(cfg["city"]))
    except Exception as e:  # noqa: BLE001
        log.warning(f"Pre-agent demo_active_city refresh skipped: {e}")
    try:
        # Run off FastAPI's sync-route threadpool. The sim fires dozens of
        # blocking POSTs during the catastrophe; a sync agent_chat would queue
        # behind them and the Apps proxy would drop the connection → browser
        # shows "Could not reach the agent."
        return await asyncio.to_thread(_agent().run, message, body.history)
    except HTTPException:
        raise
    except Exception as e:
        log.warning(f"Agent chat failed: {type(e).__name__}: {e}")
        raise HTTPException(status_code=502, detail=f"Agent error: {e}")
