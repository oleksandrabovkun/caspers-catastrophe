from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import time
import uuid
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

log = logging.getLogger(__name__)

app = FastAPI(title="Catastrophe Command Center", version="4.0.0")


@app.on_event("startup")
def _startup() -> None:
    # Open the Lakebase pool and ensure the orders/statuses/refunds/complaints
    # schema. Non-fatal: the sim runs client-side even if Lakebase is unavailable.
    db.init_db()
    cfg = db.get_config()
    if cfg and cfg.get("city"):
        _mirror_active_city_to_uc(str(cfg["city"]))

CATALOG = os.environ.get("DATABRICKS_CATALOG", "devconnect")
SIMULATOR_SCHEMA = os.environ.get("SIMULATOR_SCHEMA", "metadata")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
CATASTROPHE_SEED = os.environ.get("CATASTROPHE_SEED", "2026")
AI_GATEWAY_ENDPOINT_NAME = os.environ.get("AI_GATEWAY_ENDPOINT_NAME", "")
OPS_WAREHOUSE_NAME = os.environ.get("OPS_WAREHOUSE_NAME", "")

# Active DevConnect tour city. CITY_CONFIG is a JSON blob (bridge geography,
# banks, customer regions) produced by the Catastrophe_Command stage from the
# single-source city registry in data/canonical/catastrophe_scenarios.py.
CITY = os.environ.get("CITY", "amsterdam").strip().lower()
CITY_NAME = os.environ.get("CITY_NAME", "Amsterdam, Netherlands")

# Fallback matches the Amsterdam entry in the registry so the app still works if
# CITY_CONFIG is somehow unset (e.g. app started before a fresh stage run).
_AMSTERDAM_CITY_CONFIG: dict[str, Any] = {
    "id": "amsterdam",
    "name": "Amsterdam, Netherlands",
    "flag": "🇳🇱",
    "river": "Amstel",
    "center": [52.351755, 4.909375],
    "bridge": {"name": "Berlagebrug", "coord": [52.34734, 4.9127]},
    "alt": {"name": "Nieuwe Amstelbrug", "coord": [52.35617, 4.90605]},
    "regions": {"a": [52.342459177553344, 4.895332789164471], "b": [52.35222082244666, 4.930067210835529]},
    "approaches": {"a": [52.34651401466287, 4.909760933550911], "b": [52.34816598533713, 4.915639066449089]},
    "close_radius_m": 120,
    "catastrophe": {
        "icon": "🌉",
        "title": "Berlagebrug structural failure",
        "desc": "The Berlagebrug's bascule mechanism seized and a span support cracked. The Amstel crossing is shut for emergency structural checks.",
        "label": "closed",
    },
}


def _city_config() -> dict[str, Any]:
    raw = os.environ.get("CITY_CONFIG", "").strip()
    if raw:
        try:
            return json.loads(raw)
        except (ValueError, TypeError):
            pass
    return _AMSTERDAM_CITY_CONFIG


# ─────────────────────────────────────────────────────────────────────────────
# City registry — MIRRORS data/canonical/catastrophe_scenarios.py (CITIES,
# city_config, generate_city_kitchens). It's duplicated here because the app is
# deployed with only apps/catastrophe-command/app on its runtime path and cannot
# import the canonical module. Keep the two in sync when editing either. This is
# what lets the in-app config screen switch cities without re-running the stage.
# Each tuple: (name, flag, river, bridge_name, (lat,lon), alt_name, (lat,lon), close_radius_m)
# ─────────────────────────────────────────────────────────────────────────────
_CITIES: dict[str, dict[str, Any]] = {
    "amsterdam":    {"name": "Amsterdam, Netherlands", "flag": "🇳🇱", "river": "Amstel",         "bridge_name": "Berlagebrug",                    "bridge": (52.34734, 4.91270),   "alt_name": "Nieuwe Amstelbrug",          "alt": (52.35617, 4.90605),   "r": 120},
    "montreal":     {"name": "Montreal, QC",           "flag": "🇨🇦", "river": "St. Lawrence",    "bridge_name": "Jacques Cartier Bridge",         "bridge": (45.52144, -73.54056), "alt_name": "Victoria Bridge",            "alt": (45.48699, -73.54471), "r": 220},
    "sao_paulo":    {"name": "São Paulo, Brazil",      "flag": "🇧🇷", "river": "Pinheiros",       "bridge_name": "Ponte Estaiada",                 "bridge": (-23.61211, -46.69962),"alt_name": "Ponte Cidade Jardim",        "alt": (-23.58684, -46.69217),"r": 200},
    "vienna":       {"name": "Vienna, Austria",        "flag": "🇦🇹", "river": "Danube",          "bridge_name": "Reichsbrücke",                   "bridge": (48.22802, 16.40921),  "alt_name": "Floridsdorfer Brücke",       "alt": (48.24944, 16.38958),  "r": 220},
    "warsaw":       {"name": "Warsaw, Poland",         "flag": "🇵🇱", "river": "Vistula",         "bridge_name": "Poniatowski Bridge",             "bridge": (52.23565, 21.03829),  "alt_name": "Świętokrzyski Bridge",       "alt": (52.24147, 21.03331),  "r": 200},
    "paris":        {"name": "Paris, France",          "flag": "🇫🇷", "river": "Seine",           "bridge_name": "Pont de la Concorde",            "bridge": (48.86338, 2.31960),   "alt_name": "Pont Alexandre III",         "alt": (48.86347, 2.31352),   "r": 130},
    "lisbon":       {"name": "Lisbon, Portugal",       "flag": "🇵🇹", "river": "Tagus",           "bridge_name": "Ponte 25 de Abril",              "bridge": (38.68917, -9.17694),  "alt_name": "Ponte Vasco da Gama",        "alt": (38.76200, -9.04300),  "r": 260},
    "washington_dc":{"name": "Washington, DC",         "flag": "🇺🇸", "river": "Potomac",         "bridge_name": "Arlington Memorial Bridge",      "bridge": (38.88800, -77.05300), "alt_name": "Theodore Roosevelt Bridge",  "alt": (38.89229, -77.05984), "r": 200},
    "boston":       {"name": "Boston, MA",             "flag": "🇺🇸", "river": "Charles",         "bridge_name": "Longfellow Bridge",              "bridge": (42.36144, -71.07361), "alt_name": "Harvard Bridge",             "alt": (42.35455, -71.09130), "r": 170},
    "bangalore":    {"name": "Bangalore, India",       "flag": "🇮🇳", "river": "Outer Ring Road", "bridge_name": "Silk Board Junction",            "bridge": (12.91582, 77.62404),  "alt_name": "Agara Junction",             "alt": (12.92214, 77.64794),  "r": 200},
    "seoul":        {"name": "Seoul, South Korea",     "flag": "🇰🇷", "river": "Han",             "bridge_name": "Banpo Bridge",                   "bridge": (37.51456, 126.99651), "alt_name": "Hannam Bridge",              "alt": (37.52671, 127.01338), "r": 260},
    "tokyo":        {"name": "Tokyo, Japan",           "flag": "🇯🇵", "river": "Sumida",          "bridge_name": "Kachidoki Bridge",               "bridge": (35.66226, 139.77490), "alt_name": "Eitai Bridge",               "alt": (35.67664, 139.78615), "r": 200},
    "chicago":      {"name": "Chicago, IL",            "flag": "🇺🇸", "river": "Chicago River",   "bridge_name": "DuSable Bridge",                 "bridge": (41.88882, -87.62439), "alt_name": "Wells Street Bridge",        "alt": (41.88755, -87.63399), "r": 140},
    "minneapolis":  {"name": "Minneapolis, MN",        "flag": "🇺🇸", "river": "Mississippi",     "bridge_name": "Hennepin Avenue Bridge",         "bridge": (44.98533, -93.26386), "alt_name": "I-35W St. Anthony Falls Bridge", "alt": (44.97948, -93.24479), "r": 200},
    "new_york":     {"name": "New York, NY",           "flag": "🇺🇸", "river": "East River",      "bridge_name": "Brooklyn Bridge",                "bridge": (40.70567, -73.99633), "alt_name": "Manhattan Bridge",           "alt": (40.70722, -73.99083), "r": 280},
}

# Hardcoded, city-specific catastrophe per tour stop. Each ties to the city's
# real choke point (the primary bridge/junction that closes). icon+title+desc
# drive the in-app push banner; label is the short status on the choke marker.
# MIRRORS CITY_CATASTROPHES in data/canonical/catastrophe_scenarios.py.
_CITY_CATASTROPHES: dict[str, dict[str, str]] = {
    "amsterdam":     {"icon": "🌉", "title": "Berlagebrug structural failure",                "desc": "The Berlagebrug's bascule mechanism seized and a span support cracked. The Amstel crossing is shut for emergency structural checks.", "label": "closed"},
    "montreal":      {"icon": "🧊", "title": "Ice storm shuts the Jacques Cartier Bridge",    "desc": "Freezing rain has glazed the deck and ice is falling from the superstructure. The St. Lawrence crossing is fully closed.", "label": "iced over"},
    "sao_paulo":     {"icon": "🌊", "title": "Flash flood on Marginal Pinheiros",             "desc": "A torrential downpour has flooded the Pinheiros riverside; the Ponte Estaiada approaches are underwater.", "label": "flooded"},
    "vienna":        {"icon": "🌉", "title": "Reichsbrücke pier collapse",                    "desc": "A pier has given way and a span has dropped into the Danube. The Reichsbrücke is gone.", "label": "collapsed"},
    "warsaw":        {"icon": "💣", "title": "WWII bomb found by the Poniatowski Bridge",     "desc": "Construction crews uncovered unexploded WWII ordnance. A bomb-disposal cordon has closed the Vistula crossing.", "label": "cordoned off"},
    "paris":         {"icon": "📢", "title": "Protest blocks the Pont de la Concorde",        "desc": "A mass manifestation has flooded Place de la Concorde and blocked the Seine crossing.", "label": "blocked"},
    "lisbon":        {"icon": "🌬️", "title": "Atlantic windstorm closes Ponte 25 de Abril",   "desc": "Extreme crosswinds have forced a full safety closure of Ponte 25 de Abril. Tagus traffic is diverted to Ponte Vasco da Gama.", "label": "closed by wind"},
    "washington_dc": {"icon": "🚓", "title": "Motorcade locks down the Arlington Memorial Bridge", "desc": "A presidential motorcade and a rolling Secret Service closure have sealed the Arlington Memorial Bridge. The Potomac crossing reopens once the last black SUV clears — sirens included.", "label": "motorcade"},
    "boston":        {"icon": "🚇", "title": "Red Line derailment on the Longfellow",         "desc": "An MBTA train has derailed on the Longfellow Bridge, which carries the Red Line over the Charles. The bridge is closed.", "label": "derailed"},
    "bangalore":     {"icon": "🚗", "title": "Silk Board gridlock meltdown",                  "desc": "Monsoon waterlogging has turned the Silk Board Junction into total gridlock across the Outer Ring Road.", "label": "gridlocked"},
    "seoul":         {"icon": "🌊", "title": "Monsoon floods the Banpo Bridge",               "desc": "A monsoon cloudburst has pushed the Han over Banpo's low deck — the bridge is built to flood, and today it delivered. The crossing is closed until the river drops.", "label": "flooded"},
    "tokyo":         {"icon": "📡", "title": "Seismic sensor malfunction shuts the Kachidoki Bridge", "desc": "A faulty seismic sensor triggered a false earthquake alert; the Kachidoki Bridge over the Sumida was automatically shut and awaits inspection.", "label": "closed"},
    "chicago":       {"icon": "🌉", "title": "DuSable Bridge stuck open",                     "desc": "A bascule-lift malfunction during a boat run has left the DuSable Bridge jammed upright over the Chicago River.", "label": "stuck open"},
    "minneapolis":   {"icon": "❄️", "title": "Polar vortex ices the Hennepin Avenue Bridge",   "desc": "A polar vortex has glazed the Hennepin Avenue Bridge deck in black ice. Public Works closed the Mississippi crossing until the salt trucks win.", "label": "iced over"},
    "new_york":      {"icon": "🎬", "title": "Film shoot traps the Brooklyn Bridge",           "desc": "A movie shoot overran and a stunt car is blocking the span. The East River crossing is shut until the crew clears.", "label": "filming"},
}

_AREAS = [
    "Downtown", "Riverside", "Old Town", "Harbour", "Uptown", "Midtown",
    "North End", "South Side", "East Bank", "West Bank", "Market", "Station",
    "Garden District", "Parkside", "Hillside", "Bayview", "Central", "Latin Quarter",
    "Heights", "Wharf", "The Commons", "Terrace", "Crossing", "Grand Plaza",
]

# Clustered on the north/west bank within ~2.7 km of the Ponte 25 de Abril
# landing so the operator never has to pan away from the catastrophe bridge.
# Every point clears the Tagus (OSRM nearest < 120 m). Mirror of
# data/canonical/catastrophe_scenarios.py:LISBON_KITCHENS.
_LISBON_KITCHENS: list[tuple[str, str, float, float]] = [
    ("alcantara", "Alcântara", 38.7048, -9.1760),
    ("santo-amaro", "Santo Amaro", 38.7035, -9.1795),
    ("alto-santo-amaro", "Alto de Santo Amaro", 38.7090, -9.1820),
    ("doca", "Doca de Alcântara", 38.7042, -9.1735),
    ("alcantara-terra", "Alcântara-Terra", 38.7080, -9.1745),
    ("ajuda", "Ajuda", 38.7100, -9.1955),
    ("calcada-ajuda", "Calçada da Ajuda", 38.7120, -9.1900),
    ("belem", "Belém", 38.6985, -9.2010),
    ("junqueira", "Junqueira", 38.7008, -9.1930),
    ("restelo", "Restelo", 38.7045, -9.2075),
    ("estrela", "Estrela", 38.7135, -9.1615),
    ("lapa", "Lapa", 38.7085, -9.1660),
    ("santos", "Santos", 38.7075, -9.1585),
    ("madragoa", "Madragoa", 38.7095, -9.1560),
    ("campo-ourique", "Campo de Ourique", 38.7175, -9.1670),
    ("prazeres", "Prazeres", 38.7150, -9.1720),
    ("amoreiras", "Amoreiras", 38.7230, -9.1625),
    ("rato", "Rato", 38.7195, -9.1545),
    ("campolide", "Campolide", 38.7260, -9.1640),
    ("sao-bento", "São Bento", 38.7125, -9.1545),
    ("alto-alcantara", "Alto de Alcântara", 38.7160, -9.1755),
    ("tapada", "Tapada da Ajuda", 38.7085, -9.1860),
    ("pilar7", "Pilar 7", 38.7018, -9.1772),
    ("casal-ventoso", "Casal Ventoso", 38.7098, -9.1785),
]


def _meters_per_deg(lat: float) -> tuple[float, float]:
    return 111320.0, 111320.0 * math.cos(math.radians(lat))


def _offset(point: tuple[float, float], east_m: float, north_m: float) -> tuple[float, float]:
    mlat, mlon = _meters_per_deg(point[0])
    return (point[0] + north_m / mlat, point[1] + east_m / mlon)


def _across_river_unit(bridge: tuple[float, float], alt: tuple[float, float]) -> tuple[float, float]:
    mlat, mlon = _meters_per_deg(bridge[0])
    dx = (alt[1] - bridge[1]) * mlon
    dy = (alt[0] - bridge[0]) * mlat
    length = math.hypot(dx, dy)
    if length < 1.0:
        return 1.0, 0.0
    return -dy / length, dx / length


def _city_config_for(city_id: str) -> dict[str, Any]:
    """Full geography config for a city id, derived from its two crossings."""
    city = _CITIES.get(city_id)
    if city is None:
        return _city_config()
    bridge, alt = city["bridge"], city["alt"]
    center = ((bridge[0] + alt[0]) / 2.0, (bridge[1] + alt[1]) / 2.0)
    px, py = _across_river_unit(bridge, alt)
    region_a = _offset(bridge, px * 1300, py * 1300)
    region_b = _offset(bridge, -px * 1300, -py * 1300)
    approach_a = _offset(bridge, px * 220, py * 220)
    approach_b = _offset(bridge, -px * 220, -py * 220)
    return {
        "id": city_id,
        "name": city["name"],
        "flag": city["flag"],
        "river": city["river"],
        "center": [center[0], center[1]],
        "bridge": {"name": city["bridge_name"], "coord": [bridge[0], bridge[1]]},
        "alt": {"name": city["alt_name"], "coord": [alt[0], alt[1]]},
        "regions": {"a": [region_a[0], region_a[1]], "b": [region_b[0], region_b[1]]},
        "approaches": {"a": [approach_a[0], approach_a[1]], "b": [approach_b[0], approach_b[1]]},
        "close_radius_m": city["r"],
        "catastrophe": _CITY_CATASTROPHES.get(city_id, _CITY_CATASTROPHES["amsterdam"]),
    }


def _generate_city_kitchens(city_id: str, *, count: int = 24) -> list[dict[str, Any]]:
    """Fallback kitchens when the catastrophe_kitchens table has no usable rows.

    Lisbon uses curated land-based neighborhoods; other cities use a
    deterministic spread that the client snaps to roads on load.
    """
    if city_id == "lisbon":
        return [
            {
                "kitchen_id": f"lis-{slug}",
                "name": f"Casper's {neighborhood}",
                "neighborhood": neighborhood,
                "city": _CITIES["lisbon"]["name"],
                "location_id": 0,
                "lat": lat,
                "lon": lon,
                "address": "",
            }
            for slug, neighborhood, lat, lon in _LISBON_KITCHENS[:count]
        ]
    city = _CITIES.get(city_id)
    if city is None:
        return []
    cfg = _city_config_for(city_id)
    center = cfg["center"]
    rng = random.Random(f"{city_id}-{CATASTROPHE_SEED}")
    rows: list[dict[str, Any]] = []
    for i in range(count):
        angle = rng.uniform(0, 2 * math.pi)
        dist = 2200.0 * math.sqrt(rng.random())
        lat, lon = _offset((center[0], center[1]), dist * math.cos(angle), dist * math.sin(angle))
        area = _AREAS[i % len(_AREAS)]
        rows.append({
            "kitchen_id": f"{city_id}-{i + 1:02d}", "name": f"Casper's {area}",
            "neighborhood": area, "city": city["name"], "location_id": 0,
            "lat": round(lat, 6), "lon": round(lon, 6), "address": "",
        })
    return rows

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
            MERGE INTO {CATALOG}.{SIMULATOR_SCHEMA}.demo_active_city AS t
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
    return HTMLResponse(
        INDEX_HTML_TEXT,
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"},
    )


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "catalog": CATALOG}


@app.get("/api/cities")
def cities_api() -> list[dict[str, Any]]:
    """Selectable cities for the config screen (registry order, active first)."""
    active = CITY if CITY in _CITIES else "amsterdam"
    ordered = [active] + [cid for cid in _CITIES if cid != active]
    return [{"id": cid, "name": _CITIES[cid]["name"], "flag": _CITIES[cid]["flag"],
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
    if cid and cid in _CITIES:
        return _city_config_for(cid)
    return _city_config()


@app.get("/api/kitchens")
def kitchens_api(city: str = Query(default="")) -> list[dict[str, Any]]:
    """Ghost-kitchen locations for a city from the catastrophe_kitchens table.
    With no `city`, uses the deploy-time active city. Falls back to a generated
    spread if the table has no rows for the requested city (e.g. the stage was
    last materialized for a different city)."""
    cid = city.strip().lower()
    # Lisbon is fully curated in code (clustered near Ponte 25 de Abril, all on
    # land). The catastrophe_kitchens table may still hold an earlier spread-out
    # set, so always serve the curated list and skip the table for Lisbon.
    if cid == "lisbon":
        return _generate_city_kitchens("lisbon")
    city_name = _CITIES[cid]["name"] if cid and cid in _CITIES else CITY_NAME
    rows = _query(
        f"""
        SELECT kitchen_id, name, neighborhood, city, location_id, lat, lon, address
        FROM {CATALOG}.{SIMULATOR_SCHEMA}.catastrophe_kitchens
        WHERE city = '{_sql_str(city_name)}'
        ORDER BY kitchen_id
        """
    )
    if not rows and cid and cid in _CITIES:
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
