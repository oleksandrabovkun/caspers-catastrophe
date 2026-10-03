"""Create/reuse the Unity Gateway model service for the in-app agent.

Model services are Unity Catalog securables. The catalog is created at job
runtime, so this cannot live in the bundle — ``stages/catastrophe_command``
calls ``ensure_model_service`` after the catalog exists.

REST: ``POST /api/2.1/unity-catalog/model-services``. Get/delete also try
``w.ai_gateway`` when the installed ``databricks-sdk`` has it.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

MODEL_SERVICES_PATH = "/api/2.1/unity-catalog/model-services"

# Pay-per-token destination. ``models/`` is the UC registered-model resource
# name; system-provided FMs live in ``system.ai``.
DEFAULT_FOUNDATION_MODEL = "models/system.ai.databricks-claude-haiku-4-5"
HAIKU_MODEL_CANDIDATES = (
    DEFAULT_FOUNDATION_MODEL,
    "models/system.ai.claude-haiku-4-5",
    "models/system.ai.databricks-claude-3-5-haiku",
)
# Service-wide cap so the Demo 2 cost/rate-limit story is real out of the box.
DEFAULT_REQUESTS_PER_MINUTE = 60


def parse_model_service_fqn(fqn: str) -> tuple[str, str, str]:
    parts = [p.strip() for p in (fqn or "").split(".")]
    if len(parts) != 3 or not all(parts):
        raise ValueError(
            f"Model service name must be catalog.schema.name, got {fqn!r}"
        )
    return parts[0], parts[1], parts[2]


def _resource_name(fqn: str) -> str:
    return fqn if fqn.startswith("model-services/") else f"model-services/{fqn}"


def _is_not_found(exc: BaseException) -> bool:
    if type(exc).__name__ in {"NotFound", "ResourceDoesNotExist"}:
        return True
    msg = str(exc).lower()
    return any(
        s in msg
        for s in ("not found", "does not exist", "no such", "404", "not_found")
    )


def _is_already_exists(exc: BaseException) -> bool:
    if type(exc).__name__ in {"AlreadyExists", "ResourceAlreadyExists"}:
        return True
    msg = str(exc).lower()
    return "already exists" in msg or "already_exists" in msg


def _get_via_sdk(w, fqn: str) -> dict[str, Any] | None:
    gw = getattr(w, "ai_gateway", None)
    get_fn = getattr(gw, "get_model_service", None) if gw is not None else None
    if get_fn is None:
        return None
    for name in (_resource_name(fqn), fqn):
        try:
            obj = get_fn(name=name)
            if hasattr(obj, "as_dict"):
                return obj.as_dict()
            return {"name": fqn}
        except Exception as e:
            if _is_not_found(e):
                continue
            raise
    return None


def _get_via_rest(w, fqn: str) -> dict[str, Any] | None:
    for path in (
        f"{MODEL_SERVICES_PATH}/{fqn}",
        f"{MODEL_SERVICES_PATH}/{_resource_name(fqn)}",
    ):
        try:
            return w.api_client.do("GET", path) or {"name": fqn}
        except Exception as e:
            if _is_not_found(e):
                continue
            raise
    return None


def get_model_service(w, fqn: str) -> dict[str, Any] | None:
    try:
        found = _get_via_sdk(w, fqn)
        if found is not None:
            return found
    except Exception:
        pass
    return _get_via_rest(w, fqn)


def _create_body(
    *,
    comment: str,
    foundation_model: str,
    requests_per_minute: int,
) -> dict[str, Any]:
    model = foundation_model
    if not model.startswith("models/"):
        model = f"models/{model}"
    return {
        "comment": comment,
        "config": {
            "routing": {
                "destinations": [
                    {
                        "name": "primary",
                        "destination_type": (
                            "DESTINATION_TYPE_PAY_PER_TOKEN_FOUNDATION_MODEL"
                        ),
                        "pay_per_token_config": {"model": model},
                        "traffic_percentage": 100,
                    }
                ]
            },
            "rate_limits": [
                {
                    "key": "RATE_LIMIT_KEY_SERVICE",
                    "renewal_period": "RATE_LIMIT_RENEWAL_PERIOD_MINUTE",
                    "requests": int(requests_per_minute),
                }
            ],
        },
    }


def _create_via_rest(w, catalog: str, schema: str, leaf: str, body: dict[str, Any]):
    parent = quote(f"schemas/{catalog}.{schema}", safe="/.")
    leaf_q = quote(leaf, safe="-._")
    path = f"{MODEL_SERVICES_PATH}?parent={parent}&model_service_id={leaf_q}"
    return w.api_client.do("POST", path, body=body)


def _resolve_haiku_model(w, fallback: str) -> str:
    """Prefer a live ``system.ai`` registered model whose name contains haiku."""
    list_fn = getattr(getattr(w, "registered_models", None), "list", None)
    if list_fn is None:
        return fallback
    try:
        for m in list_fn(catalog_name="system", schema_name="ai"):
            full = getattr(m, "full_name", None) or getattr(m, "name", "") or ""
            if "haiku" in full.lower():
                return full if full.startswith("models/") else f"models/{full}"
    except Exception:
        pass
    return fallback


def ensure_model_service(
    w,
    fqn: str,
    *,
    foundation_model: str | None = None,
    requests_per_minute: int = DEFAULT_REQUESTS_PER_MINUTE,
    comment: str = "Casper's Catastrophe command-agent (created by the initializer)",
) -> tuple[dict[str, Any], bool]:
    """Return ``(service, created)``. Idempotent: existing services are reused."""
    catalog, schema, leaf = parse_model_service_fqn(fqn)
    existing = get_model_service(w, fqn)
    if existing is not None:
        return existing, False

    if foundation_model:
        models = [foundation_model]
    else:
        resolved = _resolve_haiku_model(w, DEFAULT_FOUNDATION_MODEL)
        models = [resolved, *[m for m in HAIKU_MODEL_CANDIDATES if m != resolved]]

    last_err: BaseException | None = None
    created: Any = None
    for model in models:
        body = _create_body(
            comment=comment,
            foundation_model=model,
            requests_per_minute=requests_per_minute,
        )
        try:
            created = _create_via_rest(w, catalog, schema, leaf, body)
            last_err = None
            break
        except Exception as e:
            if _is_already_exists(e):
                existing = get_model_service(w, fqn)
                return existing or {"name": fqn}, False
            last_err = e
    if last_err is not None:
        raise last_err

    if hasattr(created, "as_dict"):
        return created.as_dict(), True
    return (created or {"name": fqn}), True


def delete_model_service(w, fqn: str) -> bool:
    """Delete a model service. Returns True if deleted or already gone."""
    parse_model_service_fqn(fqn)
    gw = getattr(w, "ai_gateway", None)
    delete_fn = getattr(gw, "delete_model_service", None) if gw is not None else None
    errors: list[BaseException] = []
    if delete_fn is not None:
        for name in (_resource_name(fqn), fqn):
            try:
                delete_fn(name=name)
                return True
            except Exception as e:
                if _is_not_found(e):
                    return True
                errors.append(e)
    for path in (
        f"{MODEL_SERVICES_PATH}/{fqn}",
        f"{MODEL_SERVICES_PATH}/{_resource_name(fqn)}",
    ):
        try:
            w.api_client.do("DELETE", path)
            return True
        except Exception as e:
            if _is_not_found(e):
                return True
            errors.append(e)
    if errors:
        raise errors[-1]
    return True
