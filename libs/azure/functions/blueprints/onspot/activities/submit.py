from azure.durable_functions import Blueprint
from libs.openapi.clients.onspot import OnSpot
import logging
import json
from typing import Any
from urllib.parse import urlsplit, urlunsplit

bp = Blueprint()
logger = logging.getLogger(__name__)

try:
    from aiopenapi3.errors import RequestError as AioRequestError
except Exception:  # pragma: no cover
    AioRequestError = Exception  # fallback


_REDACT_KEYS = {"sources", "callback", "outputLocation"}


def _strip_query(url: str) -> str:
    try:
        p = urlsplit(str(url))
        return urlunsplit((p.scheme, p.netloc, p.path, "", ""))
    except Exception:
        return str(url)


def _redact(obj: Any) -> Any:
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k in _REDACT_KEYS:
                if isinstance(v, list):
                    out[k] = [_strip_query(x) for x in v]
                else:
                    out[k] = _strip_query(v)
            else:
                out[k] = _redact(v)
        return out
    if isinstance(obj, list):
        return [_redact(x) for x in obj]
    return obj


@bp.activity_trigger(input_name="ingress")
async def onspot_activity_submit(ingress: dict):
    """
    Submits a request to the OnSpotAPI and returns the response.

    This function creates a request for a specific endpoint and HTTP method
    (POST), sends the request, and returns the response.

    Parameters
    ----------
    ingress : dict
        The input for the activity function, including the endpoint and request.

    Returns
    -------
    dict
        The response from the OnSpotAPI as a JSON object.
    """

    op = OnSpot[(ingress["endpoint"], "post")]

    try:
        data = op(ingress["request"])

    except AioRequestError as e:
        # aiopenapi3 typically wraps the real exception in __cause__
        cause = e.__cause__
        resp = getattr(cause, "response", None)

        status = getattr(resp, "status_code", None)
        body = getattr(resp, "text", None)
        url = _strip_query(getattr(getattr(e, "request", None), "url", "unknown"))
        op_id = getattr(getattr(e, "operation", None), "operationId", None)

        logger.exception(
            "OnSpot call failed opId=%s url=%s status=%s response_body=%s request=%s",
            op_id,
            url,
            status,
            (body[:4000] if isinstance(body, str) else None),
            json.dumps(_redact(ingress.get("request")), default=str)[:8000],
        )

        raise RuntimeError(
            f"OnSpot request failed: opId={op_id} POST {url} status={status}. "
            f"Underlying={type(cause).__name__}: {cause}"
        ) from e
    return (
        [d.model_dump() for d in data] if isinstance(data, list) else data.model_dump()
    )
