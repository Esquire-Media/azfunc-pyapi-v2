# File: /libs/azure/functions/blueprints/esquire/audiences/egress/meta/activities/forceEndSession.py

from azure.durable_functions import Blueprint
from facebook_business.adobjects.customaudience import CustomAudience
from facebook_business.exceptions import FacebookRequestError
from libs.azure.functions.blueprints.esquire.audiences.egress.meta.utils import (
    initialize_facebook_api,
)
import uuid
from typing import Any, Dict

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceMeta_customAudienceSession_forceEnd(ingress: Dict[str, Any]):
    """
    Attempts to deterministically force-complete a stuck REPLACE session by sending a tiny final batch.

    Expects ingress like:
    {
      "audience": { "audience": "<meta_audience_id>", ... },
      "batch": {
        "session_id": "<existing session id>",
        "estimated_num_total": <int>,  # >= num_received + 1
        "batch_seq": <int>,            # next expected batch seq (1-based) for this REPLACE session
        "last_batch_flag": True
      },
      ... credentials ...
    }

    Strategy:
      - Try usersreplace with provided session payload (no extra math here).
      - If FB returns 2650/1870147 ("first batch_seq was not detected"), fall back to batch_seq=1
        for this session to deterministically "start-and-end" the REPLACE sequence.
      - Never raise; always return a structured dict so the orchestrator can continue deterministically.
    """
    api = initialize_facebook_api(ingress)
    audience_id = ingress["audience"]["audience"]
    session_payload = ingress["batch"]

    def _call_replace(session_obj: Dict[str, Any]) -> Dict[str, Any]:
        return (
            CustomAudience(fbid=audience_id, api=api)
            .create_users_replace(
                params={
                    "payload": {
                        "schema": CustomAudience.Schema.mobile_advertiser_id,
                        # Single dummy MAID is sufficient to close out a REPLACE session.
                        "data": [str(uuid.uuid4())],
                    },
                    "session": session_obj,
                }
            )
            .export_all_data()
        )

    try:
        return _call_replace(session_payload)
    except FacebookRequestError as e:
        body = e.body() if hasattr(e, "body") else {"error": {"message": str(e)}}
        err = body.get("error", {}) if isinstance(body, dict) else {}
        code = err.get("code")
        sub = err.get("error_subcode")

        # 2650/1870147: "Invalid Upload Batch for Replace" – first batch_seq was not detected
        if code == 2650 and sub == 1870147:
            fallback_session = dict(session_payload)
            fallback_session["batch_seq"] = 1
            # Make sure totals are >= 1 and we keep last flag
            fallback_session["estimated_num_total"] = max(
                int(fallback_session.get("estimated_num_total", 1)), 1
            )
            fallback_session["last_batch_flag"] = True

            try:
                result = _call_replace(fallback_session)
                return {
                    "forced_end": True,
                    "fallback_to_seq_1": True,
                    "original_error": body,
                    "result": result,
                }
            except FacebookRequestError as e2:
                return {
                    "forced_end": False,
                    "fallback_to_seq_1": True,
                    "original_error": body,
                    "error": getattr(e2, "body", lambda: {"error": {"message": str(e2)}})(),
                }

        # For any other FB error, return it without throwing to keep orchestrator deterministic.
        return {"forced_end": False, "error": body}
