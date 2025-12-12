from __future__ import annotations

from datetime import timedelta
import hashlib
import math
from typing import Any, Dict, List, Optional

from azure.durable_functions import Blueprint, DurableOrchestrationContext

bp = Blueprint()

# ---- Activity name constants (prevents typos; helps deterministic behavior) ----
ACT_FETCH_AUDIENCE = "activity_esquireAudienceMeta_fetchAudience"
ACT_CREATE_CA = "activity_esquireAudienceMeta_customAudience_create"
ACT_GET_CA = "activity_esquireAudienceMeta_customAudience_get"
ACT_UPDATE_CA = "activity_esquireAudienceMeta_customAudience_update"
ACT_PUT_AUDIENCE = "activity_esquireAudienceMeta_putAudience"
ACT_GET_SESSIONS = "activity_esquireAudienceMeta_customAudienceSessions_get"
ACT_FORCE_END = "activity_esquireAudienceMeta_customAudienceSession_forceEnd"
ACT_NEWEST_PREFIX = "activity_esquireAudiencesUtils_newestAudienceBlobPrefix"
ACT_SYNAPSE_QUERY = "activity_synapse_query"
ACT_REPLACE_USERS = "activity_esquireAudienceMeta_customAudience_replaceUsers"


@bp.orchestration_trigger(context_name="context")
def meta_customaudience_orchestrator(context: DurableOrchestrationContext):
    """
    Orchestrates creating/getting/updating a Meta Custom Audience, and
    REPLACEing users deterministically and idempotently.

    Determinism & idempotence notes:
      * All time-based behavior uses Durable timer on context.current_utc_datetime.
      * All non-deterministic/external effects live in activities.
      * REPLACE uses Meta's usersreplace endpoint with stable session_id + batch_seq.
      * Replays are safe: activities return cached results; loops & sorts are deterministic.
    """
    batch_size = 5_000

    # ---- 0) Validate ingress deterministically ----
    ingress: Dict[str, Any] = context.get_input() or {}
    if not isinstance(ingress, dict):
        return {"error": "Invalid ingress; expected object/dict."}
    ingress = dict(ingress)  # local copy; safe mutations inside orchestrator

    # ---- Deterministic session id for this orchestration instance ----
    sid_bytes = hashlib.sha256(context.instance_id.encode("utf-8")).digest()[:8]
    session_id = (int.from_bytes(sid_bytes, "big") % ((1 << 63) - 1)) + 1

    # ---- 1) Fetch ESQ audience definition (idempotent read) ----
    try:
        audience = yield context.call_activity(ACT_FETCH_AUDIENCE, ingress["audience"]["id"])
        ingress["audience"].update(audience)

        tags = ingress["audience"].get("tags") or []
        if isinstance(tags, list) and len(tags) > 0:
            ingress["audience"]["name"] = " - ".join(tags)
        else:
            ingress["audience"]["name"] = ingress["audience"]["id"]
    except Exception:
        # If not found or inactive, terminate deterministically with an empty result.
        return {}

    # ---- 2) Ensure a Meta Custom Audience exists and is named as desired ----
    custom_audience: Dict[str, Any]
    if not ingress["audience"].get("audience"):
        # No CA id recorded – create once; DF ensures we won't re-run this on replay.
        custom_audience = yield context.call_activity(ACT_CREATE_CA, ingress)
        # Persist new Meta Audience ID back to ESQ
        yield context.call_activity(
            ACT_PUT_AUDIENCE,
            {"audience": ingress["audience"]["id"], "metaAudienceId": custom_audience["id"]},
        )
    else:
        custom_audience = yield context.call_activity(ACT_GET_CA, ingress)
        if type(custom_audience) == dict:
            if "error" in custom_audience.keys():
                return custom_audience

    # If name/description drifted, update deterministically.
    if (
        (custom_audience.get("name") or "") != ingress["audience"]["name"]
        or (custom_audience.get("description") or "") != ingress["audience"]["id"]
    ):
        custom_audience = yield context.call_activity(ACT_UPDATE_CA, ingress)
        if type(custom_audience) == dict:
            if "error" in custom_audience.keys():
                return custom_audience

    # ---- 3) If audience is "Updating", deterministically try to close stuck sessions ----
    op = custom_audience.get("operation_status") or {}
    if op.get("code") in (300, 414):  # Updating / Busy
        sessions: List[Dict[str, Any]] = yield context.call_activity(ACT_GET_SESSIONS, ingress)

        # Sort deterministically for replay consistency: by num_received desc, then session_id asc
        def _num_received(s: Dict[str, Any]) -> int:
            try:
                return int(s.get("num_received", "0"))
            except Exception:
                return 0

        sessions = sorted(
            sessions,
            key=lambda s: (-_num_received(s), str(s.get("session_id", ""))),
        )

        for s in sessions:
            if s.get("stage") != "uploading":
                continue

            num_received = _num_received(s)
            # Next REPLACE batch to close: ceil(num_received / batch_size) + 1
            closing_seq = ((num_received + batch_size - 1) // batch_size) + 1
            closing_seq = max(closing_seq, 1)

            force_payload = {
                **ingress,
                "batch": {
                    "session_id": s["session_id"],
                    "estimated_num_total": max(num_received + 1, 1),
                    "batch_seq": closing_seq,  # IMPORTANT: no extra +1 in the activity
                    "last_batch_flag": True,
                },
            }

            _ = yield context.call_activity(ACT_FORCE_END, force_payload)

        # Re-fetch status after attempting closures (deterministic)
        custom_audience = yield context.call_activity(ACT_GET_CA, ingress)
        if type(custom_audience) == dict:
            if "error" in custom_audience.keys():
                return custom_audience

    # Ensure we have the Meta Audience ID on ingress from create/get
    ingress["audience"]["audience"] = custom_audience["id"]

    # ---- 4) Determine newest data prefix (blob folder) ----
    ingress["destination"]["blob_prefix"] = yield context.call_activity(
        ACT_NEWEST_PREFIX,
        {
            "conn_str": ingress["destination"]["conn_str"],
            "container_name": ingress["destination"]["container_name"],
            "audience_id": ingress["audience"]["id"],
        },
    )

    # ---- 5) Count distinct MAIDs (deterministic read) ----
    response: List[Dict[str, Any]] = yield context.call_activity(
        ACT_SYNAPSE_QUERY,
        {
            "bind": "audiences",
            "query": """
                SELECT COUNT(DISTINCT LOWER(LTRIM(RTRIM(deviceid)))) AS [count]
                FROM OPENROWSET(
                    BULK '{}/{}/*',
                    DATA_SOURCE = '{}',
                    FORMAT = 'CSV',
                    PARSER_VERSION = '2.0',
                    HEADER_ROW = TRUE
                ) AS [data]
                WHERE LEN(deviceid) = 36
            """.format(
                ingress["destination"]["container_name"],
                ingress["destination"]["blob_prefix"],
                ingress["destination"]["data_source"],
            ),
        },
    )
    total = int(response[0]["count"])

    # ---- 6) Upload users via REPLACE in deterministic batches ----
    if total <= 0:
        # Explicit, deterministic outcome when there are no users.
        # We do NOT hit the FB API with an empty dataset.
        return {
            "skipped": True,
            "reason": "No distinct MAIDs to upload.",
            "audience_id": custom_audience["id"],
            "total": 0,
        }

    last_session: Optional[Dict[str, Any]] = None

    for sequence, _ in enumerate(range(0, total, batch_size)):
        is_last = (sequence + 1) == math.ceil(total / batch_size)

        session_payload = {
            "session_id": session_id,
            "estimated_num_total": total,
            "batch_seq": sequence + 1,  # 1-based for FB REPLACE session API
            "last_batch_flag": is_last,
        }

        while True:
            context.set_custom_status("Adding users to Meta Audience (REPLACE).")
            session = yield context.call_activity(
                ACT_REPLACE_USERS,
                {
                    **ingress,
                    "sql": {
                        "bind": "audiences",
                        "query": """
                            SELECT DISTINCT LOWER(LTRIM(RTRIM(deviceid)))
                            FROM OPENROWSET(
                                BULK '{}/{}/*',
                                DATA_SOURCE = '{}',
                                FORMAT = 'CSV',
                                PARSER_VERSION = '2.0',
                                HEADER_ROW = TRUE
                            ) WITH (deviceid VARCHAR(80)) AS [data]
                            WHERE LEN(deviceid) = 36
                            ORDER BY LOWER(LTRIM(RTRIM(deviceid)))
                        """,
                    },
                    "batch": session_payload,
                    "batch_size": batch_size,
                },
            )

            # Some pages can be empty due to DISTINCT and paging math; safe and idempotent.
            if session.get("skipped"):
                last_session = session
                break

            if session.get("error"):
                err = session["error"]
                # If the audience is busy (replace in progress elsewhere), wait deterministically and retry
                if err.get("code") == 2650 and err.get("error_subcode") in (1870145, 1870158):
                    context.set_custom_status(
                        "Waiting for the audience to become Ready before retrying."
                    )
                    yield context.create_timer(
                        context.current_utc_datetime + timedelta(minutes=5)
                    )
                    continue
                # Any other error: fail the orchestration deterministically
                raise Exception(session["error"])

            last_session = session
            break

    return last_session or {
        "result": "replace-complete",
        "audience_id": custom_audience["id"],
        "total": total,
        "note": "No non-skipped page responses were returned.",
    }
