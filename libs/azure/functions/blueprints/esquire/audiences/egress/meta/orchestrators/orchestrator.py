# File path: /libs/azure/functions/blueprints/esquire/audiences/egress/meta/orchestrator.py

from datetime import timedelta
import hashlib
import math
from typing import Any, Dict, List
from azure.durable_functions import Blueprint, DurableOrchestrationContext

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def meta_customaudience_orchestrator(context: DurableOrchestrationContext):
    """
    Orchestrates managing & updating a Meta custom audience (create/get/update + replace users).
    Deterministic + idempotent by construction.
    """
    batch_size = 10_000
    ingress: Dict[str, Any] = context.get_input()

    # Deterministic session id for this orchestration instance.
    sid_bytes = hashlib.sha256(context.instance_id.encode("utf-8")).digest()[:8]
    session_id = (int.from_bytes(sid_bytes, "big") % ((1 << 63) - 1)) + 1

    # 1) Fetch ESQ audience definition
    try:
        audience = yield context.call_activity(
            "activity_esquireAudienceMeta_fetchAudience",
            ingress["audience"]["id"],
        )
        ingress["audience"].update(audience)
        ingress["audience"]["name"] = (
            " - ".join(ingress["audience"]["tags"])
            if len(ingress["audience"]["tags"])
            else ingress["audience"]["id"]
        )
    except Exception:
        # If not found or inactive, terminate deterministically with an empty result.
        return {}

    # 2) Ensure a Meta Custom Audience exists and is named as desired
    if not ingress["audience"]["audience"]:
        custom_audience = yield context.call_activity(
            "activity_esquireAudienceMeta_customAudience_create",
            ingress,
        )
        # Persist new Meta Audience ID back to ESQ
        yield context.call_activity(
            "activity_esquireAudienceMeta_putAudience",
            {
                "audience": ingress["audience"]["id"],
                "metaAudienceId": custom_audience["id"],
            },
        )
    else:
        custom_audience = yield context.call_activity(
            "activity_esquireAudienceMeta_customAudience_get",
            ingress,
        )

    if (
        custom_audience.get("name") != ingress["audience"]["name"]
        or custom_audience.get("description") != ingress["audience"]["id"]
    ):
        custom_audience = yield context.call_activity(
            "activity_esquireAudienceMeta_customAudience_update",
            ingress,
        )

    # 3) If audience is "Updating", try to deterministically close out any stuck sessions
    op = custom_audience.get("operation_status") or {}
    if op.get("code") in (300, 414):  # Updating / Busy
        sessions: List[Dict[str, Any]] = yield context.call_activity(
            "activity_esquireAudienceMeta_customAudienceSessions_get",
            ingress,
        )

        # Sort deterministically for replay consistency: by num_received desc, then session_id
        def _num_received(s):
            try:
                return int(s.get("num_received", "0"))
            except Exception:
                return 0

        sessions = sorted(
            sessions,
            key=lambda s: (_num_received(s), str(s.get("session_id", ""))),
            reverse=True,
        )

        for s in sessions:
            if s.get("stage") != "uploading":
                continue

            num_received = _num_received(s)
            # Next REPLACE batch we will send to close the session:
            # Use ceil(num_received / batch_size) + 1 to send a definitive "last" batch
            # (if num_received == 0 -> 1)
            closing_seq = ((num_received + batch_size - 1) // batch_size) + 1
            closing_seq = max(closing_seq, 1)

            force_payload = {
                **ingress,
                "batch": {
                    "session_id": s["session_id"],
                    "estimated_num_total": max(num_received + 1, 1),
                    "batch_seq": closing_seq,           # IMPORTANT: no extra +1 in the activity
                    "last_batch_flag": True,
                },
            }

            # Never let exceptions bubble up (keeps orchestration deterministic).
            _ = yield context.call_activity(
                "activity_esquireAudienceMeta_customAudienceSession_forceEnd",
                force_payload,
            )

        # Re-fetch status after attempting closures (still deterministic because it's an activity result)
        custom_audience = yield context.call_activity(
            "activity_esquireAudienceMeta_customAudience_get",
            ingress,
        )

    # Refresh Meta Audience ID on ingress (from create/get)
    ingress["audience"]["audience"] = custom_audience["id"]

    # 4) Determine newest data prefix (blob folder)
    ingress["destination"]["blob_prefix"] = yield context.call_activity(
        "activity_esquireAudiencesUtils_newestAudienceBlobPrefix",
        {
            "conn_str": ingress["destination"]["conn_str"],
            "container_name": ingress["destination"]["container_name"],
            "audience_id": ingress["audience"]["id"],
        },
    )

    # 5) Count distinct MAIDs
    response = yield context.call_activity(
        "activity_synapse_query",
        {
            "bind": "audiences",
            "query": """
                SELECT 
                    COUNT(DISTINCT deviceid) AS [count]
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

    # 6) Upload users via REPLACE in deterministic batches
    for sequence, _ in enumerate(range(0, total, batch_size)):
        is_last = (sequence + 1) == math.ceil(total / batch_size)

        session_payload = {
            "session_id": session_id,
            "estimated_num_total": total,
            "batch_seq": sequence + 1,  # 1-based for FB session API
            "last_batch_flag": is_last,
        }

        while True:
            context.set_custom_status("Adding users to Meta Audience.")
            session = yield context.call_activity(
                "activity_esquireAudienceMeta_customAudience_replaceUsers",
                {
                    **ingress,
                    "sql": {
                        "bind": "audiences",
                        "query": """
                            SELECT DISTINCT deviceid
                            FROM OPENROWSET(
                                BULK '{}/{}/*',
                                DATA_SOURCE = '{}',  
                                FORMAT = 'CSV',
                                PARSER_VERSION = '2.0',
                                HEADER_ROW = TRUE
                            ) WITH (
                                deviceid VARCHAR(80)
                            ) AS [data]
                            WHERE LEN(deviceid) = 36
                        """,
                    },
                    "batch": session_payload,
                    "batch_size": batch_size,
                },
            )

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
                # Any other error: fail the orchestration (deterministically)
                raise Exception(session["error"])
            break

    return session
