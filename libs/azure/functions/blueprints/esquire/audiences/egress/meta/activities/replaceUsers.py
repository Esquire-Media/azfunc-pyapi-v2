# File: /libs/azure/functions/blueprints/esquire/audiences/egress/meta/activities/replaceUsers.py

from azure.durable_functions import Blueprint
from facebook_business.exceptions import FacebookRequestError
from facebook_business.adobjects.customaudience import CustomAudience
from libs.azure.functions.blueprints.esquire.audiences.egress.meta.utils import (
    initialize_facebook_api,
)
from libs.data import from_bind
import pandas as pd

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceMeta_customAudience_replaceUsers(ingress: dict):
    """
    Sends a deterministic REPLACE batch to Meta using the usersreplace endpoint.

    Why idempotent:
      * Stable paging: deterministic ORDER BY deviceid with fixed batch_seq => stable page composition.
      * Using usersreplace + stable session_id/batch_seq prevents duplicates on replay.
      * Empty pages (possible due to DISTINCT) skip the API call.

    Expected ingress:
      - "audience": { "audience": "<Meta CA id>", ... }
      - "sql": { "bind": "audiences", "query": "<SELECT DISTINCT deviceid ...>" }
      - "batch": { "session_id": int, "estimated_num_total": int, "batch_seq": int (1-based), "last_batch_flag": bool }
      - "batch_size": int
      - optional Meta credentials (or via env)
    """
    # Convert 1-based FB session sequence to 0-based SQL OFFSET
    page_index_zero_based = max(int(ingress["batch"]["batch_seq"]) - 1, 0)
    offset = page_index_zero_based * int(ingress["batch_size"])
    limit = int(ingress["batch_size"])

    # Build the paged, deterministic SQL
    query = """
        {}
        ORDER BY deviceid
        OFFSET {} ROWS
        FETCH NEXT {} ROWS ONLY
    """.format(
        ingress["sql"]["query"].format(
            ingress["destination"]["container_name"],
            ingress["destination"]["blob_prefix"],
            ingress["destination"]["data_source"],
        ),
        offset,
        limit,
    )

    # Read page deterministically and cleanup resources
    provider = from_bind(ingress["sql"]["bind"])
    session = provider.connect()
    try:
        conn = session.connection()
        df = pd.read_sql(query, conn)
    finally:
        try:
            session.close()
        except Exception:
            pass

    users = (
        df["deviceid"].astype(str).str.lower().tolist()
        if isinstance(df, pd.DataFrame) and not df.empty
        else []
    )

    # If this page is empty, do not call the API (prevents (#100) and ensures idempotence)
    if not users:
        return {
            "skipped": True,
            "reason": "Empty batch; no users for this page.",
            "batch": ingress["batch"],
            "offset": offset,
            "limit": limit,
        }

    try:
        # Use usersreplace (REPLACE semantics) with stable session object
        result = (
            CustomAudience(
                fbid=ingress["audience"]["audience"],
                api=initialize_facebook_api(ingress),
            )
            .create_users_replace(
                params={
                    "payload": {
                        "schema": CustomAudience.Schema.mobile_advertiser_id,
                        "data": users,
                    },
                    "session": ingress["batch"],
                }
            )
            .export_all_data()
        )
        return result
    except FacebookRequestError as e:
        # Return structured error (deterministic for orchestrator)
        body = e.body() if hasattr(e, "body") else {"error": {"message": str(e)}}
        return {"error": body}
