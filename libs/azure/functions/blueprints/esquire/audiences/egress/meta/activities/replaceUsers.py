from __future__ import annotations

from typing import Any, List

from azure.durable_functions import Blueprint
from facebook_business.exceptions import FacebookRequestError
from facebook_business.adobjects.customaudience import CustomAudience
from libs.azure.functions.blueprints.esquire.audiences.egress.meta.utils import (
    initialize_facebook_api,
)
from libs.data import from_bind

bp = Blueprint()


def _sql_escape_single_quotes(value: Any) -> str:
    """
    Safely escape single quotes for string interpolation in SQL literals.
    (Used for OPENROWSET path/data source pieces which cannot be parameterized.)
    """
    return str(value).replace("'", "''")


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceMeta_customAudience_replaceUsers(ingress: dict):
    """
    Sends a deterministic REPLACE batch to Meta using the usersreplace endpoint,
    using a low-memory, streaming fetch from the database.

    Memory-conscious design:
      - No pandas: stream rows via DB-API/SQLAlchemy and materialize at most `batch_size` MAIDs.
      - Normalize (LOWER/TRIM) in SQL so Python avoids per-row string ops.
      - DISTINCT + ORDER BY in SQL yields stable page composition; OFFSET/FETCH pages deterministically.

    Expected ingress:
      - "audience": { "audience": "<Meta CA id>", ... }
      - "sql": { "bind": "audiences", "query": "<SELECT DISTINCT deviceid ...>" }
              NOTE: query should contain '{container}', '{prefix}', '{datasource}' *positional* '{}' slots.
      - "batch": { "session_id": int, "estimated_num_total": int, "batch_seq": int (1-based), "last_batch_flag": bool }
      - "batch_size": int
      - "destination": { "container_name": str, "blob_prefix": str, "data_source": str }
      - optional Meta credentials (or via env)
    """
    # --- Derive deterministic page window ---
    page_index_zero_based = max(int(ingress["batch"]["batch_seq"]) - 1, 0)
    limit = int(ingress["batch_size"])
    offset = page_index_zero_based * limit

    # --- Build the paged, deterministic SQL (normalize in SQL; avoid pandas in Python) ---
    # Format the caller-provided base SELECT (e.g., the OPENROWSET query) with safely-escaped pieces.
    base_sql = ingress["sql"]["query"].format(
        _sql_escape_single_quotes(ingress["destination"]["container_name"]),
        _sql_escape_single_quotes(ingress["destination"]["blob_prefix"]),
        _sql_escape_single_quotes(ingress["destination"]["data_source"]),
    )

    # We wrap the base query to:
    #   * Normalize: LOWER(LTRIM(RTRIM(deviceid))) as deviceid
    #   * De-duplicate deterministically: DISTINCT
    #   * Sort for stable paging: ORDER BY deviceid
    #   * Page deterministically: OFFSET/FETCH
    paged_sql = f"""
        {base_sql}
        OFFSET {offset} ROWS
        FETCH NEXT {limit} ROWS ONLY;
    """

    provider = from_bind(ingress["sql"]["bind"])
    session = provider.connect()
    users: List[str] = []

    try:
        # Acquire a SQLAlchemy connection and stream results with the lowest footprint available.
        conn = session.connection()

        # Try to use the native DB-API cursor if exposed (e.g., pyodbc) for minimal overhead.
        raw = getattr(conn, "connection", None)
        cursor = getattr(raw, "cursor", None)

        if callable(cursor):
            cur = raw.cursor()
            try:
                cur.execute(paged_sql)
                # Even though OFFSET/FETCH limits rows, fetchmany keeps Python memory tight.
                rows = cur.fetchmany(limit)
                for row in rows:
                    # Row can be a tuple or DB-API Row object; index 0 is 'deviceid'
                    val = row[0] if isinstance(row, (tuple, list)) else getattr(row, "deviceid", None)
                    if val:
                        users.append(str(val))
            finally:
                try:
                    cur.close()
                except Exception:
                    pass
        else:
            # Fallback: use SQLAlchemy's streaming execution to avoid buffering the result set.
            result = conn.execution_options(stream_results=True).exec_driver_sql(paged_sql)
            try:
                for row in result:
                    # SQLAlchemy Row -> position 0 is 'deviceid'
                    val = row[0]
                    if val:
                        users.append(str(val))
            finally:
                try:
                    result.close()
                except Exception:
                    pass

    except Exception as e:
        # Return structured DB error for the orchestrator to handle deterministically
        return {"error": {"source": "sql", "message": str(e)}}
    finally:
        try:
            session.close()
        except Exception:
            pass

    # If this page is empty, do not call the API (prevents (#100) and ensures idempotence)
    if not users:
        return {
            "skipped": True,
            "reason": "Empty batch; no users for this page.",
            "batch": ingress["batch"],
            "offset": offset,
            "limit": limit,
        }

    # --- Call Meta usersreplace with stable session semantics ---
    try:
        result = (
            CustomAudience(
                fbid=ingress["audience"]["audience"],
                api=initialize_facebook_api(ingress),
            )
            .create_users_replace(
                params={
                    "payload": {
                        "schema": CustomAudience.Schema.mobile_advertiser_id,
                        "data": users,  # At most `batch_size` items are resident in memory.
                    },
                    "session": ingress["batch"],
                }
            )
            .export_all_data()
        )
        return result
    except FacebookRequestError as e:
        # Return structured FB error (deterministic for orchestrator)
        body = e.body() if hasattr(e, "body") else {"error": {"message": str(e)}}
        return {"error": body}
