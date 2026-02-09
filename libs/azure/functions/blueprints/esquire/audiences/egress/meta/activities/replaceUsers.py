from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from azure.durable_functions import Blueprint
from facebook_business.exceptions import FacebookRequestError
from facebook_business.adobjects.customaudience import CustomAudience

from libs.azure.functions.blueprints.esquire.audiences.egress.meta.utils import (
    initialize_facebook_api,
)
from libs.data import from_bind

bp = Blueprint()

# Fetch in small chunks to avoid transient large row lists (peak memory).
_DB_FETCH_CHUNK = 256


def _sql_escape_single_quotes(value: Any) -> str:
    """
    Safely escape single quotes for string interpolation in SQL literals.
    (Used for OPENROWSET path/data source pieces which cannot be parameterized.)
    """
    return str(value).replace("'", "''")


def _build_paged_sql(ingress: Dict[str, Any]) -> Tuple[str, int, int]:
    """
    Builds a deterministic, paged SQL statement using caller-provided base SQL
    and the batch_seq/batch_size window.
    Returns: (paged_sql, offset, limit)
    """
    batch = ingress["batch"]
    page_index_zero_based = max(int(batch["batch_seq"]) - 1, 0)
    limit = int(ingress["batch_size"])
    offset = page_index_zero_based * limit

    # Caller-provided query contains positional {} slots for:
    #   0: container_name, 1: blob_prefix, 2: data_source
    base_sql = ingress["sql"]["query"].format(
        _sql_escape_single_quotes(ingress["destination"]["container_name"]),
        _sql_escape_single_quotes(ingress["destination"]["blob_prefix"]),
        _sql_escape_single_quotes(ingress["destination"]["data_source"]),
    )

    # Ensure we don't accidentally double-terminate.
    base_sql = base_sql.strip().rstrip(";")

    # NOTE: The base SQL MUST include a deterministic ORDER BY for stable paging.
    # OFFSET/FETCH must come *after* ORDER BY in T-SQL.
    paged_sql = (
        f"{base_sql}\n"
        f"OFFSET {offset} ROWS\n"
        f"FETCH NEXT {limit} ROWS ONLY;\n"
    )
    return paged_sql, offset, limit


def _fetch_users_page_low_memory(
    ingress: Dict[str, Any],
) -> Tuple[Optional[List[str]], Optional[Dict[str, Any]], int, int]:
    """
    Fetch up to `batch_size` MAIDs from SQL with minimal memory overhead.
    Returns: (users or None, error or None, offset, limit)
    """
    paged_sql, offset, limit = _build_paged_sql(ingress)

    provider = from_bind(ingress["sql"]["bind"])
    db_session = provider.connect()

    users: List[str] = []
    users_append = users.append

    try:
        sa_conn = db_session.connection()

        # Prefer DB-API cursor if exposed (pyodbc path) to avoid SQLAlchemy row buffering.
        raw_conn = getattr(sa_conn, "connection", None)
        cursor_factory = getattr(raw_conn, "cursor", None)

        if callable(cursor_factory):
            cur = raw_conn.cursor()
            try:
                # arraysize influences how many rows the driver buffers per roundtrip.
                try:
                    cur.arraysize = _DB_FETCH_CHUNK
                except Exception:
                    pass

                cur.execute(paged_sql)

                # Fetch in small chunks to avoid large transient lists of row objects.
                while True:
                    chunk = cur.fetchmany(_DB_FETCH_CHUNK)
                    if not chunk:
                        break

                    for row in chunk:
                        # row is usually a tuple-like; first column is deviceid
                        val = row[0] if row else None
                        if val:
                            users_append(str(val))

                    # Defensive: enforce limit even if SQL paging misbehaves.
                    if len(users) >= limit:
                        break
            finally:
                try:
                    cur.close()
                except Exception:
                    pass
        else:
            # Fallback: SQLAlchemy exec with streaming enabled.
            result = sa_conn.execution_options(stream_results=True).exec_driver_sql(paged_sql)
            try:
                for row in result:
                    val = row[0]
                    if val:
                        users_append(str(val))
                    if len(users) >= limit:
                        break
            finally:
                try:
                    result.close()
                except Exception:
                    pass

    except Exception as e:
        return None, {"error": {"source": "sql", "message": str(e)}}, offset, limit
    finally:
        try:
            db_session.close()
        except Exception:
            pass

    # Ensure we never exceed `limit` even if we defensively broke late.
    if len(users) > limit:
        del users[limit:]

    return users, None, offset, limit


def _meta_users_replace(ingress: Dict[str, Any], users: List[str]) -> Dict[str, Any]:
    """
    Calls Meta usersreplace with stable session semantics using the SDK's adobject method.
    This path is known-good in your environment (unlike api.call which was missing scheme).
    """
    api = initialize_facebook_api(ingress)
    audience_id = ingress["audience"]["audience"]

    # Build params with minimal extra copying.
    params = {
        "payload": {
            "schema": CustomAudience.Schema.mobile_advertiser_id,
            "data": users,
        },
        "session": ingress["batch"],
    }

    result = CustomAudience(fbid=audience_id, api=api).create_users_replace(params=params)

    # Response is small; exporting is fine and keeps the activity output JSON-serializable.
    return result.export_all_data()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceMeta_customAudience_replaceUsers(ingress: dict):
    """
    Sends a deterministic REPLACE batch to Meta using the usersreplace endpoint,
    using a low-memory fetch from the database.

    Memory reductions:
      - Fetch DB rows in small chunks (avoid big transient row lists).
      - Close DB resources before calling Meta.
      - Clear the users list immediately after the request.
    """
    users, db_error, offset, limit = _fetch_users_page_low_memory(ingress)

    if db_error is not None:
        return db_error

    assert users is not None

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
        return _meta_users_replace(ingress, users)
    except FacebookRequestError as e:
        body = e.body() if hasattr(e, "body") else {"error": {"message": str(e)}}
        return {"error": body}
    finally:
        # Drop the largest per-invocation structure ASAP.
        try:
            users.clear()
        except Exception:
            pass
