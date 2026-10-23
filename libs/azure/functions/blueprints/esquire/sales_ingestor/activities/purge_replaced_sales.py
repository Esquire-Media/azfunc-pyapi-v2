"""Durable activities for atomic sales-batch replacement.

All tables use fixed, schema-qualified names. The sales ingestor binding
points to the Keystone PostgreSQL database with sales and attribution schemas.
Deletion, replacement metadata, and compensation each use one transaction.
"""

from __future__ import annotations

from contextlib import contextmanager
import logging
from uuid import UUID

from azure.durable_functions import Blueprint
from sqlalchemy import bindparam, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID as PG_UUID

from libs.data import from_bind


bp = Blueprint()
logger = logging.getLogger("salesIngestor.logger")

_UUID_ARRAY = ARRAY(PG_UUID(as_uuid=True))
_UUID = PG_UUID(as_uuid=True)


@contextmanager
def _transaction():
    """Use the registered keystone SQLAlchemy Session and one atomic transaction."""
    provider = from_bind("sales_ingestor_keystone")
    if provider is None:
        raise RuntimeError("The 'sales_ingestor_keystone' data binding is not registered")

    with provider.connect() as session:
        with session.begin():
            yield session.connection()


def _check_attribution_tables(conn) -> None:
    """Fail early if the fixed attribution tables/columns are inaccessible.

    LIMIT 0 inspects metadata only; no attribution records need to exist.
    """
    conn.execute(text("SELECT batch_id FROM attribution.match_relationship LIMIT 0"))
    conn.execute(text("SELECT batch_id FROM attribution.match_geoframe_relationship LIMIT 0"))


def _replacement_ids(settings: dict) -> list[UUID]:
    raw_ids = settings.get("replace_batches")
    if not isinstance(raw_ids, list) or not 0 < len(raw_ids) <= 10:
        raise ValueError("Replacement requires 1–10 batch IDs")

    if not all(isinstance(item, str) for item in raw_ids):
        raise ValueError("Every replacement batch ID must be a UUID string")
    try:
        ids = [UUID(item) for item in raw_ids]
    except ValueError as exc:
        raise ValueError("replace_batches contains an invalid UUID") from exc

    if len(set(ids)) != len(ids):
        raise ValueError("replace_batches must contain unique UUID strings")
    if UUID(str(settings["new_batch_id"])) in ids:
        raise ValueError("A sales batch cannot replace itself")

    return sorted(ids)


def _require_owned_old_batches(conn, ids: list[UUID], tenant_id: str, lock: bool) -> None:
    """Validate target type and tenant; lock old rows during the destructive swap."""
    lock_clause = "FOR UPDATE OF b" if lock else ""
    stmt = text(f"""
        SELECT b.id
        FROM sales.entities AS b
        JOIN sales.entity_types AS et
          ON et.entity_type_id = b.entity_type_id
         AND et.name = 'sales_batch'
        WHERE b.id = ANY(:batch_ids)
          AND EXISTS (
              SELECT 1
              FROM sales.entity_attribute_values AS v
              JOIN sales.attributes AS a
                ON a.id = v.attribute_id
               AND a.entity_type_id = b.entity_type_id
               AND a.name = 'tenant_id'
               AND a.data_type = 'string'
              WHERE v.entity_id = b.id
                AND v.value_string = :tenant_id
          )
        ORDER BY b.id
        {lock_clause}
    """).bindparams(bindparam("batch_ids", type_=_UUID_ARRAY))
    owned = set(conn.execute(
        stmt, {"batch_ids": ids, "tenant_id": str(tenant_id)}
    ).scalars())

    if owned != set(ids):
        raise ValueError("Replacement targets are missing or belong to another tenant")

def _lock_new_batch(conn, new_id: UUID, tenant_id: str) -> None:
    sql = text("""
        SELECT b.id
        FROM sales.entities b
        JOIN sales.entity_types et
          ON et.entity_type_id = b.entity_type_id
         AND et.name = 'sales_batch'
        WHERE b.id = :new_id
          AND EXISTS (
              SELECT 1
              FROM sales.entity_attribute_values v
              JOIN sales.attributes a ON a.id = v.attribute_id
              WHERE v.entity_id = b.id
                AND a.name = 'tenant_id'
                AND a.entity_type_id = b.entity_type_id
                AND a.data_type = 'string'
                AND v.value_string = :tenant_id
          )
        FOR UPDATE OF b
    """).bindparams(bindparam("new_id", type_=_UUID))

    if conn.execute(sql, {"new_id": new_id, "tenant_id": str(tenant_id)}).scalar_one_or_none() is None:
        raise ValueError("Incoming batch has not been ingested or has an invalid tenant")




def _read_checkpoint(conn, new_id: UUID) -> dict:
    """The JSONB replaced_batch_ids marker and dates are committed with the DELETEs."""
    sql = text("""
        SELECT a.name, v.value_jsonb
        FROM sales.entity_attribute_values v
        JOIN sales.attributes a ON a.id = v.attribute_id
        WHERE v.entity_id = :new_id
          AND a.entity_type_id = (
              SELECT entity_type_id FROM sales.entity_types
              WHERE name = 'sales_batch'
          )
          AND a.name IN ('replaced_batch_ids', 'replacing_dates')
          AND a.data_type = 'jsonb'
    """).bindparams(bindparam("new_id", type_=_UUID))
    return {
        row["name"]: row["value_jsonb"]
        for row in conn.execute(sql, {"new_id": new_id}).mappings()
    }


def _checkpoint_matches(checkpoint: dict, ids: list[UUID]) -> bool:
    """Validate both atomic checkpoint attributes, not merely their presence."""
    if set(checkpoint) != {"replaced_batch_ids", "replacing_dates"}:
        return False

    raw_ids = checkpoint["replaced_batch_ids"]
    dates = checkpoint["replacing_dates"]
    if (
        not isinstance(raw_ids, list)
        or not isinstance(dates, dict)
        or dates.get("timezone") != "UTC"
        or not isinstance(dates.get("overall"), dict)
        or not isinstance(dates.get("batches"), list)
    ):
        return False

    try:
        checkpoint_ids = [UUID(item) for item in raw_ids]
        date_ids = [
            UUID(entry["batch_id"])
            for entry in dates["batches"]
            if isinstance(entry, dict)
        ]
    except (KeyError, ValueError, TypeError, AttributeError):
        return False

    expected = set(ids)
    return (
        len(checkpoint_ids) == len(ids)
        and set(checkpoint_ids) == expected
        and len(date_ids) == len(ids)
        and set(date_ids) == expected
    )


def _require_complete_new_batch(conn, new_id: UUID, staging_table: str) -> None:
    """Do NOT remove old data unless ALL staged orders and line items exist.

    EAV generates one transaction per distinct order and one line-item ID per
    row. If two identical source rows hash to the same line-item ID, this
    deliberately rejects replacement rather than silently dropping a sale.
    It also catches Durable 'skipped' and partially committed EAV chunks.
    """
    actual_sql = text("""
        WITH txns AS (
            SELECT txn.id
            FROM sales.entities txn
            JOIN sales.entity_types et
              ON et.entity_type_id = txn.entity_type_id
             AND et.name = 'transaction'
            WHERE txn.parent_entity_id = :new_id
        )
        SELECT
            (SELECT COUNT(*) FROM txns) AS transaction_count,
            (
                SELECT COUNT(*)
                FROM sales.entities li
                JOIN txns t ON t.id = li.parent_entity_id
                JOIN sales.entity_types lt
                  ON lt.entity_type_id = li.entity_type_id
                 AND lt.name = 'line_item'
            ) AS line_item_count
    """).bindparams(bindparam("new_id", type_=_UUID))
    actual = conn.execute(actual_sql, {"new_id": new_id}).mappings().one()
    expected_table = f"staging_{new_id}"
    if staging_table != expected_table:
        raise ValueError("Staging table does not match incoming batch upload ID")
    staging = f'"sales"."{expected_table}"'
    expected = conn.execute(
        text(f"""
            SELECT
                COUNT(DISTINCT "order_num") AS transaction_count,
                COUNT(*) AS line_item_count
            FROM {staging}
        """)
    ).mappings().one()

    expected_orders = int(expected["transaction_count"])
    expected_lines = int(expected["line_item_count"])
    actual_orders = int(actual["transaction_count"])
    actual_lines = int(actual["line_item_count"])
    if (
        expected_orders == 0
        or expected_lines == 0
        or actual_orders != expected_orders
        or actual_lines != expected_lines
    ):
        raise ValueError(
            "Incomplete incoming sales batch; refusing replacement: "
            f"orders {actual_orders}/{expected_orders}, "
            f"line items {actual_lines}/{expected_lines}. "
            "Note that duplicate identical source rows generate the same line-item ID."
        )


def _old_batch_date_ranges(conn, ids: list[UUID]) -> dict:
    """Find old transaction sale-date ranges using transaction-scoped EAV attributes.

    This follows the same entity/attribute scoping as
    sales.fn_find_matching_transactions: sale_date belongs to the transaction
    entity type. Value_ts is preferred; string values are parsed as a fallback.
    No line-item scan is necessary for the transaction sale_date contract.
    """
    stmt = text("""
        WITH transaction_type AS (
            SELECT entity_type_id
            FROM sales.entity_types
            WHERE name = 'transaction'
        ),
        sale_date_attributes AS (
            SELECT a.id
            FROM sales.attributes AS a
            JOIN transaction_type AS tt ON tt.entity_type_id = a.entity_type_id
            WHERE a.name = 'sale_date'
        ),
        candidate_transactions AS (
            SELECT t.parent_entity_id AS batch_id, t.id AS transaction_id
            FROM sales.entities AS t
            JOIN transaction_type AS tt ON tt.entity_type_id = t.entity_type_id
            WHERE t.parent_entity_id = ANY(:batch_ids)
        ),
        dated AS (
            SELECT
                t.batch_id,
                t.transaction_id,
                v.attribute_id,
                v.value_ts,
                v.value_string,
                COALESCE(
                    v.value_ts,
                    sales.try_cast_timestamp(NULLIF(BTRIM(v.value_string), ''))::timestamptz
                ) AS sale_ts
            FROM candidate_transactions AS t
            LEFT JOIN sales.entity_attribute_values AS v
              ON v.entity_id = t.transaction_id
             AND v.attribute_id IN (SELECT id FROM sale_date_attributes)
        )
        SELECT
            batch_id,
            COUNT(*) AS candidate_rows,
            COUNT(attribute_id) AS date_attribute_rows,
            COUNT(value_ts) AS timestamp_values,
            COUNT(value_string) AS string_values,
            COUNT(sale_ts) AS usable_dates,
            MIN((sale_ts AT TIME ZONE 'UTC')::date) AS min_date,
            MAX((sale_ts AT TIME ZONE 'UTC')::date) AS max_date
        FROM dated
        GROUP BY batch_id
    """).bindparams(bindparam("batch_ids", type_=_UUID_ARRAY))

    rows = {
        str(row["batch_id"]): row
        for row in conn.execute(stmt, {"batch_ids": ids}).mappings()
    }
    missing = [
        str(batch_id)
        for batch_id in ids
        if str(batch_id) not in rows
        or rows[str(batch_id)]["min_date"] is None
        or rows[str(batch_id)]["max_date"] is None
    ]
    if missing:
        diagnostics = {
            batch_id: (
                {
                    "date_attribute_rows": int(rows[batch_id]["date_attribute_rows"]),
                    "timestamp_values": int(rows[batch_id]["timestamp_values"]),
                    "string_values": int(rows[batch_id]["string_values"]),
                    "usable_dates": int(rows[batch_id]["usable_dates"]),
                }
                if batch_id in rows
                else {"candidate_transactions": 0}
            )
            for batch_id in missing
        }
        raise ValueError(
            f"No usable transaction sale_date for old batch(es): {diagnostics}"
        )

    ranges = [
        {
            "batch_id": str(batch_id),
            "start_date": rows[str(batch_id)]["min_date"].isoformat(),
            "end_date": rows[str(batch_id)]["max_date"].isoformat(),
        }
        for batch_id in ids
    ]
    return {
        "timezone": "UTC",
        "batches": ranges,
        "overall": {
            "start_date": min(row["start_date"] for row in ranges),
            "end_date": max(row["end_date"] for row in ranges),
        },
    }

def _save_checkpoint(conn, new_id: UUID, ids: list[UUID], dates: dict) -> None:
    """Persist JSONB batch lineage/date ranges in the same transaction as deletion."""
    conn.execute(text("""
        INSERT INTO sales.attributes (id, entity_type_id, name, data_type, description)
        SELECT pg_catalog.gen_random_uuid(), et.entity_type_id, n.name,
               'jsonb'::sales.attr_data_type, NULL::text
        FROM sales.entity_types et
        CROSS JOIN (VALUES ('replaced_batch_ids'), ('replacing_dates')) AS n(name)
        WHERE et.name = 'sales_batch'
        ON CONFLICT (entity_type_id, name, data_type) DO NOTHING
    """))

    attr_sql = text("""
        SELECT id, name
        FROM sales.attributes
        WHERE entity_type_id = (
            SELECT entity_type_id FROM sales.entity_types WHERE name = 'sales_batch'
        )
          AND data_type = 'jsonb'
          AND name IN ('replaced_batch_ids', 'replacing_dates')
    """)
    attributes = {
        row["name"]: row["id"] for row in conn.execute(attr_sql).mappings()
    }
    if set(attributes) != {"replaced_batch_ids", "replacing_dates"}:
        raise RuntimeError("Unable to create required sales_batch replacement attributes")

    upsert_sql = text("""
        INSERT INTO sales.entity_attribute_values (entity_id, attribute_id, value_jsonb)
        VALUES (:new_id, :attribute_id, :value_jsonb)
        ON CONFLICT (entity_id, attribute_id) DO UPDATE
        SET value_jsonb = EXCLUDED.value_jsonb
    """).bindparams(
        bindparam("new_id", type_=_UUID),
        bindparam("attribute_id", type_=_UUID),
        bindparam("value_jsonb", type_=JSONB),
    )
    for name, value in (
        ("replaced_batch_ids", [str(batch_id) for batch_id in ids]),
        ("replacing_dates", dates),
    ):
        conn.execute(
            upsert_sql,
            {
                "new_id": new_id,
                "attribute_id": attributes[name],
                "value_jsonb": value,
            },
        )


def _delete_attributions(conn, ids: list[UUID]) -> dict[str, int]:
    """Remove fixed attribution rows in the SAME transaction as sales deletion.

    Missing matches are normal: DELETE returns a rowcount of zero.
    """
    params = {"batch_ids": ids}
    bind = bindparam("batch_ids", type_=_UUID_ARRAY)

    geoframe = conn.execute(
        text("""
            DELETE FROM attribution.match_geoframe_relationship
            WHERE batch_id = ANY(:batch_ids)
        """).bindparams(bind),
        params,
    ).rowcount

    relationship = conn.execute(
        text("""
            DELETE FROM attribution.match_relationship
            WHERE batch_id = ANY(:batch_ids)
        """).bindparams(bind),
        params,
    ).rowcount

    return {
        "attribution.match_geoframe_relationship": geoframe,
        "attribution.match_relationship": relationship,
    }


@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_validateReplacementTargets(settings: dict) -> dict:
    """Read-only early validation; it does NOT reserve or delete old batches."""
    ids = _replacement_ids(settings)
    with _transaction() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '10s'"))
        conn.execute(text("SET LOCAL statement_timeout = '5min'"))
        conn.execute(text("SET LOCAL TIME ZONE 'UTC'"))
        _check_attribution_tables(conn)
        _require_owned_old_batches(
            conn, ids, str(settings["tenant_id"]), lock=False
        )
    return {"validated_batch_count": len(ids)}


@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_applyReplacement(settings: dict) -> dict:
    """
    Single atomic transaction:
      * acquire new batch row lock, check idempotency marker
      * acquire and validate old batch row locks
      * collect old date ranges while old EAV still exists
      * store lineage/date range metadata
      * DELETE attribution tables and cascade-delete old sales entities
      * COMMIT

    If any statement fails, nothing in this activity is committed. On activity
    replay after a successful commit, replaced_batch_ids acts as the checkpoint.
    """
    ids = _replacement_ids(settings)
    new_id = UUID(str(settings["new_batch_id"]))

    with _transaction() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '10s'"))
        conn.execute(text("SET LOCAL statement_timeout = '15min'"))
        conn.execute(text("SET LOCAL TIME ZONE 'UTC'"))

        _lock_new_batch(conn, new_id, str(settings["tenant_id"]))
        checkpoint = _read_checkpoint(conn, new_id)
        if checkpoint:
            if not _checkpoint_matches(checkpoint, ids):
                raise ValueError(
                    "Incoming batch has an inconsistent or conflicting replacement "
                    "checkpoint; refusing to repeat deletion"
                )
            return {
                "status": "already_applied",
                "new_batch_id": str(new_id),
                "replacing_dates": checkpoint["replacing_dates"],
            }

        _check_attribution_tables(conn)
        _require_owned_old_batches(conn, ids, str(settings["tenant_id"]), lock=True)
        _require_complete_new_batch(conn, new_id, settings["staging_table"])
        dates = _old_batch_date_ranges(conn, ids)
        _save_checkpoint(conn, new_id, ids, dates)
        deleted_attributions = _delete_attributions(conn, ids)

        stmt = text("""
            DELETE FROM sales.entities AS b
            USING sales.entity_types AS et
            WHERE b.id = ANY(:batch_ids)
              AND b.entity_type_id = et.entity_type_id
              AND et.name = 'sales_batch'
        """).bindparams(bindparam("batch_ids", type_=_UUID_ARRAY))
        deleted_batches = conn.execute(stmt, {"batch_ids": ids}).rowcount
        if deleted_batches != len(ids):
            raise RuntimeError("Replacement batch deletion count mismatch; transaction aborted")


    logger.info(
        "[LOG] Replacement committed: new_batch_id=%s, old_batch_count=%s",
        str(new_id),
        len(ids),
        extra={"context": {"PartitionKey": str(new_id)}},
    )
    return {
        "status": "applied",
        "new_batch_id": str(new_id),
        "replaced_batch_ids": [str(batch_id) for batch_id in ids],
        "replacing_dates": dates,
        "attribution_rows_deleted": deleted_attributions,
    }


@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_compensateFailedReplacement(settings: dict) -> dict:
    """Delete an incoming batch only when no replacement checkpoint was committed.

    Checkpoint and old-batch deletion share a transaction. The new-batch row
    lock serializes compensation with a replacement that is still committing.
    """
    ids = _replacement_ids(settings)  # do not compensate malformed input
    new_id = UUID(str(settings["new_batch_id"]))
    tenant_id = str(settings["tenant_id"])

    with _transaction() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '10s'"))
        conn.execute(text("SET LOCAL statement_timeout = '15min'"))

        # Lock on the same new batch entity as applyReplacement to serialize
        # commit-vs-compensation. Absence means it was already compensated or
        # EAV creation never happened.
        exists = conn.execute(
            text("""
                SELECT b.id
                FROM sales.entities b
                JOIN sales.entity_types et
                  ON et.entity_type_id = b.entity_type_id
                 AND et.name = 'sales_batch'
                WHERE b.id = :new_id
                FOR UPDATE OF b
            """).bindparams(bindparam("new_id", type_=_UUID)),
            {"new_id": new_id},
        ).scalar_one_or_none()

        if exists is None:
            return {"status": "nothing_to_compensate", "new_batch_id": str(new_id)}

        # Never delete an unowned entity even if a malicious caller constructed
        # its ID. The original preflight is not a substitute for this check.
        _lock_new_batch(conn, new_id, tenant_id)
        checkpoint = _read_checkpoint(conn, new_id)
        if checkpoint:
            if not _checkpoint_matches(checkpoint, ids):
                raise RuntimeError(
                    "Conflicting replacement checkpoint; manual reconciliation required"
                )
            return {"status": "replacement_already_committed", "new_batch_id": str(new_id)}

        # Attribution should never have been dispatched before replacement, but
        # removing any rows keyed to the incoming ID makes compensation robust.
        _check_attribution_tables(conn)
        removed_attr = _delete_attributions(conn, [new_id])

        deleted = conn.execute(
            text("""
                DELETE FROM sales.entities
                WHERE id = :new_id
            """).bindparams(bindparam("new_id", type_=_UUID)),
            {"new_id": new_id},
        ).rowcount
        if deleted != 1:
            raise RuntimeError("Incoming batch deletion count mismatch")

    return {
        "status": "compensated",
        "new_batch_id": str(new_id),
        "attribution_rows_deleted": removed_attr,
    }
