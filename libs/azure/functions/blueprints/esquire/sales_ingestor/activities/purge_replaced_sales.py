"""Durable activities for atomic replacement of completed sales batches.

Requires SALES_ATTRIBUTION_TABLES as a comma-separated allowlist of schema.table
names. Every configured table must have a batch_id column (UUID or text).
Only the trusted deployment environment determines which tables are deleted.

Assumes attribution rows, sales entities and EAV metadata live in the same
PostgreSQL database / SQLAlchemy connection.
"""

from __future__ import annotations

import logging
import os
import re
from uuid import UUID

from azure.durable_functions import Blueprint
from sqlalchemy import bindparam, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, TEXT as PG_TEXT, UUID as PG_UUID

from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import db

bp = Blueprint()
logger = logging.getLogger("salesIngestor.logger")

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_UUID_ARRAY = ARRAY(PG_UUID(as_uuid=True))
_UUID = PG_UUID(as_uuid=True)


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


def _attribution_tables() -> list[tuple[str, str]]:
    """Use a deployment-owned table allowlist; never read table names from HTTP."""
    configured = os.environ.get("SALES_ATTRIBUTION_TABLES", "").strip()
    if not configured:
        raise RuntimeError(
            "SALES_ATTRIBUTION_TABLES must list ALL attribution tables "
            "before sales replacement is enabled"
        )

    parsed = []
    for item in configured.split(","):
        parts = item.strip().split(".")
        if len(parts) != 2 or not all(_IDENT.fullmatch(part) for part in parts):
            raise ValueError(
                "SALES_ATTRIBUTION_TABLES must contain comma-separated schema.table names"
            )
        parsed.append((parts[0], parts[1]))

    if len(parsed) != len(set(parsed)):
        raise ValueError("Duplicate entries in SALES_ATTRIBUTION_TABLES")

    return parsed


def _attribution_column_types(conn, tables: list[tuple[str, str]]) -> list[tuple[str, str, str]]:
    """
    Verify every requested DELETE target before modifying any rows.
    Retain column type to permit either UUID or text batch_id columns.
    """
    sql = text("""
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name = :table
          AND column_name = 'batch_id'
    """)
    base_table_sql = text("""
        SELECT table_type
        FROM information_schema.tables
        WHERE table_schema = :schema
          AND table_name = :table
    """)
    result = []
    for schema, table in tables:
        table_type = conn.execute(
            base_table_sql, {"schema": schema, "table": table}
        ).scalar_one_or_none()
        if table_type != "BASE TABLE":
            raise RuntimeError(
                f"{schema}.{table} is missing or is not a base table"
            )
        row = conn.execute(sql, {"schema": schema, "table": table}).scalar_one_or_none()
        if row not in ("uuid", "text", "character varying"):
            raise RuntimeError(
                f"{schema}.{table}.batch_id is missing or has an unsupported type: {row}"
            )
        result.append((schema, table, row))
    return result


def _require_owned_old_batches(conn, ids: list[UUID], tenant_id: str, lock: bool) -> None:
    """Validate entity type + batch tenant. Row locks serialize competing replacements."""
    lock_clause = "FOR UPDATE OF b" if lock else ""
    sql = text(f"""
        SELECT
            b.id,
            EXISTS (
                SELECT 1
                FROM sales.entity_attribute_values v
                JOIN sales.attributes a ON a.id = v.attribute_id
                WHERE v.entity_id = b.id
                  AND a.name = 'tenant_id'
                  AND a.entity_type_id = b.entity_type_id
                  AND a.data_type = 'string'
                  AND v.value_string = :tenant_id
            ) AS same_tenant,
            NOT EXISTS (
                SELECT 1
                FROM sales.entity_attribute_values v
                JOIN sales.attributes a ON a.id = v.attribute_id
                WHERE v.entity_id = b.id
                  AND a.name = 'replacement_status'
                  AND a.entity_type_id = b.entity_type_id
                  AND a.data_type = 'string'
                  AND v.value_string IS DISTINCT FROM 'completed'
            ) AS is_ready
        FROM sales.entities b
        JOIN sales.entity_types et
          ON et.entity_type_id = b.entity_type_id
         AND et.name = 'sales_batch'
        WHERE b.id = ANY(:batch_ids)
        ORDER BY b.id
        {lock_clause}
    """).bindparams(bindparam("batch_ids", type_=_UUID_ARRAY))

    rows = conn.execute(sql, {"batch_ids": ids, "tenant_id": str(tenant_id)}).mappings().all()
    if {row["id"] for row in rows} != set(ids) or not all(
        row["same_tenant"] and row["is_ready"] for row in rows
    ):
        raise ValueError(
            "Replacement targets are missing, not owned by the tenant, "
            "or are themselves pending replacements"
        )


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


def _read_replacement_state(conn, new_id: UUID) -> str | None:
    """Read internal state; legacy batches have no replacement_status attribute."""
    stmt = text("""
        SELECT v.value_string
        FROM sales.entity_attribute_values v
        JOIN sales.attributes a ON a.id = v.attribute_id
        JOIN sales.entity_types et ON et.entity_type_id = a.entity_type_id
        WHERE v.entity_id = :new_id
          AND et.name = 'sales_batch'
          AND a.name = 'replacement_status'
          AND a.data_type = 'string'
    """).bindparams(bindparam("new_id", type_=_UUID))
    return conn.execute(stmt, {"new_id": new_id}).scalar_one_or_none()


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
    """Compute each old batch's UTC sale-date envelope once, before deletion.

    sale_date may be stored on transactions or their line items.
    We need the min/max dates for historical metadata, not a per-order
    date completeness audit. A batch with no usable sale dates is rejected.
    """
    sql = text("""
        WITH transactions AS (
            SELECT txn.parent_entity_id AS batch_id, txn.id AS txn_id
            FROM sales.entities txn
            JOIN sales.entity_types et
              ON et.entity_type_id = txn.entity_type_id
             AND et.name = 'transaction'
            WHERE txn.parent_entity_id = ANY(:batch_ids)
        ),
        dated_entities AS (
            SELECT batch_id, txn_id AS entity_id FROM transactions
            UNION ALL
            SELECT t.batch_id, li.id
            FROM transactions t
            JOIN sales.entities li ON li.parent_entity_id = t.txn_id
            JOIN sales.entity_types et
              ON et.entity_type_id = li.entity_type_id
             AND et.name = 'line_item'
        ),
        date_values AS (
            SELECT d.batch_id,
                   COALESCE(
                       v.value_ts,
                       sales.try_cast_timestamp(v.value_string)::timestamptz
                   ) AS sale_ts
            FROM dated_entities d
            JOIN sales.entity_attribute_values v ON v.entity_id = d.entity_id
            JOIN sales.attributes a ON a.id = v.attribute_id
             AND a.name = 'sale_date'
        )
        SELECT batch_id,
               (MIN(sale_ts) AT TIME ZONE 'UTC')::date AS min_date,
               (MAX(sale_ts) AT TIME ZONE 'UTC')::date AS max_date
        FROM date_values
        GROUP BY batch_id
    """).bindparams(bindparam("batch_ids", type_=_UUID_ARRAY))

    rows = conn.execute(sql, {"batch_ids": ids}).mappings()
    by_id = {
        str(row["batch_id"]): {
            "batch_id": str(row["batch_id"]),
            "start_date": row["min_date"].isoformat(),
            "end_date": row["max_date"].isoformat(),
        }
        for row in rows
        if row["min_date"] is not None and row["max_date"] is not None
    }

    if any(str(batch_id) not in by_id for batch_id in ids):
        raise ValueError("Old batch has no usable sale dates; refusing replacement")

    ranges = [by_id[str(batch_id)] for batch_id in ids]
    return {
        "timezone": "UTC",
        "batches": ranges,
        "overall": {
            "start_date": min(item["start_date"] for item in ranges),
            "end_date": max(item["end_date"] for item in ranges),
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

    # Commit "completed" in the very same database transaction as deletes.
    # Readers can opt to filter out "pending" replacements.
    result = conn.execute(text("""
        INSERT INTO sales.entity_attribute_values (
            entity_id, attribute_id, value_string
        )
        SELECT :new_id, a.id, 'completed'
        FROM sales.attributes a
        JOIN sales.entity_types et ON et.entity_type_id = a.entity_type_id
        WHERE et.name = 'sales_batch'
          AND a.name = 'replacement_status'
          AND a.data_type = 'string'
        ON CONFLICT (entity_id, attribute_id) DO UPDATE
            SET value_string = 'completed'
    """).bindparams(bindparam("new_id", type_=_UUID)), {"new_id": new_id})
    if result.rowcount != 1:
        raise RuntimeError("Missing internal replacement_status EAV attribute")


def _delete_attributions(
    conn, ids: list[UUID], table_types: list[tuple[str, str, str]]
) -> dict[str, int]:
    """
    Delete attribution rows BEFORE cascading parent entities; all operations
    remain in the same open SQL transaction.
    """
    deleted = {}
    for schema, table, data_type in table_types:
        identifier = f'"{schema}"."{table}"'
        if data_type == "uuid":
            parameters = ids
            array_type = _UUID_ARRAY
        else:
            parameters = [str(batch_id) for batch_id in ids]
            array_type = ARRAY(PG_TEXT)

        stmt = text(f"DELETE FROM {identifier} WHERE batch_id = ANY(:batch_ids)").bindparams(
            bindparam("batch_ids", type_=array_type)
        )
        deleted[f"{schema}.{table}"] = conn.execute(
            stmt, {"batch_ids": parameters}
        ).rowcount
    return deleted


@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_validateReplacementTargets(settings: dict) -> dict:
    """Read-only early validation; it does NOT reserve or delete old batches."""
    ids = _replacement_ids(settings)
    tables = _attribution_tables()

    with db() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '10s'"))
        conn.execute(text("SET LOCAL statement_timeout = '5min'"))
        conn.execute(text("SET LOCAL TIME ZONE 'UTC'"))
        _attribution_column_types(conn, tables)
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

    with db() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '10s'"))
        conn.execute(text("SET LOCAL statement_timeout = '15min'"))
        conn.execute(text("SET LOCAL TIME ZONE 'UTC'"))

        _lock_new_batch(conn, new_id, str(settings["tenant_id"]))
        replacement_status = _read_replacement_state(conn, new_id)
        checkpoint = _read_checkpoint(conn, new_id)
        if checkpoint:
            if replacement_status != "completed":
                raise RuntimeError("Replacement checkpoint/state mismatch; manual reconciliation required")
            if not _checkpoint_matches(checkpoint, ids):
                raise ValueError(
                    "Incoming sales batch has inconsistent or conflicting "
                    "replacement checkpoint metadata"
                )
            return {
                "status": "already_applied",
                "new_batch_id": str(new_id),
                "replacing_dates": checkpoint["replacing_dates"],
            }

        if replacement_status != "pending":
            raise RuntimeError(
                "Incoming batch is not marked as pending replacement; "
                "refusing destructive operation"
            )

        tables = _attribution_tables()
        table_types = _attribution_column_types(conn, tables)
        _require_owned_old_batches(conn, ids, str(settings["tenant_id"]), lock=True)
        _require_complete_new_batch(conn, new_id, settings["staging_table"])
        dates = _old_batch_date_ranges(conn, ids)
        _save_checkpoint(conn, new_id, ids, dates)
        deleted_attributions = _delete_attributions(conn, ids, table_types)

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

        conn.commit()

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
    """Delete only an uncommitted incoming batch after replacement failure.

    A committed replacement must NEVER be compensated here: the same-transaction
    checkpoint distinguishes a successful swap even when Azure lost the activity
    acknowledgment. This routine is itself idempotent.
    """
    _replacement_ids(settings)  # validate request shape; do not delete on malformed settings
    new_id = UUID(str(settings["new_batch_id"]))
    tenant_id = str(settings["tenant_id"])

    with db() as conn:
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
        replacement_status = _read_replacement_state(conn, new_id)
        if checkpoint:
            if replacement_status != "completed" or not _checkpoint_matches(
                checkpoint, _replacement_ids(settings)
            ):
                raise RuntimeError(
                    "Conflicting replacement checkpoint/state; manual reconciliation required"
                )
            return {"status": "replacement_already_committed", "new_batch_id": str(new_id)}

        if replacement_status != "pending":
            raise RuntimeError(
                "Incoming batch is not pending replacement; refusing compensation"
            )

        # Attribution should never have been dispatched before replacement, but
        # removing any rows keyed to the incoming ID makes compensation robust.
        tables = _attribution_tables()
        table_types = _attribution_column_types(conn, tables)
        removed_attr = _delete_attributions(conn, [new_id], table_types)

        deleted = conn.execute(
            text("""
                DELETE FROM sales.entities
                WHERE id = :new_id
            """).bindparams(bindparam("new_id", type_=_UUID)),
            {"new_id": new_id},
        ).rowcount
        if deleted != 1:
            raise RuntimeError("Incoming batch deletion count mismatch")
        conn.commit()

    return {
        "status": "compensated",
        "new_batch_id": str(new_id),
        "attribution_rows_deleted": removed_attr,
    }
