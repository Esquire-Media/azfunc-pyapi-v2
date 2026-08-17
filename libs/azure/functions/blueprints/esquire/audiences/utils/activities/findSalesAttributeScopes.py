from __future__ import annotations

import logging
import os
from typing import Dict, List

from azure.durable_functions import Blueprint
from sqlalchemy import Text, bindparam, create_engine, text
from sqlalchemy.dialects.postgresql import ARRAY

bp = Blueprint()

engine = create_engine(
    os.environ["DATABASE_URL"],
    pool_pre_ping=True,
)


SCOPE_QUERY = text(
    """
WITH
requested_fields AS (
  SELECT DISTINCT
    requested.logical_name
  FROM unnest(:filter_fields) AS requested(logical_name)
),

/*
Find this tenant's sales batches through the existing optimized EAV query path.
*/
tenant_sales_batches AS MATERIALIZED (
  SELECT batch.entity_id
  FROM sales.query_eav(
    'sales_batch',
    jsonb_build_object(
      '["tenant_id"]',
      jsonb_build_object(
        '==',
        :tenant_id
      )
    )
  ) AS batch(entity_id uuid)
),

attribute_candidates AS MATERIALIZED (
  /*
   * Prefer tenant-specific mappings.
   */
  SELECT
    requested.logical_name,
    attribute.id AS attribute_id,
    attribute.entity_type_id,
    entity_type.name AS scope,
    0 AS priority
  FROM requested_fields AS requested
  JOIN sales.client_header_map AS header_map
    ON header_map.tenant_id = :tenant_id
   AND header_map.mapped_header = requested.logical_name
  JOIN sales.attributes AS attribute
    ON attribute.id = header_map.attribute_id
  JOIN sales.entity_types AS entity_type
    ON entity_type.entity_type_id = attribute.entity_type_id
   AND entity_type.name IN (
     'transaction',
     'line_item'
   )

  UNION ALL

  /*
   * Canonical-name fallback.
   */
  SELECT
    requested.logical_name,
    attribute.id AS attribute_id,
    attribute.entity_type_id,
    entity_type.name AS scope,
    1 AS priority
  FROM requested_fields AS requested
  JOIN sales.attributes AS attribute
    ON attribute.name = requested.logical_name
  JOIN sales.entity_types AS entity_type
    ON entity_type.entity_type_id = attribute.entity_type_id
   AND entity_type.name IN (
     'transaction',
     'line_item'
   )
),

/*
Use tenant mappings when available. Otherwise, use canonical-name candidates.
If the winning priority contains both scopes, both remain candidates.
*/
best_attribute_candidates AS MATERIALIZED (
  SELECT DISTINCT
    candidate.logical_name,
    candidate.attribute_id,
    candidate.entity_type_id,
    candidate.scope
  FROM attribute_candidates AS candidate
  WHERE candidate.priority = (
    SELECT min(comparison.priority)
    FROM attribute_candidates AS comparison
    WHERE comparison.logical_name = candidate.logical_name
  )
),

observed_scopes AS (
  /*
   * Check transaction attributes by walking:
   *
   * EAV value -> transaction -> tenant sales batch
   *
   * EXISTS stops after the first tenant-owned occurrence.
   */
  SELECT
    candidate.logical_name,
    candidate.scope
  FROM best_attribute_candidates AS candidate
  WHERE candidate.scope = 'transaction'
    AND EXISTS (
      SELECT 1
      FROM sales.entity_attribute_values AS attribute_value
      JOIN sales.entities AS transaction_entity
        ON transaction_entity.id = attribute_value.entity_id
       AND transaction_entity.entity_type_id =
           candidate.entity_type_id
      JOIN tenant_sales_batches AS batch
        ON batch.entity_id = transaction_entity.parent_entity_id
      WHERE attribute_value.attribute_id = candidate.attribute_id
    )

  UNION ALL

  /*
   * Check line-item attributes by walking:
   *
   * EAV value -> line item -> transaction -> tenant sales batch
   *
   * EXISTS stops after the first tenant-owned occurrence.
   */
  SELECT
    candidate.logical_name,
    candidate.scope
  FROM best_attribute_candidates AS candidate
  WHERE candidate.scope = 'line_item'
    AND EXISTS (
      SELECT 1
      FROM sales.entity_attribute_values AS attribute_value
      JOIN sales.entities AS line_item_entity
        ON line_item_entity.id = attribute_value.entity_id
       AND line_item_entity.entity_type_id =
           candidate.entity_type_id
      JOIN sales.entities AS transaction_entity
        ON transaction_entity.id = line_item_entity.parent_entity_id
      JOIN tenant_sales_batches AS batch
        ON batch.entity_id = transaction_entity.parent_entity_id
      WHERE attribute_value.attribute_id = candidate.attribute_id
    )
)

SELECT
  requested.logical_name,
  COALESCE(
    array_agg(
      DISTINCT observed.scope
      ORDER BY observed.scope
    ) FILTER (
      WHERE observed.scope IS NOT NULL
    ),
    ARRAY[]::text[]
  ) AS scopes
FROM requested_fields AS requested
LEFT JOIN observed_scopes AS observed
  ON observed.logical_name = requested.logical_name
GROUP BY requested.logical_name
ORDER BY requested.logical_name
""".strip()
).bindparams(
    bindparam("tenant_id", type_=Text()),
    bindparam("filter_fields", type_=ARRAY(Text())),
)

@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_getSalesFilterScopes(
    ingress: dict,
) -> Dict[str, str]:
    """
    Return the resolved transaction or line_item scope for each requested
    sales field.

    Expected ingress:
      {
        "tenant_id": "<tenant id>",
        "filter_fields": ["store_location", "brand", "sku"]
      }

    Returns:
      {
        "store_location": "transaction",
        "brand": "transaction",
        "sku": "line_item"
      }

    When a field exists at both transaction and line_item scope,
    transaction wins.
    """
    tenant_id = ingress.get("tenant_id")
    if not isinstance(tenant_id, str) or not tenant_id.strip():
        raise ValueError("Missing ingress['tenant_id'].")

    raw_filter_fields = ingress.get("filter_fields") or []
    if not isinstance(raw_filter_fields, list):
        raise ValueError(
            "ingress['filter_fields'] must be a list of field names."
        )

    filter_fields = list(
        dict.fromkeys(
            field.strip()
            for field in raw_filter_fields
            if isinstance(field, str) and field.strip()
        )
    )

    if not filter_fields:
        return {}

    with engine.connect() as connection:
        rows = connection.execute(
            SCOPE_QUERY,
            {
                "tenant_id": tenant_id.strip(),
                "filter_fields": filter_fields,
            },
        ).mappings().all()

    result = {
        row["logical_name"]: (
            "transaction"
            if row["logical_name"] == "sale_date"
            else _resolve_scope(list(row["scopes"]))
        )
        for row in rows
        if row["scopes"]
    }

    unresolved = [
        field
        for field in filter_fields
        if field not in result
    ]

    if unresolved:
        raise ValueError(
            "No transaction or line_item scope found for: "
            + ", ".join(unresolved)
            + "."
        )

    logging.info(
        "[SalesFilterScopes] Resolved scopes for tenant %s: %s",
        tenant_id,
        result,
    )

    return result

def _resolve_scope(scopes: List[str]) -> str:
    """
    Resolve raw observed scopes to the single scope used by audience filtering.

    Precedence:
      - transaction + line_item -> transaction
      - transaction only        -> transaction
      - line_item only          -> line_item
    """
    if "transaction" in scopes:
        return "transaction"

    if "line_item" in scopes:
        return "line_item"

    raise ValueError("No transaction or line_item scope found.")

