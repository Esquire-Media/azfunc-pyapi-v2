from __future__ import annotations

import logging
import os
from typing import Dict, List

from azure.durable_functions import Blueprint
from sqlalchemy import Text, bindparam, create_engine, text
from sqlalchemy.dialects.postgresql import ARRAY

bp = Blueprint()

engine = create_engine(
    os.environ["DATABIND_SQL_KEYSTONE"],
    pool_pre_ping=True,
)


NON_SCOPED_FIELDS = {
    "tenant_id",
    "days_back",
    "sale_date",
    "state_abbreviation",
    "city",
    "zipcode"
}

SCOPE_QUERY = text(
    """
WITH
params AS (
  SELECT
    CAST(:tenant_id AS text) AS tenant_id
),

requested_filters AS (
  SELECT
    requested.filter_id,
    requested.logical_name
  FROM unnest(:filter_fields)
       WITH ORDINALITY AS requested(logical_name, filter_id)
),

entity_type_ids AS (
  SELECT
    (
      SELECT entity_type_id
      FROM sales.entity_types
      WHERE name = 'sales_batch'
    ) AS sales_batch_type_id,
    (
      SELECT entity_type_id
      FROM sales.entity_types
      WHERE name = 'transaction'
    ) AS transaction_type_id,
    (
      SELECT entity_type_id
      FROM sales.entity_types
      WHERE name = 'line_item'
    ) AS line_item_type_id
),

candidate_filter_names AS MATERIALIZED (
  /*
   * Direct logical name.
   */
  SELECT
    filter.filter_id,
    filter.logical_name,
    filter.logical_name AS attribute_name
  FROM requested_filters AS filter

  UNION

  /*
   * Tenant-specific mapped physical name.
   */
  SELECT
    filter.filter_id,
    filter.logical_name,
    mapped_attribute.name AS attribute_name
  FROM requested_filters AS filter
  CROSS JOIN params
  JOIN sales.client_header_map AS header_map
    ON header_map.tenant_id = params.tenant_id
   AND header_map.mapped_header = filter.logical_name
  JOIN sales.attributes AS mapped_attribute
    ON mapped_attribute.id = header_map.attribute_id
),

candidate_attributes AS MATERIALIZED (
  /*
   * Resolve possible transaction and line-item attributes without deciding
   * the scope yet.
   */
  SELECT DISTINCT
    candidate.filter_id,
    candidate.logical_name,
    attribute.id AS attribute_id,
    attribute.name AS attribute_name,
    CASE
      WHEN attribute.entity_type_id = types.transaction_type_id
        THEN 'transaction'
      WHEN attribute.entity_type_id = types.line_item_type_id
        THEN 'line_item'
    END AS scope
  FROM candidate_filter_names AS candidate
  CROSS JOIN entity_type_ids AS types
  JOIN sales.attributes AS attribute
    ON attribute.name = candidate.attribute_name
   AND attribute.entity_type_id IN (
     types.transaction_type_id,
     types.line_item_type_id
   )
),

tenant_attribute_ids AS MATERIALIZED (
  SELECT
    attribute.id AS attribute_id
  FROM entity_type_ids AS types
  JOIN sales.attributes AS attribute
    ON attribute.entity_type_id = types.sales_batch_type_id
   AND attribute.name = 'tenant_id'
   AND attribute.data_type = 'string'::sales.attr_data_type
),

sales_batches AS MATERIALIZED (
  SELECT DISTINCT
    batch.id AS batch_id
  FROM params
  CROSS JOIN entity_type_ids AS types
  JOIN tenant_attribute_ids AS tenant_attribute
    ON TRUE
  JOIN sales.entity_attribute_values AS tenant_value
    ON tenant_value.attribute_id = tenant_attribute.attribute_id
   AND tenant_value.value_string = params.tenant_id
  JOIN sales.entities AS batch
    ON batch.id = tenant_value.entity_id
   AND batch.entity_type_id = types.sales_batch_type_id
),

matching_value_entities AS MATERIALIZED (
  SELECT
    candidate.filter_id,
    candidate.logical_name,
    candidate.attribute_id,
    candidate.attribute_name,
    candidate.scope,
    attribute_value.entity_id
  FROM candidate_attributes AS candidate
  JOIN sales.entity_attribute_values AS attribute_value
    ON attribute_value.attribute_id = candidate.attribute_id
),

scope_hits AS MATERIALIZED (
  (
    SELECT DISTINCT ON (
      match.filter_id,
      match.scope
    )
      match.filter_id,
      match.logical_name,
      match.scope,
      match.attribute_id,
      match.attribute_name,
      match.entity_id AS sample_entity_id
    FROM matching_value_entities AS match
    CROSS JOIN entity_type_ids AS types
    JOIN sales.entities AS transaction_entity
      ON transaction_entity.id = match.entity_id
     AND transaction_entity.entity_type_id =
         types.transaction_type_id
    JOIN sales_batches AS batch
      ON batch.batch_id =
         transaction_entity.parent_entity_id
    WHERE match.scope = 'transaction'
    ORDER BY
      match.filter_id,
      match.scope,
      match.entity_id
  )

  UNION ALL

  (
    SELECT DISTINCT ON (
      match.filter_id,
      match.scope
    )
      match.filter_id,
      match.logical_name,
      match.scope,
      match.attribute_id,
      match.attribute_name,
      match.entity_id AS sample_entity_id
    FROM matching_value_entities AS match
    CROSS JOIN entity_type_ids AS types
    JOIN sales.entities AS line_item_entity
      ON line_item_entity.id = match.entity_id
     AND line_item_entity.entity_type_id =
         types.line_item_type_id
    JOIN sales.entities AS transaction_entity
      ON transaction_entity.id =
         line_item_entity.parent_entity_id
     AND transaction_entity.entity_type_id =
         types.transaction_type_id
    JOIN sales_batches AS batch
      ON batch.batch_id =
         transaction_entity.parent_entity_id
    WHERE match.scope = 'line_item'
    ORDER BY
      match.filter_id,
      match.scope,
      match.entity_id
  )
)

SELECT
  filter.logical_name,
  COALESCE(
    array_agg(
      DISTINCT hit.scope
      ORDER BY hit.scope
    ) FILTER (
      WHERE hit.scope IS NOT NULL
    ),
    ARRAY[]::text[]
  ) AS scopes
FROM requested_filters AS filter
LEFT JOIN scope_hits AS hit
  ON hit.filter_id = filter.filter_id
GROUP BY
  filter.filter_id,
  filter.logical_name
ORDER BY
  filter.filter_id
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
    Return the resolved transaction or line_item scope for requested
    sales attributes.

    tenant_id and days_back are control fields, not sales attributes,
    and are excluded from scope resolution.

    Expected ingress:
      {
        "tenant_id": "<tenant id>",
        "filter_fields": [
            "tenant_id",
            "days_back",
            "store_location",
            "brand",
            "sku"
        ]
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
            if (
                isinstance(field, str)
                and field.strip()
                and field.strip() not in NON_SCOPED_FIELDS
            )
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
        row["logical_name"]: _resolve_scope(list(row["scopes"]))
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
