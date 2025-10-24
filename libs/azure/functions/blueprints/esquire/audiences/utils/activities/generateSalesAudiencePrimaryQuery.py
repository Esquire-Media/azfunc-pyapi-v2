# File: /libs/azure/functions/blueprints/esquire/audiences/builder/activities/fetchAudience.py

from azure.durable_functions import Blueprint
from sqlalchemy import create_engine, text
# import logging
from datetime import datetime as dt, timedelta, timezone
import re
bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_generateSalesAudiencePrimaryQuery(ingress: dict):
    """
    Execution strategy (high-level):
    1) Resolve "what to look for" exactly once (entity type ids + attribute ids).
    2) Build the smallest possible set of candidate transactions as early as possible.
    3) Extract only the address entity ids referenced by those candidates and dedupe.
    4) Pivot the address text in one pass scoped to the deduped ids.

    Materialization strategy:
    • MATERIALIZED only where the result is tiny and/or reused across later scans.
    • Leave large, single-use CTEs non-materialized so the planner can inline/push down.

    Confirm indexing (will improve IO minimization):
    • sales.entity_types(name) UNIQUE or at least indexed
    • sales.attributes(entity_type_id, name) UNIQUE or indexed
    • sales.entities(id) PK, (entity_type_id, parent_entity_id) composite index
    • sales.entity_attribute_values(attribute_id, entity_id) composite index
    • sales.entity_attribute_values(attribute_id, value_ts)
    • sales.entity_attribute_values(attribute_id, value_numeric)
    • sales.entity_attribute_values(attribute_id, value_string)
    • sales.entity_attribute_values(attribute_id, value_boolean)
    """
    tenant_id = ingress["tenant_id"]
    sql_filter = ingress["audience"]["dataFilter"]
    utc_now = ingress["utc_now"]
    days_back = ingress.get("days_back", 30)
    since_dt = dt.strptime(utc_now, "%Y-%m-%d %H:%M:%S.%f%z") - timedelta(days=days_back)
    since_iso = since_dt.isoformat()

    # Extract filters
    def extract_filters(sql_filter: str):
        pattern = re.compile(r'\(?\s*"(?P<field>[^"]+)"\s*(?P<op>=|!=|>|<|>=|<=|LIKE|ILIKE)\s*(?P<val>\'?[^()\'"]+\'?)\s*\)?')
        results = []
        for match in pattern.finditer(sql_filter):
            field = match.group("field")
            op = match.group("op")
            val = match.group("val").strip("'")
            if field != "days_back":
                results.append({"field": field, "op": op, "value": val})
        return results

    filters = extract_filters(sql_filter)
    import logging
    logging.warning(f"[LOG] Filters: {filters}")

    # Query attribute scopes for this tenant
    attr_names = list(set(f["field"] for f in filters if f["field"] != "tenant_id"))
    if attr_names:
        attr_name_sql = ", ".join(f"'{a}'" for a in attr_names)
        logging.warning(f"[LOG] attr_names: {attr_names}")
        logging.warning(f"[LOG] attr_name_sql: {attr_name_sql}")

        scope_query = text(f"""
            SELECT DISTINCT
                COALESCE(chm.mapped_header, a.name) AS attr_name,
                et.name AS entity_type
            FROM sales.attributes a
            JOIN sales.entity_types et ON a.entity_type_id = et.entity_type_id
            LEFT JOIN sales.client_header_map chm
            ON chm.attribute_id = a.id AND chm.tenant_id = '{tenant_id}'
            WHERE COALESCE(chm.mapped_header, a.name) IN ({attr_name_sql})
        """)

        # Execute scope_query using a sqlalchemy connection
        import os
        with create_engine(
            os.environ["DATABIND_SQL_KEYSTONE"].replace("psycopg2", "psycopg"),      # postgresql+psycopg2://…
            pool_pre_ping=True, 
            pool_size=10,
            max_overflow=20, 
            future=True,
        ).begin() as conn:
            rows = conn.execute(scope_query)

        scope_map = {}
        for attr_name, entity_type in rows:
            if attr_name not in scope_map:
                scope_map[attr_name] = entity_type  # prefer first match

    # Build params CTE
    # params: runtime inputs (tiny; inline constants or bind parameters in app layer)
    param_rows = [
        ("sales_batch", "tenant_id", "=", tenant_id),
        ("transaction", "sale_date", ">=", since_iso),
    ]
    for f in filters:
        attr = f["field"]
        if attr == "tenant_id":
            continue
        entity_type = scope_map.get(attr, "transaction")  # fallback
        if f["op"] in ("IN", "NOT IN") and isinstance(f["value"], list):
            param_rows.append((entity_type, attr, f["op"], "{" + ",".join(f["value"]) + "}"))
        else:
            param_rows.append((entity_type, attr, f["op"], f["value"]))
        param_rows.append((entity_type, attr, f["op"], f["value"]))

    param_sql_rows = ",\n    ".join(
        f"('{etype}', '{attr}', '{op}', '{val}')" for etype, attr, op, val in param_rows
    )

    logging.warning(f"param_sql_rows: {param_sql_rows}")

    # 5. Compose query
    query = f"""
WITH
/* params: runtime inputs (tiny; inline constants or bind parameters in app layer) */
params(entity_type_name, attribute_name, comparator, value) AS (
  VALUES
    {param_sql_rows}
),
/* tenant_param: optional tenant scope (single-row; reused)
   MATERIALIZED because it's tiny and referenced in attr_resolved. */
tenant_param AS MATERIALIZED (
  SELECT value AS tenant_id
  FROM params
  WHERE entity_type_name = 'sales_batch'
    AND attribute_name = 'tenant_id'
  LIMIT 1
),
/* type_ids: map entity type names → ids (reused several times)
   MATERIALIZED to avoid re-scanning sales.entity_types. */
type_ids AS MATERIALIZED (
  SELECT
    (ARRAY_AGG(entity_type_id) FILTER (WHERE name = 'address'))[1] AS address_id,
    (ARRAY_AGG(entity_type_id) FILTER (WHERE name = 'line_item'))[1] AS line_item_id,
    (ARRAY_AGG(entity_type_id) FILTER (WHERE name = 'transaction'))[1] AS transaction_id,
    (ARRAY_AGG(entity_type_id) FILTER (WHERE name = 'sales_batch'))[1] AS sales_batch_id
  FROM sales.entity_types
),
/* Decide which attributes we will either filter by or project. */
required_from_params AS (
  SELECT DISTINCT p.entity_type_name, p.attribute_name
  FROM params p
),
required_for_path_and_projection AS (
  SELECT * FROM (VALUES
    ('line_item', 'billing_address_id'),
    ('transaction', 'billing_address_id'),
    ('address', 'delivery_line_1'),
    ('address', 'delivery_line_2'),
    ('address', 'city_name'),
    ('address', 'state_abbreviation'),
    ('address', 'zipcode'),
    ('address', 'plus4_code'),
    ('address', 'street_name'),
    ('address', 'primary_number'),
    ('address', 'latitude'),
    ('address', 'longitude')
  ) AS v(entity_type_name, attribute_name)
),
/* needed_pairs: union of filter + projection attribute references (small) */
needed_pairs AS (
  SELECT entity_type_name, attribute_name FROM required_from_params
  UNION ALL
  SELECT entity_type_name, attribute_name FROM required_for_path_and_projection
),
/* needed_attrs: carry additional names used for tenant header mapping */
needed_attrs(entity_type_name, attribute_name, default_attr_name, mapped_header) AS (
  SELECT
    np.entity_type_name,
    np.attribute_name,
    np.attribute_name AS default_attr_name,
    np.attribute_name AS mapped_header
  FROM needed_pairs np
),
/* attr_resolved: finalize attribute_id + data_type with tenant-aware overrides (reused)
   MATERIALIZED to pin tiny lookup results (saves rejoining attributes + header map). */
attr_resolved AS MATERIALIZED (
  SELECT DISTINCT ON (na.entity_type_name, na.attribute_name)
    na.entity_type_name,
    na.attribute_name,
    COALESCE(cm.attribute_id, a.id) AS attribute_id,
    lower(
      COALESCE(
        (SELECT a2.data_type::text
         FROM sales.attributes a2
         WHERE a2.id = cm.attribute_id),
        a.data_type::text
      )
    ) AS data_type
  FROM needed_attrs na
  LEFT JOIN tenant_param tp ON TRUE
  LEFT JOIN sales.client_header_map cm
    ON cm.tenant_id = tp.tenant_id
   AND cm.mapped_header = na.mapped_header
  LEFT JOIN sales.entity_types et
    ON et.name = na.entity_type_name
  LEFT JOIN sales.attributes a
    ON a.entity_type_id = et.entity_type_id
   AND a.name = na.default_attr_name
  ORDER BY na.entity_type_name, na.attribute_name,
           CASE WHEN cm.attribute_id IS NOT NULL THEN 1 ELSE 2 END
),
/* filter_params: bind params → exact entity_type_id/attribute_id (tiny; reused) */
filter_params AS MATERIALIZED (
  SELECT
    row_number() OVER () AS param_id,
    p.entity_type_name,
    p.attribute_name,
    lower(p.comparator) AS comparator,
    p.value,
    et.entity_type_id,
    ar.attribute_id,
    ar.data_type
  FROM params p
  LEFT JOIN sales.entity_types et
    ON et.name = p.entity_type_name
  LEFT JOIN attr_resolved ar
    ON ar.entity_type_name = p.entity_type_name
   AND ar.attribute_name = p.attribute_name
),
/* filter_params_tx: only gating params that apply at transaction/sales_batch level (reused) */
filter_params_tx AS MATERIALIZED (
  SELECT * FROM filter_params
  WHERE entity_type_name IN ('transaction','sales_batch')
),
/* param_matches: per-param bitmap of tx_ids that satisfy that param (likely inlined)
   Not MATERIALIZED to let the planner push predicates and avoid unnecessary spooling. */
param_matches AS (
  /* transaction-scoped params */
  SELECT DISTINCT
    fp.param_id,
    ev.entity_id AS tx_id
  FROM filter_params_tx fp
  JOIN sales.entity_attribute_values ev
    ON ev.attribute_id = fp.attribute_id
   AND (
     CASE fp.data_type
       WHEN 'string' THEN
         CASE fp.comparator
           WHEN '=' THEN ev.value_string = fp.value
           WHEN '!=' THEN ev.value_string <> fp.value
           WHEN 'like' THEN ev.value_string LIKE fp.value
           WHEN 'ilike' THEN ev.value_string ILIKE fp.value
           WHEN 'in' THEN ev.value_string = ANY(string_to_array(fp.value, ','))
           WHEN 'not in' THEN NOT (ev.value_string = ANY(string_to_array(fp.value, ',')))
           ELSE FALSE
         END
       WHEN 'timestamptz' THEN
         CASE fp.comparator
           WHEN '=' THEN ev.value_ts = fp.value::timestamptz
           WHEN '!=' THEN ev.value_ts <> fp.value::timestamptz
           WHEN '>' THEN ev.value_ts > fp.value::timestamptz
           WHEN '<' THEN ev.value_ts < fp.value::timestamptz
           WHEN '>=' THEN ev.value_ts >= fp.value::timestamptz
           WHEN '<=' THEN ev.value_ts <= fp.value::timestamptz
           WHEN 'in' THEN ev.value_ts = ANY(string_to_array(fp.value, ',')::timestamptz[])
           WHEN 'not in' THEN NOT (ev.value_ts = ANY(string_to_array(fp.value, ',')::timestamptz[]))
           ELSE FALSE
         END
       WHEN 'numeric' THEN
         CASE fp.comparator
           WHEN '=' THEN ev.value_numeric = fp.value::numeric
           WHEN '!=' THEN ev.value_numeric <> fp.value::numeric
           WHEN '>' THEN ev.value_numeric > fp.value::numeric
           WHEN '<' THEN ev.value_numeric < fp.value::numeric
           WHEN '>=' THEN ev.value_numeric >= fp.value::numeric
           WHEN '<=' THEN ev.value_numeric <= fp.value::numeric
           WHEN 'in' THEN ev.value_numeric = ANY(string_to_array(fp.value, ',')::numeric[])
           WHEN 'not in' THEN NOT (ev.value_numeric = ANY(string_to_array(fp.value, ',')::numeric[]))
           ELSE FALSE
         END
       ELSE FALSE
     END
   )
  WHERE fp.entity_type_name = 'transaction'

  UNION ALL

  /* sales-batch–scoped params → expand matching batches to their child transactions */
  SELECT DISTINCT
    fp.param_id,
    t.id AS tx_id
  FROM filter_params_tx fp
  JOIN sales.entity_attribute_values ev
    ON ev.attribute_id = fp.attribute_id
   AND (
     CASE fp.data_type
       WHEN 'string' THEN
         CASE fp.comparator
           WHEN '=' THEN ev.value_string = fp.value
           WHEN '!=' THEN ev.value_string <> fp.value
           WHEN 'like' THEN ev.value_string LIKE fp.value
           WHEN 'ilike' THEN ev.value_string ILIKE fp.value
           WHEN 'in' THEN ev.value_string = ANY(string_to_array(fp.value, ','))
           WHEN 'not in' THEN NOT (ev.value_string = ANY(string_to_array(fp.value, ',')))
           ELSE FALSE
         END
       WHEN 'timestamptz' THEN
         CASE fp.comparator
           WHEN '=' THEN ev.value_ts = fp.value::timestamptz
           WHEN '!=' THEN ev.value_ts <> fp.value::timestamptz
           WHEN '>' THEN ev.value_ts > fp.value::timestamptz
           WHEN '<' THEN ev.value_ts < fp.value::timestamptz
           WHEN '>=' THEN ev.value_ts >= fp.value::timestamptz
           WHEN '<=' THEN ev.value_ts <= fp.value::timestamptz
           WHEN 'in' THEN ev.value_ts = ANY(string_to_array(fp.value, ',')::timestamptz[])
           WHEN 'not in' THEN NOT (ev.value_ts = ANY(string_to_array(fp.value, ',')::timestamptz[]))
           ELSE FALSE
         END
       WHEN 'numeric' THEN
         CASE fp.comparator
           WHEN '=' THEN ev.value_numeric = fp.value::numeric
           WHEN '!=' THEN ev.value_numeric <> fp.value::numeric
           WHEN '>' THEN ev.value_numeric > fp.value::numeric
           WHEN '<' THEN ev.value_numeric < fp.value::numeric
           WHEN '>=' THEN ev.value_numeric >= fp.value::numeric
           WHEN '<=' THEN ev.value_numeric <= fp.value::numeric
           WHEN 'in' THEN ev.value_numeric = ANY(string_to_array(fp.value, ',')::numeric[])
           WHEN 'not in' THEN NOT (ev.value_numeric = ANY(string_to_array(fp.value, ',')::numeric[]))
           ELSE FALSE
         END
       ELSE FALSE
     END
   )
  JOIN type_ids ti ON TRUE
  JOIN sales.entities t
    ON t.parent_entity_id = ev.entity_id
   AND t.entity_type_id = ti.transaction_id
  WHERE fp.entity_type_name = 'sales_batch'
),
/* param_card: scalar with number of gating params (tiny; reused in HAVING) */
param_card AS MATERIALIZED (
  SELECT COUNT(*) AS n_params
  FROM filter_params_tx
),
/* candidate_tx: tx that satisfy ALL gating params (reused; sharply reduces working set)
   MATERIALIZED because it’s referenced multiple times downstream (tx + line_item branches). */
candidate_tx AS MATERIALIZED (
  SELECT pm.tx_id
  FROM param_matches pm
  GROUP BY pm.tx_id
  HAVING COUNT(*) = (SELECT n_params FROM param_card)
),
/* billing_attrs: attribute_ids for billing_address_id (tx + line_item)
   MATERIALIZED to reuse across the tx and line_item address fetches. */
billing_attrs AS MATERIALIZED (
  SELECT attribute_id, entity_type_name
  FROM attr_resolved
  WHERE attribute_name = 'billing_address_id'
    AND entity_type_name IN ('line_item','transaction')
),
/* addr_rows: resolve raw address-id strings referenced by candidates (no dedupe yet)
   Non-materialized: lets planner push the candidate_tx filter and avoid large intermediate storage. */
addr_rows AS (
  /* tx-level */
  SELECT v.value_string AS addr_text
  FROM billing_attrs ba
  JOIN attr_resolved ar
    ON ar.attribute_id = ba.attribute_id
   AND ba.entity_type_name = 'transaction'
  JOIN candidate_tx ct ON TRUE
  JOIN sales.entity_attribute_values v
    ON v.entity_id = ct.tx_id
   AND v.attribute_id = ar.attribute_id

  UNION ALL

  /* line_item-level */
  SELECT v.value_string AS addr_text
  FROM billing_attrs ba
  JOIN attr_resolved ar
    ON ar.attribute_id = ba.attribute_id
   AND ba.entity_type_name = 'line_item'
  JOIN type_ids ti ON TRUE
  JOIN candidate_tx ct ON TRUE
  JOIN sales.entities li
    ON li.parent_entity_id = ct.tx_id
   AND li.entity_type_id = ti.line_item_id
  JOIN sales.entity_attribute_values v
    ON v.entity_id = li.id
   AND v.attribute_id = ar.attribute_id
),
/* addresses: validate → cast → dedupe to one row per address entity (reused)
   MATERIALIZED to provide a small, reusable id list for the final pivot. */
addresses AS MATERIALIZED (
  SELECT DISTINCT e.id AS address_id
  FROM addr_rows r
  JOIN sales.entities e ON e.id = r.addr_text::uuid
  JOIN type_ids ti ON e.entity_type_id = ti.address_id
),
/* addr_text: single-pass pivot of address text fields (IO saver)
   Non-materialized; planner can push the IN (...) semi-join and aggregate only over the small id set. */
addr_text AS (
  SELECT
    v.entity_id AS address_id,
    MAX(v.value_string) FILTER (WHERE ar.attribute_name = 'delivery_line_1') AS address,
    MAX(v.value_string) FILTER (WHERE ar.attribute_name = 'delivery_line_2') AS delivery_line_2,
    MAX(v.value_string) FILTER (WHERE ar.attribute_name = 'city_name') AS city,
    MAX(v.value_string) FILTER (WHERE ar.attribute_name = 'state_abbreviation') AS state,
    MAX(v.value_string) FILTER (WHERE ar.attribute_name = 'zipcode') AS "zipCode",
    MAX(v.value_string) FILTER (WHERE ar.attribute_name = 'plus4_code') AS "plus4Code",
    MAX(v.value_string) FILTER (WHERE ar.attribute_name = 'street_name') AS street_name,
    MAX(v.value_string) FILTER (WHERE ar.attribute_name = 'primary_number') AS primary_number,
    MAX(v.value_string) FILTER (WHERE ar.attribute_name = 'latitude') AS latitude,
    MAX(v.value_string) FILTER (WHERE ar.attribute_name = 'longitude') AS longitude
  FROM sales.entity_attribute_values v
  JOIN attr_resolved ar
    ON ar.attribute_id = v.attribute_id
   AND ar.entity_type_name = 'address'
  WHERE v.entity_id IN (SELECT address_id FROM addresses) -- <<< scope aggregation to deduped ids
  GROUP BY v.entity_id
)
/* final projection: addresses as a neat, typed rowset */
SELECT
  at.address,
  at.delivery_line_2,
  at.city,
  at.state,
  at."zipCode",
  at."plus4Code",
  at.street_name,
  at.primary_number,
  at.latitude::float,
  at.longitude::float
FROM addr_text at
""".strip()

    return query.replace('%', '%%')