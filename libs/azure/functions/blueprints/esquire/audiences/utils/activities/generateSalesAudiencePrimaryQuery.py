# File: /libs/azure/functions/blueprints/esquire/audiences/builder/activities/fetchAudience.py

from azure.durable_functions import Blueprint
from libs.azure.functions.blueprints.esquire.audiences.builder.utils import (
    jsonlogic_to_sql,
)
# import logging
from datetime import datetime, timedelta, timezone

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_generateSalesAudiencePrimaryQuery(ingress: dict):
    """
    Build a wide-form SQL string (PostgreSQL) without *_attrs JSON blobs.
    - Pivots requested fields for BOTH transaction and line_item.
    - Pulls tenant_id from the parent sales_batch (via t.batch_id) and prefers that.
    - Always adds the 6 billing address columns.
    """
    """
    Wide-form SQL (PostgreSQL) without *_attrs JSON blobs.
    - Pivots requested fields for BOTH transaction and line_item.
    - Pulls tenant_id from the parent sales_batch (prefers batch).
    - Includes address columns from either billing or shipping per ingress['address_source'].
    - Applies optional audience filter via jsonlogic_to_sql on the final wide result.
    """
    tenant_id = ingress['tenant_id']

    # ---------- helpers ----------
    def _sql_lit(val: str) -> str:
        return str(val).replace("'", "''")

    def _ident(val: str) -> str:
        return '"' + str(val).replace('"', '""') + '"'

    def _mk_value_expr(prefix: str = "eav") -> str:
        return (
            "CASE a.data_type "
            " WHEN 'string'      THEN {p}.value_string "
            " WHEN 'numeric'     THEN ({p}.value_numeric)::text "
            " WHEN 'boolean'     THEN ({p}.value_boolean)::text "
            " WHEN 'timestamptz' THEN ({p}.value_ts)::text "
            " WHEN 'jsonb'       THEN ({p}.value_jsonb)::text "
            " END"
        ).format(p=prefix)

    # do not create coalescences or the like for address based information. just keep it in the final where
    fields = list(field for field in ingress.get("fields") if field not in ["days_back", "zipcode", "state_abbreviation", "city_name", "plus4_code", "latitude", "longitude"] or [])
    depth = (ingress.get("depth") or "line_item").lower()
    if depth not in ("transaction", "line_item"):
        raise ValueError("ingress['depth'] must be 'transaction' or 'line_item'")

    # address source: "billing" or "shipping"
    addr_source = (ingress.get("address_source") or "billing").lower()
    if addr_source not in ("billing", "shipping"):
        raise ValueError("ingress['address_source'] must be 'billing' or 'shipping'")
    addr_attr_name = f"{addr_source}_address_id"  # e.g. billing_address_id / shipping_address_id

    tenant_lit = _sql_lit(tenant_id)
    addr_attr_name_lit = _sql_lit(addr_attr_name)

    # get 30 days prior to run (based on orchestrator context utc datetime for replay)
    days_back = ingress.get("days_back", 30)
    utc_now = ingress.get("utc_now")
    # logging.warning(f"[LOG] utc_now: {utc_now}")
    since_dt = datetime.strptime(utc_now, "%Y-%m-%d %H:%M:%S.%f%z") - timedelta(days=days_back)
    # logging.warning(f"[LOG] since_dt: {since_dt}")
    since_utc_lit = _sql_lit(since_dt.isoformat())
    # logging.warning(f"[LOG] since_utc_lit: {since_utc_lit}")

    # ---------- dynamic LATERALs for tx & li ----------
    def _build_attrs_lateral(alias: str, entity_ref: str) -> str:
        if not fields:
            return ""
        key_list = ", ".join("'" + _sql_lit(f) + "'" for f in fields)
        val_expr = _mk_value_expr("eav")

        cols = []
        for f in fields:
            cols.append(
                f"MAX(CASE COALESCE(chm.mapped_header, a.name) "
                f"        WHEN '{_sql_lit(f)}' THEN {val_expr} END) AS {_ident(f)}"
            )
        cols_sql = ",\n      ".join(cols)

        return f"""
LEFT JOIN LATERAL (
  SELECT
      {cols_sql}
  FROM sales.entity_attribute_values eav
  JOIN sales.attributes a ON a.id = eav.attribute_id
  LEFT JOIN sales.client_header_map chm
    ON chm.attribute_id = a.id AND chm.tenant_id = '{tenant_lit}'
  WHERE eav.entity_id = {entity_ref}
    AND COALESCE(chm.mapped_header, a.name) IN ({key_list})
) {alias} ON TRUE
""".rstrip()

    tx_join_sql = _build_attrs_lateral("txw", "t.id")
    li_join_sql = _build_attrs_lateral("liw", "li.line_item_id")

    # ---------- batch (sales_batch) tenant_id ----------
    batch_val = _mk_value_expr("eav")
    batch_join_sql = f"""
LEFT JOIN LATERAL (
  SELECT
      MAX(CASE COALESCE(chm.mapped_header, a.name)
            WHEN 'tenant_id' THEN {batch_val} END) AS tenant_id
  FROM sales.entity_attribute_values eav
  JOIN sales.attributes a ON a.id = eav.attribute_id
  LEFT JOIN sales.client_header_map chm
    ON chm.attribute_id = a.id AND chm.tenant_id = '{tenant_lit}'
  WHERE eav.entity_id = t.batch_id
    AND COALESCE(chm.mapped_header, a.name) IN ('tenant_id')
) batchw ON TRUE
""".rstrip()

    # ---------- address lateral (fixed set) ----------
    addr_fields = [
        ("delivery_line_1"              , "delivery_line_1"),
        ("delivery_line_2"              , "delivery_line_2"),
        ("city_name"                    , "city_name"),
        ("state_abbreviation"           , "state_abbreviation"),
        ("zipcode"                      , "zipcode"),
        ("plus4_code"                   , "plus4_code"),
        ("latitude"                     , "latitude"),
        ("longitude"                    , "longitude"),
    ]
    
    addr_names_in = ", ".join("'" + _sql_lit(k) + "'" for k, _ in addr_fields)
    addr_val = _mk_value_expr("eav")
    addr_cols = []
    for k, alias in addr_fields:
        addr_cols.append(
            f"MAX(CASE a.name WHEN '{_sql_lit(k)}' THEN {addr_val} END) AS {_ident(alias)}"
        )
    addr_cols_sql = ",\n      ".join(addr_cols)

    addr_join_sql = f"""
LEFT JOIN LATERAL (
  SELECT
      {addr_cols_sql}
  FROM sales.entity_attribute_values eav
  JOIN sales.attributes a ON a.id = eav.attribute_id
  WHERE eav.entity_id = addr_e.id
    AND a.name IN ({addr_names_in})
) addrw ON TRUE
""".rstrip()

    # ---------- SELECT list ----------
    select_bits = []
    for f in fields:
        col = _ident(f)
        if f == "tenant_id":
            select_bits.append(f"COALESCE(batchw.tenant_id, txw.{col}, liw.{col}) AS {col}")
        else:
            if depth == "transaction":
                select_bits.append(f"COALESCE(txw.{col}, liw.{col}) AS {col}")
            else:
                select_bits.append(f"COALESCE(liw.{col}, txw.{col}) AS {col}")

    # Always include address columns
    select_bits.append("addrw.*")
    inner_select_sql = ",\n  ".join(select_bits)

    # ---------- the wide SELECT ----------
    wide_sql = f"""
WITH etypes AS (
  SELECT
    (SELECT entity_type_id FROM sales.entity_types WHERE name = 'sales_batch') AS sales_batch_type_id,
    (SELECT entity_type_id FROM sales.entity_types WHERE name = 'transaction') AS transaction_type_id,
    (SELECT entity_type_id FROM sales.entity_types WHERE name = 'line_item')  AS line_item_type_id,
    (SELECT entity_type_id FROM sales.entity_types WHERE name = 'address')    AS address_type_id
),
batches AS (
  SELECT e.id
  FROM sales.entities e
  CROSS JOIN etypes t
  WHERE e.entity_type_id = t.sales_batch_type_id
    AND EXISTS (
      SELECT 1
      FROM sales.entity_attribute_values eav
      WHERE eav.entity_id = e.id
        AND eav.value_string = '{tenant_lit}'
    )
),
tx AS (
  SELECT e.id,
         e.parent_entity_id AS batch_id
  FROM sales.entities e
  CROSS JOIN etypes t
  WHERE e.entity_type_id = t.transaction_type_id
    AND e.parent_entity_id IN (SELECT id FROM batches)
    AND EXISTS (
      SELECT 1
      FROM sales.entity_attribute_values eav
      JOIN sales.attributes a ON a.id = eav.attribute_id
      LEFT JOIN sales.client_header_map chm
        ON chm.attribute_id = a.id AND chm.tenant_id = '{tenant_lit}'
      WHERE eav.entity_id = e.id
        AND COALESCE(chm.mapped_header, a.name) = 'sale_date'
        AND eav.value_ts > '{since_utc_lit}'::timestamptz
    )
),
line_items AS (
  SELECT li.id AS line_item_id, li.parent_entity_id AS transaction_id
  FROM sales.entities li
  CROSS JOIN etypes t
  WHERE li.entity_type_id = t.line_item_type_id
    AND li.parent_entity_id IN (SELECT id FROM tx)
),
addr_ids AS (
  SELECT li.line_item_id AS line_item_id, eav.value_string AS addr_id
  FROM line_items li
  JOIN sales.entity_attribute_values eav ON eav.entity_id = li.line_item_id
  JOIN sales.attributes a ON a.id = eav.attribute_id
  WHERE a.name = '{addr_attr_name_lit}'
)
SELECT
  {inner_select_sql}
FROM line_items li
JOIN tx t ON t.id = li.transaction_id
LEFT JOIN addr_ids ai ON ai.line_item_id = li.line_item_id
LEFT JOIN sales.entities addr_e
  ON addr_e.entity_type_id = (SELECT address_type_id FROM etypes)
 AND addr_e.id::text = ai.addr_id

{tx_join_sql}

{li_join_sql}

{batch_join_sql}

{addr_join_sql}
""".strip()

    ingress['audience']['dataFilter'] = remove_days_back_clause(ingress['audience']['dataFilter'])
    # ---------- OUTER FILTER ----------
    # build the select list with aliases but no duplicates
    address_alias_map = {
        "delivery_line_1": "address",
        "city_name": "city",
        "state_abbreviation": "state",
        "plus4_code": "plus4Code",
        "zipcode": "zipCode",
    }

    # figure out all columns in typed CTE (fields + address columns)
    # fields already comes from ingress
    all_columns = list(fields) + [
        "delivery_line_1",
        "delivery_line_2",
        "city_name",
        "state_abbreviation",
        "zipcode",
        "plus4_code",
        "latitude",
        "longitude",
    ]

    # build select clause
    select_cols = []
    for col in all_columns:
        alias = address_alias_map.get(col)
        if alias:
            select_cols.append(f'typed."{col}" AS "{alias}"')
        else:
            select_cols.append(f'typed."{col}"')

    # join into SQL
    select_clause = ",\n  ".join(select_cols)

    final_sql = f"""
    WITH base AS (
    {wide_sql}
    ){build_typed_cte_from_filter(ingress['audience']['dataFilter'])}
    SELECT
      {select_clause}
    FROM typed
    WHERE {ingress['audience']['dataFilter']}
    """.strip()

    return final_sql

import re

def build_typed_cte_from_filter(data_filter: str) -> str:
    """
    Infer casts from a dataFilter clause and build a typed CTE
    that only selects the casted/filter columns + address fields.
    """

    # Regex: find ("col" OP value...) patterns
    pattern = r'"\s*([^"]+)\s*"\s*(=|!=|<>|>=|<=|>|<|IN)\s*(\([^)]+\)|[^)ANDOR]+)'
    matches = re.findall(pattern, data_filter)

    casts = []
    seen = set()

    for col, op, val in matches:
        if col in seen:
            continue
        seen.add(col)

        val = val.strip()

        # Decide cast type
        if val.startswith("'") or op.upper() == "IN":
            expr = f'    base."{col}"'
        elif val.lower() in ("true", "false"):
            expr = f'    base."{col}"::boolean AS "{col}"'
        elif re.match(r"'?\d{4}-\d{2}-\d{2}.*'?", val):  # date-like
            expr = f'    base."{col}"::timestamptz AS "{col}"'
        elif re.match(r"^-?\d+(\.\d+)?$", val):          # number
            expr = f'    base."{col}"::numeric AS "{col}"'
        else:
            expr = f'    base."{col}"'

        casts.append(expr)

    # Always include address columns
    addr_fields = [
        "delivery_line_1", "delivery_line_2", "city_name", "state_abbreviation",
        "zipcode", "plus4_code", "latitude", "longitude"
    ]
    for col in addr_fields:
        if col not in seen:  # avoid double inclusion
            casts.append(f'    base."{col}"')

    return (
        ", typed AS (\n"
        "  SELECT\n"
        + ",\n".join(casts) +
        "\n  FROM base\n)"
    )


def remove_days_back_clause(data_filter: str) -> str:
    import re
    # Define a pattern for any comparison to "days_back"
    operator_pattern = r'(=|!=|<>|>=|<=|<|>)'
    clause_pattern = rf'\(*\s*"days_back"\s*{operator_pattern}\s*\d+\s*\)*'

    # This function handles replacing and rebalancing
    def balanced_removal(text):
        # Remove logical connectors + clause
        full_pattern = rf'''
            # Middle
            (\s+(AND|OR)\s+{clause_pattern})|
            # Start
            (^({clause_pattern})\s+(AND|OR)\s+)|
            # End
            (\s+(AND|OR)\s+{clause_pattern}$)|
            # Only clause
            (^({clause_pattern})$)
        '''
        cleaned = re.sub(full_pattern, '', text, flags=re.IGNORECASE | re.VERBOSE).strip()

        # Final fallback: remove bare clause if missed
        cleaned = re.sub(clause_pattern, '', cleaned, flags=re.IGNORECASE).strip()

        # Remove dangling operators at start/end
        cleaned = re.sub(r'^(AND|OR)\s+', '', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'\s+(AND|OR)$', '', cleaned, flags=re.IGNORECASE)

        # Fix unbalanced parentheses
        open_parens = cleaned.count('(')
        close_parens = cleaned.count(')')
        if open_parens > close_parens:
            cleaned += ')' * (open_parens - close_parens)
        elif close_parens > open_parens:
            cleaned = '(' * (close_parens - open_parens) + cleaned

        return cleaned

    return balanced_removal(data_filter)