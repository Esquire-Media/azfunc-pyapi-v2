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

    fields = list(ingress.get("fields") or [])
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
        ("addressee"                    , "addressee"),
        ("default_city_name"            , "default_city_name"),
        ("last_line"                    , "last_line"),
        ("delivery_point_barcode"       , "delivery_point_barcode"),
        ("urbanization"                 , "urbanization"),
        ("primary_number"               , "primary_number"),
        ("street_name"                  , "street_name"),
        ("street_predirection"          , "street_predirection"),
        ("street_postdirection"         , "street_postdirection"),
        ("street_suffix"                , "street_suffix"),
        ("secondary_number"             , "secondary_number"),
        ("secondary_designator"         , "secondary_designator"),
        ("extra_secondary_number"       , "extra_secondary_number"),
        ("extra_secondary_designator"   , "extra_secondary_designator"),
        ("pmb_designator"               , "pmb_designator"),
        ("pmb_number"                   , "pmb_number"),
        ("delivery_point"               , "delivery_point"),
        ("delivery_point_check_digit"   , "delivery_point_check_digit"),
        ("record_type"                  , "record_type"),
        ("zip_type"                     , "zip_type"),
        ("county_fips"                  , "county_fips"),
        ("county_name"                  , "county_name"),
        ("carrier_route"                , "carrier_route"),
        ("congressional_district"       , "congressional_district"),
        ("building_default_indicator"   , "building_default_indicator"),
        ("rdi"                          , "rdi"),
        ("elot_sequence"                , "elot_sequence"),
        ("elot_sort"                    , "elot_sort"),
        ("coordinate_license"           , "coordinate_license"),
        ("precision"                    , "precision"),
        ("time_zone"                    , "time_zone"),
        ("utc_offset"                   , "utc_offset"),
        ("obeys_dst"                    , "obeys_dst"),
        ("is_ews_match"                 , "is_ews_match"),
        ("dpv_match_code"               , "dpv_match_code"),
        ("dpv_footnotes"                , "dpv_footnotes"),
        ("cmra"                         , "cmra"),
        ("vacant"                       , "vacant"),
        ("active"                       , "active"),
        ("dpv_no_stat"                  , "dpv_no_stat"),
        ("footnotes"                    , "footnotes"),
        ("lacs_link_code"               , "lacs_link_code"),
        ("lacs_link_indicator"          , "lacs_link_indicator"),
        ("is_suite_link_match"          , "is_suite_link_match"),
        ("enhanced_match"               , "enhanced_match")
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

    # ---------- OUTER FILTER ----------
    final_sql = f"""
WITH base AS (
{wide_sql}
LIMIT 1000
)
SELECT *
FROM base
WHERE {ingress['audience']['dataFilter']}
""".strip()

    return final_sql