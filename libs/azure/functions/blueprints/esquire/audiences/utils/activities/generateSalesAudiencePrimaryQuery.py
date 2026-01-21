from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Union, Optional

from azure.durable_functions import Blueprint

bp = Blueprint()

Json = Union[Dict[str, Any], List[Any], str, int, float, bool, None]


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_generateSalesAudiencePrimaryQuery(ingress: dict) -> str:
    """
    Build the primary SQL for the Sales Audience using the `sales.query_eav(...)` pattern and
    JSONLogic-style filters.

    Updates per requirements:
    - Prefer the tenant specified inside ingress["audience"]["dataFilter"] (JSONLogic) if present.
      Only fall back to ingress["tenant_id"] when JSONLogic does not provide a tenant.
      If neither is present, we raise a clear error.
    - ingress["audience"]["dataFilter"] may be a JSON string or an object (both supported).
    - `days_back` is optional. If present (either in JSONLogic or ingress fallback), we add:
          sale_date >= NOW() - INTERVAL '<days_back> DAY'
      If not present anywhere, we do **not** apply a date filter.
    - Address-scoped vars: city, state_abbreviation, zipcode
    - Transaction-scoped vars: store_location, brand, category, description, default_sale_amount
    - Custom dynamic attribute/value support:
        The attribute **name** comes from `custom.field`, and the **value** used in comparisons
        comes from either `custom.numeric_value` or `custom.text_value`.

        Examples:
          {"and":[
            {"==":[{"var":"custom.field"},"brand"]},
            {"in":[{"var":"custom.text_value"},["Sealy","Serta"]]}
          ]}
            → filters: brand IN ["Sealy","Serta"]

          {"and":[
            {"==":[{"var":"custom.field"},"default_sale_amount"]},
            {">":[{"var":"custom.numeric_value"},0]}
          ]}
            → filters: default_sale_amount > 0

        Notes:
        - We ignore invalid attribute names (must match ^[A-Za-z_][A-Za-z0-9_]*$) with a warning.
        - If multiple custom value constraints exist, they are ANDed together for the chosen field.

    Supported simple JSONLogic atoms: ==, !=, >, <, >=, <=, in
    Supported shapes for atoms:
      { "<op>": [ {"var":"name"}, <const> ] }
      { "<op>": [ <const>, {"var":"name"} ] }  # normalized by inverting order-sensitive ops

    The generated SQL mirrors the provided "new query" and injects predicates dynamically.
    """

    # -----------------------------
    # Parse JSONLogic (string or object)
    # -----------------------------
    raw_logic: Any = (ingress.get("audience") or {}).get("dataFilterRaw") or {}
    if isinstance(raw_logic, str):
        try:
            json_logic: Json = json.loads(raw_logic)
        except Exception as e:
            logging.warning(
                "[AudiencePrimaryQuery] dataFilterRaw is a string but not valid JSON; "
                "defaulting to empty JSONLogic. Error: %s",
                e,
            )
            json_logic = {}
    else:
        json_logic = raw_logic

    # Optional ingress fallback for days_back (used only if JSONLogic omits it)
    ingress_days_back: Optional[int]
    try:
        ingress_days_back = int(ingress.get("days_back", "")) if ingress.get("days_back") is not None else None
    except Exception:
        ingress_days_back = None

    # -----------------------------
    # JSONLogic helpers
    # -----------------------------
    def is_var(node: Any, name: Optional[str] = None) -> bool:
        if isinstance(node, dict) and "var" in node:
            return True if name is None else (node.get("var") == name)
        return False

    invert_op = {"<": ">", "<=": ">=", ">": "<", ">=": "<=", "==": "==", "!=": "!="}

    # Collect constraints for non-special vars
    collected: Dict[str, List[Dict[str, Any]]] = {}

    def add_constraint(var_name: str, expr: Dict[str, Any]) -> None:
        collected.setdefault(var_name, []).append(expr)

    # Extract a single-tenant string value from JSONLogic if present as equality
    def extract_tenant_id(node: Any) -> Optional[str]:
        if isinstance(node, dict):
            if "and" in node and isinstance(node["and"], list):
                for child in node["and"]:
                    v = extract_tenant_id(child)
                    if isinstance(v, str):
                        return v
            elif "or" in node and isinstance(node["or"], list):
                for child in node["or"]:
                    v = extract_tenant_id(child)
                    if isinstance(v, str):
                        return v
            elif len(node) == 1:
                (op, val), = node.items()
                if op == "==" and isinstance(val, list) and len(val) == 2:
                    left, right = val
                    if is_var(left, "tenant_id") and isinstance(right, str):
                        return right
                    if is_var(right, "tenant_id") and isinstance(left, str):
                        return left
        return None

    # Extract days_back numeric (treat any comparator as a target window)
    def extract_days_back(node: Any) -> Optional[int]:
        if isinstance(node, dict):
            if "and" in node and isinstance(node["and"], list):
                for child in node["and"]:
                    v = extract_days_back(child)
                    if isinstance(v, int):
                        return v
            elif "or" in node and isinstance(node["or"], list):
                for child in node["or"]:
                    v = extract_days_back(child)
                    if isinstance(v, int):
                        return v
            elif len(node) == 1:
                (op, val), = node.items()
                if op in {"==", ">=", "<=", ">", "<"} and isinstance(val, list) and len(val) == 2:
                    left, right = val
                    if is_var(left, "days_back") and isinstance(right, (int, float)):
                        return int(right)
                    if is_var(right, "days_back") and isinstance(left, (int, float)):
                        return int(left)
        return None

    # -----------------------------
    # Custom dynamic attribute/value accumulation
    # -----------------------------
    identifier_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
    custom_attr_name: Optional[str] = None
    custom_value_exprs: List[Dict[str, Any]] = []

    def handle_atom(op: str, left: Any, right: Any) -> None:
        nonlocal custom_attr_name  # we assign to this in the function

        # Allowed ops
        if op not in {"==", "!=", ">", "<", ">=", "<=", "in"}:
            return

        # Normalize so variable is on the left (var <op> const)
        if is_var(left) and not is_var(right):
            var_name = left["var"]
            const = right
            norm_op = op
        elif is_var(right) and not is_var(left):
            var_name = right["var"]
            const = left
            norm_op = invert_op.get(op, op)
            if op == "in":
                # JSONLogic 'in' is (needle IN haystack). If var is haystack, ambiguous → skip.
                return
        else:
            # both const or both vars → skip
            return

        # We'll source tenant from JSONLogic specially; don't store here.
        if var_name == "tenant_id":
            return

        # days_back is handled specially (optional date filter); don't store here.
        if var_name == "days_back":
            return

        # --- Custom dynamic field/value handling ---
        if var_name == "custom.field":
            # Only accept equality to a string as the attribute name.
            if norm_op == "==" and isinstance(const, str) and const.strip():
                if identifier_re.fullmatch(const.strip()):
                    custom_attr_name = const.strip()
                else:
                    logging.warning(
                        "[AudiencePrimaryQuery] Ignoring custom.field with invalid attribute name: %r", const
                    )
            return

        if var_name in {"custom.numeric_value", "custom.text_value"}:
            # Treat these as value-side comparisons for the dynamic attribute chosen via custom.field.
            # We just record the operator/constant pair; later we attach them to the resolved attribute.
            custom_value_exprs.append({norm_op: const})
            return

        # --- Default collection for regular vars ---
        add_constraint(var_name, {norm_op: const})

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if "and" in node and isinstance(node["and"], list):
                for child in node["and"]:
                    walk(child)
                return
            if "or" in node and isinstance(node["or"], list):
                for child in node["or"]:
                    walk(child)
                return
            if len(node) == 1:
                (op, val), = node.items()
                if isinstance(val, list) and len(val) == 2:
                    handle_atom(op, val[0], val[1])
                    return
        # Non-dict or unsupported shapes are ignored.

    # Walk once to populate `collected` for non-special vars and custom dynamic pieces.
    walk(json_logic)

    # Resolve tenant preference: JSONLogic > ingress
    tenant_from_logic = extract_tenant_id(json_logic)
    if tenant_from_logic is not None:
        tenant_id = tenant_from_logic
    else:
        # Only use ingress["tenant_id"] if JSONLogic tenant is absent
        tenant_id = ingress.get("tenant_id")

    if not tenant_id:
        # Without a tenant, we cannot resolve attribute names or scope sales_batches.
        raise ValueError(
            "Missing tenant_id: not provided in JSONLogic and ingress['tenant_id'] is absent."
        )

    # Resolve days_back (may be None → no date predicate)
    days_back = extract_days_back(json_logic)
    if days_back is None:
        days_back = ingress_days_back  # still possibly None

    # -----------------------------
    # Map vars to DB attributes
    # -----------------------------
    address_vars = {
        "city": "city_name",
        "state_abbreviation": "state_abbreviation",
        "zipcode": "zipcode",
    }
    transaction_vars = {
        "store_location": "store_location",
        "brand": "brand",
        "category": "category",
        "description": "description",
        "default_sale_amount": "default_sale_amount",
    }

    # -----------------------------
    # Build JSON for address filters (appended via || to base)
    # -----------------------------
    address_filter_obj: Dict[str, Any] = {}
    for var, db_attr in address_vars.items():
        exprs = collected.get(var, [])
        if not exprs:
            continue
        address_filter_obj[db_attr] = exprs[0] if len(exprs) == 1 else {"and": exprs}

    addr_filter_concat_sql = ""
    if address_filter_obj:
        addr_filter_json = json.dumps(address_filter_obj)
        addr_filter_concat_sql = f" || '{addr_filter_json}'::jsonb"

    # -----------------------------
    # Build JSON for transaction filters (excluding sale_date/parent link)
    # -----------------------------
    txn_attr_exprs: Dict[str, Any] = {}

    # Explicit transaction vars
    for var, db_attr in transaction_vars.items():
        exprs = collected.get(var, [])
        if not exprs:
            continue
        txn_attr_exprs[db_attr] = exprs[0] if len(exprs) == 1 else {"and": exprs}

    # Attach custom dynamic field/value if present
    if custom_attr_name and custom_value_exprs:
        exprs = custom_value_exprs
        if custom_attr_name in txn_attr_exprs:
            existing = txn_attr_exprs[custom_attr_name]
            if isinstance(existing, dict) and "and" in existing:
                existing["and"].extend(exprs)
            else:
                txn_attr_exprs[custom_attr_name] = {
                    "and": ([existing] if isinstance(existing, dict) else [existing]) + exprs
                }
        else:
            txn_attr_exprs[custom_attr_name] = exprs[0] if len(exprs) == 1 else {"and": exprs}

    # Render KV pairs for jsonb_build_object:
    #   sales.resolve_attribute_ids(c.tenant_id, '<attr>'), '<json>'::jsonb
    txn_kv_pairs_sql_parts: List[str] = []
    for db_attr, logic_obj in txn_attr_exprs.items():
        expr_json = json.dumps(logic_obj)
        txn_kv_pairs_sql_parts.append(
            f"sales.resolve_attribute_ids(c.tenant_id, '{db_attr}'), '{expr_json}'::jsonb"
        )

    # Optional sale_date pair (only if days_back present)
    sale_date_pair_sql = ""
    if isinstance(days_back, int) and days_back >= 0:
        sale_date_pair_sql = (
            "sales.resolve_attribute_ids(c.tenant_id, 'sale_date'), "
            f"jsonb_build_object('>=', NOW() - INTERVAL '{int(days_back)} DAY')"
        )

    all_kv_pairs: List[str] = []
    if sale_date_pair_sql:
        all_kv_pairs.append(sale_date_pair_sql)
    all_kv_pairs.extend(txn_kv_pairs_sql_parts)

    txn_filter_kvs_sql = ""
    if all_kv_pairs:
        txn_filter_kvs_sql = ",\n      " + ",\n      ".join(all_kv_pairs)

    # -----------------------------
    # Final SQL
    # -----------------------------
    query = f"""
WITH
-- Centralize constants so they're easy to change.
const AS (
  SELECT
    '{tenant_id}'::text AS tenant_id
),
-- Sales batches scoped to the tenant (prefer JSONLogic tenant, else ingress).
sales_batches AS (
  SELECT sb.entity_id
  FROM const c
  CROSS JOIN sales.query_eav(
    'sales_batch',
    jsonb_build_object('["tenant_id"]', jsonb_build_object('==', c.tenant_id))
  ) AS sb(entity_id uuid)
),
-- Transactions under those batches, filtered by optional date window and any txn-level predicates.
transactions AS (
  SELECT t.entity_id, t.billing_address_id
  FROM (
    SELECT COALESCE(jsonb_agg(entity_id), '[]'::jsonb) AS ids
    FROM sales_batches
  ) b
  CROSS JOIN const c
  CROSS JOIN sales.query_eav(
    'transaction',
    jsonb_build_object(
      'parent_entity_id', jsonb_build_object('in', b.ids){txn_filter_kvs_sql}
    ),
    ARRAY['billing_address_id']
  ) AS t(entity_id uuid, billing_address_id text)
),
-- Collect distinct billing address ids from line items tied to those transactions.
line_items AS (
  SELECT li.entity_id, li.shipping_address_id
  FROM (
    SELECT COALESCE(jsonb_agg(entity_id), '[]'::jsonb) AS ids
    FROM transactions
  ) ti
  CROSS JOIN sales.query_eav(
    'line_item',
    jsonb_build_object(
      'parent_entity_id', jsonb_build_object('in', ti.ids)
    ),
    ARRAY['shipping_address_id']
  ) AS li(entity_id uuid, shipping_address_id text)
),
-- Union of all address_ids we care about: shipping (line_items) + billing (transactions).
addresses AS (
  SELECT DISTINCT billing_address_id AS address_id
  FROM transactions
  WHERE billing_address_id IS NOT NULL

  UNION   -- de-duplicate addresses across shipping/billing

  SELECT DISTINCT shipping_address_id AS address_id
  FROM line_items
  WHERE shipping_address_id IS NOT NULL
)
-- Final address projection with consistent NULLIF handling and aliases.
SELECT
  NULLIF(a.delivery_line_1, 'NONE')    AS address,
  NULLIF(a.delivery_line_2, 'NONE')    AS delivery_line_2,
  NULLIF(a.city_name, 'NONE')          AS city,
  NULLIF(a.state_abbreviation, 'NONE') AS state,
  NULLIF(a.zipcode, 'NONE')            AS "zipCode",
  NULLIF(a.plus4_code, 'NONE')         AS "plus4Code",
  NULLIF(a.primary_number, 'NONE')     AS primary_number,
  NULLIF(a.street_name, 'NONE')        AS street_name,
  NULLIF(a.latitude, 'NONE')::float    AS latitude,
  NULLIF(a.longitude, 'NONE')::float   AS longitude
FROM (
  SELECT COALESCE(jsonb_agg(addr.address_id), '[]'::jsonb) AS ids
  FROM addresses AS addr
) ai
CROSS JOIN LATERAL sales.query_eav(
  'address',
  jsonb_build_object(
    'entity_id', jsonb_build_object('in', ai.ids)
  ){addr_filter_concat_sql},
  ARRAY['delivery_line_1','delivery_line_2','city_name','state_abbreviation','zipcode','plus4_code','primary_number','street_name','latitude','longitude']
) AS a(
  entity_id uuid,
  delivery_line_1 text,
  delivery_line_2 text,
  city_name text,
  state_abbreviation text,
  zipcode text,
  plus4_code text,
  primary_number text,
  street_name text,
  latitude text,
  longitude text
)
""".strip()

    # Escape % for SQLAlchemy text() compatibility (keeps behavior consistent with prior code paths).
    return query.replace("%", "%%")
