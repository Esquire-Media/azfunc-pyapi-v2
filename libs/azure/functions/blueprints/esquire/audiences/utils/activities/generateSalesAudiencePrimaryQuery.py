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
    Build the primary SQL for the Sales Audience using the
    `sales.fn_find_matching_addresses(...)` pattern and JSONLogic-style filters.

    Updates per requirements:
    - Prefer the tenant specified inside ingress["audience"]["dataFilterRaw"] (JSONLogic)
      if present. Only fall back to ingress["tenant_id"] when JSONLogic does not provide
      a tenant. If neither is present, we raise a clear error.
    - ingress["audience"]["dataFilterRaw"] may be a JSON string or an object
      (both supported).
    - `days_back` is optional. If present (either in JSONLogic or ingress fallback),
      we add:
          sale_date >= NOW() - INTERVAL '<days_back> DAY'
      If not present anywhere, we do **not** apply a date filter.
    - Address-scoped vars: city, state_abbreviation, zipcode
    - Transaction-scoped vars: store_location, brand, category, description,
      default_sale_amount
    - Custom dynamic attribute/value support:
        The attribute **name** comes from `custom.field`, and the **value** used in
        comparisons comes from either `custom.numeric_value` or `custom.text_value`.

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
        - We ignore invalid attribute names (must match ^[A-Za-z_][A-Za-z0-9_]*$)
          with a warning.
        - If multiple custom value constraints exist, they are emitted as separate
          filter objects and ANDed together by the database function.

    Supported simple JSONLogic atoms accepted from the existing ingress:
      ==, !=, >, <, >=, <=, in

    Supported shapes for atoms:
      { "<op>": [ {"var":"name"}, <const> ] }
      { "<op>": [ <const>, {"var":"name"} ] }  # normalized by inverting order-sensitive ops

    The database function infers each filter's data type from the populated
    scalar value field:
      - `value_string` for string equality
      - `value_strings` for string membership
      - `value_numeric` for numeric comparisons
      - `value_boolean` for boolean equality
      - `value_ts` for timestamp comparisons
      - `value_jsonb` for JSONB equality or containment

    The existing ingress currently generates:
      - string filters: ==, in
      - numeric filters: >, >=
      - timestamptz filters: >= through `days_back`

    `days_back` generates the supported timestamptz >= filter. Unsupported
    field/operator combinations raise a clear error instead of generating a
    filter that the database function cannot evaluate.

    As in the prior activity, compound JSONLogic nodes are walked recursively.
    The generated database filter array is evaluated with AND semantics.

    The generated SQL mirrors the provided "new query" and injects filter
    objects dynamically.
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
    # SQL rendering helpers
    # -----------------------------
    def sql_string(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    def render_text_array(values: Any) -> str:
        if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
            raise ValueError(
                f"Expected a list of string filter values, received {values!r}."
            )
        return "ARRAY[" + ", ".join(sql_string(value) for value in values) + "]::text[]"

    def render_filter(
        scope: str,
        logical_name: str,
        value_type: str,
        expr: Dict[str, Any],
    ) -> str:
        if len(expr) != 1:
            raise ValueError(f"Invalid sales filter expression: {expr!r}.")

        (jsonlogic_op, value), = expr.items()

        if value_type == "string":
            if jsonlogic_op == "==":
                if not isinstance(value, str):
                    raise ValueError(
                        f"String equality requires a string value, received {value!r}."
                    )
                function_op = "eq"
                value_key = "value_string"
                value_sql = sql_string(value)
            elif jsonlogic_op == "in":
                function_op = "in"
                value_key = "value_strings"
                value_sql = render_text_array(value)
            else:
                raise ValueError(
                    "sales.fn_find_matching_addresses supports only '==' and 'in' "
                    f"for string filters; received {jsonlogic_op!r} for "
                    f"{logical_name!r}."
                )
        elif value_type == "numeric":
            if jsonlogic_op not in {">", ">="}:
                raise ValueError(
                    "sales.fn_find_matching_addresses supports only '>' and '>=' "
                    f"for numeric filters; received {jsonlogic_op!r} for "
                    f"{logical_name!r}."
                )
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(
                    f"Numeric filter {logical_name!r} requires a numeric value; "
                    f"received {value!r}."
                )
            function_op = "gt" if jsonlogic_op == ">" else "gte"
            value_key = "value_numeric"
            value_sql = str(value)
        else:
            raise ValueError(f"Unsupported sales filter type: {value_type!r}.")

        return (
            "jsonb_build_object(\n"
            f"      'scope',        {sql_string(scope)},\n"
            f"      'logical_name', {sql_string(logical_name)},\n"
            f"      'op',           {sql_string(function_op)},\n"
            f"      '{value_key}', {value_sql}\n"
            "    )"
        )

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
            # Preserve the value type so the database function receives the correct expected_type.
            custom_value_exprs.append(
                {
                    "value_type": (
                        "numeric" if var_name == "custom.numeric_value" else "string"
                    ),
                    "expr": {norm_op: const},
                }
            )
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
    # Build transaction filter objects
    # -----------------------------
    filter_sql_parts: List[str] = []

    # Optional sale_date filter (only if days_back present)
    if isinstance(days_back, int) and days_back >= 0:
        filter_sql_parts.append(
            "jsonb_build_object(\n"
            "      'scope',         'transaction',\n"
            "      'logical_name',  'sale_date',\n"
            "      'expected_type', 'timestamptz',\n"
            "      'op',            'gte',\n"
            f"      'value_ts',      now() - interval '{int(days_back)} days'\n"
            "    )"
        )

    # Explicit transaction vars
    for var, db_attr in transaction_vars.items():
        exprs = collected.get(var, [])
        if not exprs:
            continue

        value_type = "numeric" if var == "default_sale_amount" else "string"
        for expr in exprs:
            filter_sql_parts.append(
                render_filter(
                    scope="transaction",
                    logical_name=db_attr,
                    value_type=value_type,
                    expr=expr,
                )
            )

    # Attach custom dynamic field/value filters if present
    if custom_attr_name and custom_value_exprs:
        for custom_value_expr in custom_value_exprs:
            filter_sql_parts.append(
                render_filter(
                    scope="transaction",
                    logical_name=custom_attr_name,
                    value_type=custom_value_expr["value_type"],
                    expr=custom_value_expr["expr"],
                )
            )

    # -----------------------------
    # Build address filter objects
    # -----------------------------
    for var, db_attr in address_vars.items():
        exprs = collected.get(var, [])
        if not exprs:
            continue

        for expr in exprs:
            filter_sql_parts.append(
                render_filter(
                    scope="address",
                    logical_name=db_attr,
                    value_type="string",
                    expr=expr,
                )
            )

    filters_sql = "jsonb_build_array()"
    if filter_sql_parts:
        filters_sql = (
            "jsonb_build_array(\n    "
            + ",\n    ".join(filter_sql_parts)
            + "\n  )"
        )

    # -----------------------------
    # Final SQL
    # -----------------------------
    query = f"""
SELECT *
FROM sales.fn_find_matching_addresses(
  p_tenant_id => {sql_string(str(tenant_id))},
  p_filters   => {filters_sql}
)
""".strip()

    # Escape % for SQLAlchemy text() compatibility (keeps behavior consistent with prior code paths).
    return query.replace("%", "%%")
