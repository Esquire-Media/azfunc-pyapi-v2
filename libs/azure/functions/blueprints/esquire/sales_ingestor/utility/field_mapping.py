from __future__ import annotations

from typing import Any


def _next_standardized_key(parent_key: str, key: str) -> str:
    """
    Match the existing field-flattening contract:
      - billing.*   -> billing_<field>
      - shipping.*  -> shipping_<field>
      - everything else -> <field>
    """
    if parent_key not in ("billing", "shipping"):
        parent_key = ""
    return f"{parent_key}_{key}" if parent_key else key


def flatten_standardized_to_original(
    fields: dict[str, Any],
    parent_key: str = "",
    result: dict[str, str] | None = None,
) -> dict[str, str]:
    """
    Convert nested request fields into:
        standardized_attribute -> original_header

    Example:
        {
          "order_info": {"order_num": "Order Number"},
          "location": {"store_location": "Store"},
          "billing": {"zipcode": "Bill Zip"}
        }

    becomes:
        {
          "order_num": "Order Number",
          "store_location": "Store",
          "billing_zipcode": "Bill Zip"
        }
    """
    if result is None:
        result = {}

    for key, value in fields.items():
        standardized_key = _next_standardized_key(parent_key, key)

        if isinstance(value, dict):
            flatten_standardized_to_original(value, standardized_key, result)
        elif isinstance(value, str) and value:
            result[standardized_key] = value

    return result

def build_raw_to_standardized_map(fields: dict) -> dict:
    out = {}

    for section in fields.values():
        for standardized, raw in section.items():
            if not raw or not raw.strip():
                continue

            raw = raw.strip()
            out.setdefault(raw, []).append(standardized)

    return out

def normalize_fields_to_standardized(
    fields: dict[str, Any],
    parent_key: str = "",
) -> dict[str, Any]:
    """
    Preserve the nested shape of settings['fields'], but replace every non-empty
    leaf value with its standardized attribute name.

    Example:
        {"order_info": {"order_num": "Order Number"}}
    becomes:
        {"order_info": {"order_num": "order_num"}}
    """
    normalized: dict[str, Any] = {}

    for key, value in fields.items():
        standardized_key = _next_standardized_key(parent_key, key)

        if isinstance(value, dict):
            normalized[key] = normalize_fields_to_standardized(value, standardized_key)
        elif isinstance(value, str):
            normalized[key] = standardized_key if value else value
        else:
            normalized[key] = value

    return normalized