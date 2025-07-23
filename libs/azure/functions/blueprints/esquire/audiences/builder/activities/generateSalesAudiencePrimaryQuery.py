# File: /libs/azure/functions/blueprints/esquire/audiences/builder/activities/fetchAudience.py

from azure.durable_functions import Blueprint
from libs.azure.functions.blueprints.esquire.audiences.builder.utils import (
    jsonlogic_to_sql,
)
from libs.data import from_bind
from sqlalchemy import select
from sqlalchemy.orm import Session, lazyload

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_generateSalesAudiencePrimaryQuery(ingress: dict):
    tenant_id = ingress["tenant_id"]
    entity_type = ingress.get("entity_type", "transaction")
    depth = ingress.get("depth", "line_item")  # "transaction" or "line_item"
    address_source = ingress.get("address_source", "shipping")  # "shipping", "billing", or "both"

    provider = from_bind("sales")
    map_model = provider.models["sales"]["ClientHeaderMap"]
    attr_model = provider.models["sales"]["Attribute"]
    session = provider.connect()

    # Step 1: Fetch all mapped headers for this tenant
    query = (
        select(
            map_model.mapped_header,
            map_model.attribute_id,
            map_model.entity_type,
            attr_model.data_type
        )
        .join(attr_model, attr_model.id == map_model.attribute_id)
        .where(map_model.tenant_id == tenant_id)
    )
    result = session.execute(query).all()

    # Step 2: Build dynamic EAV joins and select fields
    joins = []
    fields = ["t.id AS transaction_id"]
    if depth == "line_item":
        fields.append("li.id AS line_item_id")

    join_counter = 0
    for row in result:
        header, attr_id, ent_type, data_type = row
        alias = f"attr_{join_counter}"
        join_counter += 1

        if ent_type == "transaction":
            entity_ref = "t.id"
        elif ent_type == "line_item" and depth == "line_item":
            entity_ref = "li.id"
        elif ent_type == "address":
            ref_map = {
                "shipping": ["sa.id"],
                "billing": ["ba.id"],
                "both": ["sa.id", "ba.id"]
            }
            refs = ref_map.get(address_source, [])
        else:
            continue

        value_col = {
            "string": "value_string",
            "numeric": "value_numeric",
            "boolean": "value_boolean",
            "timestamptz": "value_ts",
            "jsonb": "value_jsonb"
        }.get(data_type)

        if ent_type == "address":
            for ref in refs:
                a = f"{alias}_{ref.split('.')[0]}"
                joins.append(f"""
                    LEFT JOIN entity_attribute_values {a}
                        ON {a}.entity_id = {ref}
                       AND {a}.attribute_id = '{attr_id}'
                """)
                fields.append(f"{a}.{value_col} AS {header}")
        else:
            joins.append(f"""
                LEFT JOIN entity_attribute_values {alias}
                    ON {alias}.entity_id = {entity_ref}
                   AND {alias}.attribute_id = '{attr_id}'
            """)
            fields.append(f"{alias}.{value_col} AS {header}")

    # Step 3: Base FROM and JOINs
    base = f"""
    FROM entities sb
    JOIN entity_types et_sb ON et_sb.entity_type_id = sb.entity_type_id AND et_sb.name = 'sales_batch'
    JOIN entities t ON t.parent_entity_id = sb.id
    JOIN entity_types et_t ON et_t.entity_type_id = t.entity_type_id AND et_t.name = '{entity_type}'
    """

    if depth == "line_item":
        base += """
        JOIN entities li ON li.parent_entity_id = t.id
        JOIN entity_types et_li ON et_li.entity_type_id = li.entity_type_id AND et_li.name = 'line_item'
        """

    if address_source in ("shipping", "both"):
        base += "JOIN entities sa ON sa.id = li.shipping_address_id\n"
    if address_source in ("billing", "both"):
        base += "JOIN entities ba ON ba.id = t.billing_address_id\n"

    # Step 4: Final SQL
    return f"""
    SELECT {', '.join(fields)}
    {base}
    {' '.join(joins)}
    WHERE sb.tenant_id = '{tenant_id}'
    """
