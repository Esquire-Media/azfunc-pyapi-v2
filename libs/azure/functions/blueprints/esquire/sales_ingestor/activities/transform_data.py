from azure.durable_functions import Blueprint
import pandas as pd, io, os
from uuid import uuid4

from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.generate_ids import (
    generate_deterministic_id,
    NAMESPACE_SALE
)

bp = Blueprint()

@bp.activity_trigger(input_name="settings")
def activity_transformData(settings: dict):

    df = settings['sales']
    address_ids = set()
    addresses = []
    sales = []
    upload_sales = []
    sale_attributes = []
    line_items = []
    line_item_attributes = []

    exclude_fields =[
        'billing_street_cleaned',
        'shipping_street_cleaned',
        'billing_city_cleaned',
        'shipping_city_cleaned',
        'billing_state_cleaned',
        'shipping_state_cleaned',
        'billing_zipcode_cleaned',
        'shipping_zipcode_cleaned', 
        'billing_address_id',
        'shipping_address_id',
        'sales_index'
        ]

    for order_id, group in df.groupby(settings['header_info']['order_info']['order_num'], dropna=False):

        first_row = group.iloc[0]
        sale_id = generate_deterministic_id(NAMESPACE_SALE, [settings['metadata']['upload_id'], order_id])

        sales.append({
            'id': sale_id,
            'transaction_date': first_row[settings['header_info']['order_info']['sale_date']],
            'sale_address_id': first_row['billing_address_id']
        })

        upload_sales.append({
            'upload_id': settings['metadata']['upload_id'],
            'sale_id': sale_id
        })

        if first_row['billing_address_id'] not in address_ids:
            address_ids.add(first_row['billing_address_id'])
            addresses.append(first_row[['billing_street_cleaned','billing_city_cleaned','billing_state_cleaned','billing_zipcode_cleaned', 'billing_address_id']].to_dict())

        shared_attrs = extract_shared_attributes(group, exclude_fields)
        sale_attributes.extend([{'sale_id': sale_id, **attr} for attr in shared_attrs])

        shared_columns = {
            col for col in group.columns
            if col not in exclude_fields and group[col].nunique(dropna=False) == 1
        }

        for _, row in group.iterrows():
            line_item_id = str(uuid4())

            line_items.append({
                'sale_id': sale_id,
                'id': line_item_id,
                'shipping_address_id': row['shipping_address_id']
            })

            item_attrs = extract_line_item_attributes(row, shared_columns, exclude_fields)
            line_item_attributes.extend([{'line_item_id': line_item_id, **attr} for attr in item_attrs])


    readable_attributes = pd.DataFrame({
            **{f"billing_{key}": val for key, val in settings['header_info']['billing'].items()},
            **{f"shipping_{key}": val for key, val in settings['header_info']['shipping'].items()},
            **settings['header_info']['order_info']
        }.items(),
        columns=['attribute_standardized', 'client_attribute_id']
        ).replace('',pd.NA).dropna(how='any').assign(upload_id=settings['metadata']['upload_id'])

    settings['table_data'] = {
        'sales': pd.DataFrame(sales),
        'line_items': pd.DataFrame(line_items),
        'addresses': pd.DataFrame(addresses),
        'sale_attributes': pd.DataFrame(sale_attributes),
        'line_item_attributes': pd.DataFrame(line_item_attributes),
        'upload_sales': pd.DataFrame(upload_sales),
        'readable_attributes': readable_attributes
    }

    return settings

def extract_shared_attributes(group, exclude_fields):


    if group.shape[0] == 1:
        common_cols = pd.Series(True, index=group.columns)
    else:
        common_cols = group.nunique(dropna=False) == 1
    shared = []

    for col in common_cols[common_cols].index:

        if col in exclude_fields:
            continue
        val = group[col].iloc[0]
        # if entire column is NaN, this will correctly return None
        attr_val = None if pd.isna(val) else str(val)
        shared.append({
            'attribute_id': col,
            'attribute_value': attr_val
        })

    return shared

def extract_line_item_attributes(row, shared_columns, exclude_fields):
    return [
        {
            'attribute_id': col,
            'attribute_value': str(row[col])
        }
        for col in row.index
        if col not in exclude_fields and col not in shared_columns
    ]