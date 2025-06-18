from azure.durable_functions import Blueprint
from libs.utils.smarty import bulk_validate
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.generate_ids import (
    generate_deterministic_id,
    NAMESPACE_ADDRESS
)
import pandas as pd
import numpy as np
import re
import os

bp = Blueprint()

@bp.activity_trigger(input_name="settings")
def activity_validateAddresses(settings:dict):
    """
    Validates address information and generates deterministic uuids
    """
    sales = pd.DataFrame(settings['sales'])
    header_info = settings['header_info']

    sales['sales_index'] = np.arange(sales.shape[0]) # used to merge on after Smarty call

    # do the initial set of billing addresses
    billing_adds = validate_address_set(sales, header_info['billing']).rename(
        columns={
            'delivery_line_1'   : 'billing_street_cleaned',
            'city_name'         : 'billing_city_cleaned',
            'state_abbreviation': 'billing_state_cleaned',
            'zipcode'           : 'billing_zipcode_cleaned', 
            'address_id'        : 'billing_address_id'
            })

    # if we only have one address, then no need to repeat it
    if header_info['billing'] == header_info['shipping']:
        shipping_adds = billing_adds.copy().rename(
        columns={
            'delivery_line_1'   : 'shipping_street_cleaned',
            'city_name'         : 'shipping_city_cleaned',
            'state_abbreviation': 'shipping_state_cleaned',
            'zipcode'           : 'shipping_zipcode_cleaned', 
            'address_id'        : 'shipping_address_id'
            })
    else:
        # if they're different, do the shipping
        shipping_adds = validate_address_set(sales, header_info['shipping']).rename(
        columns={
            'delivery_line_1'   : 'shipping_street_cleaned',
            'city_name'         : 'shipping_city_cleaned',
            'state_abbreviation': 'shipping_state_cleaned',
            'zipcode'           : 'shipping_zipcode_cleaned', 
            'address_id'        : 'shipping_address_id'
            })

    # combine the two sets
    cleaned_adds = billing_adds.merge(
        shipping_adds,
        on='sales_index',
        how='outer'
    )

    settings['sales'] = settings['sales'].merge(
        cleaned_adds,
        on='sales_index',
        how='left'
    )

    return settings


def validate_address_set(sales, header_info):

    ADDRESS = header_info['street']
    CITY    = header_info['city']
    STATE   = header_info['state']
    ZIPCODE = header_info['zipcode']

    # do some initial pre-cleaning to increase validation chance
    df = pre_clean(sales, ADDRESS, CITY, STATE, ZIPCODE)
    
    # get the information from smarty
    cleaned_addresses = get_smarty_addresses(df, ADDRESS, CITY, STATE, ZIPCODE)

    # generate the deterministic id for the addresses
    cleaned_addresses['address_id'] = cleaned_addresses.apply(lambda entry: generate_deterministic_id(NAMESPACE_ADDRESS, [entry[field] for field in [ADDRESS, CITY, STATE, ZIPCODE]]), axis=1)

    return cleaned_addresses[['delivery_line_1','city_name','state_abbreviation','zipcode', 'address_id', 'sales_index']]

def get_smarty_addresses(sales, ADDRESS, CITY, STATE, ZIPCODE):
    # clean sales through SmartySreets
    sales = sales[sales[ADDRESS] != '']
    smarty_sales = smarty_streets_cleaning(
        df=sales,
        ADDRESS=ADDRESS, 
        CITY=CITY, 
        STATE=STATE, 
        ZIPCODE=ZIPCODE
    )

    # collect neccessary smarty-cleaned columns and remaning untouched client sales data
    smarty_sales['sales_index'] = smarty_sales['sales_index'].astype(int)
    cleaned_sales = pd.merge(
        smarty_sales[['sales_index','delivery_line_1','city_name','state_abbreviation','zipcode']], 
        sales[ [col for col in sales.columns if col not in [ADDRESS,CITY,STATE,ZIPCODE]] ],
        on='sales_index' 
    )

    return cleaned_sales

def pre_clean(df, ADDRESS, CITY, STATE, ZIPCODE):
    # street address column formatting
    df[ADDRESS] = df[ADDRESS].replace(np.nan,'')
    df[ADDRESS] = df[ADDRESS].str.strip().str.upper()

    # creates the columns for ones that were not selected b/c missing
    # also the dropna above can drop the address columns, so add them back in if it did
    df[[col for col in [ADDRESS, CITY, STATE, ZIPCODE] if col not in df.columns]] = ''

    # zipcode column formatting
    zip_replacements = {
        '\'':'',
        'O':'0',
        '!':'0'
    }
    for key, value in zip_replacements.items():
        df[ZIPCODE] = df[ZIPCODE].str.replace(key, value, regex=False)

    # quick drop if there is no address info
    # some files have generated columns that extend way past where the actual data is, so it reads in everything
    df = df.replace('',np.nan).dropna(subset=[ADDRESS, CITY, STATE, ZIPCODE], how='all').replace(np.nan,'')

    return df

def smarty_streets_cleaning(df, ADDRESS, CITY, STATE, ZIPCODE):

    # send addresses through the Smarty Python SDK
    smarty_df = bulk_validate(
        df=df.rename(columns={ZIPCODE:'raw_zip'}), 
        address_col=ADDRESS,
        city_col=CITY,
        state_col=STATE,
        zip_col='raw_zip'
    )

    # drop duplicate entries
    smarty_df = smarty_df.drop_duplicates()
    # drop null latlongs (if applicable)
    if 'latitude' in smarty_df.columns and 'longitude' in smarty_df.columns:
        smarty_df = smarty_df.dropna(subset=['latitude','longitude'])
    
    # format the zipcodes to prevent any injection attacks when building SQL querys
    smarty_df['zipcode'] = smarty_df['zipcode'].apply(format_zipcode)
    smarty_df['plus4_code'] = smarty_df['plus4_code'].apply(format_zip4)
    smarty_df = smarty_df.dropna(subset=['zipcode'])
    smarty_df['full_zipcode'] = smarty_df['zipcode'] + '-' + smarty_df['plus4_code']

    # uppercase the text columns to prevent case matching issues later
    smarty_df['delivery_line_1'] = smarty_df['delivery_line_1'].str.upper()
    smarty_df['city_name'] = smarty_df['city_name'].str.upper()

    return smarty_df

def format_zipcode(z):
    """
    Formats a zipcode by extracting the first 5-digit portion and adding leading zeroes if needed
    Returns null if no 5-digit portion can be extracted
    """
    try:
        z = str(z)
        z = re.findall('([0-9]{4,5})',str(z))[0]
        while len(z) < 5:
            z = '0' + z
        return z
    except IndexError:
        return np.nan


def format_zip4(z):
    """
    Formats a 4-digit zip_plus_four_code
    """
    try:
        z = str(z)
        z = re.findall('([0-9]{3,4})',str(z))[0]
        while len(z) < 4:
            z = '0' + z
        return z
    except IndexError:
        return np.nan