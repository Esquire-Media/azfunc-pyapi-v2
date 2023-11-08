# File: libs/azure/functions/blueprints/daily_audience_generation/activities/address_validation.py

from libs.azure.functions import Blueprint
import pandas as pd
import os
from azure.storage.blob import ContainerClient
from io import BytesIO
from fuzzywuzzy import fuzz
from libs.utils.smarty import bulk_validate

bp: Blueprint = Blueprint()


# activity to validate the addresses
@bp.activity_trigger(input_name="ingress")
def activity_address_validation(ingress: dict):
    # load the audience addresses file into a dataframe
    container_client = ContainerClient.from_connection_string(
        conn_str=os.environ["ONSPOT_CONN_STR"],
        container_name="general",
    )
    
    address_list_blob = container_client.get_blob_client(
        f"{ingress['path']}/{ingress['audience']}/{ingress['audience']}.csv"
    )
    address_list = address_list_blob.download_blob().readall()

    # Define the data types for columns, specifying 'zipcode' as object (string) type
    dtype_dict = {"zip": str}
    address_df = (
        pd.read_csv(BytesIO(address_list), dtype=dtype_dict)
        .dropna(subset=["address"])
        .fillna("")
    )
    address_df = detect_column_names(address_df)
    # I used .dropna because the csv is loading with a lot of empty rows (only if 1st column is NaN)

    # start the address validation
    # clean addresses via the Smarty SDK
    cleaned_addresses = bulk_validate(
        df=address_df,
        address_col="street" if "street" in address_df.columns else None,
        city_col="city" if "city" in address_df.columns else None,
        state_col="state" if "state" in address_df.columns else None,
        zip_col="zipcode" if "zipcode" in address_df.columns else None,
    )

    cleaned_addresses = cleaned_addresses.dropna(subset=["delivery_line_1"]).copy()
    cleaned_addresses["query_string"] = cleaned_addresses.apply(
        lambda row: f"{row['delivery_line_1']}, {row['city_name']} {row['state_abbreviation']}, {row['zipcode']}",
        axis=1,
    ).str.upper()

    # pass back the cleaned and verified addresses as an object
    # key: audience_id value: cleaned addresses
    return {
        "audience_id": ingress["audience"],
        "addresses": cleaned_addresses[["query_string"]].to_json(orient="records"),
    }


def detect_column_names(df):
    """
    Attempts to automatically detect the address component columns in a sales file
    Returns a slice of the sales data with detected columns for [address, city, state, zip]
    """
    # dictionary of common column headers for address components
    mapping = {
        "street": ["address", "street", "delivery_line_1", "line1", "add"],
        "city": ["city", "city_name"],
        "state": ["state", "st", "state_abbreviation"],
        "zip": ["zip", "zipcode", "postal", "postalcodeid"],
    }

    # find best fit for each address field
    for dropdown, defaults in mapping.items():
        column_scores = [
            max([fuzz.ratio(column.upper(), default.upper()) for default in defaults])
            for column in df.columns
        ]
        best_fit_idx = column_scores.index(max(column_scores))
        best_fit = df.columns[best_fit_idx]
        df = df.rename(columns={best_fit: dropdown})

    return df[["street", "city", "state", "zip"]]
