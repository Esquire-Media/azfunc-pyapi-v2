# blueprints/validate_addresses_bp.py
from azure.durable_functions import Blueprint, activity_trigger
from sqlalchemy import text
import pandas as pd

# 🔁 reuse the same helpers you already ship with another function-app
from libs.utils.smarty import bulk_validate                # address → Smarty
from libs.utils.text import format_full_address            # add1 + add2
from libs.utils.text import format_zipcode, format_zip4, format_full_address

from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import db  # pooled SQLAlchemy conn

bp = Blueprint()


@bp.activity_trigger(input_name="settings")
def validate_addresses(settings: dict) -> str:
    """
    1. Pull rows from the *temp* staging table.
    2. Bulk-validate them with Smarty in one call.
    3. Normalise the response via your `format_validated_addresses`.
    4. Upsert the **full address, city, state, zip, zip4, lat, lon** back into staging.
    """

    table      = settings["table"]
    pk_col     = settings.get("id_field", "id")         # primary-key column name
    add1_col   = settings.get("add1_field", "add1")
    add2_col   = settings.get("add2_field", "add2")
    city_col   = settings.get("city_field", "city")
    state_col  = settings.get("state_field", "st")
    zip_col    = settings.get("zip_field", "zip")

    # ── 1. Load the addresses that need validation ────────────────────
    with db() as conn:
        df = pd.read_sql(
            text(f'''
                SELECT "{pk_col}", "{add1_col}" AS add1, "{add2_col}" AS add2,
                       "{city_col}" AS city, "{state_col}" AS st, "{zip_col}" AS zip
                FROM "{table}"
            '''), conn
        )

    if df.empty:
        return "no rows to validate"

    df["full_add"] = df.apply(
        lambda r: format_full_address(r.add1, r.add2), axis=1
    )

    # ── 2. One bulk call to Smarty (your helper already chunks & retries) ─
    validated = bulk_validate(
        df,
        address_col="full_add",
        city_col="city",
        state_col="st",
        zip_col="zip",
    )

    # ── 3. Re-use your formatter so we get clean, typed columns ─────────
    cleaned = format_validated_addresses(validated)   # DataFrame with address, city, state, zipcode …

    # ── 4. Upsert back into the temp staging table ──────────────────────
    # Convert DataFrame → list-of-tuples in the exact order of the VALUES clause
    rows = cleaned[[pk_col, "address", "city", "state",
                    "zipcode", "plus4Code", "latitude", "longitude"]
                  ].to_records(index=False)

    if rows.size:
        with db() as conn:
            conn.execute(
                text(f"""
                    UPDATE "{table}" AS t
                       SET {add1_col} = v.address,
                           {city_col} = v.city,
                           {state_col}= v.state,
                           {zip_col}   = v.zipcode,
                           zip4        = v.plus4Code,
                           latitude    = v.latitude,
                           longitude   = v.longitude
                      FROM (VALUES :rows) AS v({pk_col},
                                                address,
                                                city,
                                                state,
                                                zipcode,
                                                plus4Code,
                                                latitude,
                                                longitude)
                     WHERE t."{pk_col}" = v.{pk_col}
                """),
                {"rows": list(rows)}
            )

    return f"validated & updated {len(cleaned)} rows"

def format_validated_addresses(df: pd.DataFrame) -> pd.DataFrame:
    """
    Utillty function to apply all necessary formatting steps to the address dataset post-Smarty validation.
    """
    column_map = {
        "keycode": "date",
        "delivery_line_1": "address",
        "primary_number": "primaryNumber",
        "street_name": "streetName",
        "street_predirection": "streetPredirection",
        "street_postdirection": "streetPostdirection",
        "street_suffix": "streetSuffix",
        "secondary_number": "secondaryNumber",
        "secondary_designator": "secondaryDesignator",
        "city_name": "city",
        "state_abbreviation": "state",
        "zipcode": "zipcode",
        "plus4_code": "plus4Code",
        "carrier_route": "carrierCode",
        "latitude": "latitude",
        "longitude": "longitude",
        "oldcity": "oldCity",
        "oldstate": "oldState",
        "oldzip": "oldZipcode",
        "hoc": "homeOwnership",
        "addtype": "addressType",
        "p_inc_val": "estimatedIncome",
        "p_hv_val": "estimatedHomeValue",
        "age": "estimatedAge",
    }
    # filter and format column names
    for col in column_map.keys():
        if col not in df.columns:
            df[col] = None
    df = df[column_map.keys()].rename(columns=column_map)

    # fill in abbreviations with more descriptive values
    hoc_codes = {
        "R": "Renter",
        "P": "ProbableRenter",
        "W": "ProbableHomeOwner",
        "Y": "HomeOwner",
    }
    df["homeOwnership"] = df["homeOwnership"].apply(
        lambda x: hoc_codes[x] if x in hoc_codes.keys() else None
    )
    # fill in abbreviations with more descriptive values
    addtype_codes = {"S": "SingleFamily", "H": "Highrise"}
    df["addressType"] = df["addressType"].apply(
        lambda x: addtype_codes[x] if x in addtype_codes.keys() else None
    )

    # format zipcodes
    df["zipcode"] = df["zipcode"].apply(format_zipcode)
    df["plus4Code"] = df["plus4Code"].apply(format_zip4)
    df["oldZipcode"] = df["oldZipcode"].apply(format_zipcode)

    # explicity set column types
    df["date"] = df["date"].astype(dtype=str)
    df["address"] = df["address"].astype(dtype=str)
    df["primaryNumber"] = df["primaryNumber"].astype(dtype=str)
    df["streetPredirection"] = df["streetPredirection"].astype(dtype=str)
    df["streetName"] = df["streetName"].astype(dtype=str)
    df["streetSuffix"] = df["streetSuffix"].astype(dtype=str)
    df["streetPostdirection"] = df["streetPostdirection"].astype(dtype=str)
    df["secondaryDesignator"] = df["secondaryDesignator"].astype(dtype=str)
    df["secondaryNumber"] = df["secondaryNumber"].astype(dtype=str)
    df["city"] = df["city"].astype(dtype=str)
    df["state"] = df["state"].astype(dtype=str)
    df["zipcode"] = df["zipcode"].astype(dtype=str)
    df["plus4Code"] = df["plus4Code"].astype(dtype=str)
    df["carrierCode"] = df["carrierCode"].astype(dtype=str)
    df["latitude"] = df["latitude"].astype(dtype=float)
    df["longitude"] = df["longitude"].astype(dtype=float)
    df["oldCity"] = df["oldCity"].astype(dtype=str)
    df["oldState"] = df["oldState"].astype(dtype=str)
    df["oldZipcode"] = df["oldZipcode"].astype(dtype=str)
    df["homeOwnership"] = df["homeOwnership"].astype(dtype=str)
    df["addressType"] = df["addressType"].astype(dtype=str)
    df["estimatedIncome"] = df["estimatedIncome"].astype(dtype=int)
    df["estimatedHomeValue"] = df["estimatedHomeValue"].astype(dtype=int)
    df["estimatedAge"] = df["estimatedAge"].astype(dtype=int)
    df["h3_index"] = df["h3_index"].astype(dtype=str)

    return df