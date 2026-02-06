# File: libs/azure/functions/blueprints/esquire/audiences/mover_sync/activities/validate_address_chunks.py

from azure.durable_functions import Blueprint
from libs.data import from_bind
from libs.utils.azure_storage import init_blob_client
from libs.utils.smarty import bulk_validate
from libs.utils.text import format_zipcode, format_zip4, format_full_address
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
import h3, os, pandas as pd

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()


# Define an activity function
@bp.activity_trigger(input_name="settings")
def activity_moversSync_validateAddressChunk(settings: dict):
    """
    Validates addresses in a specific chunk of data and uploads the validated data to Azure Blob Storage.

    Fetches a chunk of data based on provided settings, validates the addresses using Smarty, formats the data,
    and then uploads it to Azure Blob Storage in Parquet format.

    Parameters
    ----------
    settings : dict
        A dictionary containing the following keys:
        - runtime_container: dict
            Details of the Azure Blob Storage container. Includes 'conn_str' for connection string and
            'container_name' for the name of the container.
        - chunk: dict
            Details of the data chunk to be processed. Includes 'blob_name' for the name of the blob,
            'offset' for the starting row, and 'limit' for the number of rows in the chunk.
        - rowCounts_table: dict
            Details of the Azure Table Storage for row counts. Includes 'conn_str' for connection string and
            'table_name' for the name of the table.

    Returns
    -------
    str
        Path of the uploaded blob in Azure Storage.
    """

    # logging.warning(
    #     f"activity_moversSync_validateAddressChunk | {settings['chunk']['blob_name']} | {settings['chunk']['offset']},{settings['chunk']['limit']}"
    # )

    # connect to Azure synapse cluster
    session: Session = from_bind("audiences").connect()

    # execute query to get a chunk of data from the blob using an offset/limit query
    chunk_data = pd.DataFrame(
        session.execute(
            text(
                get_chunk_query(
                    blob_path=f"{settings['runtime_container']['container_name']}/{settings['chunk']['blob_type']}/{settings['chunk']['blob_name']}",
                    offset=settings["chunk"]["offset"],
                    limit=settings["chunk"]["limit"],
                )
            )
        )
    )
    chunk_data["full_add"] = chunk_data.apply(
        lambda x: format_full_address(x["add1"], x["add2"]), axis=1
    )

    # validate addresses using Smarty
    validated = bulk_validate(
        chunk_data,
        address_col="full_add",
        city_col="city",
        state_col="st",
        zip_col="zip",
    )
    formatted = format_validated_addresses(validated)

    # upload validated data to a blob client
    outpath = f"{settings['chunk']['blob_type']}-geocoded/{settings['chunk']['blob_name']}/offset={('000000000'+str(settings['chunk']['offset']))[-9:]},limit={settings['chunk']['limit']}"
    blob_client = init_blob_client(
        conn_str=os.environ[settings["runtime_container"]["conn_str"]],
        container_name=settings["runtime_container"]["container_name"],
        blob_name=outpath,
    )
    blob_client.upload_blob(data=formatted.to_parquet(index=False), overwrite=True)

    return outpath


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

    # Append H3 column
    df["h3_index"] = df.apply(
        lambda row: h3.geo_to_h3(
            lat=row["latitude"], lng=row["longitude"], resolution=5
        ),
        axis=1,
    )

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


def get_chunk_query(blob_path: str, offset: int, limit: int) -> str:
    """
    Build a query to load one chunk of data from the CSV blob, using limit and offset values to partition the chunk.
    """

    # build query
    chunk_query = f"""
    SELECT
        [add1],
        [add2],
        [city],
        [st],
        RIGHT('00000' + [zip], 5) AS [zip],
        RIGHT('0000' + [zip4], 4) AS [zip4],
        [dt],
        CAST([keycode] AS DATE) AS [keycode],
        [oldcity],
        [oldst],
        [oldzip],
        [city_flg],
        [st_flg],
        [zip_flg],
        [cnty_flg],
        [dt_flg],
        [hoc],
        [addtype],
        [p_inc_val],
        [p_hv_val],
        [age]
    FROM OPENROWSET(
        BULK ('{blob_path}'),
        DATA_SOURCE = 'sa_esquiremovers',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = FALSE,
        FIRST_ROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a'
    ) WITH (
        [add1] VARCHAR(200) COLLATE Latin1_General_100_BIN2_UTF8,
        [add2] VARCHAR(100) COLLATE Latin1_General_100_BIN2_UTF8,
        [city] VARCHAR(100) COLLATE Latin1_General_100_BIN2_UTF8,
        [st] VARCHAR(2) COLLATE Latin1_General_100_BIN2_UTF8,
        [zip] VARCHAR(5) COLLATE Latin1_General_100_BIN2_UTF8,
        [zip4] VARCHAR(4) COLLATE Latin1_General_100_BIN2_UTF8,
        [dt] VARCHAR(1) COLLATE Latin1_General_100_BIN2_UTF8,
        [keycode] VARCHAR(8) COLLATE Latin1_General_100_BIN2_UTF8,
        [oldcity] VARCHAR(100) COLLATE Latin1_General_100_BIN2_UTF8,
        [oldst] VARCHAR(2) COLLATE Latin1_General_100_BIN2_UTF8,
        [oldzip] VARCHAR(5) COLLATE Latin1_General_100_BIN2_UTF8,
        [city_flg] VARCHAR(1) COLLATE Latin1_General_100_BIN2_UTF8,
        [st_flg] VARCHAR(1) COLLATE Latin1_General_100_BIN2_UTF8,
        [zip_flg] VARCHAR(1) COLLATE Latin1_General_100_BIN2_UTF8,
        [cnty_flg] VARCHAR(1) COLLATE Latin1_General_100_BIN2_UTF8,
        [dt_flg] VARCHAR(1) COLLATE Latin1_General_100_BIN2_UTF8,
        [hoc] VARCHAR(1) COLLATE Latin1_General_100_BIN2_UTF8,
        [addtype] VARCHAR(1) COLLATE Latin1_General_100_BIN2_UTF8,
        [p_inc_val] INT,
        [p_hv_val] INT,
        [age] INT
    ) AS [data]
    ORDER BY [add1], [add2], [city], [st]
    OFFSET {offset} ROWS
    FETCH NEXT {limit} ROWS ONLY
    """
    return chunk_query
