import logging
import os
import pandas as pd
from libs.azure.functions import Blueprint
from azure.data.tables import TableClient
from libs.data import from_bind
from sqlalchemy.orm import Session
from sqlalchemy.sql import text

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()


# Define an activity function
@bp.activity_trigger(input_name="settings")
def activity_moversSync_getBlobRowCount(settings: dict):
    # connect to Azure synapse cluster
    session: Session = from_bind("audiences").connect()

    # load the blob using Synapse and get a row count
    row_count = pd.DataFrame(
        session.execute(
            text(
                get_counts_query(blob_names=[settings["blob_name"]])
            )
        )
    )["row_count"][0]

    # write row count to table
    table_client = TableClient.from_connection_string(
        conn_str=os.environ[settings["rowCounts_table"]["conn_str"]],
        table_name=settings["rowCounts_table"]["table_name"],
    )
    table_client.upsert_entity(
        entity={
            "PartitionKey": settings["blob_name"].split('/')[-2],
            "RowKey": settings["blob_name"].split('/')[-1],
            "RowCount": int(row_count),
        }
    )

    return {}


def get_counts_query(blob_names: list) -> str:
    """
    Build a query to get the total row count of the CSV blob.
    """

    # build query
    blob_names_str = ",".join([f"'{blob_name}'" for blob_name in blob_names])
    counts_query = f"""
    SELECT
        COUNT(*) AS [row_count]
    FROM OPENROWSET(
        BULK ({blob_names_str}),
        DATA_SOURCE = 'sa_esquiremovers',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a'
    ) WITH (
        [add1] VARCHAR(200) COLLATE Latin1_General_100_BIN2_UTF8,
        [add2] VARCHAR(100) COLLATE Latin1_General_100_BIN2_UTF8,
        [city] VARCHAR(100) COLLATE Latin1_General_100_BIN2_UTF8,
        [st] VARCHAR(2) COLLATE Latin1_General_100_BIN2_UTF8,
        [zip] VARCHAR(5) COLLATE Latin1_General_100_BIN2_UTF8
    ) AS [data]
    """
    return counts_query
