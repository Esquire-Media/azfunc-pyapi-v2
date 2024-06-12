# File: libs/azure/functions/blueprints/esquire/audiences/mover_sync/activities/get_blob_row_count.py

from azure.data.tables import TableClient
from azure.durable_functions import Blueprint
from libs.data import from_bind
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
import os, pandas as pd

bp = Blueprint()


@bp.activity_trigger(input_name="settings")
def activity_moversSync_getBlobRowCount(settings: dict):
    """
    Retrieves the row count of a CSV blob in Azure Synapse and updates the count in Azure Table Storage.

    Parameters
    ----------
    settings : dict
        A dictionary containing the settings for the operation, which include:
        - 'blob_name': A string specifying the name of the blob in Azure Synapse for which the row count is required.
        - 'rowCounts_table': A dictionary with connection details for Azure Table Storage, containing:
            - 'conn_str': The name of the environment variable holding the connection string to Azure Table Storage.
            - 'table_name': The name of the table in Azure Table Storage where the row count will be updated.

    Returns
    -------
    dict
        An empty dictionary, signifying successful completion of the operation.

    Note
    ----
    The 'blob_name' is used to determine the PartitionKey and RowKey in Azure Table Storage.
    The row count is obtained from the specified blob in Azure Synapse.
    """
    # Connect to Azure Synapse cluster
    session: Session = from_bind("audiences").connect()

    # Load the blob using Synapse and get a row count
    row_count = pd.DataFrame(
        session.execute(text(get_counts_query(blob_names=[settings["blob_name"]])))
    )["row_count"][0]

    # Write row count to Azure Table Storage
    table_client = TableClient.from_connection_string(
        conn_str=os.environ[settings["rowCounts_table"]["conn_str"]],
        table_name=settings["rowCounts_table"]["table_name"],
    )
    table_client.upsert_entity(
        entity={
            "PartitionKey": settings["blob_name"].split("/")[-2],
            "RowKey": settings["blob_name"].split("/")[-1],
            "RowCount": int(row_count),
        }
    )

    return {}


def get_counts_query(blob_names: list) -> str:
    """
    Builds a SQL query to count the number of rows in a specified CSV blob.

    Parameters
    ----------
    blob_names : list
        A list of blob names for which the row count is to be calculated.

    Returns
    -------
    str
        The SQL query string to count rows in the specified blobs.
    """
    # Build the SQL query string
    blob_names_str = ",".join([f"'{blob_name}'" for blob_name in blob_names])
    return f"""
        SELECT COUNT(*) AS [row_count]
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
