# File: libs/azure/functions/blueprints/synapse/cetas.py

from datetime import datetime
from dateutil.relativedelta import relativedelta
from libs.azure.functions import Blueprint
from libs.data import from_bind
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from urllib.parse import unquote
import os

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
async def synapse_activity_cetas(ingress: dict):
    """
    Handles the creation of an external table in Azure Synapse Analytics,
    optionally creating or altering a view based on the table,
    and generating SAS URLs for blobs if requested.

    Parameters
    ----------
    ingress : dict
        A dictionary containing several key-value pairs that configure the function:
        - table : dict
            Contains the 'schema' (default 'dbo') and 'name' of the table.
        - destination : dict
            Contains 'container', 'path', 'handle', and optionally 'format'
            (default 'PARQUET') for the data destination.
        - query : str
            The SQL query to be executed.
        - bind : str
            Database connection information.
        - commit : bool, optional
            Flag to determine if changes should be committed (default is False).
        - view : bool, optional
            Flag to determine if a view should be created or altered (default is False).
        - return_urls : bool, optional
            Flag to indicate if SAS URLs for blobs should be returned (default is False).
        - instance_id : str
            An identifier for the instance.

    Returns
    -------
    list or str
        A list of SAS URLs for the blobs if 'return_urls' is True.
        Otherwise, an empty string.

    Notes
    -----
    This function is specific to Azure Synapse Analytics and requires appropriate
    Azure permissions and configurations to be set up in advance.
    """
    # Construct the table name using the provided schema (or default to 'dbo'), table name, and instance ID.
    table_name = f'[{ingress["table"].get("schema", "dbo")}].[{ingress["table"]["name"]}_{ingress["instance_id"]}]'

    # Create an external table in Azure Synapse with the specified structure and data source.
    query = f"""
        CREATE EXTERNAL TABLE {table_name}
        WITH (
            LOCATION = '{ingress["destination"]["container"]}/{ingress["destination"]["path"]}/',
            DATA_SOURCE = {ingress["destination"]["handle"]},  
            FILE_FORMAT = {ingress["destination"].get("format", "PARQUET")}
        )  
        AS
        {ingress["query"]}
    """

    # Establish a session with the database using the provided bind information.
    session: Session = from_bind(ingress["bind"]).connect()
    session.execute(text(query))

    # Commit the transaction if the 'commit' flag is set.
    if ingress.get("commit", False):
        session.commit()

        # Create or alter a view based on the external table if the 'view' flag is set.
        if ingress.get("view", False):
            session.execute(
                text(
                    f"""
                        CREATE OR ALTER VIEW [{ingress["table"].get("schema", "dbo")}].[{ingress["table"]["name"]}] AS
                            SELECT * FROM {table_name}
                    """
                )
            )
            session.commit()

    # Create or alter a view based on the files created if the 'view' flag is set.
    elif ingress.get("view", False):
        session.close()
        session: Session = from_bind(ingress["bind"]).connect()
        session.execute(
            text(
                f"""
                    CREATE OR ALTER VIEW [{ingress["table"].get("schema", "dbo")}].[{ingress["table"]["name"]}] AS
                        SELECT * FROM OPENROWSET(
                            BULK '{ingress["destination"]["container"]}/{ingress["destination"]["path"]}/*.{ingress["destination"].get("format", "PARQUET").lower()}',
                            DATA_SOURCE = '{ingress["destination"]["handle"]}',  
                            FORMAT = '{ingress["destination"].get("format", "PARQUET")}' 
                        ) AS [data]
                """
            )
        )
        session.commit()

    # Generate SAS URLs for blobs if the 'return_urls' flag is set.
    if ingress.get("return_urls", None):
        from azure.storage.blob import (
            ContainerClient,
            BlobSasPermissions,
            generate_blob_sas,
        )

        # Connect to the container client using the destination connection information.
        container = ContainerClient.from_connection_string(
            conn_str=os.getenv(
                ingress["destination"]["conn_str"], os.environ["AzureWebJobsStorage"]
            ),
            container_name=ingress["destination"].get(
                "container_name", ingress["destination"]["container"]
            ),
        )

        # List and generate SAS URLs for blobs that match the specified criteria.
        return [
            unquote(blob.url)
            + "?"
            + generate_blob_sas(
                account_name=blob.account_name,
                container_name=blob.container_name,
                blob_name=blob.blob_name,
                account_key=blob.credential.account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + relativedelta(days=2),
            )
            for blob_props in container.list_blobs(
                name_starts_with=ingress["destination"]["path"]
            )
            if blob_props.name.endswith(
                ingress["destination"].get("format", "PARQUET").lower().split("_")[0]
            )
            if (blob := container.get_blob_client(blob_props)).exists()
        ]

    return ""
