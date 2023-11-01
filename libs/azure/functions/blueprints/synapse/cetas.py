# File: libs/azure/functions/blueprints/synapse/cetas.py

from azure.storage.filedatalake import (
    FileSystemClient,
    FileSasPermissions,
    generate_file_sas,
)
from azure.storage.blob import BlobClient, ContainerClient, BlobSasPermissions, generate_blob_sas
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
    table_name = f'[{ingress["table"].get("schema", "dbo")}].[{ingress["table"]["name"]}_{ingress["instance_id"]}]'
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

    session: Session = from_bind(ingress["bind"]).connect()
    session.execute(text(query))
    if ingress.get("commit", False):
        session.commit()
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

    if ingress.get("return_urls", None):
        # filesystem = FileSystemClient.from_connection_string(
        #     os.environ[ingress["destination"]["conn_str"]]
        #     if ingress["destination"].get("conn_str", None) in os.environ.keys()
        #     else os.environ["AzureWebJobsStorage"],
        #     ingress["destination"]["container"],
        # )

        # return [
        #     file.url
        #     + "?"
        #     + generate_file_sas(
        #         file.account_name,
        #         file.file_system_name,
        #         "/".join(file.path_name.split("/")[:-1]),
        #         file.path_name.split("/")[-1],
        #         filesystem.credential.account_key,
        #         FileSasPermissions(read=True),
        #         datetime.utcnow() + relativedelta(days=2),
        #     )
        #     for item in filesystem.get_paths(ingress["destination"]["path"])
        #     if not item["is_directory"]
        #     if (file := filesystem.get_file_client(item))
        #     if file.path_name.endswith(
        #         ingress["destination"].get("format", "PARQUET").lower()
        #     )
        # ]
        
        container = ContainerClient.from_connection_string(
            conn_str=os.getenv(ingress["destination"]["conn_str"], os.environ["AzureWebJobsStorage"]),
            container_name=ingress["destination"].get("container_name", ingress["destination"]["container"])
        )

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
            for blob_props in container.list_blobs(name_starts_with=ingress["destination"]["path"])
            if blob_props.name.endswith(
                ingress["destination"].get("format", "PARQUET").lower()
            )
            if (blob := container.get_blob_client()).exists()
        ]

    return ""
