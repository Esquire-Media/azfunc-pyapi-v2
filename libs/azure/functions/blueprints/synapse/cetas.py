# File: libs/azure/functions/blueprints/synapse/cetas.py

from azure.storage.filedatalake import (
    FileSystemClient,
    FileSasPermissions,
    generate_file_sas,
)
from datetime import datetime
from dateutil.relativedelta import relativedelta
from libs.azure.functions import Blueprint
from libs.data import from_bind
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
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
    if ingress.get("commit", None):
        session.commit()
        if ingress.get("view", None):
            session.execute(
                text(
                    f"""
                        CREATE OR ALTER VIEW [{ingress["table"].get("schema", "dbo")}].[{ingress["table"]["name"]}] AS
                            SELECT * FROM {table_name}
                    """
                )
            )
            session.commit()

    if ingress.get("return_urls", None):
        filesystem = FileSystemClient.from_connection_string(
            os.environ[ingress["destination"]["conn_str"]]
            if ingress["destination"].get("conn_str", None) in os.environ.keys()
            else os.environ["AzureWebJobsStorage"],
            ingress["destination"]["container"],
        )

        return [
            file.url
            + "?"
            + generate_file_sas(
                file.account_name,
                file.file_system_name,
                "/".join(file.path_name.split("/")[:-1]),
                file.path_name.split("/")[-1],
                filesystem.credential.account_key,
                FileSasPermissions(read=True),
                datetime.utcnow() + relativedelta(days=2),
            )
            for item in filesystem.get_paths(ingress["destination"]["path"])
            if not item["is_directory"]
            if (file := filesystem.get_file_client(item))
            if file.path_name.endswith(
                ingress["destination"].get("format", "PARQUET").lower()
            )
        ]

    return ""
