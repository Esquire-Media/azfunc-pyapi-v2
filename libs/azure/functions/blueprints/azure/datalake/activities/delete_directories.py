# File: libs/azure/functions/blueprints/datalake/delete_directories.py

from azure.storage.filedatalake import FileSystemClient
from azure.durable_functions import Blueprint
import os

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
async def datalake_activity_delete_directory(ingress: dict) -> str:
    filesystem = FileSystemClient.from_connection_string(
        os.environ[ingress.get("conn_str", "AzureWebJobsStorage")],
        ingress.get("container", ingress["container_name"]),
    )
    paths = filesystem.get_paths(recursive=False)
    try:
        for path in paths:
            if path["is_directory"] and path["name"].startswith(ingress["prefix"]):
                filesystem.get_directory_client(path).delete_directory()
    except:
        pass

    return ""
