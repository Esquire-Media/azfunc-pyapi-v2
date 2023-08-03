# File: libs/azure/functions/blueprints/datalake/delete_directories.py

from azure.storage.filedatalake import FileSystemClient
from libs.azure.functions import Blueprint
import os

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
async def datalake_activity_delete_directory(ingress: dict):
    filesystem = FileSystemClient.from_connection_string(
        os.environ[ingress["conn_str"]]
        if ingress.get("conn_str", None) in os.environ.keys()
        else os.environ["AzureWebJobsStorage"],
        ingress["container"],
    )
    for item in filesystem.get_paths(recursive=False):
        if item["is_directory"] and item["name"].startswith(ingress["prefix"]):
            filesystem.get_directory_client(item).delete_directory()

    return ""
