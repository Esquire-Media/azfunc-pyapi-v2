# File: libs/azure/functions/blueprints/datalake/delete_directories.py

from azure.storage.filedatalake import FileSystemClient
from libs.azure.functions import Blueprint
import os

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
async def datalake_activity_delete_directory(ingress: dict) -> str:
    """
    Asynchronously delete directories in Azure Data Lake.

    This activity function deletes directories from Azure Data Lake based on a provided prefix.
    It establishes a connection to the Azure Data Lake filesystem and iteratively deletes
    directories that match the given prefix.

    Parameters
    ----------
    ingress : dict
        A dictionary containing necessary parameters, specified as follows:
        - conn_str (str, optional): Connection string or the name of the environment variable storing the Azure Data Lake connection string. Defaults to "AzureWebJobsStorage" if not provided.
        - container (str): Name of the Azure Data Lake container.
        - prefix (str): Prefix to match directories that should be deleted.

    Returns
    -------
    str
        An empty string, signifying the successful completion of the operation.

    Examples
    --------
    In an orchestrator function:

    .. code-block:: python

        import azure.durable_functions as df

        def orchestrator_function(context: df.DurableOrchestrationContext):
            result = yield context.call_activity('datalake_activity_delete_directory', {
                "conn_str": "AZURE_DATALAKE_CONNECTION_STRING",
                "container": "my-container",
                "prefix": "data_prefix"
            })
            return result

    Notes
    -----
    - The function uses the Azure SDK's `FileSystemClient` for operations on Azure Data Lake.
    - Only directories that start with the specified prefix will be deleted.
    """
    
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
