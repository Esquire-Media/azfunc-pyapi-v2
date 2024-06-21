# File: libs/azure/functions/blueprints/datalake/activities/copy.py
from azure.durable_functions import Blueprint
from libs.utils.azure_storage import get_blob_sas, init_blob_client
import fsspec, ssl

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_blob2ftp(ingress: dict) -> str:
    """
    Copy blob data from a source to a target FTP server.

    This function copies blob data from the specified source to the specified target in
    Azure Blob Storage.

    Parameters
    ----------
    ingress : dict
        A dictionary containing necessary parameters:
        - source (str or dict): The source can either be a string URL or a dictionary
                                with "conn_str", "container_name", and "blob_name" keys.
        - target (str or dict): The target can either be a string URL or a dictionary
                                with "conn_str", "container_name", and "blob_name" keys.

    Returns
    -------
    str
        A SAS token URL for accessing the copied content in Azure Blob Storage.

    Examples
    --------
    Using string URLs for source and target:

    .. code-block:: python

        import azure.durable_functions as df

        def orchestrator_function(context: df.DurableOrchestrationContext):
            copied_url = yield context.call_activity('azure_datalake_copy_blob', {
                "source": "https://storage_account.",
                "target": "ftps://username:password@host:port/path/to/file"
            })
            return copied_url

    Using dictionary values for source and target:

    .. code-block:: python

        import azure.durable_functions as df

        def orchestrator_function(context: df.DurableOrchestrationContext):
            copied_url = yield context.call_activity('azure_datalake_copy_blob', {
                "source": {
                    "conn_str": "SOURCE_AZURE_BLOB_CONNECTION_STRING",
                    "container_name": "source-container",
                    "blob_name": "source_blob.txt"
                },
                "target": "ftps://username:password@host:port/path/to/file"
            })
            return copied_url
    """

    if isinstance(ingress["source"], dict):
        blob = init_blob_client(**ingress["source"])
        ingress["source"] = get_blob_sas(blob)

    # Use fsspec to handle file operations
    with fsspec.open(ingress["source"], "rb") as source_file:
        try:
            with fsspec.open(ingress["target"], "wb") as target_file:
                target_file.write(source_file.read())
        except:
            # Attempt connection without verifying the SSL certificate
            context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            target_fs_ssl = fsspec.open(
                ingress["target"], "wb", transport_params={"ssl_context": context}
            )
            with target_fs_ssl as target_file:
                target_file.write(source_file.read())

    return ingress["target"]
