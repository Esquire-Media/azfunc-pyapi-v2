# File: libs/azure/functions/blueprints/datalake/activities/copy.py
from libs.azure.functions import Blueprint
from urllib.parse import unquote
from smart_open import open
from azure.storage.blob import BlobClient, BlobSasPermissions, generate_blob_sas
import datetime, os, ssl

import smart_open.ftp, ssl
from ftplib import FTP, FTP_TLS, error_reply
def _connect(hostname, username, port, password, secure_connection, transport_params):
    kwargs = smart_open.ftp.convert_transport_params_to_args(transport_params)
    if secure_connection:
        ssl_context = transport_params.get("ssl_context", ssl.create_default_context(purpose=ssl.Purpose.CLIENT_AUTH))
        ftp = FTP_TLS(context=ssl_context, **kwargs)
    else:
        ftp = FTP(**kwargs)
    try:
        ftp.connect(hostname, port)
    except Exception as e:
        smart_open.ftp.logger.error("Unable to connect to FTP server: try checking the host and port!")
        raise e
    try:
        ftp.login(username, password)
    except error_reply as e:
        smart_open.ftp.logger.error(
            "Unable to login to FTP server: try checking the username and password!"
        )
        raise e
    if secure_connection:
        ftp.prot_p()
    return ftp
smart_open.ftp._connect = _connect
from smart_open import open

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
                "source": "source_blob_url_with_read_SAS_token",
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
        blob = BlobClient.from_connection_string(
            conn_str=os.environ[ingress["source"]["conn_str"]],
            container_name=ingress["source"]["container_name"],
            blob_name=ingress["source"]["blob_name"],
        )
        ingress["source"] = (
            unquote(blob.url)
            + "?"
            + generate_blob_sas(
                account_name=blob.account_name,
                container_name=blob.container_name,
                blob_name=blob.blob_name,
                account_key=blob.credential.account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.datetime.now(datetime.UTC) + datetime.relativedelta(days=2),
            )
        )

    with open(ingress["source"], "rb") as source_file:
        try:
            with open(ingress["target"], "wb") as target_file:
                target_file.write(source_file.read())
        except:
            # Attempt connection without verifying the SSL certificate
            context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            with open(ingress["target"], "wb", transport_params={"ssl_context": context}) as target_file:
                target_file.write(source_file.read())

    return ingress["target"]
