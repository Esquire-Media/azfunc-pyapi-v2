from azure.durable_functions import Blueprint, DurableOrchestrationClient
from azure.functions import HttpRequest, HttpResponse
from azure.storage.blob import (
    BlobClient,
    BlobSasPermissions,
    generate_blob_sas,
)
from datetime import datetime, timedelta
from libs.utils.oauth2.tokens.microsoft import ValidateMicrosoft
from libs.utils.oauth2.tokens import TokenValidationError
import orjson as json, os

bp = Blueprint()


@bp.route(route="esquire/salesUploader/getUploadUrls", methods=["GET", "OPTIONS"])
@bp.durable_client_input(client_name="client")
async def http_salesUploader_getUploadUrls(
    req: HttpRequest, client: DurableOrchestrationClient
):
    """
    Starter which is called in the background while the user is making selections in the Sales Uploader Teams app.
    Provides URLs for uploading sales data and notifying the system once the upload is complete. 

    Upon validation of the Microsoft bearer token for authorization, the function generates a SAS URL for the sales data file to be uploaded to Azure Blob Storage. 
    It also returns URLs to query the upload status and to notify the orchestration client once the upload is complete. 
    This setup aids in securely and efficiently managing file uploads without exposing storage account keys or permissions directly to the client.

    Parameters:
    - req (HttpRequest): The request object. It must include a valid Microsoft bearer token in the `Authorization` header for authentication.
    - client (DurableOrchestrationClient): A client object to start new orchestrations and generate response links for the orchestration instance.

    Returns:
    - HttpResponse: A JSON response containing the SAS URL for the file upload (`uploadBlobUri`), 
        the URL to query the status of the upload (`statusQueryGetUri`), 
        and the URL to post an event once the file upload is completed (`sendEventPostUri`).

    Raises:
    - TokenValidationError: If the validation of the Microsoft bearer token fails, indicating the request is unauthorized.
    """
    # validate the MS bearer token to ensure the user is authorized to make requests
    try:
        validator = ValidateMicrosoft(
            tenant_id=os.environ["MS_TENANT_ID"], client_id=os.environ["MS_CLIENT_ID"]
        )
    except TokenValidationError as e:
        return HttpResponse(status_code=401, body=f"TokenValidationError: {e}")

    # set storage connection variables
    conn_str = (
        "MATCHBACK_CONN_STR"
        if "MATCHBACK_CONN_STR" in os.environ.keys()
        else "AzureWebJobsStorage"
    )
    egress = {
        "runtime_container": {"conn_str": conn_str, "container_name":f"{os.environ['TASK_HUB_NAME']}-largemessages"},
        "uploads_container": {"conn_str": conn_str, "container_name":"sales"},
        "client_config_table":{"conn_str":conn_str, "table_name":"clientConfig"}
    }

    # start a new instance of the orchestrator function
    instance_id = await client.start_new(
        orchestration_function_name="orchestrator_salesUploader",
        client_input={
            **egress,
        },
    )
    response_uris = client.get_client_response_links(
        request=req, instance_id=instance_id
    )

    # connect to uploads container
    blob = BlobClient.from_connection_string(
        conn_str=os.environ[egress["runtime_container"]["conn_str"]],
        container_name=egress["runtime_container"]["container_name"],
        blob_name=f"{instance_id}/01_ingress",
    )

    # generate a 10-minute SAS token for uploading the ingress blob
    sas_token = generate_blob_sas(
        account_name=blob.credential.account_name,
        account_key=blob.credential.account_key,
        container_name=blob.container_name,
        blob_name=blob.blob_name,
        permission=BlobSasPermissions(write=True, create=True),
        start=datetime.utcnow(), # NOTE: Needs to be changed from utcnow to something deterministic
        expiry=datetime.utcnow() + timedelta(minutes=10),
    )

    # generate url with token appended
    blob_sas_url = f"{blob.url}?{sas_token}"

    return HttpResponse(
        body=json.dumps(
            {
                "uploadBlobUri": blob_sas_url,
                "statusQueryGetUri": response_uris["statusQueryGetUri"],
                "sendEventPostUri": response_uris["sendEventPostUri"].replace("{eventName}", "salesUploaded"),
            }
        ).decode(),
        headers={"Content-Type": "application/json"},
        status_code=200,
    )
