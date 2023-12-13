from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
from azure.durable_functions import DurableOrchestrationClient
import logging
import json
from azure.storage.blob import (
    ContainerClient,
    ContainerSasPermissions,
    generate_blob_sas
)
from datetime import datetime, timedelta
import os
from libs.utils.oauth2.tokens.microsoft import ValidateMicrosoft
from libs.utils.oauth2.tokens import TokenValidationError
import uuid

bp = Blueprint()

@bp.route(route="esquire/matchback/sas", methods=["GET"])
@bp.durable_client_input(client_name="client")
async def http_matchback_getUploadToken(
    req: HttpRequest, client: DurableOrchestrationClient
):
    
    # validate the MS bearer token to ensure the user is authorized to make requests
    try:
        validator = ValidateMicrosoft(
            tenant_id=os.environ['MS_TENANT_ID'], 
            client_id=os.environ['MS_CLIENT_ID']
        )
        logging.warning(req.headers.get('authorization'))
        headers = validator(req.headers.get('authorization'))
    except TokenValidationError as e:
        return HttpResponse(status_code=401, body=f"TokenValidationError: {e}")

    # set storage connection variables
    conn_str = "AzureWebJobsStorage"
    container_name = "uploads"

    # connect to uploads container
    container = ContainerClient.from_connection_string(
        conn_str=os.environ[conn_str],
        container_name=container_name,
    )
    if not container.exists():
        container.create_container()

    # generate a 10-minute SAS token for uploading the upload container
    blob_name = str(uuid.uuid4())
    sas_token = generate_blob_sas(
        account_name=container.credential.account_name,
        account_key=container.credential.account_key,
        container_name=container.container_name,
        blob_name=blob_name,
        permission=ContainerSasPermissions(write=True, create=True),
        start=datetime.utcnow(),
        expiry=datetime.utcnow() + timedelta(minutes=10),
    )
    
    logging.warning(sas_token)

    # generate url with token appended
    sas_url = f"{container.url}?{sas_token}"

    return HttpResponse(json.dumps(
        {
            "credential": sas_url,
            "account_url":f"https://{container.credential.account_name}.blob.core.windows.net",
            "container_name":container.container_name,
            "blob_name":blob_name
        }
    ), status_code=200)
