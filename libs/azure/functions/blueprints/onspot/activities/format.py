from azure.durable_functions import DurableOrchestrationClient
from azure.storage.blob import (
    ContainerClient,
    ContainerSasPermissions,
    generate_container_sas,
)
from datetime import datetime
from dateutil.relativedelta import relativedelta
from libs.azure.functions import Blueprint
import uuid, os

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
@bp.durable_client_input(client_name="client")
async def onspot_activity_format(
    ingress: dict, client: DurableOrchestrationClient
):
    container = ContainerClient.from_connection_string(
        os.environ.get("ONSPOT_CONN_STR", os.environ["AzureWebJobsStorage"]),
        container_name="general",
    )
    sas_token = generate_container_sas(
        account_name=container.credential.account_name,
        account_key=container.credential.account_key,
        container_name=container.container_name,
        permission=ContainerSasPermissions(write=True, read=True),
        expiry=datetime.utcnow() + relativedelta(days=2),
    )
    event_url = client._orchestration_bindings.management_urls[
        "sendEventPostUri"
    ].replace(
        client._orchestration_bindings.management_urls["id"],
        ingress["instance_id"],
    )
    if os.environ.get("REVERSE_PROXY", None):
        event_url = client._replace_url_origin(
            os.environ["REVERSE_PROXY"],
            event_url,
        )

    if ingress["request"].get("type", None) == "FeatureCollection":
        for feature in ingress["request"]["features"]:
            if feature["type"] == "Feature":
                feature["properties"]["callback"] = event_url.replace(
                    "{eventName}", uuid.uuid4().hex
                )
                feature["properties"]["outputLocation"] = (
                    container.url.replace("https://", "az://")
                    + "/{}?".format(ingress["instance_id"])
                    + sas_token
                )

    return ingress["request"]
