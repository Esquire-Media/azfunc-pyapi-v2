# File: libs/azure/functions/blueprints/daily_audience_generation/activities/process_geoframed_audiences.py

from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from azure.storage.blob import ContainerClient
import os, json

bp: Blueprint = Blueprint()


# main orchestrator for geoframed audiences (suborchestrator for the root)
@bp.orchestration_trigger(context_name="context")
def suborchestrator_geoframed_audiences(context: DurableOrchestrationContext):
    ingress = context.get_input()
    retry = RetryOptions(15000, 1)

    # get the geoframe information for the audience
    yield context.task_all(
        [
            context.call_activity_with_retry(
                "activity_load_salesforce_geojsons",
                retry_options=retry,
                input_={
                    "instance_id": ingress["instance_id"],
                    "blob_prefix": ingress["blob_prefix"],
                    **audience,
                    "context": None,
                },
            )
            for audience in ingress["audiences"]
        ]
    )

    # make connection to the container
    container_client = ContainerClient.from_connection_string(
        conn_str=os.environ["ONSPOT_CONN_STR"],
        container_name="general",
    )
    # pass the audience information to OnSpot
    yield context.task_all(
        [
            context.call_sub_orchestrator_with_retry(
                "onspot_orchestrator",
                retry,
                {
                    "conn_str": ingress["conn_str"],
                    "container": ingress["container"],
                    "outputPath": "{}/{}/{}".format(
                        ingress["path"], audience["Id"], "devices"
                    ),
                    "endpoint": "/save/geoframe/all/devices",
                    "request": json.load(blob_client.download_blob()),
                },
                subinstance_id,
            )
            for audience in ingress["audiences"]
            if (subinstance_id := "{}:{}".format(context.instance_id, audience["Id"]))
            if (
                blob_client := container_client.get_blob_client(
                    f"{ingress['path']}/{audience['Id']}/{audience['Id']}.geojson"
                )
            ).exists()
        ]
    )

    yield context.task_all(
        [
            context.call_activity_with_retry(
                "activity_update_audience_devices",
                retry,
                {
                    "blob_prefix": ingress["blob_prefix"],
                    "instance_id": ingress["instance_id"],
                    "audience_id": audience['Id']
                },
            )
            for audience in ingress['audiences']
        ]
    )
    return {}
