# File: libs/azure/functions/blueprints/esquire/audiences/daily_audience_generation/orchestrators/process_addressed_audiences.py

from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from azure.storage.blob import (
    ContainerClient,
    ContainerSasPermissions,
    generate_container_sas,
)
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

bp: Blueprint = Blueprint()


# main orchestrator for geoframed audiences (suborchestrator for the root)
@bp.orchestration_trigger(context_name="context")
def suborchestrator_addressed_audiences(context: DurableOrchestrationContext):
    ingress = context.get_input()
    retry = RetryOptions(15000, 1)

    # logging.warning(f"Ingress: {ingress}")

    # pass all of the audiences into the acivity to get the formatted address lists and put in the 'raw' location
    yield context.task_all(
        [
            context.call_activity_with_retry(
                "activity_format_address_lists",
                retry_options=retry,
                input_={
                    "blob_prefix": ingress["blob_prefix"],
                    "instance_id": ingress["instance_id"],
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
    # read the digital neighbor files from their location and pass into load_neighbors activity
        ## this will save the correct addresses file of the neighbor's data
    # yield context.task_all(
    #     [
    #         context.call_activity_with_retry(
    #             "activity_load_neighbor_addresses",
    #             retry_options=retry,
    #             input_={
    #                 "blob_prefix": ingress["blob_prefix"],
    #                 "instance_id": ingress["instance_id"],
    #                 "filename": audience_file.name,
    #                 "context": None,
    #             },
    #         )
    #         for audience_file in container_client.list_blobs()
    #     ]
    # )
    
    # generate sas token
    sas_token = generate_container_sas(
        account_name=container_client.account_name,
        account_key=container_client.credential.account_key,
        container_name=container_client.container_name,
        permission=ContainerSasPermissions(write=True, read=True),
        expiry=datetime.utcnow() + relativedelta(days=2),
    )
    
    # pass New Movers and *Digital Neighbors* to OnSpot Orchestrator
    # yield context.task_all(
    #     [
    #         context.call_sub_orchestrator_with_retry(
    #             "onspot_orchestrator",
    #             retry,
    #             {
    #                 "conn_str": ingress["conn_str"],
    #                 "container": ingress["container"],
    #                 "outputPath": "{}/{}/{}".format(
    #                     ingress["path"], audience["Id"], "devices"
    #                 ),
    #                 "endpoint": "/save/addresses/all/devices",
    #                 "request": {
    #                     "hash": False,
    #                     "name": audience["Id"],
    #                     "fileName": audience["Id"],
    #                     "fileFormat": {
    #                         "delimiter": ",",
    #                         "quoteEncapsulate": True,
    #                     },
    #                     "mappings": {
    #                         "street": ["address"],
    #                         "city": ["city"],
    #                         "state": ["state"],
    #                         "zip": ["zipcode"],
    #                         "zip4": ["zip4Code"],
    #                     },
    #                     "matchAcceptanceThreshold": 29.9,
    #                     "sources": [
    #                         blob_client.url.replace("https://", "az://")
    #                         + "?"
    #                         + sas_token
    #                     ],
    #                 },
    #             },
    #             subinstance_id,
    #         )
    #         for audience in ingress["audiences"]
    #         if (subinstance_id := "{}:{}".format(context.instance_id, audience["Id"]))
    #         if (
    #             blob_client := container_client.get_blob_client(
    #                 f"{ingress['path']}/{audience['Id']}/{audience['Id']}.csv"
    #             )
    #         ).exists()
    #     ]
    # )

    # yield context.task_all(
    #     [
    #         context.call_activity_with_retry(
    #             "activity_update_audience_devices",
    #             retry,
    #             {
    #                 "blob_prefix": ingress["blob_prefix"],
    #                 "instance_id": ingress["instance_id"],
    #                 "audience_id": audience['Id']
    #             },
    #         )
    #         for audience in ingress['audiences']
    #     ]
    # )
    return {}
