from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext, RetryOptions
import os
import logging
import orjson as json
from azure.storage.blob import BlobClient
from libs.azure.functions.blueprints.esquire.location_insights.helpers import (
    cetas_query_unique_deviceids,
)

bp = Blueprint()

# TODO start and end dates not lining up? off by 1 hr?


@bp.orchestration_trigger(context_name="context")
def orchestrator_locationInsights_report(context: DurableOrchestrationContext):
    """
    Once device observations are pulled, the remainder of the work is done by this orchestrator on an individual report basis.
    """

    # if a custom conn string is set, use that. Otherwise use AzureWebJobsStorage
    # commonly used variables
    settings = context.get_input()
    retry = RetryOptions(15000, 1)
    egress = {**settings, "instance_id": context.instance_id}

    # call activity to get location info such as address, owner, latlong, and geometry
    egress["runtime_container"]["location_blob"] = f"{egress['batch_instance_id']}/{context.instance_id}/locations.csv"
    yield context.call_activity_with_retry(
        "activity_locationInsights_getLocationInfo",
        retry,
        {
            **egress
        },
    )

    # call activity to build the Onspot observations payload
    observations_request = yield context.call_activity_with_retry(
        "activity_locationInsights_createObservationsRequest",
        retry,
        {
            **egress
        },
    )

    # send a request to Onspot to get device observations for this location
    # returned data will be stored in CSV format inside the `observations` folder
    egress["runtime_container"]["observations_blob"] = f"{egress['batch_instance_id']}/{context.instance_id}/observations"
    onspot_job = yield context.call_sub_orchestrator(
        "orchestrator_onspot",
        {
            "conn_str": egress["runtime_container"]["conn_str"],
            "container": egress["runtime_container"]["container_name"],
            "outputPath": egress["runtime_container"]["observations_blob"],
            "endpoint": "/save/geoframe/all/observations",
            "request": observations_request,
        },
        instance_id=f"{context.instance_id}:obs",
    )

    # check that all device observation jobs were completed successfully
    failed_callbacks = [
        callback["id"]
        for callback in onspot_job["callbacks"]
        if not callback["success"]
    ]
    if len(failed_callbacks):
        raise Exception(
            f"Device Observations job failed for the following job_id: {json.dumps([job for job in onspot_job['jobs'] if job['id'] in failed_callbacks])}"
        )

    # use Synapse to extract unique deviceids from the device observations file
    egress["runtime_container"]["unique_devices_blob"] = f"{egress['batch_instance_id']}/{context.instance_id}/unique_devices"
    unique_device_urls = yield context.call_activity_with_retry(
        "activity_synapse_cetas",
        retry,
        {
            "instance_id": context.instance_id,
            "bind": "synapse-general",
            "table": {"name": "unique_deviceids"},
            "destination": {
                "conn_str": egress["runtime_container"]["conn_str"],
                "container": egress["runtime_container"]["container_name"],
                "handle": "sa_esquirereports",
                "path": egress["runtime_container"]["unique_devices_blob"],
                "format": "CSV",
            },
            "query": cetas_query_unique_deviceids(
                paths=f"{egress['runtime_container']['container_name']}/{egress['runtime_container']['observations_blob']}/*",
                handle="sa_esquirereports",
            ),
            "return_urls": True,
        },
    )

    # send a request for demographics data based on the unique devices which were partitioned out by Synapse
    egress["runtime_container"]["demographics_blob"] = f"{egress['batch_instance_id']}/{context.instance_id}/demographics"
    yield context.call_sub_orchestrator(
        "orchestrator_onspot",
        {
            "conn_str": egress["runtime_container"]["conn_str"],
            "container": egress["runtime_container"]["container_name"],
            "outputPath": egress["runtime_container"]["demographics_blob"],
            "endpoint": "/save/files/demographics/all",
            "request": {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Files",
                        "paths": [
                            url.replace("https://", "az://")
                            for url in unique_device_urls
                        ],
                        "properties": {
                            # TODO 11/7 - issue is probably in here somewhere
                            "name": "demographics",
                            "hash": False,
                        },
                    }
                ],
            },
        },
        instance_id=f"{context.instance_id}:demos",
    )

    # call activity to build the Onspot observations payload
    egress["runtime_container"]["output_blob"] = f"{egress['batch_instance_id']}/{context.instance_id}/output"
    output_blob_name = yield context.call_activity_with_retry(
        "activity_locationInsights_buildReport",
        retry,
        {
            **egress
        },
    )

    return output_blob_name