from azure.durable_functions import Blueprint
from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions
import os
import logging
import uuid
import orjson as json
from azure.storage.blob import BlobClient
from libs.azure.functions.blueprints.esquire.reporting.location_insights.helpers import (
    cetas_query_unique_deviceids,
)

bp = Blueprint()

# TODO start and end dates not lining up? off by 1 hr?


@bp.orchestration_trigger(context_name="context")
def orchestrator_locationInsights_location(context: DurableOrchestrationContext):
    """
    Pull the data for the report to then combine
    """

    # if a custom conn string is set, use that. Otherwise use AzureWebJobsStorage
    # commonly used variables
    settings = context.get_input()
    retry = RetryOptions(15000, 1)
    egress = {**settings, "instance_id": context.instance_id}
    locationID = egress['locationID']

    # call activity to get location info such as address, owner, latlong, and geometry
    egress["runtime_container"]["location_blob"] = f"{egress['batch_instance_id']}/{egress['pull_id']}/{locationID}/locations.csv"
    yield context.call_activity_with_retry(
        "activity_locationInsights_getLocationInfo",
        retry,
        {**egress},
    )

    # call activity to build the Onspot observactions payload
    observations_request = yield context.call_activity_with_retry(
        "activity_locationInsights_createObservationsRequest",
        retry,
        {**egress},
    )

    # send a request to Onspot to get device observations for this location
    # returned data will be stored in CSV format inside the `observations` folder
    egress["runtime_container"]["observations_blob"] = f"{egress['batch_instance_id']}/{egress['pull_id']}/{locationID}/observations"

    onspot_job = yield context.call_sub_orchestrator(
        "onspot_orchestrator",
        {
            "conn_str": egress["runtime_container"]["conn_str"],
            "container": egress["runtime_container"]["container_name"],
            "outputPath": egress["runtime_container"]["observations_blob"],
            "endpoint": "/save/geoframe/all/observations",
            "request": observations_request,
        },
        instance_id=f"{context.instance_id}:{locationID}:obs",
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
    egress["runtime_container"]["unique_devices_blob"] = f"{egress['batch_instance_id']}/{egress['pull_id']}/{locationID}/unique_devices"
    unique_device_urls = yield context.call_activity_with_retry(
        "synapse_activity_cetas",
        retry,
        {
            "instance_id": context.instance_id,
            "bind": "synapse-general",
            "table": {"name": "unique_deviceids"},
            "destination": {
                "conn_str": egress["runtime_container"]["conn_str"],
                "container_name": egress["runtime_container"]["container_name"],
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
    egress["runtime_container"]["demographics_blob"] = f"{egress['batch_instance_id']}/{egress['pull_id']}/{locationID}/demographics"
    yield context.call_sub_orchestrator(
        "onspot_orchestrator",
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

    return {
            "locationID": locationID,
            "location_blob": egress["runtime_container"]["location_blob"],
            "observations_blob": egress["runtime_container"]["observations_blob"],
            "demographics_blob": egress["runtime_container"]["demographics_blob"],
        }