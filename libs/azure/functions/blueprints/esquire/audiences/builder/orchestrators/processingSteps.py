# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/processingSteps.py

from azure.durable_functions import DurableOrchestrationContext
from azure.storage.blob import BlobClient
from libs.azure.functions import Blueprint
import uuid, logging

try:
    import orjson as json
except:
    import json

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiences_processingSteps(
    context: DurableOrchestrationContext,
):
    # ingress = {
    #     "source": {
    #         "conn_str": "ONSPOT_CONN_STR",
    #         "container_name": "general",
    #         "blob_prefix": "audiences",
    #     },
    #     "working": {
    #         "conn_str": "ONSPOT_CONN_STR",
    #         "container_name": "general",
    #         "blob_prefix": "raw",
    #     },
    #     "destination": {
    #         "conn_str": "ONSPOT_CONN_STR",
    #         "container_name": "general",
    #         "blob_prefix": "audiences",
    #     },
    #     "audience": {"id": "clulpbe4r001s12jigokcm2i7"},
    #     "advertiser": {
    #         "id": "test",
    #         "meta": "test",
    #         "oneview": "test",
    #         "xandr": "test",
    #     },
    #     "status": "test",
    #     "rebuild": "test",
    #     "rebuildUnit": "test",
    #     "TTL_Length": "test",
    #     "TTL_Unit": "test",
    #     "dataSource": {"id": "test", "dataType": "test"},
    #     "dataFilter": "test",
    #     "processes": {
    #         "id": "test",
    #         "sort": "test",
    #         "outputType": "test",
    #         "customCoding": "test",
    #     },
    #     "results": ["blob_urls"],
    # }
    # egress = {
    #     "working": {
    #         "conn_str": "ONSPOT_CONN_STR",
    #         "container_name": "general",
    #         "blob_prefix": "raw /instanceid/audienceid/step/working",
    #     },
    #     "destination": {
    #         "conn_str": "ONSPOT_CONN_STR",
    #         "container_name": "general",
    #         "blob_prefix": "raw /instanceid/audienceid/step",
    #     },
    # }

    ingress = context.get_input()

    # Loop through processing steps
    for step, process in enumerate(
        processes := ingress["audience"].get("processes", [])
    ):
        # Reusable common input for sub-orchestrators
        egress = {
            "working": {
                **ingress["working"],
                "blob_prefix": "{}/{}/{}/{}".format(
                    ingress["working"]["blob_prefix"],
                    ingress["instance_id"],
                    ingress["audience"]["id"],
                    step,
                ),
            },
            "destination": {
                **ingress["destination"],
                "blob_prefix": "{}/{}/{}/{}/results".format(
                    ingress["working"]["blob_prefix"],
                    ingress["instance_id"],
                    ingress["audience"]["id"],
                    step,
                ),
            },
        }
        inputType = (
            processes[step - 1]["outputType"]  # Use previous step's output type
            if step
            else ingress["audience"]["dataSource"]["dataType"]  # Use primary data type
        )
        egress["transform"] = [inputType, process["outputType"]]
        source_urls = (
            processes[step - 1].get("results", []) if step else ingress["results"]
        )
        if not source_urls:
            raise Exception(
                "No data to process from previous step. [{}]: {} -> {}".format(
                    step - 1, inputType, process["outputType"]
                )
            )
        try:
            custom_coding = json.loads(process.get("customCoding", "{}"))
        except:
            custom_coding = {}

        # Switch logistics based on the input data type
        match inputType:
            case "addresses":
                match process["outputType"]:
                    case "addresses":  # addresses -> addresses
                        # TODO: figure out what (if anything) should happen here
                        pass
                    case "device_ids":  # addresses -> deviceids
                        onspot = yield context.task_all(
                            [
                                context.call_sub_orchestrator(
                                    "onspot_orchestrator",
                                    {
                                        **egress["working"],
                                        "endpoint": "/save/addresses/all/devices",
                                        "request": {
                                            "hash": False,
                                            "name": uuid.uuid4().hex,
                                            "fileName": uuid.uuid4().hex + ".csv",
                                            "fileFormat": {
                                                "delimiter": ",",
                                                "quoteEncapsulate": True,
                                            },
                                            "mappings": {
                                                # "street": ["delivery_line_1"],
                                                "city": ["city"],
                                                "state": ["state"],
                                                "zip": ["zipcode"],
                                                "zip4": ["zip4"],
                                            },
                                            "matchAcceptanceThreshold": 29.9,
                                            "sources": [
                                                source_url.replace(
                                                    "https://", "az://"
                                                )
                                                for source_url in source_urls
                                            ],
                                        },
                                    },
                                )
                            ]
                        )
                        process["results"] = []
                        for result in onspot:
                            job_location_map = {
                                job["id"]: job["location"].replace("az://", "https://")
                                for job in result["jobs"]
                            }
                            for callback in result["callbacks"]:
                                if callback["success"]:
                                    if callback["id"] in job_location_map:
                                        process["results"].append(
                                            job_location_map[callback["id"]]
                                        )
                    case "polygons":  # addresses -> polygons
                        process["results"] = yield context.task_all(
                            [
                                context.call_sub_orchestrator(
                                    "orchestrator_esquireAudienceMaidsAddresses_footprint",
                                    {
                                        **egress,
                                        "source": source_url,
                                    },
                                )
                                for source_url in source_urls
                            ]
                        )
            case "device_ids":
                match process["outputType"]:
                    case "addresses":  # deviceids -> addresses
                        # onspot /save/files/household
                        onspot = yield context.task_all(
                            [
                                context.call_sub_orchestrator(
                                    "onspot_orchestrator",
                                    {
                                        **egress["working"],
                                        "endpoint": "/save/files/household",
                                        "request": {
                                            "type": "FeatureCollection",
                                            "features": [
                                                {
                                                    "type": "Files",
                                                    "paths": [
                                                        url.replace("https://", "az://")
                                                        for url in source_urls
                                                    ],
                                                    "properties": {
                                                        "name": uuid.uuid4().hex,
                                                        "fileName": uuid.uuid4().hex + ".csv",
                                                        "hash": False,
                                                        "fileFormat": {
                                                            "delimiter": ",",
                                                            "quoteEncapsulate": True,
                                                        },
                                                    },
                                                }
                                            ],
                                        },
                                    },
                                )
                            ]
                        )
                        process["results"] = []
                        for result in onspot:
                            job_location_map = {
                                job["id"]: job["location"].replace("az://", "https://")
                                for job in result["jobs"]
                            }
                            for callback in result["callbacks"]:
                                if callback["success"]:
                                    if callback["id"] in job_location_map:
                                        process["results"].append(
                                            job_location_map[callback["id"]]
                                        )
                    case "device_ids":  # deviceids -> deviceids
                        # onspot /save/files/demographics/all
                        process["results"] = yield context.task_all(
                            [
                                context.call_sub_orchestrator(
                                    "orchestrator_esquireAudienceMaidsDeviceIds_todevids",
                                    {
                                        "working": egress["working"],
                                        "source": source_url,
                                        "destination": {
                                            **egress["destination"],
                                            "blob_name": "{}/results/{}.csv".format(
                                                egress["destination"]["blob_prefix"],
                                                index,
                                            ),
                                        },
                                    },
                                )
                                for index, source_url in enumerate(source_urls)
                            ]
                        )
                    case "polygons":  # deviceids -> polygons
                        # TODO: figure out what (if anything) should happen here
                        pass
            case "polygons":
                match process["outputType"]:
                    case "addresses":  # polygons -> addresses
                        # onspot /save/files/household
                        pass
                    case "device_ids":  # polygons -> deviceids
                        requests = yield context.task_all(
                            [
                                context.call_activity(
                                    "activity_esquireAudienceBuilder_formatOnspotRequest",
                                    {
                                        **egress,
                                        "custom_coding": custom_coding,
                                        "source_url": source_url,
                                    },
                                )
                                for source_url in source_urls
                            ]
                        )
                        onspot = yield context.task_all(
                            [
                                context.call_sub_orchestrator(
                                    "onspot_orchestrator",
                                    {
                                        **egress["working"],
                                        "endpoint": "/save/geoframe/all/devices",
                                        "request": json.loads(
                                            BlobClient.from_blob_url(source_url)
                                            .download_blob()
                                            .readall()
                                        ),
                                    },
                                )
                                for source_url in requests
                            ]
                        )
                        process["results"] = []
                        for result in onspot:
                            job_location_map = {
                                job["id"]: job["location"].replace("az://", "https://")
                                for job in result["jobs"]
                            }
                            for callback in result["callbacks"]:
                                if callback["success"]:
                                    if callback["id"] in job_location_map:
                                        process["results"].append(
                                            job_location_map[callback["id"]]
                                        )
                    case "polygons":  # polygons -> polygons
                        # TODO: figure out what (if anything) should happen here
                        pass

        if custom_coding.get("filter", False):
            logging.warning(
                "[{}]: {} -> {}".format(step, inputType, process["outputType"])
            )
            process["results"] = yield context.task_all(
                [
                    context.call_activity(
                        "activity_esquireAudienceBuilder_filterResults",
                        {
                            "source": url,
                            "destination": egress["working"],
                            "filter": custom_coding["filter"],
                        },
                    )
                    for url in process["results"]
                ]
            )

    return ingress
