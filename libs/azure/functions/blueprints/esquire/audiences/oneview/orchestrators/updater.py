# File: libs/azure/functions/blueprints/esquire/audiences/oneview/orchestrators/updater.py

from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint
import os

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def oneview_orchestrator_segment_updater(context: DurableOrchestrationContext) -> None:
    """
    Orchestration function to update audience segments in OneView.

    This orchestrator function is responsible for processing audience data from S3,
    storing the processed data in Azure Data Lake, and uploading the consolidated
    data to OneView. The function employs both activity and sub-orchestration calls
    to achieve the desired outcome.

    Parameters
    ----------
    context : DurableOrchestrationContext
        The orchestration context object that provides methods and properties
        used for orchestrator function execution. The input for the orchestrator
        should be a dictionary with details related to the record, such as the bucket name
        and folder for the S3 data.

    Returns
    -------
    None

    Examples
    --------
    In an orchestrator function:

    .. code-block:: python

        import azure.durable_functions as df

        def main_function(context: df.DurableOrchestrationContext):
            result = yield context.call_sub_orchestrator('oneview_orchestrator_segment_updater', {
                "Bucket": "my-s3-bucket",
                "Folder": "path/to/s3/data",
                "SegmentID": "segment_1234"
            })
            return result

    Notes
    -----
    - The function retrieves and processes audience data from specified S3 paths.
    - Processed data is stored in Azure Data Lake and then uploaded to OneView.
    - Temporary data and history related to the orchestration are cleaned up at the end.
    """

    # Extract input from the context
    record = context.get_input()

    # Define egress configuration details
    egress = {
        "output": {
            "conn_str": "ONEVIEW_CONN_STR",
            "container_name": os.environ["TASK_HUB_NAME"] + "-largemessages",
            "prefix": context.instance_id,
        },
        "record": record,
    }

    if record:
        # Fetch the audience data from S3 keys
        s3_keys = yield context.call_activity(
            "s3_list_objects",
            {
                "access_key": "REPORTS_AWS_ACCESS_KEY",
                "secret_key": "REPOSTS_AWS_SECRET_KEY",
                "region": "REPORTS_AWS_REGION",
                "bucket": record["Bucket"],
                "prefix": record["Folder"],
            },
        )

        # Raise exception if no S3 keys are found
        if not s3_keys:
            raise Exception(
                "No S3 files found in {}/{}".format(record["Bucket"], record["Folder"])
            )

        # Retrieve audience data for each S3 key
        audiences = yield context.task_all(
            [
                context.call_activity(
                    "esquire_audiences_oneview_fetch_s3_data",
                    {**egress, "s3_key": key},
                )
                for key in s3_keys[:11]
            ]
        )

        # Filter blobs containing device information
        devices_blobs = [a for a in audiences if "devices" in a["columns"]]

        # Filter blobs containing address information
        addresses_blobs = [a for a in audiences if "street" in a["columns"]]
        if len(addresses_blobs):
            # Generate header for on-spot processing
            header_url = yield context.call_activity(
                "datalake_simple_write",
                {
                    "conn_str": egress["output"]["conn_str"],
                    "container_name": egress["output"]["container_name"],
                    "blob_name": "{}/raw/{}".format(
                        egress["output"]["prefix"], "header.csv"
                    ),
                    "content": "street,city,state,zip,zip4",
                },
            )

            # Call sub-orchestrator for on-spot processing
            onspot_results = yield context.call_sub_orchestrator(
                "onspot_orchestrator",
                {
                    "conn_str": egress["output"]["conn_str"],
                    "container": egress["output"]["container_name"],
                    "outputPath": "{}/devices".format(egress["output"]["prefix"]),
                    "endpoint": "/save/addresses/all/devices",
                    "request": {
                        "hash": False,
                        "name": record["SegmentID"],
                        "fileFormat": {
                            "delimiter": ",",
                            "quoteEncapsulate": True,
                        },
                        "mappings": {
                            "street": ["street"],
                            "city": ["city"],
                            "state": ["state"],
                            "zip": ["zip"],
                            "zip4": ["zip4"],
                        },
                        "matchAcceptanceThreshold": 29.9,
                        "sources": [
                            a["url"].replace("https://", "az://")
                            for a in (
                                [{"url": header_url, "columns": None}] + addresses_blobs
                            )
                        ],
                    },
                },
            )

            onspot_errors = [
                e["message"] for e in onspot_results["callbacks"] if not e["success"]
            ]
            if onspot_errors:
                raise Exception(*onspot_errors)

            # Add the processed device blobs to the list
            devices_blobs += [
                {"url": j["location"].replace("az://", "https://"), "columns": None}
                for j in onspot_results["jobs"]
            ]

        segment_url = yield context.call_sub_orchestrator(
            "oneview_orchestrator_segment_uplaoder",
            {
                "SegmentID": record["segmentID"],
                "output": {
                    **egress["output"],
                    "blob_name": "{}/{}.csv".format(
                        egress["output"]["prefix"],
                        record["segmentID"],
                    ),
                },
            },
        )

    # Cleanup - remove temporary data
    yield context.call_activity(
        "datalake_activity_delete_directory",
        {
            "conn_str": egress["output"]["conn_str"],
            "container": egress["output"]["container_name"],
            "prefix": context.instance_id,
        },
    )

    # Purge history related to this instance
    yield context.call_activity(
        "purge_instance_history",
        {"instance_id": context.instance_id},
    )
