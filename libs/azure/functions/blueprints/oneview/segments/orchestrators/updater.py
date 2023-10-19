# File: libs/azure/functions/blueprints/oneview/segments/orchestrators/updater.py

from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint
import os

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def oneview_orchestrator_segment_updater(context: DurableOrchestrationContext):
    """
    Orchestration function to update OneView segment data.

    This orchestrator fetches audience data, establishes device lists from the blobs,
    gets device ids from addresses, formats segment data, combines segment blobs, and
    uploads the segment. Finally, it cleans up the data and purges instance history.

    Parameters
    ----------
    context : DurableOrchestrationContext
        The durable function context.

    Returns
    -------
    None

    Raises
    ------
    Exception
        If no S3 files are found in the specified bucket/folder.

    Notes
    -----
    This orchestrator depends on multiple Azure Durable Functions activities and sub-orchestrators
    for its execution.
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
            "oneview_segments_fetch_audiences_s3_keys", egress
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
                    "oneview_segments_fetch_audiences_s3_data",
                    {**egress, "s3_key": key},
                )
                for key in s3_keys
            ]
        )

        # Filter blobs containing device information
        devices_blobs = [a for a in audiences if "devices" in a["columns"]]

        # Filter blobs containing address information
        addresses_blobs = [a for a in audiences if "street" in a["columns"]]
        if len(addresses_blobs):
            # Generate header for on-spot processing
            header = yield context.call_activity(
                "oneview_segments_generate_onspot_header",
                {**egress},
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
                            for a in ([header] + addresses_blobs)
                        ],
                    },
                },
            )

            # Add the processed device blobs to the list
            devices_blobs += [
                {"url": j["location"].replace("az://", "https://"), "columns": None}
                for j in onspot_results["jobs"]
            ]

        # Format segment data using Synapse
        segment_blobs = yield context.call_activity(
            "synapse_activity_cetas",
            {
                "instance_id": context.instance_id,
                "bind": "general",
                "table": {"name": f'{context.instance_id}_{record["SegmentID"]}'},
                "destination": {
                    "conn_str": egress["output"]["conn_str"],
                    "container": egress["output"]["container_name"],
                    "handle": "sa_esquireroku",
                    "format": "CSV_NOHEADER",
                    "path": f"{context.instance_id}/segment",
                },
                "query": f"""
                    WITH [devices] AS (
                        SELECT DISTINCT
                            [devices] AS [deviceid]
                        FROM OPENROWSET(
                            BULK ('{"','".join(["/".join(blob["url"].split("/")[3:]).split("?")[0] for blob in devices_blobs])}'),
                            DATA_SOURCE = 'sa_esquireroku',
                            FORMAT = 'CSV',
                            PARSER_VERSION = '2.0'
                        ) WITH (
                            [devices] VARCHAR(128)
                        ) AS [data]
                        WHERE LEN([devices]) = 36
                    )
                    SELECT
                        [deviceid],
                        'IDFA' AS [type],
                        '{record["SegmentID"]}' AS [segmentid]
                    FROM [devices]
                    UNION
                    SELECT
                        [deviceid],
                        'GOOGLE_AD_ID' AS [type],
                        '{record["SegmentID"]}' AS [segmentid]
                    FROM [devices]
                """,
                "return_urls": True,
            },
        )

        # Combine individual device blobs into a single blob
        segment_blob = yield context.call_activity(
            "oneview_segments_combine_devices_blobs",
            {**egress, "blobs": segment_blobs},
        )

        # Upload the consolidated segment to S3
        segment_url = yield context.call_activity(
            "oneview_segments_upload_segment_file",
            {**egress, "blob_name": segment_blob},
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
