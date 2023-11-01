# File: libs/azure/functions/blueprints/esquire/audiences/oneview/orchestrators/updater.py

from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint
from urllib.parse import urlparse
import json, os, logging

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def esquire_audiences_oneview_segment_updater(
    context: DurableOrchestrationContext,
) -> None:
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

    try:
        # Extract input from the context
        record = context.get_input()
        azure = {
            "conn_str": "ONEVIEW_CONN_STR",
            "container_name": os.environ["TASK_HUB_NAME"] + "-largemessages",
        }

        if record:
            s3 = {
                "access_key": "REPORTS_AWS_ACCESS_KEY",
                "secret_key": "REPOSTS_AWS_SECRET_KEY",
                "region": "REPORTS_AWS_REGION",
                "bucket": record["Bucket"],
            }
            # Fetch the audience data from S3 keys
            s3_keys = yield context.call_activity(
                "s3_list_objects",
                {
                    **s3,
                    "prefix": record["Folder"],
                },
            )

            # Raise exception if no S3 keys are found
            if not s3_keys:
                raise Exception(
                    "No S3 files found in {}/{}".format(
                        record["Bucket"], record["Folder"]
                    )
                )

            # Retrieve and normalize the first 12 audience objects from s3
            audiences = yield context.task_all(
                [
                    context.call_activity(
                        "esquire_audiences_oneview_fetch_s3_data",
                        {
                            "source": {
                                **s3,
                                "key": key,
                            },
                            "target": {
                                **azure,
                                "prefix": "{}/raw".format(context.instance_id),
                            },
                        },
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
                        **azure,
                        "blob_name": "{}/raw/{}".format(
                            context.instance_id, "header.csv"
                        ),
                        "content": "street,city,state,zip,zip4",
                    },
                )

                # Call sub-orchestrator for on-spot processing
                onspot_errors = []
                try:
                    onspot_results = yield context.call_sub_orchestrator(
                        "onspot_orchestrator",
                        {
                            "conn_str": azure["conn_str"],
                            "container": azure["container_name"],
                            "outputPath": "{}/devices".format(context.instance_id),
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
                                        [{"url": header_url, "columns": None}]
                                        + addresses_blobs
                                    )
                                ],
                            },
                        },
                    )
                except Exception as e:
                    raise Exception(*onspot_errors)

                onspot_errors += [
                    e["message"]
                    for e in onspot_results["callbacks"]
                    if not e["success"]
                ]
                if onspot_errors:
                    raise Exception(*onspot_errors)

                # Add the processed device blobs to the list
                devices_blobs += [
                    {"url": j["location"].replace("az://", "https://"), "columns": None}
                    for j in onspot_results["jobs"]
                ]

            segment_url = yield context.call_sub_orchestrator(
                "oneview_orchestrator_segment_uploader",
                {
                    "source": {
                        **azure,
                        "prefix": context.instance_id,
                        "blob_names": [
                            "/".join(urlparse(b["url"]).path.split("/")[2:])
                            for b in devices_blobs
                        ],
                    },
                    "segment_id": record["SegmentID"],
                    "target": {
                        "access_key": "ONEVIEW_SEGMENTS_AWS_ACCESS_KEY",
                        "secret_key": "ONEVIEW_SEGMENTS_AWS_SECRET_KEY",
                        "bucket": "ONEVIEW_SEGMENTS_S3_BUCKET",
                        "prefix": os.environ["ONEVIEW_SEGMENTS_S3_PREFIX"],
                    },
                },
            )

            # Retain a copy of the segment file
            yield context.call_activity(
                "datalake_copy_blob",
                {
                    "source": segment_url,
                    "target": {
                        **azure,
                        "blob_name": "segments/{}.csv".format(record["SegmentID"]),
                    },
                },
            )
    except Exception as e:
        yield context.call_http(
            method="POST",
            uri=os.environ["EXCEPTIONS_WEBHOOK_ADOPS"],
            content={
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "themeColor": "EE2A3D",
                "summary": "OneView Segment Update Error",
                "sections": [
                    {
                        "activityTitle": "OneView Segment Update Error",
                        "activitySubtitle": "{}{}".format(
                            str(e)[0:128], "..." if len(str(e)) > 128 else ""
                        ),
                        "facts": [
                            {"name": "SegmentID", "value": record["SegmentID"]},
                            {"name": "Folder", "value": record["Folder"]},
                        ],
                        "markdown": True,
                    }
                ],
                "potentialAction": [
                    {
                        "@type": "OpenUri",
                        "name": "NocoDB",
                        "targets": [
                            {
                                "os": "default",
                                "uri": "https://nocodb.aks.esqads.com/dashboard/#/nc/pifplkhdufgnyxl/mxhurkrks4sfqrj?rowId={}".format(
                                    record["Id"]
                                ),
                            }
                        ],
                    },
                    {
                        "@type": "OpenUri",
                        "name": "AWS S3",
                        "targets": [
                            {
                                "os": "default",
                                "uri": "https://s3.console.aws.amazon.com/s3/buckets/{}?region=us-east-2&prefix={}/&showversions=false".format(
                                    record["Bucket"],
                                    record["Folder"],
                                ),
                            }
                        ],
                    },
                ],
            },
        )

    # Purge history related to this instance
    yield context.call_activity(
        "purge_instance_history",
        {"instance_id": context.instance_id},
    )
