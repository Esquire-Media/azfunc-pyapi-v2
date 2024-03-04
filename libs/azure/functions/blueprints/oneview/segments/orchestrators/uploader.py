# File: libs/azure/functions/blueprints/oneview/segments/orchestrators/uploader.py

from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint
import os

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def oneview_orchestrator_segment_uploader(context: DurableOrchestrationContext) -> str:
    """
    Orchestrate the uploading of processed segment data to an S3 bucket.

    This function coordinates the formatting of segment data using Azure Synapse,
    combines processed device data blobs into a single blob, and then uploads
    this consolidated segment data to an Amazon S3 bucket.

    Parameters
    ----------
    context : DurableOrchestrationContext
        The context object provided by the Azure Durable Functions extension.
        The input to this context (ingress) should have the following keys:

        - `segment_id`: The unique identifier for the segment.
        - `source`: A dictionary containing keys:
            - `conn_str`: The connection string key for Azure storage.
            - `container_name`: The Azure blob container name.
            - `prefix`: The prefix for the blob data in Azure.
            - `blob_names`: List of blob paths
        - `target`: A dictionary containing keys:
            - `access_key`: AWS access key.
            - `secret_key`: AWS secret key.
            - `region`: AWS region for the S3 bucket.
            - `bucket`: AWS S3 bucket name.
            - `prefix`: The prefix for the S3 bucket.

    Returns
    -------
    str
        The URL of the consolidated segment data blob.

    Examples
    --------
    This orchestrator function is typically invoked by a durable function client:

    .. code-block:: python

        import azure.functions as func
        import azure.durable_functions as df

        async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
            client = df.DurableOrchestrationClient(starter)
            ingress_data = {
                "segment_id": "XS*******",
                "source": {
                    "conn_str": "AzureConnectionString",
                    "container_name": "my-container",
                    "prefix": "data-prefix",
                    "blob_names": [
                        "path/to/blob_01.csv",
                        "path/to/blob_02.csv",
                        ...
                        "path/to/blob_99.csv",
                    ]
                },
                "target": {
                    "access_key": "AWSAccessKey",
                    "secret_key": "AWSSecretKey",
                    "region": "us-west-1",
                    "bucket": "my-s3-bucket",
                    "prefix": "s3-data-prefix"
                }
            }
            instance_id = await client.start_new('oneview_orchestrator_segment_uploader', None, ingress_data)
            return client.create_check_status_response(req, instance_id)

    Notes
    -----
    - The orchestrator function is idempotent and maintains its state through the `context`.
    - The `synapse_activity_cetas` activity formats the segment data using Azure Synapse, and returns the URLs of the individual device blobs.
    - The `datalake_concat_blobs` activity combines these blobs into a single blob.
    - The `blob_to_s3` activity uploads the consolidated blob to the specified S3 bucket.
    - All the necessary parameters and configuration details are retrieved from the input object `ingress` passed to the orchestrator function.
    """

    ingress = context.get_input()
    # Format segment data using Synapse
    segment_urls = yield context.call_activity(
        "synapse_activity_cetas",
        {
            "instance_id": context.instance_id,
            "bind": "general",
            "table": {"name": f'{context.instance_id}_{ingress["segment_id"]}'},
            "destination": {
                "conn_str": ingress["source"]["conn_str"],
                "container_name": ingress["source"]["container_name"],
                "handle": "sa_esquireroku",
                "format": "CSV_NOHEADER",
                "path": "{}/segment".format(ingress["source"]["prefix"]),
            },
            "query": f"""
                WITH [devices] AS (
                    SELECT DISTINCT
                        [devices] AS [deviceid]
                    FROM OPENROWSET(
                        BULK ('{
                            "','".join(
                                [
                                    ingress["source"]["container_name"] + "/" + b 
                                    for b in ingress["source"]["blob_names"]
                                ]
                            )
                        }'),
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
                    '{ingress["segment_id"]}' AS [segmentid]
                FROM [devices]
                UNION
                SELECT
                    [deviceid],
                    'GOOGLE_AD_ID' AS [type],
                    '{ingress["segment_id"]}' AS [segmentid]
                FROM [devices]
            """,
            "return_urls": True,
        },
    )

    # Combine individual device blobs into a single blob
    segment_url = yield context.call_activity(
        "datalake_concat_blobs",
        {
            "conn_str": ingress["source"]["conn_str"],
            "container_name": ingress["source"]["container_name"],
            "blob_name": "{}/{}.csv".format(
                ingress["source"]["prefix"], ingress["segment_id"]
            ),
            "copy_source_urls": segment_urls,
        },
    )

    # Upload the consolidated segment to S3
    yield context.call_activity(
        "blob_to_s3",
        {
            "source": segment_url,
            "target": {
                "access_key": os.getenv(
                    ingress["target"]["access_key"], ingress["target"]["access_key"]
                ),
                "secret_key": os.getenv(
                    ingress["target"]["secret_key"], ingress["target"]["secret_key"]
                ),
                "region": os.getenv(
                    ingress["target"].get("region", ""),
                    ingress["target"].get("region", None),
                ),
                "bucket": os.getenv(
                    ingress["target"]["bucket"], ingress["target"]["bucket"]
                ),
                "object_key": "{}/{}.csv".format(
                    ingress["target"]["prefix"],
                    ingress["segment_id"],
                ),
            },
        },
    )

    return segment_url
