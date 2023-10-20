# File: libs/azure/functions/blueprints/oneview/segments/orchestrators/uploader.py

from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint
from urllib.parse import urlparse

bp = Blueprint()

@bp.orchestration_trigger(context_name="context")
def oneview_orchestrator_segment_updater(context: DurableOrchestrationContext) -> str:
    """
    Orchestrator to update segments by consolidating device data, combining blobs, and uploading to S3.

    The orchestrator fetches data from specified blobs, processes it to identify devices and their types,
    combines this data into a single blob, and finally uploads this consolidated segment to an S3 bucket.

    Parameters
    ----------
    context : DurableOrchestrationContext
        The durable function context that provides methods and properties 
        to interact with other durable functions. The context's input should be 
        a dictionary with the following keys:
            - output (dict): Contains keys like "conn_str", "container_name", and "prefix" for Azure Blob Storage.
            - SegmentID (str): Identifier for the segment.
            - blob_urls (list of dicts): Each dict contains a "url" key pointing to a blob.

    Returns
    -------
    str
        The URL of the consolidated segment uploaded to the S3 bucket.

    Examples
    --------
    The function is intended to be called as part of Azure Durable Functions orchestration.
    .. code-block:: python

    import azure.durable_functions as df

    def orchestrator_function(context: df.DurableOrchestrationContext):
        result = yield context.call_activity('oneview_orchestrator_segment_updater', {
            "output" : {
                "conn_str": "AZURE_CONNECTION_STRING",
                "container_name": "mycontainer",
                "prefix": "path/to/data
            },
            "SegmentID": "XS********",
            "copy_source_urls": [
                "https://storage_account_name.blob.core.windows.net/container_name/path/to/blob_01?sastoken_with_read_permission",
                "https://storage_account_name.blob.core.windows.net/container_name/path/to/blob_02?sastoken_with_read_permission",
                # ... additional URLs ...
                "https://storage_account_name.blob.core.windows.net/container_name/path/to/blob_99?sastoken_with_read_permission",
            ]
        })
        return result

    Notes
    -----
    - The orchestrator uses Synapse to format the segment data.
    - The data consolidation is done in two steps: first by fetching and processing the blob data, 
      and then by combining multiple blobs into a single one.
    - The final consolidated segment is uploaded to an S3 bucket.
    """
    ingress = context.get_input()
    # Format segment data using Synapse
    segment_urls = yield context.call_activity(
        "synapse_activity_cetas",
        {
            "instance_id": context.instance_id,
            "bind": "general",
            "table": {"name": f'{context.instance_id}_{ingress["SegmentID"]}'},
            "destination": {
                "conn_str": ingress["output"]["conn_str"],
                "container": ingress["output"]["container_name"],
                "handle": "sa_esquireroku",
                "format": "CSV_NOHEADER",
                "path": "{}/segment".format(ingress["output"]["prefix"]),
            },
            "query": f"""
                WITH [devices] AS (
                    SELECT DISTINCT
                        [devices] AS [deviceid]
                    FROM OPENROWSET(
                        BULK ('{"','".join([urlparse(blob["url"]).path for blob in ingress["blob_urls"]])}'),
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
                    '{ingress["SegmentID"]}' AS [segmentid]
                FROM [devices]
                UNION
                SELECT
                    [deviceid],
                    'GOOGLE_AD_ID' AS [type],
                    '{ingress["SegmentID"]}' AS [segmentid]
                FROM [devices]
            """,
            "return_urls": True,
        },
    )

    # Combine individual device blobs into a single blob
    segment_blob = yield context.call_activity(
        "datalake_concat_blobs",
        {**ingress, "copy_source_urls": segment_urls},
    )

    # Upload the consolidated segment to S3
    segment_url = yield context.call_activity(
        "blob_to_s3",
        {**ingress, "blob_name": segment_blob},
    )
    
    return segment_url