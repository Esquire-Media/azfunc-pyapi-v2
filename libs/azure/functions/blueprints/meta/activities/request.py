# File: libs/azure/functions/blueprints/meta/activities/request.py

from libs.azure.functions import Blueprint
from libs.openapi.clients import Meta
import orjson, os, pandas as pd

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def meta_activity_request(ingress: dict):
    """
    An Azure Durable Function activity to handle API requests.

    This function takes an input dictionary 'ingress' that specifies an API
    operation, security credentials, and optional parameters for data handling.
    It performs the API request and processes the response, including storing
    the results in Azure Blob Storage if specified.

    Parameters
    ----------
    ingress : dict
        A dictionary containing necessary information for the API request. It must
        include the 'operationId' key to identify the API operation. Optionally, it
        can contain 'access_token', 'data', 'parameters', 'destination', and 'return'
        keys for further request customization.

    Returns
    -------
    dict
        A dictionary containing the headers, data, and possibly an error message or
        the next page token for the API response.

    """
    # Initialize the Meta API client factory based on the operationId
    factory = Meta[ingress["operationId"]]

    # Set the access token for API security, preferring the ingress token, then environment variable
    factory.security.setdefault(
        "AccessToken",
        os.environ.get(
            ingress.get("AccessToken", ""), os.environ.get("META_ACCESS_TOKEN", "")
        ),
    )

    # Perform the API request
    headers, response, _ = factory.request(
        data=ingress.get("data", None),
        parameters=ingress.get("parameters", None),
    )

    # Handle any errors in the response
    if getattr(response, "error", None):
        return {"headers": headers, "error": response.error}

    url = None
    # Process and optionally store the response data
    if getattr(response, "data", False):
        df = pd.DataFrame(response.model_dump().get("root", {}).get("data", []))
        if ingress.get("destination", False):
            from azure.storage.blob import BlobClient
            import uuid

            # Create a BlobClient to upload the data to Azure Blob Storage
            blob = BlobClient.from_connection_string(
                conn_str=os.environ[
                    ingress["destination"].get("conn_str", "AzureWebJobsStorage")
                ],
                container_name=ingress["destination"]["container_name"],
                blob_name="{}/{}.parquet".format(
                    ingress["destination"]["blob_prefix"], uuid.uuid4().hex
                ),
            )
            # Upload the data as a parquet file
            blob.upload_blob(df.to_parquet(index=False))
            url = blob.url

    # Prepare and return the final response
    return {
        "headers": headers,
        "data": (
            (
                orjson.loads(response.model_dump_json())["data"]
                if hasattr(response.root, "data") and response.root.data
                else orjson.loads(response.model_dump_json())
            )
            if ingress.get("return", True)
            else url
        ),
        "next": getattr(getattr(response, "root", response), "paging", {}).get("cursors", {}).get("after", None),
    }
