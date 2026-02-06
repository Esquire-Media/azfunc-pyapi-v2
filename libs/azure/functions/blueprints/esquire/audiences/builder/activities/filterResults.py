# File: /libs/azure/functions/blueprints/esquire/audiences/builder/activities/filterResults.py

from azure.durable_functions import Blueprint
from azure.storage.blob import (
    BlobClient,
    BlobSasPermissions,
    generate_blob_sas,
    DelimitedTextDialect,
)
from libs.utils.azure_storage import get_cached_blob_client, init_blob_client
from datetime import datetime
from dateutil.relativedelta import relativedelta
from libs.azure.functions.blueprints.esquire.audiences.builder.utils import (
    jsonlogic_to_sql,
)
from urllib.parse import unquote
import os

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_filterResults(ingress: dict):
    """
    Filters results from a source blob and uploads the filtered results to a destination blob.

    This activity reads data from a source blob, applies a filter using JSON Logic converted to SQL, and writes the filtered data to a destination blob.

    Parameters:
    ingress (dict): A dictionary containing source and destination blob information, and the filter to apply.
        {
            "source": str or dict,
            "destination": dict,
            "filter": dict
        }

    Returns:
    str: The URL of the destination blob with a SAS token for read access.

    Raises:
    Exception: If an error occurs during the blob operations.
    """
    # Initialize the source BlobClient
    if isinstance(ingress["source"], str):
        input_blob = get_cached_blob_client(ingress["source"])
    else:
        input_blob = init_blob_client(
            conn_str=os.environ[ingress["source"]["conn_str"]],
            container_name=ingress["source"]["container_name"],
            blob_name=ingress["source"]["blob_name"],
        )

    # Initialize the destination BlobClient
    output_blob = init_blob_client(
        conn_str=os.environ[ingress["destination"]["conn_str"]],
        container_name=ingress["destination"]["container_name"],
        blob_name="{}/{}".format(
            ingress["destination"]["blob_prefix"],
            os.path.basename(input_blob.blob_name),
        ),
    )

    # Define the dialect for the CSV format (assumes default comma delimiters)
    dialect = DelimitedTextDialect(
        delimiter=",",  # Specify the delimiter, e.g., comma, semicolon, etc.
        quotechar='"',  # Character used to quote fields
        lineterminator="\n",  # Character used to separate records
        has_header="true",  # Use 'true' if the CSV has a header row, otherwise 'false'
    )

    # Query the source blob with the filter and upload the results to the destination blob
    output_blob.upload_blob(
        input_blob.query_blob(
            "SELECT * FROM BlobStorage WHERE {}".format(
                jsonlogic_to_sql(ingress["filter"])
            ),
            blob_format=dialect,
        ).readall(),
        overwrite=True,
    )

    # Generate a SAS token for the destination blob with read permissions
    sas_token = generate_blob_sas(
        account_name=output_blob.account_name,
        container_name=output_blob.container_name,
        blob_name=output_blob.blob_name,
        account_key=output_blob.credential.account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + relativedelta(days=2),
    )

    # Return the URL of the destination blob with the SAS token
    return unquote(output_blob.url) + "?" + sas_token
