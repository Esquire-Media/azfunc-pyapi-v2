# File: /libs/azure/functions/blueprints/esquire/audiences/builder/activities/filterResults.py.py

from azure.storage.blob import (
    BlobClient,
    BlobSasPermissions,
    generate_blob_sas,
    DelimitedTextDialect,
)
from azure.storage.blob._quick_query_helper import BlobQueryReader
from datetime import datetime
from dateutil.relativedelta import relativedelta
from libs.azure.functions import Blueprint
from libs.azure.functions.blueprints.esquire.audiences.builder.utils import (
    jsonlogic_to_sql,
)
from urllib.parse import unquote
import os, logging


bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_filterResults(ingress: dict):
    if isinstance(ingress["source"], str):
        input_blob = BlobClient.from_blob_url(ingress["source"])
    else:
        input_blob = BlobClient.from_connection_string(
            conn_str=os.environ[ingress["source"]["conn_str"]],
            container_name=ingress["source"]["container_name"],
            blob_name=ingress["source"]["blob_name"],
        )

    output_blob = BlobClient.from_connection_string(
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
        # escapechar='"',  # Character used to escape quote characters within a field
        has_header="true",  # Use 'true' if the CSV has a header row, otherwise 'false'
    )
    output_blob.upload_blob(
        input_blob.query_blob(
            "SELECT * FROM BlobStorage WHERE {}".format(
                jsonlogic_to_sql(ingress["filter"])
            ),
            blob_format=dialect,
        ).readall(),
        overwrite=True,
    )
    return (
        unquote(output_blob.url)
        + "?"
        + generate_blob_sas(
            account_name=output_blob.account_name,
            container_name=output_blob.container_name,
            blob_name=output_blob.blob_name,
            account_key=output_blob.credential.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + relativedelta(days=2),
        )
    )
