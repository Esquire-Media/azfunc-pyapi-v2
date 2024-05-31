# File: /libs/azure/functions/blueprints/esquire/audiences/builder/activities/finalize.py

from azure.storage.blob import (
    BlobClient,
    BlobSasPermissions,
    generate_blob_sas,
    DelimitedTextDialect,
)
from datetime import datetime
from dateutil.relativedelta import relativedelta
from libs.azure.functions import Blueprint
from urllib.parse import unquote
import io
import os
import pandas as pd

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_finalize(ingress: dict):
    """
    Finalizes the audience data by filtering and renaming device IDs.

    This activity reads data from a source blob, filters and renames device IDs, removes duplicates, and uploads the final data to a destination blob.

    Parameters:
    ingress (dict): A dictionary containing source and destination blob information.
        {
            "source": str or dict,
            "destination": dict
        }

    Returns:
    str: The URL of the destination blob with a SAS token for read access.

    Raises:
    Exception: If an error occurs during the blob operations.
    """
    # Initialize the source BlobClient
    if isinstance(ingress["source"], str):
        input_blob = BlobClient.from_blob_url(ingress["source"])
    else:
        input_blob = BlobClient.from_connection_string(
            conn_str=os.environ[ingress["source"]["conn_str"]],
            container_name=ingress["source"]["container_name"],
            blob_name=ingress["source"]["blob_name"],
        )

    # Initialize the destination BlobClient
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
        has_header="true",  # Use 'true' if the CSV has a header row, otherwise 'false'
    )

    # Identify the device ID column
    column = "deviceid"
    columns = (
        next(
            input_blob.query_blob(
                "SELECT * FROM BlobStorage", blob_format=dialect
            ).records()
        )
        .decode()
        .split(",")
    )
    if columns != "deviceid":
        for c in columns:
            if "device" in c:
                column = c
                break

    # Process the data: filter, rename, remove duplicates, and ensure UUID format
    output_blob.upload_blob(
        pd.read_csv(
            io.BytesIO(
                input_blob.query_blob(
                    "SELECT {} FROM BlobStorage".format(column),
                    blob_format=dialect,
                ).readall()
            )
        )
        .rename(columns={column: "deviceid"})
        .drop_duplicates(keep="first")
        .pipe(lambda df: df[df["deviceid"].str.len() == 36])  # Only UUIDs
        .to_csv(index=False),
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
