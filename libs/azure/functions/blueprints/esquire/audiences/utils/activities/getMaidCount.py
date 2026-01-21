from azure.durable_functions import Blueprint
from azure.storage.blob import (
    BlobClient,
    DelimitedTextDialect,
)

bp: Blueprint = Blueprint()


# activity to grab the geojson data and format the request files for OnSpot
@bp.activity_trigger(input_name="ingress")
async def activity_esquireAudiencesUtils_getMaidCount(ingress: str):
    blob = BlobClient.from_blob_url(ingress)
    # Define the dialect for the CSV format (assumes default comma delimiters)
    dialect = DelimitedTextDialect(
        delimiter=",",  # Specify the delimiter, e.g., comma, semicolon, etc.
        quotechar='"',  # Character used to quote fields
        lineterminator="\n",  # Character used to separate records
        has_header="true",  # Use 'true' if the CSV has a header row, otherwise 'false'
    )

    return int(
        blob.query_blob(
            "SELECT COUNT(*) FROM BlobStorage",
            blob_format=dialect,
            output_format=dialect,
        )
        .readall()
        .decode("utf-8")
        .strip()
    )
