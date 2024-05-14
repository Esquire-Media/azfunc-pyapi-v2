#  file path:libs/azure/functions/blueprints/esquire/audiences/meta/activities/get_total_maids.py

from libs.azure.functions import Blueprint
from azure.storage.blob import (
    BlobClient,
    DelimitedTextDialect,
    BlobSasPermissions,
    generate_blob_sas,
)
from memory_profiler import profile
from libs.azure.functions import Blueprint
from io import BytesIO
import logging, os, json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

bp: Blueprint = Blueprint()


# activity to grab the geojson data and format the request files for OnSpot
@bp.activity_trigger(input_name="ingress")
async def activity_esquireAudiencesMeta_getTotalMaids(ingress: dict):
    # set blob information
    blob = BlobClient.from_connection_string(
        conn_str=ingress["conn_str"],
        container_name=ingress["container_name"],
        blob_name=ingress["blob_name"],
    )

    # get maids count
    df = pd.read_csv(BytesIO(blob.download_blob().readall()), header=None)
    total_maids = len(df)

    # get the blob_url
    blob_url_with_sas = (
        blob.url
        + "?"
        + generate_blob_sas(
            account_name=blob.account_name,
            container_name=blob.container_name,
            blob_name=blob.blob_name,
            account_key=blob.credential.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + relativedelta(days=2),
        )
    )
    
    # query = "SELECT * FROM BlobStorage"
    # reader = query_csv_blob(blob_url_with_sas, query)
    # line_reader(reader)
    # logging.warning(reader)

    return (blob_url_with_sas, int(total_maids))


# def line_reader(reader):
#     with open("test.csv", "wb") as f:
#         for i, r in enumerate(reader.records()):
#             if i > 100000:
#                 f.write(r)
#             if i > 102000:
#                 break


# def query_csv_blob(blob_url_with_sas, query):
#     """
#     Queries a CSV blob using a SAS URL and a SQL-like query, using DelimitedTextDialect.

#     Args:
#     blob_url_with_sas (str): The full blob URL including the SAS token.
#     query (str): SQL-like query string to filter the CSV data.

#     Returns:
#     str: The result of the query as a string.
#     """
#     # Create a BlobClient using the Blob URL that includes the SAS token
#     blob_client = BlobClient.from_blob_url(blob_url_with_sas)

#     # Define the dialect for the CSV format (assumes default comma delimiters)
#     dialect = DelimitedTextDialect(
#         delimiter=",",  # Specify the delimiter, e.g., comma, semicolon, etc.
#         quote_character='"',  # Character used to quote fields
#         escape_character='"',  # Character used to escape quote characters within a field
#         record_separator="\n",  # Character used to separate records
#         header="true",  # Use 'true' if the CSV has a header row, otherwise 'false'
#     )

#     # Execute the query
#     reader = blob_client.query_blob(query, blob_format=dialect, output_format=dialect)

#     return reader
