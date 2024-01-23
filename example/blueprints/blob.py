from azure.functions import InputStream
from libs.azure.functions import Blueprint
import pandas as pd, logging

bp = Blueprint()


@bp.blob_trigger(
    arg_name="blob", 
    path="general/audiences/{audience_id}/addresses.csv", # Path including container name
    connection="TEST_CONN_STR", # Environmental variable key of an Azure Storage Connection String
)
def example_blob(blob: InputStream):
    for chunk in pd.read_csv(blob, chunksize=100):
        logging.warning(chunk)
    
