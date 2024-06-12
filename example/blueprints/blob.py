from azure.durable_functions import Blueprint
from azure.functions import InputStream
import pandas as pd, logging, pyarrow.parquet as pq

bp = Blueprint()


@bp.blob_trigger(
    arg_name="blob",
    path="general/audiences/{audience_id}/addresses.csv",  # Path including container name
    connection="TEST_CONN_STR",  # Environmental variable key of an Azure Storage Connection String
)
def example_blob(blob: InputStream):
    pq.ParquetFile()
    for chunk in pd.read_csv(blob, chunksize=100):
        logging.warning(chunk)
