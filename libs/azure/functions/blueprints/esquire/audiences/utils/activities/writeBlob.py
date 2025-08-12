# activities/writeBlob.py
from azure.durable_functions import Blueprint
import pandas as pd
from libs.utils.azure_storage import export_dataframe  # adjust import to where azure_storage.py lives
import httpx, logging

bp = Blueprint()

@bp.activity_trigger(input_name="payload")
def activity_write_blob(payload: dict) -> str:
    """
    payload = {
        "records": list[dict],            # rows to write
        "container": "development-largemessages",
        "blob_prefix": "a2de...:2/raw/1", # where to put it (prefix only)
        # optional:
        # "conn_str": "AzureWebJobsStorage",  # env var name holding the connection string
        # "preflight": True,
    }
    """
    df = pd.DataFrame(payload.get("records", []))
    conn_str_key = payload.get("conn_str", "AzureWebJobsStorage")

    sas_url = export_dataframe(
        df,
        {
            "conn_str": conn_str_key,
            "container_name": payload["container"],
            "blob_prefix": payload["blob_prefix"].strip("/"),
            "format": "csv",
        },
    )  # returns https URL w/ ?sp=r SAS

    if payload.get("preflight"):
        try:
            r = httpx.head(sas_url, timeout=10)
            if r.status_code not in (200, 206):
                logging.warning("HEAD %s -> %s", sas_url, r.status_code)
        except Exception as e:
            logging.warning("HEAD preflight failed for %s: %r", sas_url, e)

    return sas_url
