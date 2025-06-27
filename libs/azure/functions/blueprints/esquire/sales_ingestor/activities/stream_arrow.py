import logging
from azure.durable_functions import Blueprint, activity_trigger
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.arrow_ingest import stream_blob_to_pg

bp = Blueprint()

@bp.activity_trigger(input_name="settings")
def bulk_load_arrow(settings: dict):
    rows = stream_blob_to_pg(settings["blob_url"], settings["table"], mode="append")
    logging.info("Ingested %d rows into %s", rows, settings["table"])
