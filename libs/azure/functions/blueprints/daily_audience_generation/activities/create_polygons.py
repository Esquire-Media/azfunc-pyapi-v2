# File: libs/azure/functions/blueprints/daily_audience_generation/activities/create_polygons.py

import os
from libs.azure.functions import Blueprint
import logging

bp: Blueprint = Blueprint()

chunk_size = int(os.environ["chunk_size"])
max_sql_parameters = 1000
# maximum number of parameters that MS SQL can parse is ~2100


# activity to validate the addresses
@bp.activity_trigger(input_name="ingress")
def activity_create_polygons(ingress: dict):
    # log the ingress
    logging.warning(ingress)
    return {}