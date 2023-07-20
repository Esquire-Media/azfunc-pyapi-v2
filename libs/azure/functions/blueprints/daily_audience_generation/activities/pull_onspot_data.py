from libs.azure.functions import Blueprint
import pandas as pd
import os
import json
from io import BytesIO
from azure.durable_functions import (
    DurableOrchestrationClient,
    DurableOrchestrationContext,
    EntityId,
    RetryOptions
)
from azure.storage.blob import BlobServiceClient
from libs.azure.functions.http import HttpRequest, HttpResponse
from sqlalchemy.orm import Session
from libs.data import from_bind
import geojson
import uuid

bp:Blueprint = Blueprint()

# activity to retrieve Onspot data for each audience's competitor location(s)
@bp.activity_trigger(input_name='ingress')
def activity_pull_onspot_data(ingress:dict):
    blob_name = ingress['blob_name']
    audience_id = blob_name.split('/')[-2]
    