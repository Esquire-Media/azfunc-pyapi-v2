import os
from libs.azure.functions import Blueprint
import numpy as np
import pandas as pd
from azure.storage.blob import BlobClient
from libs.utils.azure_storage import get_blob_sas, export_dataframe
from datetime import timedelta
from azure.storage.blob import BlobClient
import logging
import httpx

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()

# Define an activity function
@bp.activity_trigger(input_name="ingress")
def activity_httpx(ingress: dict):
    """
    Params:
        -method (str)
        -url (str)
        -data (str)
        -headers (dict)

    Ex.    
    {
        "method":"POST",
        "url":"https://google.com",
        "data":"myData,
        "headers":{
            "Content-Type":"application/json"
        }
    }
    """

    response = httpx.request(
        **ingress,
        timeout=None
    )

    return {
        "headers":dict(response.headers),
        "body":str(response.content),
        "status_code":response.status_code,
    }