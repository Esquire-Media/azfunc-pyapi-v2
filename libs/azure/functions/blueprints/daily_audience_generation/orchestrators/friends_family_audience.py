# File: libs/azure/functions/blueprints/daily_audience_generation/orchestrators/friends_family_audience.py

import logging
import json
import pandas as pd
import numpy as np
import os
from fuzzywuzzy import fuzz

import azure.functions as func
import azure.durable_functions as dfunc

from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from azure.storage.blob import (
    ContainerClient,
    ContainerSasPermissions,
    generate_container_sas,
    BlobClient,
)

import logging

bp: Blueprint = Blueprint()


# main orchestrator for geoframed audiences (suborchestrator for the root)
@bp.orchestration_trigger(context_name="context")
def suborchestrator_friends_family(context: DurableOrchestrationContext):
    ingress = context.get_input()
    retry = RetryOptions(15000, 1)

    # logging.warning(f"Friends Family Ingress: {ingress}")
    # ingress: {
    #     "conn_str": "ONSPOT_CONN_STR",
    #     "container": "general",
    #     "blob_prefix": "raw",
    #     "path": "raw/cb984a09c4884215818c277aaee67c11/audiences",
    #     "audiences": [
    #         {
    #             "Id": "a0H6e00000bNazEEAS",
    #             "Audience_Name__c": "FF_Test",
    #             "Audience_Type__c": "Friends Family",
    #             "Lookback_Window__c": None,
    #             "Name": "EF~00499",
    #         }
    #     ],
    #     "instance_id": "cb984a09c4884215818c277aaee67c11",
    # }
    # just passing in one audience for testing, but building for multiples
    # test file used:
    # raw/7e22a03f12104c37905d58a5ae894682/audiences/a0H6e00000bNazEEAS_test/a0H6e00000bNazEEAS_test.csv

    # pass audiences into address validation
    ## returns list of objects with key= audience_id and value= cleaned addresses
    validated_audiences = yield context.task_all(
        [
            # pass address list into vadliation activity
            context.call_activity_with_retry(
                "activity_address_validation",
                retry_options=retry,
                input_={
                    "path": ingress["path"],
                    "audience": audience["Id"],
                    "instance_id": ingress["instance_id"],
                    "context": None,
                },
            )
            for audience in ingress["audiences"]
        ]
    )
    # logging.warning(validated_audiences)
    
    # pass suborchestrator for converting addressses into polygons
    # #geoframe -> polygon with address in properties
    # pass this list into the reach_cache activity
    yield context.task_all(
        [
            context.call_activity_with_retry(
                "activity_read_cache",
                retry_options=retry,
                input_={
                    "path": ingress["path"],
                    "instance_id": ingress["instance_id"],
                    "context": None,
                    "audience": f"{audience['audience_id']}test",
                    **audience
                },
            )
            for audience in validated_audiences
        ]
    )

    return {}


# only Friends and Family audience I see is ID: a0H6e00000bNazEEAS (it is a test audience?)