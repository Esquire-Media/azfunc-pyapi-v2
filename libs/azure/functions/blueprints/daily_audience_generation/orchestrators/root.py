# File: libs/azure/functions/blueprints/daily_audience_generation/activities/root.py

from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext, RetryOptions
import os, json, logging
from azure.storage.blob import ContainerClient

bp: Blueprint = Blueprint()

# audience types
    # use geoframes
        # 'Competitor Location', 'InMarket Shoppers'
        # manual
            # 'Friends Family'
    # use address Lists
        # 'Digital Neighbors', get from sales data.
        # 'New Movers', 
        # manual
            # 'Past Customers', Targeted Lists (estated data)
    # directly from OnSpot
        # Custom Demographic
            # doesn't use geoframes but also doesn't not provide addresses
    # others I have found
        # 'OOH Preview', 'Custom Neighbor', 
        # 'Neighbors of Movers'
            # like neighborzs with a different file type

@bp.orchestration_trigger(context_name="context")
def orchestrator_daily_audience_generation(context: DurableOrchestrationContext):
    # set connection string and the container
    conn_str = "ONSPOT_CONN_STR" if "ONSPOT_CONN_STR" in os.environ.keys() else None
    # esqdevdurablefunctions not the esqdevonspot
    container = "general"
    blob_prefix = "raw"

    retry = RetryOptions(15000, 3)
    egress = {"instance_id": context.instance_id, "blob_prefix": blob_prefix}

    # get the locations.csv file
    yield context.call_activity(
        "daily_audience_activity_locations",
        {
            "instance_id": context.instance_id,
            "conn_str": conn_str,
            "container": container,
            "outputPath": f"{blob_prefix}/{context.instance_id}/locations.csv",
        },
    )

    # file saves a CSV with example data below
    # {"location_id": "b6cec934-4151-48fe-97e2-0000041834a1", "esq_id": "EF~06133"}

    # load the audiences {"audience_id":"aud_id","start_date":"date","end_date":"date","geo":["geo_1","geo_2"]}
    blob_info = yield context.call_activity_with_retry(
        name="activity_load_salesforce", retry_options=retry, input_={**egress}
    )

    # call activities to format request and get the geojson data
    # yield context.task_all(
    #     [
    #         context.call_activity_with_retry(
    #             "activity_load_salesforce_geojsons",
    #             retry_options=retry,
    #             input_={**egress, **blob},
    #         )
    #         for blob in blob_info
    #         if blob["Audience_Type__c"] in ["Competitor Location","InMarket Shoppers"]
    #     ]
    # )

    # call activities to format request for address list
    yield context.task_all(
        [
            context.call_activity_with_retry(
                "activity_format_address_lists",
                retry_options=retry,
                input_={**egress, **blob},
            )
            for blob in blob_info
            if blob["Audience_Type__c"] in ["Digital Neighbors","New Movers"]
        ]
    )
          
    # pass New Movers and Digital Neighbors to OnSpot Orchestrator
    container_client = ContainerClient.from_connection_string(
        conn_str=os.environ["ONSPOT_CONN_STR"],
        container_name="general",
    )
    onspot_results = yield context.task_all(
        [
            context.call_sub_orchestrator_with_retry(
                "onspot_orchestrator",
                retry,
                {
                    "conn_str": conn_str,
                    "container": container,
                    "outputPath": "{}/{}/{}".format(
                        blob_prefix, context.instance_id, blob["Id"]
                    ),
                    "endpoint": "/save/addresses/all/devices",
                    "request": str(blob_client.download_blob().readall().decode("utf-8")),
                },
                subinstance_id,
            )
            for blob in blob_info
            if blob["Audience_Type__c"] in ["Digital Neighbors","New Movers"]
            if (subinstance_id := "{}:{}".format(context.instance_id, blob["Id"]))
            if (
                blob_client := container_client.get_blob_client(
                    f"{blob_prefix}/{context.instance_id}/audiencefiles/{blob['Id']}.csv"
                )
            ).exists()
        ]
    )
    
    logging.warning('Done')
    
    return {}
    
    # pass InMarket Shoppers and Competitor Location to OnSpot Orchestrator
    container_client = ContainerClient.from_connection_string(
        conn_str=os.environ["ONSPOT_CONN_STR"],
        container_name="general",
    )
    onspot_results = yield context.task_all(
        [
            context.call_sub_orchestrator_with_retry(
                "onspot_orchestrator",
                retry,
                {
                    "conn_str": conn_str,
                    "container": container,
                    "outputPath": "{}/{}/{}".format(
                        blob_prefix, context.instance_id, blob["Id"]
                    ),
                    "endpoint": "/save/geoframe/all/devices",
                    "request": json.load(blob_client.download_blob()),
                },
                subinstance_id,
            )
            for blob in blob_info
            if blob["Audience_Type__c"] in ["Competitor Location","InMarket Shoppers"]
            if (subinstance_id := "{}:{}".format(context.instance_id, blob["Id"]))
            if (
                blob_client := container_client.get_blob_client(
                    f"{blob_prefix}/{context.instance_id}/{blob['Id']}.geojson"
                )
            ).exists()
        ]
    )
    
    # use CETAS to generate parquet files for InMarket Shoppers and Competitor Location
    yield context.call_activity_with_retry(
        "synapse_activity_cetas",
        retry,
        {
            "instance_id": context.instance_id,
            "bind": "audiences",
            "table": {"name": "maids"},
            "destination": {
                "container": container,
                "handle": "sa_esqdevdurablefunctions",
                "path": f"tables/{context.instance_id}/maids",
            },
            "query": """
                SELECT DISTINCT 
                    [data].filepath(1) AS [audience], 
                    [deviceid]
                FROM OPENROWSET(
                    BULK '{}/{}/{}/*/*.csv',
                    DATA_SOURCE = 'sa_esqdevdurablefunctions',
                    FORMAT = 'CSV',
                    PARSER_VERSION = '2.0',
                    FIRST_ROW = 2
                ) WITH (
                    [deviceid] VARCHAR(64)
                ) AS [data]
            """.format(container, blob_prefix, context.instance_id),
            "view": True,
        },
    )
    
    # clean up
    # yield context.call_activity(
    #     "datalake_activity_delete_directory",
    #     {
    #         "instance_id": context.instance_id,
    #         "conn_str": conn_str,
    #         "container": container,
    #         "prefix": "raw"
    #     },
    # )

    return onspot_results
