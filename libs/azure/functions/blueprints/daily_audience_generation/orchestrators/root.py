# File: libs/azure/functions/blueprints/daily_audience_generation/activities/root.py

from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext, RetryOptions
import os, logging

bp: Blueprint = Blueprint()


# main orchestrator
@bp.orchestration_trigger(context_name="context")
def orchestrator_daily_audience_generation(context: DurableOrchestrationContext):
    # set connection string and the container
    conn_str = "ONSPOT_CONN_STR" if "ONSPOT_CONN_STR" in os.environ.keys() else None
    container = "general"
    blob_prefix = "raw"
    retry = RetryOptions(15000, 1)
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
    audiences = yield context.call_activity_with_retry(
        name="activity_load_salesforce", retry_options=retry, input_={**egress}
    )

    # FIRST SEPARATE THE AUDIENCES INTO LISTS OF HOW THE DEVICE IDS ARE GENERATED
    yield context.task_all(
        [
            # setup items for the suborchestrators for the geoframed audiences
            context.call_sub_orchestrator_with_retry(
                "suborchestrator_geoframed_audiences",
                retry,
                {
                    "conn_str": conn_str,
                    "container": container,
                    "blob_prefix": blob_prefix,
                    "path": f"{blob_prefix}/{context.instance_id}/audiences",
                    "audiences": [
                        audience
                        for audience in audiences
                        if audience["Audience_Type__c"]
                        in ["Competitor Location", "InMarket Shoppers"]
                    ][:10],
                    "instance_id": context.instance_id,
                },
            ),
            # setup items for the suborchestrators for the addressed audiences
            context.call_sub_orchestrator_with_retry(
                "suborchestrator_addressed_audiences",
                retry,
                {
                    "conn_str": conn_str,
                    "container": container,
                    "blob_prefix": blob_prefix,
                    "path": f"{blob_prefix}/{context.instance_id}/audiences",
                    "instance_id": context.instance_id,
                    "audiences": [
                        audience
                        for audience in audiences
                        if audience["Audience_Type__c"] in ["New Movers"]
                    ][:10],
                },
            ),
        ]
    )
    
    # use CETAS to generate parquet files for InMarket Shoppers and Competitor Location
    # yield context.call_activity_with_retry(
    #     "synapse_activity_cetas",
    #     retry,
    #     {
    #         "instance_id": context.instance_id,
    #         "bind": "audiences",
    #         "table": {"name": "maids"},
    #         "destination": {
    #             "container": container,
    #             "handle": "sa_esqdevdurablefunctions",
    #             "path": f"tables/{context.instance_id}/maids",
    #         },
    #         "query": """
    #             SELECT DISTINCT 
    #                 [data].filepath(1) AS [audience], 
    #                 [deviceid]
    #             FROM OPENROWSET(
    #                 BULK '{}/{}/{}/audiences/*/devices/*',
    #                 DATA_SOURCE = 'sa_esqdevdurablefunctions',
    #                 FORMAT = 'CSV',
    #                 PARSER_VERSION = '2.0',
    #                 FIRST_ROW = 2
    #             ) WITH (
    #                 [deviceid] VARCHAR(64)
    #             ) AS [data]
    #             WHERE [data].filepath(1) = [data].filepath(2)
    #         """.format(
    #             container, blob_prefix, context.instance_id
    #         ),
    #         "view": True,
    #     },
    # )
    logging.warning("COMPLETE")
    return {}