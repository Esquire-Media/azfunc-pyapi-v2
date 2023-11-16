# File: libs/azure/functions/blueprints/esquire/audiences/daily_audience_generation/orchestrators/root.py

from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext, RetryOptions
import os, logging

bp: Blueprint = Blueprint()


# main orchestrator
@bp.orchestration_trigger(context_name="context")
def orchestrator_dailyAudienceGeneration_root(context: DurableOrchestrationContext):
    # set connection string and the container
    conn_str = "ONSPOT_CONN_STR" if "ONSPOT_CONN_STR" in os.environ.keys() else "AzureWebJobsStorage"
    container_name = "general"
    blob_prefix = "raw"
    retry = RetryOptions(15000, 1)
    egress = {"instance_id": context.instance_id, "blob_prefix": blob_prefix}

    # get the locations.csv file
    yield context.call_activity(
        "activity_dailyAudienceGeneration_locations",
        {
            "instance_id": context.instance_id,
            "conn_str": conn_str,
            "container": container_name,
            "outputPath": f"{blob_prefix}/{context.instance_id}/locations.csv",
        },
    )
    # file saves a CSV with example data below
    # {"location_id": "b6cec934-4151-48fe-97e2-0000041834a1", "esq_id": "EF~06133"}

    # load the audiences {"audience_id":"aud_id","start_date":"date","end_date":"date","geo":["geo_1","geo_2"]}
    audiences = yield context.call_activity_with_retry(
        name="activity_dailyAudienceGeneration_loadSalesforce", retry_options=retry, input_={**egress}
    )

    # create testing information
    test_friends_family = {
        "Id": "a0H6e00000bNazEEAS_test",
        "Audience_Name__c": "FF_Test",
        "Audience_Type__c": "Friends Family",
        "Lookback_Window__c": None,
        "Name": "EF~00001",
    }
    # this will move the test file from general into the file path it would be in if the code was fully automated
    ## the intention of this is to simulate as much as possible the automated process, until the address
    ## files are actually automated like the other audiences.
    yield context.call_activity_with_retry(
        "activity_testing",
        retry_options=retry,
        input_={**egress},
    )

    # FIRST SEPARATE THE AUDIENCES INTO LISTS OF HOW THE DEVICE IDS ARE GENERATED
    yield context.task_all(
        [
            # testing for friends and family with sample file
            context.call_sub_orchestrator_with_retry(
                "orchestrator_audience_friendsFamily",
                retry,
                {
                    "conn_str": conn_str,
                    "container_name": container_name,
                    "blob_prefix": f"{blob_prefix}/{context.instance_id}/audiences",
                    "audience": audience,
                },
            )
            for audience in [test_friends_family]
        ]
        + [
            ## setupitems for the friends and family suborchestrator
            # context.call_sub_orchestrator_with_retry(
            #     "orchestrator_dailyAudienceGeneration_friendsFamily",
            #     retry,
            #     {
            #         "conn_str": conn_str,
            #         "container": container_name,
            #         "blob_prefix": blob_prefix,
            #         "path": f"{blob_prefix}/{context.instance_id}/audiences",
            #         "audiences": [
            #             audience
            #             for audience in audiences
            #             if audience["Audience_Type__c"]
            #             in ["Friends Family"]
            #         ],
            #         "instance_id": context.instance_id,
            #     },
            # ),
            ## setup items for the suborchestrators for the geoframed audiences
            # context.call_sub_orchestrator_with_retry(
            #     "orchestrator_dailyAudienceGeneration_geoframedAudiences",
            #     retry,
            #     {
            #         "conn_str": conn_str,
            #         "container": container_name,
            #         "blob_prefix": blob_prefix,
            #         "path": f"{blob_prefix}/{context.instance_id}/audiences",
            #         "audiences": [
            #             audience
            #             for audience in audiences
            #             if audience["Audience_Type__c"]
            #             in ["Competitor Location", "InMarket Shoppers"]
            #         ],
            #         "instance_id": context.instance_id,
            #     },
            # ),
            ## setup items for the suborchestrators for the addressed audiences
            # context.call_sub_orchestrator_with_retry(
            #     "orchestrator_dailyAudienceGeneration_addressedAudiences",
            #     retry,
            #     {
            #         "conn_str": conn_str,
            #         "container": container_name,
            #         "blob_prefix": blob_prefix,
            #         "path": f"{blob_prefix}/{context.instance_id}/audiences",
            #         "instance_id": context.instance_id,
            #         "audiences": [
            #             audience
            #             for audience in audiences
            #             if audience["Audience_Type__c"] in ["New Movers"] #["New Movers", "Digital Neighbors"]
            #         ],
            #     },
            # ),
        ]
    )

    # logging.warning(audiences)
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
