from libs.azure.functions.blueprints.esquire.audiences.builder.config import (
    MAPPING_DATASOURCE,
)
from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext, RetryOptions
import logging

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiences_builder(
    context: DurableOrchestrationContext,
):
    # The flow
    # Get a list of the audiences > get all the information we need and push it to a CETAS (use synapse to pull data based on the filter given)
    # 
    ingress = context.get_input()
    # ingress format/data
    # {
    #     "source": {
    #         "conn_str": "ONSPOT_CONN_STR",
    #         "container_name": "general",
    #         "blob_prefix": "audiences",
    #     },
    #     "working": {
    #         "conn_str": "ONSPOT_CONN_STR",
    #         "container_name": "general",
    #         "blob_prefix": "raw",
    #     },
    #     "destination": {
    #         "conn_str": "ONSPOT_CONN_STR",
    #         "container_name": "general",
    #         "blob_prefix": "audiences",
    #     },
    #     "fetch": True,
    # }

    # get audiences
    audiences = yield context.call_activity(
        "activity_esquireAudienceBuilder_fetchAudienceDatasource"
    )

    logging.warning(audiences)
    test = get_queries(audiences)           
    logging.warning(test)
    # use datasource and data filter to process the data
    # use synapse to pull data based on the filter given
    # yield context.task_all(
    #     [
    #         context.call_activity(
    #             "synapse_activity_cetas",
    #             {
    #                 "instance_id": context.instance_id,
    #                 **MAPPING_DATASOURCE[audience[1]],
    #                 "destination": {
    #                     "conn_str": ingress["working"]["conn_str"],
    #                     "container_name": ingress["working"]["container_name"],
    #                     "blob_prefix": "{}/{}/{}/source".format(
    #                         ingress["working"]["blob_prefix"],
    #                         context.instance_id,
    #                         audience[0],
    #                     ),
    #                     "handle": "sa_esqdevdurablefunctions",  # will need to change at some point
    #                     "format": "CSV",
    #                 },
    #                 "query": """
    #                     SELECT * FROM {}{}
    #                     WHERE {}
    #                 """.format(
    #                     (
    #                         "[{}].".format(
    #                             MAPPING_DATASOURCE[audience[1]]["table"]["schema"]
    #                         )
    #                         if MAPPING_DATASOURCE[audience[1]]["table"].get(
    #                             "schema", None
    #                         )
    #                         else ""
    #                     ),
    #                     "[" + MAPPING_DATASOURCE[audience[1]]["table"]["name"] + "]",
    #                     audience[2],
    #                 ),
    #             },
    #         )
    #         for audience in audiences
    #         if audience[1] == "clua3ct6p0018k0ecevpq5gzh"  # for testing purposes
    #     ]
    # )

    # # get the processing step
    # for audience in audiences:
    #     process = yield context.call_activity(
    #         "activity_esquireAudienceBuilder_fetchAudienceProcesses",
    #         audience
    #     )
    # #     audience.append(process)

    # logging.error(audiences)

    # yield context.task_all(
    #     [
    #         context.call_sub_orchestrator(
    #             'orchestrator_esquireAudiences_transformer',
    #             {
    #                 'instance_id': context.instance_id,
    #                 'audience_id': audience[0],
    #                 'storage': ingress
    #             }
    #         )
    #         for audience in audiences
    #     ]
    # )
    logging.warning("Completed")
    return

# function to get list of queries
def get_queries(audiences):
    queries = []
    for audience in audiences:
        # logging.error(f'Audience Information: {audience[1]}')
        if audience[1] == "clujtgf3m0018nh4dri1bl9aw":  # for testing purposes - deepsync
            query = """
                    SELECT * FROM {}{}
                    WHERE {}
                """.format(
                (
                    "[{}].".format(MAPPING_DATASOURCE[audience[1]]["table"]["schema"])
                    if MAPPING_DATASOURCE[audience[1]]["table"].get("schema", None)
                    else ""
                ),
                "[" + MAPPING_DATASOURCE[audience[1]]["table"]["name"] + "]",
                audience[3],
            )
            # Split the query up to get rid of "\n      "
            # Step 1: Trim whitespace
            trimmed_string = query.strip()

            # Step 2: Replace newline characters and excessive spaces
            # Using splitlines() to handle newlines and join with a space to remove excessive spaces
            clean_string = ' '.join(trimmed_string.splitlines())

            # Step 3: Further clean to ensure only single spaces between words
            final_string = ' '.join(clean_string.split())
            
            queries.append(final_string)
    
    # logging.warning(queries)
    return queries