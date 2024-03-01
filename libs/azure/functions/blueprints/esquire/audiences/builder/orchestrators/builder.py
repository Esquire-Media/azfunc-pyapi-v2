from libs.azure.functions.blueprints.esquire.audiences.builder.config import MAPPING_DATASOURCE
from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext, RetryOptions
import logging

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiences_builder(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()

    # get audiences
    audiences = yield context.call_activity(
        "activity_esquireAudienceBuilder_fetchAudienceDatasource"
    )
    logging.warning(audiences)
    
    # get processes: do we want to loop here and not query DB for all the audiences again? Just pass in Aud ID and dataSource ID?
    datasources = yield context.call_activity(
        "activity_esquireAudienceBuilder_fetchAudienceProcesses"
    )
    logging.error(datasources)
    
    
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
    #                         "[{}].".format(MAPPING_DATASOURCE[audience[1]]["table"]["schema"])
    #                         if MAPPING_DATASOURCE[audience[1]]["table"].get("schema", None) 
    #                         else ""
    #                     ),
    #                     "[" + MAPPING_DATASOURCE[audience[1]]["table"]["name"] + "]",
    #                     audience[2],
    #                 ),
    #             },
    #         )
    #         for audience in audiences
    #         if audience[1] == "clt318gpe0007t86c0psgcl3x" #for testing purposes
    #     ]
    # )
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
    return 
