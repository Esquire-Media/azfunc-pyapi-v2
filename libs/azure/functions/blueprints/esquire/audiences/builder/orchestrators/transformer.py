from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext, RetryOptions
import logging

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiences_transformer(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()

    # from ingress, need to know:
    # where source data is -> storage|working
    #
    logging.warning(
        "{}/{}/{}/{}/source".format(
            ingress["storage"]["working"]["container_name"],
            ingress["storage"]["working"]["blob_prefix"],
            ingress["instance_id"],
            ingress["audience_id"],
        )
    )
# from audience ID -> determine datasource datatype
    # do in an activity, not in orch
# need to know dtatype of the source data, and the different processes in their sort order
    # query DB based on audience ID
# then you can go from that data type to another one based on the processes on the audience
    # need an activity that will transform data from one datatype to another, 
    