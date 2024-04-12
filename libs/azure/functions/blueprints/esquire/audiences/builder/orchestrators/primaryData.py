# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/primaryData.py

from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint
from libs.azure.functions.blueprints.esquire.audiences.builder.config import (
    MAPPING_DATASOURCE,
)
from libs.azure.functions.blueprints.esquire.audiences.builder.utils import (
    CETAS_Primary,
)

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiences_primaryData(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()

    if ingress["audience"].get("dataSource"):
        # Generate a primary data set
        match MAPPING_DATASOURCE[ingress["audience"]["dataSource"]["id"]]["dbType"]:
            case "synapse": 
                ingress["results"] = yield context.call_activity(
                    "synapse_activity_cetas",
                    CETAS_Primary(instance_id=context.instance_id, ingress=ingress),
                )
            case "postgres":
                # Activity that queries the DB and streams the results to a blob(s).
                pass
    
    return ingress