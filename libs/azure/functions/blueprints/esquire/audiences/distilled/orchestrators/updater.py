# File: libs/azure/functions/blueprints/esquire/audiences/distilled/orchestrators/updater.py

from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def esquire_audiences_distilled_orchestrator_updater(
    context: DurableOrchestrationContext,
):
    # retry = RetryOptions(15000, 3)
    settings = context.get_input()

    yield context.task_all(
        [
            context.call_sub_orchestrator(
                "aws_athena_orchestrator",
                {
                    **settings["source"],
                    "query": v,
                    "destination": {
                        **settings["destination"],
                        "blob_name": f"distilled/{k}.csv",
                    },
                },
            )
            for k, v in {
                "b2b": 'SELECT * FROM "pixel"."b2b"',
                "b2c": 'SELECT * FROM "pixel"."b2c"',
                "pte": 'SELECT * FROM "pixel"."pte"',
                "hem": 'SELECT * FROM "pixel"."pixel_data_esquireadvertising"',
            }.items()
        ]
    )

    yield context.call_sub_orchestrator(
        "purge_instance_history",
        {"instance_id": context.instance_id},
    )
