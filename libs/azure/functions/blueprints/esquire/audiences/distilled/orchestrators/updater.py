# File: libs/azure/functions/blueprints/esquire/audiences/distilled/orchestrators/updater.py

from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from libs.azure.functions import Blueprint
import logging

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def esquire_audiences_distilled_orchestrator_updater(
    context: DurableOrchestrationContext,
):
    # retry = RetryOptions(15000, 3)
    settings = context.get_input()
    logging.warning(settings)

    b2b_url = yield context.call_sub_orchestrator(
        "aws_athena_orchestrator",
        {
            **settings,
            "query": 'SELECT * FROM "pixel"."b2b"',
        },
    )
    b2c_url = yield context.call_sub_orchestrator(
        "aws_athena_orchestrator",
        {
            **settings,
            "query": 'SELECT * FROM "pixel"."b2c"',
        },
    )
    pte_url = yield context.call_sub_orchestrator(
        "aws_athena_orchestrator",
        {
            **settings,
            "query": 'SELECT * FROM "pixel"."pte"',
        },
    )
    pixel_url = yield context.call_sub_orchestrator(
        "aws_athena_orchestrator",
        {
            **settings,
            "query": 'SELECT * FROM "pixel"."pixel_data_esquireadvertising"',
        },
    )

    return {"b2b": b2b_url, "b2c": b2c_url, "pte": pte_url, "all": pixel_url}
