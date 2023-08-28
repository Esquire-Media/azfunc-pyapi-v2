# File: libs/azure/functions/blueprints/esquire/dashboard/xandr/orchestrators/reporting.py

from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from datetime import datetime, timedelta
from libs.azure.functions import Blueprint
from libs.azure.functions.blueprints.esquire.dashboard.xandr.config import CETAS
from libs.openapi.clients.xandr import XandrAPI
import json, os

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def esquire_dashboard_xandr_orchestrator_reporting(
    context: DurableOrchestrationContext,
):
    pull_time = datetime.utcnow().isoformat()
    retry = RetryOptions(15000, 3)
    conn_str = "XANDR_CONN_STR" if "XANDR_CONN_STR" in os.environ.keys() else None
    container = "general"
    
    while True:
        state = yield context.call_activity_with_retry(
            "esquire_dashboard_xandr_activity_status",
            retry,
            {"instance_id": context.instance_id},
        )
        match state["status"]:
            case "ready":
                break
            case "error":
                raise Exception(state["error"])
        yield context.create_timer(datetime.utcnow() + timedelta(minutes=5))

    yield context.call_activity_with_retry(
        "esquire_dashboard_xandr_activity_download",
        retry,
        {
            "instance_id": context.instance_id,
            "conn_str": conn_str,
            "container": container,
            "outputPath": "xandr/deltas/{}/{}.parquet".format(
                state["report_type"],
                pull_time,
            ),
        },
    )

    yield context.call_activity_with_retry(
        "synapse_activity_cetas",
        retry,
        {
            "instance_id": context.instance_id,
            "bind": "xandr_dashboard",
            "table": {"schema": "dashboard", "name": state["report_type"]},
            "destination": {
                "conn_str": conn_str,
                "container": container,
                "handle": "sa_esquiregeneral",
                "path": f"xandr/tables/{state['report_type']}/{pull_time}",
            },
            "query": CETAS[state["report_type"]],
            "view": True,
        },
    )

    # yield context.call_activity_with_retry(
    #     "datalake_activity_delete_directory",
    #     retry,
    #     {
    #         "instance_id": context.instance_id,
    #         "conn_str": conn_str,
    #         "container": container,
    #         "prefix": "raw"
    #     },
    # )
