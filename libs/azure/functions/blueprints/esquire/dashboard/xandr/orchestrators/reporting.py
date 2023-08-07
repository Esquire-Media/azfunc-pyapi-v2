# File: libs/azure/functions/blueprints/esquire/dashboard/xandr/orchestrators/reporting.py

from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from datetime import datetime, timedelta
from libs.azure.functions import Blueprint
from libs.openapi.clients.xandr import XandrAPI
import os

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def esquire_dashboard_xandr_orchestrator_reporting(context: DurableOrchestrationContext):
    retry = RetryOptions(15000, 3)
    conn_str = "XANDR_CONN_STR" if "XANDR_CONN_STR" in os.environ.keys() else None
    container = "dashboard"
    
    XA = XandrAPI(asynchronus=False)
    report_status_factory = XA.createRequest(("report", "get"))
    while True:
        _, data, _ = report_status_factory(parameters={"id": context.instance_id})
        if data.response.execution_status == "ready":
            break
        yield context.create_timer(datetime.utcnow() + timedelta(minutes=1))
        
    yield context.call_activity_with_retry(
        "esquire_dashboard_xandr_activity_download",
        retry,
        {
            "instance_id": context.instance_id,
            "conn_str": conn_str,
            "container": container,
            "outputPath": f"raw/{context.instance_id}/",
        },
    )

    yield context.call_activity_with_retry(
        "synapse_activity_cetas",
        retry,
        {
            "instance_id": context.instance_id,
            "bind": "onspot",
            "table": {"name": "sisense"},
            "destination": {
                "conn_str": conn_str,
                "container": container,
                "handle": "sa_esquireonspot",
                "path": f"tables/{context.instance_id}/sisense",
            },
            "query": cetas_query_sisense(context.instance_id),
            "commit": True,
            "view": True,
        },
    )

    yield context.call_activity_with_retry(
        "datalake_activity_delete_directory",
        retry,
        {
            "instance_id": context.instance_id,
            "conn_str": conn_str,
            "container": container,
            "prefix": "raw"
        },
    )
