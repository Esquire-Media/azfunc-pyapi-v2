# File: libs/azure/functions/blueprints/esquire/dashboard/xandr/orchestrators/reporting.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions
from datetime import timedelta
from libs.azure.functions.blueprints.esquire.dashboard.xandr.config import CETAS
import os

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def esquire_dashboard_xandr_orchestrator_reporting(
    context: DurableOrchestrationContext,
):
    pull_time = context.current_utc_datetime.isoformat()
    retry = RetryOptions(15000, 3)
    conn_str = "XANDR_CONN_STR" if "XANDR_CONN_STR" in os.environ.keys() else None
    container = "general"

    try:
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
            yield context.create_timer(context.current_utc_datetime + timedelta(minutes=5))

        yield context.call_activity_with_retry(
            "esquire_dashboard_xandr_activity_download",
            retry,
            {
                "instance_id": context.instance_id,
                "conn_str": conn_str,
                "container_name": container,
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
                    "container_name": container,
                    "handle": "sa_esquiregeneral",
                    "path": f"xandr/tables/{state['report_type']}/{pull_time}",
                },
                "query": CETAS[state["report_type"]],
                "view": True,
                "commit": True,
            },
        )
    except Exception as e:
        yield context.call_http(
            method="POST",
            uri=os.environ["EXCEPTIONS_WEBHOOK_DEVOPS"],
            content={
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "themeColor": "EE2A3D",
                "summary": "Xandr Report Injestion Failed",
                "sections": [
                    {
                        "activityTitle": "Xandr Report Injestion Failed",
                        "activitySubtitle": "{}{}".format(
                            str(e)[0:128], "..." if len(str(e)) > 128 else ""
                        ),
                        "facts": [
                            {"name": "InstanceID", "value": context.instance_id},
                        ],
                        "markdown": True,
                    }
                ],
            },
        )
        raise e

    # Purge history related to this instance
    yield context.call_sub_orchestrator(
        "purge_instance_history",
        {"instance_id": context.instance_id},
    )
