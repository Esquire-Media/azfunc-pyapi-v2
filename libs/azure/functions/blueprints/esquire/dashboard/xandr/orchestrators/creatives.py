# File: libs/azure/functions/blueprints/esquire/dashboard/xandr/orchestrators/creatives.py

from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from datetime import datetime, timedelta
from libs.azure.functions import Blueprint
from libs.azure.functions.blueprints.esquire.dashboard.xandr.config import CETAS
import logging, os

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def esquire_dashboard_xandr_orchestrator_creatives(
    context: DurableOrchestrationContext,
):
    pull_time = context.current_utc_datetime.isoformat()
    retry = RetryOptions(15000, 3)
    conn_str = "XANDR_CONN_STR" if "XANDR_CONN_STR" in os.environ.keys() else None
    container_name = "general"

    try:
        lastmodified = yield context.call_activity_with_retry(
            "esquire_dashboard_xandr_activity_creatives", retry
        )
        num_elements = 100
        start_element = 0
        tries = 0
        while True:
            if not context.is_replaying:
                logging.warning((start_element, lastmodified))
            response = yield context.call_activity_with_retry(
                "esquire_dashboard_xandr_activity_creatives",
                retry,
                {
                    "num_elements": num_elements,
                    "start_element": start_element,
                    "lastmodified": lastmodified,
                    "destination": {
                        "conn_str": conn_str,
                        "container_name": container_name,
                        "blob_prefix": f"xandr/deltas/creatives/{pull_time}",
                    },
                },
            )
            match response.get("status"):
                case "OK":
                    tries = 0
                    start_element += response["num_elements"]
                    if start_element >= response["count"]:
                        break
                case _:
                    tries += 1
                    if tries > 3:
                        raise Exception("Tried too many times.")
                    yield context.create_timer(datetime.utcnow() + timedelta(minutes=1))

        yield context.call_activity_with_retry(
            "synapse_activity_cetas",
            retry,
            {
                "instance_id": context.instance_id,
                "bind": "xandr_dashboard",
                "table": {"schema": "dashboard", "name": "creatives"},
                "destination": {
                    "conn_str": conn_str,
                    "container_name": container_name,
                    "handle": "sa_esquiregeneral",
                    "path": f"xandr/tables/creatives/{pull_time}",
                },
                "query": CETAS["creatives"],
                "commit": True,
                "view": True,
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
