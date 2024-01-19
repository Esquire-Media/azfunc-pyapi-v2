# File: libs/azure/functions/blueprints/esquire/dashboard/onspot/orchestrator.py

from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from libs.azure.functions import Blueprint
import os

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def esquire_dashboard_oneview_orchestrator(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()
    retry = RetryOptions(60000, 12)
    conn_str = (
        "ONEVIEW_CONN_STR"
        if "ONEVIEW_CONN_STR" in os.environ.keys()
        else "AzureWebJobsStorage"
    )

    persistent_container = {
        "conn_str": conn_str,
        "container_name": "email-report-ingress",
        "prefix": "insights/",
    }

    try:
        download_url = yield context.call_sub_orchestrator_with_retry(
            "orchestrator_oneviewReports",
            retry,
            ingress,
        )
        # Partition the report into parquet files by date
        yield context.call_activity_with_retry(
            "esquire_dashboard_oneview_activity_partition",
            retry,
            {
                "source": download_url,
                "target": persistent_container,
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
                "summary": "OneView Report Injestion Failed",
                "sections": [
                    {
                        "activityTitle": "OneView Report Injestion Failed",
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
