# File: libs/azure/functions/blueprints/esquire/dashboard/onspot/orchestrator.py

from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from azure.durable_functions import Blueprint
import os

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def esquire_dashboard_oneview_orchestrator(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()
    retry = RetryOptions(60000, 12)
    # Get the current time to use as a timestamp for data processing
    pull_time = context.current_utc_datetime.isoformat()
    conn_str = (
        "ONEVIEW_CONN_STR"
        if "ONEVIEW_CONN_STR" in os.environ.keys()
        else "AzureWebJobsStorage"
    )

    try:
        # Run and monitor the report generation
        download_url = yield context.call_sub_orchestrator_with_retry(
            "oneview_reports_orchestrator",
            retry,
            ingress,
        )
        # Download the report to a blob
        yield context.call_activity_with_retry(
            "azure_datalake_copy_blob",
            retry,
            {
                "source": download_url,
                "target": {
                    "conn_str": conn_str,
                    "container_name": "general",
                    "blob_name": f"oneview/deltas/insights/{pull_time}.csv",
                },
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
