# File: libs/azure/functions/blueprints/esquire/dashboard/meta/orchestrators/reporting.py

from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from datetime import datetime, timedelta
from libs.azure.functions import Blueprint
from libs.azure.functions.blueprints.esquire.dashboard.meta.config import PARAMETERS
import orjson

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def esquire_dashboard_meta_orchestrator_reporting(
    context: DurableOrchestrationContext,
):
    """
    Orchestrates the process of retrieving, processing, and storing Facebook Ads data.

    This orchestrator manages the workflow for fetching Facebook Ads insights, Ads, Campaigns, and AdSets.
    It handles the requests asynchronously, monitors their completion status, and triggers subsequent
    processing and storage activities.

    Parameters
    ----------
    context : DurableOrchestrationContext
        The context object provided by Azure Durable Functions framework, used for invoking sub-orchestrators,
        activities, and managing state.

    Returns
    -------
    dict
        An empty dictionary upon successful completion of the orchestration.

    Raises
    ------
    FacebookReportError
        If there's an error in fetching the Facebook report.

    """

    # Set up retry options for sub-orchestrators and activities
    retry = RetryOptions(60000, 3)

    # Get input parameters for the orchestrator
    ingress = context.get_input()

    # Request a report for Facebook Ads Insights
    context.set_custom_status(
        "Sending report request for account {}".format(ingress["account_id"])
    )

    tries = 0
    while True:
        report_run = yield context.call_sub_orchestrator(
            "meta_orchestrator_request",
            {
                "operationId": "AdAccount.Post.Insights",
                "parameters": {
                    **PARAMETERS["AdAccount.Post.Insights"],
                    "AdAccount-Id": ingress["account_id"],
                },
            },
        )
        if not report_run.get("report_run_id", None):
            tries += 1
            context.set_custom_status(f"On try #{tries}")
            with open("logging.log", "ab") as f:
                f.write(orjson.dumps(report_run))
            if tries > 3:
                raise Exception("Insight report generation failed.")
            yield context.create_timer(datetime.utcnow() + timedelta(minutes=5))
            continue
        
        
        context.set_custom_status(
            "Polling status for report run {}".format(report_run["report_run_id"])
        )
        # Monitor the status of the report generation
        while True:
            status = yield context.call_sub_orchestrator(
                "meta_orchestrator_request",
                {
                    "operationId": "AdReportRun.Get",
                    "parameters": {"AdReportRun-Id": report_run["report_run_id"]},
                },
            )
            context.set_custom_status(
                "Report for account {} is {} percent complete.".format(
                    status["account_id"],
                    status["async_percent_completion"],
                )
            )
            # Handle the status of the report generation
            match status["async_status"]:
                case "Job Completed":
                    break
                case "Job Failed":
                    context.set_custom_status(status)
                    break
                case _:
                    yield context.create_timer(datetime.utcnow() + timedelta(minutes=1))

        if status["async_status"] == "Job Completed":
            break
        else:
            tries += 1
            context.set_custom_status(f"On try #{tries}")
            if tries > 3:
                raise Exception("Insight report generation failed.")
            yield context.create_timer(datetime.utcnow() + timedelta(minutes=1))

    # Download the generated report
    context.set_custom_status(
        "Downloading report {} for account {}".format(
            report_run["report_run_id"],
            ingress["account_id"],
        )
    )
    downloader = yield context.call_activity(
        "esquire_dashboard_meta_activity_download",
        {
            "report_run_id": report_run["report_run_id"],
            "conn_str": ingress["conn_str"],
            "container_name": ingress["container_name"],
            "blob_name": "meta/delta/adsinsights/{}/{}.parquet".format(
                ingress["pull_time"],
                status["account_id"],
            ),
        },
    )
    if not downloader["success"]:
        context.set_custom_status(
            "Downloading report {} for account {}: {}".format(
                report_run["report_run_id"],
                ingress["account_id"],
                downloader.get("message", "Unknown Error"),
            )
        )
        match downloader["message"]:
            case "Download Timeout":
                yield context.call_sub_orchestrator(
                    "meta_orchestrator_request",
                    {
                        "operationId": "AdReportRun.Get.Insights",
                        "parameters": {
                            "AdReportRun-Id": report_run["report_run_id"],
                            "limit": 750,
                        },
                        "recursive": True,
                        "destination": {
                            "conn_str": ingress["conn_str"],
                            "container_name": ingress["container_name"],
                            "blob_prefix": "meta/delta/adsinsights/{}/{}".format(
                                ingress["pull_time"], report_run["report_run_id"]
                            ),
                        },
                        "return": False,
                    },
                )
            case _:
                return {}

    # Retrieve Ads, Campaigns, and AdSets for the given account
    for entity in ["Ads", "Campaigns", "Adsets", "Adcreatives"]:
        context.set_custom_status(
            f"Getting {entity} for account {ingress['account_id']}"
        )
        yield context.call_sub_orchestrator(
            "meta_orchestrator_request",
            {
                "operationId": f"AdAccount.Get.{entity}",
                "parameters": {
                    **PARAMETERS[f"AdAccount.Get.{entity}"],
                    "AdAccount-Id": ingress["account_id"],
                },
                "recursive": True,
                "destination": {
                    "conn_str": ingress["conn_str"],
                    "container_name": ingress["container_name"],
                    "blob_prefix": "meta/delta/{}/{}".format(
                        entity.lower(), ingress["pull_time"]
                    ),
                },
                "return": False,
            },
        )

    # Final status update
    context.set_custom_status("Account {} has completed.".format(ingress["account_id"]))
    return {}
