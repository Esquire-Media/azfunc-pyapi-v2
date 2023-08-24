# File: libs/azure/functions/blueprints/esquire/dashboard/meta/orchestrators/reporting.py

from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from datetime import datetime, timedelta
from libs.azure.functions import Blueprint
from libs.azure.functions.blueprints.esquire.dashboard.meta.config import PARAMETERS

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def esquire_dashboard_meta_orchestrator_reporting(
    context: DurableOrchestrationContext,
):
    retry = RetryOptions(15000, 3)
    ingress = context.get_input()

    context.set_custom_status("Sending report request.")
    report_runs = yield context.call_sub_orchestrator(
        "meta_orchestrator_request",
        {
            "modules": [
                "AdAccount",
                "AdsInsights",
                "AdReportRun",
                "AdSet",
                "AdCampaign",
            ],
            "operationId": "AdAccount_GetInsightsAsync",
            "parameters": {
                **PARAMETERS["AdAccount_GetInsightsAsync"],
                "AdAccount-id": ingress["account_id"],
            },
        },
    )

    while True:
        status = yield context.call_sub_orchestrator(
            "meta_orchestrator_request",
            {
                "modules": ["AdReportRun"],
                "operationId": "GetAdReportRun",
                "parameters": {"AdReportRun-id": report_runs[0]["report_run_id"]},
            },
        )
        context.set_custom_status(status[0])
        match status[0]["async_status"]:
            case "Job Completed":
                break
            case "Job Failed":
                raise Exception("Job Failed")
            case _:
                yield context.create_timer(datetime.utcnow() + timedelta(minutes=5))

    context.set_custom_status("Downloading report.")
    yield context.call_activity_with_retry(
        "esquire_dashboard_meta_activity_download",
        retry,
        {
            "report_run_id": report_runs[0]["report_run_id"],
            "conn_str": ingress["conn_str"],
            "container": ingress["container"],
            "outputPath": "meta/delta/{}/{}/{}.parquet".format(
                "adsinsights",
                ingress["pull_time"],
                report_runs[0]["report_run_id"],
            ),
        },
    )
    
    context.set_custom_status("Getting Ads.")
    yield context.call_sub_orchestrator(
        "meta_orchestrator_request",
        {
            "modules": ["AdAccount", "Ad"],
            "operationId": "AdAccount_GetAds",
            "parameters": {
                "AdAccount-id": ingress["account_id"],
                **PARAMETERS["AdAccount_GetAds"],
            },
            "recursive": True,
            "destination": {
                "conn_str": ingress["conn_str"],
                "container": ingress["container"],
                "path": f"meta/delta/ads/{ingress['pull_time']}",
            },
            "return": False,
        },
    )

    context.set_custom_status("Getting Campaigns.")
    yield context.call_sub_orchestrator(
        "meta_orchestrator_request",
        {
            "modules": ["AdAccount", "Campaign"],
            "operationId": "AdAccount_GetCampaigns",
            "parameters": {
                "AdAccount-id": ingress["account_id"],
                **PARAMETERS["AdAccount_GetCampaigns"],
            },
            "recursive": True,
            "destination": {
                "conn_str": ingress["conn_str"],
                "container": ingress["container"],
                "path": f"meta/delta/campaigns/{ingress['pull_time']}",
            },
            "return": False,
        },
    )

    context.set_custom_status("Getting AdSets.")
    yield context.call_sub_orchestrator(
        "meta_orchestrator_request",
        {
            "modules": ["AdAccount", "AdSet"],
            "operationId": "AdAccount_GetAdSets",
            "parameters": {
                **PARAMETERS["AdAccount_GetAdSets"],
                "AdAccount-id": ingress["account_id"],
            },
            "recursive": True,
            "destination": {
                "conn_str": ingress["conn_str"],
                "container": ingress["container"],
                "path": f"meta/delta/adsets/{ingress['pull_time']}",
            },
            "return": False,
        },
    )

    # context.set_custom_status("Getting AdCreatives.")
    # yield context.call_sub_orchestrator(
    #     "meta_orchestrator_request",
    #     {
    #         "modules": ["AdAccount", "AdCreative"],
    #         "operationId": "AdAccount_GetAdCreatives",
    #         "parameters": {
    #             "AdAccount-id": ingress["account_id"],
    #             **PARAMETERS["AdAccount_GetAdCreatives"],
    #         },
    #         "recursive": True,
    #         "destination": {
    #             "conn_str": ingress["conn_str"],
    #             "container": ingress["container"],
    #             "path": f"meta/delta/adcreatives/{ingress['pull_time']}",
    #         },
    #         "return": False,
    #     },
    # )
    
    context.set_custom_status("")
    return {}
