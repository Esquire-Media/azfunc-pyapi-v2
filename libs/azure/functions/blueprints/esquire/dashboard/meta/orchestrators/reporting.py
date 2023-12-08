# File: libs/azure/functions/blueprints/esquire/dashboard/meta/orchestrators/reporting.py

from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from datetime import datetime, timedelta
from libs.azure.functions import Blueprint
from libs.azure.functions.blueprints.esquire.dashboard.meta.config import PARAMETERS
import logging

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def esquire_dashboard_meta_orchestrator_reporting(
    context: DurableOrchestrationContext,
):
    retry = RetryOptions(60000, 12)
    ingress = context.get_input()

    context.set_custom_status(
        "Sending report request for account {}".format(ingress["account_id"])
    )
    report_run = yield context.call_sub_orchestrator_with_retry(
        "meta_orchestrator_request",
        retry,
        {
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
                "operationId": "GetAdReportRun",
                "parameters": {"AdReportRun-id": report_run["report_run_id"]},
            },
        )
        context.set_custom_status(
            "Report for account {} is {} percent complete.".format(
                status["account_id"],
                status["async_percent_complete"],
            )
        )
        match status["async_status"]:
            case "Job Completed":
                break
            case "Job Failed":
                raise Exception("Job Failed")
            case _:
                yield context.create_timer(datetime.utcnow() + timedelta(minutes=5))

    context.set_custom_status(
        "Downloading report {} for account {}".format(
            report_run["report_run_id"],
            ingress["account_id"],
        )
    )
    yield context.call_activity_with_retry(
        "esquire_dashboard_meta_activity_download",
        retry,
        {
            "report_run_id": report_run["report_run_id"],
            "conn_str": ingress["conn_str"],
            "container_name": ingress["container_name"],
            "blob_name": "meta/delta/adsinsights/{}/{}.parquet".format(
                ingress["pull_time"],
                report_run["report_run_id"],
            ),
        },
    )
    # return status
    # blobs = yield context.call_sub_orchestrator_with_retry(
    #     "meta_orchestrator_request",
    #     retry,
    #     {
    #         "operationId": "AdReportRun_GetInsights",
    #         "parameters": {
    #             "limit": 500,
    #             "AdReportRun-id": report_run["report_run_id"],
    #         },
    #         "recursive": True,
    #         "destination": {
    #             "conn_str": ingress["conn_str"],
    #             "container_name": ingress["container_name"],
    #             "blob_prefix": "meta/delta/adsinsights/{}/{}".format(
    #                 ingress["pull_time"],
    #                 report_run["report_run_id"],
    #             ),
    #         },
    #         "return": False,
    #     },
    # )
    # logging.warning(blobs)
    # return blobs

    context.set_custom_status(
        "Getting Ads for account {}".format(ingress["account_id"])
    )
    yield context.call_sub_orchestrator_with_retry(
        "meta_orchestrator_request",
        retry,
        {
            "operationId": "AdAccount_GetAds",
            "parameters": {
                "AdAccount-id": ingress["account_id"],
                **PARAMETERS["AdAccount_GetAds"],
            },
            "recursive": True,
            "destination": {
                "conn_str": ingress["conn_str"],
                "container_name": ingress["container_name"],
                "blob_prefix": f"meta/delta/ads/{ingress['pull_time']}",
            },
            "return": False,
        },
    )

    context.set_custom_status(
        "Getting Campaigns for account {}".format(ingress["account_id"])
    )
    yield context.call_sub_orchestrator_with_retry(
        "meta_orchestrator_request",
        retry,
        {
            "operationId": "AdAccount_GetCampaigns",
            "parameters": {
                "AdAccount-id": ingress["account_id"],
                **PARAMETERS["AdAccount_GetCampaigns"],
            },
            "recursive": True,
            "destination": {
                "conn_str": ingress["conn_str"],
                "container_name": ingress["container_name"],
                "blob_prefix": f"meta/delta/campaigns/{ingress['pull_time']}",
            },
            "return": False,
        },
    )

    context.set_custom_status(
        "Getting AdSets for account {}".format(ingress["account_id"])
    )
    yield context.call_sub_orchestrator_with_retry(
        "meta_orchestrator_request",
        retry,
        {
            "operationId": "AdAccount_GetAdSets",
            "parameters": {
                **PARAMETERS["AdAccount_GetAdSets"],
                "AdAccount-id": ingress["account_id"],
            },
            "recursive": True,
            "destination": {
                "conn_str": ingress["conn_str"],
                "container_name": ingress["container_name"],
                "blob_prefix": f"meta/delta/adsets/{ingress['pull_time']}",
            },
            "return": False,
        },
    )

    # context.set_custom_status("Getting AdCreatives.")
    # yield context.call_sub_orchestrator(
    #     "meta_orchestrator_request",
    #     {
    #         "operationId": "AdAccount_GetAdCreatives",
    #         "parameters": {
    #             "AdAccount-id": ingress["account_id"],
    #             **PARAMETERS["AdAccount_GetAdCreatives"],
    #         },
    #         "recursive": True,
    #         "destination": {
    #             "conn_str": ingress["conn_str"],
    #             "container_name": ingress["container_name"],
    #             "blob_prefix": f"meta/delta/adcreatives/{ingress['pull_time']}",
    #         },
    #         "return": False,
    #     },
    # )

    context.set_custom_status("")
    return {}
