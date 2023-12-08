# File: libs/azure/functions/blueprints/esquire/dashboard/meta/orchestrators/report_batch.py

from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from libs.azure.functions import Blueprint
from libs.azure.functions.blueprints.esquire.dashboard.meta.config import (
    PARAMETERS,
    CETAS,
)
import os

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def esquire_dashboard_meta_orchestrator_report_batch(
    context: DurableOrchestrationContext,
):
    retry = RetryOptions(15000, 3)
    pull_time = context.current_utc_datetime.isoformat()
    conn_str = (
        "META_CONN_STR"
        if "META_CONN_STR" in os.environ.keys()
        else "AzureWebJobsStorage"
    )
    container_name = "general"
    try:
        context.set_custom_status("Getting AdAccounts")
        adaccounts = yield context.call_sub_orchestrator(
            "meta_orchestrator_request",
            {
                "operationId": "User_GetAdAccounts",
                "parameters": {
                    **PARAMETERS["User_GetAdAccounts"],
                    "User-id": "me",
                },
                "recursive": True,
                "destination": {
                    "conn_str": conn_str,
                    "container_name": container_name,
                    "blob_prefix": f"meta/delta/adaccounts/{pull_time}",
                },
            },
        )

        context.set_custom_status("Getting Reports")
        yield context.task_all(
            [
                context.call_sub_orchestrator(
                    "esquire_dashboard_meta_orchestrator_reporting",
                    {
                        "instance_id": context.instance_id,
                        "conn_str": conn_str,
                        "container_name": container_name,
                        "account_id": adaccount["id"],
                        "pull_time": pull_time,
                    },
                )
                for adaccount in adaccounts
                if adaccount["id"]
                not in [
                    "act_147888709160457",
                ]
                and "do no use" not in adaccount["name"].lower()
            ]
        )
        
        context.set_custom_status("Generating AdAccounts CETAS")
        yield context.call_activity_with_retry(
            "synapse_activity_cetas",
            retry,
            {
                "instance_id": context.instance_id,
                "bind": "facebook_dashboard",
                "table": {"schema": "dashboard", "name": "adaccounts"},
                "destination": {
                    "container_name": container_name,
                    "handle": "sa_esquiregeneral",
                    "blob_prefix": f"meta/tables/AdAccounts/{pull_time}",
                },
                "query": CETAS["User_GetAdAccounts"],
                "view": True,
            },
        )

        context.set_custom_status("Generating AdsInsights CETAS")
        yield context.call_activity_with_retry(
            "synapse_activity_cetas",
            retry,
            {
                "instance_id": context.instance_id,
                "bind": "facebook_dashboard",
                "table": {"schema": "dashboard", "name": "adsinsights"},
                "destination": {
                    "container_name": container_name,
                    "handle": "sa_esquiregeneral",
                    "blob_prefix": f"meta/tables/AdsInsights/{pull_time}",
                },
                "query": CETAS["AdAccount_GetInsightsAsync"],
                "view": True,
            },
        )

        context.set_custom_status("Generating Ads CETAS")
        yield context.call_activity_with_retry(
            "synapse_activity_cetas",
            retry,
            {
                "instance_id": context.instance_id,
                "bind": "facebook_dashboard",
                "table": {"schema": "dashboard", "name": "ads"},
                "destination": {
                    "container_name": container_name,
                    "handle": "sa_esquiregeneral",
                    "blob_prefix": f"meta/tables/Ads/{pull_time}",
                },
                "query": CETAS["AdAccount_GetAds"],
                "view": True,
            },
        )

        context.set_custom_status("Generating Campaigns CETAS")
        yield context.call_activity_with_retry(
            "synapse_activity_cetas",
            retry,
            {
                "instance_id": context.instance_id,
                "bind": "facebook_dashboard",
                "table": {"schema": "dashboard", "name": "campaigns"},
                "destination": {
                    "container_name": container_name,
                    "handle": "sa_esquiregeneral",
                    "blob_prefix": f"meta/tables/Campaigns/{pull_time}",
                },
                "query": CETAS["AdAccount_GetCampaigns"],
                "view": True,
            },
        )

        context.set_custom_status("Generating AdSets CETAS")
        yield context.call_activity_with_retry(
            "synapse_activity_cetas",
            retry,
            {
                "instance_id": context.instance_id,
                "bind": "facebook_dashboard",
                "table": {"schema": "dashboard", "name": "adsets"},
                "destination": {
                    "container_name": container_name,
                    "handle": "sa_esquiregeneral",
                    "blob_prefix": f"meta/tables/AdSets/{pull_time}",
                },
                "query": CETAS["AdAccount_GetAdSets"],
                "view": True,
            },
        )

        # yield context.call_activity_with_retry(
        #     "synapse_activity_cetas",
        #     retry,
        #     {
        #         "instance_id": context.instance_id,
        #         "bind": "facebook_dashboard",
        #         "table": {"schema": "dashboard", "name": "adcreatives"},
        #         "destination": {
        #             "container_name": container,
        #             "handle": "sa_esquiregeneral",
        #             "blob_prefix": f"meta/tables/AdCreatives/{context.instance_id}",
        #         },
        #         "query": CETAS["AdAccount_GetCreatives"],
        #         "view": True,
        #     },
        # )

    except Exception as e:
        yield context.call_http(
            method="POST",
            uri=os.environ["EXCEPTIONS_WEBHOOK_DEVOPS"],
            content={
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "themeColor": "EE2A3D",
                "summary": "Meta Report Injestion Failed",
                "sections": [
                    {
                        "activityTitle": "Meta Report Injestion Failed",
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
    context.set_custom_status("Purging History")
    yield context.call_sub_orchestrator(
        "purge_instance_history",
        {"instance_id": context.instance_id},
    )
