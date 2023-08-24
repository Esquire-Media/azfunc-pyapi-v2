# File: libs/azure/functions/blueprints/esquire/dashboard/meta/orchestrators/report_batch.py

from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from datetime import datetime
from libs.azure.functions import Blueprint
from libs.azure.functions.blueprints.esquire.dashboard.meta.config import (
    PARAMETERS,
    CETAS,
)
import os, logging, json

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def esquire_dashboard_meta_orchestrator_report_batch(
    context: DurableOrchestrationContext,
):
    retry = RetryOptions(15000, 3)
    pull_time = datetime.utcnow().isoformat()
    conn_str = (
        "META_CONN_STR"
        if "META_CONN_STR" in os.environ.keys()
        else "AzureWebJobsStorage"
    )
    container = "general"

    adaccounts = yield context.call_sub_orchestrator(
        "meta_orchestrator_request",
        {
            "access_token": "META_ACCESS_TOKEN",
            "modules": ["User", "AdAccount"],
            "operationId": "User_GetAdAccounts",
            "parameters": {
                **PARAMETERS["User_GetAdAccounts"],
                "User-id": "me",
            },
            "recursive": True,
            "destination": {
                "conn_str": conn_str,
                "container": container,
                "path": f"meta/delta/adaccounts/{pull_time}",
            },
        },
    )
    
    try:
        yield context.task_all(
            [
                context.call_sub_orchestrator(
                    "esquire_dashboard_meta_orchestrator_reporting",
                    {
                        "instance_id": context.instance_id,
                        "conn_str": conn_str,
                        "container": container,
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
    except:
        pass

    yield context.call_activity_with_retry(
        "synapse_activity_cetas",
        retry,
        {
            "instance_id": context.instance_id,
            "bind": "facebook_dashboard",
            "table": {"schema": "dashboard", "name": "adaccounts"},
            "destination": {
                "container": container,
                "handle": "sa_esquiregeneral",
                "path": f"meta/tables/AdAccounts/{pull_time}",
            },
            "query": CETAS["User_GetAdAccounts"],
            "view": True,
        },
    )

    yield context.call_activity_with_retry(
        "synapse_activity_cetas",
        retry,
        {
            "instance_id": context.instance_id,
            "bind": "facebook_dashboard",
            "table": {"schema": "dashboard", "name": "adsinsights"},
            "destination": {
                "container": container,
                "handle": "sa_esquiregeneral",
                "path": f"meta/tables/AdsInsights/{pull_time}",
            },
            "query": CETAS["AdAccount_GetInsightsAsync"],
            "view": True,
        },
    )

    yield context.call_activity_with_retry(
        "synapse_activity_cetas",
        retry,
        {
            "instance_id": context.instance_id,
            "bind": "facebook_dashboard",
            "table": {"schema": "dashboard", "name": "ads"},
            "destination": {
                "container": container,
                "handle": "sa_esquiregeneral",
                "path": f"meta/tables/Ads/{pull_time}",
            },
            "query": CETAS["AdAccount_GetAds"],
            "view": True,
        },
    )

    yield context.call_activity_with_retry(
        "synapse_activity_cetas",
        retry,
        {
            "instance_id": context.instance_id,
            "bind": "facebook_dashboard",
            "table": {"schema": "dashboard", "name": "campaigns"},
            "destination": {
                "container": container,
                "handle": "sa_esquiregeneral",
                "path": f"meta/tables/Campaigns/{pull_time}",
            },
            "query": CETAS["AdAccount_GetCampaigns"],
            "view": True,
        },
    )

    yield context.call_activity_with_retry(
        "synapse_activity_cetas",
        retry,
        {
            "instance_id": context.instance_id,
            "bind": "facebook_dashboard",
            "table": {"schema": "dashboard", "name": "adsets"},
            "destination": {
                "container": container,
                "handle": "sa_esquiregeneral",
                "path": f"meta/tables/AdSets/{pull_time}",
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
    #             "container": container,
    #             "handle": "sa_esquiregeneral",
    #             "path": f"meta/tables/AdCreatives/{context.instance_id}",
    #         },
    #         "query": CETAS["AdAccount_GetCreatives"],
    #         "view": True,
    #     },
    # )

    return ""