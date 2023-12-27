# File: libs/azure/functions/blueprints/esquire/dashboard/meta/orchestrators/report_batch.py

from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from libs.azure.functions import Blueprint
from libs.azure.functions.blueprints.esquire.dashboard.meta.config import (
    PARAMETERS,
    CETAS,
)
import os, logging

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def esquire_dashboard_meta_orchestrator_report_batch(
    context: DurableOrchestrationContext,
):
    """
    Orchestrate the batch processing of Facebook Ad data, including downloading, transforming,
    and loading into Azure Synapse.

    This orchestrator coordinates multiple sub-orchestrators and activities to process Facebook Ad
    data. It retrieves Ad Accounts, generates reports, and creates CETAS (Create External Table As Select)
    statements in Azure Synapse for various entities like Ad Accounts, Ads Insights, Ads, Campaigns, and Ad Sets.

    Parameters
    ----------
    context : DurableOrchestrationContext
        The context object provided by Azure Durable Functions framework, used for invoking sub-orchestrators,
        activities, and managing state.

    Raises
    ------
    Exception
        If any part of the process fails, it posts a message to a designated webhook and raises the exception.

    """

    # Set up retry options for activities
    retry = RetryOptions(15000, 3)

    # Get the current time to use as a timestamp for data processing
    pull_time = context.current_utc_datetime.isoformat()

    # Determine the connection string to use for Azure services
    conn_str = (
        "META_CONN_STR"
        if "META_CONN_STR" in os.environ.keys()
        else "AzureWebJobsStorage"
    )
    container_name = "general"

    try:
        # Get Facebook Ad Accounts
        context.set_custom_status("Getting Adaccounts")
        adaccounts = yield context.call_sub_orchestrator_with_retry(
            "meta_orchestrator_request",
            retry,
            {
                "operationId": "User.Get.Adaccounts",
                "parameters": {
                    **PARAMETERS["User.Get.Adaccounts"],
                    "User-Id": "me",
                },
                "recursive": True,
                "destination": {
                    "conn_str": conn_str,
                    "container_name": container_name,
                    "blob_prefix": f"meta/delta/adaccounts/{pull_time}",
                },
            },
        )
        logging.warning([a["name"] for a in adaccounts])

        # Process reports for each Ad Account
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
                if adaccount.get("id")
                and adaccount["id"]
                not in [
                    "act_147888709160457",
                ]
                and adaccount.get("name")
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
                "query": CETAS["User.Get.Adaccounts"],
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
                "query": CETAS["AdAccount.Post.Insights"],
                "view": True,
            },
        )

        # Generate CETAS for Ads, Campaigns, and AdSets
        for entity in ["Ads", "Campaigns", "AdSets"]:
            context.set_custom_status(f"Generating {entity} CETAS")
            yield context.call_activity_with_retry(
                "synapse_activity_cetas",
                retry,
                {
                    "instance_id": context.instance_id,
                    "bind": "facebook_dashboard",
                    "table": {"schema": "dashboard", "name": entity.lower()},
                    "destination": {
                        "container_name": container_name,
                        "handle": "sa_esquiregeneral",
                        "blob_prefix": f"meta/tables/{entity}/{pull_time}",
                    },
                    "query": CETAS[f"AdAccount.Get.{entity.title()}"],
                    "view": True,
                },
            )

        # Handle exceptions by posting to a webhook and re-raise the exception
    except Exception as e:
        yield context.call_http(
            method="POST",
            uri=os.environ["EXCEPTIONS_WEBHOOK_DEVOPS"],
            content={
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "themeColor": "EE2A3D",
                "summary": "Meta Report Ingestion Failed",
                "sections": [
                    {
                        "activityTitle": "Meta Report Ingestion Failed",
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

    # Purge history related to this instance at the end of the orchestration
    context.set_custom_status("Purging History")
    yield context.call_sub_orchestrator(
        "purge_instance_history",
        {"instance_id": context.instance_id},
    )
