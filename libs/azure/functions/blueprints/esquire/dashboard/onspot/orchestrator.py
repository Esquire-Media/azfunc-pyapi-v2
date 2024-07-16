# File: libs/azure/functions/blueprints/esquire/dashboard/onspot/orchestrator.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions
from datetime import datetime
from dateutil.relativedelta import relativedelta
from libs.azure.functions.blueprints.esquire.dashboard.onspot.helpers import (
    cetas_query_unique_deviceids,
    cetas_query_sisense,
)
import os

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def esquire_dashboard_onspot_orchestrator(context: DurableOrchestrationContext):
    retry = RetryOptions(15000, 3)
    conn_str = "ONSPOT_CONN_STR" if "ONSPOT_CONN_STR" in os.environ.keys() else None
    container = "dashboard"

    try:
        yield context.call_activity_with_retry(
            "esquire_dashboard_onspot_activity_locations",
            retry,
            {
                "instance_id": context.instance_id,
                "conn_str": conn_str,
                "container": container,
                "outputPath": f"raw/{context.instance_id}/locations.csv",
            },
        )

        geoframes = yield context.call_activity_with_retry(
            "esquire_dashboard_onspot_activity_geoframes",
            retry,
        )

        now = context.current_utc_datetime
        today = datetime(now.year, now.month, now.day)
        end = today - relativedelta(days=2)
        start = end - relativedelta(days=75)

        batch_size = 100
        yield context.task_all(
            [
                context.call_sub_orchestrator_with_retry(
                    "onspot_orchestrator",
                    retry,
                    {
                        "conn_str": conn_str,
                        "container": container,
                        "outputPath": "raw/{}/{}".format(
                            context.instance_id, "observations"
                        ),
                        "endpoint": "/save/geoframe/all/observations",
                        "request": {
                            "type": "FeatureCollection",
                            "features": [
                                {
                                    **value,
                                    "properties": {
                                        "name": key,
                                        "fileName": key,
                                        "start": start.isoformat(),
                                        "end": end.isoformat(),
                                        "hash": False,
                                        "headers": [
                                            "deviceid",
                                            "timestamp",
                                            "lat",
                                            "lng",
                                        ],
                                    },
                                }
                                for key, value in geoframes[i : i + batch_size]
                            ],
                        },
                    },
                    subinstance_id,
                )
                for i in range(0, len(geoframes), batch_size)
                if (
                    subinstance_id := "{}:{}:{}".format(
                        context.instance_id, "observations", i
                    )
                )
            ]
        )

        urls = yield context.call_activity(
            "synapse_activity_cetas",
            {
                "instance_id": context.instance_id,
                "bind": "onspot",
                "table": {"name": "unique_deviceids"},
                "destination": {
                    "conn_str": conn_str,
                    "container_name": container,
                    "handle": "sa_esquireonspot",
                    "path": f"raw/{context.instance_id}/unique_deviceids",
                    "format": "CSV",
                },
                "query": cetas_query_unique_deviceids(context.instance_id),
                "return_urls": True,
            },
        )

        yield context.task_all(
            [
                context.call_sub_orchestrator_with_retry(
                    "onspot_orchestrator",
                    retry,
                    {
                        "conn_str": conn_str,
                        "container": container,
                        "outputPath": "raw/{}/{}".format(context.instance_id, "zips"),
                        "endpoint": "/save/files/household",
                        "request": {
                            "type": "FeatureCollection",
                            "features": [
                                {
                                    "type": "Files",
                                    "paths": [urls[i].replace("https://", "az://")],
                                    "properties": {
                                        "name": "zips",
                                        "hash": False,
                                        "headers": ["deviceid", "zipcode"],
                                    },
                                }
                            ],
                        },
                    },
                    subinstance_id,
                )
                for i in range(0, len(urls), 1)
                if (subinstance_id := "{}:{}:{}".format(context.instance_id, "zips", i))
            ]
        )

        yield context.call_activity(
            "synapse_activity_cetas",
            {
                "instance_id": context.instance_id,
                "bind": "onspot",
                "table": {"name": "sisense"},
                "destination": {
                    "conn_str": conn_str,
                    "container_name": container,
                    "handle": "sa_esquireonspot",
                    "path": f"tables/{context.instance_id}/sisense",
                },
                "query": cetas_query_sisense(context.instance_id),
                "commit": True,
                "view": True,
            },
        )

        yield context.call_activity_with_retry(
            "datalake_activity_delete_directory",
            retry,
            {
                "instance_id": context.instance_id,
                "conn_str": conn_str,
                "container_name": container,
                "prefix": "raw",
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
                "summary": "OnSpot Report Injestion Failed",
                "sections": [
                    {
                        "activityTitle": "OnSpot Report Injestion Failed",
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