# File: example/blueprints/daily_dashboard_onspot/orchestrator.py

from azure.durable_functions import DurableOrchestrationContext
from datetime import datetime
from dateutil.relativedelta import relativedelta
from example.blueprints.daily_dashboard_onspot.helpers import (
    cetas_query_unique_deviceids,
    cetas_query_sisense,
)
from libs.azure.functions import Blueprint
import os

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def daily_dashboard_onspot_orchestrator(context: DurableOrchestrationContext):
    conn_str = "ONSPOT_CONN_STR" if "ONSPOT_CONN_STR" in os.environ.keys() else None
    container = "dashboard"

    yield context.call_activity(
        "daily_dashboard_onspot_activity_locations",
        {
            "instance_id": context.instance_id,
            "conn_str": conn_str,
            "container": container,
            "outputPath": f"raw/{context.instance_id}/locations.csv",
        },
    )

    geoframes = yield context.call_activity(
        name="daily_dashboard_onspot_activity_geoframes"
    )

    now = datetime.utcnow()
    today = datetime(now.year, now.month, now.day)
    end = today - relativedelta(days=2)
    start = end - relativedelta(days=75)

    batch_size = 100
    yield context.task_all(
        [
            context.call_sub_orchestrator(
                "onspot_orchestrator",
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
                                    "headers": ["deviceid", "timestamp", "lat", "lng"],
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
                "container": container,
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
            context.call_sub_orchestrator(
                "onspot_orchestrator",
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
            if (
                subinstance_id := "{}:{}:{}".format(
                    context.instance_id, "zips", i
                )
            )
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
                "container": container,
                "handle": "sa_esquireonspot",
                "path": f"tables/{context.instance_id}/sisense",
            },
            "query": cetas_query_sisense(context.instance_id),
            "commit": True,
            "view": True,
        },
    )

    # yield context.call_activity(
    #     "datalake_activity_delete_directory",
    #     {
    #         "instance_id": context.instance_id,
    #         "conn_str": conn_str,
    #         "container": container,
    #         "prefix": "raw"
    #     },
    # )
