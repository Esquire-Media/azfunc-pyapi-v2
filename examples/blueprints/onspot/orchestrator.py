# File: blueprints/orchestrator.py

from azure.durable_functions import DurableOrchestrationContext
from datetime import datetime
from dateutil.relativedelta import relativedelta
from libs.azure.functions import Blueprint

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def daily_dashboard_onspot_orchestrator(context: DurableOrchestrationContext):
    geoframes = yield context.call_activity(
        name="daily_dashboard_onspot_activity_geoframes"
    )

    now = datetime.utcnow()
    today = datetime(now.year, now.month, now.day)
    end = today - relativedelta(days=2)
    start = end - relativedelta(days=75)

    results = yield context.task_all(
        [
            context.call_sub_orchestrator(
                "onspot_orchestrator",
                {
                    "endpoint": "/save/geoframe/all/observations",
                    "request": {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                **value,
                                "properties": {
                                    "name": key,
                                    "start": start.isoformat(),
                                    "end": end.isoformat(),
                                },
                            }
                        ],
                    },
                },
                "{}:{}".format(key, context.instance_id),
            )
            for key, value in geoframes[0:10]
        ]
    )

    return {
        job["id"]: {**job, **callback}
        for result in results
        for job in result["jobs"]
        for callback in result["callbacks"]
    }
