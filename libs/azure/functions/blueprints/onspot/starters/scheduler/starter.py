from azure.data.tables import TableClient
from azure.durable_functions import DurableOrchestrationClient
from azure.functions import TimerRequest, Context
from datetime import datetime
from libs.azure.functions import Blueprint
from libs.azure.functions.blueprints.onspot.config import (
    ONSPOT_CONN_STR,
    ONSPOT_TABLE_NAMES,
)
from libs.azure.functions.blueprints.onspot.helpers import onspot_initializer
from libs.azure.functions.blueprints.onspot.starters.scheduler.schemas import JobSchema
import os
import logging

bp = Blueprint()


# Daily timer trigger
@bp.timer_trigger(arg_name="timer", schedule="0 0 0 * * *")
@bp.durable_client_input(client_name="client")
@bp.logger()
async def onspot_starter_scheduler(
    timer: TimerRequest, client: DurableOrchestrationClient, context: Context
):
    print({k:getattr(timer,k) for k in dir(timer) if not k.startswith("_")})
    job_schema = JobSchema()
    jobs_table = TableClient.from_connection_string(
        conn_str=ONSPOT_CONN_STR, table_name=ONSPOT_TABLE_NAMES["jobs"]
    )
    config_table = TableClient.from_connection_string(
        conn_str=ONSPOT_CONN_STR, table_name=ONSPOT_TABLE_NAMES["config"]
    )
    # Get all the enabled jobs
    for job in [job_schema.load(e) for e in jobs_table.list_entities()]:
        logging.warn(job)
        init = {
            "endpoint": job["EndPoint"],
            "request": {
                "features": [
                    e["Value"]
                    for e in config_table.query_entities(
                        f"PartitionKey eq '{job['PartitionKey']}' and JobKey eq '{job['RowKey']}' and Type eq 'Feature'"
                    )
                ]
            }
            if job["EndPoint"].startswith("geoframe")
            or job["EndPoint"].startswith("save/geoframe")
            else {},
            "client": client,
            "context": {
                e["Name"]: e["Value"]
                for e in config_table.query_entities(
                    f"PartitionKey eq '{job['PartitionKey']}' and JobKey eq '{job['RowKey']}' and Type eq 'Context'"
                )
            },
            "response_url": os.environ.get("REVERSE_PROXY", None),
        }
        logging.warn(init)

        # Generate an instance ID and start the OnSpot durable function flow
        # instanceId = await onspot_initializer(**init)
