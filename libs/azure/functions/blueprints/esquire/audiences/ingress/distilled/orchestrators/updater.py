# File: libs/azure/functions/blueprints/esquire/audiences/distilled/orchestrators/updater.py

from azure.durable_functions import DurableOrchestrationContext
from azure.durable_functions import Blueprint

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def esquire_audiences_distilled_orchestrator_updater(
    context: DurableOrchestrationContext,
):
    """
    Orchestrator function for updating Esquire Audiences Distilled data.

    This function coordinates the execution of sub-orchestrators to query AWS Athena
    for different audience types and then performs a purge operation. It handles
    tasks for b2b, b2c, pte, and hem audiences.

    Parameters
    ----------
    context : DurableOrchestrationContext
        The context for the orchestration, containing methods for interacting with
        the Durable Functions runtime and input data.
        - source : dict
            Configuration details for the source data, including:
            - access_key: str
                The access key for AWS Athena.
            - secret_key: str
                The secret key for AWS Athena.
            - bucket: str
                The S3 bucket name where Athena results are stored.
            - region: str
                The AWS region for Athena.
            - database: str
                The database to query in Athena.
            - workgroup: str
                The workgroup to use in Athena.
        - destination : dict
            Configuration details for the destination, including:
            - conn_str: str
                Connection string for Azure Blob Storage.
            - container_name: str
                The name of the container in Azure Blob Storage.
    """

    # retry = RetryOptions(15000, 3)
    settings = context.get_input()

    # Execute queries for different audience types and store results
    yield context.task_all(
        [
            context.call_sub_orchestrator(
                "aws_athena_orchestrator",
                {
                    **settings["source"],
                    "query": v,
                    "destination": {
                        **settings["destination"],
                        "blob_name": f"distilled/{k}.csv",
                    },
                },
            )
            for k, v in {
                "b2b": 'SELECT * FROM "pixel"."b2b"',
                "b2c": 'SELECT * FROM "pixel"."b2c"',
            }.items()
        ]
    )

    # Call sub-orchestrator to purge the instance history
    yield context.call_sub_orchestrator(
        "purge_instance_history",
        {"instance_id": context.instance_id},
    )
