from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def esquire_dashboard_unmasked_orchestrator(context: DurableOrchestrationContext):
    """
    Execute a "load and push" operation for a single Unmasked Pixel pull.

    Ingress
    ----------
    A dictionary containing the following keys:
        access_key : Athena parameter
        secret_key : Athena parameter
        bucket : Athena parameter
        region : Athena parameter
        database : Athena parameter
        workgroup : Athena parameter

        runtime_container :
            conn_str : connection string env variable name for the runtime storage container
            container_name : default container name
    """

    ingress = context.get_input()
    retry = RetryOptions(15000, 1)

    # Execute query to pull pixel data from Athena
    tasks = [
        context.call_sub_orchestrator_with_retry(
            "aws_athena_orchestrator",
            retry,
            {
                **{k: v for k, v in ingress.items() if "query" not in k},
                "query": f'SELECT * FROM resolutions.{table}',
                "destination": {
                    **ingress["runtime_container"],
                    "blob_name": f"datalake/unmasked/{table}.csv",
                },
            },
        )
        for table in ["sessions", "visitors", "pages"]
    ]
    for task in tasks:
        yield task
    
    return {}