# File: libs/azure/functions/blueprints/daily_audience_generation/orchestrators/rooftop_poly.py
from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext, RetryOptions
import logging

bp: Blueprint = Blueprint()

# main orchestrator for geoframed audiences (suborchestrator for the root)
@bp.orchestration_trigger(context_name="context")
def suborchestrator_rooftop_poly(context: DurableOrchestrationContext):
    ingress = context.get_input()
    retry = RetryOptions(15000, 1)
    
    # pass audiences into address validation
    ## returns list of objects with key= audience_id and value= cleaned addresses
    validated_audiences = yield context.task_all(
        [
            # pass address list into vadliation activity
            context.call_activity_with_retry(
                "activity_address_validation",
                retry_options=retry,
                input_={
                    "path": ingress["path"],
                    "audience": audience["Id"],
                    "instance_id": ingress["instance_id"],
                    "context": None,
                },
            )
            for audience in ingress["audiences"]
        ]
    )
    
    # pass this list into the read_cache activity
    audiences = yield context.task_all(
        [
            context.call_activity_with_retry(
                "activity_read_cache",
                retry_options=retry,
                input_={
                    "path": ingress["path"],
                    "instance_id": ingress["instance_id"],
                    "context": None,
                    # "audience": f"{audience['audience_id']}test",
                    **audience
                },
            )
            for audience in validated_audiences
        ]
    )
    
    # pass this list into the create_polygons activity
    audiences_full = yield context.task_all(
        [
            context.call_activity_with_retry(
                "activity_create_polygons",
                retry_options=retry,
                input_={
                    "path": ingress["path"],
                    "instance_id": ingress["instance_id"],
                    "context": None,
                    **audience
                },
            )
            for audience in audiences
        ]
    )
    
    
    # logging.warning(audiences)
    return {}