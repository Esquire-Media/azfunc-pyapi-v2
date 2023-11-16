# File: libs/azure/functions/blueprints/esquire/rooftop_polys/orchestrator.py
from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext, RetryOptions
bp: Blueprint = Blueprint()


# main orchestrator for geoframed audiences (suborchestrator for the root)
@bp.orchestration_trigger(context_name="context")
def orchestrator_rooftopPolys(
    context: DurableOrchestrationContext,
):
    addresses = context.get_input()
    retry = RetryOptions(15000, 1)

    # pass this list into the read_cache activity
    cached_polys = yield context.call_activity_with_retry(
        "activity_rooftopPolys_readCache",
        retry_options=retry,
        input_=addresses,
    )

    # pass this list into the create_polygons activity
    if len(cached_polys):
        new_poly_list = list(set(addresses) - set(cached_polys["Query"].values()))
    else:
        new_poly_list = addresses

    ## sending just one thing for now
    new_polys = yield context.call_activity_with_retry(
        "activity_rooftopPolys_createPolygons",
        retry_options=retry,
        input_=new_poly_list,
    )

    # pass the list of new polys into the write_cache activity
    yield context.call_activity_with_retry(
        "activity_rooftopPolys_writeCache",
        retry_options=retry,
        input_=new_polys,
    )

    return [poly["geojson"] for poly in new_polys] + [
        poly
        for poly in [entry["geometry"] for entry in cached_polys["Boundary"].values()]
    ]
