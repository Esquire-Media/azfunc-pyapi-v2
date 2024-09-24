# File: libs/azure/functions/blueprints/esquire/rooftop_polys/orchestrator.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions
import pandas as pd, logging

bp: Blueprint = Blueprint()


def batcher(iterable, n=1):
    """Generator that yields successive n-sized chunks from iterable."""
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx : min(ndx + n, l)]


# main orchestrator for geoframed audiences (suborchestrator for the root)
@bp.orchestration_trigger(context_name="context")
def orchestrator_rooftopPolys(
    context: DurableOrchestrationContext,
):
    addresses = context.get_input()
    retry = RetryOptions(15000, 1)

    # pass this list into the read_cache activity
    cached_polys = yield context.task_all(
        [
            context.call_activity_with_retry(
                "activity_rooftopPolys_readCache",
                retry_options=retry,
                input_=batch,
            )
            for batch in batcher(addresses, 1000)
        ]
    )
    cached_polys = (
        pd.concat([pd.DataFrame(cache) for cache in cached_polys]).reindex().to_dict()
    )

    ## sending just one thing for now
    new_polys = yield context.task_all(
        [
            context.call_activity_with_retry(
                "activity_rooftopPolys_createPolygons",
                retry_options=retry,
                input_=batch,
            )
            for batch in batcher(
                list(set(addresses) - set(cached_polys.get("query", []).values())),
                50,
            )
        ]
    )

    # pass the list of new polys into the write_cache activity
    yield context.task_all(
        [
            context.call_activity_with_retry(
                "activity_rooftopPolys_writeCache",
                retry_options=retry,
                input_=batch,
            )
            for batch in new_polys
        ]
    )

    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"address": poly["query"]},
                "geometry": poly["geojson"],
            }
            for batch in new_polys
            for poly in batch
        ]
        + [
            {
                "type": "Feature",
                "properties": {"address": cached_polys["query"][key]},
                "geometry": eval(cached_polys["boundary"][key]),
            }
            for key in cached_polys["query"].keys()
        ],
    }
