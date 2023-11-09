# File: libs/azure/functions/blueprints/esquire/audiences/daily_audience_generation/orchestrators/rooftop_poly.py
from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext, RetryOptions

bp: Blueprint = Blueprint()


# main orchestrator for geoframed audiences (suborchestrator for the root)
@bp.orchestration_trigger(context_name="context")
def orchestrator_rooftop_poly(context: DurableOrchestrationContext):
    addresses = context.get_input()
    retry = RetryOptions(15000, 1)
    
    # pass this list into the read_cache activity
    cached_polys = yield context.call_activity_with_retry(
        "activity_read_cache",
        retry_options=retry,
        input_=addresses,
    )
    
    # logging.warning(cached_polys)    
    
    # pass this list into the create_polygons activity
    if len(cached_polys):
        new_poly_list=list(set(addresses) - set(cached_polys["Query"].values()))[0:9]
    else:
        new_poly_list = addresses[0:9]
        
    ## sending just one thing for now
    new_polys = yield context.call_activity_with_retry(
        "activity_create_polygons",
        retry_options=retry,
        input_=new_poly_list,
    )

    # logging.warning(new_polys)
    # new_polys = '[{"query":"1512 OURAY AVE, FORT MORGAN CO, 80701","geojson":{"type":"Polygon","coordinates":[[[-103.7825338,40.2621268],[-103.7825338,40.2619488],[-103.7826776,40.2619488],[-103.7826776,40.2621268],[-103.7825338,40.2621268]]]}}]'

    # pass the list of new polys into the write_cache activity
    ## using a test
    yield context.call_activity_with_retry(
        "activity_write_cache",
        retry_options=retry,
        input_=new_polys,
    )

    geojson_data = {}

    for key, feature in cached_polys["Boundary"].items():
        if 'geometry' in feature:
            geojson_data[key] = {'type': 'Feature', 'geometry': feature['geometry']}
    
    # needs to be a geography datatype
    return geojson_data