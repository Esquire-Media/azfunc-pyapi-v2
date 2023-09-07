from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
import logging

bp:Blueprint = Blueprint()

@bp.route(route="callback/{jobid}")
async def callback_daily_audience_generation(req: HttpRequest):
    
    logging.warning([req.route_params['jobid'], req.get_json()])
    return HttpResponse("Success!", status_code=200)