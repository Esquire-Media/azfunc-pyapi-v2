from azure.functions import Blueprint, HttpRequest, HttpResponse
import logging

bp = Blueprint()

@bp.route('callback/{jobid}')
async def callback(req: HttpRequest) ->  HttpResponse:
    logging.warning((req.route_params['jobid'], req.get_body()))
    return HttpResponse('OK')