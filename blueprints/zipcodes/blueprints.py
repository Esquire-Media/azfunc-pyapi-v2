from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
from blueprints.zipcodes.engine import ZipcodeEngine
import json

bp = Blueprint()

@bp.route(route="zipcodes/list", methods=["POST"])
async def get_zipcodes_by_list(req: HttpRequest):
    zipcodes = find_parameter(req, 'zipcodes')
    engine = ZipcodeEngine()

    return HttpResponse(
        engine.load_from_list(zipcodes=zipcodes).to_json(orient='records'),
        headers={
            "content-type":"application/json"
        }
    )

@bp.route(route="zipcodes/query", methods=["POST"])
async def get_zipcodes_by_query(req: HttpRequest):

    return HttpResponse("OK")

@bp.route(route="zipcodes/point", methods=["POST"])
async def get_zipcodes_by_point(req: HttpRequest):

    return HttpResponse("OK")


# NOTE: Eventually this will be supported as a function of libs/azure/functions/http.py HTTPRequest
# The req object in the functions above is not currently pulling this in, so we define it here instead.
def find_parameter(req: HttpRequest, key:str, required:bool=True):
    """
    Utility function to find request payload parameters in a variety of formats.

    params:
    r : the request object
    key : the string name of the parameter to ingest
    required : if True, an exception will be raised if the key cannot be found. If False, None will be returned if the key cannot be found.
    """
    # try ingesting via the json payload
    try:
        value = req.get_json()[key]
        if value is None:
            raise Exception
        return value
    except:
        # try ingesting via the URL arguments, then try the form if that fails
        try:
            value = req.params.get(key, req.form.get(key))
            if value is None:
                raise Exception
            return value
        # if no value could be parsed, set equal to None or raise exception
        except:
            if required:
                raise Exception(f"Parameter `key` must be specified for this query type.")
            else:
                return None