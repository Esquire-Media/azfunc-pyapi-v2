from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
from libs.data import from_bind

# Create a Blueprint instance
bp = Blueprint()

@bp.jsonapi()
@bp.route("{binding}/jsonapi/v1")
async def api_v1_jsonapi(req: HttpRequest) -> HttpResponse:
    """
    Route handler for the JSONAPI endpoint.

    Parameters
    ----------
    req : HttpRequest
        The HTTP request object.

    Returns
    -------
    HttpResponse
        The HTTP response object.

    Notes
    -----
    This route handler processes the JSONAPI endpoint requests.
    It handles various JSONAPI operations, including GET, POST, PATCH, and DELETE.

    Steps:
    1. Retrieve the provider based on the binding from the route parameters.
    2. Based on the request method:
        - If it is a GET request:
            - Check the JSONAPI "type" in the request to determine the type of resource.
            - Retrieve the resources based on the type and optional ID or relation.
            - Return an HTTP response with the retrieved resources.
        - If it is a POST request:
            - Get the JSONAPI payload from the request body.
            - Save the payload as a new resource using the provider.
            - Return an HTTP response indicating the success of the operation.
        - If it is a PATCH request:
            - Get the JSONAPI payload from the request body.
            - Update the specified resource with the payload using the provider.
            - Return an HTTP response indicating the success of the operation.
        - If it is a DELETE request:
            - Check the JSONAPI "type" and "id" in the request to determine the resource to delete.
            - Delete the specified resource using the provider.
            - Return an HTTP response indicating the success of the operation.
    3. If none of the conditions match, return a 404 Not Found HTTP response.

    This route handler can be further customized to handle additional JSONAPI operations and implement specific business logic based on the application's requirements.
    """
    provider = from_bind(req.route_params.get("binding"))
    match req.method:
        case "GET":
            if req.jsonapi["type"] == "schema":
                resources = provider.SCHEMA
            elif not req.jsonapi.get("id"):
                resources = provider[req.jsonapi["type"]]
            elif not req.jsonapi.get("relation"):
                resources = provider[req.jsonapi["type"]](req.jsonapi["id"])
            return HttpResponse(resources=resources)
        case "POST":
            payload = req.get_json()
            provider.save(payload)
            return HttpResponse(status_code=201)
        case "PATCH":
            payload = req.get_json()
            provider.update(req.jsonapi["type"], req.jsonapi["id"], payload)
            return HttpResponse(status_code=200)
        case "DELETE":
            if req.jsonapi["type"] and req.jsonapi["id"]:
                provider.drop(req.jsonapi["type"], req.jsonapi["id"])
                return HttpResponse(status_code=204)
    return HttpResponse(status_code=404)
