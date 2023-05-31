from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse

import simplejson as json

# Create a Blueprint instance
bp = Blueprint()


@bp.route(route="whoami", methods=["GET"])
async def who_am_i(req: HttpRequest):
    """
    Route handler for the "whoami" endpoint.

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
    This route handler processes the HTTP GET request to the "whoami" endpoint.
    It retrieves the headers that start with "x-ms-client-principal-" from the request and returns them in the response.

    Steps:
    1. Retrieve the headers from the request.
    2. Filter the headers to include only those that start with "x-ms-client-principal-".
    3. Create a JSON response with the filtered headers.
    4. Return the JSON response as an HTTP response object.
    """
    return HttpResponse(
        json.dumps(
            {
                k: v
                for k, v in req.headers.items()
                if k.startswith("x-ms-client-principal-")
            }
        )
    )
