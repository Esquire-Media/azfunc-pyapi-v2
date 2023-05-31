# File: libs/azure/functions/decorators/jsonapi/__init__.py

from .exceptions import *
from libs.azure.functions.http import HttpRequest, HttpResponse
from querystring_parser import parser

try:
    import simplejson as json
except:
    import json

JSONAPI_VERSION = "1.1"


def validate_jsonapi_request(request: HttpRequest):
    """
    Validates the JSON API request.

    Parameters
    ----------
    request : HttpRequest
        The HTTP request object.

    Raises
    ------
    JsonApiMethodException
        If the HTTP method is not supported by the JSON API specification.
    JsonApiHeaderAcceptException
        If the Accept header is not set to "application/vnd.api+json".
    JsonApiHeaderContentTypeException
        If the Content-Type header is not set to "application/vnd.api+json".
    JsonApiInvalidVersionException
        If the JSONAPI version in the request does not match the expected version.
    JsonApiInvalidPayloadException
        If the body of the request is not valid JSON.
    JsonApiInvalidEmptyPayloadException
        If the body of the request should be empty but contains other data.
    JsonApiInvalidRequestException
        If the JSON API request is missing required 'data', 'type', or 'id' values.

    Notes
    -----
    This function performs various validations on the JSON API request based on the JSON API specification.
    It checks the HTTP method, Accept header, Content-Type header, JSONAPI version, payload validity, and required values.
    """
    # Validate the request method
    if request.method not in ["GET", "POST", "PATCH", "DELETE"]:
        raise JsonApiMethodException()

    # Validate the Accept header
    if request.headers.get("Accept") != "application/vnd.api+json":
        raise JsonApiHeaderAcceptException

    # Validate the JSONAPI version
    if request.method in ["POST", "PATCH", "DELETE"]:
        # Validate the content type
        if "application/vnd.api+json" not in request.headers.get("Content-Type"):
            raise JsonApiHeaderContentTypeException
        try:
            if request.method in ["POST", "PATCH"]:
                body = request.get_json()
                if body.get("jsonapi") != {"version": JSONAPI_VERSION}:
                    raise JsonApiInvalidVersionException(JSONAPI_VERSION)
                else:
                    if body != {}:
                        raise JsonApiInvalidEmptyPayloadException()
                if any([key not in body for key in ["data", "type", "id"]]):
                    raise JsonApiInvalidRequestException()
        except:
            raise JsonApiInvalidPayloadException()
    return None


def parse_query(url: str):
    """
    Parses the query parameters from the URL.

    Parameters
    ----------
    url : str
        The URL string.

    Returns
    -------
    dict
        The parsed query parameters.

    Notes
    -----
    This function parses the query parameters from the URL and returns them as a dictionary.
    The supported query parameter keys are "include", "fields", "sort", and "filters".
    """
    query = {}
    query_string = url.split("?")
    if len(query_string) > 1:
        for key, value in parser.parse(query_string[1]).items():
            key = key.lower()
            match key:
                case "include":
                    query["include"] = value.split(",")
                case "fields":
                    query["fields"] = [
                        {"type": k, "fields": v.split(",")} for k, v in value.items()
                    ]
                case "sort":
                    query["sort"] = [
                        {"field": f[-1], "dir": "DESC" if len(f) > 1 else "ASC"}
                        for field in value.split(",")
                        if (f := field.split("-"))
                    ]
                case "filters":
                    query[key] = {k: json.loads(v) for k, v in value.items()}
                case _:
                    query[key] = value

    return query


def parse_request(request: HttpRequest):
    """
    Parses the JSON API request.

    Parameters
    ----------
    request : HttpRequest
        The HTTP request object.

    Returns
    -------
    dict
        The parsed JSON API request parameters.

    Notes
    -----
    This function parses the JSON API request and returns a dictionary containing the parsed parameters.
    It validates the request using the `validate_jsonapi_request` function and extracts the resource type, resource ID,
    relation type, relation ID, and action (parsed query parameters) from the request.
    """
    validate_jsonapi_request(request)
    if request.route_params.get("relation_id"):
        if request.route_params.get("relation_type") == "relationships":
            return {
                "type": request.route_params.get("resource_type"),
                "id": request.route_params.get("resource_id"),
                "relation": request.route_params.get("relation_id"),
                "action": parse_query(request.url),
            }
        else:
            return {
                "type": request.route_params.get("resource_id"),
                "id": request.route_params.get("relation_id"),
                "relation": None,
                "action": parse_query(request.url),
            }
    else:
        return {
            "type": request.route_params.get("resource_type"),
            "id": request.route_params.get("resource_id"),
            "relation": request.route_params.get("relation_type"),
            "action": parse_query(request.url),
        }


from libs.data.structured import StructuredQueryFrame


def alter_response(response: HttpResponse, request: HttpRequest, **kwargs):
    """
    Modifies the JSON API response.

    Parameters
    ----------
    response : HttpResponse
        The HTTP response object.
    request : HttpRequest
        The HTTP request object.
    **kwargs
        Additional keyword arguments.

    Returns
    -------
    HttpResponse
        The modified HTTP response object.

    Notes
    -----
    This function is responsible for altering the JSON API response.
    It adds the "Content-Type" header with the value "application/vnd.api+json".
    If the response contains resources of type `StructuredQueryFrame`, it converts them to a JSON string.
    Otherwise, it tries to convert the resources to JSON using the `json.dumps` function.
    If that fails, it converts the resources to a string representation.
    The modified response object is then returned.
    """
    response.headers.add("Content-Type", "application/vnd.api+json")
    for resources in response.resources:
        if isinstance(resources, StructuredQueryFrame):
            response.set_body("[" + ",".join([str(row) for row in resources()]) + "]")
        else:
            try:
                response.set_body(json.dumps(resources))
            except:
                response.set_body(str(resources))
        break
    return response
