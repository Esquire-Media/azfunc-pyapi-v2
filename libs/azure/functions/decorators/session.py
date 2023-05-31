from azure.functions._http import HttpResponse
from azure.functions.http import HttpRequest
from itsdangerous import URLSafeTimedSerializer
from typing import Any
from urllib.parse import urlparse
from http.cookies import SimpleCookie
import uuid


def parse_request(request: HttpRequest, secret: str, max_age: int, **kwargs) -> dict:
    """
    Parses the request and retrieves the session information.

    Parameters
    ----------
    request : HttpRequest
        The HTTP request object.
    secret : str
        The secret key used for session serialization and signing.
    max_age : int
        The maximum age (in seconds) for the session token.
    **kwargs
        Additional keyword arguments.

    Returns
    -------
    dict
        The session information extracted from the request.

    Raises
    ------
    Exception
        If there is an error while parsing the session token.

    Notes
    -----
    If the session token is not present or invalid, a new session with a randomly generated ID will be created.
    """
    cookies = SimpleCookie()
    cookies.load(request.headers.get("Cookie", ""))
    try:
        return URLSafeTimedSerializer(secret_key=secret).loads(
            getattr(cookies.get("SessionToken", None), "value", ""),
            max_age=max_age,
        )
    except:
        return {"id": uuid.uuid4().hex}


def _generate_session_cookie(
    session: Any,
    secret: str,
    **kwargs,
) -> SimpleCookie:
    """
    Generates a session cookie based on the provided session.

    Parameters
    ----------
    session : Any
        The session information to be serialized and stored in the cookie.
    secret : str
        The secret key used for session serialization and signing.
    **kwargs
        Additional keyword arguments.

    Returns
    -------
    SimpleCookie
        The generated session cookie.

    Notes
    -----
    Additional keyword arguments will be set as attributes of the session cookie.
    """
    cookies = SimpleCookie()
    cookies["SessionToken"] = URLSafeTimedSerializer(secret_key=secret).dumps(session)
    for key, value in kwargs.items():
        cookies["SessionToken"][key.replace("_", "-")] = value
    return cookies


def alter_response(response: HttpResponse, request: HttpRequest, **kwargs):
    """
    Alters the response by adding a session cookie.

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
        The altered HTTP response object.

    Notes
    -----
    The session cookie is added to the response headers with the specified attributes.
    """
    cookies = _generate_session_cookie(
        session=getattr(request, "session", {"id": uuid.uuid4().hex}),
        secret=kwargs["secret"],
        domain=urlparse(request.url).netloc,
        path=kwargs.get("path", "/"),
        max_age=kwargs["max_age"],
        secure=kwargs.get("secure", True),
        httponly=kwargs.get("httponly", False),
    )
    for morsel in cookies.values():
        response.headers.add("Set-Cookie", morsel.OutputString())
    return response
