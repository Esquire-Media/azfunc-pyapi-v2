from aiopenapi3 import OpenAPI
from aiopenapi3.plugin import Init
import httpx
import httpx_auth
import os


class OnSpotInitPlugin(Init):
    """
    A class used to initialize the OnSpot API.

    Attributes
    ----------
    production : bool
        A flag indicating whether the API is in production mode.

    Methods
    -------
    initialized(ctx)
        Runs when the OpenAPI object is initialized.
    """

    def __init__(self, production=False):
        """
        Constructs all the necessary attributes for the OnSpotInitPlugin object.

        Parameters
        ----------
        production : bool, optional
            A flag indicating whether the API is in production mode (default is False).
        """
        super().__init__()
        self.production = production

    def initialized(self, ctx: "Init.Context") -> "Init.Context":  # pragma: no cover
        """
        Updates the server list and security schemes in the given context.

        Parameters
        ----------
        ctx : Init.Context
            The context to be initialized.

        Returns
        -------
        Init.Context
            The initialized context.
        """
        ctx.initialized.servers = [
            server
            for server in ctx.initialized.servers
            if ("Production" if self.production else "QA") in server.description
        ]
        if sigv4 := ctx.initialized.components.securitySchemes.get("sigv4", None):
            ctx.initialized.components.securitySchemes["sigv4"] = type(sigv4)(
                type="http", scheme="aws4auth"
            )


class OnSpotAPI:
    """
    A class used to interact with the OnSpot API.

    This class is a wrapper around the OpenAPI object, providing a convenient
    way to interact with the OnSpot API.

    Methods
    -------
    __new__(access_key, secret_key, api_key, region, production)
        Constructs and returns an OpenAPI object for interacting with the
        OnSpot API.

    Notes
    -----
    The `__new__` method is used instead of `__init__` because we want to
    return an instance of OpenAPI, not an instance of OnSpotAPI.

    Examples
    --------
    >>> api = OnSpotAPI()
    >>> session_factory = api.createRequest(("/geoframe/all/count", "post"))
    OR
    >>> session_factory = api.createRequest("Device Counts.geoframeAllCount")
    Both of these session_factory variables use the same endpoint. The first
    instantiates the factory by using a (path, method) tuple. The second uses
    a string in the following format Tag.OperationID.
    >>> await session_factory(
    >>>     data={
    >>>         "type": "FeatureCollection",
    >>>         "features": [
    >>>             {
    >>>                 "type": "Feature",
    >>>                 "geometry": {
    >>>                     "type": "Polygon",
    >>>                     "coordinates": [
    >>>                         [
    >>>                             [-123.46478462219238, 39.07166187346857],
    >>>                             [-123.4659218788147, 39.06921298141872],
    >>>                             [-123.46278905868529, 39.0687964947246],
    >>>                             [-123.4616732597351, 39.07079560844253],
    >>>                             [-123.46478462219238, 39.07166187346857],
    >>>                         ]
    >>>                     ],
    >>>                 },
    >>>                 "properties": {
    >>>                     "name": "Feature Example",
    >>>                     "start": "2022-06-08T00:00:00",
    >>>                     "end": "2022-06-19T23:59:59",
    >>>                     "callback": "https://iupd9.localto.net/api/logger",
    >>>                 },
    >>>             }
    >>>         ],
    >>>     }
    >>> )
    """

    def __new__(
        cls,
        access_key: str = os.environ.get("ONSPOT_ACCESS_KEY"),
        secret_key: str = os.environ.get("ONSPOT_SECRET_KEY"),
        api_key: str = os.environ.get("ONSPOT_API_KEY"),
        region: str = os.environ.get("ONSPOT_REGION", "us-east-1"),
        production: bool = False,
    ) -> OpenAPI:
        """
        Constructs and returns an OpenAPI object for interacting with the OnSpot API.

        The access key, secret key, API key, and region are obtained from the environment variables
        ONSPOT_ACCESS_KEY, ONSPOT_SECRET_KEY, ONSPOT_API_KEY, and ONSPOT_REGION, respectively. If
        ONSPOT_REGION is not set, "us-east-1" is used as the default region. The production flag is
        False by default.

        Parameters
        ----------
        access_key : str, optional
            The access key for the API.
        secret_key : str, optional
            The secret key for the API.
        api_key : str, optional
            The API key for the API.
        region : str, optional
            The region for the API.
        production : bool, optional
            A flag indicating whether the API is in production mode.

        Returns
        -------
        OpenAPI
            An OpenAPI object for interacting with the OnSpot API.

        Notes
        -----
        The OpenAPI object returned by this method is authenticated and ready
        to use for making API requests.

        The OpenAPI specification for OnSpot is only available through their API.
        Therefore, in order to bootstrap the OpenAPI client for OnSpot, this method
        first sends an authenticated GET request to the OnSpot API. The JSON response
        from this request is then used as the document parameter to initialize the
        OpenAPI object.
        """
        spec_url = f'https://api.{"" if production else "qa."}onspotdata.com/openapi'
        api = OpenAPI(
            url=spec_url,
            document=httpx.get(
                spec_url,
                auth=httpx_auth.AWS4Auth(
                    access_id=access_key,
                    secret_key=secret_key,
                    region=region,
                    service="execute-api",
                )
                + httpx_auth.HeaderApiKey(api_key=api_key, header_name="x-api-key"),
            ).json(),
            plugins=[OnSpotInitPlugin(production=production)],
        )
        api.authenticate(
            apiKey=api_key,
            sigv4={
                "access_id": access_key,
                "secret_key": secret_key,
                "service": "execute-api",
                "region": region,
            },
        )
        return api
