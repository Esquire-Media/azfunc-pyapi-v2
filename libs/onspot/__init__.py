from .endpoints import REGISTRY as ENDPOINT_REGISTRY, Endpoint
from marshmallow import Schema
from requests_auth_aws_sigv4 import AWSSigV4

import os
import requests


class OnSpotAPI:
    def __init__(
        self,
        access_key: str = os.environ.get("onspot_access_key"),
        secret_key: str = os.environ.get("onspot_secret_key"),
        api_key: str = os.environ.get("onspot_api_key"),
        host: str = os.environ.get("onspot_host", "api.qa.onspotdata.com"),
        region: str = os.environ.get("onspot_region", "us-east-1"),
    ):
        self.auth = AWSSigV4(
            service="execute-api",
            region=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        self.headers = {"x-api-key": api_key}
        self.url = f"https://{host}/"

    def submit(self, endpoint: str, request: str):
        # send request
        response = requests.post(
            url=self.url + endpoint,
            auth=self.auth,
            data=request,
            headers={**self.headers, "Content-type": "text/json"},
        )

        # deserialize response
        response_schema: Schema = endpoint.response()
        return response_schema.loads(response.content, many=True)

    @staticmethod
    def build_request(endpoint: str, request: dict, context: dict = {}):
        return ENDPOINT_REGISTRY[endpoint].request(context=context).load(request)
