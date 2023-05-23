from .endpoints import REGISTRY as ENDPOINT_REGISTRY
from requests_auth_aws_sigv4 import AWSSigV4

import os
import requests


class OnSpotAPI:
    def __init__(
        self,
        access_key: str = os.environ.get("ONSPOT_ACCESS_KEY"),
        secret_key: str = os.environ.get("ONSPOT_SECRET_KEY"),
        api_key: str = os.environ.get("ONSPOT_API_KEY"),
        host: str = os.environ.get("ONSPOT_HOST", "api.qa.onspotdata.com"),
        region: str = os.environ.get("ONSPOT_REGION", "us-east-1"),
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
        return requests.post(
            url=self.url + endpoint,
            auth=self.auth,
            data=request,
            headers={**self.headers, "Content-type": "text/json"},
        )

    @staticmethod
    def pythonic_request(endpoint: str, request: str, context: dict = {}):
        return ENDPOINT_REGISTRY[endpoint].request(context=context).loads(request)


ENDPOINT_PATHS = [
    "geoframe/all/count",
    "geoframe/all/countgroupedbyday",
    "geoframe/all/countgroupedbydevice",
    "geoframe/all/countgroupedbyhour",
    "geoframe/all/countgroupedbyinterval",
    "geoframe/all/devicefrequencycount",
    "geoframe/all/devices",
    "geoframe/all/devicelocationsbyday",
    "geoframe/all/distinctdevicelocations",
    "geoframe/all/locations",
    "geoframe/all/observations",
    "geoframe/common/count",
    "geoframe/common/devices",
    "geoframe/demographics/aggregate",
    "geoframe/demographics/aggregate/zipcodes",
    "geoframe/extension/devicesinhome",
    "geoframe/extension/devicesinhomecount",
    "geoframe/household/count",
    "geoframe/household/devices",
    "geoframe/legacyextension/devicesinhome",
    "geoframe/political/aggregate",
    "geoframes/attribution",
    "geoframe/tradearea",
    "save/addresses/all/devices",
    "save/businessaddresses/all/demographics",
    "save/businessaddresses/all/devices",
    "save/businessaddresses/all/locations",
    "save/files/demographics/all",
    "save/files/extension/socialnetwork",
    "save/files/political",
    "save/geoframe/all/countgroupedbydevice",
    "save/geoframe/all/devices",
    "save/geoframe/all/devicecountataddress",
    "save/geoframe/all/devicecountatworkplaceaddress",
    "save/geoframe/all/observations",
    "save/geoframe/demographics/all",
    "save/geoframe/extension/devicesinhome",
    "save/geoframe/extension/socialnetwork",
    "save/geoframe/legacyextension/devicesinhome",
    "save/geoframe/political",
    "save/geoframe/tradearea",
    "save/geoframes/attribution/devices",
    "save/geoframes/attribution/journey",
    "save/geoframes/attribution/locations",
    "save/files/extension/devicesinhome",
    "devices/segment/push",
    "geoframe/segment/push",
]
