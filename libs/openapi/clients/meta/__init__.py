from aiopenapi3 import OpenAPI
from functools import cached_property
from libs.openapi.clients.meta.parser import MetaSDKParser
import httpx, os

class MetaAPI:
    def __new__(
        cls,
        access_token: str = os.environ.get("META_ACCESS_TOKEN"),
        modules: list = [],
        asynchronus: bool = True,
    ) -> OpenAPI:
        api = OpenAPI(
            url=f"https://graph.facebook.com",
            document=MetaSDKParser(*modules).spec,
            session_factory=httpx.AsyncClient if asynchronus else httpx.Client,
        )
        api.authenticate(
            token=access_token,
        )
        return api

    @cached_property
    def spec():
        return MetaSDKParser().spec
        

