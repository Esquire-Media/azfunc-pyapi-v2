from aiopenapi3 import OpenAPI
from functools import cached_property
from libs.openapi.clients.meta.parser import MetaSDKParser
import httpx, pathlib, os, yaml

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

    def get_spec():
        return yaml.safe_load(
            open(pathlib.Path(pathlib.Path(__file__).parent.resolve(), "spec.yaml"))
        )
        

