from aiopenapi3 import OpenAPI
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
            use_operation_tags=False,
        )
        api.authenticate(
            access_token=access_token,
        )
        return api

    def get_spec(*modules):
        return MetaSDKParser(*modules).spec
        return yaml.safe_load(
            open(pathlib.Path(pathlib.Path(__file__).parent.resolve(), "spec.yaml"))
        )
