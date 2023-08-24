from aiopenapi3 import OpenAPI
from libs.openapi.clients.meta.parser import MetaSDKParser
import httpx, os, yaml


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
            session_factory=cls.async_session_factory
            if asynchronus
            else cls.session_factory,
            use_operation_tags=False,
        )
        api.authenticate(
            access_token=access_token,
        )
        return api

    def session_factory(*args, **kwargs) -> httpx.Client:
        return httpx.Client(*args, timeout=None, **kwargs)

    def async_session_factory(*args, **kwargs) -> httpx.AsyncClient:
        return httpx.AsyncClient(*args, timeout=None, **kwargs)

    def get_spec(*modules):
        return MetaSDKParser(*modules).spec

    def generate_yaml_file(*modules):
        yaml.representer.Representer.ignore_aliases = lambda *data: True

        # Generate spec.yaml
        with open("libs/openapi/clients/meta/spec.yaml", "w") as file:
            yaml.dump(
                MetaAPI.get_spec(*modules),
                file,
            )
