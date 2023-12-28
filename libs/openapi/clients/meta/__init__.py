# from aiopenapi3.plugin import Message
from libs.openapi.clients.meta.generator import generate_openapi
from libs.openapi.clients.meta.parser import MetaSDKParser
from libs.openapi.clients.base import OpenAPIClient


class Meta(OpenAPIClient):
    class Loader(OpenAPIClient.Loader):
        @classmethod
        def load(cls) -> dict:
            return generate_openapi()

    # class Plugins(OpenAPIClient.Plugins):
    #     class Pagination(Message):
    #         def unmarshalled(self, ctx: "Message.Context") -> "Message.Context":
    #             ctx.unmarshalled = getattr(ctx.unmarshalled, "root", ctx.unmarshalled)
    #             if hasattr(ctx.unmarshalled, "data") and hasattr(ctx.unmarshalled, "paging"):
    #                 pass
    #             return ctx

    # @classmethod
    # def plugins(cls):
    #     return [cls.Plugins.Pagination()]

# Legacy
from aiopenapi3 import OpenAPI
import httpx, yaml, os


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
