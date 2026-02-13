from aiopenapi3.plugin import Message, Document
from libs.openapi.clients.meta.generator import generate_openapi
from libs.openapi.clients.meta.parser import MetaSDKParser
from libs.openapi.clients.base import OpenAPIClient
import os


class Meta(OpenAPIClient):
    class Loader(OpenAPIClient.Loader):
        @classmethod
        def load(cls) -> dict:
            return generate_openapi()

    class Plugins(OpenAPIClient.Plugins):
        #     class Pagination(Message):
        #         def unmarshalled(self, ctx: "Message.Context") -> "Message.Context":
        #             ctx.unmarshalled = getattr(ctx.unmarshalled, "root", ctx.unmarshalled)
        #             if hasattr(ctx.unmarshalled, "data") and hasattr(ctx.unmarshalled, "paging"):
        #                 pass
        #             return ctx
        class Fixer(Document):
            def parsed(self, ctx: "Document.Context") -> "Message.Context":
                ctx.document["components"]["parameters"]["CustomAudience-Payload"] = {
                    "name": "payload",
                    "in": "query",
                    "explode": False,
                    "schema": {"type": "string"},
                }
                ctx.document["components"]["parameters"]["CustomAudience-Session"] = {
                    "name": "session",
                    "in": "query",
                    "explode": False,
                    "schema": {"type": "string"},
                }
                return ctx

    @classmethod
    def plugins(cls):
        return [cls.Plugins.Fixer()]

    def authenticate() -> dict:
        if os.environ.get("META_ACCESS_TOKEN"):
            return {"AccessToken": os.environ["META_ACCESS_TOKEN"]}
        return {}
