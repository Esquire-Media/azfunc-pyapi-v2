from aiopenapi3 import OpenAPI, ResponseDecodingError
from aiopenapi3.plugin import Message
from io import BytesIO
import httpx, os
import pandas as pd


class FacebookReportFormatter(Message):
    def received(self, ctx: "Message.Context") -> "Message.Context":
        if ctx.operationId.startswith("Download"):
            try:
                ctx.received = pd.read_csv(BytesIO(ctx.received)).to_dict()
            except Exception as e:
                raise ResponseDecodingError(ctx.received, None, None) from e
            return ctx


class FacebookAPI:
    def __new__(
        cls,
        access_token: str = os.environ["META_ACCESS_TOKEN"],
        asynchronus: bool = True,
    ) -> OpenAPI:
        api = OpenAPI(
            url=f"https://www.facebook.com",
            document=cls.get_spec(),
            session_factory=cls.async_session_factory
            if asynchronus
            else cls.session_factory,
            plugins=[FacebookReportFormatter()],
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

    def get_spec():
        return {
            "openapi": "3.1.0",
            "info": {
                "title": "Facebook Report Exporter",
                "version": "v17.0",
                "description": "Note: this endpoint is not part of our versioned Graph API and therefore does not conform to its breaking-change policy. Scripts and programs should not rely on the format of the result as it may change unexpectedly.",
                "contact": {
                    "email": "isaac@esqads.com",
                    "name": "Isaac Jesup",
                },
            },
            "servers": [{"url": "https://www.facebook.com"}],
            "components": {
                "securitySchemes": {
                    "access_token": {
                        "type": "apiKey",
                        "in": "query",
                        "name": "access_token",
                    }
                },
                "parameters": {},
                "responses": {
                    "Generic": {
                        "description": f"Results of a request who's schema is not implemented yet.",
                        "content": {
                            "application/vnd.ms-excel": {
                                "schema": {"additionalProperties": True}
                            }
                        },
                    },
                },
                "schemas": {
                    "Generic": {"additionalProperties": True},
                },
            },
            "paths": {
                "/ads/ads_insights/export_report": {
                    "get": {
                        "operationId": "Download",
                        "parameters": [
                            {
                                "in": "query",
                                "name": "name",
                                "required": "true",
                                "schema": {"type": "string"},
                            },
                            {
                                "in": "query",
                                "name": "format",
                                "required": "true",
                                "schema": {"type": "string", "enum": ["csv", "xls"]},
                            },
                            {
                                "in": "query",
                                "name": "report_run_id",
                                "required": "true",
                                "schema": {"type": "string"},
                            },
                        ],
                        "responses": {
                            "default": {"$ref": "#/components/responses/Generic"}
                        },
                        "security": [{"access_token": []}],
                    }
                }
            },
            "tags": [],
        }
