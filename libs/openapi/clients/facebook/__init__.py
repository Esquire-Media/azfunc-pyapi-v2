from aiopenapi3 import ResponseDecodingError
from aiopenapi3.plugin import Message
from io import BytesIO
from libs.openapi.clients.base import OpenAPIClient
import pandas as pd


class FacebookReportError(Exception):
    pass


class Facebook(OpenAPIClient):
    class Loader(OpenAPIClient.Loader):
        @classmethod
        def load(cls) -> dict:
            return SPEC

    class Plugins(OpenAPIClient.Plugins):
        class FacebookReportFormatter(Message):
            def received(self, ctx: "Message.Context") -> "Message.Context":
                if ctx.operationId.startswith("Download"):
                    if ctx.received == b'\n"No data available."\n':
                        ctx.received = {}
                    elif ctx.received[0:5] == b"<?xml":
                        raise FacebookReportError("Something went wrong.")
                    else:
                        try:
                            ctx.received = pd.read_csv(BytesIO(ctx.received)).to_dict()
                        except Exception as e:
                            raise ResponseDecodingError(
                                ctx.operationId, ctx.received.decode(), None
                            ) from e
                    return ctx
    @classmethod
    def plugins(cls):
        return [cls.Plugins.FacebookReportFormatter()]


SPEC = {
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
                "responses": {"default": {"$ref": "#/components/responses/Generic"}},
                "security": [{"access_token": []}],
            }
        }
    },
    "tags": [],
}
