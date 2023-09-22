from libs.openapi.clients.base import OpenAPIClient
import yarl


class MicrosoftGraphAPI(OpenAPIClient):
    class Loader(OpenAPIClient.Loader):
        url = yarl.URL(
            "https://raw.githubusercontent.com/microsoftgraph/msgraph-metadata/master/openapi/beta/openapi.yaml"
        )
        format = "yaml"
        
        @staticmethod
        def _remove_parameter(document, path, parameter_name):
            if document["paths"].get(path, {}).get("parameters"):
                document["paths"][path]["parameters"] = [
                    p
                    for p in document["paths"][path]["parameters"]
                    if p.get("name", "") != parameter_name
                ]
        
        @classmethod
        def load(cls) -> dict:
            document = super().load()
            # Drop massive unnecessary discriminator
            del document["components"]["schemas"]["microsoft.graph.entity"][
                "discriminator"
            ]

            cls._remove_parameter(
                document, "/applications(appId='{appId}')", "uniqueName"
            )
            cls._remove_parameter(
                document, "/applications(uniqueName='{uniqueName}')", "appId"
            )
            # Fix invalids
            for operation in document.get("paths", {}).values():
                for details in operation.values():
                    # Check if parameters exist for this operation
                    if isinstance(details, dict):
                        parameters = details.get("parameters", [])
                        for parameter in parameters:
                            description = parameter.get("description", "")
                            # Check if description matches the desired format
                            if description.strip() == "Usage: on='{on}'":
                                parameter["name"] = "on"
                            if "content" in parameter.keys():
                                parameter["schema"] = parameter["content"].get("application/json", {}).get("schema", {})
                                del parameter["content"]

            # Drop requirement for @odata.type since it's not actually required
            for schema in document["components"]["schemas"].values():
                if "required" in schema:
                    schema["required"] = [
                        i for i in schema["required"] if i != "@odata.type"
                    ]
                    if not schema["required"]:
                        del schema["required"]
                if isinstance(schema, dict):
                    for s in schema.get("allOf", []):
                        if "required" in s:
                            s["required"] = [
                                i for i in s["required"] if i != "@odata.type"
                            ]
                            if not s["required"]:
                                del s["required"]

            document.setdefault("security", []).append({"token": []})
            document.setdefault("components", {}).setdefault(
                "securitySchemes", {}
            ).setdefault(
                "token",
                {"type": "http", "scheme": "bearer", "bearerFormat": "JWT"},
            )
            
            return document
        
    class Plugins(OpenAPIClient.Plugins):
        class Cull(OpenAPIClient.Plugins.Cull):
            def parsed(self, ctx):
                ctx = super().parsed(ctx)
                ctx.document.setdefault("security", []).append({"bearer": []})
                ctx.document.setdefault("components", {}).setdefault(
                    "securitySchemes", {}
                ).setdefault(
                    "bearer",
                    {"type": "http", "scheme": "bearer", "bearerFormat": "JWT"},
                )
                return ctx
    