from libs.openapi.clients.base import OpenAPIClient
from typing import Dict
import yarl


class MicrosoftGraph(OpenAPIClient):
    class Loader(OpenAPIClient.Loader):
        url = yarl.URL(
            "https://raw.githubusercontent.com/microsoftgraph/msgraph-metadata/master/openapi/beta/openapi.yaml"
        )
        format = "yaml"
        
        @staticmethod
        def _remove_parameter(document: Dict, path: str, parameter_name: str):
            if document["paths"].get(path, {}).get("parameters"):
                document["paths"][path]["parameters"] = [
                    p
                    for p in document["paths"][path]["parameters"]
                    if p.get("name", "") != parameter_name
                ]

        @staticmethod
        def _drop_required(schema: Dict, requirement: str) -> None:
            if "required" in schema:
                schema["required"] = [i for i in schema["required"] if i != requirement]
                if not schema["required"]:
                    del schema["required"]
        
        @classmethod
        def load(cls) -> dict:
            document = super().load()
            # Drop massive unnecessary discriminator
            del document["components"]["schemas"]["microsoft.graph.entity"][
                "discriminator"
            ]
            # Remove superfluous parameters
            cls._remove_parameter(document, "/applications(appId='{appId}')", "uniqueName")
            cls._remove_parameter(document, "/applications(uniqueName='{uniqueName}')", "appId")
            # Fix parameter names
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
            # Drop requirement for @odata.type since it's not actually enforced
            for schema in document.get("components", {}).get("schemas", {}).values():
                if isinstance(schema, dict):
                    cls._drop_required(schema, "@odata.type")
                    for s in schema.get("allOf", []):
                        cls._drop_required(s, "@odata.type")
            return document
