from aiopenapi3 import OpenAPI
from libs.openapi.clients.base import OpenAPIClient
from typing import Dict, List, Pattern, Union
import httpx, orjson


class NocoDB(OpenAPIClient):
    def __new__(
        cls,
        host: str,
        project_id: str,
        api_token: str = None,
        auth_token: str = None,
        operations: Dict[str | Pattern, List[str | Pattern]] = None,
        sync: bool = True,
        **kwargs
    ) -> OpenAPI:
        if hasattr(cls, "plugins"):
            kwargs["plugins"] = kwargs.get("plugins", []) + cls.plugins()

        spec_url = host + "/api/v1/db/meta/projects/" + project_id + "/swagger.json"

        if api_token:
            headers = {"xc-token": api_token}
        if auth_token:
            headers = {"xc-auth": auth_token}

        spec = httpx.get(spec_url, headers=headers).json()
        prefix = "/api/v1/db/data/v1/" + project_id
        # Check and update the servers key
        if "servers" in spec and len(spec["servers"]) > 0:
            # Append prefix to the URL of the first server (assuming it's the default server)
            spec["servers"][0]["url"] += prefix
        else:
            # If no servers are defined, add a default one with the prefix
            spec["servers"] = [{"url": host + prefix}]

        # Iterate through the paths and remove the prefix
        updated_paths = {}
        for path, details in spec["paths"].items():
            updated_path = path.replace(prefix, "")
            updated_paths[updated_path] = details

        spec["paths"] = updated_paths

        api = OpenAPI.loads(
            session_factory=cls.session_sync if sync else cls.session_async,
            **kwargs,
            url=spec_url,
            data=orjson.dumps(spec),
            use_operation_tags=False
        )
        if api_token:
            api.authenticate(**{"xcToken": api_token})
        if auth_token:
            api.authenticate(**{"xcAuth": auth_token})

        return api

    @classmethod
    def save(cls, data: Union[str, Dict]):
        pass

    @classmethod
    def save_origin(cls):
        pass
