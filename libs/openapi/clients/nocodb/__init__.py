from aiopenapi3 import OpenAPI
from libs.openapi.clients.base import OpenAPIClient
from typing import Dict, List, Pattern, Union
import httpx, orjson, logging


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

        spec_url = host + "/api/v2/meta/bases/" + project_id + "/swagger.json"

        if api_token:
            headers = {"xc-token": api_token}
        if auth_token:
            headers = {"xc-auth": auth_token}

        spec = httpx.get(spec_url, headers=headers).json()

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
