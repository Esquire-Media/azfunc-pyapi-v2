from aiopenapi3 import OpenAPI
import base64, httpx, json, os


class ClickUpAPI:
    def __new__(
        cls,
        api_key: str = os.environ.get("CLICKUP_API_KEY"),
    ) -> OpenAPI:
        spec = json.loads(
            json.loads(
                base64.b64decode(
                    httpx.get(
                        "https://shuffler.io/api/v1/apps/82b7d2ef374f8f70575742652f5fd24b/config"
                    ).json()["openapi"]
                )
            )["body"]
        )
        api = OpenAPI(
            url=f'https://api.clickup.com',
            document=spec
        )
        api.authenticate(
            apiKey=api_key
        )
        return api
