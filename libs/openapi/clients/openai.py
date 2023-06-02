from aiopenapi3 import OpenAPI

class OpenAIAPI:
    def __new__(
        cls,
    ) -> OpenAPI:
        return OpenAPI.load_sync(
            f"https://raw.githubusercontent.com/APIs-guru/openapi-directory/main/APIs/openai.com/1.2.0/openapi.yaml"
        )
