from aiopenapi3 import OpenAPI
import httpx, pathlib, os, yaml


class XandrAPI:
    def __new__(
        cls,
        api_key: str = os.environ.get(
            "XANDR_API_KEY", os.environ.get("APPNEXUS_API_KEY")
        ),
        username: str = os.environ.get(
            "XANDR_USERNAME", os.environ.get("APPNEXUS_USERNAME")
        ),
        password: str = os.environ.get(
            "XANDR_PASSWORD", os.environ.get("APPNEXUS_PASSWORD")
        ),
        asynchronus: bool = True,
    ) -> OpenAPI:
        api = OpenAPI(
            url=f"https://api.appnexus.com",
            document=XandrAPI.spec,
            session_factory=httpx.AsyncClient if asynchronus else httpx.Client,
        )
        if not api_key and username and password:
            api_key = cls.get_token(username, password)
        api.authenticate(
            token=api_key,
        )
        return api

    @staticmethod
    def get_token(
        username: str = os.environ.get(
            "XANDR_USERNAME", os.environ.get("APPNEXUS_USERNAME")
        ),
        password: str = os.environ.get(
            "XANDR_PASSWORD", os.environ.get("APPNEXUS_PASSWORD")
        ),
    ):
        api = OpenAPI(
            url=f"https://api.appnexus.com",
            document=XandrAPI.spec,
            session_factory=httpx.Client,
        )
        auth = api.createRequest(("/auth", "post"))
        _, data, _ = auth.request(
            {"auth": {"password": password, "username": username}}
        )
        return data.response.token

    spec = yaml.safe_load(
        open(pathlib.Path(pathlib.Path(__file__).parent.resolve(), "spec.yaml"))
    )
