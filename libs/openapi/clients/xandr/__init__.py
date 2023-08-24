from aiopenapi3 import OpenAPI, ResponseDecodingError
from aiopenapi3.plugin import Message
from io import BytesIO
import httpx, pathlib, os, yaml
import pandas as pd


class XandrReportFormatter(Message):
    def received(self, ctx: "Message.Context") -> "Message.Context":
        if ctx.operationId.startswith("Download"):
            try:
                ctx.received = pd.read_csv(BytesIO(ctx.received)).to_dict()
            except Exception as e:
                raise ResponseDecodingError(ctx.received, None, None) from e
            return ctx


class XandrAPI:
    api_key = None
    
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
            document=XandrAPI.get_spec(),
            session_factory=httpx.AsyncClient if asynchronus else httpx.Client,
            plugins=[XandrReportFormatter()],
            use_operation_tags=False,
        )
        if cls.api_key and not api_key:
            api_key = cls.api_key
        if not api_key and username and password:
            api_key = cls.api_key = cls.get_token(username, password)
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
            document=XandrAPI.get_spec(),
            session_factory=httpx.Client,
        )
        auth = api.createRequest(("/auth", "post"))
        _, data, _ = auth.request(
            {"auth": {"password": password, "username": username}}
        )
        return data.response.token

    def get_spec():
        return yaml.safe_load(
            open(pathlib.Path(pathlib.Path(__file__).parent.resolve(), "spec.yaml"))
        )
