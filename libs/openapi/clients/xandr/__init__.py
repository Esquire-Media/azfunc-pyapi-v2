import time
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
    _token_cache = {}

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
            session_factory=cls.session_async if asynchronus else cls.session_sync,
            plugins=[XandrReportFormatter()],
            use_operation_tags=False,
        )
        if not api_key and (username and password):
            api_key = cls.get_token(username, password)
        api.authenticate(
            token=api_key,
        )
        return api

    @classmethod
    def session_sync(cls, *args, **kwargs) -> httpx.Client:
        return httpx.Client(*args, timeout=None, **kwargs)

    @classmethod
    def session_async(cls, *args, **kwargs) -> httpx.AsyncClient:
        return httpx.AsyncClient(*args, timeout=None, **kwargs)

    @classmethod
    def get_token(
        cls,
        username: str = os.environ.get(
            "XANDR_USERNAME", os.environ.get("APPNEXUS_USERNAME")
        ),
        password: str = os.environ.get(
            "XANDR_PASSWORD", os.environ.get("APPNEXUS_PASSWORD")
        ),
    ):
        cache_entry = cls._token_cache.get(username)
        current_time = time.time()
        if cache_entry:
            token, timestamp = cache_entry
            # Invalidate token if older than 120 minutes (7200 seconds)
            if current_time - timestamp < 7200:
                return token

        api = OpenAPI(
            url=f"https://api.appnexus.com",
            document=XandrAPI.get_spec(),
            session_factory=httpx.Client,
        )
        auth = api.createRequest(("/auth", "post"))
        _, data, _ = auth.request(
            {"auth": {"password": password, "username": username}}
        )
        cls._token_cache[username] = (data.response.token, current_time)
        return data.response.token

    def get_spec():
        return yaml.safe_load(
            open(pathlib.Path(pathlib.Path(__file__).parent.resolve(), "spec.yaml"))
        )
