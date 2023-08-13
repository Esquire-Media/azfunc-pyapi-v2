from aiopenapi3 import OpenAPI
from aiopenapi3.plugin import Init
from functools import cached_property
import copy, httpx, os, yaml, pathlib


class GraityFormsInitPlugin(Init):
    def __init__(self, url: str):
        super().__init__()
        self.url = url

    def initialized(self, ctx: "Init.Context") -> "Init.Context":
        return super().initialized(ctx)


class GravityFormsAPI:
    def __new__(
        cls,
        url: str = os.environ.get("GRAVITY_FORMS_URL"),
        username: str = os.environ.get("GRAVITY_FORMS_USERNAME"),
        password: str = os.environ.get("GRAVITY_FORMS_PASSWORD"),
        asynchronus: bool = True,
    ) -> OpenAPI:
        if url[-6:] != "/gf/v2":
            if url[-1] != "/":
                url += "/"
            url += "gf/v2"
        spec = copy.deepcopy(cls.get_spec())
        spec["servers"].append({"url": url})
        api = OpenAPI(
            url=url,
            document=spec,
            session_factory=httpx.AsyncClient if asynchronus else httpx.Client,
        )
        api.authenticate(
            basicAuth={
                "username": username,
                "password": password,
            }
        )
        return api

    @staticmethod
    def get_spec():
        return yaml.safe_load(
            open(pathlib.Path(pathlib.Path(__file__).parent.resolve(), "spec.yaml"))
        )
