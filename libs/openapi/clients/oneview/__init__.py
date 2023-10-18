from aiopenapi3.plugin import Message
from libs.openapi.clients.base import OpenAPIClient
from pathlib import Path
import httpx, orjson, os


class OneView(OpenAPIClient):
    class Loader(OpenAPIClient.Loader):
        @classmethod
        def load(cls) -> dict:
            with open(Path(Path(__file__).parent.resolve(), "spec.json")) as f:
                return orjson.loads(f.read())

    class Plugins(OpenAPIClient.Plugins):
        class ContentType(Message):
            def received(self, ctx: "Message.Context") -> "Message.Context":
                ctx.content_type = "application/json"
                return ctx

    @classmethod
    def authenticate(cls):
        session = httpx.Client()
        login = session.get("https://oneview.roku.com/sso/login", follow_redirects=True)
        identifier = session.post(
            "https://login.oneview.roku.com/u/login/identifier",
            params=login.url.params,
            data={
                "state": login.url.params.get("state"),
                "username": os.environ["ONEVIEW_USERNAME"],
                "js-available": "true",
                "webauthn-available": "true",
                "is-brave": "false",
                "webauthn-platform-available": "true",
                "action": "default",
            },
            follow_redirects=True,
        )
        session.post(
            "https://login.oneview.roku.com/u/login/password",
            params=login.url.params,
            data={
                "state": identifier.url.params.get("state"),
                "username": os.environ["ONEVIEW_USERNAME"],
                "password": os.environ["ONEVIEW_PASSWORD"],
                "action": "default",
            },
            follow_redirects=True,
        )
        return {k: v for k, v in session.cookies.items()}

    @classmethod
    def plugins(cls):
        return [cls.Plugins.ContentType()]
