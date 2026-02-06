from aiopenapi3 import OpenAPI
from aiopenapi3.extra import Cull
from pathlib import Path
from threading import Lock
from typing import Dict, List, Literal, Optional, Pattern, Tuple, Union
import gzip, httpx, io, orjson, yaml, yarl, re


class OperationSelector(type):
    def __getitem__(cls, item: Union[str, Tuple[str, str]]):
        with cls.cache_lock:
            if item not in cls.cached:
                match item:
                    case str():
                        cls.cached[item] = cls.__new__(cls, operations=item)
                    case re.Pattern():
                        cls.cached[item] = cls.__new__(cls, operations=item)
                    case tuple():
                        path, method = item
                        assert isinstance(path, str)
                        assert isinstance(method, str)
                        cls.cached[item] = cls.__new__(cls, operations=(path, [method]))
        op = list(cls.cached[item]._.Iter(cls.cached[item], False))[0]
        req = getattr(cls.cached[item]._, op)
        return cls.cached[item].createRequest((req.path, req.method))

    def paths(cls):
        for k, v in cls.load()["paths"].items():
            for m, o in v.items():
                if isinstance(o, dict):
                    yield (k, m)

    def operations(cls):
        for k, v in cls.load()["paths"].items():
            for m, o in v.items():
                if isinstance(o, dict):
                    yield (o.get("operationId", None), k, m)

    def items(cls):
        for k, v in cls.load()["paths"].items():
            for m, o in v.items():
                if isinstance(o, dict):
                    yield ((k, m), cls[k, m])

    def values(cls):
        for k, v in cls.load()["paths"].items():
            for m, o in v.items():
                if isinstance(o, dict):
                    yield cls[k, m]


class OpenAPIClient(metaclass=OperationSelector):
    cached = {}
    cache_lock = Lock()  # Lock for controlling access to cache

    # Connection pool settings (class-level defaults)
    limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)
    timeout = None  # Can be overridden per-subclass

    # Singleton client instances (lazily initialized)
    _sync_client: Optional[httpx.Client] = None
    _async_client: Optional[httpx.AsyncClient] = None
    _client_lock = Lock()

    @classmethod
    def get_sync_client(cls) -> httpx.Client:
        """Get or create the singleton sync httpx.Client with connection pooling."""
        if cls._sync_client is None:
            with cls._client_lock:
                if cls._sync_client is None:
                    cls._sync_client = httpx.Client(
                        limits=cls.limits, timeout=cls.timeout
                    )
        return cls._sync_client

    @classmethod
    def get_async_client(cls) -> httpx.AsyncClient:
        """Get or create the singleton async httpx.AsyncClient with connection pooling."""
        if cls._async_client is None:
            with cls._client_lock:
                if cls._async_client is None:
                    cls._async_client = httpx.AsyncClient(
                        limits=cls.limits, timeout=cls.timeout
                    )
        return cls._async_client
    
    class Loader:
        url: yarl.URL
        format: Literal["json", "yaml"]

        @classmethod
        def load(cls) -> dict:
            assert isinstance(cls.url, yarl.URL)
            data = OpenAPIClient.get_sync_client().get(str(cls.url)).content
            match cls.format:
                case "yaml":
                    return yaml.load(io.BytesIO(data), Loader=yaml.CLoader)
                case "json":
                    return orjson.loads(data)
                case _:
                    raise (Exception("origin format not supported"))

    class Plugins:
        class Cull(Cull):
            def paths(self, ctx):
                return ctx

    specs_path = Path(Path(__file__).parent.parent.resolve(), "specs")

    def __new__(
        cls,
        operations: Dict[Union[str, Pattern], List[Union[str, Pattern]]] = None,
        sync: bool = True,
        **kwargs,
    ) -> OpenAPI:
        if operations:
            kwargs["plugins"] = kwargs.get("plugins", []) + [
                cls.Plugins.Cull(operations)
            ]
        if hasattr(cls, "plugins"):
            kwargs["plugins"] = kwargs.get("plugins", []) + cls.plugins()

        api = OpenAPI.loads(
            session_factory=cls.session_sync if sync else cls.session_async,
            **kwargs,
            url=f"openapi.json",
            data=cls.load_bytes().decode(),
        )
        if hasattr(cls, "authenticate"):
            api.authenticate(**cls.authenticate())

        return api

    @classmethod
    def session_sync(cls, *args, **kwargs) -> httpx.Client:
        """Return the singleton sync client for connection reuse."""
        return cls.get_sync_client()

    @classmethod
    def session_async(cls, *args, **kwargs) -> httpx.AsyncClient:
        """Return the singleton async client for connection reuse."""
        return cls.get_async_client()

    @classmethod
    def load_bytes(cls) -> bytes:
        path = Path(cls.specs_path, cls.__name__ + ".json.gz")
        if not path.exists():
            cls.save_origin()
        with gzip.GzipFile(path, "r") as file:
            return file.read()

    @classmethod
    def load(cls) -> dict:
        return orjson.loads(cls.load_bytes())

    @classmethod
    def save(cls, data: Union[str, Dict]):
        with gzip.GzipFile(
            Path(cls.specs_path, cls.__name__ + ".json.gz"),
            "w",
            compresslevel=9,
        ) as file:
            match data:
                case str():
                    file.write(data)
                case dict():
                    file.write(orjson.dumps(data))

    @classmethod
    def save_origin(cls):
        return cls.save(cls.Loader().load())
