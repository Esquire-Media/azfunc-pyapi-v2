from aiopenapi3 import OpenAPI
from aiopenapi3.extra import Cull
from pathlib import Path
from threading import Lock
from typing import Dict, List, Literal, Pattern, Tuple, Union
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
    
    class Loader:
        url: yarl.URL
        format: Literal["json", "yaml"]

        @classmethod
        def load(cls) -> dict:
            assert isinstance(cls.url, yarl.URL)
            data = httpx.get(str(cls.url)).content
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
        return httpx.Client(*args, timeout=None, **kwargs)

    @classmethod
    def session_async(cls, *args, **kwargs) -> httpx.AsyncClient:
        return httpx.AsyncClient(*args, timeout=None, **kwargs)

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
