from libs.utils.decorators import staticproperty
from shutil import copyfileobj
from smart_open import open
from typing import Any, Callable, List
import smart_open.transport

_RENAME = {"azure": "azure_blob"}


class StreamKeyValueProvider:
    @staticproperty
    def SUPPORTED_SCHEMES(self) -> List[str]:
        return list(
            map(
                lambda x: _RENAME[x] if x in _RENAME else x,
                filter(len, smart_open.transport.SUPPORTED_SCHEMES),
            )
        )

    def __init__(self, *args, **kwargs) -> None:
        if len(args):
            self.scheme = args[0]
        if "scheme" in kwargs:
            self.scheme = kwargs.pop("scheme")
        for key, value in _RENAME.items():
            if self.scheme == value:
                self.scheme = key
        self.config = {**kwargs}
    
    def __getitem__(self, handle):
        return self.connect(handle)

    def connect(self, key: str, **kwargs) -> Any:
        return open(self.scheme + "://" + key, **{**kwargs, **self.config})

    def save(self, key: str, value: Any, encoder: Callable = None, **kwargs) -> None:
        if encoder != None:
            value = encoder(value, **kwargs)
            return self.save(key, value)
        elif callable(getattr(value, "read", None)):
            copyfileobj(value, self.connect(key))
        else:
            if isinstance(value, bytes):
                self.connect(key, mode="wb").write(value)
            elif isinstance(value, str):
                self.connect(key, mode="w").write(value)
            else:
                raise TypeError(
                    "StreamStorageProvider can only save strings and bytes. Use the encoder argument and any keyword arguments to transform the value into a bytes type object."
                )

    def load(self, key: str, decoder: Callable = None, **kwargs) -> Any:
        if decoder:
            return decoder(self.connect(key, mode="rb"), **kwargs)
        return self.connect(key, mode="r").read()

    def drop(self, key: str, **kwargs) -> None:
        """TODO: provide a delete mechanism for each schema type"""
        pass
