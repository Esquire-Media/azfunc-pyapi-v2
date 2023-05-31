from libs.utils.decorators import staticproperty
from shutil import copyfileobj
from smart_open import open
from typing import Any, Callable, List
import smart_open.transport

_RENAME = {"azure": "azure_blob"}


class StreamKeyValueProvider:
    """
    Stream-based key-value storage provider.

    This class provides methods to save, load, and delete key-value pairs
    using stream-based storage mechanisms.
    """

    @staticproperty
    def SUPPORTED_SCHEMES(self) -> List[str]:
        """
        List of supported schemes.

        Returns
        -------
        List[str]
            A list of supported schemes.
        """

        return list(
            map(
                lambda x: _RENAME[x] if x in _RENAME else x,
                filter(len, smart_open.transport.SUPPORTED_SCHEMES),
            )
        )

    def __init__(self, *args, **kwargs) -> None:
        """
        Initialize an instance of StreamKeyValueProvider.

        Parameters
        ----------
        *args : tuple
            Additional positional arguments.
        **kwargs : dict
            Additional keyword arguments.
        """

        if len(args):
            self.scheme = args[0]
        if "scheme" in kwargs:
            self.scheme = kwargs.pop("scheme")
        for key, value in _RENAME.items():
            if self.scheme == value:
                self.scheme = key
        self.config = {**kwargs}

    def connect(self, key: str, **kwargs) -> Any:
        """
        Connect to a key.

        Parameters
        ----------
        key : str
            The key to connect to.
        **kwargs : dict
            Additional keyword arguments.

        Returns
        -------
        Any
            The connection to the specified key.
        """

        return open(self.scheme + "://" + key, **{**kwargs, **self.config})

    def save(self, key: str, value: Any, encoder: Callable = None, **kwargs) -> None:
        """
        Save a key-value pair.

        Parameters
        ----------
        key : str
            The key associated with the value.
        value : Any
            The value to be saved.
        encoder : Callable, optional
            The encoder function to use for encoding the value, by default None.
        **kwargs : dict
            Additional keyword arguments.

        Raises
        ------
        TypeError
            If the value is not a supported type.
        """

        if encoder is not None:
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
        """
        Load a value from a key.

        Parameters
        ----------
        key : str
            The key associated with the value.
        decoder : Callable, optional
            The decoder function to use for decoding the value, by default None.
        **kwargs : dict
            Additional keyword arguments.

        Returns
        -------
        Any
            The loaded value.
        """

        if decoder:
            return decoder(self.connect(key, mode="rb"), **kwargs)
        return self.connect(key, mode="r").read()

    def drop(self, key: str, **kwargs) -> None:
        """
        Delete a key-value pair from the store.

        Parameters
        ----------
        key : str
            The key associated with the value to be deleted.
        **kwargs : dict
            Additional keyword arguments.
        """

        pass
